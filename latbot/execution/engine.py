"""ExecutionEngine - MANUAL support and the AUTO round trip.

AUTO flow (``execute``)::

    AUTO_SELECTED -> revalidate (latest book, risk, balance)
                  -> EXECUTING: entry LIMIT IOC at entry_vwap * (1 +/- buy_limit_slippage_pct)
                  -> no fill            -> CANCELLED
                  -> fill < min ratio   -> cancel rest, market-exit the filled part -> FAILED
                  -> PARTIALLY_FILLED / FILLED
                  -> exit LIMIT GTC at exit_target (reduceOnly on swap), wait sell_timeout_s
                  -> unfilled: exit_on_timeout == "market" -> cancel + MARKET exit
                               "hold"  -> wait until max_hold_s from entry, then MARKET
                  -> finalize (pnl), CLOSED, risk.register_close, stats.record_trade

Hard rules implemented here: never execute a stale snapshot (``revalidate``
runs on the latest book right before the entry), one place sends orders
(``_place``), every order carries an idempotent ``client_id``, a timeout is
status "unknown" (one ``fetch_order`` reconcile, never a retry), risk is
checked before every order and nothing is sent while ``risk.emergency``.

MANUAL flow: ``select_manual`` -> ``manual_support`` (live payload polled by
the UI) -> ``manual_entered`` (user bought by hand) -> ``manual_exited``
(user sold by hand) -> realized P&L.  Manual trades are ``TradeRecord``s
without orders; prices are user-reported, fees estimated from the adapter.
"""
from __future__ import annotations

import asyncio
import dataclasses
import inspect
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .. import events
from ..clock import Clock
from ..config import Settings
from ..exchanges.base import ExchangeAdapter, ExchangeError
from ..links import split_symbol
from ..models import (
    ACTIVE_STATES,
    ExecutionEconomics,
    Light,
    Mode,
    Opportunity,
    OppState,
    Order,
    OrderRequest,
    OrderStatus,
    OrderType,
    Side,
    TimeInForce,
    TradeRecord,
    new_id,
)
from .pnl import finalize
from .risk import RiskManager

if TYPE_CHECKING:  # pragma: no cover
    from ..engine.opportunity import OpportunityManager
    from ..engine.pipeline import Pipeline
    from ..engine.stats import StatsStore
    from ..storage.db import Database

log = logging.getLogger(__name__)

_EPS = 1e-9
EXPIRED_MESSAGE = "⚠️ OPPORTUNITY EXPIRED"
TOO_FAST_MESSAGE = "🔴 TOO FAST FOR HUMAN"


@dataclass(slots=True)
class _OpenPosition:
    adapter: ExchangeAdapter
    trade: TradeRecord
    base_open: float


@dataclass(slots=True)
class _Plan:
    """Numbers from the last revalidation of an opportunity (for the UI snapshot)."""

    capital: float | None = None
    econ: ExecutionEconomics | None = None
    expected_net_pct: float | None = None
    valid: bool = False
    reason: str = ""
    risk: dict[str, Any] = field(default_factory=lambda: {"ok": None, "reason": "", "allowed": None})
    balance: dict[str, Any] = field(default_factory=lambda: {"quote": "", "free": None})
    remaining_s: float | None = None


def _accepts_kwarg(func: Any, name: str) -> bool:
    """True when ``func`` has a parameter ``name`` or ``**kwargs``."""
    try:
        sig = inspect.signature(func)
    except (TypeError, ValueError):
        return False
    for p in sig.parameters.values():
        if p.kind is inspect.Parameter.VAR_KEYWORD or p.name == name:
            return True
    return False


class ExecutionEngine:
    """Drives MANUAL support and AUTO execution for selected opportunities."""

    poll_interval_s: float = 0.2

    def __init__(
        self,
        settings: Settings,
        bus: events.EventBus,
        clock: Clock,
        pipeline: "Pipeline",
        opps: "OpportunityManager",
        exec_adapters: dict[Mode, dict[str, ExchangeAdapter]],
        risk: RiskManager,
        db: "Database | None",
        stats: "StatsStore",
    ) -> None:
        self._settings = settings
        self._bus = bus
        self._clock = clock
        self._pipeline = pipeline
        self._opps = opps
        self._exec_adapters = exec_adapters
        self._risk = risk
        self._db = db
        self._stats = stats
        self._trades: dict[str, TradeRecord] = {}
        self._active: dict[str, TradeRecord] = {}  # opp_id -> running AUTO trade (duplicate guard)
        self._tasks: dict[str, asyncio.Task[TradeRecord]] = {}
        self._pending_exit: dict[str, tuple[ExchangeAdapter, Order]] = {}
        self._abort: dict[str, str] = {}
        self._expected: dict[str, ExecutionEconomics] = {}  # what the user saw when selecting
        self._plan: dict[str, _Plan] = {}
        self._open_positions: dict[str, _OpenPosition] = {}
        self._manual: dict[str, TradeRecord] = {}
        self._manual_entry_mono: dict[str, float] = {}

    # ------------------------------------------------------------------ #
    # small helpers
    # ------------------------------------------------------------------ #
    def adapter_for(self, opp: Opportunity) -> ExchangeAdapter | None:
        """Execution adapter of the current mode for the opportunity's target."""
        return self._exec_adapters.get(self._settings.mode(), {}).get(opp.target_exchange)

    def _adapters_of_mode(self, mode: Mode | None = None) -> dict[str, ExchangeAdapter]:
        return dict(self._exec_adapters.get(mode or self._settings.mode(), {}))

    def _is_swap(self, exchange: str) -> bool:
        cfg = self._settings.exchange(exchange)
        return bool(cfg is not None and cfg.market_type == "swap")

    def _taker_fee(self, exchange: str) -> float:
        cfg = self._settings.exchange(exchange)
        return float(cfg.taker_fee) if cfg is not None else 0.001

    def _persist_trade(self, trade: TradeRecord) -> None:
        if self._db is None:
            return
        try:
            self._db.save_trade(trade)
        except Exception:  # noqa: BLE001
            log.exception("db.save_trade failed for %s", trade.id)

    def _persist_order(self, order: Order | None, trade_id: str) -> None:
        if self._db is None or order is None:
            return
        try:
            self._db.save_order(order, trade_id)
        except Exception:  # noqa: BLE001
            log.exception("db.save_order failed for %s", order.id)

    async def _note(self, trade: TradeRecord, msg: str) -> None:
        """Append an event, log, persist and publish TRADE_UPDATE."""
        trade.events.append((self._clock.wall(), msg))
        log.info("trade %s [%s] %s: %s", trade.id, trade.state.value, trade.symbol, msg)
        self._persist_trade(trade)
        try:
            await self._bus.publish(events.TRADE_UPDATE, trade)
        except Exception:  # noqa: BLE001
            log.exception("TRADE_UPDATE publish failed")

    async def _transition(self, opp: Opportunity, state: OppState, reason: str = "") -> bool:
        """Move the opportunity through ``opps.transition``; never raises."""
        if opp.state == state:
            return True
        try:
            managed = await self._opps.transition(opp.id, state, reason)
            if managed is not None and managed is not opp:
                opp.state = managed.state
            else:
                opp.state = state
            return True
        except KeyError:
            opp.state = state  # not managed (backtest / synthetic) - keep local state
            return True
        except Exception as exc:  # noqa: BLE001
            log.warning("opportunity %s: transition %s -> %s refused: %s", opp.id, opp.state.value, state.value, exc)
            return False

    def _snapshot_expected(self, opp: Opportunity) -> ExecutionEconomics:
        econ = self._expected.get(opp.id)
        if econ is None:
            econ = dataclasses.replace(opp.econ)
            self._expected[opp.id] = econ
        return econ

    def _forget(self, opp_id: str) -> None:
        self._expected.pop(opp_id, None)
        self._plan.pop(opp_id, None)
        self._abort.pop(opp_id, None)
        self._pending_exit.pop(opp_id, None)

    @staticmethod
    def _merge_timing(old: Order, new: Order) -> Order:
        """Carry the engine-side timestamps over when the adapter returned a fresh object."""
        if new is old:
            return new
        for attr in ("sent_ts", "sent_mono", "ack_mono", "first_fill_mono", "done_mono"):
            if getattr(new, attr) is None and getattr(old, attr) is not None:
                setattr(new, attr, getattr(old, attr))
        if not new.client_id and old.client_id:
            new.client_id = old.client_id
        return new

    # ------------------------------------------------------------------ #
    # orders - the single place that sends them
    # ------------------------------------------------------------------ #
    async def _place(
        self,
        adapter: ExchangeAdapter,
        req: OrderRequest,
        *,
        trade_id: str,
        reduce_only: bool = False,
        closing: bool = False,
    ) -> Order:
        """Send ``req``.  Returns an ``Order`` even on failure (status FAILED,
        or SENT = unknown after a timeout that could not be reconciled)."""
        failed = Order(
            id="",
            client_id=req.client_id,
            exchange=req.exchange,
            symbol=req.symbol,
            side=req.side,
            type=req.type,
            amount=req.amount,
            price=req.price,
            status=OrderStatus.FAILED,
        )
        if self._risk.emergency and not closing:
            failed.error = "Emergency stop is active"
            log.warning("order refused (emergency): %s %s %s", req.side.value, req.symbol, req.client_id)
            return failed
        kwargs: dict[str, Any] = {}
        if reduce_only and _accepts_kwarg(adapter.place_order, "reduce_only"):
            kwargs["reduce_only"] = True
        sent_mono, sent_ts = self._clock.mono(), self._clock.wall()
        failed.sent_mono, failed.sent_ts = sent_mono, sent_ts
        log.info(
            "order send %s %s %s %s %.8g @ %s tif=%s client_id=%s%s",
            adapter.name, req.type.value, req.side.value, req.symbol, req.amount, req.price, req.tif.value,
            req.client_id, " reduceOnly" if kwargs else "",
        )
        try:
            order = await adapter.place_order(req, **kwargs)
        except asyncio.TimeoutError:
            log.error("order %s timed out -> status unknown, reconciling once", req.client_id)
            order = await self._reconcile_unknown(adapter, req, failed)
        except ExchangeError as exc:
            failed.error = str(exc)
            log.error("order %s rejected by %s: %s", req.client_id, adapter.name, exc)
            order = failed
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            failed.error = f"{type(exc).__name__}: {exc}"
            log.exception("order %s failed on %s", req.client_id, adapter.name)
            order = failed
        if order.sent_mono is None:
            order.sent_mono = sent_mono
        if order.sent_ts is None:
            order.sent_ts = sent_ts
        if order.ack_mono is None and order.status not in (OrderStatus.NEW, OrderStatus.FAILED):
            order.ack_mono = self._clock.mono()
        if order.filled > 0 and order.first_fill_mono is None:
            order.first_fill_mono = self._clock.mono()
        if order.is_done and order.done_mono is None:
            order.done_mono = self._clock.mono()
        self._persist_order(order, trade_id)
        try:
            await self._bus.publish(events.ORDER_UPDATE, order)
        except Exception:  # noqa: BLE001
            log.exception("ORDER_UPDATE publish failed")
        return order

    async def _reconcile_unknown(self, adapter: ExchangeAdapter, req: OrderRequest, unknown: Order) -> Order:
        """One fetch_order by client id after a timeout; never a retry."""
        unknown.status = OrderStatus.SENT
        unknown.error = "timeout: status unknown"
        try:
            if _accepts_kwarg(adapter.fetch_order, "client_id"):
                found = await adapter.fetch_order(req.client_id, req.symbol, client_id=req.client_id)
            else:
                found = await adapter.fetch_order(req.client_id, req.symbol)
        except Exception as exc:  # noqa: BLE001
            log.error("reconcile of %s failed: %s", req.client_id, exc)
            return unknown
        if found is None:
            return unknown
        return self._merge_timing(unknown, found)

    async def _refresh(self, adapter: ExchangeAdapter, order: Order) -> Order:
        if not order.id:
            return order
        try:
            fresh = await adapter.fetch_order(order.id, order.symbol)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("fetch_order %s failed: %s", order.id, exc)
            return order
        if fresh is None:
            return order
        return self._merge_timing(order, fresh)

    async def _wait_fill(
        self, adapter: ExchangeAdapter, order: Order, timeout_s: float, opp_id: str, *, closing: bool = False
    ) -> Order:
        """Poll ``fetch_order`` until the order is done, ``timeout_s`` elapsed,
        the trade was aborted or the emergency stop was raised.  A *closing*
        order (market exit) is waited for regardless of abort / emergency."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, float(timeout_s))
        while True:
            if order.filled > 0 and order.first_fill_mono is None:
                order.first_fill_mono = self._clock.mono()
            if order.is_done or (order.amount > 0 and order.remaining <= _EPS):
                if order.done_mono is None:
                    order.done_mono = self._clock.mono()
                if order.status not in (OrderStatus.FAILED, OrderStatus.REJECTED) and order.remaining <= _EPS and order.filled > 0:
                    order.status = OrderStatus.FILLED
                return order
            interrupted = not closing and (self._risk.emergency or opp_id in self._abort)
            if interrupted or loop.time() >= deadline:
                return order
            await asyncio.sleep(max(0.0, self.poll_interval_s))
            order = await self._refresh(adapter, order)

    async def _cancel(self, adapter: ExchangeAdapter, order: Order) -> Order:
        """Cancel an open order (best effort) and return its final state."""
        if order.is_done or not order.id:
            return order
        try:
            res = await adapter.cancel_order(order.id, order.symbol)
            if res is not None:
                order = self._merge_timing(order, res)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("cancel_order %s failed: %s", order.id, exc)
        order = await self._refresh(adapter, order)
        if not order.is_done:
            order.status = OrderStatus.CANCELLED
        if order.done_mono is None:
            order.done_mono = self._clock.mono()
        return order

    async def _exit_market(self, adapter: ExchangeAdapter, trade: TradeRecord, amount: float) -> Order:
        """MARKET exit of ``amount`` base (reduceOnly on swap).  Allowed during emergency."""
        req = OrderRequest(
            exchange=trade.exchange,
            symbol=trade.symbol,
            side=Side.SELL if trade.direction > 0 else Side.BUY,
            amount=amount,
            type=OrderType.MARKET,
            price=None,
            tif=TimeInForce.IOC,
            opportunity_id=trade.opportunity_id or None,
        )
        order = await self._place(adapter, req, trade_id=trade.id, reduce_only=self._is_swap(trade.exchange), closing=True)
        if order.status in (OrderStatus.FAILED, OrderStatus.REJECTED):
            return order
        return await self._wait_fill(adapter, order, self._settings.buy_fill_timeout_s, trade.opportunity_id, closing=True)

    @staticmethod
    def _combine(orders: list[Order | None]) -> Order | None:
        """Merge a partially filled limit exit and its market remainder into one Order."""
        real = [o for o in orders if o is not None]
        if not real:
            return None
        if len(real) == 1:
            return real[0]
        first = real[0]
        fills = [f for o in real for f in o.fills]
        filled = sum(o.filled for o in real)
        notional = sum((o.avg_price or 0.0) * o.filled for o in real if o.avg_price)
        market_filled = sum(o.filled for o in real if o.type == OrderType.MARKET)
        amount = max(first.amount, filled)
        if filled >= amount - _EPS and filled > 0:
            status = OrderStatus.FILLED
        elif filled > 0:
            status = OrderStatus.PARTIALLY_FILLED if any(not o.is_done for o in real) else OrderStatus.CANCELLED
        else:
            status = real[-1].status
        monos = [o.sent_mono for o in real if o.sent_mono is not None]
        acks = [o.ack_mono for o in real if o.ack_mono is not None]
        firsts = [o.first_fill_mono for o in real if o.first_fill_mono is not None]
        dones = [o.done_mono for o in real if o.done_mono is not None]
        return Order(
            id="+".join(o.id or o.client_id for o in real),
            client_id=first.client_id,
            exchange=first.exchange,
            symbol=first.symbol,
            side=first.side,
            type=OrderType.MARKET if market_filled >= filled - _EPS and market_filled > 0 else first.type,
            amount=amount,
            price=first.price,
            status=status,
            filled=filled,
            avg_price=(notional / filled) if filled > 0 and notional > 0 else None,
            fills=fills,
            fee_cost_quote=sum(o.fee_cost_quote for o in real),
            sent_ts=first.sent_ts,
            sent_mono=min(monos) if monos else None,
            ack_mono=min(acks) if acks else None,
            first_fill_mono=min(firsts) if firsts else None,
            done_mono=max(dones) if dones else None,
            error="; ".join(o.error for o in real if o.error) or None,
            raw={"orders": [o.id for o in real], "market_filled": market_filled},
        )

    # ------------------------------------------------------------------ #
    # MANUAL
    # ------------------------------------------------------------------ #
    def _expired_payload(self, opp: Opportunity | None, opp_id: str, message: str = EXPIRED_MESSAGE) -> dict[str, Any]:
        d: dict[str, Any] = {
            "opp_id": opp_id,
            "valid": False,
            "status": "expired",
            "side": None,
            "exchange": None,
            "symbol": None,
            "entry_price_now": None,
            "exit_target_now": None,
            "net_now_pct": None,
            "expected_slippage_pct": None,
            "remaining_s": 0.0,
            "human": {"light": Light.RED.value, "label": "NOT EXECUTABLE", "est_execution_s": self._settings.human_execution_time_s},
            "capital": None,
            "base_amount": None,
            "expired": True,
            "message": message,
            "state": None,
            "quote": "",
            "pair_url": "",
            "fresh": False,
        }
        if opp is not None:
            d.update(
                side="LONG" if opp.direction > 0 else "SHORT",
                exchange=opp.target_exchange,
                symbol=opp.target_symbol,
                state=opp.state.value,
                quote=opp.quote,
                pair_url=opp.pair_url,
                entry_price_now=opp.econ.entry_vwap,
                exit_target_now=opp.econ.exit_target,
                net_now_pct=opp.econ.net_pct,
                capital=opp.econ.executable_quote,
                base_amount=opp.econ.base_amount,
            )
        return d

    async def select_manual(self, opp_id: str) -> dict[str, Any]:
        """Mark the opportunity MANUAL_SELECTED and return the live support payload."""
        opp = self._opps.get(opp_id)
        if opp is None:
            return self._expired_payload(None, opp_id)
        if opp_id in self._active:
            return self._expired_payload(opp, opp_id, "⚠️ AUTO EXECUTION IN PROGRESS")
        if opp.state == OppState.AUTO_SELECTED:
            await self._transition(opp, OppState.VALID, "switched to manual")
        if opp.state in (OppState.VALID, OppState.NOTIFIED):
            await self._transition(opp, OppState.MANUAL_SELECTED, "user selected MANUAL")
        if opp.state == OppState.MANUAL_SELECTED or opp.is_active:
            self._snapshot_expected(opp)
        return await self.manual_support(opp_id)

    async def manual_support(self, opp_id: str) -> dict[str, Any]:
        """Live numbers for the MANUAL modal, recomputed from the latest book."""
        s = self._settings
        opp = self._opps.get(opp_id)
        manual = self._manual.get(opp_id)
        if opp is None:
            return self._expired_payload(None, opp_id)
        if not opp.is_active and manual is None:
            return self._expired_payload(opp, opp_id)
        capital = manual.capital_quote if manual is not None else float(opp.econ.capital_quote or s.capital_quote)
        try:
            fresh = await self._pipeline.refresh(opp, capital)
        except Exception as exc:  # noqa: BLE001
            log.warning("manual_support %s: refresh failed: %s", opp_id, exc)
            fresh = None
        if fresh is None:
            d = self._expired_payload(opp, opp_id, "⚠️ NO FRESH DATA")
            d["status"] = "no_data"
            d["expired"] = not opp.is_active and manual is None
            return d
        econ = fresh.econ
        now = self._clock.mono()
        remaining = fresh.remaining_s(now)
        human = fresh.human
        expected = self._expected.get(opp_id, opp.econ)
        if manual is not None:
            status, message = "in_position", "🟢 IN POSITION - report your exit price"
        elif remaining < s.human_execution_time_s or human.light == Light.RED:
            status, message = "too_fast", TOO_FAST_MESSAGE
        elif human.light == Light.GREEN:
            status, message = "ok", "🟢 EXECUTABLE"
        else:
            status, message = "ok", "🟡 EXECUTABLE - hurry"
        return {
            "opp_id": opp_id,
            "valid": True,
            "status": status,
            "side": "LONG" if opp.direction > 0 else "SHORT",
            "exchange": opp.target_exchange,
            "symbol": opp.target_symbol,
            "entry_price_now": econ.entry_vwap,
            "exit_target_now": econ.exit_target,
            "net_now_pct": econ.net_pct,
            "expected_net_pct": expected.net_pct,
            "expected_slippage_pct": econ.entry_slippage_pct + econ.exit_slippage_pct,
            "remaining_s": round(remaining, 2),
            "human": {"light": human.light.value, "label": human.label, "est_execution_s": human.est_execution_s},
            "human_execution_time_s": s.human_execution_time_s,
            "capital": econ.executable_quote,
            "base_amount": econ.base_amount,
            "expired": False,
            "message": message,
            "state": opp.state.value,
            "quote": opp.quote or split_symbol(opp.target_symbol)[1],
            "pair_url": opp.pair_url,
            "buy_price": econ.buy_price,
            "sell_price": econ.sell_price,
            "fresh": True,
            "trade_id": manual.id if manual is not None else None,
        }

    async def manual_entered(self, opp_id: str, price: float, quote_amount: float) -> dict[str, Any]:
        """The user did the entry by hand: record a TradeRecord and return the exit target."""
        s = self._settings
        opp = self._opps.get(opp_id)
        if opp is None:
            return {"ok": False, "reason": "unknown opportunity", "message": EXPIRED_MESSAGE}
        if opp_id in self._manual:
            return {"ok": False, "reason": "manual position already open for this opportunity", "trade_id": self._manual[opp_id].id}
        if opp_id in self._active:
            return {"ok": False, "reason": "AUTO execution in progress for this opportunity"}
        try:
            price = float(price)
            quote_amount = float(quote_amount)
        except (TypeError, ValueError):
            return {"ok": False, "reason": "price and quote_amount must be numbers"}
        if price <= 0 or quote_amount <= 0:
            return {"ok": False, "reason": "price and quote_amount must be positive"}
        if opp.state in (OppState.CLOSED, OppState.CANCELLED, OppState.FAILED):
            return {"ok": False, "reason": f"opportunity is {opp.state.value}", "message": EXPIRED_MESSAGE}
        if opp.state == OppState.AUTO_SELECTED:
            await self._transition(opp, OppState.VALID, "switched to manual")
        if opp.state in (OppState.VALID, OppState.NOTIFIED):
            await self._transition(opp, OppState.MANUAL_SELECTED, "user selected MANUAL")
        expected = self._snapshot_expected(opp)
        adapter = self.adapter_for(opp)
        taker = self._taker_fee(opp.target_exchange)
        if adapter is not None:
            try:
                taker = float((await adapter.fees(opp.target_symbol)).taker)
            except Exception as exc:  # noqa: BLE001
                log.debug("fees for %s unavailable (%s); using config", opp.target_symbol, exc)
        base_amount = quote_amount / price
        entry_fee = quote_amount * taker
        exit_fee_est = expected.exit_target * base_amount * taker
        trade = TradeRecord(
            id=new_id("tr_"),
            opportunity_id=opp.id,
            mode=s.mode(),
            exchange=opp.target_exchange,
            asset=opp.asset,
            symbol=opp.target_symbol,
            direction=opp.direction,
            capital_quote=quote_amount,
            expected_net_pct=expected.net_pct,
            expected_profit_quote=quote_amount * expected.net_pct / 100.0,
            expected_entry_price=expected.entry_vwap,
            expected_exit_price=expected.exit_target,
            state=OppState.EXECUTING,
            entry_order=None,
            exit_order=None,
            actual_entry_price=price,
            actual_fees_quote=entry_fee,
            started_ts=self._clock.wall(),
        )
        self._trades[trade.id] = trade
        self._manual[opp_id] = trade
        self._manual_entry_mono[opp_id] = self._clock.mono()
        self._risk.register_open(trade.id, trade.exchange, trade.asset, quote_amount, trade.mode)
        opp.execution_id = trade.id
        if opp.state == OppState.MANUAL_SELECTED:
            await self._transition(opp, OppState.EXECUTING, "manual entry reported")
        else:
            log.warning("manual entry recorded for %s in state %s (opportunity not transitioned)", opp_id, opp.state.value)
        await self._note(trade, f"manual entry at {price:.8g} for {quote_amount:.2f} ({base_amount:.8g} {opp.asset})")
        exit_target_now: float | None = None
        try:
            fresh = await self._pipeline.refresh(opp, quote_amount)
            if fresh is not None:
                exit_target_now = fresh.econ.exit_target
        except Exception as exc:  # noqa: BLE001
            log.debug("manual_entered %s: refresh failed: %s", opp_id, exc)
        return {
            "ok": True,
            "opp_id": opp_id,
            "trade_id": trade.id,
            "side": "LONG" if opp.direction > 0 else "SHORT",
            "exit_side": "SELL" if opp.direction > 0 else "BUY",
            "entry_price": price,
            "quote_amount": quote_amount,
            "base_amount": base_amount,
            "exit_target": expected.exit_target,
            "exit_target_now": exit_target_now,
            "expected_net_pct": expected.net_pct,
            "estimated_fees_quote": entry_fee + exit_fee_est,
            "taker_fee": taker,
            "symbol": opp.target_symbol,
            "exchange": opp.target_exchange,
            "state": trade.state.value,
        }

    async def manual_exited(self, opp_id: str, price: float) -> TradeRecord:
        """The user sold by hand: realized P&L, CLOSED."""
        trade = self._manual.get(opp_id)
        if trade is None:
            raise KeyError(f"no open manual trade for opportunity {opp_id}")
        price = float(price)
        if price <= 0:
            raise ValueError("exit price must be positive")
        entry_price = float(trade.actual_entry_price or 0.0)
        base_amount = trade.capital_quote / entry_price if entry_price > 0 else 0.0
        taker = self._taker_fee(trade.exchange)
        opp = self._opps.get(opp_id)
        adapter = self.adapter_for(opp) if opp is not None else None
        if adapter is not None:
            try:
                taker = float((await adapter.fees(trade.symbol)).taker)
            except Exception:  # noqa: BLE001
                pass
        fees = trade.capital_quote * taker + price * base_amount * taker
        trade.finished_ts = self._clock.wall()
        finalize(
            trade,
            fees_quote=fees,
            entry_price=entry_price,
            exit_price=price,
            base_amount=base_amount,
            entry_sent_mono=self._manual_entry_mono.pop(opp_id, None),
            exit_done_mono=self._clock.mono(),
        )
        trade.state = OppState.CLOSED
        self._manual.pop(opp_id, None)
        pnl = float(trade.realized_pnl_quote or 0.0)
        self._risk.register_close(trade.id, pnl)
        self._record_stats(trade, success=pnl > 0)
        if opp is not None:
            await self._transition(opp, OppState.CLOSED, f"manual exit at {price:.8g}: realized {pnl:+.2f}")
        self._forget(opp_id)
        await self._note(trade, f"manual exit at {price:.8g}: realized {pnl:+.2f} ({trade.diff_reason})")
        return trade

    def _record_stats(self, trade: TradeRecord, success: bool) -> None:
        try:
            self._stats.record_trade(
                trade.asset, trade.exchange, success, trade.actual_net_pct, trade.actual_slippage_pct, trade.entry_latency_ms
            )
        except Exception:  # noqa: BLE001
            log.exception("stats.record_trade failed for %s", trade.id)

    # ------------------------------------------------------------------ #
    # AUTO - selection / confirmation / cancel
    # ------------------------------------------------------------------ #
    def _emergency_reason(self) -> str:
        why = self._risk.emergency_reason
        return f"Emergency stop is active: {why}" if why else "Emergency stop is active"

    async def _prepare_auto(self, opp: Opportunity) -> str | None:
        """Bring ``opp`` to AUTO_SELECTED; returns a failure reason or None."""
        if self._risk.emergency:
            return self._emergency_reason()
        if opp.id in self._active:
            return "AUTO execution already in progress for this opportunity"
        if opp.id in self._manual:
            return "manual position is open for this opportunity"
        if self.adapter_for(opp) is None:
            return f"No {self._settings.mode().value} execution adapter for {opp.target_exchange}"
        if opp.state == OppState.MANUAL_SELECTED:
            await self._transition(opp, OppState.VALID, "switched to auto")
        if opp.state in (OppState.VALID, OppState.NOTIFIED):
            await self._transition(opp, OppState.AUTO_SELECTED, "user selected AUTO")
        if opp.state != OppState.AUTO_SELECTED:
            if opp.is_active:
                return f"opportunity is still {opp.state.value}; try again"
            return EXPIRED_MESSAGE
        self._snapshot_expected(opp)
        return None

    async def select_auto(self, opp_id: str) -> dict[str, Any]:
        """AUTO_SELECTED; execute now when ``settings.auto_confirm`` else return a confirmation snapshot."""
        opp = self._opps.get(opp_id)
        if opp is None:
            return {"ok": False, "reason": "unknown opportunity", "opp_id": opp_id}
        reason = await self._prepare_auto(opp)
        if reason is not None:
            return {"ok": False, "reason": reason, "opp_id": opp_id, "state": opp.state.value}
        if self._settings.auto_confirm:
            return await self.confirm_auto(opp_id)
        ok, why, _econ = await self.revalidate(opp)
        return {
            "ok": True,
            "confirmation_required": True,
            "opp_id": opp_id,
            "state": opp.state.value,
            "snapshot": self._snapshot(opp, ok, why),
        }

    def _snapshot(self, opp: Opportunity, valid: bool, reason: str) -> dict[str, Any]:
        plan = self._plan.get(opp.id) or _Plan()
        expected = self._expected.get(opp.id, opp.econ)
        econ = plan.econ or opp.econ
        capital = plan.capital if plan.capital is not None else econ.executable_quote
        now = self._clock.mono()
        return {
            "valid": bool(valid),
            "reason": reason,
            "net_now_pct": econ.net_pct,
            "expected_net_pct": expected.net_pct,
            "expected_profit_quote": capital * econ.net_pct / 100.0,
            "capital": capital,
            "risk": dict(plan.risk),
            "balance": dict(plan.balance),
            "side": "LONG" if opp.direction > 0 else "SHORT",
            "exchange": opp.target_exchange,
            "symbol": opp.target_symbol,
            "entry_price": econ.entry_vwap,
            "exit_target": econ.exit_target,
            "remaining_s": round(opp.remaining_s(now), 2),
            "mode": self._settings.mode().value,
            "quote": opp.quote or split_symbol(opp.target_symbol)[1],
        }

    async def confirm_auto(self, opp_id: str, wait: bool = True) -> dict[str, Any]:
        """Revalidate + execute.  ``wait=False`` starts the trade in the background."""
        opp = self._opps.get(opp_id)
        if opp is None:
            return {"ok": False, "reason": "unknown opportunity", "opp_id": opp_id}
        reason = await self._prepare_auto(opp)
        if reason is not None:
            trade = self._active.get(opp_id)
            return {"ok": False, "reason": reason, "opp_id": opp_id, "state": opp.state.value, "trade_id": trade.id if trade else None}
        task = asyncio.create_task(self.execute(opp), name=f"execute-{opp_id}")
        self._tasks[opp_id] = task
        task.add_done_callback(lambda t, oid=opp_id: self._tasks.pop(oid, None) if self._tasks.get(oid) is t else None)
        if not wait:
            return {"ok": True, "started": True, "opp_id": opp_id, "state": OppState.EXECUTING.value}
        try:
            trade = await task
        except asyncio.CancelledError:
            if task.cancelled():
                return {"ok": False, "reason": "execution task was cancelled", "opp_id": opp_id}
            raise
        except Exception as exc:  # noqa: BLE001
            log.exception("execute(%s) crashed", opp_id)
            return {"ok": False, "reason": f"execution error: {exc}", "opp_id": opp_id}
        return {
            "ok": trade.state == OppState.CLOSED,
            "opp_id": opp_id,
            "state": trade.state.value,
            "reason": trade.error or trade.diff_reason,
            "trade_id": trade.id,
            "trade": trade.to_dict(),
        }

    async def cancel(self, opp_id: str, reason: str = "user") -> dict[str, Any]:
        """Abort a running AUTO trade or cancel a selected opportunity."""
        task = self._tasks.get(opp_id)
        if task is not None and not task.done():
            self._abort[opp_id] = reason
            log.warning("cancel requested for running trade on %s: %s", opp_id, reason)
            return {"ok": True, "aborting": True, "opp_id": opp_id, "reason": reason}
        if opp_id in self._manual:
            return {"ok": False, "opp_id": opp_id, "reason": "manual position is open; report the exit price instead"}
        opp = self._opps.get(opp_id)
        if opp is None:
            return {"ok": False, "opp_id": opp_id, "reason": "unknown opportunity"}
        if opp.state in ACTIVE_STATES:
            await self._transition(opp, OppState.CANCELLED, reason)
            self._forget(opp_id)
            return {"ok": True, "opp_id": opp_id, "state": opp.state.value, "reason": reason}
        return {"ok": False, "opp_id": opp_id, "state": opp.state.value, "reason": f"cannot cancel in state {opp.state.value}"}

    # ------------------------------------------------------------------ #
    # revalidation (hard rule 4: never execute a stale snapshot)
    # ------------------------------------------------------------------ #
    async def revalidate(self, opp: Opportunity) -> tuple[bool, str, ExecutionEconomics | None]:
        s = self._settings
        plan = _Plan()
        self._plan[opp.id] = plan

        def fail(reason: str) -> tuple[bool, str, None]:
            plan.valid, plan.reason = False, reason
            log.info("revalidate %s: FAIL - %s", opp.id, reason)
            return False, reason, None

        if self._risk.emergency:
            return fail(self._emergency_reason())
        adapter = self.adapter_for(opp)
        if adapter is None:
            return fail(f"No {s.mode().value} execution adapter for {opp.target_exchange}")
        now = self._clock.mono()
        health = getattr(adapter, "health", None)
        if health is not None and hasattr(health, "is_usable") and not health.is_usable(now):
            why = getattr(health, "reason", "") or "stale / disconnected / rate limited"
            return fail(f"Exchange {opp.target_exchange} is not usable ({why})")

        symbol = opp.target_symbol
        book = adapter.order_book(symbol)
        age = book.age_ms(now) if book is not None else None
        if book is None or age is None or age > s.max_data_age_ms:
            try:
                rest = await adapter.fetch_order_book(symbol)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("revalidate %s: REST book failed: %s", opp.id, exc)
                rest = None
            if rest is not None:
                book = rest
                try:
                    await self._bus.publish(events.BOOK, rest)
                except Exception:  # noqa: BLE001
                    log.exception("BOOK publish failed")
                age = book.age_ms(self._clock.mono())
        if book is None:
            return fail(f"No order book for {symbol} on {opp.target_exchange}")
        if age is None or age > s.max_data_age_ms:
            return fail(f"Order book is stale ({age or 0:.0f} ms > {s.max_data_age_ms:.0f} ms)")

        expected = self._expected.get(opp.id, opp.econ)
        expected_net = float(expected.net_pct)
        plan.expected_net_pct = expected_net
        capital = float(expected.capital_quote or s.capital_quote)
        try:
            fresh = await self._pipeline.refresh(opp, capital)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            return fail(f"Revalidation failed: {exc}")
        if fresh is None:
            return fail("Revalidation failed: no fresh economics")
        econ = fresh.econ
        plan.econ = econ
        plan.remaining_s = fresh.remaining_s(self._clock.mono())
        if not econ.liquidity_ok:
            return fail(f"Insufficient liquidity: {econ.liquidity_reason or 'book too thin'}")
        if econ.book_age_ms > s.max_data_age_ms:
            return fail(f"Order book is stale ({econ.book_age_ms:.0f} ms > {s.max_data_age_ms:.0f} ms)")
        required = max(float(s.min_net_pct_auto), expected_net * float(s.revalidate_tolerance))
        if econ.net_pct < required:
            return fail(f"Current net {econ.net_pct:+.2f}% is below the required {required:.2f}% (expected {expected_net:+.2f}%)")
        if fresh.bot.light == Light.RED:
            why = "; ".join(fresh.bot.reasons) or fresh.bot.label
            return fail(f"Bot light is RED: {why}")
        executable = float(econ.executable_quote)
        if executable < s.min_trade_quote:
            return fail(f"Executable size ${executable:.2f} is below the minimum trade size ${s.min_trade_quote:.2f}")

        quote = split_symbol(symbol)[1] or (opp.quote or "").upper()
        plan.balance = {"quote": quote, "free": None}
        try:
            balances = await adapter.fetch_balances()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            return fail(f"Balance check failed: {exc}")
        bal = balances.get(quote) if balances else None
        free = float(bal.free) if bal is not None else 0.0
        plan.balance["free"] = free
        capital_final = executable
        notes: list[str] = []
        if free + _EPS < capital_final:
            if s.allow_downsize and free >= s.min_trade_quote:
                notes.append(f"downsized to the free {quote} balance {free:.2f}")
                capital_final = free
            else:
                return fail(f"Insufficient {quote} balance: free {free:.2f} < needed {capital_final:.2f}")

        ok, why, allowed = self._risk.check(opp.target_exchange, opp.asset, capital_final, s.mode())
        plan.risk = {"ok": ok, "reason": why, "allowed": allowed}
        if not ok:
            return fail(why)
        if allowed + _EPS < capital_final:
            notes.append(why)
            capital_final = allowed
        if capital_final + _EPS < executable:
            ratio = capital_final / executable
            econ = dataclasses.replace(
                econ,
                capital_quote=capital_final,
                executable_quote=capital_final,
                base_amount=econ.base_amount * ratio,
                net_quote=capital_final * econ.net_pct / 100.0,
            )
        plan.capital = capital_final
        plan.econ = econ
        plan.valid, plan.reason = True, ("; ".join(notes) or "OK")
        log.info("revalidate %s: OK net %+.2f%% on %.2f %s (%s)", opp.id, econ.net_pct, capital_final, quote, plan.reason)
        return True, plan.reason, econ

    # ------------------------------------------------------------------ #
    # execution
    # ------------------------------------------------------------------ #
    async def execute(self, opp: Opportunity) -> TradeRecord:
        """Full AUTO round trip for ``opp`` (see module docstring)."""
        existing = self._active.get(opp.id)
        if existing is not None:
            log.warning("execute(%s): already running as trade %s", opp.id, existing.id)
            return existing
        s = self._settings
        expected = self._snapshot_expected(opp)
        trade = TradeRecord(
            id=new_id("tr_"),
            opportunity_id=opp.id,
            mode=s.mode(),
            exchange=opp.target_exchange,
            asset=opp.asset,
            symbol=opp.target_symbol,
            direction=opp.direction,
            capital_quote=float(expected.executable_quote),
            expected_net_pct=float(expected.net_pct),
            expected_profit_quote=float(expected.net_quote),
            expected_entry_price=float(expected.entry_vwap),
            expected_exit_price=float(expected.exit_target),
            state=OppState.EXECUTING,
            started_ts=self._clock.wall(),
        )
        self._trades[trade.id] = trade
        self._active[opp.id] = trade
        opp.execution_id = trade.id
        adapter = self.adapter_for(opp)
        registered = False
        position_open = 0.0
        try:
            registered, position_open = await self._run(opp, adapter, trade, expected)
        except asyncio.CancelledError:
            trade.state = OppState.FAILED
            trade.error = "execution task cancelled"
            trade.finished_ts = self._clock.wall()
            log.error("trade %s: task cancelled (position may be open)", trade.id)
            self._persist_trade(trade)
            raise
        except Exception as exc:  # noqa: BLE001
            log.exception("trade %s: unexpected error", trade.id)
            await self._fail(opp, adapter, trade, f"error: {exc}", registered=registered, position_open=position_open)
        finally:
            self._active.pop(opp.id, None)
            self._forget(opp.id)
        return trade

    async def _run(
        self, opp: Opportunity, adapter: ExchangeAdapter | None, trade: TradeRecord, expected: ExecutionEconomics
    ) -> tuple[bool, float]:
        """The flow.  Returns (exposure registered, base still open) for the error path."""
        s = self._settings
        ok, reason, econ = await self.revalidate(opp)
        if not ok or econ is None or adapter is None:
            await self._cancelled(opp, trade, reason or "revalidation failed", registered=False)
            return False, 0.0
        trade.capital_quote = float(econ.executable_quote)
        trade.expected_profit_quote = trade.capital_quote * trade.expected_net_pct / 100.0
        await self._transition(opp, OppState.EXECUTING, "revalidated")
        self._risk.register_open(trade.id, trade.exchange, trade.asset, trade.capital_quote, trade.mode)
        registered = True
        await self._note(trade, f"revalidated: net {econ.net_pct:+.2f}% on {trade.capital_quote:.2f} ({reason})")

        if self._risk.emergency:
            await self._cancelled(opp, trade, self._emergency_reason(), registered=registered)
            return registered, 0.0
        if opp.id in self._abort:
            await self._cancelled(opp, trade, f"cancelled before entry: {self._abort[opp.id]}", registered=registered)
            return registered, 0.0

        # ---- entry: LIMIT IOC ------------------------------------------
        d = 1 if trade.direction > 0 else -1
        entry_price = float(econ.entry_vwap) * (1.0 + d * float(s.buy_limit_slippage_pct) / 100.0)
        entry_req = OrderRequest(
            exchange=trade.exchange,
            symbol=trade.symbol,
            side=Side.BUY if d > 0 else Side.SELL,
            amount=float(econ.base_amount),
            type=OrderType.LIMIT,
            price=entry_price,
            tif=TimeInForce.IOC,
            opportunity_id=opp.id,
        )
        entry = await self._place(adapter, entry_req, trade_id=trade.id)
        trade.entry_order = entry
        if entry.status in (OrderStatus.FAILED, OrderStatus.REJECTED):
            await self._fail(opp, adapter, trade, f"entry order failed: {entry.error or entry.status.value}", registered=registered, position_open=0.0)
            return registered, 0.0
        await self._note(trade, f"entry {entry_req.side.value} {entry_req.amount:.8g} @ {entry_price:.8g} IOC sent ({entry.status.value})")
        entry = await self._wait_fill(adapter, entry, s.buy_fill_timeout_s, opp.id)
        if not entry.is_done:
            entry = await self._cancel(adapter, entry)
        trade.entry_order = entry
        self._persist_order(entry, trade.id)
        filled = float(entry.filled)
        if filled <= _EPS:
            await self._cancelled(opp, trade, "entry not filled (CANCELLED)", registered=registered)
            return registered, 0.0
        ratio = filled / entry_req.amount if entry_req.amount > 0 else 0.0
        if ratio + _EPS < float(s.min_partial_fill_ratio):
            await self._note(trade, f"entry filled only {ratio * 100:.0f}% - exiting the filled part at market")
            exit_order = await self._exit_market(adapter, trade, filled)
            trade.exit_order = exit_order
            self._persist_order(exit_order, trade.id)
            still_open = max(0.0, filled - float(exit_order.filled))
            await self._fail(
                opp, adapter, trade,
                f"partial fill {ratio * 100:.0f}% below minimum {float(s.min_partial_fill_ratio) * 100:.0f}%: exited at market",
                registered=registered, position_open=still_open,
            )
            return registered, still_open
        await self._transition(opp, OppState.FILLED if ratio >= 1.0 - 1e-6 else OppState.PARTIALLY_FILLED, f"entry filled {ratio * 100:.0f}%")
        await self._note(trade, f"entry filled {filled:.8g} @ {entry.avg_price or entry_price:.8g} ({ratio * 100:.0f}%)")
        position_open = filled

        if self._risk.emergency:
            await self._fail(opp, adapter, trade, "emergency stop before exit: position left open", registered=registered, position_open=position_open)
            return registered, position_open
        abort = self._abort.get(opp.id)
        if abort is not None:
            await self._note(trade, f"cancel requested ({abort}): exiting at market")
            exit_order = await self._exit_market(adapter, trade, position_open)
            return await self._close(opp, adapter, trade, [exit_order], registered, position_open)

        # ---- exit: LIMIT GTC at exit_target ------------------------------
        exit_req = OrderRequest(
            exchange=trade.exchange,
            symbol=trade.symbol,
            side=Side.SELL if d > 0 else Side.BUY,
            amount=position_open,
            type=OrderType.LIMIT,
            price=float(econ.exit_target),
            tif=TimeInForce.GTC,
            opportunity_id=opp.id,
        )
        exit_limit = await self._place(adapter, exit_req, trade_id=trade.id, reduce_only=self._is_swap(trade.exchange))
        orders: list[Order | None] = []
        if exit_limit.status in (OrderStatus.FAILED, OrderStatus.REJECTED):
            await self._note(trade, f"exit limit rejected ({exit_limit.error or exit_limit.status.value}): exiting at market")
            market = await self._exit_market(adapter, trade, position_open)
            return await self._close(opp, adapter, trade, [market], registered, position_open)
        self._pending_exit[opp.id] = (adapter, exit_limit)
        await self._note(trade, f"exit {exit_req.side.value} {exit_req.amount:.8g} @ {exit_req.price:.8g} GTC sent ({exit_limit.status.value})")
        exit_limit = await self._wait_fill(adapter, exit_limit, s.sell_timeout_s, opp.id)
        if not exit_limit.is_done and not self._risk.emergency and opp.id not in self._abort and s.exit_on_timeout == "hold":
            entry_mono = entry.sent_mono if entry.sent_mono is not None else self._clock.mono()
            hold_left = float(s.max_hold_s) - (self._clock.mono() - entry_mono)
            if hold_left > 0:
                await self._note(trade, f"exit not filled after {s.sell_timeout_s:.0f} s - holding up to {hold_left:.0f} s more")
                exit_limit = await self._wait_fill(adapter, exit_limit, hold_left, opp.id)
        self._pending_exit.pop(opp.id, None)
        orders.append(exit_limit)
        if not exit_limit.is_done:
            exit_limit = await self._cancel(adapter, exit_limit)
            orders[-1] = exit_limit
        self._persist_order(exit_limit, trade.id)
        remaining = max(0.0, position_open - float(exit_limit.filled))
        if remaining > _EPS:
            if self._risk.emergency:
                combined = self._combine(orders)
                trade.exit_order = combined
                await self._fail(opp, adapter, trade, "emergency stop during exit: position left open", registered=registered, position_open=remaining)
                return registered, remaining
            why = "cancel requested" if opp.id in self._abort else "exit timeout"
            await self._note(trade, f"{why}: exiting {remaining:.8g} at market")
            market = await self._exit_market(adapter, trade, remaining)
            orders.append(market)
        return await self._close(opp, adapter, trade, orders, registered, position_open)

    async def _close(
        self,
        opp: Opportunity,
        adapter: ExchangeAdapter,
        trade: TradeRecord,
        exit_orders: list[Order | None],
        registered: bool,
        position_open: float,
    ) -> tuple[bool, float]:
        """Finalize after the exit leg; FAILED when part of the position is still open."""
        combined = self._combine(exit_orders)
        trade.exit_order = combined
        for o in exit_orders:
            if o is not None and o.id and "+" not in o.id:
                self._persist_order(o, trade.id)
        exit_filled = float(combined.filled) if combined is not None else 0.0
        still_open = max(0.0, position_open - exit_filled)
        if still_open > _EPS:
            await self._fail(opp, adapter, trade, f"exit incomplete: {still_open:.8g} {trade.asset} still open", registered=registered, position_open=still_open)
            return registered, still_open
        trade.finished_ts = self._clock.wall()
        finalize(trade)
        trade.state = OppState.CLOSED
        pnl = float(trade.realized_pnl_quote or 0.0)
        self._persist_trade(trade)
        if registered:
            self._risk.register_close(trade.id, pnl)
        self._record_stats(trade, success=pnl > 0)
        await self._transition(opp, OppState.CLOSED, f"realized {pnl:+.2f}")
        await self._note(trade, f"closed: realized {pnl:+.2f} ({trade.diff_reason})")
        return False, 0.0

    async def _cancelled(self, opp: Opportunity, trade: TradeRecord, reason: str, *, registered: bool) -> None:
        trade.state = OppState.CANCELLED
        trade.error = reason
        trade.finished_ts = self._clock.wall()
        finalize(trade)
        if registered:
            self._risk.register_close(trade.id, float(trade.realized_pnl_quote or 0.0))
        await self._transition(opp, OppState.CANCELLED, reason)
        await self._note(trade, f"cancelled: {reason}")

    async def _fail(
        self,
        opp: Opportunity,
        adapter: ExchangeAdapter | None,
        trade: TradeRecord,
        reason: str,
        *,
        registered: bool,
        position_open: float,
    ) -> None:
        trade.state = OppState.FAILED
        trade.error = reason
        trade.finished_ts = self._clock.wall()
        finalize(trade)
        pnl = float(trade.realized_pnl_quote or 0.0)
        if position_open > _EPS and adapter is not None:
            # exposure stays registered until emergency_close_positions closes it
            self._open_positions[trade.id] = _OpenPosition(adapter=adapter, trade=trade, base_open=position_open)
            log.error("trade %s FAILED with %.8g %s still open: %s", trade.id, position_open, trade.asset, reason)
        elif registered:
            self._risk.register_close(trade.id, pnl)
        self._record_stats(trade, success=False)
        await self._transition(opp, OppState.FAILED, reason)
        await self._note(trade, f"failed: {reason}")

    # ------------------------------------------------------------------ #
    # emergency
    # ------------------------------------------------------------------ #
    async def emergency_stop(self, reason: str = "user") -> dict[str, Any]:
        """Block new AUTO trades, abort trades before entry, cancel pending orders.
        Open positions are NOT closed here (see ``emergency_close_positions``)."""
        await self._risk.set_emergency(True, reason)
        aborted: list[str] = []
        running = [t for t in self._tasks.values() if not t.done()]
        for opp_id, task in list(self._tasks.items()):
            if not task.done():
                self._abort[opp_id] = f"emergency stop: {reason}"
                aborted.append(opp_id)
        cancelled = 0
        for opp_id, (adapter, order) in list(self._pending_exit.items()):
            try:
                await adapter.cancel_order(order.id, order.symbol)
                cancelled += 1
            except Exception as exc:  # noqa: BLE001
                log.warning("emergency: cancel of %s on %s failed: %s", order.id, adapter.name, exc)
        for mode_adapters in self._exec_adapters.values():
            for adapter in mode_adapters.values():
                try:
                    cancelled += int(await adapter.cancel_all() or 0)
                except Exception as exc:  # noqa: BLE001
                    log.warning("emergency: cancel_all on %s failed: %s", adapter.name, exc)
        if running:
            wait_s = min(2.0, max(0.05, self.poll_interval_s * 10))
            await asyncio.wait(running, timeout=wait_s)
        result = {
            "ok": True,
            "emergency": True,
            "reason": reason,
            "cancelled_orders": cancelled,
            "aborted": aborted,
            "open_positions": self._open_position_list(),
        }
        log.warning("EMERGENCY STOP: %s", result)
        return result

    def _open_position_list(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for tid, pos in self._open_positions.items():
            out.append({"trade_id": tid, "opp_id": pos.trade.opportunity_id, "exchange": pos.trade.exchange,
                        "symbol": pos.trade.symbol, "base": pos.base_open, "source": "auto"})
        for opp_id, trade in self._manual.items():
            base = trade.capital_quote / trade.actual_entry_price if trade.actual_entry_price else None
            out.append({"trade_id": trade.id, "opp_id": opp_id, "exchange": trade.exchange,
                        "symbol": trade.symbol, "base": base, "source": "manual"})
        return out

    async def emergency_close_positions(self) -> list[TradeRecord]:
        """Market-exit every open position: tracked FAILED trades first, then
        whatever the adapters of the current mode still report (``positions``)."""
        closed: list[TradeRecord] = []
        for tid, pos in list(self._open_positions.items()):
            trade = pos.trade
            await self._note(trade, f"emergency close: exiting {pos.base_open:.8g} {trade.asset} at market")
            market = await self._exit_market(pos.adapter, trade, pos.base_open)
            trade.exit_order = self._combine([trade.exit_order, market])
            self._persist_order(market, trade.id)
            left = max(0.0, pos.base_open - float(market.filled))
            if left > _EPS:
                pos.base_open = left
                await self._note(trade, f"emergency close incomplete: {left:.8g} {trade.asset} still open")
                continue
            self._open_positions.pop(tid, None)
            trade.finished_ts = self._clock.wall()
            finalize(trade)
            trade.state = OppState.CLOSED
            trade.error = (trade.error + "; " if trade.error else "") + "closed by emergency close"
            self._risk.register_close(trade.id, float(trade.realized_pnl_quote or 0.0))
            await self._note(trade, f"emergency close done: realized {float(trade.realized_pnl_quote or 0.0):+.2f}")
            closed.append(trade)
        for name, adapter in self._adapters_of_mode().items():
            positions = getattr(adapter, "positions", None)
            if positions is None:
                continue
            try:
                pmap = positions() if callable(positions) else positions
                pmap = {str(k): dict(v) for k, v in dict(pmap).items()}
            except Exception as exc:  # noqa: BLE001
                log.warning("emergency close: positions of %s unreadable: %s", name, exc)
                continue
            for symbol, p in pmap.items():
                amount = float(p.get("amount") or 0.0)
                if amount <= _EPS:
                    continue
                side = str(p.get("side") or "long").lower()
                direction = -1 if side == "short" else 1
                entry = float(p.get("entry") or 0.0)
                base_ccy = split_symbol(symbol)[0]
                trade = TradeRecord(
                    id=new_id("tr_"),
                    opportunity_id="",
                    mode=self._settings.mode(),
                    exchange=name,
                    asset=base_ccy,
                    symbol=symbol,
                    direction=direction,
                    capital_quote=entry * amount,
                    expected_net_pct=0.0,
                    expected_profit_quote=0.0,
                    expected_entry_price=entry,
                    expected_exit_price=entry,
                    state=OppState.EXECUTING,
                    actual_entry_price=entry if entry > 0 else None,
                    started_ts=self._clock.wall(),
                )
                self._trades[trade.id] = trade
                await self._note(trade, f"emergency close: untracked {side} position {amount:.8g} {base_ccy} on {name}")
                market = await self._exit_market(adapter, trade, amount)
                trade.exit_order = market
                self._persist_order(market, trade.id)
                trade.finished_ts = self._clock.wall()
                finalize(
                    trade,
                    entry_price=entry if entry > 0 else None,
                    base_amount=float(market.filled) if market.filled > 0 else None,
                )
                left = max(0.0, amount - float(market.filled))
                if left > _EPS:
                    trade.state = OppState.FAILED
                    trade.error = f"emergency close incomplete: {left:.8g} {base_ccy} still open"
                    self._open_positions[trade.id] = _OpenPosition(adapter=adapter, trade=trade, base_open=left)
                else:
                    trade.state = OppState.CLOSED
                await self._note(trade, f"emergency close {trade.state.value}: realized {float(trade.realized_pnl_quote or 0.0):+.2f}")
                closed.append(trade)
        return closed

    async def resume(self) -> None:
        """Clear the emergency stop; new AUTO trades are allowed again."""
        await self._risk.set_emergency(False, "resume")
        self._abort.clear()
        log.warning("execution resumed")

    # ------------------------------------------------------------------ #
    # queries
    # ------------------------------------------------------------------ #
    def trades(self, limit: int = 200) -> list[TradeRecord]:
        """In-memory trades of this process, newest first."""
        items = list(self._trades.values())
        items.reverse()
        return items[: max(0, int(limit))]

    def trade(self, trade_id: str) -> TradeRecord | None:
        return self._trades.get(trade_id)

    def active_trade(self, opp_id: str) -> TradeRecord | None:
        return self._active.get(opp_id) or self._manual.get(opp_id)

    def open_positions(self) -> list[dict[str, Any]]:
        return self._open_position_list()
