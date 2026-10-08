"""ExecutionEngine - MANUAL support and the AUTO round trip.

AUTO flow (``execute``)::

    AUTO_SELECTED -> revalidate (latest book, risk, balance)
                  -> EXECUTING: entry LIMIT IOC at entry_vwap * (1 +/- buy_limit_slippage_pct)
                  -> no fill            -> CANCELLED
                  -> fill < min ratio   -> cancel rest, market-exit the filled part -> FAILED
                  -> PARTIALLY_FILLED / FILLED
                  -> exit revalidated on the latest book (repriced; edge gone -> MARKET exit)
                  -> exit LIMIT GTC at exit_target (reduceOnly on swap), wait sell_timeout_s
                  -> unfilled: exit_on_timeout == "market" -> cancel + MARKET exit
                               "hold"  -> wait until max_hold_s from entry, then MARKET
                  -> finalize (pnl), CLOSED, risk.register_close, stats.record_trade

Hard rules implemented here: never execute a stale snapshot (``revalidate``
runs on the latest book right before the entry, and the exit is repriced from
the latest book right before the exit order), one place sends orders
(``_place``), every order carries an idempotent ``client_id``, a timeout is
status "unknown" (one ``fetch_order`` by client id, never a retry), an order
whose state can not be confirmed is never assumed cancelled: the trade FAILS
with the position tracked (``open_positions()``, risk exposure kept) until
``emergency_close_positions`` resolves the order and exits what is really
open.  Risk is checked before every order and nothing is sent while
``risk.emergency``.  ``shutdown`` drains running trades (abort -> market exit)
and ``restore_from_db`` picks up positions a crash / restart left behind.

MANUAL flow: ``select_manual`` -> ``manual_support`` (live payload polled by
the UI; "NO FRESH DATA" when the target book is older than max_data_age_ms)
-> ``manual_entered`` (user bought by hand) -> ``manual_exited`` (user sold by
hand) -> realized P&L.  Manual trades are ``TradeRecord``s without orders;
prices are user-reported, fees estimated from the adapter.
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
from ..engine.explain import cap_price
from ..exchanges.base import ExchangeAdapter, ExchangeError, OrderRejected, RateLimited
from ..links import split_symbol
from ..logging_setup import redact
from ..models import (
    ACTIVE_STATES,
    TERMINAL_STATES,
    EXECUTION_STATES,
    ExecutionEconomics,
    Light,
    Mode,
    Opportunity,
    OppState,
    Order,
    OrderBook,
    OrderRequest,
    OrderStatus,
    OrderType,
    Side,
    TimeInForce,
    TradeRecord,
    new_id,
)
from ..storage.db import trade_from_dict
from .pnl import finalize
from .risk import RiskManager

if TYPE_CHECKING:  # pragma: no cover
    from ..engine.opportunity import OpportunityManager
    from ..engine.pipeline import Pipeline
    from ..engine.stats import StatsStore
    from ..storage.db import Database

log = logging.getLogger(__name__)

_EPS = 1e-9
_CANCEL_REFRESH_ATTEMPTS = 3  # fetch_order attempts after a failed cancel before the state counts as unknown
_MAX_RETRY_SLEEP_S = 5.0
_RL_MAX_CLOSE_WAIT_S = 300.0  # longest the AUTO flow's closing order / cancel waits for an exchange rate-limit window
_RL_MAX_EMERGENCY_WAIT_S = 5.0  # an emergency close is awaited by a person (API / Telegram): short wait, else refuse
_GONE_GRACE_S = 10.0  # "order not found" resolves an order only this long after it was sent (propagation lag)
_RESTORE_MAX_AGE_S = 3 * 86400.0  # older rows are not restored as positions (logged instead)
_NOTE_TIMEOUT_S = 2.0
EXPIRED_MESSAGE = "⚠️ OPPORTUNITY EXPIRED"
TOO_FAST_MESSAGE = "🔴 TOO FAST FOR HUMAN"
NO_DATA_MESSAGE = "⚠️ NO FRESH DATA"


@dataclass(slots=True)
class _OpenPosition:
    """A position the engine still owns: a FAILED trade with base left, or the
    progress of a running trade (``_in_flight``).

    ``pending`` is an order whose final state is not confirmed (no exchange id
    after a timeout, a cancel that could not be confirmed, an order in flight
    when the task was cancelled).  It must be resolved - fetched by id / client
    id, cancelled if it still rests - before the remainder may be exited at
    market: never a second order for the same quantity.  ``entry_unknown``
    marks ``pending`` as the entry; ``base_open`` is then the worst case (the
    full amount) until the fill is known."""

    adapter: ExchangeAdapter
    trade: TradeRecord
    base_open: float
    pending: Order | None = None
    pending_filled: float = 0.0  # pending.filled when it was recorded
    entry_unknown: bool = False

    @property
    def unresolved(self) -> bool:
        return self.pending is not None and not self.pending.is_done


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
        self._pending_exit: dict[str, tuple[ExchangeAdapter, Order]] = {}  # running trades' resting exits
        self._in_flight: dict[str, _OpenPosition] = {}  # opp_id -> progress of the running trade
        self._abort: dict[str, str] = {}
        self._expected: dict[str, ExecutionEconomics] = {}  # what the user saw when selecting
        self._plan: dict[str, _Plan] = {}
        self._open_positions: dict[str, _OpenPosition] = {}
        self._manual: dict[str, TradeRecord] = {}
        self._manual_entry_mono: dict[str, float] = {}
        self._manual_capital: dict[str, float] = {}  # amount the owner chose in the session ("сколько вложить")
        self._manual_selected_mono: dict[str, float] = {}  # "Я ЗАШЁЛ" pressed (session opened)
        self._rest_book_mono: dict[tuple[str, str], float] = {}  # (exchange, symbol) -> last REST book attempt
        self._rate_limited_until: dict[str, float] = {}  # exchange -> loop time a RateLimited told us to stay away until
        self._restored: list[TradeRecord] = []

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
            log.exception("db.save_order failed for %s", order.id or order.client_id)

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
        self._manual_capital.pop(opp_id, None)
        self._manual_selected_mono.pop(opp_id, None)
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

    @staticmethod
    def _unresolved(orders: list[Order | None]) -> Order | None:
        """The first order whose final state is not confirmed (not done)."""
        for o in orders:
            if o is not None and not o.is_done:
                return o
        return None

    async def _fresh_book(
        self, adapter: ExchangeAdapter, symbol: str, *, throttle_s: float = 0.0
    ) -> tuple[OrderBook | None, float | None]:
        """The adapter's book for ``symbol`` when it is younger than
        ``max_data_age_ms``; otherwise one REST snapshot (published as BOOK so
        the pipeline's state sees it too).  ``throttle_s`` bounds the REST rate
        for callers that poll.  Returns ``(book, age_ms)``; the caller decides."""
        s = self._settings
        now = self._clock.mono()
        book = adapter.order_book(symbol)
        age = book.age_ms(now) if book is not None else None
        if book is not None and age is not None and age <= s.max_data_age_ms:
            return book, age
        key = (adapter.name, symbol)
        last = self._rest_book_mono.get(key)
        if throttle_s > 0 and last is not None and now - last < throttle_s:
            return book, age
        self._rest_book_mono[key] = now
        try:
            rest = await adapter.fetch_order_book(symbol)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("%s: REST book for %s failed: %s", adapter.name, symbol, exc)
            return book, age
        if rest is None:
            return book, age
        try:
            await self._bus.publish(events.BOOK, rest)
        except Exception:  # noqa: BLE001
            log.exception("BOOK publish failed")
        return rest, rest.age_ms(self._clock.mono())

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
        max_rl_wait_s: float = _RL_MAX_CLOSE_WAIT_S,
    ) -> Order:
        """Send ``req``.  Returns an ``Order`` even on failure (status FAILED,
        or SENT without an id = unknown after a timeout that could not be
        reconciled; the caller must never treat that as "not filled").  A
        *closing* order waits up to ``max_rl_wait_s`` for the exchange's
        rate-limit window and comes back FAILED (nothing sent) when the
        window is longer."""
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
        if closing:
            # a closing order may go out during an emergency, but never into an active rate-limit ban
            try:
                await self._await_rate_limit(adapter, f"{req.type.value} {req.side.value} {req.symbol}", max_rl_wait_s)
            except RateLimited as exc:
                failed.error = redact(str(exc))
                log.error("order %s not sent to %s: %s", req.client_id, adapter.name, exc)
                return failed
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
            if isinstance(exc, RateLimited):
                self._note_rate_limit(adapter, exc)
            failed.error = redact(str(exc))
            log.error("order %s rejected by %s: %s", req.client_id, adapter.name, exc)
            order = failed
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            if type(exc).__name__ == "RequestTimeout":  # ccxt's network timeout is not an asyncio one
                log.error("order %s timed out on %s (%s) -> status unknown, reconciling once", req.client_id, adapter.name, type(exc).__name__)
                order = await self._reconcile_unknown(adapter, req, failed)
            else:
                failed.error = redact(f"{type(exc).__name__}: {exc}")
                log.exception("order %s failed on %s", req.client_id, adapter.name)
                order = failed
        if order.status is OrderStatus.SENT and not order.id:
            # the adapter swallowed a timeout (ccxt adapter contract): the exchange may well have the order
            log.error("order %s on %s came back without an id -> status unknown, reconciling once", req.client_id, adapter.name)
            order = await self._reconcile_unknown(adapter, req, self._merge_timing(failed, order))
        if order.sent_mono is None:
            order.sent_mono = sent_mono
        if order.sent_ts is None:
            order.sent_ts = sent_ts
        if order.ack_mono is None and order.status not in (OrderStatus.NEW, OrderStatus.SENT, OrderStatus.FAILED):
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
        """One fetch_order by client id after a timeout; never a retry.  The
        order stays SENT without an id when the exchange can not be asked."""
        unknown.status = OrderStatus.SENT
        unknown.error = unknown.error or "timeout: status unknown"
        found, why = await self._fetch_state(adapter, unknown)
        if found is None:
            log.error("reconcile of %s on %s failed (%s): order state unknown", req.client_id, adapter.name, why)
            return unknown
        return found

    async def _fetch_state(self, adapter: ExchangeAdapter, order: Order) -> tuple[Order | None, str]:
        """``fetch_order`` by exchange id, or by client id when the id is unknown
        (adapters whose ``fetch_order`` accepts ``client_id``).  Returns the
        order merged with the fresh state and ``"ok"``, or ``(None, why)`` with
        why in ``unavailable`` (no way to ask), ``gone`` (the exchange says the
        order does not exist) or ``error``."""
        ident = order.id or order.client_id
        try:
            if order.id:
                fresh = await adapter.fetch_order(order.id, order.symbol)
            elif order.client_id and _accepts_kwarg(adapter.fetch_order, "client_id"):
                fresh = await adapter.fetch_order("", order.symbol, client_id=order.client_id)
            else:
                return None, "unavailable"
        except asyncio.CancelledError:
            raise
        except OrderRejected as exc:
            log.warning("fetch_order %s on %s: not found (%s)", ident, adapter.name, exc)
            return None, "gone"
        except RateLimited as exc:
            retry = self._note_rate_limit(adapter, exc)
            log.warning("fetch_order %s on %s rate limited: no request for %.1f s", ident, adapter.name, retry)
            return None, "rate_limited"
        except Exception as exc:  # noqa: BLE001
            log.warning("fetch_order %s on %s failed: %s", ident, adapter.name, exc)
            return None, "error"
        if fresh is None:
            return None, "error"
        return self._merge_timing(order, fresh), "ok"

    async def _refresh(self, adapter: ExchangeAdapter, order: Order) -> Order:
        fresh, _why = await self._fetch_state(adapter, order)
        return fresh if fresh is not None else order

    # ---- rate limits: never knock on an exchange inside its back-off window ----
    def _note_rate_limit(self, adapter: ExchangeAdapter, exc: BaseException) -> float:
        """Remember a ``RateLimited`` from ``adapter``; returns its retry delay."""
        retry = max(0.0, float(getattr(exc, "retry_after_s", 0.0) or 0.0))
        until = asyncio.get_running_loop().time() + retry
        if until > self._rate_limited_until.get(adapter.name, 0.0):
            self._rate_limited_until[adapter.name] = until
        return retry

    def _rate_limit_wait_s(self, adapter: ExchangeAdapter) -> float:
        """Seconds until ``adapter`` may be called again (0 = now): the longer
        of the last ``RateLimited.retry_after_s`` and the adapter's health window."""
        wait = self._rate_limited_until.get(adapter.name, 0.0) - asyncio.get_running_loop().time()
        until = getattr(getattr(adapter, "health", None), "rate_limited_until_mono", None)
        if until:
            wait = max(wait, float(until) - self._clock.mono())
        return max(0.0, wait)

    async def _await_rate_limit(self, adapter: ExchangeAdapter, what: str, max_wait_s: float = _RL_MAX_CLOSE_WAIT_S) -> None:
        """Wait out the exchange's rate-limit window before ``what``.  When the
        window outlasts ``max_wait_s`` raise ``RateLimited`` at once instead -
        a request sent into an active ban only extends the ban."""
        wait = self._rate_limit_wait_s(adapter)
        if wait <= 0.0:
            return
        if wait > max_wait_s:
            raise RateLimited(retry_after_s=wait, msg=f"{what} not sent: {adapter.name} rate-limit window ends in {wait:.0f} s")
        log.warning("%s on %s delayed %.1f s: inside the exchange's rate-limit window", what, adapter.name, wait)
        await asyncio.sleep(wait)

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
            wait = max(self.poll_interval_s, self._rate_limit_wait_s(adapter))
            await asyncio.sleep(max(0.0, min(wait, deadline - loop.time())))
            if self._rate_limit_wait_s(adapter) > 0.0:
                continue  # window not over (the deadline cut the sleep short): no request into an active ban
            order = await self._refresh(adapter, order)

    async def _cancel(self, adapter: ExchangeAdapter, order: Order) -> Order:
        """Cancel an open order and return its confirmed state.

        The order is marked CANCELLED only when the exchange accepted the cancel
        (or reports the order done).  When the cancel and the follow-up fetches
        all fail, or the order has no id the exchange knows, it is returned as
        it is - not done - and the caller must treat its state as unknown,
        never as cancelled (hard rule 5)."""
        if order.is_done:
            return order
        if not order.id:
            order = await self._refresh(adapter, order)  # learn the id by client id
            if order.is_done:
                return order
            if not order.id:
                order.error = order.error or "cancel skipped: order id unknown"
                log.error("cancel of %s on %s skipped: order id unknown", order.client_id, adapter.name)
                return order
        confirmed = False
        retry_after = 0.0
        try:
            await self._await_rate_limit(adapter, f"cancel of {order.id}")
        except RateLimited as exc:
            order.error = order.error or redact(str(exc))
            log.error("cancel of %s on %s not sent: %s", order.id, adapter.name, exc)
            return order  # still resting as far as we know: the caller treats it as unresolved, never as cancelled
        try:
            res = await adapter.cancel_order(order.id, order.symbol)
            confirmed = True
            if res is not None:
                order = self._merge_timing(order, res)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            retry_after = float(getattr(exc, "retry_after_s", 0.0) or 0.0)
            if isinstance(exc, RateLimited):
                self._note_rate_limit(adapter, exc)
            log.warning("cancel_order %s on %s failed: %s", order.id, adapter.name, exc)
        attempts = 1 if confirmed else _CANCEL_REFRESH_ATTEMPTS
        for i in range(attempts):
            try:
                await self._await_rate_limit(adapter, f"fetch_order {order.id}")
            except RateLimited as exc:
                log.error("fetch_order %s on %s not sent: %s", order.id, adapter.name, exc)
                break
            fresh, _why = await self._fetch_state(adapter, order)
            if fresh is not None:
                order = fresh
                break
            if i + 1 < attempts:
                await asyncio.sleep(min(_MAX_RETRY_SLEEP_S, max(self.poll_interval_s, retry_after)))
        if order.is_done:
            if order.done_mono is None:
                order.done_mono = self._clock.mono()
            return order
        if confirmed:
            order.status = OrderStatus.CANCELLED
            order.done_mono = self._clock.mono()
            return order
        order.error = order.error or "cancel failed: order state unknown"
        log.error("order %s on %s: cancel not confirmed and state unknown", order.id, adapter.name)
        return order

    async def _exit_market(
        self, adapter: ExchangeAdapter, trade: TradeRecord, amount: float, *, max_rl_wait_s: float = _RL_MAX_CLOSE_WAIT_S
    ) -> Order:
        """MARKET exit of ``amount`` base (reduceOnly on swap).  Allowed during
        emergency; waits up to ``max_rl_wait_s`` for a rate-limit window."""
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
        holder = self._in_flight.get(trade.opportunity_id or "")
        if holder is not None and holder.trade is not trade:
            holder = None
        order = await self._place(
            adapter, req, trade_id=trade.id, reduce_only=self._is_swap(trade.exchange), closing=True, max_rl_wait_s=max_rl_wait_s
        )
        if order.status in (OrderStatus.FAILED, OrderStatus.REJECTED):
            return order
        if holder is not None:
            holder.pending, holder.pending_filled = order, 0.0
        order = await self._wait_fill(adapter, order, self._settings.buy_fill_timeout_s, trade.opportunity_id, closing=True)
        if holder is not None:
            holder.pending = None if order.is_done else order
            holder.pending_filled = 0.0 if order.is_done else float(order.filled)
            holder.base_open = max(0.0, holder.base_open - float(order.filled))
        return order

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
            raw={"orders": [o.id for o in real], "client_ids": [o.client_id for o in real], "market_filled": market_filled},
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
            "entry_cap_price": None,
            "plan": {},
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
                entry_cap_price=cap_price(opp.econ, self._settings) or None,
                plan=dict(opp.plan),  # the last plan built on real data (carries its own "обновлено" time)
            )
        return d

    def _no_data_payload(self, opp: Opportunity, manual: TradeRecord | None) -> dict[str, Any]:
        """No fresh target book: no live price is shown (hard rule 1)."""
        d = self._expired_payload(opp, opp.id, NO_DATA_MESSAGE)
        d.update(
            status="no_data",
            expired=not opp.is_active and manual is None,
            entry_price_now=None,
            exit_target_now=None,
            net_now_pct=None,
            entry_cap_price=None,  # a buy limit from a stale book is not a limit
            plan={},
            remaining_s=round(opp.remaining_s(self._clock.mono()), 2),
            trade_id=manual.id if manual is not None else None,
            entry_price=manual.actual_entry_price if manual is not None else None,
            quote_amount=manual.capital_quote if manual is not None else None,
            exit_side="SELL" if opp.direction > 0 else "BUY",
        )
        return d

    def _position_only_payload(self, opp_id: str, manual: TradeRecord) -> dict[str, Any]:
        """An open MANUAL position whose opportunity no longer exists (restored
        after a restart): the exit stays reportable, nothing live is invented
        (hard rule 1) - no price, no net, no window."""
        d = self._expired_payload(None, opp_id, "🟢 IN POSITION - opportunity no longer tracked; report your exit price")
        d.update(
            valid=True,
            status="in_position",
            expired=False,
            side="LONG" if manual.direction > 0 else "SHORT",
            exchange=manual.exchange,
            symbol=manual.symbol,
            state=manual.state.value,
            quote=split_symbol(manual.symbol)[1],
            capital=manual.capital_quote,
            base_amount=(manual.capital_quote / manual.actual_entry_price) if manual.actual_entry_price else None,
            expected_net_pct=manual.expected_net_pct,
            trade_id=manual.id,
            entry_price=manual.actual_entry_price,
            quote_amount=manual.capital_quote,
            exit_side="SELL" if manual.direction > 0 else "BUY",
        )
        return d

    def _mode_mismatch(self, opp: Opportunity) -> str | None:
        """Hard rule 8: the card the owner acts on must show the mode that
        would execute.  After a PAPER/LIVE switch the pipeline re-labels the
        active cards within one sweep; until then nothing executes from them."""
        mode = self._settings.mode()
        if opp.mode is mode:
            return None
        return (
            f"mode changed to {mode.value.upper()} since this opportunity was detected "
            f"(card shows {opp.mode.value.upper()}); wait for the card to refresh and select again"
        )

    def _set_manual_capital(self, opp_id: str, capital: float | None) -> None:
        if capital is None:
            return
        try:
            value = float(capital)
        except (TypeError, ValueError):
            return
        if value > 0:
            self._manual_capital[opp_id] = value

    async def select_manual(self, opp_id: str, capital: float | None = None) -> dict[str, Any]:
        """Mark the opportunity MANUAL_SELECTED ("Я ЗАШЁЛ") and return the live
        support payload; ``capital`` = the amount the owner wants to put in."""
        self._set_manual_capital(opp_id, capital)
        opp = self._opps.get(opp_id)
        if opp is None:
            manual = self._manual.get(opp_id)
            if manual is not None:  # restored after a restart: the card is gone, the position is not
                return self._position_only_payload(opp_id, manual)
            return self._expired_payload(None, opp_id)
        if opp_id in self._active:
            return self._expired_payload(opp, opp_id, "⚠️ AUTO EXECUTION IN PROGRESS")
        mismatch = self._mode_mismatch(opp)
        if mismatch is not None and opp.is_active:
            d = self._expired_payload(opp, opp_id, f"⚠️ {mismatch}")
            d["status"] = "mode_changed"
            d["expired"] = False
            return d
        if opp.state == OppState.AUTO_SELECTED:
            await self._transition(opp, OppState.VALID, "switched to manual")
        if opp.state in (OppState.VALID, OppState.NOTIFIED):
            await self._transition(opp, OppState.MANUAL_SELECTED, "user selected MANUAL")
        if opp.state == OppState.MANUAL_SELECTED or opp.is_active:
            self._snapshot_expected(opp)
            self._manual_selected_mono.setdefault(opp_id, self._clock.mono())
        return await self.manual_support(opp_id)

    async def manual_support(self, opp_id: str, capital: float | None = None) -> dict[str, Any]:
        """Live numbers for the MANUAL modal, recomputed from the latest book.

        The target book must be younger than ``max_data_age_ms`` (one throttled
        REST snapshot otherwise); without fresh data the payload says so
        (``status="no_data"``) and the pipeline is NOT refreshed, so the poll
        can not keep a dead opportunity alive."""
        s = self._settings
        self._set_manual_capital(opp_id, capital)
        opp = self._opps.get(opp_id)
        manual = self._manual.get(opp_id)
        if opp is None:
            if manual is not None:  # restored after a restart: the card is gone, the position is not
                return self._position_only_payload(opp_id, manual)
            return self._expired_payload(None, opp_id)
        if not opp.is_active and manual is None:
            return self._expired_payload(opp, opp_id)
        requested = self._manual_capital.get(opp_id)
        if manual is not None:
            capital = manual.capital_quote
        elif requested is not None:
            capital = requested
        else:
            capital = float(opp.econ.capital_quote or s.capital_quote)
        adapter = self.adapter_for(opp)
        if adapter is not None:
            book, age = await self._fresh_book(adapter, opp.target_symbol, throttle_s=s.max_data_age_ms / 1000.0)
            if book is None or age is None or age > s.max_data_age_ms:
                log.debug("manual_support %s: no fresh book for %s on %s (age %s ms)", opp_id, opp.target_symbol, opp.target_exchange, age)
                return self._no_data_payload(opp, manual)
        try:
            fresh = await self._pipeline.refresh(opp, capital)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("manual_support %s: refresh failed: %s", opp_id, exc)
            fresh = None
        if fresh is None or float(fresh.econ.book_age_ms) > s.max_data_age_ms:
            return self._no_data_payload(opp, manual)
        econ = fresh.econ
        now = self._clock.mono()
        remaining = fresh.remaining_s(now)
        human = fresh.human
        expected = self._expected.get(opp_id, opp.econ)
        if manual is not None:
            status, message = "in_position", "🟢 IN POSITION - report your exit price"
        elif remaining < s.human_execution_time_s:
            status, message = "too_fast", TOO_FAST_MESSAGE
        elif human.light == Light.RED:  # time is fine, the size / book is not
            status = "not_executable"
            message = "⚠️ NOT EXECUTABLE - " + (human.reasons[0] if human.reasons else human.label)
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
            # the manual trader's limit ("купить <= X") and the Russian plan built on this fresh book
            "entry_cap_price": cap_price(econ, s),
            "plan": dict(fresh.plan),
            "fresh": True,
            "trade_id": manual.id if manual is not None else None,
            # what a fresh client (page reload / another tab) needs to rebuild the exit step of an open position
            "entry_price": manual.actual_entry_price if manual is not None else None,
            "quote_amount": manual.capital_quote if manual is not None else None,
            "exit_side": "SELL" if opp.direction > 0 else "BUY",
            # MANUAL trade session: exact quantities for the chosen amount + the escort
            "requested_capital": requested if manual is None else manual.capital_quote,
            "buy_amount": econ.base_amount,
            "net_quote": econ.net_quote,
            "exit_price_now": econ.exit_vwap_now,
            **self._escort(opp, manual, econ, remaining, lag_gone=float(fresh.signal.lag_pct) < float(s.min_lag_pct)),
        }

    def _escort(
        self, opp: Opportunity, manual: TradeRecord | None, econ: Any, remaining: float, lag_gone: bool = False
    ) -> dict[str, Any]:
        """What to do with the exit leg right now, from the latest real book.

        Before the entry: the amount to sell later = the amount bought.  In a
        position: the target is the one the owner entered for; "sell_now" when
        the book already pays it, "sell_market" when the window is over (the
        lag is gone - waiting is a bet), "wait" otherwise."""
        q = opp.quote or split_symbol(opp.target_symbol)[1]
        if manual is None:
            return {"sell_amount": econ.base_amount, "sell_signal": None, "sell_text": "", "pnl_now_pct": None, "pnl_now_quote": None}
        d = opp.direction
        entry = float(manual.actual_entry_price or 0.0)
        base = manual.capital_quote / entry if entry > 0 else 0.0
        target = float(manual.expected_exit_price or econ.exit_target)
        now_px = float(econ.exit_vwap_now or 0.0)
        taker = self._taker_fee(manual.exchange)
        pnl_quote = pnl_pct = None
        if entry > 0 and now_px > 0:
            pnl_quote = (now_px - entry) * base * d - (manual.capital_quote * taker + now_px * base * taker)
            pnl_pct = pnl_quote / manual.capital_quote * 100.0 if manual.capital_quote else None
        reached = now_px > 0 and (now_px - target) * d >= 0
        side_word = "ПРОДАВАЙ" if d > 0 else "ОТКУПАЙ"
        if reached:
            signal = "sell_now"
            text = f"{side_word} СЕЙЧАС: стакан даёт {now_px:.8g} {q} ≥ цели {target:.8g} {q}"
        elif lag_gone:
            signal = "sell_market"
            text = f"Рынок откатился, лаг исчез — продай по рынку (сейчас {now_px:.8g} {q}); цель {target:.8g} {q} уже не придёт"
        elif remaining <= 0 or opp.state in TERMINAL_STATES:
            signal = "sell_market"
            text = f"Окно закрылось — продай по рынку (сейчас {now_px:.8g} {q}); цель {target:.8g} {q} уже не обещана"
        else:
            signal = "wait"
            text = f"ЖДИ: продавать при цене ≥ {target:.8g} {q}, сейчас {now_px:.8g} {q} · осталось ~{remaining:.0f} с"
        return {"sell_amount": base, "sell_signal": signal, "sell_text": text, "pnl_now_pct": pnl_pct, "pnl_now_quote": pnl_quote,
                "exit_target_now": target}

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
        mismatch = self._mode_mismatch(opp)
        if mismatch is not None:
            return {"ok": False, "reason": mismatch}
        if opp.state == OppState.AUTO_SELECTED:
            await self._transition(opp, OppState.VALID, "switched to manual")
        if opp.state in (OppState.VALID, OppState.NOTIFIED):
            await self._transition(opp, OppState.MANUAL_SELECTED, "user selected MANUAL")
        expected = self._snapshot_expected(opp)
        now_mono = self._clock.mono()
        in_window = bool(opp.is_active and opp.remaining_s(now_mono) > 0)
        selected = self._manual_selected_mono.get(opp_id)
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
            manual=True,
            entered_in_window=in_window,
            time_to_entry_s=(now_mono - selected) if selected is not None else None,
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

    async def manual_exited(
        self, opp_id: str, price: float, reported_pnl_quote: float | None = None, note: str = ""
    ) -> TradeRecord:
        """"Я УШЁЛ СО СДЕЛКИ": realized P&L from the owner's exit price, CLOSED.
        ``reported_pnl_quote`` / ``note``: the result the exchange showed the
        owner, stored next to the expected one (and used for the learning)."""
        trade = self._manual.get(opp_id)
        if trade is None:
            raise KeyError(f"no open manual trade for opportunity {opp_id}")
        price = float(price)
        if price <= 0:
            raise ValueError("exit price must be positive")
        if reported_pnl_quote is not None:
            trade.user_reported_pnl_quote = float(reported_pnl_quote)
        if note:
            trade.user_note = str(note)[:2000]
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
        result = trade.user_reported_pnl_quote if trade.user_reported_pnl_quote is not None else pnl
        try:
            self._stats.record_manual_result(trade.asset, trade.exchange, bool(trade.entered_in_window), result > 0)
        except Exception:  # noqa: BLE001
            log.exception("stats.record_manual_result failed for %s", trade.id)
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
        mismatch = self._mode_mismatch(opp)
        if mismatch is not None:
            return mismatch
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

    async def select_auto(self, opp_id: str, wait: bool = True) -> dict[str, Any]:
        """AUTO_SELECTED; execute now when ``settings.auto_confirm`` else return a confirmation snapshot.

        ``wait=False`` (what the HTTP route passes) starts the auto-confirmed trade
        in the background exactly like ``confirm_auto(wait=False)`` instead of
        holding the caller for the whole round trip."""
        opp = self._opps.get(opp_id)
        if opp is None:
            return {"ok": False, "reason": "unknown opportunity", "opp_id": opp_id}
        reason = await self._prepare_auto(opp)
        if reason is not None:
            return {"ok": False, "reason": reason, "opp_id": opp_id, "state": opp.state.value}
        if self._settings.auto_confirm:
            return await self.confirm_auto(opp_id, wait=wait)
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
            return {"ok": False, "reason": redact(f"execution error: {exc}"), "opp_id": opp_id}
        return {
            "ok": trade.state == OppState.CLOSED,
            "opp_id": opp_id,
            "state": trade.state.value,
            "reason": trade.error or trade.diff_reason,
            "trade_id": trade.id,
            "trade": trade.to_dict(),
        }

    async def deselect_auto(self, opp_id: str, reason: str = "user backed out of AUTO confirmation") -> dict[str, Any]:
        """Back out of the AUTO confirmation: AUTO_SELECTED -> VALID, nothing cancelled.

        The opportunity stays on the board (MANUAL or AUTO can be selected again).
        Any other state - a running trade, an open manual position, a terminal
        opportunity - is left untouched and reported with ``ok: False``."""
        opp = self._opps.get(opp_id)
        if opp is None:
            return {"ok": False, "opp_id": opp_id, "reason": "unknown opportunity"}
        task = self._tasks.get(opp_id)
        if (task is not None and not task.done()) or opp_id in self._active:
            return {"ok": False, "opp_id": opp_id, "state": opp.state.value, "reason": "AUTO execution already in progress; use cancel to abort"}
        if opp.state != OppState.AUTO_SELECTED:
            return {"ok": False, "opp_id": opp_id, "state": opp.state.value, "reason": f"nothing to deselect in state {opp.state.value}"}
        if not await self._transition(opp, OppState.VALID, reason):
            return {"ok": False, "opp_id": opp_id, "state": opp.state.value, "reason": "state transition refused"}
        self._plan.pop(opp_id, None)
        self._expected.pop(opp_id, None)
        return {"ok": True, "opp_id": opp_id, "state": opp.state.value, "reason": reason}

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
            reason = redact(reason)
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
        book, age = await self._fresh_book(adapter, symbol)
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
        quality = getattr(fresh, "quality", None)
        if isinstance(quality, dict) and quality and not quality.get("passed", False):
            # Phase 3: AUTO only on signals that pass the quality check on the LATEST book
            vetoes = "; ".join(str(v) for v in (quality.get("vetoes") or []))
            detail = vetoes or str(quality.get("summary") or "")
            return fail(f"Signal quality {quality.get('score', '?')}/100 did not pass" + (f": {detail}" if detail else ""))
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
        # The entry order locks notional + taker fee + the limit markup (exchanges
        # reserve price * amount for a limit buy), not the bare capital.
        reserve_factor = 1.0 + float(econ.entry_fee_pct) / 100.0 + float(s.buy_limit_slippage_pct) / 100.0
        needed = capital_final * reserve_factor
        if free + _EPS < needed:
            downsized = free / reserve_factor * (1.0 - 1e-4)  # small margin against rounding
            if s.allow_downsize and downsized >= s.min_trade_quote:
                notes.append(f"downsized to the free {quote} balance {free:.2f}")
                capital_final = downsized
            else:
                return fail(f"Insufficient {quote} balance: free {free:.2f} < needed {needed:.2f}")

        ok, why, allowed = self._risk.check(opp.target_exchange, opp.asset, capital_final, s.mode())
        plan.risk = {"ok": ok, "reason": why, "allowed": allowed}
        if not ok:
            return fail(why)
        if allowed + _EPS < capital_final:
            notes.append(why)
            capital_final = allowed
        if capital_final + _EPS < executable:
            # linear scale at the same VWAP: a smaller walk of the same book never fills worse
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

    async def _fresh_exit_target(
        self, opp: Opportunity, adapter: ExchangeAdapter, trade: TradeRecord, econ: ExecutionEconomics, entry_avg: float
    ) -> tuple[float, float | None]:
        """Hard rule 4 for the exit leg: reprice the exit from the latest book.
        Returns ``(exit_target, net % on the held position)``; the planned
        target and ``None`` when no fresh economics are available."""
        s = self._settings
        planned = float(econ.exit_target)
        why = ""
        fresh: Opportunity | None = None
        try:
            book, age = await self._fresh_book(adapter, trade.symbol)
            if book is None or age is None or age > s.max_data_age_ms:
                why = "no fresh book" if age is None else f"book {age:.0f} ms old"
            else:
                fresh = await self._pipeline.refresh(opp, trade.capital_quote)
                if fresh is None:
                    why = "no fresh economics"
                elif float(fresh.econ.book_age_ms) > s.max_data_age_ms:
                    why = f"book {fresh.econ.book_age_ms:.0f} ms old"
                    fresh = None
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            why = redact(f"{type(exc).__name__}: {exc}")
            fresh = None
        if fresh is None or not float(fresh.econ.exit_target) > 0:
            why = why or "no target"
            log.warning("trade %s: exit revalidation unavailable (%s); using the planned target %.8g", trade.id, why, planned)
            await self._note(trade, f"exit revalidation unavailable ({why}): using the planned target {planned:.8g}")
            return planned, None
        target = float(fresh.econ.exit_target)
        if entry_avg <= 0:
            return target, None
        d = 1 if trade.direction > 0 else -1
        pos_net = d * (target / entry_avg - 1.0) * 100.0 - float(econ.total_cost_pct)
        return target, pos_net

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
        try:
            await self._run(opp, adapter, trade, expected)
        except asyncio.CancelledError:
            await self._on_cancelled(opp, trade)
            raise
        except Exception as exc:  # noqa: BLE001
            log.exception("trade %s: unexpected error", trade.id)
            pos = self._in_flight.get(opp.id)
            await self._fail(
                opp, adapter, trade, redact(f"error: {exc}"),
                registered=pos is not None,
                position_open=pos.base_open if pos is not None else 0.0,
                pending=pos.pending if pos is not None else None,
                entry_unknown=pos.entry_unknown if pos is not None else False,
            )
        finally:
            self._in_flight.pop(opp.id, None)
            self._active.pop(opp.id, None)
            self._forget(opp.id)
        return trade

    async def _on_cancelled(self, opp: Opportunity, trade: TradeRecord) -> None:
        """The execute task was cancelled (hard stop, crash): keep whatever may
        be open tracked - exposure is never released on a guess."""
        pos = self._in_flight.get(opp.id)
        trade.finished_ts = self._clock.wall()
        if pos is not None and (pos.base_open > _EPS or pos.unresolved):
            trade.state = OppState.FAILED
            trade.error = "execution task cancelled: position may be open"
            finalize(trade)
            self._open_positions[trade.id] = pos
            log.error(
                "trade %s: task cancelled with %.8g %s open%s", trade.id, pos.base_open, trade.asset,
                " (order state unknown)" if pos.unresolved else "",
            )
        else:
            finalize(trade)
            round_trip = trade.exit_order is not None and float(trade.exit_order.filled) > _EPS
            trade.state = OppState.CLOSED if round_trip else OppState.CANCELLED
            trade.error = "execution task cancelled" + (" after the exit filled" if round_trip else "")
            if pos is not None:
                self._risk.register_close(trade.id, float(trade.realized_pnl_quote or 0.0))
        self._persist_trade(trade)
        try:
            await asyncio.wait_for(self._announce(opp, trade, trade.error or "cancelled"), _NOTE_TIMEOUT_S)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.debug("trade %s: cancel announcement skipped: %s", trade.id, exc)

    async def _announce(self, opp: Opportunity, trade: TradeRecord, reason: str) -> None:
        await self._transition(opp, trade.state, reason)
        await self._note(trade, reason)

    async def _run(
        self, opp: Opportunity, adapter: ExchangeAdapter | None, trade: TradeRecord, expected: ExecutionEconomics
    ) -> tuple[bool, float]:
        """The flow.  Returns (exposure registered, base still open)."""
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
        holder = _OpenPosition(adapter=adapter, trade=trade, base_open=0.0)
        self._in_flight[opp.id] = holder
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
        holder.pending, holder.pending_filled, holder.entry_unknown = entry, 0.0, True
        holder.base_open = float(entry_req.amount)  # worst case until the fill is confirmed
        await self._note(trade, f"entry {entry_req.side.value} {entry_req.amount:.8g} @ {entry_price:.8g} IOC sent ({entry.status.value})")
        entry = await self._wait_fill(adapter, entry, s.buy_fill_timeout_s, opp.id)
        if not entry.is_done:
            entry = await self._cancel(adapter, entry)
        trade.entry_order = entry
        self._persist_order(entry, trade.id)
        if not entry.is_done:
            # hard rule 5: unknown is unknown - never "not filled"
            detail = "no order id after timeout" if not entry.id else "cancel not confirmed"
            await self._fail(
                opp, adapter, trade, f"entry order state unknown ({detail}): position may be open",
                registered=registered, position_open=float(entry_req.amount), pending=entry, entry_unknown=True,
            )
            return registered, float(entry_req.amount)
        filled = float(entry.filled)
        holder.pending, holder.entry_unknown, holder.base_open = None, False, filled
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
                registered=registered, position_open=still_open, pending=self._unresolved([exit_order]),
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

        # ---- exit revalidation (hard rule 4, second half) ---------------
        entry_avg = float(entry.avg_price or entry_price)
        exit_target, pos_net = await self._fresh_exit_target(opp, adapter, trade, econ, entry_avg)
        if pos_net is not None:
            await self._note(trade, f"exit revalidated: target {exit_target:.8g}, net on position {pos_net:+.2f}%")
            if pos_net <= 0.0:
                await self._note(trade, "exit revalidation: edge gone - exiting at market")
                market = await self._exit_market(adapter, trade, position_open)
                return await self._close(opp, adapter, trade, [market], registered, position_open)

        # ---- exit: LIMIT GTC at the (fresh) exit_target --------------------
        exit_req = OrderRequest(
            exchange=trade.exchange,
            symbol=trade.symbol,
            side=Side.SELL if d > 0 else Side.BUY,
            amount=position_open,
            type=OrderType.LIMIT,
            price=float(exit_target),
            tif=TimeInForce.GTC,
            opportunity_id=opp.id,
        )
        exit_limit = await self._place(adapter, exit_req, trade_id=trade.id, reduce_only=self._is_swap(trade.exchange))
        orders: list[Order | None] = []
        if exit_limit.status in (OrderStatus.FAILED, OrderStatus.REJECTED):
            await self._note(trade, f"exit limit rejected ({exit_limit.error or exit_limit.status.value}): exiting at market")
            market = await self._exit_market(adapter, trade, position_open)
            return await self._close(opp, adapter, trade, [market], registered, position_open)
        trade.exit_order = exit_limit
        holder.pending, holder.pending_filled = exit_limit, float(exit_limit.filled)
        self._pending_exit[opp.id] = (adapter, exit_limit)
        await self._note(trade, f"exit {exit_req.side.value} {exit_req.amount:.8g} @ {exit_req.price:.8g} GTC sent ({exit_limit.status.value})")
        exit_limit = await self._wait_fill(adapter, exit_limit, s.sell_timeout_s, opp.id)
        if not exit_limit.is_done and not self._risk.emergency and opp.id not in self._abort and s.exit_on_timeout == "hold":
            entry_mono = entry.sent_mono if entry.sent_mono is not None else self._clock.mono()
            hold_left = float(s.max_hold_s) - (self._clock.mono() - entry_mono)
            if hold_left > 0:
                await self._note(trade, f"exit not filled after {s.sell_timeout_s:.0f} s - holding up to {hold_left:.0f} s more")
                exit_limit = await self._wait_fill(adapter, exit_limit, hold_left, opp.id)
        if not exit_limit.is_done:
            exit_limit = await self._cancel(adapter, exit_limit)
        orders.append(exit_limit)
        self._persist_order(exit_limit, trade.id)
        trade.exit_order = exit_limit
        self._pending_exit.pop(opp.id, None)
        remaining = max(0.0, position_open - float(exit_limit.filled))
        if not exit_limit.is_done:
            # the GTC may still rest - or fill - on the exchange: no second exit for the same quantity
            detail = "no order id after timeout" if not exit_limit.id else "cancel not confirmed"
            await self._fail(
                opp, adapter, trade, f"exit order state unknown ({detail}): {remaining:.8g} {trade.asset} left open",
                registered=registered, position_open=remaining, pending=exit_limit,
            )
            return registered, remaining
        holder.pending, holder.base_open = None, remaining
        if remaining > _EPS:
            if self._risk.emergency:
                combined = self._combine(orders)
                trade.exit_order = combined
                await self._fail(opp, adapter, trade, "emergency stop during exit: position left open", registered=registered, position_open=remaining)
                return registered, remaining
            why = f"cancel requested ({self._abort[opp.id]})" if opp.id in self._abort else "exit timeout"
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
        """Finalize after the exit leg; FAILED when part of the position is still
        open or the state of an exit order could not be confirmed."""
        pending = self._unresolved(exit_orders)
        combined = self._combine([o for o in exit_orders if o is not None and o is not pending])
        trade.exit_order = combined if combined is not None else pending
        for o in exit_orders:
            if o is not None and "+" not in (o.id or ""):
                self._persist_order(o, trade.id)
        exit_filled = sum(float(o.filled) for o in exit_orders if o is not None)
        still_open = max(0.0, position_open - exit_filled)
        if still_open > _EPS or pending is not None:
            why = f"exit incomplete: {still_open:.8g} {trade.asset} still open"
            if pending is not None:
                why += " (exit order state unknown)"
            await self._fail(opp, adapter, trade, why, registered=registered, position_open=still_open, pending=pending)
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
        reason = redact(reason)
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
        pending: Order | None = None,
        entry_unknown: bool = False,
    ) -> None:
        """FAILED.  With base open - or an order whose state is unknown - the
        position stays tracked (``open_positions``) and risk exposure stays
        registered until ``emergency_close_positions`` resolves it."""
        reason = redact(reason)
        trade.state = OppState.FAILED
        trade.error = reason
        trade.finished_ts = self._clock.wall()
        finalize(trade)
        pnl = float(trade.realized_pnl_quote or 0.0)
        unresolved = pending is not None and not pending.is_done
        if adapter is not None and (position_open > _EPS or unresolved):
            self._open_positions[trade.id] = _OpenPosition(
                adapter=adapter,
                trade=trade,
                base_open=max(0.0, position_open),
                pending=pending if unresolved else None,
                pending_filled=float(pending.filled) if unresolved and pending is not None else 0.0,
                entry_unknown=bool(entry_unknown and unresolved),
            )
            log.error(
                "trade %s FAILED with %.8g %s still open%s: %s", trade.id, position_open, trade.asset,
                " (order state unknown)" if unresolved else "", reason,
            )
        elif registered:
            self._risk.register_close(trade.id, pnl)
        self._record_stats(trade, success=False)
        await self._transition(opp, OppState.FAILED, reason)
        await self._note(trade, f"failed: {reason}")

    # ------------------------------------------------------------------ #
    # unknown orders
    # ------------------------------------------------------------------ #
    async def _resolve_pending(self, pos: _OpenPosition) -> Order | None:
        """Confirm the final state of ``pos.pending`` (fetch by id / client id,
        cancel it when it still rests) and fold its fills into the position.
        Returns the resolved order, or ``None`` when the exchange could not
        confirm it - then nothing is sent for this position (hard rule 5)."""
        order = pos.pending
        if order is None:
            return None
        adapter, trade = pos.adapter, pos.trade
        if not order.is_done:
            fresh, why = await self._fetch_state(adapter, order)
            now = self._clock.mono()
            if fresh is None:
                sent = order.sent_mono
                if why == "gone" and sent is not None and now - sent >= _GONE_GRACE_S:
                    # the exchange does not know the order: it can not fill any more
                    order.status = OrderStatus.CANCELLED
                    order.done_mono = now
                    order.error = (order.error + "; " if order.error else "") + "not found on the exchange"
                    fresh = order
                else:
                    log.error("trade %s: order %s on %s still unresolved (%s)", trade.id, order.id or order.client_id, adapter.name, why)
                    return None
            order = fresh
            if not order.is_done:
                try:
                    res = await adapter.cancel_order(order.id, order.symbol)
                    if res is not None:
                        order = self._merge_timing(order, res)
                    if not order.is_done:
                        order.status = OrderStatus.CANCELLED
                        order.done_mono = self._clock.mono()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    log.warning("trade %s: cancel of %s on %s failed: %s", trade.id, order.id, adapter.name, exc)
                    again, _why = await self._fetch_state(adapter, order)
                    if again is None or not again.is_done:
                        pos.pending = order
                        return None
                    order = again
        new_fills = max(0.0, float(order.filled) - pos.pending_filled)
        if pos.entry_unknown:
            trade.entry_order = order
            pos.base_open = float(order.filled)
        else:
            pos.base_open = max(0.0, pos.base_open - new_fills)
            if trade.exit_order is None or trade.exit_order.client_id == order.client_id:
                trade.exit_order = order
            else:
                trade.exit_order = self._combine([trade.exit_order, order])
        pos.pending, pos.pending_filled, pos.entry_unknown = None, 0.0, False
        self._persist_order(order, trade.id)
        await self._note(
            trade,
            f"order {order.client_id} resolved: {order.status.value}, filled {order.filled:.8g}; {pos.base_open:.8g} {trade.asset} open",
        )
        return order

    async def _emergency_cancel(self, adapter: ExchangeAdapter, order: Order) -> int:
        """Cancel a running trade's resting exit (the task itself reconciles the rest)."""
        try:
            if not order.id:
                fresh, _why = await self._fetch_state(adapter, order)
                if fresh is None or not fresh.id:
                    log.warning("emergency: order %s on %s has no known id yet; the running trade reconciles it", order.client_id, adapter.name)
                    return 0
                order = fresh
            if order.is_done:
                return 0
            await adapter.cancel_order(order.id, order.symbol)
            return 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("emergency: cancel of %s on %s failed: %s", order.id or order.client_id, adapter.name, exc)
            return 0

    # ------------------------------------------------------------------ #
    # emergency
    # ------------------------------------------------------------------ #
    async def emergency_stop(self, reason: str = "user") -> dict[str, Any]:
        """Block new AUTO trades, abort trades before entry, cancel pending
        orders - on the exec adapters of the CURRENT mode only (hard rule 8:
        a PAPER stop never reaches a LIVE account).  Open positions are NOT
        closed here (see ``emergency_close_positions``)."""
        await self._risk.set_emergency(True, reason)
        aborted: list[str] = []
        running = [t for t in self._tasks.values() if not t.done()]
        for opp_id, task in list(self._tasks.items()):
            if not task.done():
                self._abort[opp_id] = f"emergency stop: {reason}"
                aborted.append(opp_id)
        cancelled = 0
        touched: dict[int, ExchangeAdapter] = {}
        for _opp_id, (adapter, order) in list(self._pending_exit.items()):
            touched[id(adapter)] = adapter
            cancelled += await self._emergency_cancel(adapter, order)
        for pos in list(self._open_positions.values()):
            touched[id(pos.adapter)] = pos.adapter
            if pos.unresolved:
                resolved = await self._resolve_pending(pos)
                if resolved is not None and resolved.status is OrderStatus.CANCELLED:
                    cancelled += 1
        for adapter in self._adapters_of_mode().values():
            touched.setdefault(id(adapter), adapter)
        for adapter in touched.values():
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
            out.append({
                "trade_id": tid, "opp_id": pos.trade.opportunity_id, "exchange": pos.trade.exchange,
                "symbol": pos.trade.symbol, "base": pos.base_open, "source": "auto", "mode": pos.trade.mode.value,
                "unresolved": pos.unresolved, "pending_order": pos.pending.client_id if pos.pending is not None else None,
                "error": pos.trade.error,
            })
        for opp_id, trade in self._manual.items():
            base = trade.capital_quote / trade.actual_entry_price if trade.actual_entry_price else None
            out.append({
                "trade_id": trade.id, "opp_id": opp_id, "exchange": trade.exchange, "symbol": trade.symbol, "base": base,
                "source": "manual", "mode": trade.mode.value, "unresolved": False, "pending_order": None, "error": trade.error,
            })
        return out

    async def emergency_close_positions(self) -> list[TradeRecord]:
        """Market-exit every open position: tracked FAILED trades first (after
        resolving an order whose state is unknown - never a second order for a
        quantity that may already be gone), then whatever the adapters of the
        current mode still report (``positions``)."""
        closed: list[TradeRecord] = []
        for tid, pos in list(self._open_positions.items()):
            trade = pos.trade
            if pos.unresolved:
                pending = pos.pending
                resolved = await self._resolve_pending(pos)
                if resolved is None:
                    await self._note(
                        trade,
                        f"emergency close skipped: order {pending.client_id if pending else '?'} state unknown (exchange not answering) - retry",
                    )
                    continue
            if pos.base_open <= _EPS:
                # nothing is open: the unknown entry never filled, or the exit had filled after all
                self._open_positions.pop(tid, None)
                trade.finished_ts = self._clock.wall()
                finalize(trade)
                pnl = float(trade.realized_pnl_quote or 0.0)
                round_trip = trade.exit_order is not None and float(trade.exit_order.filled) > _EPS
                trade.state = OppState.CLOSED if round_trip else OppState.CANCELLED
                trade.error = (trade.error + "; " if trade.error else "") + ("closed: the exit had filled" if round_trip else "entry never filled")
                self._risk.register_close(trade.id, pnl)
                await self._note(trade, f"resolved without an open position: {trade.state.value} (realized {pnl:+.2f})")
                closed.append(trade)
                continue
            await self._note(trade, f"emergency close: exiting {pos.base_open:.8g} {trade.asset} at market")
            market = await self._exit_market(pos.adapter, trade, pos.base_open, max_rl_wait_s=_RL_MAX_EMERGENCY_WAIT_S)
            self._persist_order(market, trade.id)
            if not market.is_done:
                pos.pending, pos.pending_filled = market, float(market.filled)
                pos.base_open = max(0.0, pos.base_open - float(market.filled))
                await self._note(trade, f"emergency close: market order {market.client_id} state unknown - will be resolved first next time")
                continue
            trade.exit_order = self._combine([trade.exit_order, market])
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
                market = await self._exit_market(adapter, trade, amount, max_rl_wait_s=_RL_MAX_EMERGENCY_WAIT_S)
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
                    self._open_positions[trade.id] = _OpenPosition(
                        adapter=adapter, trade=trade, base_open=left, pending=self._unresolved([market]),
                        pending_filled=float(market.filled) if not market.is_done else 0.0,
                    )
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
    # process lifecycle: shutdown / restart
    # ------------------------------------------------------------------ #
    async def shutdown(self, timeout_s: float = 10.0) -> list[dict[str, Any]]:
        """Drain running trades before the process goes away: abort them (the
        flow cancels a resting exit and market-exits the remainder, exactly
        like ``cancel``), wait up to ``timeout_s``, cancel the rest (their
        positions stay tracked).  Returns the positions still open."""
        running = {oid: t for oid, t in self._tasks.items() if not t.done()}
        for oid in running:
            self._abort.setdefault(oid, "shutdown")
        if running:
            log.warning("shutdown: %d running trade(s) - aborting (market exit), waiting up to %.0f s", len(running), timeout_s)
            _done, pending = await asyncio.wait(list(running.values()), timeout=max(0.0, float(timeout_s)))
            if pending:
                log.error("shutdown: %d trade(s) did not finish in time - cancelling", len(pending))
                for t in pending:
                    t.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
        positions = self._open_position_list()
        if positions:
            log.error("shutdown with %d open position(s): %s", len(positions), positions)
        return positions

    def restore_from_db(self) -> list[TradeRecord]:
        """After a restart: trades the previous process left EXECUTING /
        PARTIALLY_FILLED / FILLED, or FAILED with base open, become tracked
        open positions (risk exposure registered, order state to be resolved
        by ``emergency_close_positions``).  Nothing is traded automatically."""
        if self._db is None:
            return []
        try:
            rows = self._db.list_trades(limit=500)
        except Exception:  # noqa: BLE001
            log.exception("restore: list_trades failed")
            return []
        now_ts = self._clock.wall()
        restored: list[TradeRecord] = []
        for row in rows:
            try:
                trade = trade_from_dict(row)
            except Exception as exc:  # noqa: BLE001
                log.warning("restore: trade row unreadable (%s): %s", exc, row.get("id") if isinstance(row, dict) else row)
                continue
            if trade.id in self._trades:
                continue
            in_flight = trade.state in EXECUTION_STATES
            if not in_flight and trade.state is not OppState.FAILED:
                continue
            if now_ts - float(trade.started_ts) > _RESTORE_MAX_AGE_S:
                if in_flight:
                    log.warning("restore: trade %s (%s) is %.0f h old - not restored; check the exchange by hand",
                                trade.id, trade.state.value, (now_ts - float(trade.started_ts)) / 3600.0)
                continue
            entry, exit_ = trade.entry_order, trade.exit_order
            if entry is None:
                if in_flight and trade.actual_entry_price and trade.opportunity_id and trade.opportunity_id not in self._manual:
                    # a MANUAL position the owner still holds
                    self._trades[trade.id] = trade
                    self._manual[trade.opportunity_id] = trade
                    self._risk.register_open(trade.id, trade.exchange, trade.asset, trade.capital_quote, trade.mode)
                    trade.events.append((now_ts, "manual position restored after restart - report the exit price when sold"))
                    self._persist_trade(trade)
                    restored.append(trade)
                    log.error("restore: MANUAL position %s on %s (%s %.2f) restored - report the exit price when sold",
                              trade.id, trade.exchange, trade.asset, trade.capital_quote)
                elif in_flight:
                    self._finish_restored(trade, OppState.FAILED, "process restarted before the entry was sent", now_ts)
                continue
            entry_filled = float(entry.filled)
            exit_filled = float(exit_.filled) if exit_ is not None else 0.0
            pending: Order | None = None
            entry_unknown = False
            if not entry.is_done:
                pending, entry_unknown, base_open = entry, True, float(entry.amount)
            else:
                base_open = max(0.0, entry_filled - exit_filled)
                if exit_ is not None and not exit_.is_done:
                    pending = exit_
            if base_open <= _EPS and pending is None:
                if in_flight:
                    if entry_filled > _EPS:
                        self._finish_restored(trade, OppState.CLOSED, "process restarted after the exit filled", now_ts)
                    else:
                        self._finish_restored(trade, OppState.CANCELLED, "process restarted: entry not filled", now_ts)
                continue
            adapter = self._exec_adapters.get(trade.mode, {}).get(trade.exchange)
            if adapter is None:
                log.error(
                    "restore: trade %s has %.8g %s open on %s (%s) but there is no %s execution adapter - NOT tracked; check the exchange by hand",
                    trade.id, base_open, trade.asset, trade.exchange, trade.state.value, trade.mode.value,
                )
                continue
            self._trades[trade.id] = trade
            self._risk.register_open(trade.id, trade.exchange, trade.asset, trade.capital_quote, trade.mode)
            self._open_positions[trade.id] = _OpenPosition(
                adapter=adapter,
                trade=trade,
                base_open=base_open,
                pending=pending,
                pending_filled=float(pending.filled) if pending is not None else 0.0,
                entry_unknown=entry_unknown,
            )
            if in_flight:
                trade.state = OppState.FAILED
                trade.error = "process restarted during execution: position left open"
                trade.finished_ts = now_ts
                finalize(trade)
            trade.events.append((
                now_ts,
                f"restored after restart: {base_open:.8g} {trade.asset} open" + (" (order state unknown)" if pending is not None else ""),
            ))
            self._persist_trade(trade)
            restored.append(trade)
            log.error(
                "restore: trade %s on %s has %.8g %s open%s (%s) - tracked; use EMERGENCY CLOSE or close it by hand",
                trade.id, trade.exchange, base_open, trade.asset, " with an unresolved order" if pending is not None else "",
                trade.mode.value,
            )
        self._restored.extend(restored)
        return restored

    def _finish_restored(self, trade: TradeRecord, state: OppState, why: str, now_ts: float) -> None:
        trade.state = state
        trade.error = (trade.error + "; " if trade.error else "") + why
        trade.finished_ts = now_ts
        finalize(trade)
        trade.events.append((now_ts, why))
        self._trades[trade.id] = trade
        self._persist_trade(trade)
        log.warning("restore: trade %s -> %s (%s)", trade.id, state.value, why)

    async def announce_open_positions(self) -> None:
        """TRADE_UPDATE for every position restored from the database (UI / Telegram)."""
        restored, self._restored = self._restored, []
        for trade in restored:
            pos = self._open_positions.get(trade.id)
            if pos is not None:
                msg = f"open position after restart: {pos.base_open:.8g} {trade.asset} on {trade.exchange}"
                msg += " (order state unknown)" if pos.unresolved else ""
                msg += " - EMERGENCY CLOSE exits it at market, or close it by hand"
            else:
                msg = f"manual position after restart: {trade.capital_quote:.2f} on {trade.exchange} - report the exit price when sold"
            await self._note(trade, msg)

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
