"""Backtest - replay recorded order books through the real detection stack.

The recorded JSONL (see ``recorder``) is replayed in time order through a
``SimClock`` and one ``FakeAdapter`` per exchange.  Every book goes through
``Pipeline.on_book`` (MarketState -> MoveDetector -> economics -> lights ->
OpportunityManager) exactly as live data would.  The bot "selects AUTO" for
every opportunity whose bot light is GREEN; ``settings.auto_confirm`` is
forced on.

Realism rules
-------------
* An opportunity seen at time ``t`` is executed at ``t + L`` where ``L`` is
  the simulated API latency (``latency_ms`` or ``settings.default_api_latency_ms``):
  the clock is advanced to ``t + L`` with every record up to that moment fed
  first, the opportunity is revalidated by the real ``ExecutionEngine.revalidate``
  (latest book, risk limits, balance) and the entry LIMIT IOC order fills
  against the target book as of ``t + L`` through ``PaperAdapter`` (fees and
  slippage from the real book walk).  A stale book at ``t + L`` or an expired
  opportunity means no trade.
* The exit is a resting LIMIT at ``exit_target`` that fills when a later book
  of the same market reaches it; after ``sell_timeout_s`` (or ``max_hold_s``
  when ``exit_on_timeout == "hold"``) the remainder is sold at MARKET on the
  latest book - the same policy as the live engine.
* Entries due after the last record are not attempted (no data to fill
  against); open trades are closed at timeout on the last book.
* Recordings carry no 24h volume, so the volume gates
  (``min_mover_volume_quote`` / ``min_target_volume_quote``) are disabled in
  the backtest copy of the settings; ``min_movers`` stays.
* REST confirmation of stale books is disabled (``confirm_with_rest_book``):
  a fake REST snapshot would present old data as fresh.

The live ``ExecutionEngine.execute`` waits for fills in real time, so the
backtest sequences the two legs itself (revalidate -> entry -> exit) using
the same adapters, risk manager, P&L finalisation and statistics.
"""
from __future__ import annotations

import asyncio
import dataclasses
import heapq
import inspect
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Callable

from ..clock import SimClock
from ..config import ExchangeConfig, Settings
from ..events import EventBus
from ..exchanges.base import ExchangeError
from ..links import split_symbol
from ..models import (
    ACTIVE_STATES,
    ExchangeRole,
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
    mean,
    median,
    new_id,
)
from .recorder import load_records

log = logging.getLogger(__name__)

_EPS = 1e-9
_MONO0 = 1000.0

DEP_NAMES: dict[str, tuple[str, str]] = {
    "MarketState": ("latbot.market.state", "MarketState"),
    "StatsStore": ("latbot.engine.stats", "StatsStore"),
    "MoveDetector": ("latbot.engine.mover", "MoveDetector"),
    "DurationEstimator": ("latbot.engine.duration", "DurationEstimator"),
    "OpportunityManager": ("latbot.engine.opportunity", "OpportunityManager"),
    "Pipeline": ("latbot.engine.pipeline", "Pipeline"),
    "FakeAdapter": ("latbot.exchanges.fake", "FakeAdapter"),
    "PaperAdapter": ("latbot.exchanges.paper", "PaperAdapter"),
    "RiskManager": ("latbot.execution.risk", "RiskManager"),
    "ExecutionEngine": ("latbot.execution.engine", "ExecutionEngine"),
    "finalize": ("latbot.execution.pnl", "finalize"),
}

REPORT_KEYS = (
    "records",
    "opportunities",
    "executed",
    "closed",
    "failed",
    "expected_net_mean",
    "realized_net_mean",
    "realized_net_median",
    "human_executable",
    "bot_executable",
    "false_signals",
    "by_asset",
    "by_exchange",
    "pnl_quote",
)


# ---------------------------------------------------------------------- #
# dependency resolution
# ---------------------------------------------------------------------- #
def _resolve_deps(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Lazily import the engine classes; ``overrides`` replace any of them."""
    deps: dict[str, Any] = dict(overrides or {})
    missing: list[str] = []
    for name, (module, attr) in DEP_NAMES.items():
        if name in deps and deps[name] is not None:
            continue
        try:
            mod = __import__(module, fromlist=[attr])
            deps[name] = getattr(mod, attr)
        except Exception as exc:  # noqa: BLE001
            if name == "finalize":
                deps[name] = None  # optional: local finalisation
                log.warning("pnl.finalize unavailable (%s); using local finalisation", exc)
            else:
                missing.append(f"{name} ({module})")
    if missing:
        raise RuntimeError("backtest dependencies missing: " + ", ".join(missing))
    return deps


def _accepts(func: Any, name: str) -> bool:
    try:
        sig = inspect.signature(func)
    except (TypeError, ValueError):
        return False
    for p in sig.parameters.values():
        if p.kind is inspect.Parameter.VAR_KEYWORD or p.name == name:
            return True
    return False


def _construct(cls: Any, *args: Any, **kwargs: Any) -> Any:
    """Instantiate ``cls`` passing only the keyword arguments it accepts."""
    clean = {k: v for k, v in kwargs.items() if _accepts(cls, k)}
    return cls(*args, **clean)


def _replay_paper_class(paper_cls: Any, latency_ms: float) -> Any:
    """PaperAdapter subclass without the real-time latency sleep: the replay
    models latency by filling at ``t + L``, so sleeping adds nothing."""
    if not hasattr(paper_cls, "_latency_s"):
        return paper_cls

    class ReplayPaperAdapter(paper_cls):  # type: ignore[misc,valid-type]
        _sim_latency_ms = float(latency_ms)

        def _latency_s(self) -> float:
            return 0.0

        def _mark_order_latency(self, ms: float) -> None:
            super()._mark_order_latency(self._sim_latency_ms)

    ReplayPaperAdapter.__name__ = "ReplayPaperAdapter"
    return ReplayPaperAdapter


def backtest_settings(settings: Settings, latency_ms: float | None = None) -> Settings:
    """A copy of ``settings`` with the backtest overrides applied."""
    bt = Settings.from_dict(json.loads(settings.to_json()))
    lat = float(latency_ms) if latency_ms is not None else float(settings.default_api_latency_ms)
    bt.update(
        {
            "auto_confirm": True,
            "paper_mode": True,
            "live_mode": False,
            "record_market_data": False,
            "ticker_poll_enabled": False,
            "watchlist_auto": False,
            "confirm_with_rest_book": False,
            "min_mover_volume_quote": 0.0,
            "min_target_volume_quote": 0.0,
            "default_api_latency_ms": max(0.0, lat),
        }
    )
    return bt


# ---------------------------------------------------------------------- #
# local finalisation (used only when execution.pnl is unavailable)
# ---------------------------------------------------------------------- #
def _order_price(order: Order | None) -> float | None:
    if order is None:
        return None
    if order.avg_price:
        return float(order.avg_price)
    if order.fills:
        amt = sum(f.amount for f in order.fills)
        return float(sum(f.price * f.amount for f in order.fills) / amt) if amt > 0 else None
    return None


def _local_finalize(trade: TradeRecord) -> TradeRecord:
    d = 1 if trade.direction > 0 else -1
    entry, exit_ = trade.entry_order, trade.exit_order
    entry_px, exit_px = _order_price(entry), _order_price(exit_)
    base = min(float(entry.filled) if entry else 0.0, float(exit_.filled) if exit_ else 0.0)
    fees = (float(entry.fee_cost_quote) if entry else 0.0) + (float(exit_.fee_cost_quote) if exit_ else 0.0)
    trade.actual_entry_price = entry_px
    trade.actual_exit_price = exit_px
    trade.actual_fees_quote = fees
    if base > _EPS and entry_px and exit_px:
        realized = d * (exit_px - entry_px) * base - fees
        trade.realized_pnl_quote = realized
        trade.actual_net_pct = realized / (entry_px * base) * 100.0
    else:
        trade.realized_pnl_quote = -fees
        trade.actual_net_pct = None
    if entry is not None and entry.sent_mono is not None and exit_ is not None and exit_.done_mono is not None:
        trade.execution_time_s = max(0.0, exit_.done_mono - entry.sent_mono)
    trade.diff_reason = f"expected {trade.expected_net_pct:+.2f}% -> actual {trade.actual_net_pct}"
    return trade


def _merge_orders(orders: list[Order]) -> Order | None:
    """Merge a (partially filled) limit exit and its market remainder."""
    real = [o for o in orders if o is not None]
    if not real:
        return None
    if len(real) == 1:
        return real[0]
    first = real[0]
    fills = [f for o in real for f in o.fills]
    filled = sum(float(o.filled) for o in real)
    notional = sum(float(o.avg_price or 0.0) * float(o.filled) for o in real)
    market_filled = sum(float(o.filled) for o in real if o.type is OrderType.MARKET)
    amount = max(float(first.amount), filled)
    monos = [o.sent_mono for o in real if o.sent_mono is not None]
    dones = [o.done_mono for o in real if o.done_mono is not None]
    status = OrderStatus.FILLED if filled >= amount - _EPS and filled > 0 else (OrderStatus.PARTIALLY_FILLED if filled > 0 else real[-1].status)
    return Order(
        id="+".join(o.id for o in real),
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
        fee_cost_quote=sum(float(o.fee_cost_quote) for o in real),
        sent_ts=first.sent_ts,
        sent_mono=min(monos) if monos else None,
        first_fill_mono=min((o.first_fill_mono for o in real if o.first_fill_mono is not None), default=None),
        done_mono=max(dones) if dones else None,
        raw={"orders": [o.id for o in real], "market_filled": market_filled},
    )


# ---------------------------------------------------------------------- #
# bookkeeping
# ---------------------------------------------------------------------- #
@dataclass
class OppSeen:
    """What the backtest observed about one opportunity."""

    id: str
    asset: str
    exchange: str
    direction: int
    first_mono: float
    net_first: float
    net_max: float
    human_ok: bool = False
    bot_ok: bool = False
    final_state: str = ""
    caught_up: bool | None = None
    duration_s: float | None = None


@dataclass
class _SimTrade:
    trade: TradeRecord
    adapter: Any
    opp_id: str
    base_open: float
    exit_target: float
    entry_mono: float
    deadline_mono: float
    hold_deadline_mono: float | None
    exit_limit: Order | None = None
    exits: list[Order] = field(default_factory=list)
    held: bool = False


class _Replay:
    """Owns the simulated AUTO trades of one backtest run."""

    def __init__(
        self,
        settings: Settings,
        clock: SimClock,
        opps: Any,
        execution: Any,
        risk: Any,
        stats: Any,
        paper: dict[str, Any],
        finalize: Callable[[TradeRecord], TradeRecord] | None,
        latency_s: float,
    ) -> None:
        self.s = settings
        self.clock = clock
        self.opps = opps
        self.execution = execution
        self.risk = risk
        self.stats = stats
        self.paper = paper
        self.finalize = finalize or _local_finalize
        self.L = latency_s
        self.seen: dict[str, OppSeen] = {}
        self.expected: dict[str, ExecutionEconomics] = {}
        self.queue: list[tuple[float, int, str]] = []
        self._seq = 0
        self.scheduled: set[str] = set()
        self.attempted: set[str] = set()
        self.open: dict[str, list[_SimTrade]] = {}  # (exchange|asset) -> trades
        self.trades: list[TradeRecord] = []
        self.rejected: dict[str, int] = {}
        self.exit_types: dict[str, str] = {}
        self.last_book: dict[str, OrderBook] = {}

    # ---- observation ---------------------------------------------------
    @staticmethod
    def _key(exchange: str, asset: str) -> str:
        return f"{exchange}|{asset}"

    def observe(self, opp: Opportunity, now: float) -> None:
        rec = self.seen.get(opp.id)
        if rec is None:
            rec = OppSeen(
                id=opp.id,
                asset=opp.asset,
                exchange=opp.target_exchange,
                direction=opp.direction,
                first_mono=now,
                net_first=float(opp.econ.net_pct),
                net_max=float(opp.econ.net_pct),
            )
            self.seen[opp.id] = rec
        rec.net_max = max(rec.net_max, float(opp.econ.net_pct))
        if opp.human.light is not Light.RED:
            rec.human_ok = True
        if opp.bot.light is not Light.RED:
            rec.bot_ok = True
        if (
            opp.bot.light is Light.GREEN
            and opp.state in ACTIVE_STATES
            and opp.id not in self.scheduled
            and opp.id not in self.attempted
            and opp.target_exchange in self.paper
        ):
            self.scheduled.add(opp.id)
            self.expected[opp.id] = dataclasses.replace(opp.econ)
            self._seq += 1
            heapq.heappush(self.queue, (now + self.L, self._seq, opp.id))
            log.debug("backtest: %s scheduled for entry at +%.3fs", opp.id, self.L)

    def finalize_seen(self) -> None:
        for rec in self.seen.values():
            opp = self.opps.get(rec.id) if hasattr(self.opps, "get") else None
            if opp is None:
                continue
            rec.final_state = opp.state.value
            rec.caught_up = opp.caught_up
            rec.duration_s = opp.actual_duration_s

    def _reject(self, reason: str) -> None:
        self.rejected[reason] = self.rejected.get(reason, 0) + 1

    # ---- scheduling ----------------------------------------------------
    def has_due(self, before_mono: float) -> bool:
        """True when an entry is due strictly before ``before_mono`` (records at
        exactly ``t + L`` are fed before the entry)."""
        return bool(self.queue) and self.queue[0][0] < before_mono

    async def run_due(self, upto_mono: float, t0_wall: float) -> None:
        """Enter every scheduled opportunity whose ``t + L`` is <= ``upto_mono``
        (the clock is set to the exact entry moment)."""
        while self.queue and self.queue[0][0] <= upto_mono:
            due, _, opp_id = heapq.heappop(self.queue)
            self.scheduled.discard(opp_id)
            self.attempted.add(opp_id)
            self.clock.set(wall=t0_wall + (due - _MONO0), mono=due)
            try:
                await self._enter(opp_id)
            except Exception:  # noqa: BLE001
                log.exception("backtest: entry of %s crashed", opp_id)
                self._reject("error")

    def reject_pending(self, reason: str) -> None:
        while self.queue:
            _, _, opp_id = heapq.heappop(self.queue)
            self.scheduled.discard(opp_id)
            self.attempted.add(opp_id)
            self._reject(reason)

    async def _transition(self, opp: Opportunity, state: OppState, reason: str) -> None:
        if opp.state is state:
            return
        try:
            await self.opps.transition(opp.id, state, reason)
        except Exception as exc:  # noqa: BLE001
            log.debug("backtest: transition %s -> %s refused: %s", opp.id, state.value, exc)

    # ---- entry ---------------------------------------------------------
    async def _enter(self, opp_id: str) -> None:
        s = self.s
        now = self.clock.mono()
        opp = self.opps.get(opp_id)
        if opp is None or opp.state not in ACTIVE_STATES:
            self._reject("expired_before_entry")
            return
        adapter = self.paper.get(opp.target_exchange)
        if adapter is None:
            self._reject("no_execution_adapter")
            return
        book = adapter.order_book(opp.target_symbol)
        if book is None or book.age_ms(now) > s.max_data_age_ms:
            self._reject("stale_book_at_entry")
            return
        ok, reason, econ = await self.execution.revalidate(opp)
        if not ok or econ is None:
            self._reject(_short_reason(reason))
            log.info("backtest: %s not executed at +%.0fms: %s", opp_id, self.L * 1000, reason)
            return
        if opp.state in (OppState.VALID, OppState.NOTIFIED):
            await self._transition(opp, OppState.AUTO_SELECTED, "backtest AUTO")
        if opp.state is not OppState.AUTO_SELECTED:
            self._reject("state_not_selectable")
            return
        expected = self.expected.get(opp_id) or dataclasses.replace(opp.econ)
        d = 1 if opp.direction > 0 else -1
        trade = TradeRecord(
            id=new_id("bt_"),
            opportunity_id=opp.id,
            mode=Mode.PAPER,
            exchange=opp.target_exchange,
            asset=opp.asset,
            symbol=opp.target_symbol,
            direction=d,
            capital_quote=float(econ.executable_quote),
            expected_net_pct=float(expected.net_pct),
            expected_profit_quote=float(econ.executable_quote) * float(expected.net_pct) / 100.0,
            expected_entry_price=float(expected.entry_vwap),
            expected_exit_price=float(expected.exit_target),
            state=OppState.EXECUTING,
            started_ts=self.clock.wall(),
        )
        self.trades.append(trade)
        await self._transition(opp, OppState.EXECUTING, "backtest entry")
        opp.execution_id = trade.id
        if _accepts(self.risk.register_open, "mode"):
            self.risk.register_open(trade.id, trade.exchange, trade.asset, trade.capital_quote, mode=Mode.PAPER)
        else:
            self.risk.register_open(trade.id, trade.exchange, trade.asset, trade.capital_quote)
        entry_price = float(econ.entry_vwap) * (1.0 + d * float(s.buy_limit_slippage_pct) / 100.0)
        req = OrderRequest(
            exchange=trade.exchange,
            symbol=trade.symbol,
            side=Side.BUY if d > 0 else Side.SELL,
            amount=float(econ.base_amount),
            type=OrderType.LIMIT,
            price=entry_price,
            tif=TimeInForce.IOC,
            opportunity_id=opp.id,
        )
        try:
            entry = await adapter.place_order(req)
        except ExchangeError as exc:
            await self._fail(opp, trade, f"entry rejected: {exc}", 0.0)
            return
        if entry.sent_mono is not None and (entry.ack_mono is None or entry.ack_mono <= entry.sent_mono):
            entry.ack_mono = entry.sent_mono + self.L  # the simulated round trip (modelled by the t + L shift)
        trade.entry_order = entry
        trade.events.append((self.clock.wall(), f"entry {req.side.value} {req.amount:.8g} @ {entry_price:.8g} IOC -> {entry.status.value}"))
        filled = float(entry.filled)
        if filled <= _EPS:
            await self._cancel(opp, trade, "entry not filled")
            return
        ratio = filled / req.amount if req.amount > 0 else 0.0
        if ratio + _EPS < float(s.min_partial_fill_ratio):
            market = await self._market_exit(adapter, trade, filled)
            trade.exit_order = market
            left = max(0.0, filled - float(market.filled))
            await self._fail(opp, trade, f"partial fill {ratio * 100:.0f}% below minimum: exited at market", left)
            return
        await self._transition(opp, OppState.FILLED if ratio >= 1.0 - 1e-6 else OppState.PARTIALLY_FILLED, f"entry filled {ratio * 100:.0f}%")
        hold = (now + s.effective_max_hold_s()) if s.exit_on_timeout == "hold" else None
        st = _SimTrade(
            trade=trade,
            adapter=adapter,
            opp_id=opp.id,
            base_open=filled,
            exit_target=float(econ.exit_target),
            entry_mono=now,
            deadline_mono=now + s.effective_sell_timeout_s(),
            hold_deadline_mono=hold,
        )
        exit_req = OrderRequest(
            exchange=trade.exchange,
            symbol=trade.symbol,
            side=Side.SELL if d > 0 else Side.BUY,
            amount=filled,
            type=OrderType.LIMIT,
            price=float(econ.exit_target),
            tif=TimeInForce.GTC,
            opportunity_id=opp.id,
        )
        try:
            kwargs: dict[str, Any] = {}
            if self._is_swap(trade.exchange) and _accepts(adapter.place_order, "reduce_only"):
                kwargs["reduce_only"] = True
            st.exit_limit = await adapter.place_order(exit_req, **kwargs)
        except ExchangeError as exc:
            log.warning("backtest: exit limit rejected for %s (%s); market exit", trade.id, exc)
            market = await self._market_exit(adapter, trade, filled)
            await self._close(st, [market], "market")
            return
        trade.events.append((self.clock.wall(), f"exit {exit_req.side.value} {filled:.8g} @ {exit_req.price:.8g} GTC -> {st.exit_limit.status.value}"))
        self.open.setdefault(self._key(trade.exchange, trade.asset), []).append(st)
        log.info(
            "backtest: %s entered %s %s @ %.8g (%.8g) target %.8g", trade.id, trade.asset, trade.exchange,
            entry.avg_price or entry_price, filled, st.exit_target,
        )
        if st.exit_limit.is_done:
            await self._progress(st)

    def _is_swap(self, exchange: str) -> bool:
        cfg = self.s.exchange(exchange)
        return bool(cfg is not None and cfg.market_type == "swap")

    # ---- exits ---------------------------------------------------------
    async def on_book(self, book: OrderBook) -> None:
        """Progress the resting exits of the market ``book`` belongs to."""
        asset = book.asset or split_symbol(book.symbol)[0]
        self.last_book[self._key(book.exchange, asset)] = book
        for st in list(self.open.get(self._key(book.exchange, asset), [])):
            try:
                if st.exit_limit is not None and not st.exit_limit.is_done:
                    st.exit_limit = await st.adapter.fetch_order(st.exit_limit.id, st.trade.symbol)
                await self._progress(st)
            except Exception:  # noqa: BLE001
                log.exception("backtest: exit refresh failed for %s", st.trade.id)

    async def _progress(self, st: _SimTrade) -> None:
        limit = st.exit_limit
        if limit is not None and limit.is_done and float(limit.filled) >= st.base_open - _EPS:
            await self._close(st, [limit], "limit")

    async def tick(self, now: float) -> None:
        """Apply the exit timeouts (sell_timeout_s / max_hold_s)."""
        for st in [t for lst in self.open.values() for t in lst]:
            try:
                await self._check_timeout(st, now)
            except Exception:  # noqa: BLE001
                log.exception("backtest: timeout handling failed for %s", st.trade.id)

    async def _check_timeout(self, st: _SimTrade, now: float) -> None:
        if now < st.deadline_mono:
            return
        if st.hold_deadline_mono is not None and now < st.hold_deadline_mono:
            if not st.held:
                st.held = True
                st.trade.events.append((self.clock.wall(), f"exit not filled after {self.s.effective_sell_timeout_s():.0f} s - holding"))
            return
        limit = st.exit_limit
        orders: list[Order] = []
        if limit is not None:
            if not limit.is_done:
                try:
                    limit = await st.adapter.cancel_order(limit.id, st.trade.symbol)
                except Exception as exc:  # noqa: BLE001
                    log.warning("backtest: cancel of %s failed: %s", limit.id, exc)
            st.exit_limit = limit
            orders.append(limit)
        remaining = max(0.0, st.base_open - (float(limit.filled) if limit is not None else 0.0))
        kind = "limit"
        if remaining > _EPS:
            st.trade.events.append((self.clock.wall(), f"exit timeout: {remaining:.8g} at market"))
            market = await self._market_exit(st.adapter, st.trade, remaining)
            orders.append(market)
            kind = "market" if float(limit.filled if limit else 0.0) <= _EPS else "mixed"
        await self._close(st, orders, kind)

    async def _market_exit(self, adapter: Any, trade: TradeRecord, amount: float) -> Order:
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
        kwargs: dict[str, Any] = {}
        if self._is_swap(trade.exchange) and _accepts(adapter.place_order, "reduce_only"):
            kwargs["reduce_only"] = True
        try:
            return await adapter.place_order(req, **kwargs)
        except ExchangeError as exc:
            log.error("backtest: market exit failed for %s: %s", trade.id, exc)
            return Order(
                id="",
                client_id=req.client_id,
                exchange=req.exchange,
                symbol=req.symbol,
                side=req.side,
                type=req.type,
                amount=amount,
                price=None,
                status=OrderStatus.FAILED,
                error=str(exc),
                sent_mono=self.clock.mono(),
                done_mono=self.clock.mono(),
            )

    # ---- finalisation --------------------------------------------------
    def _forget(self, st: _SimTrade) -> None:
        key = self._key(st.trade.exchange, st.trade.asset)
        lst = self.open.get(key)
        if lst and st in lst:
            lst.remove(st)
        if not lst:
            self.open.pop(key, None)

    async def _close(self, st: _SimTrade, exit_orders: list[Order], kind: str) -> None:
        self._forget(st)
        trade = st.trade
        combined = _merge_orders([o for o in exit_orders if o is not None])
        trade.exit_order = combined
        exit_filled = float(combined.filled) if combined is not None else 0.0
        still_open = max(0.0, st.base_open - exit_filled)
        if still_open > _EPS:
            opp = self.opps.get(st.opp_id)
            await self._fail(opp, trade, f"exit incomplete: {still_open:.8g} {trade.asset} still open", still_open)
            return
        trade.finished_ts = self.clock.wall()
        self.finalize(trade)
        trade.state = OppState.CLOSED
        self.exit_types[trade.id] = kind
        pnl = float(trade.realized_pnl_quote or 0.0)
        self.risk.register_close(trade.id, pnl)
        self._record_stats(trade, pnl > 0)
        opp = self.opps.get(st.opp_id)
        if opp is not None:
            await self._transition(opp, OppState.CLOSED, f"backtest: realized {pnl:+.2f}")
        trade.events.append((self.clock.wall(), f"closed ({kind}): realized {pnl:+.2f} ({trade.diff_reason})"))
        log.info("backtest: %s closed (%s) realized %+.4f net %s", trade.id, kind, pnl, trade.actual_net_pct)

    async def _cancel(self, opp: Opportunity, trade: TradeRecord, reason: str) -> None:
        trade.state = OppState.CANCELLED
        trade.error = reason
        trade.finished_ts = self.clock.wall()
        self.finalize(trade)
        self.risk.register_close(trade.id, float(trade.realized_pnl_quote or 0.0))
        await self._transition(opp, OppState.CANCELLED, reason)
        trade.events.append((self.clock.wall(), f"cancelled: {reason}"))

    async def _fail(self, opp: Opportunity | None, trade: TradeRecord, reason: str, position_open: float) -> None:
        trade.state = OppState.FAILED
        trade.error = reason
        trade.finished_ts = self.clock.wall()
        self.finalize(trade)
        self.risk.register_close(trade.id, float(trade.realized_pnl_quote or 0.0))
        self._record_stats(trade, False)
        if position_open > _EPS:
            log.error("backtest: %s FAILED with %.8g %s still open: %s", trade.id, position_open, trade.asset, reason)
        if opp is not None:
            await self._transition(opp, OppState.FAILED, reason)
        trade.events.append((self.clock.wall(), f"failed: {reason}"))

    def _record_stats(self, trade: TradeRecord, success: bool) -> None:
        if self.stats is None or not hasattr(self.stats, "record_trade"):
            return
        try:
            self.stats.record_trade(
                trade.asset, trade.exchange, success, trade.actual_net_pct, trade.actual_slippage_pct, trade.entry_latency_ms
            )
        except Exception:  # noqa: BLE001
            log.exception("backtest: stats.record_trade failed")

    def open_count(self) -> int:
        return sum(len(v) for v in self.open.values())

    def next_deadline(self) -> float | None:
        deadlines = []
        for lst in self.open.values():
            for st in lst:
                deadlines.append(st.hold_deadline_mono if st.hold_deadline_mono is not None else st.deadline_mono)
        return min(deadlines) if deadlines else None


def _short_reason(reason: str) -> str:
    r = (reason or "").lower()
    if "stale" in r:
        return "stale_book"
    if "net" in r and "below" in r:
        return "net_decayed"
    if "liquidity" in r:
        return "liquidity"
    if "red" in r:
        return "bot_red"
    if "balance" in r:
        return "balance"
    if "risk" in r or "limit" in r or "simultaneous" in r or "capital" in r or "emergency" in r:
        return "risk"
    return "revalidation_failed"


# ---------------------------------------------------------------------- #
# report
# ---------------------------------------------------------------------- #
def _group(
    seen: list[OppSeen], trades: list[TradeRecord], key_opp: Callable[[OppSeen], str], key_trade: Callable[[TradeRecord], str]
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    keys = sorted({key_opp(o) for o in seen} | {key_trade(t) for t in trades})
    for k in keys:
        opps_k = [o for o in seen if key_opp(o) == k]
        trades_k = [t for t in trades if key_trade(t) == k]
        closed = [t for t in trades_k if t.state is OppState.CLOSED]
        out[k] = {
            "opportunities": len(opps_k),
            "executed": len(trades_k),
            "closed": len(closed),
            "failed": len(trades_k) - len(closed),
            "expected_net_mean": mean([float(t.expected_net_pct) for t in trades_k]),
            "realized_net_mean": mean([float(t.actual_net_pct) for t in closed if t.actual_net_pct is not None]),
            "pnl_quote": sum(float(t.realized_pnl_quote or 0.0) for t in trades_k),
            "human_executable": sum(1 for o in opps_k if o.human_ok),
            "bot_executable": sum(1 for o in opps_k if o.bot_ok),
        }
    return out


def summarize(
    seen: list[OppSeen],
    trades: list[TradeRecord],
    *,
    records: int = 0,
    span_s: float = 0.0,
    exchanges: list[str] | None = None,
    assets: list[str] | None = None,
    latency_ms: float = 0.0,
    rejected: dict[str, int] | None = None,
    false_signals: int | None = None,
    exit_types: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Pure report builder (documented keys + extras)."""
    closed = [t for t in trades if t.state is OppState.CLOSED]
    realized = [float(t.actual_net_pct) for t in closed if t.actual_net_pct is not None]
    if false_signals is None:
        false_signals = sum(1 for o in seen if o.final_state == OppState.EXPIRED.value and o.caught_up is False)
    exit_types = exit_types or {}
    trade_rows = [
        {
            "id": t.id,
            "opportunity_id": t.opportunity_id,
            "asset": t.asset,
            "exchange": t.exchange,
            "side": "LONG" if t.direction > 0 else "SHORT",
            "state": t.state.value,
            "capital_quote": t.capital_quote,
            "expected_net_pct": t.expected_net_pct,
            "actual_net_pct": t.actual_net_pct,
            "realized_pnl_quote": t.realized_pnl_quote,
            "entry_price": t.actual_entry_price,
            "exit_price": t.actual_exit_price,
            "exit_type": exit_types.get(t.id),
            "execution_time_s": t.execution_time_s,
            "error": t.error,
            "diff_reason": t.diff_reason,
        }
        for t in trades
    ]
    return {
        "records": int(records),
        "span_s": float(span_s),
        "exchanges": list(exchanges or []),
        "assets": list(assets or []),
        "latency_ms": float(latency_ms),
        "opportunities": len(seen),
        "executed": len(trades),
        "closed": len(closed),
        "failed": len(trades) - len(closed),
        "rejected": dict(rejected or {}),
        "expected_net_mean": mean([float(t.expected_net_pct) for t in trades]),
        "expected_net_median": median([float(t.expected_net_pct) for t in trades]),
        "opp_net_mean": mean([o.net_first for o in seen]),
        "opp_net_max_mean": mean([o.net_max for o in seen]),
        "realized_net_mean": mean(realized),
        "realized_net_median": median(realized),
        "human_executable": sum(1 for o in seen if o.human_ok),
        "bot_executable": sum(1 for o in seen if o.bot_ok),
        "false_signals": int(false_signals),
        "expired": sum(1 for o in seen if o.final_state == OppState.EXPIRED.value),
        "by_asset": _group(seen, trades, lambda o: o.asset, lambda t: t.asset),
        "by_exchange": _group(seen, trades, lambda o: o.exchange, lambda t: t.exchange),
        "pnl_quote": sum(float(t.realized_pnl_quote or 0.0) for t in trades),
        "trades": trade_rows,
    }


def _empty_report(latency_ms: float) -> dict[str, Any]:
    return summarize([], [], latency_ms=latency_ms)


# ---------------------------------------------------------------------- #
# the run
# ---------------------------------------------------------------------- #
async def run_backtest_async(
    paths: list[str],
    settings: Settings,
    *,
    latency_ms: float | None = None,
    deps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Replay ``paths`` (JSONL / .gz) and return the report (see module doc)."""
    records = load_records(paths)
    bt = backtest_settings(settings, latency_ms)
    lat_ms = float(bt.default_api_latency_ms)
    if not records:
        log.warning("backtest: no records in %s", paths)
        return _empty_report(lat_ms)
    d = _resolve_deps(deps)
    L = lat_ms / 1000.0
    t0 = float(records[0]["t"])
    t_last = float(records[-1]["t"])
    clock = SimClock(wall=t0, mono=_MONO0)
    bus = EventBus()

    # exchanges / symbols present in the data
    symbols: dict[str, dict[str, str]] = {}
    for rec in records:
        ex = str(rec["ex"])
        asset = str(rec.get("asset") or split_symbol(str(rec.get("symbol", "")))[0]).upper()
        symbols.setdefault(ex, {}).setdefault(asset, str(rec.get("symbol") or f"{asset}/USDT"))
    for ex in symbols:
        if bt.exchange(ex) is None:
            log.warning("backtest: exchange %s is not configured; treated as a SIGNAL source", ex)
            bt.exchanges.append(ExchangeConfig(name=ex, role=ExchangeRole.SIGNAL, stream="bbo"))
    adapters: dict[str, Any] = {}
    for ex, syms in symbols.items():
        cfg = bt.exchange(ex)
        adapters[ex] = _construct(d["FakeAdapter"], cfg, bus, clock, syms)
        start = getattr(adapters[ex], "start", None)
        if start is not None:
            await start(list(syms.values()))

    state = d["MarketState"](bt, clock)
    stats = _construct(d["StatsStore"], bt, db=None)
    detector = _construct(d["MoveDetector"], bt, state, stats, clock, bus=bus)
    opps = _construct(d["OpportunityManager"], bt, bus, clock, db=None, stats=stats)
    duration = d["DurationEstimator"](bt, stats)
    pipeline = _construct(d["Pipeline"], bt, bus, clock, state, detector, stats, opps, adapters, duration, db=None)
    paper_cls = _replay_paper_class(d["PaperAdapter"], lat_ms)
    paper: dict[str, Any] = {}
    exec_names = set(bt.execution_exchanges())
    for ex, inner in adapters.items():
        if ex not in exec_names:
            continue
        first_symbol = next(iter(symbols[ex].values()), "")
        quote = split_symbol(first_symbol)[1] or "USDT"
        paper[ex] = _construct(
            paper_cls,
            inner,
            bt,
            bus,
            clock,
            db=None,
            initial_quote=float(bt.paper_balance_quote),
            quote_asset=quote,
            seed=0,
            quote_assets=list(bt.quote_assets),
        )
    risk = _construct(d["RiskManager"], bt, None, clock, bus=bus)
    execution = d["ExecutionEngine"](bt, bus, clock, pipeline, opps, {Mode.PAPER: paper}, risk, None, stats)
    replay = _Replay(bt, clock, opps, execution, risk, stats, paper, d.get("finalize"), L)
    log.info(
        "backtest: %d records, %d exchange(s) (%d execution), %d asset(s), span %.1f s, latency %.0f ms",
        len(records), len(adapters), len(paper), len({a for s in symbols.values() for a in s}), t_last - t0, lat_ms,
    )

    for rec in records:
        t = float(rec["t"])
        mono = _MONO0 + (t - t0)
        if replay.has_due(mono):
            await replay.run_due(mono, t0)
        clock.set(wall=t, mono=mono)
        ex = str(rec["ex"])
        asset = str(rec.get("asset") or split_symbol(str(rec.get("symbol", "")))[0]).upper()
        try:
            book = await adapters[ex].push_book(asset, _pairs(rec.get("b")), _pairs(rec.get("a")), exchange_ts=rec.get("ets"))
        except Exception:  # noqa: BLE001
            log.exception("backtest: bad record at t=%s on %s skipped", t, ex)
            continue
        try:
            new_opps = await pipeline.on_book(book)
        except Exception:  # noqa: BLE001
            log.exception("backtest: pipeline failed at t=%s", t)
            new_opps = []
        await replay.on_book(book)
        await replay.tick(mono)
        for opp in new_opps:
            replay.observe(opp, mono)
        for opp in opps.active():
            replay.observe(opp, mono)
        try:
            await opps.sweep(mono)
        except Exception:  # noqa: BLE001
            log.exception("backtest: sweep failed at t=%s", t)
        for adapter in adapters.values():
            check = getattr(adapter, "check_staleness", None)
            if check is not None:
                check(mono, bt.stale_exchange_s)

    # ---- after the data: entries due inside the data, no more afterwards,
    # then time out the open trades on the last books -----------------------
    end_mono = _MONO0 + (t_last - t0)
    await replay.run_due(end_mono, t0)
    replay.reject_pending("data_ended")
    while replay.open_count():
        deadline = replay.next_deadline()
        if deadline is None:
            break
        end_mono = max(end_mono, deadline)
        clock.set(wall=t0 + (end_mono - _MONO0), mono=end_mono)
        await replay.tick(end_mono)
        if replay.open_count() and replay.next_deadline() == deadline:
            log.error("backtest: %d trade(s) could not be closed", replay.open_count())
            break
    try:
        await opps.sweep(end_mono + 2.0 * bt.move_window_s + 1.0)
    except Exception:  # noqa: BLE001
        log.exception("backtest: final sweep failed")
    replay.finalize_seen()

    false_signals: int | None = None
    try:
        rows = stats.summary()
        false_signals = sum(int(r.get("false_signal_count", 0) or 0) for r in rows)
    except Exception:  # noqa: BLE001
        log.debug("backtest: stats summary unavailable", exc_info=True)
    report = summarize(
        list(replay.seen.values()),
        replay.trades,
        records=len(records),
        span_s=t_last - t0,
        exchanges=sorted(adapters),
        assets=sorted({a for s in symbols.values() for a in s}),
        latency_ms=lat_ms,
        rejected=replay.rejected,
        false_signals=false_signals,
        exit_types=replay.exit_types,
    )
    report["volume_gate"] = False
    log.info(
        "backtest done: %d opportunities, %d executed, %d closed, pnl %+.2f",
        report["opportunities"], report["executed"], report["closed"], report["pnl_quote"],
    )
    return report


def _pairs(levels: Any) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    for lv in levels or []:
        try:
            p, s = float(lv[0]), float(lv[1])
        except (TypeError, ValueError, IndexError):
            continue
        if p > 0 and s > 0:
            out.append((p, s))
    return out


def run_backtest(
    paths: list[str],
    settings: Settings,
    *,
    latency_ms: float | None = None,
    deps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Synchronous wrapper around :func:`run_backtest_async`."""
    return asyncio.run(run_backtest_async(paths, settings, latency_ms=latency_ms, deps=deps))


# ---------------------------------------------------------------------- #
# formatting (CLI)
# ---------------------------------------------------------------------- #
def _pct(v: float | None) -> str:
    return "n/a" if v is None else f"{v:+.2f}%"


def fmt_report(report: dict[str, Any]) -> str:
    """Compact text table of a report."""
    lines = [
        f"latbot backtest: {report.get('records', 0)} records, {len(report.get('exchanges', []))} exchange(s), "
        f"{len(report.get('assets', []))} asset(s), span {report.get('span_s', 0.0):.0f} s, latency {report.get('latency_ms', 0.0):.0f} ms",
        f"{'opportunities':<20}{report.get('opportunities', 0)}",
        f"{'executed':<20}{report.get('executed', 0)}",
        f"{'closed':<20}{report.get('closed', 0)}",
        f"{'failed':<20}{report.get('failed', 0)}",
        f"{'expected NET mean':<20}{_pct(report.get('expected_net_mean'))}",
        f"{'realized NET mean':<20}{_pct(report.get('realized_net_mean'))}",
        f"{'realized NET median':<20}{_pct(report.get('realized_net_median'))}",
        f"{'human executable':<20}{report.get('human_executable', 0)}",
        f"{'bot executable':<20}{report.get('bot_executable', 0)}",
        f"{'false signals':<20}{report.get('false_signals', 0)}",
        f"{'P&L (quote)':<20}{report.get('pnl_quote', 0.0):+.2f}",
    ]
    rejected = report.get("rejected") or {}
    if rejected:
        lines.append("rejected: " + ", ".join(f"{k} {v}" for k, v in sorted(rejected.items())))
    for title, key in (("by asset", "by_asset"), ("by exchange", "by_exchange")):
        rows = report.get(key) or {}
        if not rows:
            continue
        lines.append(f"{title}:")
        lines.append(f"  {'name':<12}{'opps':>6}{'exec':>6}{'closed':>8}{'exp NET':>10}{'real NET':>10}{'pnl':>10}")
        for name, r in rows.items():
            lines.append(
                f"  {name:<12}{r.get('opportunities', 0):>6}{r.get('executed', 0):>6}{r.get('closed', 0):>8}"
                f"{_pct(r.get('expected_net_mean')):>10}{_pct(r.get('realized_net_mean')):>10}{r.get('pnl_quote', 0.0):>+10.2f}"
            )
    return "\n".join(lines)
