"""Offline tests for latbot.execution.engine.ExecutionEngine.

Everything is stubbed locally: a scripted ``StubAdapter``, ``StubPipeline``
(returns an opportunity with the next economics), ``StubOpps`` (enforces
``models.TRANSITIONS``) and ``StubStats``.  ``RiskManager`` and
``Database(":memory:")`` are the real ones.
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
from dataclasses import dataclass, field
from typing import Any, Iterable

import pytest

from latbot import events
from latbot.clock import SimClock
from latbot.config import ExchangeConfig, Settings
from latbot.events import EventBus
from latbot.exchanges.base import ExchangeAdapter, ExchangeError, InsufficientBalance, OrderRejected, RateLimited
from latbot.execution.engine import ExecutionEngine
from latbot.execution.risk import RiskManager
from latbot.logging_setup import register_secret
from latbot.models import (
    TRANSITIONS,
    Balance,
    BookLevel,
    ExchangeRole,
    ExecAssessment,
    ExecutionEconomics,
    Fees,
    Fill,
    LagSignal,
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
    Ticker,
    TimeInForce,
    Timestamps,
    TradeRecord,
    new_id,
)
from latbot.storage.db import Database

T0 = 1_700_000_000.0
M0 = 1000.0


# --------------------------------------------------------------------------- #
# stubs
# --------------------------------------------------------------------------- #
class StubAdapter(ExchangeAdapter):
    """Scripted execution adapter.

    * entry (LIMIT IOC): fills ``entry_fill_ratio`` of the amount at
      ``entry_fill_price`` (default: the limit price); remainder CANCELLED.
    * exit (LIMIT GTC): ``exit_mode`` = "immediate" | "never" | "after_polls"
      (fills after ``exit_after_polls`` fetch_order calls) | "partial_then_never"
      (50 % filled, rest rests forever).
    * MARKET: fills fully at ``market_price`` (default: limit of the exit).
    """

    def __init__(self, cfg: ExchangeConfig, bus: EventBus, clock: SimClock, *, symbols: dict[str, str],
                 entry_fill_ratio: float = 1.0, entry_fill_price: float | None = None, exit_mode: str = "immediate",
                 exit_after_polls: int = 2, market_price: float | None = None, balances: dict[str, float] | None = None,
                 taker: float = 0.001, latency_s: float = 0.05) -> None:
        super().__init__(cfg, bus, clock)
        self.health.ws_connected = True
        self.health.last_msg_mono = clock.mono()
        self._symbols = dict(symbols)
        self.entry_fill_ratio = entry_fill_ratio
        self.entry_fill_price = entry_fill_price
        self.exit_mode = exit_mode
        self.exit_after_polls = exit_after_polls
        self.market_price = market_price
        self.taker = taker
        self.latency_s = latency_s
        self.balances: dict[str, Balance] = {a: Balance(a, v, v) for a, v in (balances or {"USDT": 1000.0}).items()}
        self.books: dict[str, OrderBook] = {}
        self.rest_fail = False
        self.rest_calls = 0
        self.reject_next = False
        self.orders: dict[str, Order] = {}
        self.calls: list[tuple[Any, ...]] = []
        self.positions: dict[str, dict[str, Any]] = {}
        self._polls: dict[str, int] = {}
        self._n = 0

    # ---- lifecycle / market data -----------------------------------------
    async def start(self, symbols: Iterable[str]) -> None:
        return None

    async def stop(self) -> None:
        return None

    def supported_symbols(self) -> set[str]:
        return set(self._symbols.values())

    def symbol_for(self, asset: str) -> str | None:
        return self._symbols.get(asset)

    def order_book(self, symbol: str) -> OrderBook | None:
        return self.books.get(symbol)

    async def fetch_order_book(self, symbol: str, limit: int = 20) -> OrderBook:
        self.rest_calls += 1
        if self.rest_fail:
            raise OrderRejected("REST unavailable")
        book = make_book(symbol, self.clock, recv_mono=self.clock.mono(), exchange=self.name)
        self.books[symbol] = book
        return book

    async def fetch_ticker(self, symbol: str) -> Ticker:
        return Ticker(exchange=self.name, symbol=symbol, bid=99.95, ask=100.05, last=100.0, ts=Timestamps(T0, M0))

    async def fetch_balances(self) -> dict[str, Balance]:
        self.calls.append(("balances",))
        return dict(self.balances)

    async def fees(self, symbol: str) -> Fees:
        return Fees(maker=self.taker, taker=self.taker)

    # ---- trading -----------------------------------------------------------
    def _fill(self, order: Order, amount: float, price: float) -> None:
        fee = amount * price * self.taker
        order.fills.append(Fill(order_id=order.id, price=price, amount=amount, fee_cost=fee, fee_asset="USDT", ts=self.clock.wall()))
        total = order.filled + amount
        order.avg_price = (sum(f.price * f.amount for f in order.fills)) / total
        order.filled = total
        order.fee_cost_quote += fee
        if order.first_fill_mono is None:
            order.first_fill_mono = self.clock.mono()
        if order.filled >= order.amount - 1e-12:
            order.status = OrderStatus.FILLED
            order.done_mono = self.clock.mono()
        else:
            order.status = OrderStatus.PARTIALLY_FILLED

    async def place_order(self, req: OrderRequest, *, reduce_only: bool = False) -> Order:
        return await self._place(req, reduce_only)

    async def _place(self, req: OrderRequest, reduce_only: bool) -> Order:
        self.calls.append(("place", req, reduce_only))
        if self.reject_next:
            self.reject_next = False
            raise OrderRejected("rejected by stub")
        if req.side == Side.BUY and req.type == OrderType.LIMIT and req.price:
            # like a real venue (and the SimBroker): a limit buy needs notional + taker fee free
            quote = req.symbol.split("/")[1].split(":")[0]
            free = self.balances[quote].free if quote in self.balances else 0.0
            need = float(req.price) * float(req.amount) * (1.0 + self.taker)
            if need > free + 1e-9:
                raise InsufficientBalance(f"need {need:.8g} {quote}, free {free:.8g}")
        self._n += 1
        order = Order(id=f"{self.name}-o{self._n}", client_id=req.client_id, exchange=self.name, symbol=req.symbol,
                      side=req.side, type=req.type, amount=req.amount, price=req.price, status=OrderStatus.ACKNOWLEDGED,
                      sent_mono=self.clock.mono(), sent_ts=self.clock.wall(), ack_mono=self.clock.mono() + self.latency_s)
        self.orders[order.id] = order
        self._polls[order.id] = 0
        if req.type == OrderType.MARKET:
            price = self.market_price if self.market_price is not None else (req.price or 100.0)
            self._fill(order, req.amount, price)
        elif req.tif == TimeInForce.IOC:
            amount = req.amount * self.entry_fill_ratio
            if amount > 0:
                self._fill(order, amount, self.entry_fill_price if self.entry_fill_price is not None else float(req.price))
            if order.status != OrderStatus.FILLED:
                order.status = OrderStatus.CANCELLED
                order.done_mono = self.clock.mono()
        else:  # GTC exit
            if self.exit_mode == "immediate":
                self._fill(order, req.amount, float(req.price))
            elif self.exit_mode == "partial_then_never":
                self._fill(order, req.amount * 0.5, float(req.price))
        return order

    async def fetch_order(self, order_id: str, symbol: str) -> Order:
        self.calls.append(("fetch", order_id))
        order = self.orders[order_id]
        self._polls[order_id] = self._polls.get(order_id, 0) + 1
        if (order.type == OrderType.LIMIT and order.status in (OrderStatus.ACKNOWLEDGED, OrderStatus.PARTIALLY_FILLED)
                and self.exit_mode == "after_polls" and self._polls[order_id] >= self.exit_after_polls):
            self._fill(order, order.remaining, float(order.price))
        return order

    async def cancel_order(self, order_id: str, symbol: str) -> Order:
        self.calls.append(("cancel", order_id))
        order = self.orders[order_id]
        if not order.is_done:
            order.status = OrderStatus.CANCELLED
            order.done_mono = self.clock.mono()
        return order

    async def cancel_all(self, symbol: str | None = None) -> int:
        self.calls.append(("cancel_all",))
        n = 0
        for o in self.orders.values():
            if not o.is_done:
                o.status = OrderStatus.CANCELLED
                o.done_mono = self.clock.mono()
                n += 1
        return n

    # ---- helpers for assertions ----------------------------------------------
    def placed(self) -> list[OrderRequest]:
        return [c[1] for c in self.calls if c[0] == "place"]

    def reduce_only_flags(self) -> list[bool]:
        return [c[2] for c in self.calls if c[0] == "place"]


class NoReduceOnlyAdapter(StubAdapter):
    """place_order without the reduce_only kwarg (older adapter)."""

    async def place_order(self, req: OrderRequest) -> Order:  # type: ignore[override]
        return await self._place(req, False)


class StubPipeline:
    def __init__(self, next_econ: ExecutionEconomics | None = None, fail: bool = False,
                 bot: ExecAssessment | None = None, on_refresh: Any = None) -> None:
        self.next_econ = next_econ
        self.fail = fail
        self.bot = bot
        self.on_refresh = on_refresh
        self.return_none = False
        self.calls: list[tuple[str, float | None]] = []

    async def refresh(self, opp: Opportunity, capital_quote: float | None = None) -> Opportunity | None:
        self.calls.append((opp.id, capital_quote))
        if self.fail:
            raise RuntimeError("no book")
        if self.return_none:
            return None
        if self.next_econ is not None:
            opp.econ = self.next_econ
        if self.bot is not None:
            opp.bot = self.bot
        if self.on_refresh is not None:
            await self.on_refresh(opp)
        return opp


class StubOpps:
    def __init__(self, bus: EventBus, clock: SimClock) -> None:
        self._bus = bus
        self._clock = clock
        self._opps: dict[str, Opportunity] = {}
        self.transitions: list[tuple[str, OppState, OppState, str]] = []

    def add(self, opp: Opportunity) -> Opportunity:
        self._opps[opp.id] = opp
        return opp

    def get(self, opp_id: str) -> Opportunity | None:
        return self._opps.get(opp_id)

    async def transition(self, opp_id: str, new_state: OppState, reason: str = "") -> Opportunity:
        opp = self._opps[opp_id]
        if opp.state == new_state:
            return opp
        if new_state not in TRANSITIONS[opp.state]:
            raise ValueError(f"invalid transition {opp.state.value} -> {new_state.value}")
        old = opp.state
        opp.state = new_state
        opp.state_history.append((self._clock.wall(), new_state.value, reason))
        self.transitions.append((opp_id, old, new_state, reason))
        await self._bus.publish(events.OPP_STATE, {"opportunity": opp, "from": old.value, "to": new_state.value, "reason": reason})
        return opp

    def states(self, opp_id: str) -> list[OppState]:
        return [t[2] for t in self.transitions if t[0] == opp_id]


class StubStats:
    def __init__(self) -> None:
        self.trades: list[tuple[Any, ...]] = []

    def record_trade(self, asset, exchange, success, realized_pct, slippage_pct, exec_latency_ms) -> None:
        self.trades.append((asset, exchange, success, realized_pct, slippage_pct, exec_latency_ms))


# --------------------------------------------------------------------------- #
# fixtures
# --------------------------------------------------------------------------- #
def make_settings(**over: Any) -> Settings:
    base: dict[str, Any] = dict(
        exchanges=[
            ExchangeConfig(name="testex", role=ExchangeRole.EXECUTION, market_type="spot", taker_fee=0.001, maker_fee=0.001),
            ExchangeConfig(name="swapex", role=ExchangeRole.EXECUTION, market_type="swap", allow_short=True, taker_fee=0.0006, maker_fee=0.0002),
        ],
        total_capital_quote=1000.0,
        max_risk_per_trade_pct=10.0,
        max_trade_quote=200.0,
        max_simultaneous_trades=2,
        max_daily_loss_quote=50.0,
        capital_quote=100.0,
        min_trade_quote=10.0,
        buy_fill_timeout_s=0.05,
        sell_timeout_s=0.05,
        max_hold_s=0.15,
        auto_confirm=False,
        human_execution_time_s=7.0,
    )
    base.update(over)
    return Settings(**base)


def make_book(symbol: str, clock: SimClock, recv_mono: float | None = None, exchange: str = "testex", mid: float = 100.0) -> OrderBook:
    rm = clock.mono() if recv_mono is None else recv_mono
    return OrderBook(exchange=exchange, symbol=symbol, bids=[BookLevel(mid - 0.05, 10.0)], asks=[BookLevel(mid + 0.05, 10.0)],
                     ts=Timestamps(recv_ts=clock.wall(), recv_mono=rm), asset=symbol.split("/")[0])


def make_econ(net: float = 4.8, direction: int = 1, capital: float = 100.0, entry: float = 100.0, liquidity_ok: bool = True,
              book_age_ms: float = 50.0) -> ExecutionEconomics:
    total_cost = 0.25
    gross = net + total_cost
    exit_target = entry * (1 + direction * gross / 100.0)
    return ExecutionEconomics(direction=direction, capital_quote=capital, executable_quote=capital, base_amount=capital / entry,
                              entry_best=entry, entry_vwap=entry, exit_best=entry, exit_vwap_now=entry, exit_target=exit_target,
                              spread_pct=0.1, entry_fee_pct=0.1, exit_fee_pct=0.1, entry_slippage_pct=0.02, exit_slippage_pct=0.03,
                              execution_loss_pct=0.05, gross_pct=gross, total_cost_pct=total_cost, net_pct=net,
                              net_quote=capital * net / 100.0, liquidity_ok=liquidity_ok, liquidity_reason="" if liquidity_ok else "book too thin",
                              book_age_ms=book_age_ms)


def make_assess(light: Light = Light.GREEN, est: float = 1.0, margin: float = 20.0) -> ExecAssessment:
    label = {Light.GREEN: "HIGH", Light.YELLOW: "MEDIUM", Light.RED: "NOT EXECUTABLE"}[light]
    return ExecAssessment(light=light, label=label, est_execution_s=est, margin_s=margin, probability=0.8, reasons=["stub"])


def make_opp(clock: SimClock, *, exchange: str = "testex", symbol: str = "BTC/USDT", asset: str = "BTC", direction: int = 1,
             net: float = 4.8, capital: float = 100.0, state: OppState = OppState.VALID, duration_s: float = 30.0,
             human: Light = Light.GREEN, bot: Light = Light.GREEN) -> Opportunity:
    sig = LagSignal(asset=asset, direction=direction, first_mover="binance", first_move_pct=5.0, first_move_start_mono=clock.mono() - 2,
                    reacted={"binance": 5.0}, target=exchange, target_symbol=symbol, target_move_pct=0.2, lag_pct=4.8,
                    expected_catchup_pct=5.05, catchup_ratio=1.0, detected_mono=clock.mono(), detected_ts=clock.wall(),
                    movers_count=2, target_quote=symbol.split("/")[1].split(":")[0])
    return Opportunity(id=new_id("opp_"), asset=asset, target_exchange=exchange, target_symbol=symbol, first_mover="binance",
                       direction=direction, mode=Mode.PAPER, state=state, signal=sig, econ=make_econ(net, direction, capital),
                       human=make_assess(human, est=7.0), bot=make_assess(bot), why="stub", detected_ts=clock.wall(),
                       first_seen_mono=clock.mono(), last_valid_mono=clock.mono(), last_update_mono=clock.mono(),
                       estimated_duration_s=duration_s, quote=symbol.split("/")[1].split(":")[0])


@dataclass
class Env:
    settings: Settings
    bus: EventBus
    clock: SimClock
    db: Database
    risk: RiskManager
    opps: StubOpps
    pipeline: StubPipeline
    stats: StubStats
    adapters: dict[str, StubAdapter]
    engine: ExecutionEngine
    events: list[tuple[str, Any]] = field(default_factory=list)

    def trade_updates(self) -> list[TradeRecord]:
        return [p for t, p in self.events if t == events.TRADE_UPDATE]

    def adapter(self, name: str = "testex") -> StubAdapter:
        return self.adapters[name]


def make_env(settings: Settings | None = None, *, pipeline: StubPipeline | None = None, adapter_cls: type = StubAdapter,
             mode: Mode = Mode.PAPER, **adapter_kwargs: Any) -> Env:
    settings = settings or make_settings()
    bus = EventBus()
    clock = SimClock(wall=T0, mono=M0)
    db = Database(":memory:", clock=clock)
    risk = RiskManager(settings, db, clock, bus)
    opps = StubOpps(bus, clock)
    pipeline = pipeline or StubPipeline()
    stats = StubStats()
    adapters: dict[str, StubAdapter] = {}
    for cfg in settings.exchanges:
        kw = dict(adapter_kwargs)
        symbols = {"BTC": "BTC/USDT"} if cfg.market_type == "spot" else {"BTC": "BTC/USDT:USDT"}
        a = adapter_cls(cfg, bus, clock, symbols=symbols, **kw)
        for sym in symbols.values():
            a.books[sym] = make_book(sym, clock, exchange=cfg.name)
        adapters[cfg.name] = a
    engine = ExecutionEngine(settings, bus, clock, pipeline, opps, {mode: adapters}, risk, db, stats)  # type: ignore[arg-type]
    engine.poll_interval_s = 0.001
    env = Env(settings, bus, clock, db, risk, opps, pipeline, stats, adapters, engine)
    bus.subscribe_all(lambda topic, payload: env.events.append((topic, payload)))
    return env


async def run_auto(env: Env, opp: Opportunity) -> tuple[dict[str, Any], TradeRecord]:
    sel = await env.engine.select_auto(opp.id)
    assert sel.get("confirmation_required") is True, sel
    res = await env.engine.confirm_auto(opp.id)
    trade = env.engine.trade(res["trade_id"]) if res.get("trade_id") else None
    assert trade is not None, res
    return res, trade


async def wait_until(pred: Any, timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not pred():
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.001)


# --------------------------------------------------------------------------- #
# revalidation
# --------------------------------------------------------------------------- #
async def test_revalidate_cancels_when_net_dropped() -> None:
    env = make_env(pipeline=StubPipeline(next_econ=make_econ(net=1.2)))
    opp = env.opps.add(make_opp(env.clock, net=4.8))
    res, trade = await run_auto(env, opp)
    assert res["ok"] is False
    assert res["reason"] == "Current net +1.20% is below the required 2.40% (expected +4.80%)"
    assert trade.state == OppState.CANCELLED
    assert trade.error == res["reason"]
    assert opp.state == OppState.CANCELLED
    assert env.opps.states(opp.id) == [OppState.AUTO_SELECTED, OppState.CANCELLED]
    assert env.adapter().placed() == []  # nothing sent
    assert env.risk.open_trades() == 0
    assert trade.expected_net_pct == 4.8  # what the user saw is kept
    assert env.db.list_trades()[0]["state"] == "CANCELLED"


async def test_revalidate_rejects_stale_book_when_rest_fails() -> None:
    env = make_env()
    a = env.adapter()
    a.books["BTC/USDT"] = make_book("BTC/USDT", env.clock, recv_mono=env.clock.mono() - 2.4)
    a.rest_fail = True
    opp = env.opps.add(make_opp(env.clock))
    ok, reason, econ = await env.engine.revalidate(opp)
    assert ok is False and econ is None
    assert reason == "Order book is stale (2400 ms > 1500 ms)"
    assert a.rest_calls == 1
    res, trade = await run_auto(env, opp)
    assert trade.state == OppState.CANCELLED and "stale" in trade.error
    assert opp.state == OppState.CANCELLED


async def test_revalidate_refreshes_stale_book_via_rest_and_publishes_it() -> None:
    env = make_env()
    a = env.adapter()
    a.books["BTC/USDT"] = make_book("BTC/USDT", env.clock, recv_mono=env.clock.mono() - 5.0)
    opp = env.opps.add(make_opp(env.clock))
    ok, reason, econ = await env.engine.revalidate(opp)
    assert ok is True and econ is not None and reason == "OK"
    assert a.rest_calls == 1
    assert any(t == events.BOOK for t, _ in env.events)


async def test_revalidate_missing_book_and_no_adapter_and_unusable_health() -> None:
    env = make_env()
    a = env.adapter()
    a.books.clear()
    a.rest_fail = True
    opp = env.opps.add(make_opp(env.clock))
    ok, reason, _ = await env.engine.revalidate(opp)
    assert not ok and reason == "No order book for BTC/USDT on testex"
    a.health.ws_connected = False
    ok, reason, _ = await env.engine.revalidate(opp)
    assert not ok and reason.startswith("Exchange testex is not usable")
    other = env.opps.add(make_opp(env.clock, exchange="nowhere"))
    ok, reason, _ = await env.engine.revalidate(other)
    assert not ok and reason == "No paper execution adapter for nowhere"


async def test_revalidate_rejects_liquidity_red_bot_and_refresh_failure() -> None:
    env = make_env(pipeline=StubPipeline(next_econ=make_econ(liquidity_ok=False)))
    opp = env.opps.add(make_opp(env.clock))
    ok, reason, _ = await env.engine.revalidate(opp)
    assert not ok and reason == "Insufficient liquidity: book too thin"

    env = make_env(pipeline=StubPipeline(bot=make_assess(Light.RED)))
    opp = env.opps.add(make_opp(env.clock))
    ok, reason, _ = await env.engine.revalidate(opp)
    assert not ok and reason.startswith("Bot light is RED")

    env = make_env(pipeline=StubPipeline(fail=True))
    opp = env.opps.add(make_opp(env.clock))
    ok, reason, _ = await env.engine.revalidate(opp)
    assert not ok and reason == "Revalidation failed: no book"


# --------------------------------------------------------------------------- #
# AUTO flows
# --------------------------------------------------------------------------- #
async def test_auto_happy_path() -> None:
    env = make_env(entry_fill_price=100.1)
    opp = env.opps.add(make_opp(env.clock, net=4.8))
    sel = await env.engine.select_auto(opp.id)
    assert sel["ok"] is True and sel["confirmation_required"] is True
    snap = sel["snapshot"]
    assert snap["valid"] is True
    assert snap["net_now_pct"] == 4.8 and snap["expected_net_pct"] == 4.8
    assert snap["capital"] == 100.0 and snap["expected_profit_quote"] == pytest.approx(4.8)
    assert snap["risk"] == {"ok": True, "reason": "OK", "allowed": 100.0}
    assert snap["balance"] == {"quote": "USDT", "free": 1000.0}
    assert snap["side"] == "LONG" and snap["exchange"] == "testex" and snap["symbol"] == "BTC/USDT"
    assert snap["entry_price"] == 100.0 and snap["exit_target"] == pytest.approx(105.05)
    assert opp.state == OppState.AUTO_SELECTED

    res = await env.engine.confirm_auto(opp.id)
    assert res["ok"] is True and res["state"] == "CLOSED"
    trade = env.engine.trades()[0]
    assert trade.id == res["trade_id"] and res["trade"]["state"] == "CLOSED"
    assert trade.state == OppState.CLOSED
    assert opp.state == OppState.CLOSED and opp.execution_id == trade.id
    assert env.opps.states(opp.id) == [OppState.AUTO_SELECTED, OppState.EXECUTING, OppState.FILLED, OppState.CLOSED]
    # orders
    a = env.adapter()
    reqs = a.placed()
    assert len(reqs) == 2
    entry, exit_ = reqs
    assert entry.side == Side.BUY and entry.type == OrderType.LIMIT and entry.tif == TimeInForce.IOC
    assert entry.price == pytest.approx(100.0 * 1.002) and entry.amount == pytest.approx(1.0)
    assert entry.opportunity_id == opp.id
    assert exit_.side == Side.SELL and exit_.type == OrderType.LIMIT and exit_.tif == TimeInForce.GTC
    assert exit_.price == pytest.approx(105.05) and exit_.amount == pytest.approx(1.0)
    assert entry.client_id != exit_.client_id
    assert a.reduce_only_flags() == [False, False]
    # P&L
    assert trade.entry_order.status == OrderStatus.FILLED and trade.exit_order.status == OrderStatus.FILLED
    assert trade.actual_entry_price == pytest.approx(100.1)
    assert trade.actual_exit_price == pytest.approx(105.05)
    fees = 100.1 * 0.001 + 105.05 * 0.001
    assert trade.actual_fees_quote == pytest.approx(fees)
    assert trade.realized_pnl_quote == pytest.approx(105.05 - 100.1 - fees)
    assert trade.realized_pnl_quote > 0
    assert trade.actual_net_pct == pytest.approx(trade.realized_pnl_quote / 100.1 * 100)
    assert trade.diff_reason.startswith("expected +4.80% -> actual +")
    assert "slippage +0.10%" in trade.diff_reason and "latency 50 ms" in trade.diff_reason
    assert trade.finished_ts is not None and trade.execution_time_s is not None
    assert trade.entry_latency_ms == pytest.approx(50.0)
    # bookkeeping
    assert env.risk.open_trades() == 0 and env.risk.exposure() == 0.0
    assert env.risk.daily_pnl(Mode.PAPER) == pytest.approx(trade.realized_pnl_quote)
    assert env.stats.trades == [("BTC", "testex", True, trade.actual_net_pct, trade.actual_slippage_pct, trade.entry_latency_ms)]
    rows = env.db.list_trades(mode="paper")
    assert len(rows) == 1 and rows[0]["state"] == "CLOSED" and rows[0]["realized_pnl_quote"] == pytest.approx(trade.realized_pnl_quote)
    orders = env.db.list_orders(trade.id)
    assert {o["id"] for o in orders} == {trade.entry_order.id, trade.exit_order.id}
    updates = env.trade_updates()
    assert len(updates) >= 4 and all(u.id == trade.id for u in updates)
    assert updates[-1].state == OppState.CLOSED
    assert any(t == events.ORDER_UPDATE for t, _ in env.events)
    assert len(trade.events) >= 4


async def test_select_auto_with_auto_confirm_executes_immediately() -> None:
    env = make_env(make_settings(auto_confirm=True))
    opp = env.opps.add(make_opp(env.clock))
    res = await env.engine.select_auto(opp.id)
    assert "confirmation_required" not in res
    assert res["ok"] is True and res["state"] == "CLOSED"
    assert opp.state == OppState.CLOSED


async def test_select_auto_with_auto_confirm_can_start_in_background() -> None:
    """``wait=False`` (what the HTTP route passes) must not hold the caller for the
    whole round trip: the trade runs as a tracked task and finishes on its own."""
    env = make_env(make_settings(auto_confirm=True))
    opp = env.opps.add(make_opp(env.clock))
    res = await env.engine.select_auto(opp.id, wait=False)
    assert "confirmation_required" not in res
    assert res["ok"] is True and res["started"] is True and res["state"] == "EXECUTING"
    assert opp.id in env.engine._tasks and not opp.is_terminal  # scheduled as a tracked task, caller not held
    await wait_until(lambda: opp.state == OppState.CLOSED and opp.id not in env.engine._tasks)
    trade = env.engine.trades()[0]
    assert trade.state == OppState.CLOSED and opp.execution_id == trade.id
    assert env.risk.open_trades() == 0


async def test_select_auto_stale_book_snapshot_marks_risk_unchecked() -> None:
    """Revalidation that fails before ``RiskManager.check`` leaves ``risk.ok`` None
    (not checked) - never True.  The UI must not paint that green."""
    env = make_env()
    a = env.adapter()
    a.books["BTC/USDT"] = make_book("BTC/USDT", env.clock, recv_mono=env.clock.mono() - 2.4)
    a.rest_fail = True
    opp = env.opps.add(make_opp(env.clock))
    sel = await env.engine.select_auto(opp.id)
    assert sel["ok"] is True and sel["confirmation_required"] is True
    snap = sel["snapshot"]
    assert snap["valid"] is False and "stale" in snap["reason"]
    assert snap["risk"]["ok"] is None and snap["risk"]["allowed"] is None
    assert snap["balance"]["free"] is None


async def test_no_fill_cancelled() -> None:
    env = make_env(entry_fill_ratio=0.0)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    assert res["ok"] is False
    assert trade.state == OppState.CANCELLED
    assert trade.error == "entry not filled (CANCELLED)"
    assert len(env.adapter().placed()) == 1
    assert trade.exit_order is None
    assert trade.realized_pnl_quote == 0.0
    assert "no fill" in trade.diff_reason
    assert opp.state == OppState.CANCELLED
    assert env.opps.states(opp.id) == [OppState.AUTO_SELECTED, OppState.EXECUTING, OppState.CANCELLED]
    assert env.risk.open_trades() == 0
    assert env.stats.trades == []


async def test_partial_fill_below_min_ratio_failed_with_market_exit() -> None:
    env = make_env(entry_fill_ratio=0.1, market_price=99.5)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    assert res["ok"] is False
    assert trade.state == OppState.FAILED
    assert trade.error == "partial fill 10% below minimum 20%: exited at market"
    reqs = env.adapter().placed()
    assert len(reqs) == 2
    assert reqs[1].type == OrderType.MARKET and reqs[1].side == Side.SELL and reqs[1].amount == pytest.approx(0.1)
    assert trade.exit_order.type == OrderType.MARKET and trade.exit_order.filled == pytest.approx(0.1)
    assert trade.realized_pnl_quote == pytest.approx((99.5 - 100.2) * 0.1 - (100.2 * 0.1 + 99.5 * 0.1) * 0.001)
    assert "partial fill 10%" in trade.diff_reason and "exit at market" in trade.diff_reason
    assert opp.state == OppState.FAILED
    assert env.risk.open_trades() == 0
    assert env.engine.open_positions() == []
    assert env.stats.trades[0][2] is False


async def test_partial_fill_above_min_ratio_continues() -> None:
    env = make_env(entry_fill_ratio=0.5)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    assert res["ok"] is True and trade.state == OppState.CLOSED
    assert env.opps.states(opp.id) == [OppState.AUTO_SELECTED, OppState.EXECUTING, OppState.PARTIALLY_FILLED, OppState.CLOSED]
    reqs = env.adapter().placed()
    assert reqs[1].amount == pytest.approx(0.5) and reqs[1].type == OrderType.LIMIT
    assert "partial fill 50%" in trade.diff_reason


async def test_exit_timeout_market_exit() -> None:
    env = make_env(exit_mode="never", market_price=103.0)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    assert res["ok"] is True and trade.state == OppState.CLOSED
    a = env.adapter()
    reqs = a.placed()
    assert [r.type for r in reqs] == [OrderType.LIMIT, OrderType.LIMIT, OrderType.MARKET]
    limit_id = next(c[1] for c in a.calls if c[0] == "place" and c[1].tif == TimeInForce.GTC).client_id
    assert any(c[0] == "cancel" for c in a.calls)
    assert a.orders[next(o.id for o in a.orders.values() if o.client_id == limit_id)].status == OrderStatus.CANCELLED
    assert trade.exit_order.type == OrderType.MARKET
    assert trade.exit_order.filled == pytest.approx(1.0) and trade.exit_order.avg_price == pytest.approx(103.0)
    assert trade.exit_order.raw["market_filled"] == pytest.approx(1.0)
    assert "exit at market" in trade.diff_reason and "catch-up short by" in trade.diff_reason
    assert trade.realized_pnl_quote == pytest.approx((103.0 - 100.2) - (100.2 + 103.0) * 0.001)
    assert any("exit timeout" in m for _, m in trade.events)


async def test_exit_partial_limit_then_market_combined() -> None:
    env = make_env(exit_mode="partial_then_never", market_price=104.0)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    assert trade.state == OppState.CLOSED
    reqs = env.adapter().placed()
    assert reqs[2].type == OrderType.MARKET and reqs[2].amount == pytest.approx(0.5)
    x = trade.exit_order
    assert x.filled == pytest.approx(1.0) and x.status == OrderStatus.FILLED
    assert x.avg_price == pytest.approx((105.05 * 0.5 + 104.0 * 0.5))
    assert x.raw["market_filled"] == pytest.approx(0.5) and len(x.raw["orders"]) == 2
    assert len(x.fills) == 2
    assert "exit 50% at market" in trade.diff_reason


async def test_exit_hold_then_market_after_max_hold() -> None:
    env = make_env(make_settings(exit_on_timeout="hold", sell_timeout_s=0.02, max_hold_s=0.08), exit_mode="never")
    opp = env.opps.add(make_opp(env.clock))
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    res, trade = await run_auto(env, opp)
    elapsed = loop.time() - t0
    assert trade.state == OppState.CLOSED
    assert elapsed >= 0.08
    assert any("holding" in m for _, m in trade.events)
    assert env.adapter().placed()[-1].type == OrderType.MARKET


async def test_exit_filled_after_polls() -> None:
    env = make_env(exit_mode="after_polls", exit_after_polls=3)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    assert trade.state == OppState.CLOSED
    assert [r.type for r in env.adapter().placed()] == [OrderType.LIMIT, OrderType.LIMIT]
    assert trade.exit_order.status == OrderStatus.FILLED and trade.exit_order.avg_price == pytest.approx(105.05)
    assert trade.exit_order.first_fill_mono is not None and trade.exit_order.done_mono is not None


async def test_entry_rejected_failed() -> None:
    env = make_env()
    env.adapter().reject_next = True
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    assert trade.state == OppState.FAILED
    assert trade.error.startswith("entry order failed: rejected by stub")
    assert trade.entry_order.status == OrderStatus.FAILED
    assert opp.state == OppState.FAILED
    assert env.risk.open_trades() == 0


async def test_swap_short_uses_reduce_only_when_supported() -> None:
    env = make_env(entry_fill_price=100.0, market_price=95.0)
    opp = env.opps.add(make_opp(env.clock, exchange="swapex", symbol="BTC/USDT:USDT", direction=-1, net=4.8))
    res, trade = await run_auto(env, opp)
    assert trade.state == OppState.CLOSED, res
    a = env.adapter("swapex")
    reqs = a.placed()
    assert reqs[0].side == Side.SELL and reqs[0].price == pytest.approx(100.0 * (1 - 0.002))
    assert reqs[1].side == Side.BUY and reqs[1].price == pytest.approx(100.0 * (1 - 0.0505))
    assert a.reduce_only_flags() == [False, True]
    assert trade.realized_pnl_quote > 0
    assert trade.direction == -1


async def test_swap_short_without_reduce_only_param() -> None:
    env = make_env(adapter_cls=NoReduceOnlyAdapter, exit_mode="never", market_price=96.0)
    opp = env.opps.add(make_opp(env.clock, exchange="swapex", symbol="BTC/USDT:USDT", direction=-1))
    res, trade = await run_auto(env, opp)
    assert trade.state == OppState.CLOSED, res
    a = env.adapter("swapex")
    assert a.reduce_only_flags() == [False, False, False]  # kwarg never passed
    assert a.placed()[-1].type == OrderType.MARKET and a.placed()[-1].side == Side.BUY


# --------------------------------------------------------------------------- #
# emergency
# --------------------------------------------------------------------------- #
async def test_emergency_blocks_new_execution() -> None:
    env = make_env()
    res = await env.engine.emergency_stop("test")
    assert res["emergency"] is True and env.risk.emergency is True
    assert any(t == events.EMERGENCY for t, _ in env.events)
    opp = env.opps.add(make_opp(env.clock))
    sel = await env.engine.select_auto(opp.id)
    assert sel["ok"] is False and sel["reason"] == "Emergency stop is active: test"
    assert opp.state == OppState.VALID
    # direct execute() is refused too (revalidate fails first)
    opp2 = env.opps.add(make_opp(env.clock, state=OppState.AUTO_SELECTED))
    trade = await env.engine.execute(opp2)
    assert trade.state == OppState.CANCELLED and "Emergency stop" in trade.error
    assert env.adapter().placed() == []
    await env.engine.resume()
    assert env.risk.emergency is False
    sel = await env.engine.select_auto(opp.id)
    assert sel["ok"] is True and sel["confirmation_required"] is True


async def test_emergency_cancels_pending_exit_then_close_positions() -> None:
    env = make_env(exit_mode="never", market_price=101.0)
    opp = env.opps.add(make_opp(env.clock))
    env.settings.sell_timeout_s = 5.0  # long enough for us to interrupt
    start = await env.engine.confirm_auto(opp.id, wait=False)
    assert start["ok"] is True and start["started"] is True
    await wait_until(lambda: opp.id in env.engine._pending_exit)
    dup = await env.engine.confirm_auto(opp.id)
    assert dup["ok"] is False and "already in progress" in dup["reason"]
    res = await env.engine.emergency_stop("panic")
    assert res["emergency"] is True and res["cancelled_orders"] >= 1 and res["aborted"] == [opp.id]
    await wait_until(lambda: opp.id not in env.engine._active)
    trade = env.engine.trades()[0]
    assert trade.state == OppState.FAILED and "emergency stop" in trade.error
    assert opp.state == OppState.FAILED
    a = env.adapter()
    assert any(c[0] == "cancel" for c in a.calls)
    assert [r.type for r in a.placed()] == [OrderType.LIMIT, OrderType.LIMIT]  # no market exit yet
    assert env.risk.open_trades() == 1  # exposure kept while the position is open
    positions = env.engine.open_positions()
    assert len(positions) == 1 and positions[0]["trade_id"] == trade.id and positions[0]["base"] == pytest.approx(1.0)
    closed = await env.engine.emergency_close_positions()
    assert [t.id for t in closed] == [trade.id]
    assert trade.state == OppState.CLOSED
    assert a.placed()[-1].type == OrderType.MARKET and a.placed()[-1].amount == pytest.approx(1.0)
    assert trade.exit_order.filled == pytest.approx(1.0) and trade.exit_order.raw["market_filled"] == pytest.approx(1.0)
    assert trade.realized_pnl_quote == pytest.approx((101.0 - 100.2) - (100.2 + 101.0) * 0.001)
    assert env.risk.open_trades() == 0
    assert env.engine.open_positions() == []
    assert env.db.get_trade(trade.id)["state"] == "CLOSED"
    await env.engine.resume()
    assert env.engine._abort == {}


async def test_emergency_close_untracked_adapter_positions() -> None:
    env = make_env(market_price=98.0)
    a = env.adapter("swapex")
    a.positions["BTC/USDT:USDT"] = {"side": "long", "amount": 0.5, "entry": 100.0}
    closed = await env.engine.emergency_close_positions()
    assert len(closed) == 1
    t = closed[0]
    assert t.state == OppState.CLOSED and t.exchange == "swapex" and t.symbol == "BTC/USDT:USDT" and t.direction == 1
    assert t.asset == "BTC" and t.capital_quote == pytest.approx(50.0)
    req = a.placed()[-1]
    assert req.type == OrderType.MARKET and req.side == Side.SELL and req.amount == pytest.approx(0.5)
    assert a.reduce_only_flags() == [True]
    assert t.actual_entry_price == 100.0 and t.actual_exit_price == 98.0
    assert t.realized_pnl_quote == pytest.approx((98.0 - 100.0) * 0.5 - 98.0 * 0.5 * 0.001)  # stub taker 0.1 %
    assert env.engine.trades()[0] is t
    # nothing on the spot adapter
    assert env.adapter("testex").placed() == []


# --------------------------------------------------------------------------- #
# risk limits / balance
# --------------------------------------------------------------------------- #
async def test_daily_loss_limit_blocks_execution() -> None:
    env = make_env()
    lost = TradeRecord(id="tr_old", opportunity_id="x", mode=Mode.PAPER, exchange="testex", asset="BTC", symbol="BTC/USDT",
                       direction=1, capital_quote=100.0, expected_net_pct=1.0, expected_profit_quote=1.0,
                       expected_entry_price=100.0, expected_exit_price=101.0, state=OppState.CLOSED,
                       realized_pnl_quote=-60.0, started_ts=T0 - 100, finished_ts=T0 - 50)
    env.db.save_trade(lost)
    opp = env.opps.add(make_opp(env.clock))
    sel = await env.engine.select_auto(opp.id)
    assert sel["snapshot"]["valid"] is False
    assert sel["snapshot"]["reason"] == "Daily loss limit reached: -60.00 <= -50.00 (paper)"
    assert sel["snapshot"]["risk"]["ok"] is False
    res = await env.engine.confirm_auto(opp.id)
    assert res["ok"] is False and "Daily loss limit" in res["reason"]
    assert env.adapter().placed() == []


async def test_max_simultaneous_trades_blocks() -> None:
    env = make_env()
    env.risk.register_open("a", "testex", "ETH", 10.0)
    env.risk.register_open("b", "testex", "SOL", 10.0)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    assert trade.state == OppState.CANCELLED
    assert trade.error == "Too many simultaneous trades (2 of 2 open)"


async def test_exposure_limit_downsizes_and_blocks() -> None:
    env = make_env(make_settings(max_exposure_per_asset_quote=150.0))
    env.risk.register_open("a", "other", "BTC", 100.0)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    assert trade.state == OppState.CLOSED
    assert trade.capital_quote == pytest.approx(50.0)
    assert env.adapter().placed()[0].amount == pytest.approx(0.5)
    assert any("asset exposure" in m for _, m in trade.events)


async def test_risk_downsizes_to_max_trade_quote() -> None:
    env = make_env(make_settings(max_trade_quote=40.0))
    opp = env.opps.add(make_opp(env.clock))
    sel = await env.engine.select_auto(opp.id)
    snap = sel["snapshot"]
    assert snap["valid"] is True and snap["capital"] == 40.0
    assert snap["risk"]["allowed"] == 40.0 and "max trade size" in snap["risk"]["reason"]
    res = await env.engine.confirm_auto(opp.id)
    trade = env.engine.trade(res["trade_id"])
    assert trade.state == OppState.CLOSED and trade.capital_quote == 40.0
    assert env.adapter().placed()[0].amount == pytest.approx(0.4)
    assert trade.expected_profit_quote == pytest.approx(40.0 * 4.8 / 100)


async def test_balance_downsize_when_allowed() -> None:
    env = make_env(balances={"USDT": 40.0})
    opp = env.opps.add(make_opp(env.clock))
    sel = await env.engine.select_auto(opp.id)
    snap = sel["snapshot"]
    assert snap["balance"] == {"quote": "USDT", "free": 40.0}
    # the entry order locks notional + taker fee (0.1 %) + the limit markup (0.2 %): downsize below the free balance
    assert snap["capital"] == pytest.approx(40.0 / 1.003, rel=1e-3) and snap["capital"] < 40.0 / 1.003
    assert snap["valid"] is True
    assert "downsized" in snap["reason"]
    res = await env.engine.confirm_auto(opp.id)
    trade = env.engine.trade(res["trade_id"])
    assert trade.state == OppState.CLOSED, res
    assert trade.capital_quote == pytest.approx(40.0 / 1.003, rel=1e-3)
    assert trade.entry_order.status == OrderStatus.FILLED
    assert env.adapter().placed()[0].amount == pytest.approx(0.4 / 1.003, rel=1e-3)


async def test_balance_insufficient_without_downsize() -> None:
    env = make_env(make_settings(allow_downsize=False), balances={"USDT": 40.0})
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    assert trade.state == OppState.CANCELLED
    assert trade.error == "Insufficient USDT balance: free 40.00 < needed 100.30"
    env2 = make_env(balances={"USDT": 5.0})  # below min trade size even with downsizing
    opp2 = env2.opps.add(make_opp(env2.clock))
    ok, reason, _ = await env2.engine.revalidate(opp2)
    assert not ok and reason == "Insufficient USDT balance: free 5.00 < needed 100.30"


# --------------------------------------------------------------------------- #
# cancel / duplicate guard / misc
# --------------------------------------------------------------------------- #
async def test_cancel_selected_opportunity() -> None:
    env = make_env()
    opp = env.opps.add(make_opp(env.clock))
    await env.engine.select_auto(opp.id)
    res = await env.engine.cancel(opp.id, "changed my mind")
    assert res["ok"] is True and opp.state == OppState.CANCELLED
    assert (await env.engine.cancel(opp.id))["ok"] is False
    assert (await env.engine.cancel("nope"))["ok"] is False


async def test_deselect_auto_returns_to_valid_without_cancelling() -> None:
    """Backing out of the AUTO confirmation is not a cancel: the opportunity goes
    back to VALID, stays on the board and can be traded MANUAL (or AUTO) again."""
    env = make_env()
    opp = env.opps.add(make_opp(env.clock))
    sel = await env.engine.select_auto(opp.id)
    assert sel["confirmation_required"] is True and opp.state == OppState.AUTO_SELECTED
    res = await env.engine.deselect_auto(opp.id)
    assert res["ok"] is True and res["opp_id"] == opp.id and res["state"] == "VALID"
    assert opp.state == OppState.VALID and opp.is_active
    assert env.opps.states(opp.id) == [OppState.AUTO_SELECTED, OppState.VALID]
    assert env.engine.trades() == [] and env.adapter().placed() == []
    assert opp.id not in env.engine._plan and opp.id not in env.engine._expected
    # the lag is still live: MANUAL on the same id works
    support = await env.engine.select_manual(opp.id)
    assert support["valid"] is True and opp.state == OppState.MANUAL_SELECTED
    # any other state is left untouched
    r = await env.engine.deselect_auto(opp.id)
    assert r["ok"] is False and r["state"] == "MANUAL_SELECTED" and opp.state == OppState.MANUAL_SELECTED
    assert (await env.engine.manual_entered(opp.id, 100.0, 50.0))["ok"] is True and opp.state == OppState.EXECUTING
    r = await env.engine.deselect_auto(opp.id)
    assert r["ok"] is False and opp.state == OppState.EXECUTING and env.risk.open_trades() == 1
    closed = env.opps.add(make_opp(env.clock, state=OppState.CLOSED))
    r = await env.engine.deselect_auto(closed.id)
    assert r["ok"] is False and r["state"] == "CLOSED" and closed.state == OppState.CLOSED
    assert (await env.engine.deselect_auto("nope"))["ok"] is False


async def test_cancel_before_entry_aborts() -> None:
    env = make_env()
    opp = env.opps.add(make_opp(env.clock))

    async def hook(o: Opportunity) -> None:  # user presses cancel while revalidation runs
        env.engine._abort[o.id] = "user"

    env.pipeline.on_refresh = hook
    res, trade = await run_auto(env, opp)
    assert trade.state == OppState.CANCELLED and "cancelled before entry" in trade.error
    assert env.adapter().placed() == []
    assert env.risk.open_trades() == 0


async def test_cancel_during_exit_wait_exits_at_market() -> None:
    env = make_env(exit_mode="never", market_price=102.0)
    env.settings.sell_timeout_s = 5.0
    opp = env.opps.add(make_opp(env.clock))
    await env.engine.confirm_auto(opp.id, wait=False)
    await wait_until(lambda: opp.id in env.engine._pending_exit)
    res = await env.engine.cancel(opp.id, "user")
    assert res["ok"] is True and res["aborting"] is True
    await wait_until(lambda: opp.id not in env.engine._active)
    trade = env.engine.trades()[0]
    assert trade.state == OppState.CLOSED
    assert env.adapter().placed()[-1].type == OrderType.MARKET
    assert any("cancel requested" in m for _, m in trade.events)


async def test_duplicate_execute_guard_and_manual_blocked_during_auto() -> None:
    env = make_env(exit_mode="after_polls", exit_after_polls=50)
    opp = env.opps.add(make_opp(env.clock))
    await env.engine.select_auto(opp.id)
    task = asyncio.create_task(env.engine.execute(opp))
    await wait_until(lambda: opp.id in env.engine._active)
    same = await env.engine.execute(opp)  # duplicate guard returns the running trade
    assert same is env.engine._active[opp.id]
    manual = await env.engine.select_manual(opp.id)
    assert manual["valid"] is False and "AUTO" in manual["message"]
    entered = await env.engine.manual_entered(opp.id, 100.0, 50.0)
    assert entered["ok"] is False
    trade = await task
    assert trade.state == OppState.CLOSED
    assert len(env.engine.trades()) == 1


async def test_unknown_opportunity_and_expired_state() -> None:
    env = make_env()
    assert (await env.engine.select_auto("nope"))["ok"] is False
    assert (await env.engine.confirm_auto("nope"))["ok"] is False
    opp = env.opps.add(make_opp(env.clock, state=OppState.EXPIRED))
    sel = await env.engine.select_auto(opp.id)
    assert sel["ok"] is False and sel["reason"] == "⚠️ OPPORTUNITY EXPIRED"


async def test_trades_newest_first_and_limit() -> None:
    env = make_env()
    ids = []
    for _ in range(3):
        opp = env.opps.add(make_opp(env.clock))
        res, trade = await run_auto(env, opp)
        ids.append(trade.id)
    assert [t.id for t in env.engine.trades()] == list(reversed(ids))
    assert len(env.engine.trades(limit=2)) == 2
    assert len(env.db.list_trades()) == 3


# --------------------------------------------------------------------------- #
# MANUAL
# --------------------------------------------------------------------------- #
async def test_manual_flow_to_closed() -> None:
    env = make_env()
    opp = env.opps.add(make_opp(env.clock))
    support = await env.engine.select_manual(opp.id)
    assert opp.state == OppState.MANUAL_SELECTED
    for key in ("opp_id", "valid", "status", "side", "exchange", "symbol", "entry_price_now", "exit_target_now",
                "net_now_pct", "expected_slippage_pct", "remaining_s", "human", "capital", "base_amount", "expired", "message"):
        assert key in support, key
    assert support["valid"] is True and support["expired"] is False and support["status"] == "ok"
    assert support["human"] == {"light": "green", "label": "HIGH", "est_execution_s": 7.0}
    assert support["entry_price_now"] == 100.0 and support["exit_target_now"] == pytest.approx(105.05)
    assert support["net_now_pct"] == 4.8 and support["capital"] == 100.0 and support["base_amount"] == 1.0
    assert support["remaining_s"] == 30.0 and support["side"] == "LONG" and support["symbol"] == "BTC/USDT"
    assert env.pipeline.calls[-1] == (opp.id, 100.0)

    entered = await env.engine.manual_entered(opp.id, 100.2, 100.0)
    assert entered["ok"] is True
    assert entered["exit_side"] == "SELL" and entered["exit_target"] == pytest.approx(105.05)
    assert entered["base_amount"] == pytest.approx(100.0 / 100.2)
    assert entered["exit_target_now"] == pytest.approx(105.05)
    assert entered["estimated_fees_quote"] == pytest.approx(0.1 + 105.05 * (100.0 / 100.2) * 0.001)
    assert opp.state == OppState.EXECUTING and opp.execution_id == entered["trade_id"]
    assert env.risk.open_trades() == 1 and env.risk.exposure(exchange="testex") == 100.0
    trade = env.engine.trade(entered["trade_id"])
    assert trade.entry_order is None and trade.exit_order is None and trade.mode == Mode.PAPER
    assert trade.actual_entry_price == 100.2
    in_pos = await env.engine.manual_support(opp.id)
    assert in_pos["valid"] is True and in_pos["status"] == "in_position" and in_pos["trade_id"] == trade.id
    # everything a fresh client needs to rebuild the SELL step after a page reload / in another tab
    assert in_pos["entry_price"] == 100.2 and in_pos["quote_amount"] == 100.0 and in_pos["exit_side"] == "SELL"
    again = await env.engine.select_manual(opp.id)  # re-selecting MANUAL on the EXECUTING opp returns the open position
    assert again["valid"] is True and again["expired"] is False and again["status"] == "in_position"
    assert again["trade_id"] == trade.id and again["entry_price"] == 100.2 and again["quote_amount"] == 100.0
    assert opp.state == OppState.EXECUTING and opp.execution_id == trade.id
    assert (await env.engine.cancel(opp.id))["ok"] is False  # position open: report the exit instead
    assert (await env.engine.manual_entered(opp.id, 100.0, 10.0))["ok"] is False  # once

    done = await env.engine.manual_exited(opp.id, 105.0)
    assert done is trade and trade.state == OppState.CLOSED
    base = 100.0 / 100.2
    fees = 100.0 * 0.001 + 105.0 * base * 0.001
    assert trade.actual_fees_quote == pytest.approx(fees)
    assert trade.realized_pnl_quote == pytest.approx((105.0 - 100.2) * base - fees)
    assert trade.realized_pnl_quote > 0
    assert trade.diff_reason.startswith("expected +4.80% -> actual +")
    assert trade.finished_ts is not None and trade.execution_time_s is not None
    assert opp.state == OppState.CLOSED
    assert env.risk.open_trades() == 0
    assert env.stats.trades[0][:3] == ("BTC", "testex", True)
    assert env.db.get_trade(trade.id)["state"] == "CLOSED"
    assert env.trade_updates()[-1].state == OppState.CLOSED
    with pytest.raises(KeyError):
        await env.engine.manual_exited(opp.id, 105.0)


async def test_manual_support_expired_and_too_fast_and_no_data() -> None:
    env = make_env()
    gone = env.opps.add(make_opp(env.clock, state=OppState.EXPIRED))
    p = await env.engine.manual_support(gone.id)
    assert p["valid"] is False and p["expired"] is True and p["message"] == "⚠️ OPPORTUNITY EXPIRED"
    assert p["status"] == "expired" and p["opp_id"] == gone.id and p["symbol"] == "BTC/USDT"
    missing = await env.engine.manual_support("nope")
    assert missing["valid"] is False and missing["expired"] is True

    fast = env.opps.add(make_opp(env.clock, duration_s=3.0, human=Light.RED))
    p = await env.engine.select_manual(fast.id)
    assert p["valid"] is True and p["status"] == "too_fast" and p["message"] == "🔴 TOO FAST FOR HUMAN"
    assert p["remaining_s"] == 3.0

    env.pipeline.fail = True
    p = await env.engine.manual_support(fast.id)
    assert p["valid"] is False and p["fresh"] is False and p["status"] == "no_data" and p["expired"] is False


async def test_manual_support_stays_in_position_after_window_closes() -> None:
    """Server contract the client must honour: an open manual position is never
    'too fast' or 'expired' - only the remaining window shrinks to zero."""
    env = make_env(make_settings(human_execution_time_s=7.0))
    opp = env.opps.add(make_opp(env.clock, duration_s=10.0))
    assert (await env.engine.manual_entered(opp.id, 100.2, 100.0))["ok"] is True
    env.clock.advance(12.0)  # window closed, remaining 0 < human execution time
    p = await env.engine.manual_support(opp.id)
    assert p["fresh"] is True and p["valid"] is True and p["expired"] is False
    assert p["status"] == "in_position" and p["remaining_s"] == 0.0
    assert p["message"] == "🟢 IN POSITION - report your exit price"
    assert p["human"]["est_execution_s"] == 7.0
    assert opp.state == OppState.EXECUTING


async def test_manual_entered_validation() -> None:
    env = make_env()
    opp = env.opps.add(make_opp(env.clock))
    assert (await env.engine.manual_entered(opp.id, 0, 100.0))["ok"] is False
    assert (await env.engine.manual_entered(opp.id, "abc", 100.0))["ok"] is False
    assert (await env.engine.manual_entered("nope", 100.0, 100.0))["ok"] is False
    closed = env.opps.add(make_opp(env.clock, state=OppState.CANCELLED))
    assert (await env.engine.manual_entered(closed.id, 100.0, 100.0))["ok"] is False
    # entering directly from VALID selects manual first
    r = await env.engine.manual_entered(opp.id, 100.0, 50.0)
    assert r["ok"] is True and opp.state == OppState.EXECUTING
    with pytest.raises(ValueError):
        await env.engine.manual_exited(opp.id, -1)


async def test_manual_short_on_swap() -> None:
    env = make_env()
    opp = env.opps.add(make_opp(env.clock, exchange="swapex", symbol="BTC/USDT:USDT", direction=-1))
    r = await env.engine.manual_entered(opp.id, 100.0, 100.0)
    assert r["exit_side"] == "BUY" and r["side"] == "SHORT"
    trade = await env.engine.manual_exited(opp.id, 96.0)
    # fees from the adapter (stub taker 0.1 %), not from the config (0.06 %)
    assert trade.realized_pnl_quote == pytest.approx((100.0 - 96.0) * 1.0 - (100.0 + 96.0) * 0.001)
    assert trade.actual_slippage_pct == pytest.approx(0.0 + (96.0 / (100.0 * (1 - 0.0505)) - 1) * 100)


async def test_auto_and_manual_refuse_when_mode_changed_since_detection() -> None:
    """The card says PAPER; the owner flips to LIVE: nothing executes from that
    card until it has been re-labelled (adapter_for follows the current mode)."""
    live_adapters: dict[str, StubAdapter] = {}
    env = make_env(make_settings(auto_confirm=True))
    for cfg in env.settings.exchanges:
        symbols = {"BTC": "BTC/USDT"} if cfg.market_type == "spot" else {"BTC": "BTC/USDT:USDT"}
        a = StubAdapter(cfg, env.bus, env.clock, symbols=symbols)
        for sym in symbols.values():
            a.books[sym] = make_book(sym, env.clock, exchange=cfg.name)
        live_adapters[cfg.name] = a
    env.engine._exec_adapters[Mode.LIVE] = live_adapters  # type: ignore[index]
    opp = env.opps.add(make_opp(env.clock))
    assert opp.mode is Mode.PAPER
    env.settings.update({"paper_mode": False, "live_mode": True})
    assert env.settings.mode() is Mode.LIVE
    r = await env.engine.select_auto(opp.id)
    assert r["ok"] is False and "mode changed to LIVE" in r["reason"]
    p = await env.engine.select_manual(opp.id)
    assert p["valid"] is False and p["status"] == "mode_changed" and "LIVE" in p["message"]
    r2 = await env.engine.manual_entered(opp.id, 100.0, 50.0)
    assert r2["ok"] is False and "mode changed" in r2["reason"]
    assert opp.state is OppState.VALID and env.engine.trades() == []
    assert all(a.placed() == [] for a in list(env.adapters.values()) + list(live_adapters.values()))
    # once the card carries the current mode the flows work again (on the LIVE adapters)
    opp.mode = Mode.LIVE
    r = await env.engine.select_auto(opp.id)
    assert r["ok"] is True
    await wait_until(lambda: env.engine.trade(r["trade_id"]) is not None and env.engine.trade(r["trade_id"]).finished_ts is not None)
    trade = env.engine.trade(r["trade_id"])
    assert trade.mode is Mode.LIVE and live_adapters["testex"].placed() and env.adapters["testex"].placed() == []


async def test_manual_support_reports_not_executable_for_size_not_time() -> None:
    env = make_env()
    opp = env.opps.add(make_opp(env.clock, duration_s=30.0, human=Light.RED))
    opp.human.reasons = ["not executable at size: book supports only $7.00 (min trade $10.00)", "net +4.80%"]
    p = await env.engine.manual_support(opp.id)
    assert p["valid"] is True and p["status"] == "not_executable"
    assert p["message"] == "⚠️ NOT EXECUTABLE - not executable at size: book supports only $7.00 (min trade $10.00)"
    # too little time stays TOO FAST
    fast = env.opps.add(make_opp(env.clock, duration_s=3.0, human=Light.RED))
    assert (await env.engine.manual_support(fast.id))["status"] == "too_fast"


# --------------------------------------------------------------------------- #
# order timeouts / unknown order state (hard rule 5)
# --------------------------------------------------------------------------- #
class TimeoutAdapter(StubAdapter):
    """The exchange executes the order but the response is lost: ``place_order``
    returns the ccxt-adapter timeout shape (status SENT, id "") once, for the
    ``timeout_leg`` ("entry" IOC | "exit" GTC) - or raises ``ccxt.RequestTimeout``
    when ``raise_ccxt``.  ``fetch_order`` resolves an empty id by client id unless
    ``fetch_fails``."""

    def __init__(self, *args: Any, timeout_leg: str = "entry", fetch_fails: bool = False, raise_ccxt: bool = False,
                 **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.timeout_leg = timeout_leg
        self.fetch_fails = fetch_fails
        self.raise_ccxt = raise_ccxt
        self.client_fetches: list[str] = []
        self._timed_out = False

    async def place_order(self, req: OrderRequest, *, reduce_only: bool = False) -> Order:
        order = await self._place(req, reduce_only)  # executed on the "exchange"
        if req.type == OrderType.LIMIT and req.tif == TimeInForce.IOC:
            leg = "entry"
        elif req.type == OrderType.LIMIT and req.tif == TimeInForce.GTC:
            leg = "exit"
        else:
            leg = "market"
        if leg != self.timeout_leg or self._timed_out:
            return order
        self._timed_out = True
        if self.raise_ccxt:
            import ccxt

            raise ccxt.RequestTimeout("slow")
        return Order(id="", client_id=req.client_id, exchange=self.name, symbol=req.symbol, side=req.side, type=req.type,
                     amount=req.amount, price=req.price, status=OrderStatus.SENT, sent_mono=order.sent_mono,
                     sent_ts=order.sent_ts, error="timeout: status unknown")

    async def fetch_order(self, order_id: str, symbol: str, *, client_id: str | None = None) -> Order:  # type: ignore[override]
        if self.fetch_fails:
            self.calls.append(("fetch-fail", order_id or client_id))
            raise ExchangeError("fetch_order unavailable")
        if not order_id:
            self.client_fetches.append(client_id or "")
            found = next((o for o in self.orders.values() if o.client_id == client_id), None)
            if found is None:
                raise ExchangeError(f"unknown client id {client_id}")
            order_id = found.id
        return await super().fetch_order(order_id, symbol)


class OutageAdapter(StubAdapter):
    """``cancel_order`` / ``fetch_order`` raise ``RateLimited`` while ``outage``
    is set.  The outage starts when the ``outage_on`` order is placed ("exit" =
    the GTC exit, "entry" = the IOC entry, which then comes back ACKNOWLEDGED
    with its fill unknown)."""

    def __init__(self, *args: Any, outage_on: str = "exit", **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.outage_on = outage_on
        self.outage = False

    async def place_order(self, req: OrderRequest, *, reduce_only: bool = False) -> Order:
        if self.outage_on == "entry" and req.type == OrderType.LIMIT and req.tif == TimeInForce.IOC and not self.outage:
            self.calls.append(("place", req, reduce_only))
            self._n += 1
            order = Order(id=f"{self.name}-o{self._n}", client_id=req.client_id, exchange=self.name, symbol=req.symbol,
                          side=req.side, type=req.type, amount=req.amount, price=req.price, status=OrderStatus.ACKNOWLEDGED,
                          sent_mono=self.clock.mono(), sent_ts=self.clock.wall(), ack_mono=self.clock.mono() + self.latency_s)
            self.orders[order.id] = order
            self._polls[order.id] = 0
            self.outage = True
            return order
        order = await self._place(req, reduce_only)
        if self.outage_on == "exit" and req.type == OrderType.LIMIT and req.tif == TimeInForce.GTC:
            self.outage = True
        return order

    async def fetch_order(self, order_id: str, symbol: str) -> Order:
        if self.outage:
            self.calls.append(("fetch-fail", order_id))
            raise RateLimited(retry_after_s=0.001, msg="rate limited")
        return await super().fetch_order(order_id, symbol)

    async def cancel_order(self, order_id: str, symbol: str) -> Order:
        if self.outage:
            self.calls.append(("cancel-fail", order_id))
            raise RateLimited(retry_after_s=0.001, msg="rate limited")
        return await super().cancel_order(order_id, symbol)


class RateLimitedOnceAdapter(StubAdapter):
    """The first ``fetch_order`` of the GTC exit raises ``RateLimited``
    (``retry_after_s``); every call is stamped with the loop time."""

    def __init__(self, *args: Any, retry_after_s: float = 0.3, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.retry_after_s = retry_after_s
        self.times: list[tuple[str, float]] = []
        self.limited_at: float | None = None

    def _stamp(self, what: str) -> None:
        self.times.append((what, asyncio.get_running_loop().time()))

    async def place_order(self, req: OrderRequest, *, reduce_only: bool = False) -> Order:
        self._stamp("place-market" if req.type == OrderType.MARKET else "place-limit")
        return await super().place_order(req, reduce_only=reduce_only)

    async def fetch_order(self, order_id: str, symbol: str) -> Order:
        order = self.orders[order_id]
        if self.limited_at is None and order.type == OrderType.LIMIT and order.side == Side.SELL:  # the GTC exit
            self.limited_at = asyncio.get_running_loop().time()
            self._stamp("fetch-limited")
            raise RateLimited(retry_after_s=self.retry_after_s, msg="rate limited")
        self._stamp("fetch")
        return await super().fetch_order(order_id, symbol)

    async def cancel_order(self, order_id: str, symbol: str) -> Order:
        self._stamp("cancel")
        return await super().cancel_order(order_id, symbol)

    def after_limit(self) -> list[tuple[str, float]]:
        assert self.limited_at is not None
        return [(w, t - self.limited_at) for w, t in self.times if t > self.limited_at and w != "fetch-limited"]


async def test_wait_fill_backs_off_for_retry_after_instead_of_hammering_fetch_order() -> None:
    env = make_env(make_settings(sell_timeout_s=1.0), adapter_cls=RateLimitedOnceAdapter, exit_mode="after_polls",
                   exit_after_polls=2, retry_after_s=0.3)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    a = env.adapter()
    assert res["ok"] is True and trade.state == OppState.CLOSED, res
    after = a.after_limit()
    assert after and all(dt >= 0.3 - 0.02 for _, dt in after), after  # nothing sent during the retry window
    assert [w for w, _ in after] == ["fetch", "fetch"]  # resumed polling, filled by the limit (no market exit)
    assert after[0][1] < 1.0  # and resumed as soon as the window was over, inside sell_timeout_s


async def test_closing_orders_wait_for_the_rate_limit_window() -> None:
    env = make_env(make_settings(sell_timeout_s=0.05), adapter_cls=RateLimitedOnceAdapter, exit_mode="never",
                   market_price=101.0, retry_after_s=0.3)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    a = env.adapter()
    assert trade.state == OppState.CLOSED, res
    after = a.after_limit()
    assert [w for w, _ in after] == ["cancel", "fetch", "place-market", "fetch"] or [w for w, _ in after] == ["cancel", "fetch", "place-market"], after
    assert all(dt >= 0.3 - 0.02 for _, dt in after), after  # the cancel and the market exit waited for the window
    assert after[0][1] < 0.6
    assert [r.type for r in a.placed()] == [OrderType.LIMIT, OrderType.LIMIT, OrderType.MARKET]
    assert env.risk.open_trades() == 0


async def test_emergency_close_refuses_to_send_into_a_long_rate_limit_window() -> None:
    """An emergency close is awaited by a person (API / Telegram): it waits a
    few seconds for a short window, but a long ban makes it refuse at once -
    nothing is sent into the ban and the position stays tracked for a retry."""
    env = make_env(adapter_cls=OutageAdapter, exit_mode="never", market_price=101.0)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    a = env.adapter()
    assert trade.state == OppState.FAILED and len(env.engine.open_positions()) == 1, res
    a.outage = False
    await env.engine.emergency_stop("outage over")  # the resting GTC is cancelled, the position is known
    a.health.rate_limited_until_mono = env.clock.mono() + 600.0  # a ban no route may wait for
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    closed = await env.engine.emergency_close_positions()
    assert closed == [] and loop.time() - t0 < 1.0
    assert OrderType.MARKET not in [r.type for r in a.placed()]  # nothing sent into the ban
    assert len(env.engine.open_positions()) == 1 and env.risk.open_trades() == 1
    # the window is over: the close goes through
    a.health.rate_limited_until_mono = None
    closed = await env.engine.emergency_close_positions()
    assert [t.id for t in closed] == [trade.id] and trade.state == OppState.CLOSED
    assert a.placed()[-1].type == OrderType.MARKET and env.engine.open_positions() == []


@pytest.mark.parametrize("raise_ccxt", [False, True])
async def test_entry_timeout_is_reconciled_by_client_id(raise_ccxt: bool) -> None:
    env = make_env(adapter_cls=TimeoutAdapter, raise_ccxt=raise_ccxt)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    a = env.adapter()
    assert res["ok"] is True and trade.state == OppState.CLOSED, res
    assert a.client_fetches and a.client_fetches[0] == trade.entry_order.client_id
    assert trade.entry_order.id and trade.entry_order.status == OrderStatus.FILLED
    assert [r.type for r in a.placed()] == [OrderType.LIMIT, OrderType.LIMIT]  # never a retry
    assert not any(c[0] == "cancel" for c in a.calls)
    assert env.risk.open_trades() == 0


async def test_entry_timeout_unreconciled_fails_and_keeps_position_tracked() -> None:
    env = make_env(adapter_cls=TimeoutAdapter, fetch_fails=True, market_price=101.0)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    a = env.adapter()
    assert trade.state == OppState.FAILED, res
    assert "unknown" in trade.error and "not filled" not in trade.error
    assert trade.entry_order.status == OrderStatus.SENT and trade.entry_order.id == ""
    assert trade.entry_order.ack_mono is None  # never acknowledged
    assert len(a.placed()) == 1  # never a retry
    assert opp.state == OppState.FAILED
    assert env.risk.open_trades() == 1 and env.risk.exposure(exchange="testex") == pytest.approx(100.0)  # exposure kept
    positions = env.engine.open_positions()
    assert len(positions) == 1 and positions[0]["trade_id"] == trade.id and positions[0]["base"] == pytest.approx(1.0)
    assert positions[0]["unresolved"] is True
    assert env.db.get_trade(trade.id)["state"] == "FAILED"
    # the exchange answers again: emergency close learns the entry had filled and exits at market once
    a.fetch_fails = False
    closed = await env.engine.emergency_close_positions()
    assert [t.id for t in closed] == [trade.id] and trade.state == OppState.CLOSED
    assert trade.entry_order.status == OrderStatus.FILLED and trade.entry_order.filled == pytest.approx(1.0)
    assert a.placed()[-1].type == OrderType.MARKET and a.placed()[-1].amount == pytest.approx(1.0)
    assert trade.realized_pnl_quote == pytest.approx((101.0 - 100.2) - (100.2 + 101.0) * 0.001)
    assert env.risk.open_trades() == 0 and env.engine.open_positions() == []


async def test_exit_timeout_unreconciled_never_sends_a_duplicate_exit() -> None:
    env = make_env(adapter_cls=TimeoutAdapter, timeout_leg="exit", fetch_fails=True, exit_mode="never", market_price=101.0)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    a = env.adapter()
    assert trade.state == OppState.FAILED, res
    assert "unknown" in trade.error
    assert [r.type for r in a.placed()] == [OrderType.LIMIT, OrderType.LIMIT]  # no MARKET exit on top of an unknown GTC
    assert trade.exit_order is not None and trade.exit_order.status == OrderStatus.SENT and trade.exit_order.id == ""
    assert env.risk.open_trades() == 1
    positions = env.engine.open_positions()
    assert len(positions) == 1 and positions[0]["base"] == pytest.approx(1.0) and positions[0]["unresolved"] is True
    gtc = next(o for o in a.orders.values() if o.type == OrderType.LIMIT and o.side == Side.SELL)
    assert not gtc.is_done  # still resting on the exchange
    # exchange reachable again: emergency stop finds the GTC by client id and cancels it ...
    a.fetch_fails = False
    res = await env.engine.emergency_stop("panic")
    assert res["cancelled_orders"] >= 1 and gtc.status == OrderStatus.CANCELLED
    assert env.engine.open_positions()[0]["unresolved"] is False
    # ... and emergency close exits the position at market exactly once
    closed = await env.engine.emergency_close_positions()
    assert [t.id for t in closed] == [trade.id] and trade.state == OppState.CLOSED
    assert [r.type for r in a.placed()] == [OrderType.LIMIT, OrderType.LIMIT, OrderType.MARKET]
    assert trade.exit_order.filled == pytest.approx(1.0) and trade.exit_order.raw["market_filled"] == pytest.approx(1.0)
    assert env.risk.open_trades() == 0


async def test_cancel_failure_does_not_market_exit_unknown_order() -> None:
    env = make_env(adapter_cls=OutageAdapter, exit_mode="never", market_price=101.0)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    a = env.adapter()
    assert trade.state == OppState.FAILED, res
    assert "unknown" in trade.error
    assert [r.type for r in a.placed()] == [OrderType.LIMIT, OrderType.LIMIT]  # no MARKET exit
    assert any(c[0] == "cancel-fail" for c in a.calls) and sum(1 for c in a.calls if c[0] == "fetch-fail") >= 3
    gtc = next(o for o in a.orders.values() if o.type == OrderType.LIMIT and o.side == Side.SELL)
    assert gtc.status == OrderStatus.ACKNOWLEDGED  # still live on the exchange - and the engine says so
    assert trade.exit_order is not None and trade.exit_order.status == OrderStatus.ACKNOWLEDGED
    assert env.risk.open_trades() == 1
    positions = env.engine.open_positions()
    assert len(positions) == 1 and positions[0]["trade_id"] == trade.id and positions[0]["unresolved"] is True
    # the outage ends: emergency stop cancels the resting GTC, emergency close exits at market
    a.outage = False
    res = await env.engine.emergency_stop("outage over")
    assert res["cancelled_orders"] >= 1 and gtc.status == OrderStatus.CANCELLED
    closed = await env.engine.emergency_close_positions()
    assert [t.id for t in closed] == [trade.id] and trade.state == OppState.CLOSED
    assert a.placed()[-1].type == OrderType.MARKET and a.placed()[-1].amount == pytest.approx(1.0)
    assert env.risk.open_trades() == 0 and env.engine.open_positions() == []


async def test_entry_cancel_failure_reports_unknown_state_not_no_fill() -> None:
    env = make_env(adapter_cls=OutageAdapter, outage_on="entry", market_price=101.0)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    a = env.adapter()
    assert trade.state == OppState.FAILED, res
    assert "unknown" in trade.error and "not filled" not in trade.error
    assert len(a.placed()) == 1 and trade.exit_order is None
    assert env.risk.open_trades() == 1 and env.engine.open_positions()[0]["unresolved"] is True
    # meanwhile the IOC had actually filled on the exchange; once reachable, emergency close learns that and exits
    entry = next(iter(a.orders.values()))
    a._fill(entry, 1.0, 100.2)
    a.outage = False
    closed = await env.engine.emergency_close_positions()
    assert [t.id for t in closed] == [trade.id] and trade.state == OppState.CLOSED
    assert trade.entry_order.filled == pytest.approx(1.0) and trade.actual_entry_price == pytest.approx(100.2)
    assert a.placed()[-1].type == OrderType.MARKET and a.placed()[-1].amount == pytest.approx(1.0)
    assert env.risk.open_trades() == 0


async def test_unknown_entry_that_never_filled_is_cancelled_on_emergency_close() -> None:
    env = make_env(adapter_cls=OutageAdapter, outage_on="entry")
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    assert trade.state == OppState.FAILED
    a = env.adapter()
    a.outage = False
    closed = await env.engine.emergency_close_positions()
    assert [t.id for t in closed] == [trade.id]
    assert trade.state == OppState.CANCELLED and trade.entry_order.status == OrderStatus.CANCELLED
    assert len(a.placed()) == 1  # nothing to sell: no market order
    assert env.risk.open_trades() == 0 and env.engine.open_positions() == []


# --------------------------------------------------------------------------- #
# emergency stop is scoped to the active mode (hard rule 8)
# --------------------------------------------------------------------------- #
async def test_emergency_stop_never_touches_adapters_of_the_other_mode() -> None:
    env = make_env(exit_mode="never")
    cfg = env.settings.exchange("testex")
    live = StubAdapter(cfg, env.bus, env.clock, symbols={"BTC": "BTC/USDT"}, exit_mode="never")
    req = OrderRequest(exchange="testex", symbol="BTC/USDT", side=Side.SELL, amount=1.0, type=OrderType.LIMIT, price=120.0,
                       tif=TimeInForce.GTC)
    resting = await live.place_order(req)  # the owner's own order on the real account
    env.engine._exec_adapters[Mode.LIVE] = {"testex": live}
    res = await env.engine.emergency_stop("paper panic")
    assert res["emergency"] is True
    assert ("cancel_all",) in env.adapter().calls and ("cancel_all",) in env.adapter("swapex").calls
    assert ("cancel_all",) not in live.calls and resting.status == OrderStatus.ACKNOWLEDGED
    # mirror: a LIVE emergency stop leaves the paper venue alone
    live_settings = make_settings(paper_mode=False, live_mode=True)
    env2 = make_env(live_settings, mode=Mode.LIVE, exit_mode="never")
    assert env2.settings.mode() is Mode.LIVE
    paper = StubAdapter(live_settings.exchange("testex"), env2.bus, env2.clock, symbols={"BTC": "BTC/USDT"}, exit_mode="never")
    resting2 = await paper.place_order(req)
    env2.engine._exec_adapters[Mode.PAPER] = {"testex": paper}
    await env2.engine.emergency_stop("live panic")
    assert ("cancel_all",) in env2.adapter().calls
    assert ("cancel_all",) not in paper.calls and resting2.status == OrderStatus.ACKNOWLEDGED


# --------------------------------------------------------------------------- #
# secrets never leak through trade / order / plan text (hard rule 7)
# --------------------------------------------------------------------------- #
async def test_exchange_error_text_is_redacted_everywhere() -> None:
    key = "VVE0TZMN9xTBtU1AK0"
    register_secret(key)
    leak = f'AuthenticationError: bybit {{"retCode":10004,"retMsg":"error sign! origin_string[1751658755149{key}5000..."}}'

    class LeakyAdapter(StubAdapter):
        leak_balances = False

        async def _place(self, req: OrderRequest, reduce_only: bool) -> Order:
            self.calls.append(("place", req, reduce_only))
            raise OrderRejected(leak)

        async def fetch_balances(self) -> dict[str, Balance]:
            if self.leak_balances:
                raise ExchangeError(leak)
            return await super().fetch_balances()

    env = make_env(adapter_cls=LeakyAdapter)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    assert trade.state == OppState.FAILED
    assert trade.error.startswith("entry order failed: AuthenticationError") and "***" in trade.error
    sinks = {
        "trade": trade.to_dict(),
        "confirm": res,
        "orders_db": env.db.list_orders(trade.id),
        "trades_db": env.db.list_trades(),
        "events": [(t, p.to_dict() if isinstance(p, TradeRecord) else str(p)) for t, p in env.events],
        "opp": opp.to_dict(env.clock.mono()),
    }
    blob = json.dumps(sinks, default=str, ensure_ascii=False)
    assert key not in blob
    assert key not in str(trade.entry_order.error) and key not in trade.diff_reason

    env2 = make_env(adapter_cls=LeakyAdapter)
    env2.adapter().leak_balances = True
    opp2 = env2.opps.add(make_opp(env2.clock))
    sel = await env2.engine.select_auto(opp2.id)
    assert sel["snapshot"]["valid"] is False and sel["snapshot"]["reason"].startswith("Balance check failed")
    assert key not in json.dumps(sel, default=str, ensure_ascii=False)
    res2 = await env2.engine.confirm_auto(opp2.id)
    assert key not in json.dumps(res2, default=str, ensure_ascii=False)


# --------------------------------------------------------------------------- #
# MANUAL support never shows a dead book as live (hard rules 1 and 4)
# --------------------------------------------------------------------------- #
async def test_manual_support_reports_no_fresh_data_on_stale_book() -> None:
    env = make_env()
    a = env.adapter()
    opp = env.opps.add(make_opp(env.clock))
    p = await env.engine.select_manual(opp.id)
    assert p["valid"] is True and p["fresh"] is True
    calls = len(env.pipeline.calls)
    # the target feed dropped and REST is down: honest NO FRESH DATA, and no refresh
    # (a refresh would keep bumping last_update_mono and defeat the sweep's "no updates" expiry)
    a.books["BTC/USDT"] = make_book("BTC/USDT", env.clock, recv_mono=env.clock.mono() - 3.0)
    a.rest_fail = True
    p = await env.engine.manual_support(opp.id)
    assert p["valid"] is False and p["fresh"] is False and p["status"] == "no_data"
    assert p["message"] == "⚠️ NO FRESH DATA" and p["expired"] is False and p["entry_price_now"] is None
    assert len(env.pipeline.calls) == calls and a.rest_calls == 1
    # the modal polls every 300 ms: REST is not hammered inside max_data_age_ms
    p = await env.engine.manual_support(opp.id)
    assert p["status"] == "no_data" and a.rest_calls == 1
    # REST is back: one snapshot, published as BOOK, and the modal is live again
    env.clock.advance(2.0)
    a.rest_fail = False
    p = await env.engine.manual_support(opp.id)
    assert p["valid"] is True and p["fresh"] is True and p["status"] == "ok"
    assert a.rest_calls == 2 and len(env.pipeline.calls) == calls + 1
    assert any(t == events.BOOK for t, _ in env.events)
    # the pipeline itself reporting a stale book is not fresh either
    env.pipeline.next_econ = make_econ(book_age_ms=5000.0)
    p = await env.engine.manual_support(opp.id)
    assert p["valid"] is False and p["status"] == "no_data"
    env.pipeline.next_econ = None
    env.pipeline.return_none = True
    p = await env.engine.manual_support(opp.id)
    assert p["valid"] is False and p["fresh"] is False and p["status"] == "no_data"


# --------------------------------------------------------------------------- #
# revalidation before the exit order (hard rule 4, second half)
# --------------------------------------------------------------------------- #
async def test_exit_is_repriced_from_the_fresh_book() -> None:
    n = {"calls": 0}

    async def hook(o: Opportunity) -> None:
        n["calls"] += 1
        if n["calls"] == 3:  # select_auto + confirm_auto revalidate; the 3rd refresh is right before the exit order
            o.econ = make_econ(net=2.0)

    env = make_env(pipeline=StubPipeline(on_refresh=hook))
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    assert trade.state == OppState.CLOSED, res
    assert len(env.pipeline.calls) == 3
    reqs = env.adapter().placed()
    assert reqs[1].tif == TimeInForce.GTC and reqs[1].price == pytest.approx(102.25)
    assert any("exit revalidated" in m for _, m in trade.events)
    assert trade.expected_exit_price == pytest.approx(105.05)  # expected-vs-actual keeps what the user saw
    assert trade.actual_exit_price == pytest.approx(102.25)


async def test_exit_markets_out_when_the_fresh_edge_is_gone() -> None:
    n = {"calls": 0}

    async def hook(o: Opportunity) -> None:
        n["calls"] += 1
        if n["calls"] == 3:
            o.econ = dataclasses.replace(make_econ(), exit_vwap_now=99.0, exit_target=99.0)

    env = make_env(pipeline=StubPipeline(on_refresh=hook), exit_mode="never", market_price=99.0)
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    assert trade.state == OppState.CLOSED, res
    reqs = env.adapter().placed()
    assert [r.type for r in reqs] == [OrderType.LIMIT, OrderType.MARKET]  # no GTC at a target that no longer exists
    assert trade.exit_order.type == OrderType.MARKET and trade.actual_exit_price == pytest.approx(99.0)
    assert trade.realized_pnl_quote < 0
    assert any("edge gone" in m for _, m in trade.events)


async def test_exit_falls_back_to_the_planned_target_when_refresh_fails() -> None:
    n = {"calls": 0}

    async def hook(o: Opportunity) -> None:
        n["calls"] += 1
        if n["calls"] == 3:
            raise RuntimeError("book gone")

    env = make_env(pipeline=StubPipeline(on_refresh=hook))
    opp = env.opps.add(make_opp(env.clock))
    res, trade = await run_auto(env, opp)
    assert trade.state == OppState.CLOSED, res  # a position is never left unmanaged
    reqs = env.adapter().placed()
    assert reqs[1].tif == TimeInForce.GTC and reqs[1].price == pytest.approx(105.05)
    assert any("exit revalidation unavailable" in m for _, m in trade.events)


# --------------------------------------------------------------------------- #
# shutdown / crash / restart never abandon a position (hard rules 5 and 6)
# --------------------------------------------------------------------------- #
async def test_shutdown_market_exits_a_pending_exit() -> None:
    env = make_env(exit_mode="never", market_price=101.0)
    env.settings.sell_timeout_s = 5.0
    opp = env.opps.add(make_opp(env.clock))
    await env.engine.confirm_auto(opp.id, wait=False)
    await wait_until(lambda: opp.id in env.engine._pending_exit)
    left = await env.engine.shutdown(2.0)
    assert left == []
    trade = env.engine.trades()[0]
    assert trade.state == OppState.CLOSED and env.engine._tasks == {}
    a = env.adapter()
    assert any(c[0] == "cancel" for c in a.calls) and a.placed()[-1].type == OrderType.MARKET
    assert [o for o in a.orders.values() if not o.is_done] == []
    assert env.risk.open_trades() == 0
    assert env.db.get_trade(trade.id)["state"] == "CLOSED"
    assert any("shutdown" in m for _, m in trade.events)


async def test_cancelled_task_keeps_the_open_position_tracked() -> None:
    env = make_env(exit_mode="never", market_price=101.0)
    env.settings.sell_timeout_s = 5.0
    opp = env.opps.add(make_opp(env.clock))
    await env.engine.confirm_auto(opp.id, wait=False)
    await wait_until(lambda: opp.id in env.engine._pending_exit)
    task = env.engine._tasks[opp.id]
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    trade = env.engine.trades()[0]
    assert trade.state == OppState.FAILED and "cancelled" in trade.error
    assert env.db.get_trade(trade.id)["state"] == "FAILED"
    assert env.risk.open_trades() == 1
    positions = env.engine.open_positions()
    assert len(positions) == 1 and positions[0]["trade_id"] == trade.id and positions[0]["base"] == pytest.approx(1.0)
    assert positions[0]["unresolved"] is True  # the GTC exit is still resting
    assert env.trade_updates()[-1].state == OppState.FAILED
    assert opp.id not in env.engine._active and opp.id not in env.engine._tasks
    a = env.adapter()
    gtc = next(o for o in a.orders.values() if o.side == Side.SELL)
    assert not gtc.is_done
    closed = await env.engine.emergency_close_positions()
    assert [t.id for t in closed] == [trade.id] and trade.state == OppState.CLOSED
    assert gtc.status == OrderStatus.CANCELLED and a.placed()[-1].type == OrderType.MARKET
    assert env.risk.open_trades() == 0


async def test_restore_open_trades_from_db() -> None:
    env = make_env(market_price=101.0)
    a = env.adapter()
    entry = Order(id="testex-e1", client_id="o-entry", exchange="testex", symbol="BTC/USDT", side=Side.BUY, type=OrderType.LIMIT,
                  amount=1.0, price=100.2, status=OrderStatus.FILLED, filled=1.0, avg_price=100.2, fee_cost_quote=0.1002,
                  fills=[Fill(order_id="testex-e1", price=100.2, amount=1.0, fee_cost=0.1002, fee_asset="USDT", ts=T0)],
                  sent_ts=T0, sent_mono=M0, ack_mono=M0 + 0.05, first_fill_mono=M0 + 0.05, done_mono=M0 + 0.05)
    exit_ = Order(id="testex-x1", client_id="o-exit", exchange="testex", symbol="BTC/USDT", side=Side.SELL, type=OrderType.LIMIT,
                  amount=1.0, price=105.05, status=OrderStatus.ACKNOWLEDGED, sent_ts=T0 + 0.1, sent_mono=M0 + 0.1, ack_mono=M0 + 0.15)
    a.orders[exit_.id] = exit_  # still resting on the exchange
    crashed = TradeRecord(id="tr_crash", opportunity_id="opp_crash", mode=Mode.PAPER, exchange="testex", asset="BTC", symbol="BTC/USDT",
                          direction=1, capital_quote=100.0, expected_net_pct=4.8, expected_profit_quote=4.8, expected_entry_price=100.0,
                          expected_exit_price=105.05, state=OppState.EXECUTING, entry_order=entry, exit_order=exit_, started_ts=T0)
    env.db.save_trade(crashed)
    env.db.save_order(entry, crashed.id)
    env.db.save_order(exit_, crashed.id)
    # noise that must NOT come back: a closed trade, a trade on an exchange without an adapter, an ancient one
    done_exit = dataclasses.replace(exit_, status=OrderStatus.FILLED, filled=1.0, avg_price=105.05)
    env.db.save_trade(dataclasses.replace(crashed, id="tr_done", state=OppState.CLOSED, exit_order=done_exit))
    env.db.save_trade(dataclasses.replace(crashed, id="tr_noadapter", exchange="nowhere"))
    env.db.save_trade(dataclasses.replace(crashed, id="tr_ancient", started_ts=T0 - 30 * 86400))
    restored = env.engine.restore_from_db()
    assert [t.id for t in restored] == ["tr_crash"]
    trade = env.engine.trade("tr_crash")
    assert trade is not None and trade.state == OppState.FAILED and "restart" in trade.error
    assert env.db.get_trade("tr_crash")["state"] == "FAILED"
    assert env.risk.open_trades() == 1 and env.risk.exposure(exchange="testex") == pytest.approx(100.0)
    positions = env.engine.open_positions()
    assert len(positions) == 1 and positions[0]["trade_id"] == "tr_crash" and positions[0]["base"] == pytest.approx(1.0)
    assert positions[0]["unresolved"] is True
    assert env.engine.restore_from_db() == []  # idempotent
    # emergency close: the resting GTC is cancelled first, then the position is exited at market once
    closed = await env.engine.emergency_close_positions()
    assert [t.id for t in closed] == ["tr_crash"] and trade.state == OppState.CLOSED
    assert exit_.status == OrderStatus.CANCELLED
    assert a.placed()[-1].type == OrderType.MARKET and a.placed()[-1].amount == pytest.approx(1.0)
    assert trade.realized_pnl_quote == pytest.approx((101.0 - 100.2) - (100.2 + 101.0) * 0.001)
    assert env.risk.open_trades() == 0 and env.engine.open_positions() == []


async def test_restored_manual_position_can_be_exited_without_the_opportunity() -> None:
    """After a restart the opportunity is gone (OpportunityManager starts empty)
    but the MANUAL position the owner still holds is restored with its risk
    exposure: the support payload must report the position (never EXPIRED, no
    invented live numbers) and the exit must be reportable so the exposure is
    released - otherwise the position is stuck in EXECUTING forever."""
    env = make_env()
    held = TradeRecord(id="tr_manual", opportunity_id="opp_gone", mode=Mode.PAPER, exchange="testex", asset="BTC", symbol="BTC/USDT",
                       direction=1, capital_quote=100.0, expected_net_pct=4.8, expected_profit_quote=4.8, expected_entry_price=100.0,
                       expected_exit_price=105.05, state=OppState.EXECUTING, entry_order=None, exit_order=None,
                       actual_entry_price=100.2, actual_fees_quote=0.1, started_ts=T0)
    env.db.save_trade(held)
    restored = env.engine.restore_from_db()
    assert [t.id for t in restored] == ["tr_manual"]
    assert env.opps.get("opp_gone") is None  # the card itself is not restored
    assert env.risk.open_trades() == 1 and env.risk.exposure(exchange="testex") == pytest.approx(100.0)
    active = env.engine.active_trade("opp_gone")
    assert active is not None and active.id == "tr_manual"
    for payload in (await env.engine.manual_support("opp_gone"), await env.engine.select_manual("opp_gone")):
        assert payload["status"] == "in_position" and payload["valid"] is True and payload["expired"] is False, payload
        assert payload["trade_id"] == "tr_manual" and payload["entry_price"] == 100.2 and payload["quote_amount"] == 100.0
        assert payload["exit_side"] == "SELL" and payload["side"] == "LONG"
        assert payload["exchange"] == "testex" and payload["symbol"] == "BTC/USDT" and payload["quote"] == "USDT"
        assert payload["capital"] == 100.0 and payload["base_amount"] == pytest.approx(100.0 / 100.2)
        # no opportunity -> no live price / net is invented (hard rule 1)
        assert payload["entry_price_now"] is None and payload["exit_target_now"] is None and payload["net_now_pct"] is None
        assert payload["fresh"] is False and "IN POSITION" in payload["message"]
    # a new entry needs a live opportunity
    assert (await env.engine.manual_entered("opp_gone", 100.0, 50.0))["ok"] is False
    assert env.risk.open_trades() == 1
    trade = await env.engine.manual_exited("opp_gone", 106.0)
    assert trade.id == "tr_manual" and trade.state == OppState.CLOSED
    base = 100.0 / 100.2
    assert trade.realized_pnl_quote == pytest.approx((106.0 - 100.2) * base - (100.0 + 106.0 * base) * 0.001)
    assert env.risk.open_trades() == 0 and env.engine.active_trade("opp_gone") is None
    assert env.db.get_trade("tr_manual")["state"] == "CLOSED"
    assert (await env.engine.manual_support("opp_gone"))["status"] == "expired"
    with pytest.raises(KeyError):
        await env.engine.manual_exited("opp_gone", 106.0)
