"""Offline end-to-end tests for latbot.engine.pipeline.Pipeline."""
from __future__ import annotations

import asyncio

import pytest

from latbot import events
from latbot.clock import SimClock
from latbot.config import ExchangeConfig, Settings
from latbot.engine.duration import DurationEstimator
from latbot.engine.mover import MoveDetector
from latbot.engine.opportunity import OpportunityManager
from latbot.engine.pipeline import Pipeline
from latbot.engine.stats import StatsStore
from latbot.events import EventBus
from latbot.market.state import MarketState
from latbot.models import (
    BookLevel,
    ExchangeHealth,
    ExchangeRole,
    Fees,
    Light,
    OppState,
    OrderBook,
    Ticker,
    Timestamps,
)

SYMBOLS = {"binance": "BTC/USDT", "kucoin": "BTC/USDT", "kraken": "BTC/EUR"}


class StubAdapter:
    """Minimal adapter double: fees(), health, check_staleness(), fetch_order_book()."""

    def __init__(self, name: str, clock: SimClock, taker: float = 0.001, maker: float = 0.001) -> None:
        self.name = name
        self.clock = clock
        self.cfg = None
        self.taker = taker
        self.maker = maker
        self.health = ExchangeHealth(exchange=name, ws_connected=True, order_latency_ms=200.0, ws_latency_ms=50.0)
        self.fees_calls: list[str] = []
        self.rest_book: OrderBook | None = None
        self.rest_calls: list[str] = []
        self.fail_fees = False

    async def fees(self, symbol: str) -> Fees:
        self.fees_calls.append(symbol)
        if self.fail_fees:
            raise RuntimeError("no fees")
        return Fees(maker=self.maker, taker=self.taker)

    def check_staleness(self, now_mono: float, stale_after_s: float) -> bool:
        if self.health.last_msg_mono is None:
            self.health.stale = True
        else:
            self.health.stale = (now_mono - self.health.last_msg_mono) > stale_after_s
        return self.health.stale

    async def fetch_order_book(self, symbol: str, limit: int = 20) -> OrderBook:
        self.rest_calls.append(symbol)
        if self.rest_book is None:
            raise RuntimeError("rest down")
        return self.rest_book

    def mark_msg(self) -> None:
        self.health.ws_connected = True
        self.health.last_msg_mono = self.clock.mono()
        self.health.stale = False


def make_settings(**over) -> Settings:
    base = dict(
        exchanges=[
            ExchangeConfig(name="binance", role=ExchangeRole.SIGNAL, stream="bbo", label="Binance"),
            ExchangeConfig(name="kucoin", role=ExchangeRole.SIGNAL, stream="bbo", label="KuCoin"),
            ExchangeConfig(name="kraken", role=ExchangeRole.EXECUTION, taker_fee=0.004, label="Kraken"),
        ],
        min_mover_volume_quote=0.0,
        min_target_volume_quote=0.0,
        min_movers=2,
        price_sample_min_interval_ms=0.0,
        ui_push_interval_ms=50,
    )
    base.update(over)
    return Settings(**base)


class Env:
    def __init__(self, settings: Settings | None = None, db=None, with_stats: bool = True) -> None:
        self.settings = settings or make_settings()
        self.bus = EventBus()
        self.clock = SimClock()
        self.state = MarketState(self.settings, self.clock)
        self.stats = StatsStore(self.settings) if with_stats else None
        self.detector = MoveDetector(self.settings, self.state, self.stats or StatsStore(self.settings), self.clock, self.bus)
        self.opps = OpportunityManager(self.settings, self.bus, self.clock, db=db, stats=self.stats)
        self.adapters = {name: StubAdapter(name, self.clock) for name in SYMBOLS}
        self.duration = DurationEstimator(self.settings, self.stats)
        self.pipeline = Pipeline(self.settings, self.bus, self.clock, self.state, self.detector, self.stats,
                                 self.opps, self.adapters, self.duration, db=db)
        self.topics: list[tuple[str, object]] = []
        self.bus.subscribe_all(lambda t, p: self.topics.append((t, p)))

    def ts(self) -> Timestamps:
        return Timestamps(recv_ts=self.clock.wall(), recv_mono=self.clock.mono())

    def book(self, ex: str, mid: float, size: float = 50.0) -> OrderBook:
        bids = [BookLevel(mid * (1 - 0.0001 * k), size) for k in (1, 2, 3)]
        asks = [BookLevel(mid * (1 + 0.0001 * k), size) for k in (1, 2, 3)]
        return OrderBook(exchange=ex, symbol=SYMBOLS[ex], bids=bids, asks=asks, ts=self.ts(), asset="BTC")

    async def push(self, ex: str, mid: float, size: float = 50.0):
        if ex in self.adapters:
            self.adapters[ex].mark_msg()
        return await self.pipeline.on_book(self.book(ex, mid, size))

    async def push_all(self, prices: dict[str, float]):
        out = []
        for ex, p in prices.items():
            out += await self.push(ex, p)
        return out

    async def warm_up(self) -> None:
        await self.push_all({"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
        self.clock.advance(0.5)
        await self.push_all({"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
        self.clock.advance(1.0)

    def topic_names(self, *only: str) -> list[str]:
        return [t for t, _ in self.topics if not only or t in only]


async def detect(env: Env):
    """binance + kucoin jump +2 %, kraken lags -> opportunity on kraken."""
    await env.warm_up()
    assert await env.push("binance", 102.0) == []  # one venue is not the market
    opps = await env.push("kucoin", 102.0)
    assert len(opps) == 1
    return opps[0]


async def test_end_to_end_opportunity_new_then_update():
    env = Env()
    opp = await detect(env)
    assert opp.target_exchange == "kraken" and opp.first_mover == "binance" and opp.direction == 1
    assert opp.asset == "BTC" and opp.target_symbol == "BTC/EUR" and opp.quote == "EUR"
    assert "kraken" in opp.pair_url
    assert opp.signal.movers_count == 2 and opp.signal.lag_pct == pytest.approx(2.0)
    assert opp.state is OppState.VALID
    assert [s for _, s, _ in opp.state_history] == ["DETECTED", "VALIDATING", "VALID"]
    # economics on the REAL kraken book: catch-up 1.2 %, adapter fees 0.1 % per leg + 0.05 % execution loss
    assert opp.econ.liquidity_ok and opp.econ.executable_quote == pytest.approx(100.0)
    assert opp.econ.entry_vwap == pytest.approx(100.01) and opp.econ.exit_vwap_now == pytest.approx(99.99)
    assert opp.econ.exit_target == pytest.approx(99.99 * 1.012)
    assert opp.econ.total_cost_pct == pytest.approx(0.25)
    assert 0.5 < opp.econ.net_pct < 2.0
    assert opp.econ.net_pct == pytest.approx(opp.econ.gross_pct - 0.25)
    assert opp.human.light is Light.YELLOW  # 8 s window vs 7 s needed
    assert opp.bot.light is Light.GREEN and opp.bot.est_execution_s == pytest.approx(0.4)
    assert "Binance moved +2.0%" in opp.why and "Kraken" in opp.why and "€100" in opp.why
    assert env.adapters["kraken"].fees_calls == ["BTC/EUR"]
    assert env.topic_names(events.OPP_NEW, events.OPP_UPDATE) == [events.OPP_NEW]
    assert env.topic_names(events.SIGNAL) == [events.SIGNAL]
    assert env.opps.best(env.clock.mono()) is opp
    assert opp.caught_up is None

    # the next kraken book -> same opportunity updated (no second OPP_NEW)
    env.clock.advance(0.2)
    updated = await env.push("kraken", 100.0)
    assert updated == [opp] and opp.update_count == 1
    assert env.topic_names(events.OPP_NEW, events.OPP_UPDATE) == [events.OPP_NEW, events.OPP_UPDATE]
    assert len(env.opps.active()) == 1
    assert env.adapters["kraken"].fees_calls == ["BTC/EUR"]  # cached
    # a mover's book also refreshes the opportunity through a new signal
    env.clock.advance(0.2)
    await env.push("binance", 102.0)
    assert opp.update_count == 2


async def test_no_opportunity_when_net_below_show_threshold():
    env = Env()
    env.adapters["kraken"].taker = 0.01  # 1 % per leg eats the 1.2 % catch-up
    await env.warm_up()
    await env.push("binance", 102.0)
    assert await env.push("kucoin", 102.0) == []
    assert env.opps.active() == [] and env.topic_names(events.OPP_NEW) == []
    assert env.topic_names(events.SIGNAL) == [events.SIGNAL]  # the signal existed, the economics said no


async def test_stale_target_stream_gives_no_signal():
    env = Env()
    await env.push_all({"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
    env.clock.advance(0.5)
    await env.push_all({"binance": 100.0, "kucoin": 100.0})
    env.clock.advance(1.5)  # kraken data is now 2 s old (> max_data_age_ms): not a valid target
    await env.push("binance", 102.0)
    assert await env.push("kucoin", 102.0) == []
    assert env.opps.active() == [] and env.topic_names(events.SIGNAL) == []


def ticker(env: Env, ex: str, mid: float) -> Ticker:
    return Ticker(exchange=ex, symbol=SYMBOLS[ex], bid=mid - 0.01, ask=mid + 0.01, last=mid, ts=env.ts(),
                  asset="BTC", quote_volume=1e9)


async def poll_scenario(env: Env):
    """Polled tickers (no order books at all): binance + kucoin jump, kraken lags."""
    for ex in SYMBOLS:
        env.state.on_ticker(ticker(env, ex, 100.0))
    env.clock.advance(1.0)
    for ex in SYMBOLS:
        env.state.on_ticker(ticker(env, ex, 100.0))
    env.clock.advance(1.0)
    env.state.on_ticker(ticker(env, "binance", 102.0))
    assert env.detector.on_ticker(ticker(env, "binance", 102.0)) == []
    t = ticker(env, "kucoin", 102.0)
    env.state.on_ticker(t)
    signals = env.detector.on_ticker(t)
    assert len(signals) == 1 and signals[0].source == "poll" and signals[0].target == "kraken"
    return signals[0]


async def test_polled_signal_requires_rest_book_confirmation():
    env = Env(make_settings(confirm_with_rest_book=False))
    sig = await poll_scenario(env)
    assert await env.pipeline.evaluate(sig) is None  # no book -> no opportunity
    assert env.adapters["kraken"].rest_calls == []
    # REST confirmation: the fresh snapshot is stored and used for the economics
    env2 = Env(make_settings(confirm_with_rest_book=True))
    sig2 = await poll_scenario(env2)
    env2.adapters["kraken"].rest_book = env2.book("kraken", 100.0)
    opp = await env2.pipeline.evaluate(sig2)
    assert opp is not None and env2.adapters["kraken"].rest_calls == ["BTC/EUR"]
    assert env2.state.book("kraken", "BTC") is env2.adapters["kraken"].rest_book
    assert opp.signal.source == "poll" and opp.state is OppState.VALID and 0.5 < opp.econ.net_pct < 2.0
    # REST failure -> still no opportunity (no fake signals)
    env3 = Env(make_settings(confirm_with_rest_book=True))
    sig3 = await poll_scenario(env3)
    assert await env3.pipeline.evaluate(sig3) is None
    # a stale WS book is re-fetched the same way
    env4 = Env(make_settings(confirm_with_rest_book=True))
    env4.state.on_book(env4.book("kraken", 100.0))
    env4.clock.advance(3.0)
    sig4 = await poll_scenario(env4)
    sig4.source = "ws"
    env4.adapters["kraken"].rest_book = env4.book("kraken", 100.0)
    assert await env4.pipeline.evaluate(sig4) is not None
    assert env4.adapters["kraken"].rest_calls == ["BTC/EUR"]


async def test_target_catching_up_decays_net_and_expires():
    env = Env()
    opp = await detect(env)
    env.clock.advance(0.5)
    await env.push("kraken", 102.0)  # kraken caught up: lag gone
    assert opp.caught_up is True  # reaction listener -> mark_caught_up
    assert opp.econ.net_pct < 0 and opp.signal.lag_pct == pytest.approx(0.0)
    assert env.stats.get("BTC", "kraken").reaction_delays_s == [pytest.approx(0.5)]
    expired = await env.pipeline.sweep_once()
    assert expired == [opp] and opp.state is OppState.EXPIRED and opp.expire_reason == "net below threshold"
    assert opp.human_would_make_it is False  # 0.5 s window < 7 s
    assert env.stats.get("BTC", "kraken").human_window_total == 1
    assert env.topic_names(events.DASHBOARD) == [events.DASHBOARD]


async def test_first_mover_reverting_decays_opportunity():
    env = Env()
    opp = await detect(env)
    env.clock.advance(0.5)
    await env.push_all({"binance": 100.0, "kucoin": 100.0})
    env.clock.advance(0.2)
    await env.push("kraken", 100.0)  # no signal anymore -> refreshed against live data
    assert opp.econ.net_pct < 0
    assert (await env.pipeline.sweep_once())[0] is opp


async def test_refresh_for_executing_opportunity_returns_copy():
    env = Env()
    opp = await detect(env)
    await env.opps.transition(opp.id, OppState.AUTO_SELECTED)
    await env.opps.transition(opp.id, OppState.EXECUTING)
    env.clock.advance(0.3)
    env.state.on_book(env.book("kraken", 100.5))
    copy = await env.pipeline.refresh(opp, capital_quote=50.0)
    assert copy is not opp and copy.id == opp.id
    assert copy.econ.capital_quote == 50.0 and copy.econ.entry_vwap == pytest.approx(100.51005)
    assert opp.econ.entry_vwap == pytest.approx(100.01) and opp.update_count == 0
    assert len(copy.net_history) == len(opp.net_history) + 1
    assert copy.state is OppState.EXECUTING
    # a refresh of an active opp goes through upsert (OPP_UPDATE) with the given capital
    env2 = Env()
    opp2 = await detect(env2)
    env2.clock.advance(0.3)
    same = await env2.pipeline.refresh(opp2, capital_quote=50.0)
    assert same is opp2 and opp2.update_count == 1 and opp2.econ.capital_quote == 50.0
    assert env2.topic_names(events.OPP_UPDATE) == [events.OPP_UPDATE]


async def test_sweep_once_staleness_and_dashboard():
    env = Env()
    await detect(env)
    env.clock.advance(11.0)  # beyond stale_exchange_s
    await env.pipeline.sweep_once()
    health_events = [p for t, p in env.topics if t == events.HEALTH]
    assert {h.exchange for h in health_events} == set(SYMBOLS)
    assert all(h.stale for h in health_events)
    assert env.state.health("kraken") is env.adapters["kraken"].health
    assert not env.state.is_fresh("kraken", "BTC", env.clock.mono(), 100_000.0)
    env.topics.clear()
    await env.pipeline.sweep_once()
    assert [t for t, _ in env.topics if t == events.HEALTH] == []  # only on change
    env.adapters["kraken"].mark_msg()
    await env.pipeline.sweep_once()
    assert len([t for t, _ in env.topics if t == events.HEALTH]) == 1
    d = env.pipeline.dashboard()
    assert d["mode"] == "paper" and d["avg_latency_ms"] == pytest.approx(200.0)
    for k in ("active", "today", "green", "yellow", "red", "human_executable", "bot_executable", "telegram_alerts",
              "paper_pnl", "live_pnl", "realized_pnl", "failed_trades", "expired", "avg_execution_time_s",
              "success_rate"):
        assert k in d


async def test_dashboard_merges_db_counters():
    class StubDb:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def counters_today(self, mode: str) -> dict:
            self.calls.append(mode)
            return {"opportunities_today": 5, "trades": 4, "failed": 1, "expired": 3,
                    "realized_pnl": 2.5 if mode == "paper" else -1.0}

        def save_opportunity(self, opp) -> None:
            pass

    db = StubDb()
    env = Env(db=db)
    d = env.pipeline.dashboard()
    assert set(db.calls) == {"paper", "live"}
    assert d["paper_pnl"] == 2.5 and d["live_pnl"] == -1.0 and d["realized_pnl"] == 2.5
    assert d["failed_trades"] == 1 and d["today"] == 5 and d["expired"] == 3
    assert d["success_rate"] == pytest.approx(0.75)


async def test_start_stop_subscriptions_and_sweep_loop():
    env = Env()
    await env.pipeline.start()
    await env.warm_up()
    for ex, p in (("binance", 102.0), ("kucoin", 102.0)):
        env.adapters[ex].mark_msg()
        await env.bus.publish(events.BOOK, env.book(ex, p))
    assert len(env.opps.active()) == 1
    opp = env.opps.active()[0]
    # tickers / trades / health route into the state
    env.state.on_health(ExchangeHealth(exchange="x"))
    await env.bus.publish(events.HEALTH, ExchangeHealth(exchange="binance", ws_connected=True))
    assert env.state.health("binance") is not None
    await env.bus.publish(events.TICKER, Ticker(exchange="kucoin", symbol="BTC/USDT", bid=101.9, ask=102.1, last=102.0,
                                                ts=env.ts(), asset="BTC", quote_volume=1e9))
    assert env.state.volume24h("kucoin", "BTC") == 1e9
    # the sweep loop runs on the event loop and expires the opp once its window closes
    env.clock.advance(30.0)
    await asyncio.sleep(0.2)
    assert opp.state is OppState.EXPIRED
    assert events.DASHBOARD in env.topic_names()
    await env.pipeline.stop()
    await env.pipeline.stop()  # idempotent
    env.topics.clear()
    late = env.book("kraken", 100.0)
    await env.bus.publish(events.BOOK, late)
    assert env.state.book("kraken", "BTC") is not late  # unsubscribed
    assert env.topic_names() == [events.BOOK]


async def test_fees_fallback_to_config_and_no_stats():
    env = Env(with_stats=False)
    env.adapters["kraken"].fail_fees = True
    opp = await detect(env)
    assert opp.econ.entry_fee_pct == pytest.approx(0.4)  # ExchangeConfig taker 0.004
    assert opp.human.stats_n == 0 and opp.bot.stats_n == 0
    env2 = Env()
    del env2.adapters["kraken"]
    opp2 = await detect(env2)
    assert opp2.econ.entry_fee_pct == pytest.approx(0.4)
    assert opp2.bot.light is Light.RED  # no health information without an adapter
