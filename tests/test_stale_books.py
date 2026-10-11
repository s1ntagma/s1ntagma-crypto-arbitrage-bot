"""Stale order-book caches: a crossed book invalidates the stream everywhere, crossed REST snapshots
and crossed tickers are refused, the ccxt cache is dropped on reconnects, a trade print outside the
cached top of book marks it broken, and the learned data of older builds starts over once.
(Review of 58a300b: 7 verified findings.)"""
from __future__ import annotations

import asyncio

import test_execution as tx
import test_pipeline as tp
from latbot import events
from latbot.clock import SimClock
from latbot.config import Settings
from latbot.engine.economics import compute_economics
from latbot.engine.metalabel import MetaLabelStore
from latbot.engine.shadow import ShadowTrader
from latbot.engine.stats import StatsStore
from latbot.market.state import MarketState
from latbot.models import Fees, OppState, Ticker, Timestamps
from latbot.storage.db import Database
from test_ccxt_adapter import FakeCcxt, Harness, spot_cfg
from test_crossed_books import SANE, book


def ticker(bid: float, ask: float, clock: SimClock, exchange: str = "revolutx") -> Ticker:
    return Ticker(exchange=exchange, symbol="BTC/EUR", bid=bid, ask=ask, last=(bid + ask) / 2,
                  ts=Timestamps(recv_ts=clock.wall(), recv_mono=clock.mono()), asset="BTC")


# ---- 1. a crossed book means "no current price" in MarketState, not only in the adapter ----------
def test_crossed_book_invalidates_the_previous_book_until_a_sane_one():
    clock = SimClock()
    st = MarketState(Settings(), clock)
    st.on_book(book(99.99, 100.01, clock))
    clock.advance(0.2)
    st.on_book(book(101.98, 100.01, clock))  # stale ask: the stream is broken
    assert st.book("bitvavo", "BTC") is None
    assert not st.is_fresh("bitvavo", "BTC", clock.mono(), 1500.0)
    st.on_ticker(ticker(101.9, 102.0, clock, "bitvavo"), append_mid=False)  # a polled ticker: liveness only
    assert not st.is_fresh("bitvavo", "BTC", clock.mono(), 1500.0)
    assert st.mid_series("bitvavo", "BTC")[-1][1] == 100.0  # no mid from the broken period
    clock.advance(0.2)
    st.on_book(book(101.99, 102.01, clock))  # the first sane book clears the mark
    assert st.book("bitvavo", "BTC") is not None and st.is_fresh("bitvavo", "BTC", clock.mono(), 1500.0)


async def test_pipeline_no_fake_lag_after_a_crossed_target_book():
    """Sane kraken book, crossed kraken book, leaders +2 %, sane kraken book at 102: no opportunity,
    no virtual trade - the laggard moved with the leaders, its cache was just broken."""
    env = tp.Env(tp.make_settings())
    sh = ShadowTrader(env.settings, env.bus, env.clock)
    await env.warm_up()
    crossed = book(101.98, 100.01, env.clock, exchange="kraken")
    assert await env.pipeline.on_book(crossed) == []
    env.clock.advance(0.1)
    assert await env.push("binance", 102.0) == []
    assert await env.push("kucoin", 102.0) == []  # kraken has no current price: nothing to measure a lag on
    env.clock.advance(0.3)
    assert await env.push("kraken", 102.0) == []
    assert env.opps.active() == [] and sh.trades() == []
    assert env.pipeline.diag.snapshot()["funnel"]["rejected"].get("target_no_data", 0) >= 1


async def test_active_opportunity_expires_when_the_target_book_breaks():
    env = tp.Env(tp.make_settings())
    sh = ShadowTrader(env.settings, env.bus, env.clock)
    opp = await tp.detect(env)
    assert sh.trades() and sh.trades()[0]["open"]
    env.clock.advance(0.1)
    await env.pipeline.on_book(book(101.98, 100.01, env.clock, exchange="kraken"))
    assert env.opps.get(opp.id).state is OppState.EXPIRED and "broken" in env.opps.get(opp.id).expire_reason
    row = sh.trades()[0]
    assert row["open"] is False and row["barrier"] == "no_data"  # unknown outcome, never a win


# ---- 2. crossed snapshots / economics never export a price ----------------------------------------
def test_crossed_economics_export_no_prices():
    clock = SimClock()
    e = compute_economics(book(105.0, 103.0, clock), 1, Fees(maker=0.001, taker=0.001), 100.0, 1.0, Settings(), clock.mono())
    assert not e.liquidity_ok and e.entry_vwap == 0.0 and e.exit_vwap_now == 0.0 and e.exit_target == 0.0
    assert e.entry_best == 0.0 and e.exit_best == 0.0


async def test_pipeline_refuses_a_crossed_rest_snapshot():
    env = tp.Env(tp.make_settings())
    opp = await tp.detect(env)
    env.clock.advance(2.0)  # the stored kraken book is older than max_data_age_ms
    env.adapters["kraken"].rest_book = book(105.0, 103.0, env.clock, exchange="kraken")
    assert await env.pipeline.refresh(opp) is None
    assert env.state.book("kraken", "BTC") is None
    assert env.pipeline.diag.snapshot()["exchanges"]["kraken"]["crossed"] >= 1


async def test_engine_refuses_a_crossed_rest_snapshot():
    env = tx.make_env()
    a = env.adapters["testex"]
    env.clock.advance(5.0)  # cached book stale -> REST
    crossed = book(105.0, 103.0, env.clock, exchange="testex")
    crossed.symbol = "BTC/USDT"

    async def rest(symbol, limit=20):
        return crossed

    a.fetch_order_book = rest
    b, age = await env.engine._fresh_book(a, "BTC/USDT")
    assert b is None and age is None


async def test_manual_support_reports_no_data_on_a_crossed_book():
    env = tx.make_env()
    opp = env.opps.add(tx.make_opp(env.clock))
    a = env.adapters["testex"]
    crossed = book(105.0, 103.0, env.clock, exchange="testex")
    crossed.symbol = "BTC/USDT"
    a.books["BTC/USDT"] = crossed

    async def rest(symbol, limit=20):
        return crossed  # the venue keeps answering with the broken book

    a.fetch_order_book = rest
    p = await env.engine.manual_support(opp.id)
    assert p["status"] == "no_data" and p["valid"] is False


# ---- 6. crossed tickers --------------------------------------------------------------------------
def test_crossed_ticker_gives_no_mid_exit_or_leader_price():
    clock = SimClock()
    st = MarketState(Settings(), clock)
    st.on_ticker(ticker(100.0, 100.2, clock))
    clock.advance(0.1)
    st.on_ticker(ticker(101.0, 100.0, clock))  # bid >= ask
    assert st.mid_series("revolutx", "BTC")[-1][1] == 100.1  # no point from the crossed ticker
    env = tp.Env(tp.make_settings())
    t = ticker(101.0, 100.0, env.clock)
    assert env.pipeline.target_exit_price(None, t, 1.0, 1, env.clock.mono(), 5000.0) is None
    env.state.on_ticker(ticker(101.0, 100.0, env.clock, "binance"))
    assert env.pipeline.leader_price("BTC", "binance") is None


# ---- 3 / 7. the ccxt cache: dropped on reconnect, broken by a print outside the top of book --------
class Reconnecting(FakeCcxt):
    """ccxt.pro keeps ``orderbooks[symbol]`` across reconnects; the adapter must drop it."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.orderbooks: dict = {"BTC/EUR": {"nonce": 100}}
        self.has = dict(self.has, unWatchOrderBook=True)
        self.unwatched: list[str] = []

    async def un_watch_order_book(self, symbol, params=None):
        self.unwatched.append(symbol)

    async def watch_order_book(self, symbol, limit=None, params=None):
        clock = getattr(self, "clock", None)
        if clock is not None:
            clock.advance(0.06)  # the adapter's clock moves between two books (periodic re-subscribe)
        return await self._next(self.books)


async def test_ccxt_book_cache_is_dropped_before_rewatching_after_an_error():
    h = Harness(spot_cfg(), fake_cls=Reconnecting, backoff_min_s=0.01, backoff_max_s=0.02)
    h.public.books = [dict(SANE), RuntimeError("socket closed"), dict(SANE)]
    await h.adapter.start(["BTC"])
    try:
        await asyncio.sleep(0.1)
    finally:
        await h.adapter.stop()
    assert "BTC/EUR" not in h.public.orderbooks  # popped on the error path, before the re-watch
    assert len([b for b in h.of(events.BOOK) if not b.is_crossed]) == 2


async def test_a_print_outside_the_cached_top_of_book_marks_it_broken():
    h = Harness(spot_cfg(), fake_cls=Reconnecting)
    h.public.books = [dict(SANE)]  # bid 101 / ask 102 stays cached
    h.public.rest_book = {"bids": [[102.4, 1.0]], "asks": [[102.6, 1.0]], "timestamp": None}
    h.public.trades = [[{"id": "t1", "price": 102.5, "amount": 0.2, "side": "buy", "timestamp": 1_700_000_000_000}]]
    await h.adapter.start(["BTC"])
    try:
        await asyncio.sleep(0.1)
    finally:
        await h.adapter.stop()
    assert h.adapter.crossed_books == 1 and h.public.unwatched == ["BTC/EUR"]
    books = h.of(events.BOOK)
    assert books[-1].best_bid == 102.4  # the REST snapshot replaced the stale cache
    assert h.adapter.order_book("BTC").best_bid == 102.4


async def test_books_are_resubscribed_periodically():
    h = Harness(spot_cfg(), settings=Settings(ws_book_resubscribe_s=0.05), fake_cls=Reconnecting)
    h.public.books = [dict(SANE) for _ in range(6)]
    await h.adapter.start(["BTC"])
    h.public.clock = h.clock  # the loop has not run yet: every book advances the clock by 0.06 s
    try:
        await asyncio.sleep(0.1)
    finally:
        await h.adapter.stop()
    assert h.public.unwatched.count("BTC/EUR") >= 2 and "BTC/EUR" not in h.public.orderbooks


# ---- 4 / 5. learned data of older builds starts over once ------------------------------------------
def test_metalabel_outcomes_of_older_builds_are_cleared_once():
    clock = SimClock()
    db = Database(":memory:", clock=clock)
    db.kv_set("metalabel_clean_books_v1", True)
    first = MetaLabelStore(Settings(), clock, db)
    for i in range(25):
        first.record({"id": f"m{i}", "asset": "BTC", "target": "bitvavo", "first_mover": "htx", "lag_pct": 1.0,
                      "outcome": "win", "barrier": "time", "net_pct": 0.5, "ts": clock.wall()})
    assert len(MetaLabelStore(Settings(), clock, db)) == 25
    db.kv_delete("metalabel_clean_books_v1")  # a DB written by a build before the crossed-book fix
    fresh = MetaLabelStore(Settings(), clock, db)
    assert len(fresh) == 0 and len(db.load_metalabel_outcomes()) == 0
    assert db.kv_get("metalabel_clean_books_v1")
    fresh.record({"id": "new", "asset": "BTC", "target": "bitvavo", "first_mover": "htx", "lag_pct": 1.0,
                  "outcome": "loss", "barrier": "time", "net_pct": -0.2, "ts": clock.wall()})
    assert len(MetaLabelStore(Settings(), clock, db)) == 1


def test_stats_measured_on_broken_books_are_cleared_once_keeping_real_trades():
    db = Database(":memory:", clock=SimClock())
    s = Settings()
    st = StatsStore(s, db)
    st.record_lagging("BTC", "bitvavo", 1.0)
    st.record_reaction("BTC", "bitvavo", 0.5, 0.8)
    st.record_catchup("BTC", "bitvavo", 0.9)
    rs = st.get("BTC", "bitvavo")
    rs.opp_durations_s.append(12.0)
    rs.caught_up_total, rs.caught_up_ok, rs.human_window_total, rs.human_window_ok = 4, 3, 4, 2
    rs.false_signal_count, rs.first_mover_count = 2, 1
    rs.manual_total, rs.manual_wins, rs.success_count = 3, 2, 1
    st.save()
    db.kv_set("catchup_unbiased_v2", True)
    db.kv_delete("catchup_clean_books_v3")
    fresh = StatsStore(s, db).get("BTC", "bitvavo")
    assert fresh.catchup_ratios == [] and fresh.reaction_delays_s == [] and fresh.opp_durations_s == []
    assert fresh.lag_sizes_pct == [] and fresh.caught_up_total == 0 and fresh.human_window_ok == 0
    assert fresh.false_signal_count == 0 and fresh.lagging_count == 0 and fresh.first_mover_count == 0
    assert fresh.manual_total == 3 and fresh.manual_wins == 2 and fresh.success_count == 1  # real trades stay
