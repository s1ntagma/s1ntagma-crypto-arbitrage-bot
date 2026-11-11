"""Crossed order books (best bid >= best ask) are data errors, never prices (hard rule 1).

Found by the cloud training run: after WebSocket reconnects Bitvavo's cached book kept stale asks
while the bids moved on, so 61 of 80 virtual trades bought at a stale ask and sold above it - fake
profit that would have proven pairs and opened READY for real money."""
from __future__ import annotations

import asyncio

import test_execution as tx
from latbot import events
from latbot.clock import SimClock
from latbot.config import Settings
from latbot.engine.diagnostics import Diagnostics
from latbot.engine.economics import compute_economics
from latbot.engine.shadow import PERIOD_KEY, ShadowTrader
from latbot.engine.stats import StatsStore
from latbot.events import EventBus
from latbot.market.state import MarketState
from latbot.models import BookLevel, Fees, OrderBook, Timestamps
from latbot.storage.db import Database
from test_ccxt_adapter import Harness, spot_cfg

CROSSED = {"bids": [[105.0, 1.0], [104.0, 1.0]], "asks": [[103.0, 1.0], [106.0, 1.0]], "timestamp": None}
SANE = {"bids": [[101.0, 1.0]], "asks": [[102.0, 1.0]], "timestamp": None}


def book(bid: float, ask: float, clock: SimClock | None = None, exchange: str = "bitvavo") -> OrderBook:
    clock = clock or SimClock()
    return OrderBook(exchange=exchange, symbol="BTC/EUR", bids=[BookLevel(bid, 1.0)], asks=[BookLevel(ask, 1.0)],
                     ts=Timestamps(recv_ts=clock.wall(), recv_mono=clock.mono()), asset="BTC")


def test_is_crossed():
    assert book(105.0, 103.0).is_crossed and book(103.0, 103.0).is_crossed  # locked = crossed: cannot rest
    assert not book(101.0, 102.0).is_crossed
    empty = book(101.0, 102.0)
    empty.asks = []
    assert not empty.is_crossed


# ---- the adapter never publishes a crossed WS book; one REST snapshot replaces it ---------------------
async def test_crossed_ws_book_is_dropped_and_rest_snapshot_used():
    h = Harness(spot_cfg())
    h.public.books = [dict(CROSSED)]
    h.public.rest_book = dict(SANE)
    await h.adapter.start(["BTC"])
    try:
        await asyncio.sleep(0.05)
    finally:
        await h.adapter.stop()
    books = h.of(events.BOOK)
    assert [b.is_broken for b in books] == [True, False]  # the broken one first (invalidation), then the REST snapshot
    assert books[1].best_bid == 101.0
    assert h.adapter.crossed_books == 1
    assert not h.adapter.order_book("BTC").is_crossed


async def test_crossed_ws_book_without_a_sane_snapshot_leaves_no_price():
    h = Harness(spot_cfg())
    h.public.books = [dict(SANE), dict(CROSSED)]
    h.public.rest_book = dict(CROSSED)
    await h.adapter.start(["BTC"])
    try:
        await asyncio.sleep(0.05)
    finally:
        await h.adapter.stop()
    assert [b.is_broken for b in h.of(events.BOOK)] == [False, True]  # the sane book, then the invalidation
    assert h.adapter.order_book("BTC") is None  # the last sane book is not kept as if it were current
    assert h.adapter.crossed_books >= 1


async def test_crossed_ws_book_is_resubscribed_for_a_fresh_snapshot():
    from test_ccxt_adapter import FakeCcxt

    class Unwatchable(FakeCcxt):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            self.has = dict(self.has, unWatchOrderBook=True)
            self.unwatched: list[str] = []

        async def un_watch_order_book(self, symbol, params=None):
            self.unwatched.append(symbol)

    h = Harness(spot_cfg(), fake_cls=Unwatchable)
    h.public.books = [dict(CROSSED), dict(CROSSED)]
    await h.adapter.start(["BTC"])
    try:
        await asyncio.sleep(0.05)
    finally:
        await h.adapter.stop()
    assert h.public.unwatched == ["BTC/EUR"]  # once per 30 s, not per message


# ---- every consumer refuses one --------------------------------------------------------------------------
def test_market_state_ignores_a_crossed_book():
    clock = SimClock()
    st = MarketState(Settings(), clock)
    st.on_book(book(105.0, 103.0, clock))
    assert st.book("bitvavo", "BTC") is None and not st.is_fresh("bitvavo", "BTC", clock.mono(), 1500.0)


def test_economics_refuse_a_crossed_book():
    clock = SimClock()
    e = compute_economics(book(105.0, 103.0, clock), 1, Fees(maker=0.001, taker=0.001), 100.0, 1.0, Settings(), clock.mono())
    assert not e.liquidity_ok and "bid >= ask" in e.liquidity_reason and e.executable_quote == 0.0


def test_paper_venue_never_fills_from_a_crossed_book():
    from latbot.exchanges.paper import PaperAdapter

    env = tx.make_env()
    inner = env.adapters["testex"]
    inner.books["BTC/USDT"] = book(105.0, 103.0, env.clock, exchange="testex")
    inner.books["BTC/USDT"].symbol = "BTC/USDT"
    paper = PaperAdapter(inner, env.settings, env.bus, env.clock)
    assert paper._fresh_book("BTC/USDT") is None


async def test_pipeline_ignores_a_crossed_book():
    import test_pipeline as tp

    env = tp.Env(tp.make_settings())
    await env.warm_up()
    crossed = book(105.0, 103.0, env.clock, exchange="kraken")
    assert await env.pipeline.on_book(crossed) == []
    assert env.pipeline.diag.snapshot()["exchanges"]["kraken"]["crossed"] == 1


# ---- training data that may sit on crossed books starts over once ----------------------------------
def test_stats_period_starts_over_once_for_this_build():
    clock = SimClock()
    db = Database(":memory:", clock=clock)
    db.kv_set(PERIOD_KEY, clock.wall() - 86400.0)  # a period from an older build
    sh = ShadowTrader(Settings(), EventBus(), clock, db=db)
    assert sh._period_start == clock.wall()
    clock.advance(100.0)
    again = ShadowTrader(Settings(), EventBus(), clock, db=db)
    assert again._period_start == sh._period_start  # only once


def test_catch_up_ratios_measured_on_crossed_books_are_dropped_once():
    db = Database(":memory:", clock=SimClock())
    s = Settings()
    st = StatsStore(s, db)
    for _ in range(3):
        st.record_catchup("BTC", "bitvavo", 0.9)
    st.save()
    db.kv_set("catchup_unbiased_v2", True)
    db.kv_delete("catchup_clean_books_v3")  # these samples were recorded by an older build
    fresh = StatsStore(s, db)
    assert fresh.catchup_ratio("BTC", "bitvavo") is None
    fresh.record_catchup("BTC", "bitvavo", 0.4)
    fresh.save()
    assert StatsStore(s, db).get("BTC", "bitvavo").catchup_ratios == [0.4]


# ---- DIAG counts signals the way it counts rejections ----------------------------------------------
def test_signals_are_deduplicated_like_rejections():
    clock = SimClock()
    d = Diagnostics(Settings(), clock=clock)
    for _ in range(50):  # one lag re-evaluated on every book update
        d.signal("BTC", "bitvavo", 1)
    assert d.signals == 1
    clock.advance(60.0)
    d.signal("BTC", "bitvavo", 1)
    assert d.signals == 2
