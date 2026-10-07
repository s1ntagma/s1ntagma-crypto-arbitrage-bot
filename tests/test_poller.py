"""Offline tests for latbot.market.poller.TickerPoller through the real wiring
(build_context + FakeAdapters): volumes feed the Phase-2 gates, polled moves
become opportunities only after REST book confirmation, backoff on errors."""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from latbot import events
from latbot.app import build_context
from latbot.clock import SimClock
from latbot.config import ExchangeConfig, Secrets, Settings
from latbot.events import EventBus
from latbot.exchanges.base import RateLimited
from latbot.exchanges.fake import FakeAdapter
from latbot.market.poller import TickerPoller
from latbot.models import ExchangeRole, Light, OppState

SYMBOLS = {"binance": "BTC/USDT", "kucoin": "BTC/USDT", "kraken": "BTC/EUR"}


def make_settings(tmp_path: Path, **over: Any) -> Settings:
    base: dict[str, Any] = dict(
        db_path=":memory:",
        watchlist=["BTC"],
        exchanges=[
            ExchangeConfig(name="binance", role=ExchangeRole.SIGNAL, stream="bbo"),
            ExchangeConfig(name="kucoin", role=ExchangeRole.SIGNAL, stream="bbo"),
            ExchangeConfig(name="kraken", role=ExchangeRole.EXECUTION, taker_fee=0.001, maker_fee=0.001),
        ],
        min_movers=2,
        min_mover_volume_quote=1_000_000.0,  # real gate: volume must come from tickers
        min_target_volume_quote=100_000.0,
        ticker_poll_enabled=True,
        ticker_poll_interval_s=2.0,
        default_opp_duration_s=30.0,
        record_market_data=False,
        log_dir=str(tmp_path / "logs"),
        port=0,
    )
    base.update(over)
    return Settings(**base)


class Env:
    def __init__(self, tmp_path: Path, **over: Any) -> None:
        self.bus, self.clock = EventBus(), SimClock()
        self.fakes: dict[str, FakeAdapter] = {}

        def factory(cfg: ExchangeConfig, creds: Any) -> FakeAdapter:
            a = FakeAdapter(cfg, self.bus, self.clock, symbols={"BTC": SYMBOLS[cfg.name]})
            self.fakes[cfg.name] = a
            return a

        self.ctx = build_context(make_settings(tmp_path, **over), Secrets(), bus=self.bus, clock=self.clock,
                                 adapter_factory=factory, config_path=tmp_path / "config.json")
        self.poller: TickerPoller = self.ctx.poller
        self.topics: list[tuple[str, Any]] = []
        self.bus.subscribe_all(lambda t, p: self.topics.append((t, p)))

    def book(self, name: str, mid: float, volume: float = 5_000_000.0) -> None:
        """Store a book + 24h volume in the fake WITHOUT publishing (pure REST world)."""
        fake = self.fakes[name]
        bids = [(mid * (1 - 0.0001 * k), 50.0) for k in (1, 2, 3)]
        asks = [(mid * (1 + 0.0001 * k), 50.0) for k in (1, 2, 3)]
        from latbot.exchanges._fill import levels_from_pairs
        from latbot.models import OrderBook, Timestamps

        fake._books["BTC"] = OrderBook(exchange=name, symbol=SYMBOLS[name], bids=levels_from_pairs(bids, descending=True),
                                       asks=levels_from_pairs(asks, descending=False),
                                       ts=Timestamps(recv_ts=self.clock.wall(), recv_mono=self.clock.mono()), asset="BTC")
        fake.volumes["BTC"] = volume
        fake._mark_msg(None, self.clock.wall(), self.clock.mono())

    def books(self, prices: dict[str, float]) -> None:
        for name, mid in prices.items():
            self.book(name, mid)


def test_build_context_creates_the_poller(tmp_path: Path) -> None:
    env = Env(tmp_path)
    assert isinstance(env.poller, TickerPoller)
    assert env.poller.running is False and env.poller.assets is None
    env.ctx.db.close()


async def test_poll_feeds_volume_and_mid_into_market_state(tmp_path: Path) -> None:
    env = Env(tmp_path)
    env.books({"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
    n = await env.poller.poll_once()
    assert n == 3 and env.poller.polls == 1 and env.poller.tickers_seen == 3
    state = env.ctx.state
    for name in SYMBOLS:
        assert state.volume24h(name, "BTC") == pytest.approx(5_000_000.0)
        assert state.ticker(name, "BTC") is not None
        assert list(state.mid_series(name, "BTC"))[-1][1] == pytest.approx(100.0)
    env.ctx.db.close()


async def test_polled_move_becomes_opportunity_after_rest_confirmation(tmp_path: Path) -> None:
    env = Env(tmp_path)
    env.books({"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
    await env.poller.poll_once()
    env.clock.advance(0.5)
    env.books({"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
    await env.poller.poll_once()
    env.clock.advance(1.0)
    env.books({"binance": 102.0, "kucoin": 102.0, "kraken": 100.0})
    await env.poller.poll_once()
    assert env.poller.signals_seen >= 1 and env.ctx.pipeline.pending_confirmations == 1
    await env.ctx.pipeline.wait_confirmations()  # the REST book confirmation runs in the background
    signals = [p for t, p in env.topics if t == events.SIGNAL]
    opps = env.ctx.opps.active()
    assert len(opps) == 1, [p for t, p in env.topics]
    opp = opps[0]
    assert opp.signal.source == "poll" and opp.target_exchange == "kraken" and opp.signal.movers_count == 2
    assert opp.signal.mover_volumes_quote  # the gate saw real volumes
    assert opp.econ.book_age_ms <= 1.0  # economics on the freshly fetched REST book
    assert env.ctx.state.book("kraken", "BTC") is not None
    assert opp.state in (OppState.VALID, OppState.NOTIFIED) and opp.bot.light is not Light.RED
    assert not signals  # poll signals are not published on the SIGNAL topic (only evaluated)
    # the next round finds the fresh REST book and refreshes the same opportunity synchronously
    env.clock.advance(1.0)
    await env.poller.poll_once()
    assert env.poller.evaluated >= 1 and env.ctx.opps.active() == [opp]
    env.ctx.db.close()


async def test_volume_gate_blocks_without_tickers(tmp_path: Path) -> None:
    """Same move, but no poller round -> movers have unknown volume -> no opportunity (no fake signals)."""
    env = Env(tmp_path)
    await env.ctx.pipeline.start()
    try:
        for prices in ({"binance": 100.0, "kucoin": 100.0, "kraken": 100.0},) * 2 + ({"binance": 102.0, "kucoin": 102.0, "kraken": 100.0},):
            for name, mid in prices.items():
                bids = [(mid * (1 - 0.0001 * k), 50.0) for k in (1, 2, 3)]
                asks = [(mid * (1 + 0.0001 * k), 50.0) for k in (1, 2, 3)]
                await env.fakes[name].push_book("BTC", bids, asks)
            env.clock.advance(0.7)
        assert env.ctx.opps.active() == []
    finally:
        await env.ctx.pipeline.stop()
        env.ctx.db.close()


async def test_streamed_assets_are_not_detected_twice(tmp_path: Path) -> None:
    env = Env(tmp_path)
    await env.fakes["kraken"].start(["BTC"])  # kraken streams BTC over "WebSocket"
    env.books({"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
    calls: list[tuple[str, str]] = []
    original = env.ctx.detector.on_ticker

    def spy(ticker, source="poll"):
        calls.append((ticker.exchange, source))
        return original(ticker, source=source)

    env.ctx.detector.on_ticker = spy  # type: ignore[method-assign]
    await env.poller.poll_once()
    # detection is asset-wide: once per asset per round, triggered by a non-streamed exchange only
    assert len(calls) == 1 and calls[0][1] == "poll" and calls[0][0] in ("binance", "kucoin")
    assert env.ctx.state.volume24h("kraken", "BTC") == pytest.approx(5_000_000.0)  # volume still recorded
    assert env.ctx.state.ticker("kraken", "BTC") is not None
    env.ctx.db.close()


async def test_set_assets_restricts_polling(tmp_path: Path) -> None:
    env = Env(tmp_path)
    env.books({"binance": 100.0})
    env.fakes["binance"].volumes["ETH"] = 1.0
    env.poller.set_assets(["btc", "BTC", " "])
    assert env.poller.assets == ["BTC"]
    n = await env.poller.poll_once()
    assert n == 1 and env.ctx.state.volume24h("binance", "ETH") is None
    env.poller.set_assets(None)
    n = await env.poller.poll_once()
    assert n == 2 and env.ctx.state.volume24h("binance", "ETH") == pytest.approx(1.0)
    env.ctx.db.close()


async def test_backoff_on_errors_and_rate_limits(tmp_path: Path) -> None:
    env = Env(tmp_path)
    env.books({"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
    env.fakes["binance"].fail_fetch_tickers = True
    assert await env.poller.poll_once() == 2  # binance skipped, others fine
    env.fakes["binance"].fail_fetch_tickers = False
    assert await env.poller.poll_once() == 2  # still in backoff (2 s)
    env.clock.advance(2.1)
    assert await env.poller.poll_once() == 3  # recovered
    # escalating backoff: 2 -> 4 s
    env.fakes["binance"].fail_fetch_tickers = True
    await env.poller.poll_once()
    env.clock.advance(2.1)
    await env.poller.poll_once()
    env.fakes["binance"].fail_fetch_tickers = False
    env.clock.advance(2.1)
    assert await env.poller.poll_once() == 2  # 4 s backoff not over
    env.clock.advance(2.0)
    assert await env.poller.poll_once() == 3

    async def limited(*_a: Any, **_k: Any) -> dict:
        raise RateLimited(retry_after_s=7.0)

    env.fakes["kucoin"].fetch_tickers = limited  # type: ignore[method-assign]
    assert await env.poller.poll_once() == 2
    env.clock.advance(6.0)
    assert await env.poller.poll_once() == 2  # kucoin waits retry_after_s
    env.ctx.db.close()


async def test_rate_limited_health_pauses_polling_of_that_exchange(tmp_path: Path) -> None:
    """A ban discovered by another call on the same exchange (e.g. the private
    instance, same IP budget) keeps the poller away until it is over."""
    env = Env(tmp_path)
    env.books({"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
    env.fakes["kucoin"].health.rate_limited_until_mono = env.clock.mono() + 30.0
    assert await env.poller.poll_once() == 2
    env.clock.advance(10.0)
    assert await env.poller.poll_once() == 2
    env.clock.advance(21.0)
    assert await env.poller.poll_once() == 3
    env.ctx.db.close()


async def test_disabled_exchange_and_poll_tickers_flag_are_respected(tmp_path: Path) -> None:
    env = Env(tmp_path)
    env.books({"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
    env.ctx.settings.exchange("binance").poll_tickers = False
    env.ctx.settings.exchange("kucoin").enabled = False
    assert [n for n, _ in env.poller._targets()] == ["kraken"]
    assert await env.poller.poll_once() == 1
    env.ctx.db.close()


async def test_start_stop_lifecycle(tmp_path: Path) -> None:
    env = Env(tmp_path, ticker_poll_interval_s=0.05)
    env.books({"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
    await env.poller.start(["BTC"])
    assert env.poller.running and env.poller.assets == ["BTC"]
    for _ in range(50):
        if env.poller.polls >= 2:
            break
        await asyncio.sleep(0.02)
    assert env.poller.polls >= 2
    await env.poller.stop()
    assert env.poller.running is False
    polls = env.poller.polls
    await asyncio.sleep(0.12)
    assert env.poller.polls == polls
    env.ctx.db.close()


async def test_start_is_noop_when_polling_disabled(tmp_path: Path) -> None:
    env = Env(tmp_path, ticker_poll_enabled=False)
    await env.poller.start()
    assert env.poller.running is False
    await env.poller.stop()
    env.ctx.db.close()


async def test_polled_ticker_does_not_overwrite_streamed_mid(tmp_path: Path) -> None:
    """A REST ticker is 1-2 s old; for assets the exchange streams over WebSocket
    it must never become the newest point of the live mid series."""
    env = Env(tmp_path)
    await env.ctx.pipeline.start()
    try:
        for name in SYMBOLS:
            await env.fakes[name].start(["BTC"])  # every exchange streams BTC
        env.books({"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
        await env.poller.poll_once()  # volumes for the gates

        async def ws(name: str, mid: float) -> None:
            bids = [(mid * (1 - 0.0001 * k), 50.0) for k in (1, 2, 3)]
            asks = [(mid * (1 + 0.0001 * k), 50.0) for k in (1, 2, 3)]
            await env.fakes[name].push_book("BTC", bids, asks)

        for name in SYMBOLS:
            await ws(name, 100.0)
        env.clock.advance(0.5)
        for name in SYMBOLS:
            await ws(name, 100.0)
        env.clock.advance(1.0)
        await ws("binance", 102.0)
        await ws("kucoin", 102.0)
        [opp] = env.ctx.opps.active()
        assert opp.signal.lag_pct == pytest.approx(2.0) and opp.first_mover == "binance"
        # the poller round: binance's REST ticker still carries the pre-move price
        env.clock.advance(0.2)
        stale = env.fakes["binance"]._ticker_of("BTC")
        stale.bid, stale.ask, stale.last = 99.99, 100.01, 100.0

        async def stale_tickers(*_a: Any, **_k: Any) -> dict:
            return {"BTC": stale}

        env.fakes["binance"].fetch_tickers = stale_tickers  # type: ignore[method-assign]
        await env.poller.poll_once()
        assert list(env.ctx.state.mid_series("binance", "BTC"))[-1][1] == pytest.approx(102.0)
        assert env.ctx.state.volume24h("binance", "BTC") == pytest.approx(5_000_000.0)
        await ws("kraken", 100.0)  # a target book refreshes the live opportunity against the series
        assert opp.signal.lag_pct == pytest.approx(2.0) and opp.econ.net_pct > 0 and opp.state is OppState.VALID
        assert await env.ctx.pipeline.sweep_once() == []
        await ws("binance", 102.0)
        assert [o.id for o in env.ctx.opps.active()] == [opp.id]
        assert [t for t, _ in env.topics if t == events.OPP_NEW] == [events.OPP_NEW]
    finally:
        await env.ctx.pipeline.stop()
        env.ctx.db.close()


class _StubState:
    def __init__(self) -> None:
        self.tickers: list[tuple[str, str, bool]] = []

    def on_ticker(self, ticker: Any, *, append_mid: bool = True) -> None:
        self.tickers.append((ticker.exchange, ticker.asset, append_mid))


class _StubDetector:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def on_ticker(self, ticker: Any, source: str = "poll") -> list:
        self.calls.append((ticker.exchange, ticker.asset))
        return []


class _StubAdapter:
    def __init__(self, name: str, assets: list[str], clock: SimClock, streamed: set[str] | None = None) -> None:
        self.name = name
        self._assets = assets
        self._clock = clock
        self._streamed = streamed or set()

    def supported_symbols(self) -> set[str]:
        return {f"{a}/USDT" for a in self._streamed}

    def asset_for(self, symbol: str) -> str:
        return symbol.split("/")[0]

    async def fetch_tickers(self) -> dict:
        from latbot.models import Ticker, Timestamps

        ts = Timestamps(recv_ts=self._clock.wall(), recv_mono=self._clock.mono())
        return {a: Ticker(exchange=self.name, symbol=f"{a}/USDT", bid=99.9, ask=100.1, last=100.0, ts=ts, asset=a,
                          quote_volume=1e6) for a in self._assets}


def _stub_poller(n_assets: int, streamed: dict[str, set[str]] | None = None) -> tuple[TickerPoller, _StubState, _StubDetector]:
    names = ["binance", "kucoin", "kraken"]
    settings = Settings(exchanges=[ExchangeConfig(name=n, role=ExchangeRole.SIGNAL) for n in names])
    clock = SimClock()
    assets = [f"A{i}" for i in range(n_assets)]
    adapters = {n: _StubAdapter(n, assets, clock, (streamed or {}).get(n)) for n in names}
    state, det = _StubState(), _StubDetector()
    return TickerPoller(settings, adapters, state, det, pipeline=None, clock=clock), state, det


async def test_detector_runs_once_per_asset_per_round() -> None:
    poller, state, det = _stub_poller(50)
    assert await poller.poll_once() == 150
    assert len(state.tickers) == 150 and all(flag for _, _, flag in state.tickers)
    assert len(det.calls) == 50 and sorted(a for _, a in det.calls) == sorted(f"A{i}" for i in range(50))
    # an asset streamed by every polled exchange is not detected from polls at all; streamed
    # exchanges still feed volume (append_mid=False) and never trigger the detector
    streamed = {"binance": {"A0", "A1"}, "kucoin": {"A0"}, "kraken": {"A0"}}
    poller2, state2, det2 = _stub_poller(3, streamed=streamed)
    await poller2.poll_once()
    assert sorted(a for _, a in det2.calls) == ["A1", "A2"]  # A0 is streamed everywhere: never detected from polls
    assert all(a not in streamed[ex] for ex, a in det2.calls)  # triggered by an exchange that does not stream it
    assert sorted(x for x in state2.tickers if not x[2]) == [("binance", "A0", False), ("binance", "A1", False),
                                                            ("kraken", "A0", False), ("kucoin", "A0", False)]


async def test_poll_once_yields_to_event_loop() -> None:
    poller, _state, _det = _stub_poller(1000)
    ticks = 0
    stop = False

    async def spin() -> None:
        nonlocal ticks
        while not stop:
            ticks += 1
            await asyncio.sleep(0)

    task = asyncio.create_task(spin())
    await asyncio.sleep(0)
    before = ticks
    assert await poller.poll_once() == 3000
    stop = True
    await task
    assert ticks - before > 1  # the round let other tasks run (WS frames, UI, fill polling)


async def test_poll_round_does_not_wait_for_rest_confirmations(tmp_path: Path) -> None:
    """Several lagging assets: the REST book confirmations run in the background
    (bounded per exchange), the ticker round itself returns at once."""
    import time

    env = Env(tmp_path)
    assets = ["BTC", "ETH", "SOL", "XRP"]
    from latbot.exchanges._fill import levels_from_pairs
    from latbot.models import OrderBook, Timestamps

    def books(prices: dict[str, float]) -> None:
        for name, mid in prices.items():
            fake = env.fakes[name]
            for asset in assets:
                sym = fake.symbol_for(asset)
                bids = [(mid * (1 - 0.0001 * k), 50.0) for k in (1, 2, 3)]
                asks = [(mid * (1 + 0.0001 * k), 50.0) for k in (1, 2, 3)]
                fake._books[asset] = OrderBook(exchange=name, symbol=sym, bids=levels_from_pairs(bids, descending=True),
                                               asks=levels_from_pairs(asks, descending=False),
                                               ts=Timestamps(recv_ts=env.clock.wall(), recv_mono=env.clock.mono()), asset=asset)
                fake.volumes[asset] = 5_000_000.0
            fake._mark_msg(None, env.clock.wall(), env.clock.mono())

    kraken = env.fakes["kraken"]
    original = kraken.fetch_order_book
    in_flight = 0
    max_in_flight = 0

    async def slow_fetch(symbol: str, limit: int = 20):
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        try:
            await asyncio.sleep(0.2)
            return await original(symbol, limit)
        finally:
            in_flight -= 1

    kraken.fetch_order_book = slow_fetch  # type: ignore[method-assign]
    books({"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
    await env.poller.poll_once()
    env.clock.advance(0.5)
    books({"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
    await env.poller.poll_once()
    env.clock.advance(1.0)
    books({"binance": 102.0, "kucoin": 102.0, "kraken": 100.0})
    t = time.perf_counter()
    await env.poller.poll_once()
    assert time.perf_counter() - t < 0.15  # not 4 x 0.2 s of sequential REST calls
    assert env.ctx.pipeline.pending_confirmations == 4 and env.ctx.opps.active() == []
    await env.ctx.pipeline.wait_confirmations()
    assert sorted(o.asset for o in env.ctx.opps.active()) == sorted(assets)
    assert 1 <= max_in_flight <= 2  # bounded per exchange
    env.ctx.db.close()
