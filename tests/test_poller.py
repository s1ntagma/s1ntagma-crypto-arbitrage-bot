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
    signals = [p for t, p in env.topics if t == events.SIGNAL]
    opps = env.ctx.opps.active()
    assert len(opps) == 1, [p for t, p in env.topics]
    opp = opps[0]
    assert opp.signal.source == "poll" and opp.target_exchange == "kraken" and opp.signal.movers_count == 2
    assert opp.signal.mover_volumes_quote  # the gate saw real volumes
    assert opp.econ.book_age_ms <= 1.0  # economics on the freshly fetched REST book
    assert env.ctx.state.book("kraken", "BTC") is not None
    assert opp.state in (OppState.VALID, OppState.NOTIFIED) and opp.bot.light is not Light.RED
    assert env.poller.evaluated >= 1 and env.poller.signals_seen >= 1
    assert not signals  # poll signals are not published on the SIGNAL topic (only evaluated)
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
    assert sorted(calls) == [("binance", "poll"), ("kucoin", "poll")]
    assert env.ctx.state.volume24h("kraken", "BTC") == pytest.approx(5_000_000.0)  # volume still recorded
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
