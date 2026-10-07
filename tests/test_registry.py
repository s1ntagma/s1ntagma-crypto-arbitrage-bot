"""Tests for exchanges/registry.py (adapter building, watchlist resolution)."""
from __future__ import annotations

import time
from typing import Any

import pytest

from latbot.clock import SimClock
from latbot.config import ExchangeConfig, ExchangeCredentials, Secrets, Settings
from latbot.events import EventBus
from latbot.exchanges.base import ExchangeError
from latbot.exchanges.fake import FakeAdapter
from latbot.exchanges.paper import PaperAdapter
from latbot.exchanges.registry import (
    _market_age_days,
    build_execution_adapters,
    build_market_adapters,
    resolve_universe,
    resolve_watchlist,
)
from latbot.models import ExchangeRole, Mode


def settings(**over: Any) -> Settings:
    base = dict(
        exchanges=[
            ExchangeConfig(name="binance", role=ExchangeRole.SIGNAL, stream="bbo"),
            ExchangeConfig(name="kraken", role=ExchangeRole.EXECUTION),
            ExchangeConfig(name="bybit", role=ExchangeRole.EXECUTION, demo=True),
            ExchangeConfig(name="gate", role=ExchangeRole.EXECUTION, enabled=False),
            ExchangeConfig(name="okx", ccxt_id="myokx", role=ExchangeRole.EXECUTION),
        ],
        quote_assets=["EUR", "USDC", "USD", "USDT"],
    )
    base.update(over)
    return Settings(**base)


class Builder:
    def __init__(self, fail: set[str] | None = None) -> None:
        self.bus = EventBus()
        self.clock = SimClock()
        self.calls: list[tuple[str, ExchangeCredentials | None]] = []
        self.fail = fail or set()

    def __call__(self, cfg: ExchangeConfig, creds: ExchangeCredentials | None) -> FakeAdapter:
        self.calls.append((cfg.name, creds))
        if cfg.name in self.fail:
            raise ExchangeError(f"{cfg.name} unavailable")
        return FakeAdapter(cfg, self.bus, self.clock, symbols={"BTC": "BTC/EUR"})


def creds(name: str) -> ExchangeCredentials:
    return ExchangeCredentials(name=name, api_key="key-" + "x" * 12, api_secret="sec-" + "y" * 12)


def test_build_market_adapters_every_enabled_exchange_without_creds():
    b = Builder()
    s = settings()
    secrets = Secrets(exchanges={"kraken": creds("kraken")})
    market = build_market_adapters(s, secrets, b.bus, b.clock, adapter_factory=b)
    assert set(market) == {"binance", "kraken", "bybit", "okx"}
    assert all(c is None for _, c in b.calls)
    assert market["kraken"].name == "kraken" and market["okx"].cfg.ccxt_name == "myokx"


def test_build_market_adapters_skips_failures():
    b = Builder(fail={"bybit"})
    market = build_market_adapters(settings(), Secrets(), b.bus, b.clock, adapter_factory=b)
    assert set(market) == {"binance", "kraken", "okx"}


def test_execution_paper_wraps_market_adapters_and_no_live_without_creds():
    b = Builder()
    s = settings()
    market = build_market_adapters(s, Secrets(), b.bus, b.clock, adapter_factory=b)
    before = len(b.calls)
    ex = build_execution_adapters(s, Secrets(), b.bus, b.clock, market, db=None, adapter_factory=b)
    assert set(ex) == {Mode.PAPER, Mode.LIVE}
    assert set(ex[Mode.PAPER]) == {"kraken", "bybit", "okx"}  # execution + enabled only
    for name, adapter in ex[Mode.PAPER].items():
        assert isinstance(adapter, PaperAdapter) and adapter.inner is market[name] and adapter.name == name
    assert ex[Mode.LIVE] == {}
    assert len(b.calls) == before  # no real adapters built for paper


async def test_paper_adapters_credit_every_quote_asset():
    b = Builder()
    s = settings(paper_balance_quote=777.0)
    market = build_market_adapters(s, Secrets(), b.bus, b.clock, adapter_factory=b)
    ex = build_execution_adapters(s, Secrets(), b.bus, b.clock, market, db=None, adapter_factory=b)
    bal = await ex[Mode.PAPER]["kraken"].fetch_balances()
    assert {a for a in bal} >= {"EUR", "USDC", "USD", "USDT"} and bal["EUR"].free == 777.0


def test_execution_live_only_with_creds_and_not_for_demo():
    b = Builder()
    s = settings()
    secrets = Secrets(exchanges={"okx": creds("okx"), "bybit": creds("bybit"), "binance": creds("binance")})
    market = build_market_adapters(s, secrets, b.bus, b.clock, adapter_factory=b)
    ex = build_execution_adapters(s, secrets, b.bus, b.clock, market, db=None, adapter_factory=b)
    assert set(ex[Mode.LIVE]) == {"okx"}  # kraken: no creds; bybit: demo-configured; binance: signal only
    live_calls = [(n, c) for n, c in b.calls if c is not None]
    assert live_calls == [("okx", secrets.creds("okx"))]
    assert isinstance(ex[Mode.PAPER]["bybit"], PaperAdapter)  # internal paper engine by default
    assert isinstance(ex[Mode.LIVE]["okx"], FakeAdapter) and ex[Mode.LIVE]["okx"] is not market["okx"]


def test_execution_live_factory_failure_is_skipped():
    b = Builder(fail={"okx"})
    s = settings()
    secrets = Secrets(exchanges={"okx": creds("okx"), "kraken": creds("kraken")})
    market = {"kraken": FakeAdapter(s.exchange("kraken"), b.bus, b.clock), "okx": FakeAdapter(s.exchange("okx"), b.bus, b.clock)}
    ex = build_execution_adapters(s, secrets, b.bus, b.clock, market, db=None, adapter_factory=b)
    assert set(ex[Mode.LIVE]) == {"kraken"} and set(ex[Mode.PAPER]) == {"kraken", "okx"}


def test_bybit_demo_paper_engine_uses_real_adapter():
    b = Builder()
    s = settings(paper_engine="bybit_demo")
    secrets = Secrets(exchanges={"bybit": creds("bybit")})
    market = build_market_adapters(s, secrets, b.bus, b.clock, adapter_factory=b)
    ex = build_execution_adapters(s, secrets, b.bus, b.clock, market, db=None, adapter_factory=b)
    paper_bybit = ex[Mode.PAPER]["bybit"]
    assert isinstance(paper_bybit, FakeAdapter) and not isinstance(paper_bybit, PaperAdapter)
    assert paper_bybit is not market["bybit"]
    assert ("bybit", secrets.creds("bybit")) in b.calls
    assert "bybit" not in ex[Mode.LIVE]
    assert isinstance(ex[Mode.PAPER]["kraken"], PaperAdapter)
    # without demo creds -> internal paper simulator
    ex2 = build_execution_adapters(s, Secrets(), b.bus, b.clock, market, db=None, adapter_factory=b)
    assert isinstance(ex2[Mode.PAPER]["bybit"], PaperAdapter)
    # factory failure -> fallback to the internal simulator
    b3 = Builder(fail={"bybit"})
    ex3 = build_execution_adapters(s, secrets, b3.bus, b3.clock, market, db=None, adapter_factory=b3)
    assert isinstance(ex3[Mode.PAPER]["bybit"], PaperAdapter)


def test_execution_without_market_adapter_skips_paper():
    b = Builder()
    s = settings()
    ex = build_execution_adapters(s, Secrets(), b.bus, b.clock, {}, db=None, adapter_factory=b)
    assert ex[Mode.PAPER] == {} and ex[Mode.LIVE] == {}


class KV:
    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    def kv_get(self, key, default=None):
        return self.data.get(key, default)

    def kv_set(self, key, value):
        self.data[key] = value


def test_paper_adapter_receives_db():
    b = Builder()
    s = settings()
    db = KV()
    market = build_market_adapters(s, Secrets(), b.bus, b.clock, adapter_factory=b)
    ex = build_execution_adapters(s, Secrets(), b.bus, b.clock, market, db=db, adapter_factory=b)
    assert ex[Mode.PAPER]["kraken"].db is db


# --------------------------------------------------------------------------- #
# watchlist
# --------------------------------------------------------------------------- #
async def test_resolve_watchlist_static():
    s = settings(watchlist=["ETH", "BTC"], watchlist_auto=False)
    out = await resolve_watchlist(s, {})
    assert out == ["ETH", "BTC"] and out is not s.watchlist


def kraken_with_volumes(volumes: dict[str, float], markets: dict[str, dict] | None = None) -> FakeAdapter:
    b = Builder()
    fake = FakeAdapter(ExchangeConfig(name="kraken"), b.bus, b.clock)
    fake.volumes.update(volumes)
    fake.markets.update(markets or {})
    return fake


async def test_resolve_watchlist_auto_orders_by_volume_btc_first_and_limits():
    now_ms = time.time() * 1000
    fake = kraken_with_volumes(
        {"ETH": 500.0, "BTC": 300.0, "SOL": 800.0, "NEW": 900.0, "OLD": 50.0, "XRP": 10.0},
        {
            "NEW": {"info": {"launchTime": str(int(now_ms - 100 * 86400 * 1000))}},
            "OLD": {"info": {"launchTime": str(int(now_ms - 800 * 86400 * 1000))}},
            "XRP": {"info": {}},
        },
    )
    s = settings(watchlist_auto=True, watchlist_exchange="kraken", watchlist_size=3, watchlist=["DOGE"])
    out = await resolve_watchlist(s, {"kraken": fake})
    assert out == ["BTC", "SOL", "ETH"]
    s.watchlist_size = 10
    assert await resolve_watchlist(s, {"kraken": fake}) == ["BTC", "SOL", "ETH", "OLD", "XRP"]


async def test_resolve_watchlist_auto_fallbacks():
    s = settings(watchlist_auto=True, watchlist_exchange="kraken", watchlist=["DOGE", "ADA"])
    assert await resolve_watchlist(s, {}) == ["DOGE", "ADA"]  # exchange missing
    failing = kraken_with_volumes({"ETH": 1.0})
    failing.fail_fetch_tickers = True
    assert await resolve_watchlist(s, {"kraken": failing}) == ["DOGE", "ADA"]  # tickers failed
    empty = kraken_with_volumes({})
    assert await resolve_watchlist(s, {"kraken": empty}) == ["DOGE", "ADA"]  # nothing listed


async def test_resolve_watchlist_btc_inserted_when_market_exists():
    fake = kraken_with_volumes({"ETH": 5.0, "SOL": 7.0})
    s = settings(watchlist_auto=True, watchlist_exchange="kraken")
    assert await resolve_watchlist(s, {"kraken": fake}) == ["BTC", "SOL", "ETH"]  # FakeAdapter lists every asset


async def test_resolve_watchlist_accepts_raw_ccxt_tickers_and_missing_markets():
    class Raw:
        name = "kraken"

        async def fetch_tickers(self):
            return {
                "ETH/USD": {"symbol": "ETH/USD", "quoteVolume": 2.0},
                "BTC/USD": {"symbol": "BTC/USD", "quoteVolume": 1.0},
                "SOL/USD": {"symbol": "SOL/USD", "quoteVolume": "oops"},
                "ADA/USD": {"symbol": "ADA/USD", "quoteVolume": 3.0},
            }

        def symbol_for(self, asset):
            return None if asset == "ADA" else f"{asset}/USD"

    s = settings(watchlist_auto=True, watchlist_exchange="kraken")
    assert await resolve_watchlist(s, {"kraken": Raw()}) == ["BTC", "ETH", "SOL"]


def test_market_age_days():
    now = 1_700_000_000.0
    assert _market_age_days(None, now) is None
    assert _market_age_days({"info": {}}, now) is None
    assert _market_age_days({"info": {"launchTime": "abc"}}, now) is None
    assert _market_age_days({"info": {"launchTime": (now - 86400 * 10) * 1000}}, now) == pytest.approx(10)
    assert _market_age_days({"info": {"launchTime": now - 86400 * 3}}, now) == pytest.approx(3)


async def test_resolve_universe_ws_and_poll():
    b = Builder()
    s = settings(watchlist=["BTC", "ETH"], ws_assets=1, poll_assets_max=2)

    def fake(name: str, volumes: dict[str, float]) -> FakeAdapter:
        a = FakeAdapter(s.exchange(name), b.bus, b.clock)
        a.volumes.update(volumes)
        return a

    adapters = {
        "binance": fake("binance", {"BTC": 10, "ETH": 9, "SOL": 8, "DOGE": 7, "ADA": 6, "ONLY": 5}),
        "kraken": fake("kraken", {"BTC": 1, "ETH": 1, "SOL": 1, "DOGE": 1}),
        "okx": fake("okx", {"ADA": 100, "SOL": 1}),
    }
    adapters["okx"].fail_fetch_tickers = False
    ws, poll = await resolve_universe(s, adapters)
    assert ws == ["BTC"]
    # ETH is excluded only because it is... not: ws_assets=1 keeps ETH out of ws, so it is polled
    assert poll == ["ADA", "ETH"]  # ADA total 106, ETH 10, SOL 10 (tie broken by insertion), DOGE 8; capped at 2
    s.poll_assets_max = 10
    ws, poll = await resolve_universe(s, adapters)
    assert "ONLY" not in poll and set(poll) == {"ADA", "ETH", "SOL", "DOGE"}
    adapters["kraken"].fail_fetch_tickers = True
    ws, poll = await resolve_universe(s, adapters)
    assert set(poll) == {"ADA", "SOL"}  # only okx remains as execution venue
