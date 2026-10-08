"""Phase 3 gate: assets outside the narrow pool are ingested (prices, volumes) but never detected."""
from __future__ import annotations

from latbot.clock import SimClock
from latbot.config import ExchangeConfig, Settings
from latbot.events import EventBus
from latbot.exchanges.fake import FakeAdapter
from latbot.exchanges.registry import listing_map
from latbot.market.state import MarketState
from latbot.models import BookLevel, ExchangeRole, OrderBook, Timestamps
from tests.test_pipeline import Env, detect


class StubPool:
    def __init__(self, assets: set[str]) -> None:
        self.assets = set(assets)

    def contains(self, asset: str) -> bool:
        return asset in self.assets


async def test_pool_asset_is_detected():
    env = Env()
    env.pipeline.pool = StubPool({"BTC"})
    opp = await detect(env)
    assert opp.asset == "BTC"


async def test_asset_outside_the_pool_gives_no_signal_but_prices_are_kept():
    env = Env()
    env.pipeline.pool = StubPool({"ETH"})
    await env.warm_up()
    await env.push("binance", 102.0)
    out = await env.push("kucoin", 102.0)
    assert out == []
    assert env.opps.active() == []
    assert env.state.mid_series("binance", "BTC")  # still ingested (bars / reactions / chart need prices)
    assert "BTC" in env.pipeline.pool_skipped


async def test_no_pool_means_every_asset_is_scanned():
    env = Env()
    assert env.pipeline.scanned("ANY") is True
    env.pipeline.pool = StubPool(set())
    assert env.pipeline.scanned("ANY") is False


def _book(ex: str, mid: float, clock: SimClock, asset: str = "BTC") -> OrderBook:
    ts = Timestamps(recv_ts=clock.wall(), recv_mono=clock.mono())
    return OrderBook(exchange=ex, symbol=f"{asset}/USDT", bids=[BookLevel(mid * 0.999, 1.0)], asks=[BookLevel(mid * 1.001, 1.0)], ts=ts, asset=asset)


def test_median_mid_uses_fresh_streams_only():
    s = Settings(price_sample_min_interval_ms=0.0)
    clock = SimClock()
    st = MarketState(s, clock)
    st.on_book(_book("binance", 100.0, clock))
    st.on_book(_book("kucoin", 102.0, clock))
    clock.advance(10.0)
    st.on_book(_book("bitget", 101.0, clock))
    st.on_book(_book("htx", 103.0, clock))
    st.on_book(_book("okx", 104.0, clock))
    now = clock.mono()
    assert st.median_mid("BTC", now, max_age_ms=1500.0) == 103.0  # bitget, htx, okx: the 10 s old ones are out
    assert st.median_mid("BTC", now, max_age_ms=1500.0, exchanges=["bitget", "htx"]) == 102.0
    assert st.median_mid("BTC", now, max_age_ms=60_000.0, exchanges=["binance", "kucoin"]) == 101.0
    assert st.median_mid("ETH", now, max_age_ms=1500.0) is None
    assert st.median_mid("BTC", now, max_age_ms=1500.0, exchanges=["binance"]) is None


async def test_listing_map_asset_to_exchange_volumes():
    bus, clock = EventBus(), SimClock()
    s = Settings(exchanges=[
        ExchangeConfig(name="binance", role=ExchangeRole.SIGNAL, stream="bbo"),
        ExchangeConfig(name="kraken", role=ExchangeRole.EXECUTION),
        ExchangeConfig(name="gate", role=ExchangeRole.EXECUTION, enabled=False),
    ])
    adapters = {}
    for name, vols in (("binance", {"BTC": 10.0, "ETH": 5.0}), ("kraken", {"BTC": 2.0, "SOL": 1.0}), ("gate", {"BTC": 9.0})):
        a = FakeAdapter(s.exchange(name), bus, clock)
        a.volumes.update(vols)
        adapters[name] = a
    m = await listing_map(s, adapters)
    assert m["BTC"] == {"binance": 10.0, "kraken": 2.0}  # gate is disabled
    assert m["ETH"] == {"binance": 5.0} and m["SOL"] == {"kraken": 1.0}
    adapters["kraken"].fail_fetch_tickers = True
    m = await listing_map(s, adapters)
    assert "SOL" not in m and m["BTC"] == {"binance": 10.0}
