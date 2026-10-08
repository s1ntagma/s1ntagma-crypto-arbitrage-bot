"""Faster signals: every pool asset on WebSocket (ws_assets 25), new pool assets get streams at runtime."""
from __future__ import annotations

from types import SimpleNamespace

from latbot import app as app_mod
from latbot.config import Settings
from test_ccxt_adapter import Harness, spot_cfg


async def test_add_streams_within_budget_keeps_running_streams():
    h = Harness(spot_cfg(), settings=Settings(ws_assets=2))
    assert await h.adapter.add_streams(["ETH"]) == 0  # not streaming yet: start() owns the first subscription
    await h.adapter.start(["BTC"])
    try:
        assert h.adapter.streamed_assets() == ["BTC"]
        assert await h.adapter.add_streams(["ETH", "SOL", "BTC"]) == 1  # SOL has no allowed market
        assert h.adapter.supported_symbols() == {"BTC/EUR", "ETH/USDC"}
        assert await h.adapter.add_streams(["XRP"]) == 0  # budget (2) used
    finally:
        await h.adapter.stop()


async def test_add_streams_respects_max_ws_subscriptions():
    h = Harness(spot_cfg(max_ws_subscriptions=1), settings=Settings(ws_assets=25))
    await h.adapter.start(["BTC"])
    try:
        assert h.adapter.ws_budget() == 1 and await h.adapter.add_streams(["ETH"]) == 0
    finally:
        await h.adapter.stop()


def test_old_default_ws_assets_is_raised_custom_value_kept():
    assert Settings.from_dict({"config_version": 3, "ws_assets": 10}).ws_assets == 25
    assert Settings.from_dict({"config_version": 3, "ws_assets": 6}).ws_assets == 6
    assert Settings().ws_assets == 25
    s = Settings(ws_assets=500)
    s.validate()
    assert s.ws_assets == 50


async def test_stream_pool_offers_each_new_asset_once():
    calls: list[list[str]] = []

    async def add(assets):
        calls.append(list(assets))
        return len(assets)

    ready = SimpleNamespace(add_streams=add, streamed_assets=lambda: ["BTC"])
    idle = SimpleNamespace(add_streams=add, streamed_assets=lambda: [])  # still starting: retried later
    ctx = SimpleNamespace(adapters={"kraken": ready, "bitvavo": idle, "fake": object()})
    tried: dict[str, set[str]] = {}
    await app_mod._stream_pool(ctx, ["BTC", "ETH", "SOL"], tried)
    await app_mod._stream_pool(ctx, ["BTC", "ETH", "SOL"], tried)
    await app_mod._stream_pool(ctx, ["BTC", "ETH", "SOL", "XRP"], tried)
    assert calls == [["BTC", "ETH", "SOL"], ["XRP"]] and not tried.get("bitvavo")
