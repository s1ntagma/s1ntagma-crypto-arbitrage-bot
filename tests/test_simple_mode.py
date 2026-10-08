"""Simple mode (owner): the coin moved on its most liquid venue, the target has not moved yet ->
buy there, sell when it catches up or after 15 s.  No advisor gates; hard rules unchanged."""
from __future__ import annotations

import json

import pytest

import test_execution as tx
import test_telegram as tt
from latbot.config import CONFIG_VERSION, Settings, load_settings
from latbot.engine.shadow import ShadowTrader
from test_mover import Harness, make_settings as mover_settings
from test_pipeline import Env, make_settings as pipe_settings

FAILED = {"score": 20, "grade": "D", "passed": False, "vetoes": ["Макро-события: CPI через 90 с"], "summary": "вето"}


def simple_harness() -> Harness:
    h = Harness(mover_settings(simple_mode=True))
    h.state.on_ticker(h.ticker("a", 100.0, 5e8))  # a = the most liquid venue (leader)
    h.state.on_ticker(h.ticker("b", 100.0, 1e8))
    return h


# ---- leader --------------------------------------------------------------------------------------
def test_leader_alone_gives_the_signal():
    h = simple_harness()
    h.warm_up()
    h.clock.advance(1.0)
    signals = h.push("a", 101.0)  # only the leader moved: enough in simple mode
    assert len(signals) == 1 and signals[0].first_mover == "a" and signals[0].target == "c"
    assert signals[0].movers_count == 1


def test_a_move_on_a_smaller_venue_is_not_a_signal():
    h = simple_harness()
    h.warm_up()
    h.clock.advance(1.0)
    assert h.push("b", 101.0) == []  # b is not where the coin trades most
    assert h.det.diag.snapshot()["funnel"]["rejected"].get("leader_not_moved", 0) >= 1


def test_unknown_volumes_mean_no_leader_no_signal():
    h = Harness(mover_settings(simple_mode=True))  # no 24h volumes yet
    h.warm_up()
    h.clock.advance(1.0)
    assert h.push("a", 101.0) == [] and h.push("b", 101.0) == []
    assert h.det.diag.snapshot()["funnel"]["rejected"].get("leader_unknown", 0) >= 1


def test_full_mode_keeps_two_movers():
    h = Harness(mover_settings(simple_mode=False))
    h.state.on_ticker(h.ticker("a", 100.0, 5e8))
    h.warm_up()
    h.clock.advance(1.0)
    assert h.push("a", 101.0) == []


# ---- pipeline: fresh entry, 15 s hold, no advisor gates --------------------------------------------
def simple_env(**over) -> Env:
    env = Env(pipe_settings(simple_mode=True, **over))
    for ex, vol in (("binance", 5e8), ("kucoin", 1e8)):
        env.state.on_ticker(tt_ticker(env, ex, vol))
    return env


def tt_ticker(env: Env, ex: str, vol: float):
    from latbot.models import Ticker
    return Ticker(exchange=ex, symbol="BTC/USDT", bid=99.99, ask=100.01, last=100.0, ts=env.ts(), asset="BTC",
                  quote_volume=vol)


async def test_leader_move_opens_the_opportunity_with_a_15s_window():
    env = simple_env()
    await env.warm_up()
    opps = await env.push("binance", 102.0)
    assert len(opps) == 1 and opps[0].target_exchange == "kraken"
    assert opps[0].estimated_duration_s <= 15.0


async def test_no_new_entry_after_the_entry_deadline():
    env = simple_env(simple_entry_deadline_s=3.0, ride_enabled=False)
    await env.warm_up()
    env.adapters["kraken"].health.ws_connected = True
    await env.push("binance", 102.0)
    first = env.opps.active()
    assert len(first) == 1
    await env.opps.expire(first[0].id, "test: closed")
    env.clock.advance(3.5)  # the leader moved 3.5 s ago: the laggard either followed or it is too late
    await env.push("binance", 102.0)
    assert await env.push("kraken", 100.0) == []
    assert env.pipeline.diag.snapshot()["funnel"]["rejected"].get("late_entry", 0) >= 1


async def test_hold_limit_closes_after_15s_and_shadow_sells_at_market():
    env = simple_env(ride_enabled=False)
    sh = ShadowTrader(env.settings, env.bus, env.clock)
    await env.warm_up()
    opp = (await env.push("binance", 102.0))[0]
    for _ in range(16):  # books stay fresh, the target never follows
        env.clock.advance(1.0)
        await env.push_all({"binance": 102.0, "kucoin": 102.0, "kraken": 100.0})
        await env.pipeline.sweep_once()
    assert env.opps.get(opp.id).state.value in ("EXPIRED", "CLOSED", "CANCELLED")
    row = sh.trades()[0]
    assert row["open"] is False and row["barrier"] == "time" and "hold limit" in row["exit_reason"]


async def test_quality_is_info_only_in_simple_mode():
    env = simple_env(min_quality_score=99)
    await env.warm_up()
    opp = (await env.push("binance", 102.0))[0]
    assert opp.quality and opp.quality["enforced"] is False  # computed, shown, not deciding


def test_telegram_and_auto_ignore_quality_when_not_enforced():
    env = tt.Env()
    assert env.notifier.should_alert(tt.make_opp(env.clock, quality=dict(FAILED, enforced=False)))
    assert not env.notifier.should_alert(tt.make_opp(env.clock, quality=dict(FAILED)))


async def test_auto_revalidation_ignores_quality_when_not_enforced():
    async def set_quality(opp):
        opp.quality = dict(FAILED, enforced=False)

    env = tx.make_env(pipeline=tx.StubPipeline(on_refresh=set_quality))
    opp = env.opps.add(tx.make_opp(env.clock))
    ok, reason, _ = await env.engine.revalidate(opp)
    assert ok or "quality" not in reason.lower()


def test_effective_hold_and_settings():
    s = Settings(simple_mode=True, ride_enabled=False, max_hold_s=60.0, sell_timeout_s=20.0)
    assert s.effective_max_hold_s() == 15.0 and s.effective_sell_timeout_s() == 15.0
    full = Settings(simple_mode=False, max_hold_s=60.0, sell_timeout_s=20.0)
    assert full.effective_max_hold_s() == 60.0 and full.effective_sell_timeout_s() == 20.0
    ride = Settings(simple_mode=True, ride_max_hold_s=300.0)
    assert ride.ride_active() and ride.effective_max_hold_s() == 300.0


def test_owner_configs_switch_to_simple_mode_once(tmp_path):
    p = tmp_path / "config.json"
    assert load_settings(p).simple_mode is True  # fresh install
    p.write_text(json.dumps({"config_version": 4}), encoding="utf-8")
    assert load_settings(p).simple_mode is True  # upgrade
    p.write_text(json.dumps({"config_version": CONFIG_VERSION, "simple_mode": False}), encoding="utf-8")
    assert load_settings(p).simple_mode is False  # the owner's later choice stays


@pytest.mark.parametrize("value", [0.0, -1.0])
def test_limits_are_sane(value):
    s = Settings(simple_mode=True, simple_entry_deadline_s=value, simple_max_hold_s=value)
    s.validate()
    assert s.simple_entry_deadline_s > 0 and s.simple_max_hold_s >= 1.0
