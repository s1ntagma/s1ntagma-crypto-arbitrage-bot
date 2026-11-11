"""Autopilot (owner: "чтобы он всё делал сам"): AUTO without a click, through the same gates as a
click on AUTO - proven pair, revalidation on the latest book, risk limits, READY for LIVE."""
from __future__ import annotations

import asyncio

import test_execution as tx
from latbot import events
from latbot.execution.autopilot import Autopilot
from latbot.models import Light, Mode, OppState


def make(**over):
    env = tx.make_env(tx.make_settings(autopilot=True, **over))
    pilot = Autopilot(env.settings, env.bus, env.engine, env.risk)
    return env, pilot


async def settle(env) -> None:
    for _ in range(50):
        await asyncio.sleep(0)
    for task in list(env.engine._tasks.values()):
        await task


async def test_off_by_default():
    env = tx.make_env()
    pilot = Autopilot(env.settings, env.bus, env.engine, env.risk)
    opp = env.opps.add(tx.make_opp(env.clock))
    opp.proven = {"enforced": True, "proven": True}  # a proven pair
    assert await pilot.consider(opp) == "autopilot off"
    await settle(env)
    assert env.engine.trades() == [] and env.adapters["testex"].placed() == []


async def test_valid_proven_opportunity_is_traded_once():
    env, pilot = make()
    opp = env.opps.add(tx.make_opp(env.clock))
    opp.proven = {"enforced": True, "proven": True}  # a proven pair
    assert await pilot.consider(opp) is None
    await settle(env)
    assert len(env.engine.trades()) == 1 and env.adapters["testex"].placed()
    assert await pilot.consider(opp) == "already tried"


async def test_unproven_pair_is_not_traded():
    env, pilot = make()
    opp = env.opps.add(tx.make_opp(env.clock))
    opp.proven = {"enforced": True, "proven": False, "text": "TRAINING 3/30"}
    assert await pilot.consider(opp) == "pair not proven"
    await settle(env)
    assert env.engine.trades() == []


async def test_low_net_waits_and_is_retried_when_it_rises():
    env, pilot = make(min_net_pct_auto=1.0)
    opp = env.opps.add(tx.make_opp(env.clock, net=0.6))
    opp.proven = {"enforced": True, "proven": True}  # a proven pair
    assert (await pilot.consider(opp)).startswith("net")
    opp.econ = tx.make_econ(4.8, 1, 100.0)
    assert await pilot.consider(opp) is None
    await settle(env)
    assert len(env.engine.trades()) == 1


async def test_red_bot_light_and_emergency_block():
    env, pilot = make()
    opp = env.opps.add(tx.make_opp(env.clock, bot=Light.RED))
    opp.proven = {"enforced": True, "proven": True}  # a proven pair
    assert await pilot.consider(opp) == "bot light red"
    await env.risk.set_emergency(True, "test")
    opp2 = env.opps.add(tx.make_opp(env.clock))
    opp2.proven = {"enforced": True, "proven": True}  # a proven pair
    assert await pilot.consider(opp2) == "emergency stop"
    await settle(env)
    assert env.engine.trades() == []


async def test_valid_state_event_starts_the_trade():
    env, pilot = make()
    opp = env.opps.add(tx.make_opp(env.clock))
    opp.proven = {"enforced": True, "proven": True}  # a proven pair
    await env.bus.publish(events.OPP_STATE, {"opportunity": opp, "from": "VALIDATING", "to": OppState.VALID.value, "reason": ""})
    await settle(env)
    assert len(env.engine.trades()) == 1


async def test_live_waits_for_ready():
    env = tx.make_env(tx.make_settings(autopilot=True, paper_mode=False, live_mode=True), mode=Mode.LIVE)
    env.engine.readiness = lambda: {"ready": False, "checks": [{"label": "at least one proven pair", "ok": False}]}
    pilot = Autopilot(env.settings, env.bus, env.engine, env.risk)
    opp = env.opps.add(tx.make_opp(env.clock))
    opp.proven = {"enforced": True, "proven": True}  # a proven pair
    opp.mode = Mode.LIVE
    why = await pilot.consider(opp)
    await settle(env)
    assert why.startswith("not ready for LIVE") and "proven pair" in why
    assert env.adapters["testex"].placed() == [] and env.engine.trades() == []  # no cancelled row per signal
    env.engine.readiness = lambda: {"ready": True, "checks": []}
    env.clock.advance(31.0)  # the cached answer expires
    assert await pilot.consider(opp) is None
    await settle(env)
    assert env.adapters["testex"].placed()
