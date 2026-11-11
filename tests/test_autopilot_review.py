"""Review fixes for the autopilot / READY gate / exchange budgets (each test = one verified finding)."""
from __future__ import annotations

import asyncio

import pytest

import test_execution as tx
from latbot.config import Settings
from latbot.execution.autopilot import Autopilot
from latbot.models import Mode, OppState
from test_risk import make_risk, make_settings as risk_settings

PROVEN = {"enforced": True, "proven": True}


def make(**over):
    env = tx.make_env(tx.make_settings(autopilot=True, **over))
    return env, Autopilot(env.settings, env.bus, env.engine, env.risk)


def proven_opp(env, **kw):
    opp = env.opps.add(tx.make_opp(env.clock, **kw))
    opp.proven = dict(PROVEN)
    return opp


async def settle(env) -> None:
    for _ in range(50):
        await asyncio.sleep(0)
    for task in list(env.engine._tasks.values()):
        await task


# ---- 1. the owner's MANUAL / AUTO choice is never overridden -----------------------------------------
async def test_switch_to_manual_does_not_start_a_hidden_auto_trade():
    env, pilot = make(min_net_pct_auto=5.0)  # 4.8 % net: the autopilot skips it at first
    opp = proven_opp(env)
    await env.engine.select_auto(opp.id)  # owner opens the AUTO confirmation
    opp.econ = tx.make_econ(6.0, 1, 100.0)  # the net rises above the autopilot threshold
    await env.engine.select_manual(opp.id)  # owner takes it by hand: AUTO_SELECTED -> VALID -> MANUAL_SELECTED
    await settle(env)
    assert opp.state is OppState.MANUAL_SELECTED
    assert env.engine.trades() == [] and env.adapters["testex"].placed() == []
    assert await pilot.consider(opp) == "the owner chose this opportunity"


async def test_backing_out_of_auto_is_respected():
    env, pilot = make(min_net_pct_auto=5.0)
    opp = proven_opp(env)
    await env.engine.select_auto(opp.id)
    opp.econ = tx.make_econ(6.0, 1, 100.0)
    await env.engine.deselect_auto(opp.id)  # "user backed out": VALID again, stays on the board
    await settle(env)
    assert opp.state is OppState.VALID and env.engine.trades() == []


async def test_confirm_does_not_replace_a_running_task():
    env = tx.make_env(tx.make_settings(), exit_mode="never")
    opp = proven_opp(env)
    first = await env.engine.confirm_auto(opp.id, wait=False)
    assert first["ok"]
    task = env.engine._tasks[opp.id]
    second = await env.engine.confirm_auto(opp.id, wait=False)
    assert not second["ok"] and env.engine._tasks.get(opp.id) is task
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


# ---- 2. limits that already block: no attempt, no cancelled row ---------------------------------------
async def test_risk_limit_blocks_before_any_attempt():
    env, pilot = make(max_simultaneous_trades=1)
    env.risk.register_open("t-open", "testex", "ETH", 50.0, "paper")
    opp = proven_opp(env)
    why = await pilot.consider(opp)
    assert why.startswith("risk:") and "simultaneous" in why
    await settle(env)
    assert env.engine.trades() == [] and opp.state is OppState.VALID
    env.risk.register_close("t-open", 0.0)
    assert await pilot.consider(opp) is None  # not marked as tried: retried once the limit frees up


# ---- 3. one position per pair -------------------------------------------------------------------------
async def test_no_second_entry_while_the_pair_has_a_position():
    env, pilot = make()
    env.adapters["testex"].exit_mode = "never"
    first = proven_opp(env)
    assert await pilot.consider(first) is None
    for _ in range(200):  # the first trade holds its position (exit never fills)
        await asyncio.sleep(0)
        if env.engine.trades() and env.engine.trades()[0].state is OppState.FILLED:
            break
    second = proven_opp(env)
    why = await pilot.consider(second)
    assert why is not None and "BTC/USDT@testex" in why
    res = await env.engine.select_auto(second.id)  # a click is refused too
    assert not res["ok"] and "BTC/USDT@testex" in res["reason"]
    for t in list(env.engine._tasks.values()):
        t.cancel()
    await asyncio.gather(*env.engine._tasks.values(), return_exceptions=True)


# ---- 4. proven pairs only, whatever the mode -----------------------------------------------------------
@pytest.mark.parametrize("proven", [{}, {"enforced": False, "proven": False, "text": "TRAINING 3/30"}])
async def test_autopilot_needs_a_proven_pair_even_outside_simple_mode(proven):
    env, pilot = make(simple_mode=False)
    opp = env.opps.add(tx.make_opp(env.clock))
    opp.proven = proven
    assert await pilot.consider(opp) == "pair not proven"


async def test_live_auto_needs_a_proven_pair_even_outside_simple_mode():
    env = tx.make_env(tx.make_settings(simple_mode=False, paper_mode=False, live_mode=True), mode=Mode.LIVE)
    env.engine.readiness = lambda: {"ready": True, "checks": []}
    opp = env.opps.add(tx.make_opp(env.clock))
    opp.proven = {"enforced": False, "proven": False, "text": "TRAINING 3/30"}
    ok, reason, _ = await env.engine.revalidate(opp)
    assert not ok and reason.startswith("Pair not proven")


# ---- 5. READY cache never reuses a "ready" answer ------------------------------------------------------
async def test_ready_answer_is_not_cached():
    env = tx.make_env(tx.make_settings(paper_mode=False, live_mode=True), mode=Mode.LIVE)
    answer = {"ready": True, "checks": []}
    env.engine.readiness = lambda: answer
    assert env.engine.not_ready(max_age_s=30) is None
    answer = {"ready": False, "checks": [{"label": "at least 200 virtual trades in this period", "ok": False}]}
    env.clock.advance(5.0)
    assert "200 virtual trades" in (env.engine.not_ready(max_age_s=30) or "")


# ---- 6. budgets: names as the UI shows them, 0 = nothing on that venue, typos are errors ----------------
def test_budget_names_resolve_to_the_configured_venues():
    s = Settings()
    s.update({"exchange_budget_quote": "Crypto.com=50, OKX EEA=60, Revolut X=70, Gate EU=80"})
    assert s.exchange_budget_quote == {"cryptocom": 50.0, "okx": 60.0, "revolutx": 70.0, "gate": 80.0}
    s.update({"exchange_budget_quote": "myokx=90"})
    assert s.exchange_budget_quote == {"okx": 90.0}


@pytest.mark.parametrize("text", ["foo=50", "kraken=200; okx=500", "kraken=200 EUR", "kraken", "kraken=-5", "kraken=nan"])
def test_unreadable_budget_text_is_an_error(text):
    s = Settings()
    with pytest.raises(ValueError):
        s.update({"exchange_budget_quote": text})


def test_zero_budget_blocks_the_venue():
    risk = make_risk(risk_settings(exchange_budget_quote={"kraken": 0.0}))
    ok, why, _ = risk.check("kraken", "BTC", 100.0, "paper")
    assert not ok and "budget on kraken" in why


def test_saved_budgets_with_unknown_names_are_dropped_on_load():
    s = Settings.from_dict({"exchange_budget_quote": {"Kraken": "200", "nowhere": 5, "okx": -1}})
    assert s.exchange_budget_quote == {"kraken": 200.0}


# ---- 7. booleans from a hand-edited config.json ---------------------------------------------------------
def test_string_booleans_are_coerced():
    s = Settings.from_dict({"autopilot": "false", "live_requires_ready": "0"})
    assert s.autopilot is False and s.live_requires_ready is False
    s = Settings.from_dict({"autopilot": "true"})
    assert s.autopilot is True
    s.update({"autopilot": False})
    assert s.autopilot is False
