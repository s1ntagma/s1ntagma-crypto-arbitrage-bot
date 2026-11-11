"""AUTO readiness (owner: "потом просто дать ключи, и он всё делает сам"):
LIVE AUTO only when the measured record says READY, and a spend budget per exchange."""
from __future__ import annotations

import test_execution as tx
from latbot.config import Settings
from latbot.models import Mode
from test_risk import make_risk, make_settings as risk_settings

READY = {"ready": True, "checks": [{"key": "trades", "label": "at least 200 virtual trades in this period", "ok": True}]}
NOT_READY = {"ready": False, "checks": [
    {"key": "trades", "label": "at least 200 virtual trades in this period", "ok": False},
    {"key": "proven", "label": "at least one proven pair", "ok": True},
]}


# ---- budget per exchange ------------------------------------------------------------------------
def test_exchange_budget_caps_the_trade():
    risk = make_risk(risk_settings(exchange_budget_quote={"kraken": 60.0}))
    ok, why, allowed = risk.check("kraken", "BTC", 100.0, "paper")
    assert ok and allowed == 60.0 and "budget on kraken" in why
    ok, _why, allowed = risk.check("okx", "BTC", 100.0, "paper")  # no budget set: the global limits only
    assert ok and allowed == 100.0


def test_exchange_budget_counts_open_positions():
    risk = make_risk(risk_settings(exchange_budget_quote={"kraken": 60.0}))
    risk.register_open("t1", "kraken", "ETH", 55.0, "paper")
    ok, why, _allowed = risk.check("kraken", "BTC", 100.0, "paper")
    assert not ok and "budget on kraken" in why  # 5 left < min trade 10


def test_budget_setting_parses_text_from_the_ui():
    s = Settings()
    s.update({"exchange_budget_quote": "Kraken = 200, okx=500.5, gate=0"})
    assert s.exchange_budget_quote == {"kraken": 200.0, "okx": 500.5, "gate": 0.0}
    s.update({"exchange_budget_quote": ""})
    assert s.exchange_budget_quote == {}


# ---- LIVE needs READY ---------------------------------------------------------------------------
def live_env(**over):
    env = tx.make_env(tx.make_settings(paper_mode=False, live_mode=True, **over), mode=Mode.LIVE)
    return env


async def test_live_auto_refused_until_ready():
    env = live_env()
    env.engine.readiness = lambda: NOT_READY
    opp = env.opps.add(tx.make_opp(env.clock))
    opp.proven = {"enforced": True, "proven": True}  # a proven pair
    ok, reason, _ = await env.engine.revalidate(opp)
    assert not ok and reason.startswith("Not ready for LIVE") and "200 virtual trades" in reason
    assert env.adapters["testex"].placed() == []


async def test_live_auto_without_a_record_is_refused():
    env = live_env()  # no readiness source wired: no measured record -> no real-money trade
    opp = env.opps.add(tx.make_opp(env.clock))
    opp.proven = {"enforced": True, "proven": True}  # a proven pair
    ok, reason, _ = await env.engine.revalidate(opp)
    assert not ok and reason.startswith("Not ready for LIVE")


async def test_live_auto_allowed_when_ready():
    env = live_env()
    env.engine.readiness = lambda: READY
    opp = env.opps.add(tx.make_opp(env.clock))
    opp.proven = {"enforced": True, "proven": True}  # a proven pair
    ok, reason, _ = await env.engine.revalidate(opp)
    assert ok, reason


async def test_paper_auto_does_not_need_ready():
    env = tx.make_env()
    env.engine.readiness = lambda: NOT_READY
    opp = env.opps.add(tx.make_opp(env.clock))
    opp.proven = {"enforced": True, "proven": True}  # a proven pair
    ok, reason, _ = await env.engine.revalidate(opp)
    assert ok, reason


async def test_the_owner_can_switch_the_ready_gate_off():
    env = live_env(live_requires_ready=False)
    env.engine.readiness = lambda: NOT_READY
    opp = env.opps.add(tx.make_opp(env.clock))
    opp.proven = {"enforced": True, "proven": True}  # a proven pair
    ok, reason, _ = await env.engine.revalidate(opp)
    assert ok, reason
