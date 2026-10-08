"""Phase 3 gates: Telegram and AUTO require a passed quality check (when the quality was computed)."""
from __future__ import annotations

import test_execution as tx
import test_telegram as tt

PASSED = {"score": 78, "grade": "B", "passed": True, "vetoes": [], "summary": "ok"}
FAILED = {"score": 52, "grade": "C", "passed": False, "vetoes": [], "summary": "ниже порога"}
VETO = {"score": 40, "grade": "D", "passed": False, "vetoes": ["CPI через 90 с"], "summary": "вето"}


def test_telegram_alert_needs_passed_quality():
    env = tt.Env()
    n = env.notifier
    assert n.should_alert(tt.make_opp(env.clock))  # quality not computed (context off): unchanged behaviour
    assert n.should_alert(tt.make_opp(env.clock, quality=PASSED))
    assert not n.should_alert(tt.make_opp(env.clock, quality=FAILED))
    assert not n.should_alert(tt.make_opp(env.clock, quality=VETO))


def test_alert_card_shows_quality_and_leverage():
    env = tt.Env()
    opp = tt.make_opp(env.clock, quality=PASSED, leverage={"recommended": 1.0, "max_by_signal": 3.0, "venue_kind": "spot"})
    text = env.notifier.format_alert(opp, env.clock.mono())
    assert "Качество 78/100 (B)" in text and "плечо до 3×" in text
    plain = tt.make_opp(env.clock)
    assert "Качество" not in env.notifier.format_alert(plain, env.clock.mono())


async def test_auto_revalidation_rejects_failed_quality():
    async def set_quality(opp):
        opp.quality = dict(FAILED)

    env = tx.make_env(pipeline=tx.StubPipeline(on_refresh=set_quality))
    opp = env.opps.add(tx.make_opp(env.clock))
    ok, reason, _ = await env.engine.revalidate(opp)
    assert not ok and reason.startswith("Signal quality 52/100")

    async def set_veto(opp):
        opp.quality = dict(VETO)

    env = tx.make_env(pipeline=tx.StubPipeline(on_refresh=set_veto))
    opp = env.opps.add(tx.make_opp(env.clock))
    ok, reason, _ = await env.engine.revalidate(opp)
    assert not ok and "CPI через 90 с" in reason
