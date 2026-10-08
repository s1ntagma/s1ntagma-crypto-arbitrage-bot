"""Shadow trading = training (Phase 3): every opportunity with NET >= shadow_min_net_pct is traded
virtually regardless of its quality; three-way outcome (target / stop / time) feeds the meta-label table."""
from __future__ import annotations

import pytest

from latbot.engine.metalabel import MetaLabelStore
from latbot.engine.shadow import ShadowTrader
from test_pipeline import Env, detect


def _env(**kw):
    env = Env()
    ml = MetaLabelStore(env.settings, env.clock)
    sh = ShadowTrader(env.settings, env.bus, env.clock, metalabel=ml, **kw)
    return env, sh, ml


async def test_quality_and_leverage_are_recorded_and_target_is_a_win():
    env, sh, ml = _env()
    opp = await detect(env)
    row = sh.trades()[0]
    assert row["quality_score"] is None and row["grade"] == ""  # not computed in this bare pipeline: honest empty
    assert row["first_mover"] == opp.first_mover and row["lag_pct"] == pytest.approx(opp.signal.lag_pct)
    assert row["stop_pct"] == pytest.approx(env.settings.shadow_stop_pct if hasattr(env.settings, "shadow_stop_pct") else 1.0)
    env.clock.advance(0.3)
    await env.push("kraken", 101.5)
    closed = sh.trades()[0]
    assert closed["barrier"] == "target"
    assert len(ml) == 1 and ml.outcomes()[0]["outcome"] == "win" and ml.outcomes()[0]["barrier"] == "target"


async def test_stop_barrier_closes_as_loss():
    env, sh, ml = _env()
    await detect(env)
    env.clock.advance(0.3)
    await env.push("kraken", 98.0)  # the target venue goes the other way by 2 % > 1 % stop
    row = sh.trades()[0]
    assert row["open"] is False and row["barrier"] == "stop" and row["actual_net_pct"] < 0
    assert ml.outcomes()[0]["outcome"] == "loss"


async def test_window_end_is_the_time_barrier():
    env, sh, ml = _env()
    opp = await detect(env)
    await env.opps.expire(opp.id, "window over")
    row = sh.trades()[0]
    assert row["barrier"] == "time"
    assert ml.outcomes()[0]["outcome"] == ("win" if row["actual_net_pct"] > 0 else "loss")


async def test_quality_from_the_opportunity_is_copied():
    env, sh, ml = _env()
    await env.warm_up()
    await env.push("binance", 102.0)
    # the enricher (Phase 3 integration) fills opp.quality / opp.leverage before OPP_NEW; emulate it
    orig = env.pipeline._enricher

    def enricher():
        base = orig()

        def enrich(o):
            base(o)
            o.quality = {"score": 77, "grade": "B", "passed": True, "btc_vol_pctile": 0.4}
            o.leverage = {"max_by_signal": 2.0, "stop_pct": 0.8, "recommended": 1.0}
        return enrich

    env.pipeline._enricher = enricher
    await env.push("kucoin", 102.0)
    row = sh.trades()[0]
    assert row["quality_score"] == 77 and row["grade"] == "B" and row["leverage_max"] == 2.0
    assert row["stop_pct"] == pytest.approx(0.8)  # the leverage plan's stop wins over the default
    env.clock.advance(0.3)
    await env.push("kraken", 101.5)
    row = sh.trades()[0]
    assert row["pnl_leveraged_quote"] == pytest.approx(row["pnl_quote"] * 2.0)
    assert sh.summary()["by_grade"]["B"]["closed"] == 1
    assert ml.outcomes()[0]["grade"] == "B" and ml.outcomes()[0]["btc_vol_pctile"] == pytest.approx(0.4)


async def test_below_training_threshold_is_skipped():
    env, sh, _ = _env()
    env.settings.shadow_min_net_pct = 50.0
    await detect(env)
    assert sh.trades() == [] and sh.skipped.get("NET below training threshold") == 1
