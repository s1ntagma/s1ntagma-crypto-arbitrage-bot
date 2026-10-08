"""Every opportunity carries a quality report and a leverage plan, recomputed on every update."""
from __future__ import annotations

from latbot.context.base import Factor
from latbot.engine.metalabel import MetaLabelStore
from tests.test_pipeline import Env, detect


class StubContext:
    def __init__(self, factors=None, event=False):
        self._factors = factors or []
        self.event = event
        self.calls = 0

    def factors(self, asset, direction, now_wall=None):
        self.calls += 1
        return list(self._factors)

    def asset_stats(self, asset):
        return {"vol_1m_pct": 0.05, "tail_1m_pct": 0.2, "vol_pctile": 0.4}

    def funding(self, asset):
        return None

    def event_window(self, now_wall=None):
        return self.event


async def test_quality_and_leverage_on_every_opportunity_without_context():
    env = Env()
    opp = await detect(env)
    q, lev = opp.quality, opp.leverage
    assert isinstance(q["score"], int) and q["grade"] in "ABCD" and isinstance(q["passed"], bool)
    assert q["coverage"] == 0.0  # no context hub: nothing counted as support
    assert lev["recommended"] >= 1.0 and lev["venue_kind"] == "spot"
    assert lev["recommended"] == 1.0  # spot venue (default max_leverage 1): never leveraged
    assert env.pipeline.diag.quality_checked == 1  # counted once per NEW opportunity


async def test_context_factors_flow_into_quality_and_veto_blocks():
    env = Env()
    veto = Factor("macro_event", "Macro", True, score=0.0, weight=1.0, detail="CPI через 90 с", veto=True)
    env.pipeline.context = StubContext([veto])
    opp = await detect(env)
    assert opp.quality["passed"] is False and any("CPI" in v for v in opp.quality["vetoes"])
    assert opp.quality["btc_vol_pctile"] == 0.4
    assert opp.leverage["sigma_hold_pct"] is not None  # volatility from the context bars
    assert env.pipeline.diag.quality_fail_reasons  # DIAG knows why


async def test_metalabel_estimate_is_used_for_history():
    env = Env()
    ml = MetaLabelStore(env.settings, env.clock)
    for i in range(30):
        ml.record({"id": f"x{i}", "asset": "BTC", "target": "kraken", "first_mover": "binance", "lag_pct": 1.5,
                   "btc_vol_pctile": 0.4, "barrier": "target", "net_pct": 0.5, "ts": float(i)})
    env.pipeline.metalabel = ml
    env.pipeline.context = StubContext()
    opp = await detect(env)
    hist = next(c for c in opp.quality["components"] if c["key"] == "history")
    assert hist["available"] is True and hist["value"] > 0.5


async def test_quality_recomputed_on_refresh():
    env = Env()
    ctx = StubContext()
    env.pipeline.context = ctx
    opp = await detect(env)
    before = ctx.calls
    env.clock.advance(0.2)
    await env.push("kraken", 100.2)
    assert ctx.calls > before
    assert env.opps.get(opp.id).quality["score"] is not None
