"""Proven-only offers, best-size virtual training and the live-readiness checklist."""
from __future__ import annotations

import test_execution as tx
import test_telegram as tt
from latbot.engine.proven import ProvenBook
from latbot.engine.shadow import ShadowTrader, _readiness
from latbot.config import Settings
from test_proven import trades
from test_simple_mode import simple_env

TRAINING = {"status": "training", "proven": False, "enforced": True, "n": 3, "need": 30, "wins": 2, "text": "TRAINING · 3/30"}
PROVEN = dict(TRAINING, status="proven", proven=True, n=30, wins=29, text="PROVEN · 29/30")


def test_telegram_announces_only_proven_pairs():
    env = tt.Env()
    assert not env.notifier.should_alert(tt.make_opp(env.clock, proven=dict(TRAINING)))
    assert env.notifier.should_alert(tt.make_opp(env.clock, proven=dict(PROVEN)))
    assert env.notifier.should_alert(tt.make_opp(env.clock, proven=dict(TRAINING, enforced=False)))  # info only


async def test_auto_refuses_a_pair_that_is_not_proven():
    async def training(opp):
        opp.proven = dict(TRAINING)

    env = tx.make_env(pipeline=tx.StubPipeline(on_refresh=training))
    opp = env.opps.add(tx.make_opp(env.clock))
    ok, reason, _ = await env.engine.revalidate(opp)
    assert not ok and reason.startswith("Pair not proven yet")


async def test_offer_is_training_until_the_pair_proves_itself_but_is_always_trained():
    env = simple_env()
    book = ProvenBook(env.settings)
    env.pipeline.proven = book
    sh = ShadowTrader(env.settings, env.bus, env.clock, market=env.pipeline.market_view, proven=book)
    await env.warm_up()
    opp = (await env.push("binance", 102.0))[0]
    assert opp.proven["status"] == "training" and opp.proven["enforced"] is True
    assert sh.trades()[0]["proven_at_entry"] is False  # traded virtually anyway (training)
    for t in trades(30, 0, asset="BTC", target="kraken"):
        book.record(t)
    await env.push("binance", 102.0)
    assert env.opps.get(opp.id).proven["proven"] is True


async def test_virtual_trade_uses_the_best_size_on_the_book():
    env = simple_env()
    sh = ShadowTrader(env.settings, env.bus, env.clock, market=env.pipeline.market_view)
    await env.warm_up()
    opp = (await env.push("binance", 102.0))[0]
    b = opp.best_size
    assert b and b["capital_quote"] > 0 and b["net_pct"] >= env.settings.min_net_pct_show
    assert b["net_quote"] >= opp.econ.net_quote - 1e-9  # never worse than the card capital
    assert sh.trades()[0]["capital_quote"] == b["capital_quote"]


def test_readiness_checklist_names_what_is_missing():
    s = Settings(simple_mode=True, ready_min_trades=10)
    rows = [{"closed_ts": float(i), "actual_net_pct": 0.3 if i % 5 else -0.1, "proven_at_entry": i >= 5} for i in range(12)]
    r = _readiness(rows, {"proven": 1}, s)
    assert r["ready"] is True and all(c["ok"] for c in r["checks"])
    r = _readiness(rows[:3], {"proven": 0}, s)
    assert r["ready"] is False
    missing = {c["key"] for c in r["checks"] if not c["ok"]}
    assert {"trades", "proven"} <= missing
