"""Regression tests for the ride / proven review findings."""
from __future__ import annotations

import asyncio

import test_execution as tx
from latbot.engine.exit_policy import RideConfig, RidePosition, decide
from latbot.engine.proven import ProvenBook
from latbot.engine.shadow import ShadowTrader
from latbot.models import OppState
from latbot.storage.db import Database
from test_proven import trades
from test_ride import ride_env, step
from test_simple_mode import simple_env


def test_short_anchors_use_signed_moves():
    p = RidePosition(direction=-1, entry_price=100.0, cost_pct=0.2, entry_mono=0.0, first_move_pct=-1.0, target_move_pct=-0.1)
    p.set_anchors(99.0, 99.9)
    assert abs(p.leader_anchor - 100.0) < 1e-9 and abs(p.target_anchor - 100.0) < 1e-9
    d = decide(p, 5.5, 99.95, 99.0, RideConfig(), target_mid=99.9)
    assert d.lag_pct > 0.8 and d.code != "caught_up"  # the target has not followed: no false 'догнала'


async def test_target_fresh_leader_stale_still_stops():
    env, sh = ride_env()
    await env.warm_up()
    await env.push("binance", 101.0)
    for i in range(20):  # the leader goes silent; the target keeps streaming and falls only later
        env.clock.advance(1.0)
        await env.push("kraken", 100.0 if i < 14 else 98.0)
        await env.pipeline.sweep_once()
    assert env.pipeline.leader_price("BTC", "binance") is None  # the leader really is stale by now
    row = sh.trades()[0]
    assert row["open"] is False and row["exit_kind"] == "stop" and row["barrier"] == "stop"


async def test_book_that_cannot_carry_the_position_gives_no_price():
    env = simple_env()
    await env.warm_up()
    v = env.pipeline.market_view("BTC", "kraken", "binance", 1e6, 1)  # far more than the 3 visible levels
    assert v is None
    assert env.pipeline.market_view("BTC", "kraken", "binance", 1.0, 1) is not None


async def test_one_virtual_position_per_pair_while_it_rides():
    env, sh = ride_env()
    await env.warm_up()
    await env.push("binance", 101.0)
    [opp] = env.opps.active()
    await env.opps.expire(opp.id, "test: offer ended")
    await step(env, {"binance": 102.0, "kucoin": 100.0, "kraken": 100.0})
    assert len(sh.trades()) == 1 and sh.skipped.get("position already riding on this pair", 0) >= 1


def test_orphan_open_virtual_trades_are_closed_as_no_data_on_start():
    db = Database(":memory:")
    db.save_shadow({"id": "o1", "asset": "BTC", "target": "kraken", "opened_ts": 1.0, "closed_ts": None,
                    "actual_net_pct": None, "pnl_quote": None})
    env = simple_env()
    sh = ShadowTrader(env.settings, env.bus, env.clock, db=db)
    row = [r for r in sh.trades() if r["id"] == "o1"][0]
    assert row["closed_ts"] is not None and row["barrier"] == "no_data" and sh.summary()["open"] == 0


def test_new_period_resets_proven_pairs():
    db = Database(":memory:")
    env = simple_env()
    book = ProvenBook(env.settings, db=db)
    sh = ShadowTrader(env.settings, env.bus, env.clock, db=db, proven=book)
    for t in trades(30, 0, asset="BTC", target="kraken"):
        t["opened_ts"] = env.clock.wall()
        book.record(t)
    assert book.assess("BTC", "kraken")["proven"]
    env.clock.advance(10.0)
    sh.new_period()
    assert not book.assess("BTC", "kraken")["proven"] and book.counts()["proven"] == 0


async def test_limited_account_falls_back_to_the_card_capital():
    env = simple_env(shadow_unlimited=False, shadow_balance_quote=150.0)
    sh = ShadowTrader(env.settings, env.bus, env.clock, market=env.pipeline.market_view)
    await env.warm_up()
    opp = (await env.push("binance", 102.0))[0]
    rows = sh.trades()
    assert len(rows) == 1 and rows[0]["capital_quote"] <= 150.0
    assert opp.best_size["capital_quote"] > 150.0  # the best size did not fit, the card capital did


async def test_auto_sends_no_entry_when_the_offer_expired_during_revalidation():
    async def expire(opp):
        opp.state = OppState.EXPIRED  # e.g. the leader reversed while revalidation waited on the network

    env = tx.make_env(pipeline=tx.StubPipeline(on_refresh=expire))
    opp = env.opps.add(tx.make_opp(env.clock))
    await env.engine.select_auto(opp.id)
    await env.engine.confirm_auto(opp.id)
    await asyncio.sleep(0.05)
    assert env.adapter().placed() == []


async def test_entry_window_counts_from_the_leaders_move():
    env, _ = ride_env(ride_entry_max_age_s=20.0)
    await env.warm_up()
    await env.push("binance", 101.0)
    [opp] = env.opps.active()
    for _ in range(4):
        await step(env, {"binance": 101.5, "kucoin": 100.0, "kraken": 100.0}, dt=5.5)
    assert env.opps.get(opp.id).state.value not in ("VALID", "NOTIFIED", "DETECTED")


def test_proven_pairs_follow_the_stats_period_on_first_start():
    db = Database(":memory:")
    env = simple_env()
    for t in trades(30, 0, asset="BTC", target="kraken"):
        t["opened_ts"] = env.clock.wall() - 3600.0  # an older build's trades, before any period existed
        db.save_shadow(t)
    book = ProvenBook(env.settings, db=db)
    assert book.assess("BTC", "kraken")["proven"]  # no period yet: everything counts
    ShadowTrader(env.settings, env.bus, env.clock, db=db, proven=book)  # opens the first period now
    assert not book.assess("BTC", "kraken")["proven"]
