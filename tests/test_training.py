"""Training on every priced signal (the deadlock of 2026-10-12: 66 signals, 0 offers, 0 virtual trades).

A signal whose model NET is below the show threshold is still entered VIRTUALLY (ride the leader, same
exit policy, same fees) so the bot measures what it would really have earned.  Such a training trade
never becomes an Opportunity: no card, no Telegram, no AUTO.  Its realized capture (the target's
executable price move from entry to exit, relative to the lag at entry) teaches the catch-up ratio,
so a pair the model under-rates can earn offers from measurements - and one that never follows
keeps being refused."""
from __future__ import annotations

import pytest

from latbot import events
from latbot.config import Settings
from latbot.engine.proven import ProvenBook
from latbot.engine.shadow import ShadowTrader
from latbot.engine.stats import StatsStore
from latbot.clock import SimClock
from latbot.storage.db import Database
from test_ride import step
from test_simple_mode import simple_env

FLAT = {"binance": 100.0, "kucoin": 100.0, "kraken": 100.0}


def training_env(with_proven: bool = False, **over):
    env = simple_env(**over)
    env.adapters["kraken"].taker = 0.004  # 2 x 0.4 % + 0.05: a 1 % lag x 0.6 does not pay -> no offer
    env.proven = ProvenBook(env.settings) if with_proven else None
    sh = ShadowTrader(env.settings, env.bus, env.clock, market=env.pipeline.market_view, stats=env.stats,
                      proven=env.proven)
    env.pipeline.sweep_listeners.append(sh.tick)
    return env, sh


def training_rows(sh: ShadowTrader) -> list[dict]:
    return [r for r in sh.trades() if r.get("training")]


async def test_a_signal_rejected_by_costs_opens_one_training_trade_and_no_opportunity():
    env, sh = training_env()
    await env.warm_up()
    assert await env.push("binance", 101.0) == []  # the model says: costs eat it
    assert env.opps.active() == []
    assert env.topic_names(events.OPP_NEW, events.OPP_UPDATE, events.OPP_STATE) == []  # no card, Telegram, AUTO
    rejected = env.pipeline.diag.snapshot()["funnel"]["rejected"]
    assert rejected.get("fees", 0) + rejected.get("below_min_net", 0) >= 1
    [row] = training_rows(sh)
    assert row["open"] is True and row["expected_net_pct"] < 0.1 and row["lag_pct"] > 0.9
    assert sh.training_opened == 1 and sh.opened == 0


async def test_one_leader_move_is_one_training_trial():
    env, sh = training_env()
    await env.warm_up()
    await env.push("binance", 101.0)
    await step(env, {"binance": 101.2, "kucoin": 100.0, "kraken": 100.0})  # the same move, re-evaluated
    assert len(training_rows(sh)) == 1


async def test_a_training_trade_rides_and_teaches_the_catch_up_from_its_capture():
    env, sh = training_env()
    await env.warm_up()
    await env.push("binance", 101.0)
    for b, k in ((102.0, 100.5), (103.0, 101.5), (103.0, 102.0)):
        await step(env, {"binance": b, "kucoin": 100.0, "kraken": k})
    await step(env, {"binance": 102.5, "kucoin": 100.0, "kraken": 102.0})  # leader -0.49 % from its peak
    [row] = training_rows(sh)
    assert row["open"] is False and row["exit_kind"] == "leader_reversed"
    # capture is measured bid to bid (the spread is already in entry and exit prices, never twice)
    assert row["capture_pct"] == pytest.approx((row["exit_price"] / row["entry_exit_price"] - 1) * 100)
    assert row["capture_pct"] > 1.5
    ratios = env.stats.get("BTC", "kraken").ride_capture_ratios
    # the leader ran on, so the same move was offered too: that offered ride records its own capture
    assert any(r == pytest.approx(row["capture_pct"] / row["lag_pct"]) for r in ratios)
    assert len(ratios) == 1 + sh.opened


async def test_training_results_stay_out_of_the_offered_record_and_the_virtual_balance():
    env, sh = training_env()
    await env.warm_up()
    await env.push("binance", 101.0)
    await step(env, {"binance": 100.5, "kucoin": 100.0, "kraken": 100.0})  # the leader turns: a loss
    s = sh.summary()
    assert s["training"]["closed"] == 1 and s["training"]["avg_actual_net_pct"] < 0
    assert s["closed"] == 0 and s["balance_now"] == s["balance_start"]  # Stats / READY count offers only
    assert {c["key"]: c["value"] for c in s["readiness"]["checks"]}["trades"] == 0
    assert "kraken" in s["training"]["by_target"]


async def test_training_results_do_not_count_for_proven_pairs():
    env, sh = training_env(with_proven=True)
    await env.warm_up()
    await env.push("binance", 101.0)
    await step(env, {"binance": 100.5, "kucoin": 100.0, "kraken": 100.0})
    assert training_rows(sh)[0]["open"] is False
    # the proven gate decides about offers: it is judged on offered trades only (review 2026-10-13)
    assert [p for p in env.proven.pairs() if p["target"] == "kraken"] == []


async def test_an_offer_on_the_same_move_still_gets_its_offered_virtual_trade():
    env, sh = training_env()
    await env.warm_up()
    await env.push("binance", 101.0)  # small lag: rejected on costs -> training ride
    await step(env, {"binance": 102.5, "kucoin": 100.0, "kraken": 100.0})  # the lag grew: offered
    assert len(env.opps.active()) == 1
    assert sh.opened == 1 and sh.training_opened == 1  # the training ride never blocks the offer's trade
    assert "position already riding on this pair" not in sh.skipped


async def test_training_skips_are_counted_once_per_move_and_apart_from_offers():
    env, sh = training_env(min_trade_quote=1e9)  # no book supports the minimum: every trial is skipped
    await env.warm_up()
    await env.push("binance", 101.0)
    for _ in range(3):
        await step(env, {"binance": 101.1, "kucoin": 100.0, "kraken": 100.0})
    assert sh.training_skipped == {"book below min trade size": 1}
    assert sh.skipped == {}


def test_training_rows_never_crowd_offered_trades_out_of_the_record():
    clock = SimClock()
    db = Database(":memory:", clock=clock)
    sh = ShadowTrader(Settings(), events.EventBus(), clock, db=db)
    t0 = clock.wall()
    for i in range(10):  # the period's offered trades, older than the training flood
        db.save_shadow({"id": f"o{i}", "asset": "BTC", "target": "kraken", "opened_ts": t0 + i, "closed_ts": t0 + i + 1,
                        "actual_net_pct": -1.0, "pnl_quote": -1.0, "expected_net_pct": 0.2, "training": False})
    for i in range(5100):
        db.save_shadow({"id": f"t{i}", "asset": "BTC", "target": "kraken", "opened_ts": t0 + 100 + i,
                        "closed_ts": t0 + 101 + i, "actual_net_pct": 0.1, "pnl_quote": 0.1, "training": True})
    s = sh.summary()
    assert s["closed"] == 10 and s["avg_actual_net_pct"] == pytest.approx(-1.0)
    assert {c["key"]: c["value"] for c in s["readiness"]["checks"]}["trades"] == 10
    assert s["all_time_closed"] == 10
    restarted = ShadowTrader(Settings(), events.EventBus(), clock, db=db)
    assert restarted.summary()["balance_now"] == pytest.approx(restarted.summary()["balance_start"] - 10.0)


def test_old_shadow_tables_get_the_training_column(tmp_path):
    import json
    import sqlite3

    path = tmp_path / "old.sqlite3"
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE shadow_trades (id TEXT PRIMARY KEY, opened_ts REAL, closed_ts REAL, json TEXT NOT NULL)")
    con.execute("INSERT INTO shadow_trades VALUES ('a', 1, 2, ?)", (json.dumps({"id": "a", "opened_ts": 1, "training": True}),))
    con.execute("INSERT INTO shadow_trades VALUES ('b', 1, 2, ?)", (json.dumps({"id": "b", "opened_ts": 1}),))
    con.commit()
    con.close()
    db = Database(str(path))
    assert [r["id"] for r in db.list_shadow(10, training=True)] == ["a"]
    assert [r["id"] for r in db.list_shadow(10, training=False)] == ["b"]


async def test_measured_captures_turn_an_under_rated_pair_into_an_offer():
    env, sh = training_env()
    for _ in range(5):  # five rides on kraken captured 1.5 x the lag
        env.stats.record_ride_capture("BTC", "kraken", 1.5)
    await env.warm_up()
    assert len(await env.push("binance", 101.0)) == 1  # 1 % lag x 1.5 - 0.85 % costs pays: an offer
    assert training_rows(sh) == []  # offered, so traded as a normal virtual trade, not as training


async def test_no_training_when_switched_off():
    env, sh = training_env(shadow_train_all_signals=False)
    await env.warm_up()
    await env.push("binance", 101.0)
    assert training_rows(sh) == []


# ---- the catch-up estimator ------------------------------------------------------------------------
def test_catch_up_prefers_ride_captures_and_uses_the_mean():
    st = StatsStore(Settings(simple_mode=True, ride_enabled=True))
    assert st.catchup_ratio("BTC", "kraken") is None
    for r in (0.0, 0.0, 0.9):  # bimodal episode samples: the median (0) hides a 30 % follow rate
        st.record_catchup("BTC", "kraken", r)
    assert st.catchup_ratio("BTC", "kraken") == pytest.approx(0.3)
    for r in (1.0, 1.2, 0.8, -0.5):
        st.record_ride_capture("BTC", "kraken", r)
    assert st.catchup_ratio("BTC", "kraken") == pytest.approx(0.3)  # 4 rides are not enough yet
    st.record_ride_capture("BTC", "kraken", 1.0)
    assert st.catchup_ratio("BTC", "kraken") == pytest.approx((1.0 + 1.2 + 0.8 - 0.5 + 1.0) / 5)


def test_ride_captures_teach_only_the_ride_exit():
    st = StatsStore(Settings(simple_mode=True, ride_enabled=False))  # another exit logic catches something else
    for r in (0.0, 0.0, 0.9):
        st.record_catchup("BTC", "kraken", r)
    for _ in range(5):
        st.record_ride_capture("BTC", "kraken", 2.0)
    assert st.catchup_ratio("BTC", "kraken") == pytest.approx(0.3)


def test_ride_capture_is_clipped():
    st = StatsStore(Settings())
    st.record_ride_capture("BTC", "kraken", 9.0)
    st.record_ride_capture("BTC", "kraken", -9.0)
    assert st.get("BTC", "kraken").ride_capture_ratios == [2.0, -1.5]


# ---- learned data measured against the wrong leader starts over once (2026-10-13) --------------------
def test_leader_relative_samples_of_older_builds_are_dropped_once():
    clock = SimClock()
    db = Database(":memory:", clock=clock)
    s = Settings(simple_mode=True)
    st = StatsStore(s, db)
    for _ in range(5):
        st.record_catchup("HBAR", "kraken", 0.0)
        st.record_ride_capture("HBAR", "kraken", 0.0)
    st.get("HBAR", "kraken").manual_wins = 2  # the owner's real trades stay
    st.save()
    db.kv_delete("catchup_main_markets_v4")  # recorded by a build that watched the wrong leader
    fresh = StatsStore(s, db)
    rs = fresh.get("HBAR", "kraken")
    assert rs.catchup_ratios == [] and rs.ride_capture_ratios == [] and rs.manual_wins == 2
    fresh.record_ride_capture("HBAR", "kraken", 1.0)
    fresh.save()
    assert StatsStore(s, db).get("HBAR", "kraken").ride_capture_ratios == [1.0]  # only once
