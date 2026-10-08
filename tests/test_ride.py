"""Ride the leader end to end: entry while the leader's move is intact, the virtual position rides on
after the entry offer ends and is sold by the exit policy (leader reversal, caught up, trailing)."""
from __future__ import annotations

from latbot.engine.shadow import ShadowTrader
from test_simple_mode import simple_env


def ride_env(**over):
    env = simple_env(**over)
    sh = ShadowTrader(env.settings, env.bus, env.clock, market=env.pipeline.market_view)
    env.pipeline.sweep_listeners.append(sh.tick)
    return env, sh


async def step(env, prices, dt=1.0):
    env.clock.advance(dt)
    await env.push_all(prices)
    await env.pipeline.sweep_once()


async def test_entry_is_allowed_while_the_leader_still_climbs():
    env, _ = ride_env()
    await env.warm_up()
    assert len(await env.push("binance", 101.0)) == 1  # +1 %: entry
    [opp] = env.opps.active()
    await env.opps.expire(opp.id, "test: owner skipped it")
    for px in (101.5, 102.0, 102.5):  # 20 s later the leader is still making highs
        await step(env, {"binance": px, "kucoin": 100.0, "kraken": 100.0}, dt=5.0)
    assert len(env.opps.active()) == 1  # a new entry 15 s after the move began is still fine


async def test_no_entry_after_the_leader_turned():
    env, _ = ride_env()
    await env.warm_up()
    await env.push("binance", 102.0)
    [opp] = env.opps.active()
    await step(env, {"binance": 101.5, "kucoin": 100.0, "kraken": 100.0})  # -0.5 % from the peak
    assert env.opps.get(opp.id).state.value not in ("VALID", "NOTIFIED", "DETECTED")
    assert env.pipeline.diag.snapshot()["funnel"]["rejected"].get("leader_reversed", 0) >= 1


async def test_shadow_rides_the_move_and_sells_when_the_leader_turns():
    env, sh = ride_env()
    await env.warm_up()
    await env.push("binance", 101.0)
    assert sh.trades()[0]["open"] is True
    # the leader keeps climbing, kraken follows late
    for b, k in ((102.0, 100.5), (103.0, 101.5), (104.0, 103.0), (104.0, 103.6)):
        await step(env, {"binance": b, "kucoin": 100.0, "kraken": k})
        assert sh.trades()[0]["open"] is True, "held while the leader climbs"
    await step(env, {"binance": 103.5, "kucoin": 100.0, "kraken": 103.5})  # leader -0.48 % from its peak
    row = sh.trades()[0]
    assert row["open"] is False and row["exit_kind"] == "leader_reversed" and row["actual_net_pct"] > 2.0
    assert sh.summary()["by_exit"] == {"leader_reversed": 1}


async def test_position_outlives_the_entry_offer_and_cuts_on_reversal():
    env, sh = ride_env()
    await env.warm_up()
    await env.push("binance", 101.0)
    [opp] = env.opps.active()
    await env.opps.expire(opp.id, "test: entry window over")
    assert sh.trades()[0]["open"] is True  # the offer ended, the position did not
    await step(env, {"binance": 100.6, "kucoin": 100.0, "kraken": 100.0})
    row = sh.trades()[0]
    assert row["open"] is False and row["exit_kind"] == "leader_reversed" and row["barrier"] == "time"


async def test_no_fresh_price_closes_as_no_data_after_ride_stale_s():
    env, sh = ride_env(ride_stale_s=5.0)
    await env.warm_up()
    await env.push("binance", 101.0)
    for _ in range(12):  # nothing arrives at all (prices count as fresh for ~11 s, then 5 s grace)
        env.clock.advance(2.0)
        await env.pipeline.sweep_once()
    row = sh.trades()[0]
    assert row["open"] is False and row["barrier"] == "no_data"


# ---- MANUAL escort: the same exit policy tells the owner to hold or sell ------------------------
def test_manual_escort_says_hold_then_sell_when_the_leader_turns():
    from types import SimpleNamespace

    import test_execution as tx

    prices = {"target_exit": 100.0, "target_mid": 100.05, "leader_mid": 101.0}

    class ViewPipeline(tx.StubPipeline):
        def market_view(self, asset, target, leader, base, direction):
            return dict(prices)

    env = tx.make_env(tx.make_settings(simple_mode=True), pipeline=ViewPipeline())
    opp = env.opps.add(tx.make_opp(env.clock))
    manual = SimpleNamespace(capital_quote=100.0, actual_entry_price=100.0, expected_exit_price=105.0, exchange=opp.target_exchange)
    econ = SimpleNamespace(exit_target=105.0, exit_vwap_now=100.0)
    first = env.engine._escort(opp, manual, econ, 30.0)
    assert first["sell_signal"] == "wait" and first["sell_text"].startswith("ДЕРЖИ") and first["exit_advice"]["action"] == "hold"
    prices.update(target_exit=103.0, target_mid=103.05, leader_mid=106.0)  # leader runs, target follows
    env.clock.advance(1.0)
    assert env.engine._escort(opp, manual, SimpleNamespace(exit_target=105.0, exit_vwap_now=103.0), 30.0)["sell_signal"] == "wait"
    prices.update(target_exit=103.2, target_mid=103.25, leader_mid=105.5)  # leader -0.47 % from its peak
    env.clock.advance(1.0)
    out = env.engine._escort(opp, manual, SimpleNamespace(exit_target=105.0, exit_vwap_now=103.2), 30.0)
    assert out["sell_signal"] == "sell_now" and "лидер развернулся" in out["sell_text"]


# ---- AUTO: one market exit when the policy says sell ----------------------------------------------
async def test_auto_rides_and_exits_once_at_market_on_leader_reversal():
    import asyncio

    import test_execution as tx
    from latbot.models import OrderType

    prices = {"target_exit": 100.3, "target_mid": 100.35, "leader_mid": 105.0}

    class ViewPipeline(tx.StubPipeline):
        def market_view(self, asset, target, leader, base, direction):
            return dict(prices)

        def target_exit_price(self, book, ticker, base, direction, now, bound):
            assert book is not None  # AUTO prices the exit on the fresh target book (hard rule 4)
            return prices["target_exit"], prices["target_mid"], False

        def leader_price(self, asset, leader, now=None):
            return prices["leader_mid"]

    env = tx.make_env(tx.make_settings(simple_mode=True), pipeline=ViewPipeline(), market_price=104.0)
    opp = env.opps.add(tx.make_opp(env.clock))
    task = asyncio.create_task(tx.run_auto(env, opp))
    await tx.wait_until(lambda: len(env.adapter().placed()) == 1)  # entry sent and filled, now riding
    await asyncio.sleep(0.6)
    assert not task.done() and len(env.adapter().placed()) == 1  # holding: the leader is still at its high
    prices.update(target_exit=104.0, target_mid=104.05, leader_mid=104.4)  # leader -0.57 % from its peak
    _res, trade = await asyncio.wait_for(task, 3.0)
    reqs = env.adapter().placed()
    assert [r.type for r in reqs] == [OrderType.LIMIT, OrderType.MARKET]  # one entry, one exit - nothing else
    assert trade.state.value == "CLOSED" and trade.realized_pnl_quote > 0
    assert any("лидер развернулся" in m for _, m in trade.events)
