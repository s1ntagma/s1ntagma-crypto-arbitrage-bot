"""Offline tests for latbot.engine.opportunity.OpportunityManager."""
from __future__ import annotations

import pytest

from latbot import events
from latbot.clock import SimClock
from latbot.config import Settings
from latbot.engine.opportunity import OpportunityManager
from latbot.events import EventBus
from latbot.models import (
    ExecAssessment,
    ExecutionEconomics,
    LagSignal,
    Light,
    Mode,
    Opportunity,
    OppState,
)


def make_signal(**over) -> LagSignal:
    d = dict(asset="BTC", direction=1, first_mover="binance", first_move_pct=2.0, first_move_start_mono=1000.0,
             reacted={"binance": 2.0}, target="bybit", target_symbol="BTC/USDT", target_move_pct=0.0, lag_pct=2.0,
             expected_catchup_pct=1.2, catchup_ratio=0.6, detected_mono=1000.0, detected_ts=1_700_000_000.0,
             movers_count=2, target_quote="USDT")
    d.update(over)
    return LagSignal(**d)


def make_econ(net: float = 1.0) -> ExecutionEconomics:
    return ExecutionEconomics(direction=1, capital_quote=100.0, executable_quote=100.0, base_amount=1.0,
                              entry_best=100.0, entry_vwap=100.0, exit_best=99.9, exit_vwap_now=99.9,
                              exit_target=101.0, spread_pct=0.1, entry_fee_pct=0.1, exit_fee_pct=0.1,
                              entry_slippage_pct=0.0, exit_slippage_pct=0.0, execution_loss_pct=0.05,
                              gross_pct=net + 0.25, total_cost_pct=0.25, net_pct=net, net_quote=net,
                              liquidity_ok=True)


def make_assess(light: Light = Light.GREEN) -> ExecAssessment:
    label = {Light.GREEN: "HIGH", Light.YELLOW: "MEDIUM", Light.RED: "NOT EXECUTABLE"}[light]
    return ExecAssessment(light=light, label=label, est_execution_s=1.0, margin_s=5.0, probability=0.8)


class SpyStats:
    def __init__(self) -> None:
        self.durations: list[tuple] = []
        self.human: list[tuple] = []

    def record_opportunity_duration(self, asset, exchange, duration_s):
        self.durations.append((asset, exchange, duration_s))

    def record_human_outcome(self, asset, exchange, window_ok, caught_up):
        self.human.append((asset, exchange, window_ok, caught_up))


class SpyDb:
    def __init__(self) -> None:
        self.saved: list[tuple[str, str]] = []

    def save_opportunity(self, opp: Opportunity) -> None:
        self.saved.append((opp.id, opp.state.value))


class Env:
    def __init__(self, **over) -> None:
        self.settings = Settings(**over)
        self.bus = EventBus()
        self.clock = SimClock()
        self.stats = SpyStats()
        self.db = SpyDb()
        self.mgr = OpportunityManager(self.settings, self.bus, self.clock, db=self.db, stats=self.stats)
        self.topics: list[tuple[str, object]] = []
        self.bus.subscribe_all(lambda t, p: self.topics.append((t, p)))

    async def upsert(self, net: float = 1.0, est: float = 8.0, human=Light.GREEN, bot=Light.GREEN, **sig):
        return await self.mgr.upsert(make_signal(**sig), make_econ(net), make_assess(human), make_assess(bot),
                                     "why", est, Mode.PAPER)

    def topic_names(self) -> list[str]:
        return [t for t, _ in self.topics]


async def test_upsert_creates_then_updates_same_key():
    env = Env()
    opp, is_new = await env.upsert(net=1.0)
    assert is_new and opp.id.startswith("opp_") and opp.state is OppState.DETECTED
    assert opp.asset == "BTC" and opp.target_exchange == "bybit" and opp.direction == 1
    assert opp.quote == "USDT" and "bybit.com" in opp.pair_url and "BTC" in opp.pair_url
    assert opp.mode is Mode.PAPER and opp.update_count == 0 and opp.max_net_pct == 1.0
    assert opp.state_history == [(env.clock.wall(), "DETECTED", "detected")]
    assert opp.net_history == [(env.clock.mono(), 1.0)]
    assert env.topic_names() == [events.OPP_NEW]
    assert env.mgr.find_active("BTC", "bybit", 1) is opp
    assert env.db.saved == [(opp.id, "DETECTED")]

    env.clock.advance(0.5)
    opp2, is_new2 = await env.upsert(net=1.5, est=9.0)
    assert opp2 is opp and not is_new2
    assert opp.update_count == 1 and opp.econ.net_pct == 1.5 and opp.max_net_pct == 1.5
    assert opp.estimated_duration_s == 9.0 and opp.last_update_mono == env.clock.mono()
    assert opp.last_valid_mono == env.clock.mono() and len(opp.net_history) == 2
    assert env.topic_names() == [events.OPP_NEW, events.OPP_UPDATE]
    assert len(env.mgr.active()) == 1
    assert env.db.saved == [(opp.id, "DETECTED")]  # persistence throttled to 1/s

    # net below the show threshold does not refresh last_valid_mono
    valid_before = opp.last_valid_mono
    env.clock.advance(0.6)
    await env.upsert(net=0.05)
    assert opp.last_valid_mono == valid_before and opp.update_count == 2
    assert env.db.saved[-1] == (opp.id, "DETECTED") and len(env.db.saved) == 2


async def test_new_id_per_key():
    env = Env()
    a, _ = await env.upsert()
    b, new_b = await env.upsert(direction=-1)
    c, new_c = await env.upsert(target="kraken", target_symbol="BTC/EUR", target_quote="EUR")
    d, new_d = await env.upsert(asset="ETH", target_symbol="ETH/USDT")
    assert new_b and new_c and new_d
    assert len({a.id, b.id, c.id, d.id}) == 4
    assert OpportunityManager.key("BTC", "bybit", 1) == "BTC:bybit:L"
    assert OpportunityManager.key("BTC", "bybit", -1) == "BTC:bybit:S"
    assert c.quote == "EUR" and "kraken" in c.pair_url
    assert [o.econ.net_pct for o in env.mgr.active()] == [1.0, 1.0, 1.0, 1.0]
    assert env.mgr.get(a.id) is a and env.mgr.get("nope") is None


async def test_transitions_enforced():
    env = Env()
    opp, _ = await env.upsert()
    await env.mgr.transition(opp.id, OppState.VALIDATING, "econ")
    await env.mgr.transition(opp.id, OppState.VALID, "ok")
    assert opp.state is OppState.VALID
    assert [s for _, s, _ in opp.state_history] == ["DETECTED", "VALIDATING", "VALID"]
    with pytest.raises(ValueError):
        await env.mgr.transition(opp.id, OppState.FILLED)
    with pytest.raises(ValueError):
        await env.mgr.transition(opp.id, OppState.CLOSED)
    with pytest.raises(KeyError):
        await env.mgr.transition("opp_missing", OppState.VALID)
    same = await env.mgr.transition(opp.id, OppState.VALID)  # no-op
    assert same is opp and len(opp.state_history) == 3
    states = [p for t, p in env.topics if t == events.OPP_STATE]
    assert len(states) == 2
    assert states[-1]["opportunity"] is opp and states[-1]["from"] == "VALIDATING" and states[-1]["to"] == "VALID"
    assert states[-1]["reason"] == "ok"
    assert env.db.saved[-1] == (opp.id, "VALID")


async def test_sweep_expires_closed_window():
    env = Env()
    opp, _ = await env.upsert(est=8.0)
    await env.mgr.transition(opp.id, OppState.VALIDATING)
    await env.mgr.transition(opp.id, OppState.VALID)
    env.clock.advance(4.0)
    await env.upsert(est=8.0)
    assert await env.mgr.sweep(env.clock.mono()) == []
    env.clock.advance(4.0)
    expired = await env.mgr.sweep(env.clock.mono())
    assert expired == [opp]
    assert opp.state is OppState.EXPIRED and opp.expire_reason == "window closed"
    assert opp.expired_mono == env.clock.mono() and opp.actual_duration_s == pytest.approx(8.0)
    assert env.stats.durations == [("BTC", "bybit", pytest.approx(8.0))]
    assert env.mgr.find_active("BTC", "bybit", 1) is None and env.mgr.active() == []
    assert env.mgr.history() == [opp] and env.mgr.get(opp.id) is opp
    assert opp.caught_up is None and opp.human_would_make_it is False
    assert env.stats.human == [("BTC", "bybit", False, False)]
    assert await env.mgr.expire(opp.id, "again") is None
    assert await env.mgr.expire("nope", "x") is None
    # a fresh opportunity for the same key gets a new id
    opp2, is_new = await env.upsert()
    assert is_new and opp2.id != opp.id


async def test_sweep_no_updates_and_net_below_threshold():
    env = Env(move_window_s=5.0, min_net_pct_show=0.1)
    a, _ = await env.upsert(est=100.0)
    b, _ = await env.upsert(est=100.0, target="kraken", target_symbol="BTC/EUR")
    env.clock.advance(10.5)
    await env.upsert(est=100.0, net=0.05)  # a updated with a tiny net
    expired = await env.mgr.sweep(env.clock.mono())
    assert {o.id: o.expire_reason for o in expired} == {a.id: "net below threshold", b.id: "no updates"}


async def test_executing_opportunities_are_untouched():
    env = Env()
    opp, _ = await env.upsert(est=8.0)
    for st in (OppState.VALIDATING, OppState.VALID, OppState.AUTO_SELECTED, OppState.EXECUTING):
        await env.mgr.transition(opp.id, st)
    assert env.mgr.executing() == [opp] and env.mgr.active() == []
    env.clock.advance(20.0)
    assert await env.mgr.sweep(env.clock.mono()) == []
    assert opp.state is OppState.EXECUTING
    assert await env.mgr.expire(opp.id, "x") is None
    same, is_new = await env.upsert(net=5.0)
    assert same is opp and not is_new and opp.update_count == 0 and opp.econ.net_pct == 1.0
    assert env.mgr.find_active("BTC", "bybit", 1) is None
    await env.mgr.transition(opp.id, OppState.FILLED)
    env.clock.advance(1.0)
    await env.mgr.transition(opp.id, OppState.CLOSED, "done")
    assert opp.is_terminal and opp.actual_duration_s == pytest.approx(21.0)
    assert env.mgr.history()[0] is opp and env.mgr.executing() == []
    assert env.stats.human == [("BTC", "bybit", False, False)]
    # key is free again
    nxt, is_new = await env.upsert()
    assert is_new and nxt is not opp


async def test_best_ignores_double_red():
    env = Env()
    assert env.mgr.best(env.clock.mono()) is None
    red, _ = await env.upsert(net=5.0, human=Light.RED, bot=Light.RED)
    good, _ = await env.upsert(net=1.0, human=Light.YELLOW, bot=Light.RED, target="kraken", target_symbol="BTC/EUR")
    assert env.mgr.best(env.clock.mono()) is good
    assert env.mgr.active()[0] is red
    better, _ = await env.upsert(net=2.0, human=Light.RED, bot=Light.GREEN, asset="ETH", target_symbol="ETH/USDT")
    assert env.mgr.best(env.clock.mono()) is better
    env.clock.advance(100.0)  # windows closed
    assert env.mgr.best(env.clock.mono()) is None


async def test_counters():
    env = Env()
    g, _ = await env.upsert(net=1.0, human=Light.GREEN, bot=Light.RED)
    y, _ = await env.upsert(net=1.0, human=Light.YELLOW, bot=Light.RED, target="kraken", target_symbol="BTC/EUR")
    r, _ = await env.upsert(net=1.0, human=Light.RED, bot=Light.RED, asset="ETH", target_symbol="ETH/USDT")
    g.notified_ts = env.clock.wall()
    c = env.mgr.counters(env.clock.mono())
    assert c["active"] == 3 and c["today"] == 3
    assert (c["green"], c["yellow"], c["red"]) == (1, 1, 1)
    assert c["human_executable"] == 2 and c["bot_executable"] == 0
    assert c["telegram_alerts"] == 1 and c["expired"] == 0
    for k in ("paper_pnl", "live_pnl", "realized_pnl", "failed_trades", "avg_latency_ms", "avg_execution_time_s",
              "success_rate"):
        assert k in c
    await env.mgr.expire(r.id, "test")
    c2 = env.mgr.counters(env.clock.mono())
    assert c2["active"] == 2 and c2["expired"] == 1 and c2["today"] == 3


async def test_history_ring_and_eviction():
    env = Env(opp_history_keep=2)
    ids = []
    for i, asset in enumerate(("BTC", "ETH", "SOL")):
        opp, _ = await env.upsert(asset=asset, target_symbol=f"{asset}/USDT")
        ids.append(opp.id)
        env.clock.advance(1.0)
        await env.mgr.expire(opp.id, "test")
    hist = env.mgr.history()
    assert [o.id for o in hist] == [ids[2], ids[1]]
    assert env.mgr.get(ids[0]) is None and env.mgr.get(ids[2]) is not None
    assert len(env.mgr.history(limit=1)) == 1


async def test_mark_caught_up_feeds_human_outcome():
    env = Env(human_execution_time_s=7.0)
    opp, _ = await env.upsert(est=20.0)
    assert env.mgr.mark_caught_up("BTC", "bybit", 1) == 1
    assert env.mgr.mark_caught_up("BTC", "bybit", -1) == 0
    assert env.mgr.mark_caught_up("BTC", "kraken") == 0
    assert opp.caught_up is True
    env.clock.advance(10.0)
    await env.mgr.expire(opp.id, "window closed")
    assert opp.human_would_make_it is True
    assert env.stats.human == [("BTC", "bybit", True, True)]
    # too short a window: caught up but a human could not have made it
    opp2, _ = await env.upsert(est=20.0)
    env.mgr.mark_caught_up("BTC", "bybit")
    env.clock.advance(3.0)
    await env.mgr.expire(opp2.id, "window closed")
    assert opp2.human_would_make_it is False
    assert env.stats.human[-1] == ("BTC", "bybit", False, True)


async def test_cancel_does_not_record_human_outcome_and_db_failure_tolerated():
    class BadDb:
        def save_opportunity(self, opp):
            raise RuntimeError("disk full")

    settings = Settings()
    bus, clock = EventBus(), SimClock()
    mgr = OpportunityManager(settings, bus, clock, db=BadDb(), stats=SpyStats())
    opp, _ = await mgr.upsert(make_signal(), make_econ(), make_assess(), make_assess(), "why", 8.0, Mode.LIVE)
    assert opp.mode is Mode.LIVE
    await mgr.transition(opp.id, OppState.CANCELLED, "user")
    assert opp.is_terminal and opp.expired_mono is not None and mgr.history() == [opp]
    assert mgr._stats.human == []  # noqa: SLF001


async def test_upsert_relabels_mode_on_update():
    env = Env()
    opp, _ = await env.upsert()
    assert opp.mode is Mode.PAPER
    live, is_new = await env.mgr.upsert(make_signal(), make_econ(1.0), make_assess(), make_assess(), "why", 8.0, Mode.LIVE)
    assert live is opp and not is_new and opp.mode is Mode.LIVE
    assert opp.to_dict(env.clock.mono())["mode"] == "live" and env.topic_names()[-1] == events.OPP_UPDATE
    # relabel_mode re-labels every active card (the pipeline sweep calls it after a PAPER/LIVE switch)
    env.topics.clear()
    assert await env.mgr.relabel_mode(Mode.PAPER) == [opp]
    assert opp.mode is Mode.PAPER and env.topic_names() == [events.OPP_UPDATE]
    assert await env.mgr.relabel_mode(Mode.PAPER) == []
    await env.mgr.transition(opp.id, OppState.VALIDATING)
    await env.mgr.transition(opp.id, OppState.VALID)
    await env.mgr.transition(opp.id, OppState.AUTO_SELECTED)
    await env.mgr.transition(opp.id, OppState.EXECUTING)
    assert await env.mgr.relabel_mode(Mode.LIVE) == [] and opp.mode is Mode.PAPER  # executing: owned by the engine
