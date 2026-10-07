"""Offline tests for latbot.engine.duration.DurationEstimator."""
from __future__ import annotations

import pytest

from latbot.config import Settings
from latbot.engine.duration import DurationEstimator
from latbot.engine.stats import StatsStore
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
             expected_catchup_pct=1.2, catchup_ratio=0.6, detected_mono=1000.0, detected_ts=1_700_000_000.0)
    d.update(over)
    return LagSignal(**d)


def make_econ(net: float = 1.0) -> ExecutionEconomics:
    return ExecutionEconomics(direction=1, capital_quote=100.0, executable_quote=100.0, base_amount=1.0,
                              entry_best=100.0, entry_vwap=100.0, exit_best=99.9, exit_vwap_now=99.9,
                              exit_target=101.0, spread_pct=0.1, entry_fee_pct=0.1, exit_fee_pct=0.1,
                              entry_slippage_pct=0.0, exit_slippage_pct=0.0, execution_loss_pct=0.05,
                              gross_pct=net + 0.25, total_cost_pct=0.25, net_pct=net, net_quote=net,
                              liquidity_ok=True)


def make_assess() -> ExecAssessment:
    return ExecAssessment(light=Light.GREEN, label="HIGH", est_execution_s=1.0, margin_s=5.0, probability=0.8)


def make_opp(first_seen: float, net_history: list[tuple[float, float]], est: float = 8.0) -> Opportunity:
    sig = make_signal()
    return Opportunity(id="opp_x", asset="BTC", target_exchange="bybit", target_symbol="BTC/USDT",
                       first_mover="binance", direction=1, mode=Mode.PAPER, state=OppState.VALID, signal=sig,
                       econ=make_econ(), human=make_assess(), bot=make_assess(), why="", detected_ts=0.0,
                       first_seen_mono=first_seen, last_valid_mono=first_seen, last_update_mono=first_seen,
                       estimated_duration_s=est, net_history=list(net_history))


def test_base_from_default_and_stats():
    s = Settings(default_opp_duration_s=8.0)
    assert DurationEstimator(s, None).estimate(make_signal(), None, 1000.0) == 8.0
    st = StatsStore(s)
    est = DurationEstimator(s, st)
    assert est.estimate(make_signal(), None, 1000.0) == 8.0
    st.record_reaction("BTC", "bybit", 3.0, 0.5)
    assert est.estimate(make_signal(), None, 1000.0) == 3.0  # median reaction delay
    st.record_opportunity_duration("BTC", "bybit", 12.0)
    assert est.estimate(make_signal(), None, 1000.0) == 12.0  # median duration wins


def test_live_decay_blend():
    s = Settings(default_opp_duration_s=8.0)
    est = DurationEstimator(s, None)
    opp = make_opp(1000.0, [(1000.0, 1.0), (1001.0, 0.8), (1002.0, 0.6)])
    # slope -0.2 %/s, net_now 0.6 -> remaining 3 s, live = age 2 + 3 = 5, blend = 0.5*8 + 0.5*5
    assert est.estimate(make_signal(), opp, 1002.0) == pytest.approx(6.5)
    # rising net: no decay fit -> base
    opp_up = make_opp(1000.0, [(1000.0, 0.5), (1001.0, 0.8), (1002.0, 1.0)])
    assert est.estimate(make_signal(), opp_up, 1002.0) == 8.0
    # too few points / too short span -> base
    opp_few = make_opp(1000.0, [(1000.0, 1.0), (1000.2, 0.5), (1000.4, 0.1)])
    assert est.estimate(make_signal(), opp_few, 1000.4) == 8.0


def test_clamps_and_age_floor():
    s = Settings(default_opp_duration_s=8.0, min_opp_duration_s=2.0, max_opp_duration_s=20.0)
    est = DurationEstimator(s, None)
    # steep decay -> live tiny -> clamp to min
    opp = make_opp(1000.0, [(1000.0, 0.03), (1001.0, 0.02), (1002.0, 0.0)])
    assert est.estimate(make_signal(), opp, 1002.0) == pytest.approx(5.0)  # 0.5*8 + 0.5*(2+0)=5
    opp2 = make_opp(1000.0, [(1000.0, 0.03), (1002.0, 0.02), (1003.0, 0.0)])
    r = est.estimate(make_signal(), opp2, 1003.0)
    assert r >= 3.1
    # slow decay -> live huge -> clamp to max
    opp3 = make_opp(1000.0, [(1000.0, 5.0), (1001.0, 4.999), (1002.0, 4.998)])
    assert est.estimate(make_signal(), opp3, 1002.0) == 20.0
    # old opportunity: never below age + 0.1 even beyond max
    old = make_opp(1000.0, [])
    assert est.estimate(make_signal(), old, 1200.0) == pytest.approx(200.1)
    assert est.estimate(make_signal(), None, 1200.0) == 8.0
