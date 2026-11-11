"""Ride the leader: hold while the leader climbs, sell when its move is over."""
from __future__ import annotations

from latbot.engine.exit_policy import RideConfig, RidePosition, decide

CFG = RideConfig(stop_pct=1.0, reversal_pct=0.3, lag_closed_pct=0.1, stall_s=5.0, trail_arm_pct=0.3,
                 trail_pct=0.3, max_hold_s=300.0)


def pos(**kw) -> RidePosition:
    base = dict(direction=1, entry_price=100.0, cost_pct=0.2, entry_mono=0.0, leader_anchor=100.0, target_anchor=100.0)
    base.update(kw)
    return RidePosition(**base)


def test_hold_while_the_leader_keeps_climbing():
    p = pos()
    for t, leader, target in ((1, 101.0, 100.1), (2, 101.5, 100.4), (3, 102.0, 100.9)):
        d = decide(p, t, target, leader, CFG)
        assert d.action == "hold" and d.code == "leader_rising"


def test_leader_reversal_sells():
    p = pos()
    decide(p, 1, 100.5, 102.0, CFG)  # leader peak 102
    d = decide(p, 2, 101.0, 101.6, CFG)  # -0.39 % from the peak
    assert d.sell and d.code == "leader_reversed" and "развернулся" in d.text


def test_caught_up_and_leader_stalled_takes_the_profit():
    p = pos()
    decide(p, 1, 101.0, 102.0, CFG)  # leader peak at t=1
    assert decide(p, 3, 101.95, 102.0, CFG).action == "hold"  # caught up, but the leader stalled only 2 s
    d = decide(p, 6.5, 101.95, 102.0, CFG)  # 5.5 s without a new high, lag 0.05 % <= 0.1 %
    assert d.sell and d.code == "caught_up" and d.net_pct > 1.5


def test_trailing_keeps_most_of_a_big_move():
    p = pos(leader_anchor=None, target_anchor=None)  # lag unknown: trailing still works
    for t, px in ((1, 101.0), (2, 102.5), (3, 103.0)):
        assert decide(p, t, px, 100.0 + t, CFG).action == "hold"
    d = decide(p, 4, 102.6, 103.0, CFG)  # leader holds, target pulls back 0.39 % from 103
    assert d.sell and d.code == "trailing" and d.net_pct > 2.0


def test_stop_and_max_hold():
    p = pos()
    assert decide(p, 1, 98.9, 100.0, CFG).code == "stop"
    p = pos()
    decide(p, 1, 100.1, 100.5, CFG)
    assert decide(p, 301, 100.1, 100.5, CFG).code in ("max_hold", "caught_up")
    p = pos(leader_anchor=None)
    assert decide(p, 301, 100.1, 100.5, CFG).code == "max_hold"


def test_short_side_is_mirrored():
    p = pos(direction=-1)
    assert decide(p, 1, 99.5, 98.0, CFG).action == "hold"  # leader falling = going our way
    d = decide(p, 2, 99.4, 98.4, CFG)  # leader bounced +0.41 % from its low
    assert d.sell and d.code == "leader_reversed"


def test_no_price_is_hold_not_a_decision():
    p = pos()
    d = decide(p, 1, None, 101.0, CFG)
    assert d.action == "hold" and d.code == "no_price"
    assert d.to_dict()["net_pct"] is None
