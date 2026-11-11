"""Ride the leader: when to sell a position bought on the lagging venue (owner's simple mode).

The leader (the coin's most liquid venue) climbs; the lagging venue follows later.  The position is
held while the leader keeps going and sold when the move is over.  Pure and in-memory: the shadow
trader, the MANUAL escort and AUTO execution call the same ``decide`` with the latest prices.

Rules, first match wins (all thresholds are settings, percents are percents):
1. ``stop``            NET on the position <= -ride_stop_pct
2. ``leader_reversed`` the leader fell ride_reversal_pct from its best price since the entry
3. ``caught_up``       the target is within ride_lag_closed_pct of the leader's move and the leader
                       made no new high for ride_stall_s
4. ``trailing``        NET reached ride_trail_arm_pct and the target fell ride_trail_pct from its best
5. ``max_hold``        the position is older than ride_max_hold_s
otherwise ``hold`` ("leader_rising" while the leader keeps making highs, else "waiting").
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

SELL_REASONS = {
    "stop": "стоп: минус {stop:g}% на позиции",
    "leader_reversed": "лидер развернулся: откат {drop:.2f}% от его пика",
    "caught_up": "догнала, лидер больше не растёт",
    "trailing": "прибыль откатилась от пика на {pull:.2f}%",
    "max_hold": "лимит времени {max_hold:g} с",
}
HOLD_REASONS = {
    "leader_rising": "лидер ещё растёт",
    "waiting": "лидер стоит, твоя биржа ещё не догнала",
    "no_price": "нет свежей цены",
}


@dataclass(slots=True)
class RideConfig:
    stop_pct: float = 1.0
    reversal_pct: float = 0.3
    lag_closed_pct: float = 0.1
    stall_s: float = 5.0
    trail_arm_pct: float = 0.3
    trail_pct: float = 0.3
    max_hold_s: float = 300.0

    @classmethod
    def from_settings(cls, s: Any) -> "RideConfig":
        return cls(
            stop_pct=float(getattr(s, "ride_stop_pct", 1.0)),
            reversal_pct=float(getattr(s, "ride_reversal_pct", 0.3)),
            lag_closed_pct=float(getattr(s, "ride_lag_closed_pct", 0.1)),
            stall_s=float(getattr(s, "ride_stall_s", 5.0)),
            trail_arm_pct=float(getattr(s, "ride_trail_arm_pct", 0.3)),
            trail_pct=float(getattr(s, "ride_trail_pct", 0.3)),
            max_hold_s=float(getattr(s, "ride_max_hold_s", 300.0)),
        )


@dataclass(slots=True)
class RidePosition:
    direction: int  # +1 long (bought on the laggard), -1 short
    entry_price: float  # what the position was entered at on the target
    cost_pct: float  # round trip: entry fee + exit fee + execution loss (percent)
    entry_mono: float
    leader_anchor: float | None = None  # leader price before its move began
    target_anchor: float | None = None  # target price before the leader's move began
    leader_peak: float | None = None  # best leader price since the entry (in the trade's direction)
    leader_peak_mono: float | None = None
    target_peak: float | None = None  # best exit price on the target since the entry
    peak_net_pct: float = field(default=float("-inf"))
    last_fresh_mono: float | None = None
    first_move_pct: float | None = None  # the leader's signed move when the position was opened
    target_move_pct: float | None = None  # the target's signed move when the position was opened

    def set_anchors(self, leader_mid: float | None, target_mid: float | None) -> None:
        """Pre-move prices from the signed moves measured at the entry: price / (1 + move / 100).  An
        anchor whose price is missing now stays unset and is filled from a later view."""
        if self.leader_anchor is None and leader_mid and self.first_move_pct is not None:
            self.leader_anchor = leader_mid / (1.0 + float(self.first_move_pct) / 100.0)
        if self.target_anchor is None and target_mid and self.target_move_pct is not None:
            self.target_anchor = target_mid / (1.0 + float(self.target_move_pct) / 100.0)

    def net_at(self, exit_price: float) -> float:
        if self.entry_price <= 0 or exit_price <= 0:
            return float("-inf")
        return (exit_price / self.entry_price - 1.0) * 100.0 * self.direction - self.cost_pct

    def _better(self, a: float, b: float) -> bool:
        return (a - b) * self.direction > 0

    def update(self, now_mono: float, target_exit: float, leader_price: float | None) -> None:
        """Track the best prices since the entry (call with every fresh price)."""
        self.last_fresh_mono = now_mono
        if self.target_peak is None or self._better(target_exit, self.target_peak):
            self.target_peak = target_exit
        self.peak_net_pct = max(self.peak_net_pct, self.net_at(target_exit))
        if leader_price is not None and leader_price > 0:
            if self.leader_peak is None or self._better(leader_price, self.leader_peak):
                self.leader_peak = leader_price
                self.leader_peak_mono = now_mono

    def lag_pct(self, target_mid: float | None, leader_price: float | None) -> float | None:
        """Leader's move minus the target's move, both from their pre-move prices (None if unknown)."""
        if not (self.leader_anchor and self.target_anchor and target_mid and leader_price):
            return None
        d = self.direction
        leader_move = (leader_price / self.leader_anchor - 1.0) * 100.0 * d
        target_move = (target_mid / self.target_anchor - 1.0) * 100.0 * d
        return leader_move - target_move


@dataclass(slots=True)
class Decision:
    action: str  # "hold" | "sell"
    code: str
    text: str  # Russian, for the UI / Telegram
    net_pct: float | None = None
    peak_net_pct: float | None = None
    lag_pct: float | None = None

    @property
    def sell(self) -> bool:
        return self.action == "sell"

    def to_dict(self) -> dict[str, Any]:
        def num(v: float | None) -> float | None:
            return None if v is None or v != v or abs(v) == float("inf") else round(v, 4)
        return {"action": self.action, "code": self.code, "text": self.text, "net_pct": num(self.net_pct),
                "peak_net_pct": num(self.peak_net_pct), "lag_pct": num(self.lag_pct)}


def _pct_from_peak(peak: float | None, price: float | None, direction: int) -> float:
    if not peak or not price:
        return 0.0
    return (peak - price) / peak * 100.0 * direction


def decide(pos: RidePosition, now_mono: float, target_exit: float | None, leader_price: float | None,
           cfg: RideConfig, target_mid: float | None = None) -> Decision:
    """Hold or sell now.  Updates the position's peaks with the given (fresh) prices."""
    if target_exit is None or target_exit <= 0:
        return Decision("hold", "no_price", HOLD_REASONS["no_price"])
    pos.update(now_mono, target_exit, leader_price)
    net = pos.net_at(target_exit)
    lag = pos.lag_pct(target_mid or target_exit, leader_price)

    def sell(code: str, **kw: float) -> Decision:
        return Decision("sell", code, SELL_REASONS[code].format(**kw), net, pos.peak_net_pct, lag)

    if net <= -cfg.stop_pct:
        return sell("stop", stop=cfg.stop_pct)
    drop = _pct_from_peak(pos.leader_peak, leader_price, pos.direction)
    if leader_price is not None and drop >= cfg.reversal_pct:
        return sell("leader_reversed", drop=drop)
    stalled = pos.leader_peak_mono is not None and now_mono - pos.leader_peak_mono >= cfg.stall_s
    if lag is not None and lag <= cfg.lag_closed_pct and stalled:
        return sell("caught_up")
    pull = _pct_from_peak(pos.target_peak, target_exit, pos.direction)
    if pos.peak_net_pct >= cfg.trail_arm_pct and pull >= cfg.trail_pct:
        return sell("trailing", pull=pull)
    if now_mono - pos.entry_mono >= cfg.max_hold_s:
        return sell("max_hold", max_hold=cfg.max_hold_s)
    code = "waiting" if stalled else "leader_rising"
    return Decision("hold", code, HOLD_REASONS[code], net, pos.peak_net_pct, lag)


__all__ = ["Decision", "RideConfig", "RidePosition", "decide", "SELL_REASONS", "HOLD_REASONS"]
