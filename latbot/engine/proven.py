"""Proven pairs: offer only what the bot's own virtual trades have shown to work.

A profit is never certain in a lag trade (the exit is in the future), so "наверняка" is a measured,
conservative rule per (asset, target venue), counted only over virtual trades of the CURRENT strategy
(ride / simple / full - outcomes of another exit logic would mislead):

* proven   - at least ``proven_min_trades`` trades, the 10th percentile of the win probability
             (Beta(1 + wins, 1 + losses)) >= ``proven_min_p10`` and average NET > ``proven_min_avg_net_pct``;
* failing  - at least ``proven_min_trades`` trades and the 90th percentile < 0.5 or average NET <= 0;
* training - everything else (not enough trades yet, or not convincing).

Only proven offers reach the board, Telegram and AUTO in simple mode; every offer keeps being traded
virtually (training), so a pair can become proven - or stop being proven - from new measurements.
Nothing is fitted: thresholds are settings, the rest is counting.
"""
from __future__ import annotations

import logging
import math
from typing import Any

from .metalabel import beta_ppf

log = logging.getLogger(__name__)

_LOAD_LIMIT = 20000


def current_strategy(settings: Any) -> str:
    """The exit logic virtual trades are measured with (outcomes of different logics never mix)."""
    active = getattr(settings, "ride_active", None)
    if callable(active) and active():
        return "ride"
    return "simple" if getattr(settings, "simple_mode", False) else "full"


def _num(v: Any) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


class _Pair:
    __slots__ = ("n", "wins", "net_sum", "net_sq", "last_ts")

    def __init__(self) -> None:
        self.n = 0
        self.wins = 0
        self.net_sum = 0.0
        self.net_sq = 0.0
        self.last_ts = 0.0

    def add(self, net_pct: float, ts: float) -> None:
        self.n += 1
        self.wins += 1 if net_pct > 0 else 0
        self.net_sum += net_pct
        self.net_sq += net_pct * net_pct
        self.last_ts = max(self.last_ts, ts)

    @property
    def avg(self) -> float | None:
        return self.net_sum / self.n if self.n else None


class ProvenBook:
    """Per (asset, target) results of closed virtual trades of the current strategy."""

    def __init__(self, settings: Any, db: Any | None = None) -> None:
        self._settings = settings
        self._db = db
        self._pairs: dict[tuple[str, str, str], _Pair] = {}  # (strategy, asset, target)
        self._ids: set[str] = set()
        self._load()

    # ------------------------------------------------------------------ settings
    def _f(self, name: str, default: float) -> float:
        v = _num(getattr(self._settings, name, default))
        return default if v is None else v

    @property
    def min_trades(self) -> int:
        return max(1, int(self._f("proven_min_trades", 30)))

    @property
    def enforced(self) -> bool:
        """The gate decides (simple mode with proven_gate_enabled); otherwise information only."""
        return bool(getattr(self._settings, "simple_mode", False) and getattr(self._settings, "proven_gate_enabled", True))

    # ------------------------------------------------------------------ recording
    def _load(self) -> None:
        fn = getattr(self._db, "list_shadow", None) if self._db is not None else None
        if fn is None:
            return
        try:
            rows = fn(_LOAD_LIMIT)
        except Exception:  # noqa: BLE001
            log.exception("proven pairs: shadow history not loaded")
            return
        for r in reversed(rows):  # oldest first
            self.record(r)

    def record(self, trade: dict[str, Any]) -> bool:
        """One closed virtual trade (ShadowTrade.to_dict()).  Unknown outcome / no strategy tag -> ignored."""
        if not isinstance(trade, dict) or trade.get("closed_ts") is None:
            return False
        net = _num(trade.get("actual_net_pct"))
        strategy = str(trade.get("strategy") or "")
        asset, target = str(trade.get("asset") or ""), str(trade.get("target") or "")
        tid = str(trade.get("id") or "")
        if net is None or not strategy or not asset or not target or (tid and tid in self._ids):
            return False
        if tid:
            self._ids.add(tid)
        self._pairs.setdefault((strategy, asset, target), _Pair()).add(net, _num(trade.get("closed_ts")) or 0.0)
        return True

    # ------------------------------------------------------------------ queries
    def _status(self, p: _Pair | None) -> dict[str, Any]:
        need = self.min_trades
        n = p.n if p else 0
        wins = p.wins if p else 0
        avg = p.avg if p else None
        a, b = 1.0 + wins, 1.0 + (n - wins)
        p10 = beta_ppf(0.10, a, b)
        p90 = beta_ppf(0.90, a, b)
        min_p10 = self._f("proven_min_p10", 0.60)
        min_avg = self._f("proven_min_avg_net_pct", 0.05)
        if n >= need and p10 >= min_p10 and avg is not None and avg > min_avg:
            status, text = "proven", f"PROVEN · {wins}/{n} wins · worst-case win rate {p10 * 100:.0f}% · avg NET {avg:+.2f}%"
        elif n >= need and (p90 < 0.5 or (avg is not None and avg <= 0.0)):
            status, text = "failing", f"FAILING · {wins}/{n} wins · avg NET {avg:+.2f}% - hidden, still training"
        elif n >= need:
            status, text = "training", f"TRAINING · {wins}/{n} wins: not convincing yet (needs worst-case ≥ {min_p10 * 100:.0f}%)"
        else:
            status, text = "training", f"TRAINING · {n}/{need} virtual trades"
        return {"status": status, "proven": status == "proven", "n": n, "wins": wins, "need": need,
                "win_rate": wins / n if n else None, "p10": p10, "p90": p90, "avg_net_pct": avg, "text": text,
                "enforced": self.enforced}

    def assess(self, asset: str, target: str) -> dict[str, Any]:
        key = (current_strategy(self._settings), str(asset), str(target))
        return self._status(self._pairs.get(key))

    def pairs(self) -> list[dict[str, Any]]:
        """Every measured pair of the current strategy, proven first, then by trades."""
        strategy = current_strategy(self._settings)
        out = []
        for (st, asset, target), p in self._pairs.items():
            if st != strategy:
                continue
            out.append({"asset": asset, "target": target, **self._status(p)})
        order = {"proven": 0, "training": 1, "failing": 2}
        out.sort(key=lambda r: (order.get(r["status"], 3), -r["n"], r["asset"], r["target"]))
        return out

    def counts(self) -> dict[str, int]:
        c = {"proven": 0, "training": 0, "failing": 0}
        for r in self.pairs():
            c[r["status"]] = c.get(r["status"], 0) + 1
        return c


__all__ = ["ProvenBook", "current_strategy"]
