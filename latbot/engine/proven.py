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
from collections import deque
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
    __slots__ = ("n", "wins", "net_sum", "net_sq", "last_ts", "recent")

    def __init__(self, window: int) -> None:
        self.n = 0
        self.wins = 0
        self.net_sum = 0.0
        self.net_sq = 0.0
        self.last_ts = 0.0
        self.recent: deque[float] = deque(maxlen=max(1, window))  # the last proven_min_trades results

    def add(self, net_pct: float, ts: float) -> None:
        self.n += 1
        self.wins += 1 if net_pct > 0 else 0
        self.net_sum += net_pct
        self.net_sq += net_pct * net_pct
        self.last_ts = max(self.last_ts, ts)
        self.recent.append(net_pct)

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
        self._period_start = self._read_period()
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
    def _read_period(self) -> float | None:
        """Only trades of the current Stats period count (New period = prove again on the new trades)."""
        kv_get = getattr(self._db, "kv_get", None) if self._db is not None else None
        if kv_get is None:
            return None
        try:
            return _num(kv_get("stats_period_start_ts"))
        except Exception:  # noqa: BLE001
            return None

    def reset(self, period_start: float | None) -> None:
        self._pairs.clear()
        self._ids.clear()
        self._period_start = period_start

    def follow_period(self, period_start: float | None) -> None:
        """Count exactly the trades of ``period_start``'s period (reloaded from the DB when it differs)."""
        if period_start == self._period_start:
            return
        self.reset(period_start)
        self._load()

    def _load(self) -> None:
        fn = getattr(self._db, "list_shadow", None) if self._db is not None else None
        if fn is None:
            return
        try:
            try:
                rows = fn(_LOAD_LIMIT, training=False)
            except TypeError:  # a store without the filter
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
        if trade.get("training"):
            return False  # the gate decides about offers: it is judged on offered trades only
        net = _num(trade.get("actual_net_pct"))
        strategy = str(trade.get("strategy") or "")
        asset, target = str(trade.get("asset") or ""), str(trade.get("target") or "")
        tid = str(trade.get("id") or "")
        if net is None or not strategy or not asset or not target or (tid and tid in self._ids):
            return False
        opened = _num(trade.get("opened_ts"))
        if self._period_start is not None and opened is not None and opened < self._period_start:
            return False
        if tid:
            self._ids.add(tid)
        pair = self._pairs.get((strategy, asset, target))
        if pair is None:
            pair = self._pairs[(strategy, asset, target)] = _Pair(self.min_trades)
        pair.add(net, _num(trade.get("closed_ts")) or 0.0)
        return True

    # ------------------------------------------------------------------ queries
    def _passes(self, wins: int, n: int, avg: float | None) -> tuple[bool, float, str]:
        """(rule holds, worst-case win rate, what fails) for one record."""
        q = min(0.5, max(0.001, self._f("proven_quantile", 0.05)))
        lo = beta_ppf(q, 1.0 + wins, 1.0 + (n - wins))
        min_lo = self._f("proven_min_p10", 0.60)
        min_avg = self._f("proven_min_avg_net_pct", 0.05)
        if lo < min_lo:
            return False, lo, f"needs worst-case win rate ≥ {min_lo * 100:.0f}% (now {lo * 100:.0f}%)"
        if avg is None or avg <= min_avg:
            return False, lo, f"needs avg NET > {min_avg:+.2f}%"
        return True, lo, ""

    def _status(self, p: _Pair | None) -> dict[str, Any]:
        need = self.min_trades
        n = p.n if p else 0
        wins = p.wins if p else 0
        avg = p.avg if p else None
        ok, lo, why = self._passes(wins, n, avg)
        if ok and p is not None:  # the record must also hold on its last `need` trades (repeated checks)
            rec = list(p.recent)
            r_ok, _lo, r_why = self._passes(sum(1 for x in rec if x > 0), len(rec), sum(rec) / len(rec) if rec else None)
            if not r_ok:
                ok, why = False, "recent trades: " + r_why
        p90 = beta_ppf(0.90, 1.0 + wins, 1.0 + (n - wins))
        if n >= need and ok:
            status, reason = "proven", ""
            text = f"PROVEN · {wins}/{n} wins · worst-case win rate {lo * 100:.0f}% · avg NET {avg:+.2f}%"
        elif n >= need and (p90 < 0.5 or (avg is not None and avg <= 0.0)):
            status, reason = "failing", "losing record"
            text = f"FAILING · {wins}/{n} wins · avg NET {avg:+.2f}% - hidden, still training"
        elif n >= need:
            status, reason = "training", why
            text = f"NOT CONVINCING · {wins}/{n} wins: {why}"
        else:
            status, reason = "training", f"needs {need} trades"
            text = f"TRAINING · {n}/{need} virtual trades"
        return {"status": status, "proven": status == "proven", "n": n, "wins": wins, "need": need,
                "win_rate": wins / n if n else None, "p10": lo, "p90": p90, "avg_net_pct": avg, "text": text,
                "reason": reason, "enforced": self.enforced}

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
