"""MetaLabelStore - measured win/loss outcomes of shadow trades per signal context.

Measurement only (ARCHITECTURE.md P3.12 "Meta-label table"): every closed
shadow trade records a triple-barrier outcome (target -> win, stop -> loss,
time limit -> win iff NET > 0) under the key

    (asset, target, first_mover, lag bucket, BTC volatility regime)

``estimate()`` reports a Beta posterior of the win probability - mean and the
10th-percentile lower bound (p10) - with back-off to coarser keys
(full -> (asset, target) -> asset -> global) when a key has too few outcomes.
The prior is ``metalabel_kappa`` pseudo-trades centred on the global base rate.
Nothing here is fitted to past trades: the table only counts outcomes; how
they are used (quality "history" component, Kelly cap) is fixed elsewhere.

Pure Python (stdlib only), deterministic, bounded memory (the newest
``metalabel_keep`` outcomes are kept; older ones are evicted from every
aggregate).  Persistence is duck-typed: ``db.save_metalabel_outcome(dict)``
and ``db.load_metalabel_outcomes(limit)``.

Settings (read with ``getattr(settings, name, default)``):
    metalabel_min_n  = 20      outcomes needed before a level is "trusted"
    metalabel_kappa  = 10.0    prior strength in pseudo-trades
    metalabel_keep   = 20000   outcomes kept in memory / loaded at start
"""
from __future__ import annotations

import logging
import math
from collections import deque
from functools import lru_cache
from typing import Any

log = logging.getLogger(__name__)

LAG_BUCKETS: tuple[str, ...] = ("<0.5", "0.5-1", "1-2", ">2")
REGIMES: tuple[str, ...] = ("low", "mid", "high", "unknown")
LEVELS: tuple[str, ...] = ("full", "asset_target", "asset", "global")
BARRIERS: tuple[str, ...] = ("target", "stop", "time")
UNKNOWN = "unknown"

DEFAULT_MIN_N = 20
DEFAULT_KAPPA = 10.0
DEFAULT_KEEP = 20000
LOWER_QUANTILE = 0.10

_REGIME_LOW = 0.33
_REGIME_HIGH = 0.66

Key = tuple[str, ...]


# ---------------------------------------------------------------------- #
# bucketing / labelling helpers
# ---------------------------------------------------------------------- #
def _num(value: Any) -> float | None:
    """float(value) when it is a finite number, else None (never a guess)."""
    if value is None or isinstance(value, bool):
        return None
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def lag_bucket(lag_pct: Any) -> str:
    """|lag| in percent -> "<0.5" | "0.5-1" | "1-2" | ">2" ("unknown" when missing)."""
    x = _num(lag_pct)
    if x is None:
        return UNKNOWN
    x = abs(x)
    if x < 0.5:
        return "<0.5"
    if x < 1.0:
        return "0.5-1"
    if x < 2.0:
        return "1-2"
    return ">2"


def regime_bucket(btc_vol_pctile: Any) -> str:
    """BTC volatility percentile (0..1) -> "low" (<0.33) | "mid" | "high" (>0.66) | "unknown"."""
    p = _num(btc_vol_pctile)
    if p is None:
        return UNKNOWN
    if p < _REGIME_LOW:
        return "low"
    if p > _REGIME_HIGH:
        return "high"
    return "mid"


def label_outcome(barrier: Any, net_pct: Any) -> str | None:
    """Triple-barrier label: target -> win, stop -> loss, time -> win iff NET > 0.

    Returns None when the outcome cannot be derived from measured data.
    """
    b = str(barrier).strip().lower() if barrier is not None else ""
    if b == "target":
        return "win"
    if b == "stop":
        return "loss"
    if b == "time":
        net = _num(net_pct)
        if net is None:
            return None
        return "win" if net > 0 else "loss"
    return None


def _name(value: Any) -> str:
    if value is None:
        return UNKNOWN
    s = str(value).strip()
    return s or UNKNOWN


# ---------------------------------------------------------------------- #
# Beta distribution (stdlib only)
# ---------------------------------------------------------------------- #
_CF_MIN_ITER = 300
_CF_EPS = 3.0e-16
_CF_FPMIN = 1.0e-300


def _betacf(a: float, b: float, x: float) -> float:
    """Continued fraction for the incomplete beta function (Numerical Recipes betacf)."""
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < _CF_FPMIN:
        d = _CF_FPMIN
    d = 1.0 / d
    h = d
    # convergence needs O(sqrt(max(a, b))) iterations (Numerical Recipes)
    max_iter = _CF_MIN_ITER + int(10.0 * math.sqrt(max(a, b)))
    for m in range(1, max_iter + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < _CF_FPMIN:
            d = _CF_FPMIN
        c = 1.0 + aa / c
        if abs(c) < _CF_FPMIN:
            c = _CF_FPMIN
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < _CF_FPMIN:
            d = _CF_FPMIN
        c = 1.0 + aa / c
        if abs(c) < _CF_FPMIN:
            c = _CF_FPMIN
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < _CF_EPS:
            break
    return h


def betainc(a: float, b: float, x: float) -> float:
    """Regularized incomplete beta I_x(a, b) = CDF of Beta(a, b) at x (a, b > 0)."""
    if a <= 0.0 or b <= 0.0:
        raise ValueError("betainc needs a > 0 and b > 0")
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    log_bt = (math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
              + a * math.log(x) + b * math.log1p(-x))
    bt = math.exp(log_bt)
    if x < (a + 1.0) / (a + b + 2.0):
        value = bt * _betacf(a, b, x) / a
    else:
        value = 1.0 - bt * _betacf(b, a, 1.0 - x) / b
    return min(1.0, max(0.0, value))


def beta_ppf(q: float, a: float, b: float, *, tol: float = 1e-12, max_iter: int = 200) -> float:
    """Quantile of Beta(a, b) by bisection on ``betainc`` (deterministic).

    Degenerate limits: a == 0 -> point mass at 0, b == 0 -> point mass at 1.
    """
    if not 0.0 <= q <= 1.0:
        raise ValueError("quantile must be within [0, 1]")
    if a < 0.0 or b < 0.0 or (a == 0.0 and b == 0.0):
        raise ValueError("beta_ppf needs a, b >= 0 and not both zero")
    if a == 0.0:
        return 0.0
    if b == 0.0:
        return 1.0
    if q <= 0.0:
        return 0.0
    if q >= 1.0:
        return 1.0
    lo, hi = 0.0, 1.0
    for _ in range(max_iter):
        mid = 0.5 * (lo + hi)
        if betainc(a, b, mid) < q:
            lo = mid
        else:
            hi = mid
        if hi - lo < tol:
            break
    return 0.5 * (lo + hi)


@lru_cache(maxsize=4096)
def _ppf_cached(q: float, a: float, b: float) -> float:
    return beta_ppf(q, a, b)


# ---------------------------------------------------------------------- #
# aggregates
# ---------------------------------------------------------------------- #
class _Agg:
    __slots__ = ("n", "wins", "net_sum", "net_n")

    def __init__(self) -> None:
        self.n = 0
        self.wins = 0
        self.net_sum = 0.0
        self.net_n = 0

    def add(self, row: dict, sign: int) -> None:
        self.n += sign
        if row["outcome"] == "win":
            self.wins += sign
        net = row.get("net_pct")
        if net is not None:
            self.net_sum += sign * net
            self.net_n += sign

    @property
    def avg_net_pct(self) -> float | None:
        if self.net_n <= 0:
            return None
        return self.net_sum / self.net_n


def _key_str(key: Key) -> str:
    return "|".join(key) if key else "*"


class MetaLabelStore:
    """Bounded table of triple-barrier outcomes with Beta-posterior estimates."""

    def __init__(self, settings: Any, clock: Any, db: Any | None = None) -> None:
        self._settings = settings
        self._clock = clock
        self._db = db
        self._rows: deque[dict] = deque()
        self._aggs: dict[tuple[str, Key], _Agg] = {}
        self._grades: dict[str, _Agg] = {}
        self._barriers: dict[str, int] = {}
        self._ids: set[str] = set()
        self._updated_ts: float | None = None
        self._rejected = 0
        self._duplicates = 0
        self._load()

    # ------------------------------------------------------------------ #
    # settings
    # ------------------------------------------------------------------ #
    @property
    def min_n(self) -> int:
        v = _num(getattr(self._settings, "metalabel_min_n", DEFAULT_MIN_N))
        return max(1, int(v)) if v is not None else DEFAULT_MIN_N

    @property
    def kappa(self) -> float:
        v = _num(getattr(self._settings, "metalabel_kappa", DEFAULT_KAPPA))
        return max(0.0, v) if v is not None else DEFAULT_KAPPA

    @property
    def keep(self) -> int:
        v = _num(getattr(self._settings, "metalabel_keep", DEFAULT_KEEP))
        return max(1, int(v)) if v is not None else DEFAULT_KEEP

    # ------------------------------------------------------------------ #
    # keys
    # ------------------------------------------------------------------ #
    @staticmethod
    def key(asset: Any, target: Any, first_mover: Any, lag_pct: Any, btc_vol_pctile: Any) -> Key:
        """Full key: (asset, target, first_mover, lag bucket, regime bucket)."""
        return (_name(asset), _name(target), _name(first_mover),
                lag_bucket(lag_pct), regime_bucket(btc_vol_pctile))

    @staticmethod
    def _level_keys(full: Key) -> list[tuple[str, Key]]:
        return [("full", full), ("asset_target", full[:2]), ("asset", full[:1]), ("global", ())]

    # ------------------------------------------------------------------ #
    # recording
    # ------------------------------------------------------------------ #
    def _normalize(self, outcome: dict, *, stamp_now: bool = True) -> dict | None:
        if not isinstance(outcome, dict):
            return None
        asset = str(outcome.get("asset") or "").strip()
        target = str(outcome.get("target") or "").strip()
        if not asset or not target:
            return None
        barrier_raw = outcome.get("barrier")
        barrier = str(barrier_raw).strip().lower() if barrier_raw is not None else None
        barrier = barrier or None
        net_pct = _num(outcome.get("net_pct"))
        label = str(outcome.get("outcome") or "").strip().lower()
        if label not in ("win", "loss"):
            derived = label_outcome(barrier, net_pct)
            if derived is None:
                return None
            label = derived
        lag_pct = _num(outcome.get("lag_pct"))
        pctile = _num(outcome.get("btc_vol_pctile"))
        grade_raw = outcome.get("grade")
        grade = str(grade_raw).strip() if grade_raw is not None else None
        ts = _num(outcome.get("ts"))
        if ts is None and stamp_now:
            ts = float(self._clock.wall())
        fm = outcome.get("first_mover")
        rid = outcome.get("id")
        return {
            "id": str(rid) if rid not in (None, "") else None,
            "asset": asset,
            "target": target,
            "first_mover": str(fm).strip() if fm not in (None, "") else None,
            "lag_pct": lag_pct,
            "btc_vol_pctile": pctile,
            "outcome": label,
            "barrier": barrier,
            "net_pct": net_pct,
            "quality_score": _num(outcome.get("quality_score")),
            "grade": grade or None,
            "ts": ts,
        }

    def _apply(self, row: dict, sign: int) -> None:
        full = self.key(row["asset"], row["target"], row["first_mover"],
                        row["lag_pct"], row["btc_vol_pctile"])
        for level, k in self._level_keys(full):
            ak = (level, k)
            agg = self._aggs.get(ak)
            if agg is None:
                if sign < 0:
                    continue
                agg = self._aggs[ak] = _Agg()
            agg.add(row, sign)
            if agg.n <= 0:
                del self._aggs[ak]
        g = row["grade"] or UNKNOWN
        gagg = self._grades.get(g)
        if gagg is None and sign > 0:
            gagg = self._grades[g] = _Agg()
        if gagg is not None:
            gagg.add(row, sign)
            if gagg.n <= 0:
                del self._grades[g]
        b = row["barrier"] or UNKNOWN
        cnt = self._barriers.get(b, 0) + sign
        if cnt > 0:
            self._barriers[b] = cnt
        else:
            self._barriers.pop(b, None)

    def _push(self, row: dict) -> bool:
        rid = row["id"]
        if rid is not None:
            if rid in self._ids:
                self._duplicates += 1
                return False
            self._ids.add(rid)
        self._rows.append(row)
        self._apply(row, +1)
        keep = self.keep
        while len(self._rows) > keep:
            old = self._rows.popleft()
            self._apply(old, -1)
            if old["id"] is not None:
                self._ids.discard(old["id"])
        ts = row["ts"]
        if ts is not None and (self._updated_ts is None or ts > self._updated_ts):
            self._updated_ts = ts
        return True

    def record(self, outcome: dict) -> bool:
        """Record one closed shadow trade.

        ``outcome``: asset, target, first_mover, lag_pct, btc_vol_pctile,
        outcome "win"|"loss" (derived from barrier/net_pct when absent),
        barrier "target"|"stop"|"time", net_pct, quality_score, grade, ts and an
        optional idempotency ``id`` (e.g. the shadow trade id).  Returns False -
        and records nothing - when measured data is missing or the id was
        already recorded.
        """
        row = self._normalize(outcome)
        if row is None:
            self._rejected += 1
            log.debug("metalabel: outcome rejected (missing asset/target/outcome)")
            return False
        if not self._push(row):
            log.debug("metalabel: duplicate outcome id %s ignored", row["id"])
            return False
        save = getattr(self._db, "save_metalabel_outcome", None) if self._db is not None else None
        if callable(save):
            try:
                save(dict(row))
            except Exception:  # noqa: BLE001
                log.exception("metalabel: save_metalabel_outcome failed")
        return True

    def _load(self) -> None:
        load = getattr(self._db, "load_metalabel_outcomes", None) if self._db is not None else None
        if not callable(load):
            return
        try:
            raw = list(load(self.keep) or [])
        except Exception:  # noqa: BLE001
            log.exception("metalabel: load_metalabel_outcomes failed; starting empty")
            return
        rows = [r for r in (self._normalize(dict(x), stamp_now=False) if isinstance(x, dict) else None
                            for x in raw)
                if r is not None]
        # oldest first whatever the db order (stable; rows without ts keep their place up front)
        rows.sort(key=lambda r: (r["ts"] is not None, r["ts"] or 0.0))
        for row in rows[-self.keep:]:
            self._push(row)
        log.info("metalabel: loaded %d outcomes (%d skipped)", len(rows), len(raw) - len(rows))

    # ------------------------------------------------------------------ #
    # estimates
    # ------------------------------------------------------------------ #
    def _agg(self, level: str, key: Key) -> _Agg | None:
        return self._aggs.get((level, key))

    def base_rate(self) -> float:
        """Global measured win rate (0.5 when nothing was recorded)."""
        g = self._agg("global", ())
        if g is None or g.n <= 0:
            return 0.5
        return g.wins / g.n

    def _posterior(self, wins: int, n: int) -> tuple[float, float, float | None, float | None]:
        """Beta(kappa*base + wins, kappa*(1-base) + losses) -> (alpha, beta, mean, p10)."""
        base = self.base_rate()
        kappa = self.kappa
        alpha = kappa * base + wins
        beta = kappa * (1.0 - base) + (n - wins)
        if alpha + beta <= 0.0:  # kappa == 0 and nothing recorded: no data, no estimate
            return alpha, beta, None, None
        return alpha, beta, alpha / (alpha + beta), _ppf_cached(LOWER_QUANTILE, alpha, beta)

    def estimate(self, asset: Any, target: Any, first_mover: Any, lag_pct: Any,
                 btc_vol_pctile: Any) -> dict:
        """Posterior win probability for this context with back-off.

        Returns {"n", "wins", "mean", "p10", "key", "level"} plus the
        transparent extras "losses", "alpha", "beta", "base_rate",
        "avg_net_pct", "min_n", "levels" (n / wins at every back-off level).
        """
        full = self.key(asset, target, first_mover, lag_pct, btc_vol_pctile)
        levels = self._level_keys(full)
        min_n = self.min_n
        chosen: tuple[str, Key] | None = None
        for level, k in levels:
            agg = self._agg(level, k)
            if agg is not None and agg.n >= min_n:
                chosen = (level, k)
                break
        if chosen is None:
            for level, k in levels:
                agg = self._agg(level, k)
                if agg is not None and agg.n > 0:
                    chosen = (level, k)
                    break
        if chosen is None:
            chosen = ("global", ())
        level, k = chosen
        agg = self._agg(level, k)
        n = agg.n if agg is not None else 0
        wins = agg.wins if agg is not None else 0
        alpha, beta, mean, p10 = self._posterior(wins, n)
        counts = []
        for lv, lk in levels:
            la = self._agg(lv, lk)
            counts.append({"level": lv, "n": la.n if la is not None else 0,
                           "wins": la.wins if la is not None else 0})
        return {
            "n": n,
            "wins": wins,
            "losses": n - wins,
            "mean": mean,
            "p10": p10,
            "key": k,
            "level": level,
            "alpha": alpha,
            "beta": beta,
            "base_rate": self.base_rate(),
            "avg_net_pct": agg.avg_net_pct if agg is not None else None,
            "min_n": min_n,
            "levels": counts,
        }

    # ------------------------------------------------------------------ #
    # reporting
    # ------------------------------------------------------------------ #
    def __len__(self) -> int:
        return len(self._rows)

    def outcomes(self, limit: int | None = None) -> list[dict]:
        """Kept outcomes, oldest first (copies)."""
        rows = list(self._rows)
        if limit is not None:
            rows = rows[-max(0, int(limit)):] if limit > 0 else []
        return [dict(r) for r in rows]

    def by_grade(self) -> dict[str, dict]:
        """grade -> {"n", "win_rate", "avg_net_pct"} (raw measured rates, for STATS)."""
        out: dict[str, dict] = {}
        for grade in sorted(self._grades):
            agg = self._grades[grade]
            out[grade] = {
                "n": agg.n,
                "win_rate": agg.wins / agg.n if agg.n else None,
                "avg_net_pct": agg.avg_net_pct,
            }
        return out

    def snapshot(self, max_keys: int = 50) -> dict:
        """JSON-safe summary: totals, settings, per-grade and the busiest full keys."""
        g = self._agg("global", ())
        n = g.n if g is not None else 0
        wins = g.wins if g is not None else 0
        keys = sorted(((k, agg) for (level, k), agg in self._aggs.items() if level == "full"),
                      key=lambda item: (-item[1].n, item[0]))
        rows = []
        for k, agg in keys[:max(0, int(max_keys))]:
            _, _, mean, p10 = self._posterior(agg.wins, agg.n)
            rows.append({
                "key": _key_str(k),
                "asset": k[0], "target": k[1], "first_mover": k[2],
                "lag_bucket": k[3], "regime": k[4],
                "n": agg.n,
                "wins": agg.wins,
                "win_rate": agg.wins / agg.n if agg.n else None,
                "avg_net_pct": agg.avg_net_pct,
                "mean": mean,
                "p10": p10,
            })
        return {
            "n": n,
            "wins": wins,
            "win_rate": wins / n if n else None,
            "base_rate": self.base_rate(),
            "updated_ts": self._updated_ts,
            "rejected": self._rejected,
            "duplicates": self._duplicates,
            "settings": {"min_n": self.min_n, "kappa": self.kappa, "keep": self.keep},
            "by_grade": self.by_grade(),
            "by_barrier": {b: self._barriers[b] for b in sorted(self._barriers)},
            "keys_total": len(keys),
            "keys": rows,
        }
