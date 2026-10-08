"""How BTC / ETH / an asset moved +10 s / +60 s / +5 min / +30 min after each event
type (ARCHITECTURE P3.7).  Measurement only: the profile is shown and used as
a context factor; no strategy parameter is fitted to it.

* ``watch()`` registers an event (macro release: scheduled time; news: the time
  the bot learned about it - a base price cannot be captured retroactively).
* ``tick()`` (every ~5 s) captures the base price at ``event_ts`` (the last
  sample at or before it within ``reaction_base_tolerance_s``, else the first
  one after it within the tolerance) and each horizon ``event_ts + h`` (first
  sample at or after it, within max(base tolerance, 10 % of h)).
* A missed base skips the asset for that event; a missed horizon is skipped.
  Nothing is interpolated.
* Every captured horizon is one sample, persisted immediately through the
  duck-typed ``db.save_event_reaction(dict)`` (upsert by event_key + asset +
  horizon_s) and reloaded with ``db.load_event_reactions(limit)`` at start.
"""
from __future__ import annotations

import logging
import math
import statistics
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_HORIZONS_S: tuple[int, ...] = (10, 60, 300, 1800)


def _norm_asset(asset: Any) -> str:
    return str(asset or "").strip().upper()


def _norm_surprise(s: Any) -> int | None:
    if s is None or isinstance(s, bool):
        return None
    try:
        f = float(s)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return 0 if f == 0 else (1 if f > 0 else -1)


def _price(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) and f > 0 else None


def _num(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


@dataclass(slots=True)
class _Watch:
    key: str
    event_type: str
    event_ts: float
    assets: list[str]
    surprise: int | None
    created_ts: float
    base: dict[str, tuple[float, float]] = field(default_factory=dict)  # asset -> (price, ts)
    base_missed: set[str] = field(default_factory=set)
    captured: dict[str, dict[int, float]] = field(default_factory=dict)  # asset -> horizon -> pct
    missed: dict[str, set[int]] = field(default_factory=dict)  # asset -> horizons missed


class EventReactionStore:
    def __init__(self, settings: Any, clock: Any, price_fn: Callable[[str], float | None], db: Any = None) -> None:
        self._settings = settings
        self._clock = clock
        self._price_fn = price_fn
        self._db = db
        self._pending: dict[str, _Watch] = {}
        # samples: (event_key, asset, horizon_s) -> row (JSON-safe dict, the persisted form)
        self._samples: dict[tuple[str, str, int], dict[str, Any]] = {}
        self._by_type_asset: dict[tuple[str, str], set[tuple[str, str, int]]] = {}
        self._by_event: dict[str, set[tuple[str, str, int]]] = {}
        self._done: set[str] = set()  # event keys measured (or abandoned) - never watched twice
        self.counters: dict[str, int] = {
            "watched": 0, "rejected_late": 0, "captures": 0, "samples": 0, "missed_base": 0,
            "missed_horizons": 0, "loaded": 0, "save_errors": 0,
        }
        self._load()

    # ------------------------------------------------------------------ #
    # settings
    # ------------------------------------------------------------------ #
    def _cfg(self, name: str, default: float) -> float:
        v = getattr(self._settings, name, default)
        try:
            f = float(v)
        except (TypeError, ValueError):
            return float(default)
        return f if math.isfinite(f) else float(default)

    def horizons(self) -> list[int]:
        raw = getattr(self._settings, "reaction_horizons_s", DEFAULT_HORIZONS_S)
        out: set[int] = set()
        try:
            for h in raw:
                h = int(h)
                if h > 0:
                    out.add(h)
        except (TypeError, ValueError):
            out = set(DEFAULT_HORIZONS_S)
        return sorted(out or DEFAULT_HORIZONS_S)

    def _base_tol(self) -> float:
        return max(0.0, self._cfg("reaction_base_tolerance_s", 5.0))

    def _horizon_tol(self, h: int) -> float:
        return max(self._base_tol(), self._cfg("reaction_horizon_tolerance_frac", 0.1) * h)

    # ------------------------------------------------------------------ #
    # persistence
    # ------------------------------------------------------------------ #
    def _load(self) -> None:
        if self._db is None or not hasattr(self._db, "load_event_reactions"):
            return
        keep = max(1, int(self._cfg("reaction_keep", 20000)))
        try:
            rows = self._db.load_event_reactions(keep)
        except Exception as e:  # noqa: BLE001 - history is optional, the store still works
            log.warning("reactions: could not load history: %s", e)
            return
        n = 0
        for row in rows or []:
            sample = self._normalise_row(row)
            if sample is not None:
                self._add_sample(sample)
                n += 1
        self.counters["loaded"] = n
        self._trim()

    @staticmethod
    def _normalise_row(row: Any) -> dict[str, Any] | None:
        if not isinstance(row, dict):
            return None
        key = str(row.get("event_key") or "").strip()
        etype = str(row.get("event_type") or "").strip()
        asset = _norm_asset(row.get("asset"))
        try:
            h = int(row.get("horizon_s"))
        except (TypeError, ValueError):
            return None
        move = _num(row.get("move_pct"))
        ets = _num(row.get("event_ts"))
        if not key or not etype or not asset or h <= 0 or move is None or ets is None:
            return None
        out = dict(row)
        out.update(event_key=key, event_type=etype, asset=asset, horizon_s=h, move_pct=move, event_ts=ets,
                   surprise=_norm_surprise(row.get("surprise")))
        return out

    def _save(self, sample: dict[str, Any]) -> None:
        if self._db is None or not hasattr(self._db, "save_event_reaction"):
            return
        try:
            self._db.save_event_reaction(dict(sample))
        except Exception as e:  # noqa: BLE001 - a DB hiccup must not stop measuring
            self.counters["save_errors"] += 1
            log.warning("reactions: could not save %s/%s/%s: %s", sample.get("event_key"), sample.get("asset"),
                        sample.get("horizon_s"), e)

    def _add_sample(self, sample: dict[str, Any]) -> None:
        sk = (sample["event_key"], sample["asset"], int(sample["horizon_s"]))
        self._samples[sk] = sample
        self._by_type_asset.setdefault((sample["event_type"], sample["asset"]), set()).add(sk)
        self._by_event.setdefault(sample["event_key"], set()).add(sk)
        self._done.add(sample["event_key"])

    def _trim(self) -> None:
        keep = max(1, int(self._cfg("reaction_keep", 20000)))
        if len(self._samples) <= keep:
            return
        ordered = sorted(self._samples.items(), key=lambda kv: (kv[1]["event_ts"], kv[0][2]))
        for sk, sample in ordered[: len(self._samples) - keep]:
            del self._samples[sk]
            ta = self._by_type_asset.get((sample["event_type"], sample["asset"]))
            if ta is not None:
                ta.discard(sk)
                if not ta:
                    del self._by_type_asset[(sample["event_type"], sample["asset"])]
            ev = self._by_event.get(sk[0])
            if ev is not None:
                ev.discard(sk)
                if not ev:
                    del self._by_event[sk[0]]

    # ------------------------------------------------------------------ #
    # watching
    # ------------------------------------------------------------------ #
    def watch(self, event_key: str, event_type: str, event_ts: float, assets: list[str],
              surprise: int | None = None) -> bool:
        """Start measuring an event.  False when the key is already watched/measured
        (its surprise is still updated when given), the base moment has already
        passed by more than ``reaction_base_tolerance_s``, or the input is unusable."""
        key = str(event_key or "").strip()
        etype = str(event_type or "").strip()
        ets = _num(event_ts)
        names = [a for a in dict.fromkeys(_norm_asset(a) for a in (assets or [])) if a]
        if not key or not etype or ets is None or not names:
            return False
        if key in self._pending or key in self._done:
            if surprise is not None:
                self.set_surprise(key, surprise)
            return False
        now = self._clock.wall()
        if now > ets + self._base_tol():
            self.counters["rejected_late"] += 1
            log.debug("reactions: %s (%s) not watched: base moment passed %.1f s ago", key, etype, now - ets)
            return False
        if len(self._pending) >= max(1, int(self._cfg("reaction_max_pending", 200))):
            log.warning("reactions: too many pending events, %s not watched", key)
            return False
        w = _Watch(key=key, event_type=etype, event_ts=ets, assets=names, surprise=_norm_surprise(surprise),
                   created_ts=now)
        self._pending[key] = w
        self.counters["watched"] += 1
        self._capture(w, now)  # an event starting right now gets its base immediately
        return True

    def set_surprise(self, event_key: str, surprise: int | None) -> None:
        """Attach / correct the surprise sign of a pending or already measured event
        (measured samples are re-saved so the DB stays consistent)."""
        key = str(event_key or "").strip()
        s = _norm_surprise(surprise)
        w = self._pending.get(key)
        if w is not None:
            w.surprise = s
        for sk in sorted(self._by_event.get(key, ())):
            sample = self._samples.get(sk)
            if sample is not None and sample.get("surprise") != s:
                sample["surprise"] = s
                self._save(sample)

    # ------------------------------------------------------------------ #
    # capturing
    # ------------------------------------------------------------------ #
    def _safe_price(self, asset: str, cache: dict[str, float | None]) -> float | None:
        if asset not in cache:
            try:
                cache[asset] = _price(self._price_fn(asset))
            except Exception as e:  # noqa: BLE001
                log.debug("reactions: price_fn(%s) failed: %s", asset, e)
                cache[asset] = None
        return cache[asset]

    def tick(self, now_wall: float | None = None) -> int:
        """Capture due bases / horizons of every pending event; returns captures made."""
        now = self._clock.wall() if now_wall is None else float(now_wall)
        cache: dict[str, float | None] = {}
        n = 0
        for w in list(self._pending.values()):
            n += self._capture(w, now, cache)
            if self._complete(w, now):
                del self._pending[w.key]
                self._done.add(w.key)
        if n:
            self._trim()
        return n

    def _capture(self, w: _Watch, now: float, cache: dict[str, float | None] | None = None) -> int:
        cache = {} if cache is None else cache
        tol = self._base_tol()
        hs = self.horizons()
        n = 0
        for asset in w.assets:
            if asset in w.base_missed:
                continue
            base = w.base.get(asset)
            if now <= w.event_ts:
                if now >= w.event_ts - tol:
                    p = self._safe_price(asset, cache)
                    if p is not None:
                        if base is None:
                            n += 1
                        w.base[asset] = (p, now)  # the latest sample at/before event_ts wins
                continue
            if base is None:
                if now > w.event_ts + tol:
                    w.base_missed.add(asset)
                    self.counters["missed_base"] += 1
                    log.debug("reactions: %s %s base missed", w.key, asset)
                    continue
                p = self._safe_price(asset, cache)
                if p is None:
                    continue
                base = w.base[asset] = (p, now)
                n += 1
            captured = w.captured.setdefault(asset, {})
            missed = w.missed.setdefault(asset, set())
            for h in hs:
                if h in captured or h in missed:
                    continue
                target = w.event_ts + h
                if now < target:
                    break  # horizons ascend: the later ones are not due either
                if now > target + self._horizon_tol(h):
                    missed.add(h)
                    self.counters["missed_horizons"] += 1
                    continue
                if now <= base[1]:
                    continue  # never measure a reaction over zero elapsed time
                p = self._safe_price(asset, cache)
                if p is None:
                    continue  # retry on the next tick while still inside the tolerance
                move = (p / base[0] - 1.0) * 100.0
                captured[h] = move
                n += 1
                self._record(w, asset, h, move, p, now)
        self.counters["captures"] += n
        return n

    def _record(self, w: _Watch, asset: str, h: int, move: float, price: float, now: float) -> None:
        base_price, base_ts = w.base[asset]
        sample = {
            "event_key": w.key, "event_type": w.event_type, "event_ts": w.event_ts, "asset": asset,
            "horizon_s": int(h), "move_pct": move, "surprise": w.surprise,
            "base_price": base_price, "base_ts": base_ts, "price": price, "capture_ts": now,
            "elapsed_s": now - base_ts,
        }
        self._add_sample(sample)
        self.counters["samples"] += 1
        self._save(sample)

    def _complete(self, w: _Watch, now: float) -> bool:
        hs = self.horizons()
        if now > w.event_ts + hs[-1] + self._horizon_tol(hs[-1]) + 60.0:
            return True  # safety net: nothing can be captured any more
        if now <= w.event_ts:
            return False
        for asset in w.assets:
            if asset in w.base_missed:
                continue
            if asset not in w.base:
                return False
            done = w.captured.get(asset, {}).keys() | w.missed.get(asset, set())
            if any(h not in done for h in hs):
                return False
        return True

    # ------------------------------------------------------------------ #
    # read API
    # ------------------------------------------------------------------ #
    def profile(self, event_type: str, asset: str, surprise: int | None = None) -> dict[int, dict]:
        """horizon_s -> {"n", "mean_pct", "median_pct", "up_share", "mean_abs_pct"} over
        the recorded samples of this event type and asset (only horizons with data).
        With ``surprise`` given, only events with that surprise sign count."""
        keys = self._by_type_asset.get((str(event_type or "").strip(), _norm_asset(asset)))
        if not keys:
            return {}
        s = _norm_surprise(surprise) if surprise is not None else None
        by_h: dict[int, list[float]] = {}
        for sk in keys:
            sample = self._samples.get(sk)
            if sample is None:
                continue
            if surprise is not None and sample.get("surprise") != s:
                continue
            by_h.setdefault(int(sample["horizon_s"]), []).append(float(sample["move_pct"]))
        out: dict[int, dict] = {}
        for h in sorted(by_h):
            v = by_h[h]
            out[h] = {
                "n": len(v),
                "mean_pct": statistics.fmean(v),
                "median_pct": statistics.median(v),
                "up_share": sum(1 for x in v if x > 0) / len(v),
                "mean_abs_pct": statistics.fmean(abs(x) for x in v),
            }
        return out

    def event_types(self) -> list[str]:
        return sorted({t for t, _ in self._by_type_asset})

    def pending(self) -> list[dict[str, Any]]:
        out = []
        for w in sorted(self._pending.values(), key=lambda w: w.event_ts):
            out.append({
                "event_key": w.key, "event_type": w.event_type, "event_ts": w.event_ts, "assets": list(w.assets),
                "surprise": w.surprise,
                "base": {a: p for a, (p, _) in w.base.items()},
                "base_missed": sorted(w.base_missed),
                "captured": {a: dict(sorted(c.items())) for a, c in w.captured.items() if c},
                "missed": {a: sorted(m) for a, m in w.missed.items() if m},
            })
        return out

    def snapshot(self) -> dict[str, Any]:
        types: dict[str, dict[str, int]] = {}
        for (etype, asset), keys in sorted(self._by_type_asset.items()):
            events = {sk[0] for sk in keys}
            types.setdefault(etype, {})[asset] = len(events)
        recent = sorted(self._samples.values(), key=lambda s: (s["event_ts"], s["horizon_s"]))[-20:]
        return {
            "horizons_s": self.horizons(),
            "base_tolerance_s": self._base_tol(),
            "pending": self.pending(),
            "samples": len(self._samples),
            "events": len(self._by_event),
            "types": types,
            "recent": [dict(s) for s in reversed(recent)],
            "counters": dict(self.counters),
        }


__all__ = ["DEFAULT_HORIZONS_S", "EventReactionStore"]
