"""StatsStore - learned statistics per (asset, exchange).

Reaction delays, lag sizes, catch-up ratios, opportunity durations, trade
outcomes and the "would a human have made it" counters are recorded here and
fed back into detection / executability.  Lists are bounded by
``settings.stats_max_samples`` (oldest dropped).  Persistence is duck-typed:
``db.load_stats() -> list[ReactionStats]`` and ``db.save_stats(ReactionStats)``.
Strategy parameters are never fitted here - only measurements are kept.
"""
from __future__ import annotations

import logging
from typing import Any

from ..config import Settings
from ..models import ReactionStats, median, percentile

log = logging.getLogger(__name__)

Key = tuple[str, str]  # (asset, exchange)


_CATCHUP_FLAG = "catchup_unbiased_v2"
_CLEAN_BOOKS_FLAG = "catchup_clean_books_v3"  # samples measured before crossed books were rejected


class StatsStore:
    """In-memory store of ReactionStats with optional db persistence."""

    def __init__(self, settings: Settings, db: Any | None = None) -> None:
        self._settings = settings
        self._db = db
        self._stats: dict[Key, ReactionStats] = {}
        self._dirty: set[Key] = set()
        if db is not None and hasattr(db, "load_stats"):
            try:
                loaded = db.load_stats() or []
                for rs in loaded:
                    self._stats[(rs.asset, rs.exchange)] = rs
                log.info("stats loaded: %d (asset, exchange) rows", len(loaded))
                self._drop_biased_catchups()
            except Exception:  # noqa: BLE001
                log.exception("stats load failed; starting empty")

    def _drop_biased_catchups(self) -> None:
        """Catch-up samples recorded by older builds are dropped once per fix; everything else stays:
        v2 - only targets that DID follow were recorded (survivor bias); v3 - target moves were measured
        on crossed (broken) books, which moved the target's mid without a real trade."""
        kv_get, kv_set = getattr(self._db, "kv_get", None), getattr(self._db, "kv_set", None)
        if kv_get is None or kv_set is None:
            return
        missing = [flag for flag in (_CATCHUP_FLAG, _CLEAN_BOOKS_FLAG) if not kv_get(flag)]
        if not missing:
            return
        n = 0
        for rs in self._stats.values():
            if rs.catchup_ratios:
                n += len(rs.catchup_ratios)
                rs.catchup_ratios = []
                self._touch(rs)
        self.save()
        for flag in missing:
            kv_set(flag, True)
        log.info("stats: dropped %d catch-up samples of older builds (%s); re-learned from every episode now",
                 n, ", ".join(missing))

    # ------------------------------------------------------------------ #
    # access
    # ------------------------------------------------------------------ #
    def get(self, asset: str, exchange: str) -> ReactionStats:
        key = (asset, exchange)
        rs = self._stats.get(key)
        if rs is None:
            rs = ReactionStats(asset=asset, exchange=exchange)
            self._stats[key] = rs
        return rs

    def keys(self) -> list[Key]:
        return sorted(self._stats)

    def _touch(self, rs: ReactionStats) -> None:
        self._dirty.add((rs.asset, rs.exchange))

    def _append(self, lst: list[float], value: float) -> None:
        lst.append(float(value))
        max_n = max(1, int(self._settings.stats_max_samples))
        if len(lst) > max_n:
            del lst[: len(lst) - max_n]

    # ------------------------------------------------------------------ #
    # recording
    # ------------------------------------------------------------------ #
    def record_first_mover(self, asset: str, exchange: str) -> None:
        rs = self.get(asset, exchange)
        rs.first_mover_count += 1
        self._touch(rs)

    def record_lagging(self, asset: str, exchange: str, lag_pct: float) -> None:
        rs = self.get(asset, exchange)
        rs.lagging_count += 1
        self._append(rs.lag_sizes_pct, lag_pct)
        self._touch(rs)

    def record_reaction(self, asset: str, exchange: str, delay_s: float, catchup_ratio: float) -> None:
        rs = self.get(asset, exchange)
        self._append(rs.reaction_delays_s, max(0.0, delay_s))
        self._append(rs.catchup_ratios, catchup_ratio)
        self._touch(rs)

    def record_catchup(self, asset: str, exchange: str, catchup_ratio: float) -> None:
        """Catch-up of a lagging target that did not cross the move threshold before the episode
        ended (no reaction delay to record) - usually ~0."""
        rs = self.get(asset, exchange)
        self._append(rs.catchup_ratios, max(0.0, catchup_ratio))
        self._touch(rs)

    def record_false_signal(self, asset: str, exchange: str) -> None:
        rs = self.get(asset, exchange)
        rs.false_signal_count += 1
        self._touch(rs)

    def record_opportunity_duration(self, asset: str, exchange: str, duration_s: float) -> None:
        rs = self.get(asset, exchange)
        self._append(rs.opp_durations_s, max(0.0, duration_s))
        self._touch(rs)

    def record_trade(
        self,
        asset: str,
        exchange: str,
        success: bool,
        realized_pct: float | None,
        slippage_pct: float | None,
        exec_latency_ms: float | None,
    ) -> None:
        rs = self.get(asset, exchange)
        if success:
            rs.success_count += 1
        else:
            rs.fail_count += 1
        if realized_pct is not None:
            self._append(rs.realized_returns_pct, realized_pct)
        if slippage_pct is not None:
            self._append(rs.slippages_pct, slippage_pct)
        if exec_latency_ms is not None:
            self._append(rs.exec_latencies_ms, exec_latency_ms)
        self._touch(rs)

    def record_human_outcome(self, asset: str, exchange: str, window_ok: bool, caught_up: bool) -> None:
        """Phase 2.4: outcome of one finished opportunity for the human-trading
        statistics.  ``window_ok`` is "a human would have made it" (window
        lasted >= human_execution_time_s AND the target caught up);
        ``caught_up`` is tracked separately."""
        rs = self.get(asset, exchange)
        rs.human_window_total += 1
        if window_ok:
            rs.human_window_ok += 1
        rs.caught_up_total += 1
        if caught_up:
            rs.caught_up_ok += 1
        self._touch(rs)

    def record_manual_result(self, asset: str, exchange: str, in_window: bool, win: bool) -> None:
        """One finished MANUAL trade of the owner: did the entry happen while the
        window was open, and did it end with a profit.  Feeds assess_human."""
        rs = self.get(asset, exchange)
        rs.manual_total += 1
        if in_window:
            rs.manual_in_window += 1
        if win:
            rs.manual_wins += 1
        self._touch(rs)

    # ------------------------------------------------------------------ #
    # derived values
    # ------------------------------------------------------------------ #
    def _existing(self, asset: str, exchange: str) -> ReactionStats | None:
        return self._stats.get((asset, exchange))

    def median_reaction_delay(self, asset: str, exchange: str) -> float | None:
        rs = self._existing(asset, exchange)
        return median(rs.reaction_delays_s) if rs else None

    def reaction_percentile(self, asset: str, exchange: str, p: float) -> float | None:
        rs = self._existing(asset, exchange)
        return percentile(rs.reaction_delays_s, p) if rs else None

    def median_duration(self, asset: str, exchange: str) -> float | None:
        rs = self._existing(asset, exchange)
        return median(rs.opp_durations_s) if rs else None

    def catchup_ratio(self, asset: str, exchange: str) -> float | None:
        """Median learned catch-up ratio; None until 3 samples exist."""
        rs = self._existing(asset, exchange)
        if rs is None or len(rs.catchup_ratios) < 3:
            return None
        return median(rs.catchup_ratios)

    def first_mover_frequency(self, asset: str, exchange: str) -> float | None:
        rs = self._existing(asset, exchange)
        if rs is None:
            return None
        n = rs.first_mover_count + rs.lagging_count
        return None if n == 0 else rs.first_mover_count / n

    def summary(self) -> list[dict[str, Any]]:
        """One row per (asset, exchange) for the UI / API."""
        rows: list[dict[str, Any]] = []
        for key in sorted(self._stats):
            rs = self._stats[key]
            rows.append(
                {
                    "asset": rs.asset,
                    "exchange": rs.exchange,
                    "first_mover_count": rs.first_mover_count,
                    "lagging_count": rs.lagging_count,
                    "first_mover_frequency": self.first_mover_frequency(rs.asset, rs.exchange),
                    "reaction_samples": len(rs.reaction_delays_s),
                    "median_reaction_delay_s": median(rs.reaction_delays_s),
                    "p90_reaction_delay_s": percentile(rs.reaction_delays_s, 90),
                    "median_lag_pct": median(rs.lag_sizes_pct),
                    "catchup_ratio": self.catchup_ratio(rs.asset, rs.exchange),
                    "median_catchup_ratio": median(rs.catchup_ratios),
                    "median_duration_s": median(rs.opp_durations_s),
                    "success_count": rs.success_count,
                    "fail_count": rs.fail_count,
                    "success_rate": rs.success_rate,
                    "false_signal_count": rs.false_signal_count,
                    "median_realized_pct": median(rs.realized_returns_pct),
                    "median_slippage_pct": median(rs.slippages_pct),
                    "median_exec_latency_ms": median(rs.exec_latencies_ms),
                    "human_window_total": rs.human_window_total,
                    "human_window_ok": rs.human_window_ok,
                    "human_success_rate": rs.human_success_rate,
                    "caught_up_total": rs.caught_up_total,
                    "caught_up_ok": rs.caught_up_ok,
                    "catchup_rate": rs.catchup_rate,
                }
            )
        return rows

    # ------------------------------------------------------------------ #
    # persistence
    # ------------------------------------------------------------------ #
    def save(self) -> None:
        """Persist dirty rows via ``db.save_stats``; no-op without a db."""
        if self._db is None or not hasattr(self._db, "save_stats"):
            self._dirty.clear()
            return
        for key in sorted(self._dirty):
            rs = self._stats.get(key)
            if rs is None:
                continue
            try:
                self._db.save_stats(rs)
            except Exception:  # noqa: BLE001
                log.exception("stats save failed for %s/%s", key[0], key[1])
                return  # keep dirty, retry next time
        self._dirty.clear()
