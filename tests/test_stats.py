"""Offline tests for latbot.engine.stats.StatsStore."""
from __future__ import annotations

import pytest

from latbot.config import Settings
from latbot.engine.stats import StatsStore
from latbot.models import ReactionStats


class StubDb:
    def __init__(self, rows: list[ReactionStats] | None = None) -> None:
        self.rows = rows or []
        self.saved: list[ReactionStats] = []

    def load_stats(self) -> list[ReactionStats]:
        return list(self.rows)

    def save_stats(self, rs: ReactionStats) -> None:
        self.saved.append(rs)


def test_get_creates_and_records_cap():
    st = StatsStore(Settings(stats_max_samples=3))
    rs = st.get("BTC", "bybit")
    assert rs.asset == "BTC" and rs.exchange == "bybit"
    assert st.get("BTC", "bybit") is rs
    for d in (1.0, 2.0, 3.0, 4.0, 5.0):
        st.record_reaction("BTC", "bybit", d, 0.5)
    assert rs.reaction_delays_s == [3.0, 4.0, 5.0]
    assert rs.catchup_ratios == [0.5, 0.5, 0.5]


def test_medians_percentiles_and_ratios():
    st = StatsStore(Settings())
    assert st.median_reaction_delay("BTC", "bybit") is None
    assert st.reaction_percentile("BTC", "bybit", 90) is None
    assert st.catchup_ratio("BTC", "bybit") is None
    assert st.first_mover_frequency("BTC", "bybit") is None
    st.record_reaction("BTC", "bybit", 3.0, 0.4)
    st.record_reaction("BTC", "bybit", 5.0, 0.8)
    assert st.median_reaction_delay("BTC", "bybit") == 4.0
    assert st.catchup_ratio("BTC", "bybit") is None  # needs 3 samples
    st.record_reaction("BTC", "bybit", 1.0, 0.6)
    assert st.catchup_ratio("BTC", "bybit") == 0.6
    assert st.median_reaction_delay("BTC", "bybit") == 3.0
    assert st.reaction_percentile("BTC", "bybit", 100) == 5.0
    assert st.reaction_percentile("BTC", "bybit", 0) == 1.0
    st.record_first_mover("BTC", "bybit")
    st.record_lagging("BTC", "bybit", 1.2)
    st.record_lagging("BTC", "bybit", 0.8)
    assert st.first_mover_frequency("BTC", "bybit") == pytest.approx(1 / 3)
    assert st.get("BTC", "bybit").lag_sizes_pct == [1.2, 0.8]
    st.record_opportunity_duration("BTC", "bybit", 9.0)
    st.record_opportunity_duration("BTC", "bybit", 11.0)
    assert st.median_duration("BTC", "bybit") == 10.0
    assert st.median_duration("ETH", "bybit") is None
    st.record_false_signal("BTC", "bybit")
    assert st.get("BTC", "bybit").false_signal_count == 1


def test_record_trade_and_human_outcome():
    st = StatsStore(Settings())
    st.record_trade("BTC", "bybit", True, 1.5, 0.1, 300.0)
    st.record_trade("BTC", "bybit", False, None, None, None)
    rs = st.get("BTC", "bybit")
    assert rs.success_count == 1 and rs.fail_count == 1
    assert rs.realized_returns_pct == [1.5] and rs.slippages_pct == [0.1] and rs.exec_latencies_ms == [300.0]
    assert rs.success_rate == 0.5
    st.record_human_outcome("BTC", "bybit", True, True)
    st.record_human_outcome("BTC", "bybit", False, True)
    st.record_human_outcome("BTC", "bybit", False, False)
    assert rs.human_window_total == 3 and rs.human_window_ok == 1
    assert rs.caught_up_total == 3 and rs.caught_up_ok == 2
    assert rs.human_success_rate == pytest.approx(1 / 3)


def test_summary_rows():
    st = StatsStore(Settings())
    st.record_reaction("ETH", "kraken", 2.0, 0.5)
    st.record_first_mover("BTC", "binance")
    rows = st.summary()
    assert [(r["asset"], r["exchange"]) for r in rows] == [("BTC", "binance"), ("ETH", "kraken")]
    eth = rows[1]
    assert eth["median_reaction_delay_s"] == 2.0
    assert eth["reaction_samples"] == 1
    assert eth["catchup_ratio"] is None
    assert eth["success_rate"] is None
    assert "false_signal_count" in eth and "human_success_rate" in eth
    assert rows[0]["first_mover_frequency"] == 1.0


def test_save_and_load_with_stub_db():
    db = StubDb([ReactionStats(asset="BTC", exchange="bybit", reaction_delays_s=[2.0, 4.0])])
    st = StatsStore(Settings(), db)
    assert st.median_reaction_delay("BTC", "bybit") == 3.0
    st.save()
    assert db.saved == []  # nothing dirty yet
    st.record_first_mover("BTC", "bybit")
    st.record_lagging("ETH", "kraken", 0.5)
    st.save()
    assert {(r.asset, r.exchange) for r in db.saved} == {("BTC", "bybit"), ("ETH", "kraken")}
    db.saved.clear()
    st.save()
    assert db.saved == []
    # without db: no-op
    StatsStore(Settings()).save()


def test_load_failure_is_tolerated():
    class BadDb:
        def load_stats(self):
            raise RuntimeError("boom")

        def save_stats(self, rs):
            raise RuntimeError("boom")

    st = StatsStore(Settings(), BadDb())
    st.record_first_mover("BTC", "bybit")
    st.save()  # must not raise
    assert st.get("BTC", "bybit").first_mover_count == 1


def test_biased_catchup_ratios_are_dropped_once():
    from latbot.engine.stats import StatsStore
    from latbot.models import ReactionStats
    from latbot.config import Settings
    from latbot.storage.db import Database

    db = Database(":memory:")
    rs = ReactionStats(asset="BTC", exchange="kraken")
    rs.catchup_ratios = [1.0, 0.9, 1.2]  # collected from successes only (old builds)
    rs.reaction_delays_s = [2.0, 3.0, 4.0]
    db.save_stats(rs)
    db.kv_set("catchup_clean_books_v3", True)  # only the v2 (survivor bias) cleanup is pending here
    st = StatsStore(Settings(), db)
    assert st.get("BTC", "kraken").catchup_ratios == [] and st.get("BTC", "kraken").reaction_delays_s == [2.0, 3.0, 4.0]
    st.record_catchup("BTC", "kraken", 0.0)
    st.save()
    assert StatsStore(Settings(), db).get("BTC", "kraken").catchup_ratios == [0.0]  # once, not on every start
