"""DIAG for Phase 3: pool size / assets outside the pool, quality pass rate and what blocks it."""
from __future__ import annotations

from latbot.clock import SimClock
from latbot.config import Settings
from latbot.engine.diagnostics import Diagnostics
from tests.test_pipeline import Env


def test_quality_counters_and_top_reasons():
    d = Diagnostics(Settings(), SimClock())
    d.quality(True, "")
    d.quality(False, "CPI через 90 с")
    d.quality(False, "CPI через 90 с")
    d.quality(False, "качество 52/100 < 60")
    snap = d.snapshot()
    f = snap["funnel"]
    assert f["quality_checked"] == 4 and f["quality_passed"] == 1 and f["quality_failed"] == 3
    assert snap["quality_fail_reasons"][0] == ["CPI через 90 с", 2]


def test_pool_info_and_why_lines():
    env = Env()

    class Pool:
        def contains(self, a):
            return a == "BTC"

        def assets(self):
            return ["BTC", "ETH"]

    env.pipeline.pool = Pool()
    env.pipeline.scanned("DOGE")
    env.pipeline.diag.quality(False, "CPI через 90 с")
    snap = env.pipeline.diagnostics()
    assert snap["pool"] == {"enabled": True, "size": 2, "assets": ["BTC", "ETH"], "skipped_n": 1, "skipped": ["DOGE"]}
    why = " ".join(snap["why"])
    assert "Пул" in why and "вне пула" in why
    assert "качеств" in why.lower()


def test_no_pool_reported_as_disabled():
    env = Env()
    snap = env.pipeline.diagnostics()
    assert snap["pool"]["enabled"] is False
