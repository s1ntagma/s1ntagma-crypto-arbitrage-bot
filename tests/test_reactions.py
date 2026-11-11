"""Offline tests for latbot.context.reactions.EventReactionStore."""
from __future__ import annotations

import statistics
from types import SimpleNamespace

import pytest

from latbot.clock import SimClock
from latbot.context.reactions import EventReactionStore

T = 1_791_450_000.0  # event time (wall)


class Prices:
    def __init__(self) -> None:
        self.p: dict[str, float | None] = {}
        self.calls = 0

    def __call__(self, asset: str) -> float | None:
        self.calls += 1
        return self.p.get(asset)


class FakeDb:
    """Same contract as storage.db: one row per (event_key, asset, horizon_s), upsert, oldest first."""

    def __init__(self) -> None:
        self.rows: dict[tuple, dict] = {}
        self.saves = 0

    def save_event_reaction(self, row: dict) -> None:
        self.saves += 1
        self.rows[(row["event_key"], row["asset"], int(row["horizon_s"]))] = dict(row)

    def load_event_reactions(self, limit: int = 20000) -> list[dict]:
        rows = sorted(self.rows.values(), key=lambda r: (r["event_ts"], r["horizon_s"]))
        return [dict(r) for r in rows[-limit:]]


def _store(prices: Prices, clock: SimClock, db=None, **kw) -> EventReactionStore:
    return EventReactionStore(SimpleNamespace(**kw), clock, prices, db)


def _run(store: EventReactionStore, clock: SimClock, prices: Prices, path, start: float, end: float, step: float = 5.0) -> int:
    """Tick every ``step`` seconds; ``path(t_rel)`` sets prices (t_rel = seconds since T)."""
    n, t = 0, start
    while t <= end + 1e-9:
        clock.set(wall=T + t)
        path(t)
        n += store.tick()
        t += step
    return n


def _linear(prices: Prices, asset: str = "BTC", base: float = 100.0, slope_pct_per_s: float = 0.001):
    def path(t: float) -> None:
        prices.p[asset] = base * (1 + slope_pct_per_s / 100.0 * max(0.0, t))
    return path


def test_base_and_all_horizons_are_captured():
    clock, prices, db = SimClock(wall=T - 60), Prices(), FakeDb()
    store = _store(prices, clock, db)
    assert store.watch("cpi-2026-10", "CPI", T, ["btc"], surprise=1) is True
    n = _run(store, clock, prices, _linear(prices), -30, 2000)
    assert n == 1 + 4  # base + 10 s / 60 s / 5 min / 30 min
    prof = store.profile("CPI", "BTC")
    assert sorted(prof) == [10, 60, 300, 1800]
    for h in (10, 60, 300, 1800):
        assert prof[h]["n"] == 1
        assert prof[h]["mean_pct"] == pytest.approx(0.001 * h, rel=1e-6)  # base exactly at T, ticks hit T+h
        assert prof[h]["up_share"] == 1.0
    assert len(db.rows) == 4
    row = db.rows[("cpi-2026-10", "BTC", 60)]
    assert row["event_type"] == "CPI" and row["surprise"] == 1 and row["base_ts"] == T
    assert row["capture_ts"] == T + 60 and row["move_pct"] == pytest.approx(0.06)
    assert store.snapshot()["pending"] == []  # finished events leave the pending list


def test_base_is_the_last_sample_at_or_before_the_event():
    clock, prices = SimClock(wall=T - 60), Prices()
    store = _store(prices, clock)
    store.watch("e1", "FOMC_RATE", T, ["BTC"])
    for t, p in ((-4, 100.0), (-2, 101.0), (0, 102.0)):
        clock.set(wall=T + t)
        prices.p["BTC"] = p
        store.tick()
    clock.set(wall=T + 10)
    prices.p["BTC"] = 104.04
    store.tick()
    assert store.profile("FOMC_RATE", "BTC")[10]["mean_pct"] == pytest.approx(2.0)


def test_base_after_the_event_within_tolerance_and_outside_is_missed():
    clock, prices = SimClock(wall=T - 60), Prices()
    store = _store(prices, clock)
    store.watch("late-ok", "NFP", T, ["BTC"])
    store.watch("late-miss", "PPI", T, ["ETH"])
    clock.set(wall=T + 3)
    prices.p.update(BTC=100.0, ETH=None)  # ETH has no fresh price in the base window
    store.tick()
    clock.set(wall=T + 6)  # 6 s > 5 s tolerance: the ETH base is gone for good
    prices.p["ETH"] = 3000.0
    store.tick()
    clock.set(wall=T + 12)
    prices.p.update(BTC=101.0, ETH=3030.0)
    store.tick()
    assert store.profile("NFP", "BTC")[10]["mean_pct"] == pytest.approx(1.0)  # measured over 9 s from base
    assert store.profile("PPI", "ETH") == {}
    assert store.counters["missed_base"] == 1
    _run(store, clock, prices, lambda t: None, 20, 2100, step=60)
    assert store.profile("PPI", "ETH") == {}  # never interpolated afterwards


def test_missed_horizon_is_skipped_and_a_retry_inside_tolerance_counts():
    clock, prices = SimClock(wall=T - 10), Prices()
    store = _store(prices, clock)
    store.watch("ev", "CPI", T, ["BTC"])

    def path(t: float) -> None:
        if 55 <= t <= 70:
            prices.p["BTC"] = None  # stale around +60 s (tolerance 6 s) -> missed
        elif 295 <= t < 310:
            prices.p["BTC"] = None  # stale at +300 s, back at +310 (tolerance 30 s) -> captured late
        else:
            prices.p["BTC"] = 100.0 + max(0.0, t) / 100.0

    _run(store, clock, prices, path, -5, 2000)
    prof = store.profile("CPI", "BTC")
    assert sorted(prof) == [10, 300, 1800]
    assert prof[300]["mean_pct"] == pytest.approx(3.1)  # measured when captured (+310 s), not interpolated
    assert store.counters["missed_horizons"] == 1


def test_watch_rejects_late_duplicate_and_empty_events():
    clock, prices = SimClock(wall=T + 30), Prices()
    store = _store(prices, clock)
    assert store.watch("old", "CPI", T, ["BTC"]) is False  # base moment 30 s ago
    assert store.counters["rejected_late"] == 1
    assert store.watch("x", "CPI", T + 100, []) is False
    assert store.watch("", "CPI", T + 100, ["BTC"]) is False
    assert store.watch("y", "", T + 100, ["BTC"]) is False
    assert store.watch("future", "news:hack", T + 3600, ["BTC", "ETH", "btc"]) is True
    assert store.watch("future", "news:hack", T + 3600, ["BTC"]) is False
    pend = store.snapshot()["pending"]
    assert len(pend) == 1 and pend[0]["assets"] == ["BTC", "ETH"] and pend[0]["base"] == {}


def test_watch_at_the_event_captures_the_base_immediately():
    clock, prices = SimClock(wall=T + 2), Prices()
    prices.p["BTC"] = 50.0
    store = _store(prices, clock)
    assert store.watch("news-1", "news:etf", T, ["BTC"]) is True
    assert store.pending()[0]["base"] == {"BTC": 50.0}
    assert store.counters["captures"] == 1


def test_profile_statistics_and_surprise_filter():
    clock, prices = SimClock(wall=T - 60), Prices()
    store = _store(prices, clock, reaction_horizons_s=[60])
    moves = [(0.5, 1), (0.3, 1), (-0.2, 1), (-0.4, -1), (-0.6, -1), (0.1, None)]
    for i, (move, surprise) in enumerate(moves):
        ets = T + i * 1000
        clock.set(wall=ets - 1)
        prices.p["BTC"] = 100.0
        assert store.watch(f"cpi-{i}", "CPI", ets, ["BTC"], surprise=surprise)
        store.tick()
        clock.set(wall=ets + 60)
        prices.p["BTC"] = 100.0 * (1 + move / 100)
        store.tick()
    all_ = store.profile("CPI", "BTC")[60]
    vals = [m for m, _ in moves]
    assert all_["n"] == 6
    assert all_["mean_pct"] == pytest.approx(statistics.fmean(vals))
    assert all_["median_pct"] == pytest.approx(statistics.median(vals))
    assert all_["up_share"] == pytest.approx(3 / 6)
    assert all_["mean_abs_pct"] == pytest.approx(statistics.fmean(abs(v) for v in vals))
    hot = store.profile("CPI", "BTC", surprise=1)[60]
    assert hot["n"] == 3 and hot["up_share"] == pytest.approx(2 / 3)
    cold = store.profile("CPI", "BTC", surprise=-1)[60]
    assert cold["n"] == 2 and cold["up_share"] == 0.0 and cold["mean_pct"] == pytest.approx(-0.5)
    assert store.profile("CPI", "BTC", surprise=0) == {}
    assert store.profile("CPI", "ETH") == {} and store.profile("NFP", "BTC") == {}
    assert store.snapshot()["types"] == {"CPI": {"BTC": 6}}


def test_set_surprise_updates_pending_and_recorded_samples():
    clock, prices, db = SimClock(wall=T - 1), Prices(), FakeDb()
    store = _store(prices, clock, db)
    prices.p["BTC"] = 100.0
    store.watch("cpi", "CPI", T, ["BTC"])
    store.tick()
    clock.set(wall=T + 10)
    prices.p["BTC"] = 99.0
    store.tick()
    assert db.rows[("cpi", "BTC", 10)]["surprise"] is None
    store.set_surprise("cpi", 1)  # learned later (squawk / BLS)
    assert db.rows[("cpi", "BTC", 10)]["surprise"] == 1
    assert store.pending()[0]["surprise"] == 1
    assert store.profile("CPI", "BTC", surprise=1)[10]["n"] == 1
    clock.set(wall=T + 60)
    store.tick()
    assert db.rows[("cpi", "BTC", 60)]["surprise"] == 1  # later horizons carry it too
    store.watch("cpi", "CPI", T, ["BTC"], surprise=-1)  # duplicate watch still corrects the sign
    assert {r["surprise"] for r in db.rows.values()} == {-1}
    saves = db.saves
    store.set_surprise("cpi", -1)
    assert db.saves == saves  # unchanged -> not re-saved


def test_history_is_loaded_from_the_db_at_construction():
    clock, prices, db = SimClock(wall=T - 60), Prices(), FakeDb()
    store = _store(prices, clock, db)
    store.watch("nfp-1", "NFP", T, ["BTC", "ETH"], surprise=-1)
    _run(store, clock, prices, lambda t: prices.p.update(BTC=100.0 + max(0.0, t) / 100, ETH=10.0), -10, 2000)
    assert len(db.rows) == 8
    db.rows[("junk", "BTC", 10)] = {"event_key": "junk", "asset": "BTC", "horizon_s": 10, "event_ts": T}  # no move
    again = _store(Prices(), SimClock(wall=T + 5000), db)
    assert again.counters["loaded"] == 8
    assert again.profile("NFP", "BTC") == store.profile("NFP", "BTC")
    assert again.profile("NFP", "ETH", surprise=-1)[1800]["mean_pct"] == pytest.approx(0.0)
    assert again.watch("nfp-1", "NFP", T + 4999, ["BTC"]) is False  # already measured


def test_db_failures_do_not_stop_measuring():
    class BrokenDb:
        def load_event_reactions(self, limit):
            raise RuntimeError("locked")

        def save_event_reaction(self, row):
            raise RuntimeError("disk full")

    clock, prices = SimClock(wall=T - 1), Prices()
    store = _store(prices, clock, BrokenDb())
    prices.p["BTC"] = 1.0
    store.watch("e", "CPI", T, ["BTC"])
    store.tick()
    clock.set(wall=T + 10)
    prices.p["BTC"] = 1.01
    assert store.tick() == 1
    assert store.profile("CPI", "BTC")[10]["n"] == 1 and store.counters["save_errors"] == 1


def test_price_fn_errors_are_missing_prices():
    clock = SimClock(wall=T)

    def boom(asset):
        raise RuntimeError("no books")

    store = EventReactionStore(SimpleNamespace(), clock, boom)
    assert store.watch("e", "CPI", T, ["BTC"]) is True
    clock.set(wall=T + 6)
    assert store.tick() == 0
    assert store.counters["missed_base"] == 1


def test_keep_limit_trims_oldest_samples_and_snapshot_shape():
    clock, prices = SimClock(wall=T - 1), Prices()
    store = _store(prices, clock, reaction_horizons_s=[10], reaction_keep=3)
    for i in range(5):
        ets = T + i * 100
        clock.set(wall=ets)
        prices.p["BTC"] = 100.0
        store.watch(f"e{i}", "news:hack", ets, ["BTC"])
        clock.set(wall=ets + 10)
        prices.p["BTC"] = 100.0 + i
        store.tick()
    assert store.profile("news:hack", "BTC")[10]["n"] == 3
    snap = store.snapshot()
    assert snap["samples"] == 3 and snap["horizons_s"] == [10] and snap["base_tolerance_s"] == 5.0
    assert [r["event_key"] for r in snap["recent"]] == ["e4", "e3", "e2"]
    assert store.event_types() == ["news:hack"]


def test_default_horizons_and_bad_settings_fall_back():
    store = EventReactionStore(SimpleNamespace(reaction_horizons_s="junk", reaction_base_tolerance_s="x"),
                               SimClock(), lambda a: None)
    assert store.horizons() == [10, 60, 300, 1800]
    assert store.snapshot()["base_tolerance_s"] == 5.0
    assert EventReactionStore(SimpleNamespace(reaction_horizons_s=[300, 0, 60, 60]), SimClock(),
                              lambda a: None).horizons() == [60, 300]
