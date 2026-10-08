"""Phase 3 persistence: news, event reactions, derivative samples, meta-label outcomes."""
from __future__ import annotations

from latbot.clock import SimClock
from latbot.storage.db import Database


def _db() -> Database:
    return Database(":memory:", clock=SimClock())


def test_news_round_trip_newest_first_and_upsert():
    db = _db()
    db.save_news({"id": "a", "ts": 100.0, "title": "old", "category": "etf"})
    db.save_news({"id": "b", "ts": 200.0, "title": "new", "category": "hack"})
    db.save_news({"id": "a", "ts": 100.0, "title": "old (reclassified)", "category": "etf", "importance": 0.9})
    rows = db.list_news()
    assert [r["id"] for r in rows] == ["b", "a"]
    assert rows[1]["title"] == "old (reclassified)" and rows[1]["importance"] == 0.9
    assert [r["id"] for r in db.list_news(since_ts=150.0)] == ["b"]
    assert len(db.list_news(limit=1)) == 1


def test_event_reactions_round_trip_oldest_first():
    db = _db()
    db.save_event_reaction({"event_key": "cpi-1", "event_type": "CPI", "asset": "BTC", "event_ts": 10.0, "horizon_s": 60, "move_pct": 0.4})
    db.save_event_reaction({"event_key": "cpi-1", "event_type": "CPI", "asset": "BTC", "event_ts": 10.0, "horizon_s": 10, "move_pct": 0.1})
    db.save_event_reaction({"event_key": "nfp-1", "event_type": "NFP", "asset": "ETH", "event_ts": 5.0, "horizon_s": 60, "move_pct": -0.2})
    # the same (event_key, asset, horizon) twice is one sample
    db.save_event_reaction({"event_key": "cpi-1", "event_type": "CPI", "asset": "BTC", "event_ts": 10.0, "horizon_s": 60, "move_pct": 0.5})
    rows = db.load_event_reactions()
    assert len(rows) == 3
    assert rows[0]["event_key"] == "nfp-1"
    cpi60 = [r for r in rows if r["event_key"] == "cpi-1" and r["horizon_s"] == 60]
    assert cpi60[0]["move_pct"] == 0.5
    assert len(db.load_event_reactions(limit=1)) == 1


def test_deriv_samples_by_asset_and_time():
    db = _db()
    for ts in (100.0, 200.0, 300.0):
        db.save_deriv_sample({"asset": "BTC", "venue": "okx", "ts": ts, "funding_rate_8h": 0.0001 * ts})
    db.save_deriv_sample({"asset": "ETH", "venue": "okx", "ts": 250.0, "funding_rate_8h": 0.0002})
    rows = db.load_deriv_samples("BTC", since_ts=150.0)
    assert [r["ts"] for r in rows] == [200.0, 300.0]
    assert db.load_deriv_samples("SOL", since_ts=0.0) == []


def test_metalabel_outcomes_round_trip():
    db = _db()
    db.save_metalabel_outcome({"id": "s1", "asset": "BTC", "outcome": "win", "ts": 1.0})
    db.save_metalabel_outcome({"id": "s2", "asset": "ETH", "outcome": "loss", "ts": 2.0})
    db.save_metalabel_outcome({"asset": "SOL", "outcome": "win", "ts": 3.0})  # no id -> still stored
    rows = db.load_metalabel_outcomes()
    assert [r["asset"] for r in rows] == ["BTC", "ETH", "SOL"]
    assert len(db.load_metalabel_outcomes(limit=2)) == 2
    assert [r["asset"] for r in db.load_metalabel_outcomes(limit=2)] == ["ETH", "SOL"]  # the most recent ones, oldest first


def test_phase3_rows_survive_reopen(tmp_path):
    path = str(tmp_path / "x.sqlite3")
    db = Database(path, clock=SimClock())
    db.save_news({"id": "n", "ts": 1.0, "title": "t"})
    db.save_metalabel_outcome({"id": "m", "asset": "BTC", "outcome": "win", "ts": 1.0})
    db.close()
    db2 = Database(path, clock=SimClock())
    assert db2.list_news()[0]["id"] == "n"
    assert db2.load_metalabel_outcomes()[0]["id"] == "m"
    db2.close()
