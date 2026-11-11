"""GET /api/context, /api/pool and the learning block of /api/shadow through the real app."""
from __future__ import annotations

import json

from fastapi.testclient import TestClient

from latbot.api.server import create_app
from latbot.context.hub import ContextHub
from test_app import build


def test_context_disabled_still_reports_the_pool(tmp_path):
    b = build(tmp_path)
    with TestClient(create_app(b.ctx)) as c:
        d = c.get("/api/context").json()
        assert d["enabled"] is False and "context_enabled" in d["reason"]
        assert d["pool"]["enabled"] is True and "BTC" in [r["asset"] for r in d["pool"]["included"]]
        p = c.get("/api/pool").json()
        assert p["size"] >= 2
        sh = c.get("/api/shadow").json()
        assert sh["learning"]["n"] == 0


def test_context_snapshot_through_the_api(tmp_path):
    b = build(tmp_path)
    b.ctx.context = ContextHub(b.ctx.settings, b.ctx.clock, pool=b.ctx.pool, metalabel=b.ctx.metalabel)
    with TestClient(create_app(b.ctx)) as c:
        d = c.get("/api/context").json()
    assert d["enabled"] is True and "macro" in d and "news" in d and "sources" in d and "weights" in d
    json.dumps(d)


def test_new_stats_period_through_the_api(tmp_path):
    b = build(tmp_path)
    with TestClient(create_app(b.ctx)) as c:
        before = c.get("/api/shadow").json()["summary"]
        assert before["period_start_ts"] is not None and before["all_time_closed"] == 0
        b.clock.advance(30.0)
        r = c.post("/api/stats/period").json()
        assert r["ok"] is True and r["period_start_ts"] == b.clock.wall()
        assert c.get("/api/shadow").json()["summary"]["period_start_ts"] == b.clock.wall()
