"""Trade journal: notes, screenshot upload, Claude review (fake client) - end to end
through the real app wiring (build_context + TestClient)."""
from __future__ import annotations

import io
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from latbot import analysis
from latbot.api.server import create_app
from latbot.config import Secrets
from latbot.storage.db import Database
from test_app import build, detect

PNG = (b"\x89PNG\r\n\x1a\n" + b"\x00" * 64)


class FakeClaude:
    """Stands in for anthropic.AsyncAnthropic: records the request, returns text."""

    def __init__(self, text: str = "1) На скриншоте видно ... 2) NET потерялся на ...", stop_reason: str = "end_turn") -> None:
        self.text = text
        self.stop_reason = stop_reason
        self.calls: list[dict] = []
        self.messages = self

    async def create(self, **kw):
        self.calls.append(kw)
        return SimpleNamespace(stop_reason=self.stop_reason, content=[SimpleNamespace(type="text", text=self.text)])


async def _trade(tmp_path: Path):
    b = build(tmp_path, db_path=str(tmp_path / "data" / "latbot.sqlite3"))
    opp = await detect(b)
    await b.ctx.execution.select_manual(opp.id)
    entered = await b.ctx.execution.manual_entered(opp.id, price=100.01, quote_amount=100.0)
    assert entered["ok"] is True
    t = b.ctx.execution.trade(entered["trade_id"])  # open MANUAL position: a real TradeRecord
    assert t is not None
    return b, t


def test_db_journal_roundtrip():
    db = Database(":memory:")
    assert db.get_trade_note("t1") == "" and db.journal_summary() == {}
    db.set_trade_note("t1", "вошёл поздно")
    sid = db.add_screenshot("t1", "/x/a.png", "image/png")
    assert db.get_trade_note("t1") == "вошёл поздно" and db.list_screenshots("t1")[0]["id"] == sid
    db.set_screenshot_analysis(sid, "разбор", "claude-opus-5-5")
    assert db.get_screenshot(sid)["analysis"] == "разбор"
    assert db.journal_summary() == {"t1": {"note": "вошёл поздно", "screenshots": 1, "analyzed": 1}}
    db.set_trade_note("t1", "   ")
    assert db.get_trade_note("t1") == "" and db.journal_summary()["t1"]["note"] == ""


async def test_journal_api_without_key_is_honest_not_fake(tmp_path: Path):
    b, t = await _trade(tmp_path)
    with TestClient(create_app(b.ctx)) as c:
        assert c.get("/api/trades/nope/journal").status_code == 404
        j = c.get(f"/api/trades/{t.id}/journal").json()
        assert j["note"] == "" and j["screenshots"] == [] and j["analysis_available"] is False
        r = c.post(f"/api/trades/{t.id}/note", json={"note": "продал по рынку рано"})
        assert r.status_code == 200 and r.json()["note"] == "продал по рынку рано"
        r = c.post(f"/api/trades/{t.id}/screenshot", files={"file": ("shot.png", io.BytesIO(PNG), "image/png")})
        assert r.status_code == 200, r.text
        shot = r.json()
        assert shot["trade_id"] == t.id and shot["analysis"] is None
        assert Path(shot["url"].replace("/api/screenshots/", "")).name.isdigit()
        assert c.get(shot["url"]).content == PNG
        assert (tmp_path / "data" / "screenshots").is_dir()
        bad = c.post(f"/api/trades/{t.id}/screenshot", files={"file": ("x.txt", io.BytesIO(b"hi"), "text/plain")})
        assert bad.status_code == 400
        r = c.post(f"/api/screenshots/{shot['id']}/analyze")
        assert r.status_code == 409 and "ANTHROPIC_API_KEY" in r.json()["error"]
        rows = c.get("/api/trades").json()
        mine = next(x for x in rows if x["id"] == t.id)
        assert mine["journal"] == {"note": "продал по рынку рано", "screenshots": 1, "analyzed": 0}


async def test_screenshot_analysis_with_fake_claude(tmp_path: Path):
    b, t = await _trade(tmp_path)
    b.ctx.secrets = Secrets(anthropic_api_key="sk-ant-test-key-0000000000")
    fake = FakeClaude()
    b.ctx.analysis_client = fake
    with TestClient(create_app(b.ctx)) as c:
        c.post(f"/api/trades/{t.id}/note", json={"note": "заметка"})
        shot = c.post(f"/api/trades/{t.id}/screenshot", files={"file": ("s.png", io.BytesIO(PNG), "image/png")}).json()
        r = c.post(f"/api/screenshots/{shot['id']}/analyze")
        assert r.status_code == 200, r.text
        assert r.json()["analysis"].startswith("1) На скриншоте") and r.json()["analysis_model"] == b.ctx.settings.analysis_model
        j = c.get(f"/api/trades/{t.id}/journal").json()
        assert j["analysis_available"] is True and j["screenshots"][0]["analysis"].startswith("1)")
    call = fake.calls[0]
    assert call["model"] == b.ctx.settings.analysis_model and call["system"] == analysis.SYSTEM_PROMPT
    blocks = call["messages"][0]["content"]
    assert blocks[0]["type"] == "image" and blocks[0]["source"]["media_type"] == "image/png"
    assert t.id in blocks[1]["text"] and "заметка" in blocks[1]["text"]
    assert "sk-ant" not in repr(call)


async def test_analysis_errors_never_leak_the_key():
    secrets = Secrets(anthropic_api_key="sk-ant-secret-1234567890")

    class Boom:
        messages = property(lambda self: self)

        async def create(self, **kw):
            raise ValueError("bad key sk-ant-secret-1234567890")

    with pytest.raises(RuntimeError) as ei:
        await analysis.analyze_screenshot(secrets, "m", {"id": "t"}, PNG, "image/png", client=Boom())
    assert "sk-ant" not in str(ei.value) and "ValueError" in str(ei.value)
    with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY"):
        await analysis.analyze_screenshot(Secrets(), "m", {"id": "t"}, PNG, "image/png")
    with pytest.raises(RuntimeError, match="отказалась"):
        await analysis.analyze_screenshot(secrets, "m", {"id": "t"}, PNG, "image/png", client=FakeClaude(stop_reason="refusal"))
