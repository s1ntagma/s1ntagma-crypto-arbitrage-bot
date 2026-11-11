"""Build stamp: same code -> same stamp (Windows CRLF too); changed code -> new stamp."""
from __future__ import annotations

import shutil
from pathlib import Path

from fastapi.testclient import TestClient

from latbot.buildinfo import PACKAGE_DIR, build_stamp, compute_build


def test_stamp_is_stable_crlf_insensitive_and_changes_with_code(tmp_path: Path):
    copy = tmp_path / "latbot"
    shutil.copytree(PACKAGE_DIR, copy, ignore=shutil.ignore_patterns("__pycache__"))
    a = compute_build(copy)
    assert len(a) == 7 and a == build_stamp()
    f = copy / "engine" / "mover.py"
    f.write_bytes(f.read_bytes().replace(b"\n", b"\r\n"))  # Windows checkout
    assert compute_build(copy) == a
    f.write_bytes(f.read_bytes() + b"\n# changed\n")
    assert compute_build(copy) != a


def test_stamp_in_health_state_and_status(tmp_path):
    from latbot.api.server import create_app
    from test_app import build
    b = build(tmp_path)
    with TestClient(create_app(b.ctx)) as c:
        assert c.get("/api/health").json()["build"] == build_stamp()
        assert c.get("/api/state").json()["build"] == build_stamp()
    if b.ctx.notifier is not None:
        assert build_stamp() in b.ctx.notifier._status_text()
