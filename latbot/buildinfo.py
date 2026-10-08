"""Build stamp: a short hash of the bot's own source files, so a screenshot
(UI top bar, /api/health, the startup log, Telegram /status) shows exactly
which version is running.  A GitHub ZIP has no .git, so the stamp is computed
from the files themselves; CRLF/LF differences are normalised so the same
code gives the same stamp on Windows and Linux.
"""
from __future__ import annotations

import hashlib
from functools import lru_cache
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent


def _files(root: Path) -> list[Path]:
    out = [p for p in root.rglob("*.py") if "__pycache__" not in p.parts]
    ui = root / "ui"
    if ui.is_dir():
        out += [p for p in ui.iterdir() if p.is_file() and p.suffix in (".js", ".css", ".html")]
    return sorted(set(out))


def compute_build(root: Path = PACKAGE_DIR) -> str:
    h = hashlib.sha1()
    for p in _files(root):
        rel = p.relative_to(root).as_posix()
        try:
            data = p.read_bytes().replace(b"\r\n", b"\n")
        except OSError:
            continue
        h.update(rel.encode("utf-8") + b"\0" + data + b"\0")
    return h.hexdigest()[:7]


@lru_cache(maxsize=1)
def build_stamp() -> str:
    try:
        return compute_build()
    except Exception:  # noqa: BLE001 - a stamp must never stop the bot
        return "unknown"


__all__ = ["build_stamp", "compute_build"]
