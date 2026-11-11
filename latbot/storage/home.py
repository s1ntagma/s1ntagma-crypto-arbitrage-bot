"""Where the bot keeps what must survive: a data folder OUTSIDE the program folder.

Every new build is unpacked into its own folder (``latbot-<build>``), so a database kept inside the
program folder (``data/latbot.sqlite3``) would start empty with every update.  The default database lives
in the user's data folder instead (``%LOCALAPPDATA%\\latbot`` on Windows, ``~/.latbot`` elsewhere,
``LATBOT_HOME`` overrides).  On the first start with an empty store, the history of the newest previous
install (its ``data/latbot.sqlite3`` and caches) is copied in; ``config.json`` and ``.env`` of the previous
install are copied into a new install that has none.  Nothing existing is ever overwritten.

``python -m latbot.storage.home`` runs the install step (``start.bat`` calls it before it would create a
new ``.env``).  File contents are never printed or logged - only file names.
"""
from __future__ import annotations

import logging
import os
import shutil
import sqlite3
import sys
from pathlib import Path

log = logging.getLogger(__name__)

DEFAULT_DB = "data/latbot.sqlite3"
DB_NAME = "latbot.sqlite3"
_INSTALL_FILES = ("config.json", ".env")
_MAX_DIRS = 400


def _windows() -> bool:
    return os.name == "nt"


def data_home() -> Path:
    override = os.environ.get("LATBOT_HOME")
    if override:
        return Path(override).expanduser()
    if _windows() and os.environ.get("LOCALAPPDATA"):
        return Path(os.environ["LOCALAPPDATA"]) / "latbot"
    return Path.home() / ".latbot"


def _is_install(d: Path) -> bool:
    return (d / "latbot").is_dir()


def _installs(program_dir: Path) -> list[Path]:
    """The program folder, its sibling installs, and the installs one level deeper next to its parent
    (Windows "Extract All" makes ``Downloads\\latbot-x\\latbot-x``)."""
    program_dir = program_dir.resolve()
    out: list[Path] = [program_dir]
    seen = {program_dir}
    roots = [program_dir.parent, program_dir.parent.parent]
    for depth, root in enumerate(roots):
        try:
            children = [c for c in root.iterdir() if c.is_dir()][:_MAX_DIRS]
        except OSError:
            continue
        for c in children:
            cands = [c]
            if depth == 1:
                try:
                    cands += [g for g in c.iterdir() if g.is_dir()][:20]
                except OSError:
                    pass
            for d in cands:
                d = d.resolve()
                if d not in seen and _is_install(d):
                    seen.add(d)
                    out.append(d)
    return out


def _mtime(p: Path) -> float:
    times = []
    for q in (p, p.with_name(p.name + "-wal")):
        try:
            times.append(q.stat().st_mtime)
        except OSError:
            pass
    return max(times, default=0.0)


def adopt_previous_data(store: Path, program_dir: Path) -> Path | None:
    """Copy the newest previous database (and its caches) into an empty ``store``.  Returns the source."""
    store = Path(store)
    if (store / DB_NAME).exists():
        return None
    dbs = [d / DEFAULT_DB for d in _installs(Path(program_dir))]
    dbs = [p for p in dbs if p.is_file() and p.stat().st_size > 0]
    if not dbs:
        return None
    src = max(dbs, key=_mtime)
    store.mkdir(parents=True, exist_ok=True)
    tmp = store / (DB_NAME + ".importing")
    try:
        try:
            con = sqlite3.connect(src.resolve().as_uri() + "?mode=ro", uri=True)
        except sqlite3.Error:
            con = sqlite3.connect(str(src))
        try:
            dst = sqlite3.connect(str(tmp))
            try:
                con.backup(dst)  # consistent copy, WAL included
            finally:
                dst.close()
        finally:
            con.close()
        tmp.replace(store / DB_NAME)
    except (sqlite3.Error, OSError) as exc:
        log.error("previous history not imported from %s: %s", src, exc)
        tmp.unlink(missing_ok=True)
        return None
    for f in src.parent.glob("*.json"):
        if not (store / f.name).exists():
            try:
                shutil.copy2(f, store / f.name)
            except OSError:
                pass
    log.info("history of the previous install imported: %s -> %s", src, store / DB_NAME)
    return src


def _same_text(a: Path, b: Path) -> bool:
    try:
        return a.read_bytes().strip() == b.read_bytes().strip()
    except OSError:
        return False


def adopt_previous_install(program_dir: Path) -> list[str]:
    """``config.json`` / ``.env`` of the newest previous install into a new install that has none
    (an ``.env`` still equal to ``.env.example`` counts as none).  Returns the file names copied."""
    program_dir = Path(program_dir).resolve()
    others = [d for d in _installs(program_dir) if d != program_dir]
    copied: list[str] = []
    for name in _INSTALL_FILES:
        mine = program_dir / name
        template = program_dir / ".env.example"
        missing = not mine.exists() or (name == ".env" and template.exists() and _same_text(mine, template))
        if not missing:
            continue
        cands = [d / name for d in others if (d / name).is_file()]
        if name == ".env":
            cands = [c for c in cands if not ((c.parent / ".env.example").exists() and _same_text(c, c.parent / ".env.example"))]
        if not cands:
            continue
        src = max(cands, key=lambda p: p.stat().st_mtime)
        try:
            shutil.copy2(src, mine)
            copied.append(name)
        except OSError as exc:
            log.error("%s not copied from the previous install: %s", name, exc)
    return copied


def main() -> int:
    names = adopt_previous_install(Path.cwd())
    if names:
        print("[latbot] taken from the previous install: " + ", ".join(names))
    return 0


if __name__ == "__main__":
    sys.exit(main())
