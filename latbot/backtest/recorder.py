"""MarketRecorder - append every BOOK as one JSON line for later replay.

Line format (``MarketRecorder.encode``)::

    {"t": recv_ts, "ex": "kraken", "asset": "BTC", "symbol": "BTC/EUR",
     "ets": exchange_ts | null, "b": [[price, size], ... <= depth], "a": [...]}

Files are ``<out_dir>/YYYY-MM-DD.jsonl`` (UTC day of ``recv_ts``), opened in
append mode (restart-safe).  When the day changes the finished file is
gzipped to ``YYYY-MM-DD.jsonl.gz`` in a background thread (never blocks the
event loop).  Recording is active only while ``settings.record_market_data``
is True - the flag is read per book, so the UI toggle works live.

``iter_records`` / ``load_records`` read plain or gzipped files back (bad
lines are skipped and counted, one warning per file).
"""
from __future__ import annotations

import gzip
import json
import logging
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any, Iterable, Iterator

from .. import events
from ..clock import Clock
from ..config import Settings
from ..links import split_symbol
from ..models import OrderBook

log = logging.getLogger(__name__)

_DAY_FILE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}\.jsonl$")
_GZIP_MAGIC = b"\x1f\x8b"


def _gzip_file(src: Path) -> Path | None:
    """Compress ``src`` to ``src.gz`` and remove the plain file.  Returns the gz path."""
    dst = src.with_name(src.name + ".gz")
    try:
        with open(src, "rb") as fin, gzip.open(dst, "wb", compresslevel=6) as fout:
            while True:
                chunk = fin.read(1 << 20)
                if not chunk:
                    break
                fout.write(chunk)
        os.remove(src)
        log.info("market data compressed: %s", dst.name)
        return dst
    except Exception:  # noqa: BLE001
        log.exception("gzip of %s failed (plain file kept)", src.name)
        try:
            if dst.exists():
                dst.unlink()
        except OSError:
            pass
        return None


class MarketRecorder:
    """Writes order books to daily JSONL files for backtests."""

    def __init__(
        self,
        settings: Settings,
        bus: events.EventBus,
        clock: Clock,
        out_dir: str | os.PathLike[str] = "data/market",
        depth: int = 10,
        flush_every: int = 50,
    ) -> None:
        self._settings = settings
        self._bus = bus
        self._clock = clock
        self._out_dir = Path(out_dir)
        self._depth = max(1, int(depth))
        self._flush_every = max(1, int(flush_every))
        self._file: IO[str] | None = None
        self._path: Path | None = None
        self._day: str | None = None
        self._pending = 0
        self._unsub: Any = None
        self._error_logged = False
        self._threads: list[threading.Thread] = []
        self.records_written = 0

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def day_of(ts: float) -> str:
        """UTC ``YYYY-MM-DD`` of a wall timestamp."""
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d")

    @staticmethod
    def encode(book: OrderBook, depth: int = 10) -> dict[str, Any]:
        """One JSON-able record for ``book`` (top ``depth`` levels per side)."""
        asset = book.asset or split_symbol(book.symbol)[0]
        return {
            "t": float(book.ts.recv_ts),
            "ex": book.exchange,
            "asset": asset,
            "symbol": book.symbol,
            "ets": None if book.ts.exchange_ts is None else float(book.ts.exchange_ts),
            "b": [[float(lv.price), float(lv.size)] for lv in book.bids[:depth]],
            "a": [[float(lv.price), float(lv.size)] for lv in book.asks[:depth]],
        }

    @property
    def current_path(self) -> Path | None:
        return self._path

    @property
    def out_dir(self) -> Path:
        return self._out_dir

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        """Subscribe to BOOK; compress leftover plain files of previous days."""
        if self._unsub is not None:
            return
        self._unsub = self._bus.subscribe(events.BOOK, self.on_book)
        try:
            self._compress_leftovers()
        except Exception:  # noqa: BLE001
            log.exception("market recorder: leftover scan failed")
        log.info(
            "market recorder %s (dir %s, depth %d)",
            "recording" if self._settings.record_market_data else "idle (record_market_data off)",
            self._out_dir,
            self._depth,
        )

    async def stop(self) -> None:
        """Flush and close the open file (the current day is not gzipped)."""
        if self._unsub is not None:
            try:
                self._unsub()
            except Exception:  # noqa: BLE001
                pass
            self._unsub = None
        self._close()
        self.join_rotation(timeout=30.0)
        log.info("market recorder stopped (%d records written)", self.records_written)

    def join_rotation(self, timeout: float | None = None) -> None:
        """Wait for background gzip threads (tests / shutdown)."""
        for t in list(self._threads):
            t.join(timeout)
            if not t.is_alive():
                self._threads.remove(t)

    # ------------------------------------------------------------------ #
    # writing
    # ------------------------------------------------------------------ #
    def on_book(self, book: OrderBook) -> None:
        """Sync BOOK handler; never raises."""
        if not self._settings.record_market_data:
            return
        try:
            rec = self.encode(book, self._depth)
            day = self.day_of(rec["t"])
            if day != self._day or self._file is None:
                self._rotate(day)
            assert self._file is not None
            self._file.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
            self._pending += 1
            self.records_written += 1
            if self._pending >= self._flush_every:
                self.flush()
        except Exception:  # noqa: BLE001
            if not self._error_logged:
                self._error_logged = True
                log.exception("market recorder write failed (further errors suppressed)")

    def flush(self) -> None:
        if self._file is not None and self._pending:
            try:
                self._file.flush()
            except Exception:  # noqa: BLE001
                log.debug("market recorder flush failed", exc_info=True)
            self._pending = 0

    def _close(self) -> None:
        self.flush()
        if self._file is not None:
            try:
                self._file.close()
            except Exception:  # noqa: BLE001
                log.debug("market recorder close failed", exc_info=True)
        self._file = None

    def _rotate(self, day: str) -> None:
        previous = self._path
        self._close()
        self._out_dir.mkdir(parents=True, exist_ok=True)
        self._path = self._out_dir / f"{day}.jsonl"
        self._file = open(self._path, "a", encoding="utf-8", newline="\n")
        self._day = day
        self._pending = 0
        log.info("market recorder writing %s", self._path.name)
        if previous is not None and previous != self._path and previous.exists():
            self._compress_async(previous)

    def _compress_async(self, path: Path) -> None:
        t = threading.Thread(target=_gzip_file, args=(path,), name=f"gzip-{path.name}", daemon=True)
        self._threads.append(t)
        t.start()

    def _compress_leftovers(self) -> None:
        """Plain files of previous days (bot was off at midnight) -> gzip."""
        if not self._out_dir.exists():
            return
        today = self.day_of(self._clock.wall())
        for p in sorted(self._out_dir.iterdir()):
            if _DAY_FILE_RE.match(p.name) and p.name[:10] < today and not p.with_name(p.name + ".gz").exists():
                self._compress_async(p)


# ---------------------------------------------------------------------- #
# readers
# ---------------------------------------------------------------------- #
def _is_gzip(path: Path) -> bool:
    if path.suffix == ".gz":
        return True
    try:
        with open(path, "rb") as f:
            return f.read(2) == _GZIP_MAGIC
    except OSError:
        return False


def _open_text(path: Path) -> IO[str]:
    if _is_gzip(path):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


def _valid(rec: Any) -> bool:
    return (
        isinstance(rec, dict)
        and isinstance(rec.get("t"), (int, float))
        and isinstance(rec.get("ex"), str)
        and bool(rec.get("ex"))
        and (bool(rec.get("asset")) or bool(rec.get("symbol")))
        and isinstance(rec.get("b"), list)
        and isinstance(rec.get("a"), list)
    )


def iter_records(paths: Iterable[str | os.PathLike[str]]) -> Iterator[dict[str, Any]]:
    """Yield records from plain / gzipped JSONL files in the given order.
    Malformed lines are skipped (one warning per file with the count)."""
    for raw in paths:
        path = Path(raw)
        bad = 0
        try:
            with _open_text(path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except ValueError:
                        bad += 1
                        continue
                    if not _valid(rec):
                        bad += 1
                        continue
                    if not rec.get("asset"):
                        rec["asset"] = split_symbol(str(rec.get("symbol", "")))[0]
                    yield rec
        except OSError as exc:
            log.error("cannot read %s: %s", path, exc)
            continue
        if bad:
            log.warning("%s: %d malformed line(s) skipped", path.name, bad)


def load_records(paths: Iterable[str | os.PathLike[str]]) -> list[dict[str, Any]]:
    """All records of ``paths`` merged and stable-sorted by ``t``."""
    records = list(iter_records(paths))
    records.sort(key=lambda r: float(r["t"]))
    return records
