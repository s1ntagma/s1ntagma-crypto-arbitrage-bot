"""Offline tests for latbot.backtest.recorder (MarketRecorder + readers)."""
from __future__ import annotations

import gzip
import json
from pathlib import Path

from latbot import events
from latbot.backtest.recorder import MarketRecorder, iter_records, load_records
from latbot.clock import SimClock
from latbot.config import Settings
from latbot.events import EventBus
from latbot.models import BookLevel, OrderBook, Timestamps

DAY1 = 1_700_000_000.0  # 2023-11-14 22:13:20 UTC
DAY2 = DAY1 + 86_400.0


def book(ts: float, exchange: str = "kraken", mid: float = 100.0, levels: int = 3, asset: str = "BTC", ets: float | None = None) -> OrderBook:
    bids = [BookLevel(mid * (1 - 0.0001 * k), 1.0 * k) for k in range(1, levels + 1)]
    asks = [BookLevel(mid * (1 + 0.0001 * k), 2.0 * k) for k in range(1, levels + 1)]
    return OrderBook(
        exchange=exchange,
        symbol="BTC/EUR",
        bids=bids,
        asks=asks,
        ts=Timestamps(recv_ts=ts, recv_mono=1000.0 + (ts - DAY1), exchange_ts=ets),
        asset=asset,
    )


class Env:
    def __init__(self, tmp_path: Path, record: bool = True, **kw) -> None:
        self.settings = Settings(record_market_data=record)
        self.bus = EventBus()
        self.clock = SimClock(wall=DAY1, mono=1000.0)
        self.dir = tmp_path / "market"
        self.recorder = MarketRecorder(self.settings, self.bus, self.clock, out_dir=self.dir, **kw)


def read_lines(path: Path) -> list[dict]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as f:  # type: ignore[operator]
        return [json.loads(line) for line in f if line.strip()]


async def test_writes_documented_record_per_book(tmp_path: Path):
    env = Env(tmp_path, flush_every=1)
    await env.recorder.start()
    await env.bus.publish(events.BOOK, book(DAY1 + 1, levels=12, ets=DAY1 + 0.9))
    path = env.dir / "2023-11-14.jsonl"
    assert env.recorder.current_path == path and path.exists()
    rows = read_lines(path)
    assert len(rows) == 1
    rec = rows[0]
    assert set(rec) == {"t", "ex", "asset", "symbol", "ets", "b", "a"}
    assert rec["t"] == DAY1 + 1 and rec["ex"] == "kraken" and rec["asset"] == "BTC" and rec["symbol"] == "BTC/EUR"
    assert rec["ets"] == DAY1 + 0.9
    assert len(rec["b"]) == 10 and len(rec["a"]) == 10  # depth capped
    assert rec["b"][0] == [100.0 * (1 - 0.0001), 1.0] and rec["a"][0] == [100.0 * (1 + 0.0001), 2.0]
    assert rec["b"][0][0] > rec["b"][1][0] and rec["a"][0][0] < rec["a"][1][0]
    await env.recorder.stop()
    assert env.recorder.records_written == 1


async def test_asset_from_symbol_and_null_exchange_ts(tmp_path: Path):
    rec = MarketRecorder.encode(book(DAY1, asset=""), depth=2)
    assert rec["asset"] == "BTC" and rec["ets"] is None and len(rec["b"]) == 2
    assert MarketRecorder.day_of(DAY1) == "2023-11-14" and MarketRecorder.day_of(DAY2) == "2023-11-15"


async def test_disabled_setting_writes_nothing_and_toggle_works_live(tmp_path: Path):
    env = Env(tmp_path, record=False, flush_every=1)
    await env.recorder.start()
    await env.bus.publish(events.BOOK, book(DAY1 + 1))
    assert not env.dir.exists() and env.recorder.current_path is None
    env.settings.record_market_data = True
    await env.bus.publish(events.BOOK, book(DAY1 + 2))
    assert len(read_lines(env.dir / "2023-11-14.jsonl")) == 1
    env.settings.record_market_data = False
    await env.bus.publish(events.BOOK, book(DAY1 + 3))
    await env.recorder.stop()
    assert len(read_lines(env.dir / "2023-11-14.jsonl")) == 1


async def test_day_rotation_gzips_previous_day(tmp_path: Path):
    env = Env(tmp_path, flush_every=1)
    await env.recorder.start()
    await env.bus.publish(events.BOOK, book(DAY1 + 1))
    await env.bus.publish(events.BOOK, book(DAY1 + 2))
    await env.bus.publish(events.BOOK, book(DAY2 + 1))
    env.recorder.join_rotation(timeout=10)
    assert not (env.dir / "2023-11-14.jsonl").exists()
    gz = env.dir / "2023-11-14.jsonl.gz"
    assert gz.exists() and [r["t"] for r in read_lines(gz)] == [DAY1 + 1, DAY1 + 2]
    assert env.recorder.current_path == env.dir / "2023-11-15.jsonl"
    await env.bus.publish(events.BOOK, book(DAY2 + 2))
    await env.recorder.stop()
    assert [r["t"] for r in read_lines(env.dir / "2023-11-15.jsonl")] == [DAY2 + 1, DAY2 + 2]
    assert not (env.dir / "2023-11-15.jsonl.gz").exists()  # the open day is never compressed


async def test_restart_appends_and_compresses_leftover_days(tmp_path: Path):
    env = Env(tmp_path, flush_every=1)
    await env.recorder.start()
    await env.bus.publish(events.BOOK, book(DAY1 + 1))
    await env.recorder.stop()
    # restart on the same day: append
    env2 = Env(tmp_path, flush_every=1)
    await env2.recorder.start()
    await env2.bus.publish(events.BOOK, book(DAY1 + 5))
    await env2.recorder.stop()
    assert [r["t"] for r in read_lines(env2.dir / "2023-11-14.jsonl")] == [DAY1 + 1, DAY1 + 5]
    # restart the next day: yesterday's plain file is compressed on start
    env3 = Env(tmp_path, flush_every=1)
    env3.clock.set(wall=DAY2 + 10)
    await env3.recorder.start()
    env3.recorder.join_rotation(timeout=10)
    assert (env3.dir / "2023-11-14.jsonl.gz").exists() and not (env3.dir / "2023-11-14.jsonl").exists()
    await env3.recorder.stop()


async def test_stop_flushes_buffered_lines(tmp_path: Path):
    env = Env(tmp_path, flush_every=1000)
    await env.recorder.start()
    for i in range(5):
        await env.bus.publish(events.BOOK, book(DAY1 + i))
    assert read_lines(env.dir / "2023-11-14.jsonl") == []  # still buffered
    env.recorder.flush()
    assert len(read_lines(env.dir / "2023-11-14.jsonl")) == 5
    await env.bus.publish(events.BOOK, book(DAY1 + 9))
    await env.recorder.stop()
    assert len(read_lines(env.dir / "2023-11-14.jsonl")) == 6
    await env.bus.publish(events.BOOK, book(DAY1 + 10))  # unsubscribed after stop
    assert len(read_lines(env.dir / "2023-11-14.jsonl")) == 6


async def test_on_book_never_raises(tmp_path: Path, monkeypatch, caplog):
    env = Env(tmp_path)
    await env.recorder.start()
    monkeypatch.setattr(MarketRecorder, "encode", staticmethod(lambda b, depth=10: (_ for _ in ()).throw(ValueError("bad"))))
    for _ in range(3):
        env.recorder.on_book(book(DAY1))
    assert caplog.text.count("market recorder write failed") == 1
    await env.recorder.stop()


def test_iter_and_load_records_merge_gz_and_plain(tmp_path: Path, caplog):
    plain = tmp_path / "a.jsonl"
    plain.write_text(
        json.dumps({"t": 3.0, "ex": "kraken", "asset": "BTC", "symbol": "BTC/EUR", "ets": None, "b": [[1, 1]], "a": [[2, 1]]}) + "\n"
        + "not json\n"
        + json.dumps({"t": 1.0, "ex": "kraken", "symbol": "BTC/EUR", "ets": None, "b": [[1, 1]], "a": [[2, 1]]}) + "\n"
        + json.dumps({"ex": "kraken"}) + "\n"
        + "\n",
        encoding="utf-8",
    )
    gz = tmp_path / "b.jsonl.gz"
    with gzip.open(gz, "wt", encoding="utf-8") as f:
        f.write(json.dumps({"t": 2.0, "ex": "binance", "asset": "BTC", "symbol": "BTC/USDT", "ets": 1.9, "b": [[1, 1]], "a": [[2, 1]]}) + "\n")
    magic = tmp_path / "c.dat"  # gzip without the extension (detected by magic bytes)
    with gzip.open(magic, "wt", encoding="utf-8") as f:
        f.write(json.dumps({"t": 2.5, "ex": "kucoin", "asset": "BTC", "symbol": "BTC/USDT", "ets": None, "b": [], "a": []}) + "\n")
    recs = list(iter_records([plain]))
    assert [r["t"] for r in recs] == [3.0, 1.0] and recs[1]["asset"] == "BTC"
    assert "2 malformed line(s) skipped" in caplog.text
    merged = load_records([plain, gz, magic])
    assert [(r["t"], r["ex"]) for r in merged] == [(1.0, "kraken"), (2.0, "binance"), (2.5, "kucoin"), (3.0, "kraken")]
    assert load_records([tmp_path / "missing.jsonl"]) == []
