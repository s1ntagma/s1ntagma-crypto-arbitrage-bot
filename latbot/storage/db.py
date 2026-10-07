"""SQLite storage - synchronous, thread-safe, WAL.

One ``Database`` per process.  Every row keeps the full object as a JSON blob
(``json`` column) plus a handful of indexed columns for filtering and the
dashboard counters.  ``list_*`` methods return the parsed JSON dicts.

Serialisation: objects with ``to_dict()`` use it (``Opportunity``,
``TradeRecord``), other dataclasses go through ``dataclasses.asdict``; enums
are stored as their ``.value``.

``":memory:"`` is supported for tests (WAL is a no-op there).
"""
from __future__ import annotations

import dataclasses
import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from ..clock import Clock
from ..models import Fill, Mode, Opportunity, OppState, Order, OrderStatus, OrderType, ReactionStats, Side, TradeRecord

log = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS opportunities (
    id TEXT PRIMARY KEY,
    asset TEXT NOT NULL,
    exchange TEXT NOT NULL,
    direction INTEGER NOT NULL,
    state TEXT NOT NULL,
    net_pct REAL,
    detected_ts REAL,
    updated_ts REAL,
    json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_opp_state ON opportunities(state);
CREATE INDEX IF NOT EXISTS ix_opp_asset ON opportunities(asset);
CREATE INDEX IF NOT EXISTS ix_opp_exchange ON opportunities(exchange);
CREATE INDEX IF NOT EXISTS ix_opp_detected ON opportunities(detected_ts);

CREATE TABLE IF NOT EXISTS trades (
    id TEXT PRIMARY KEY,
    opportunity_id TEXT,
    mode TEXT NOT NULL,
    exchange TEXT NOT NULL,
    asset TEXT NOT NULL,
    state TEXT NOT NULL,
    expected_net_pct REAL,
    actual_net_pct REAL,
    realized_pnl_quote REAL,
    started_ts REAL,
    finished_ts REAL,
    json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_trades_mode ON trades(mode);
CREATE INDEX IF NOT EXISTS ix_trades_finished ON trades(finished_ts);
CREATE INDEX IF NOT EXISTS ix_trades_started ON trades(started_ts);

CREATE TABLE IF NOT EXISTS orders (
    id TEXT NOT NULL,
    trade_id TEXT,
    exchange TEXT NOT NULL,
    symbol TEXT,
    side TEXT,
    status TEXT,
    json TEXT NOT NULL,
    PRIMARY KEY (exchange, id)
);
CREATE INDEX IF NOT EXISTS ix_orders_trade ON orders(trade_id);

CREATE TABLE IF NOT EXISTS stats (
    asset TEXT NOT NULL,
    exchange TEXT NOT NULL,
    json TEXT NOT NULL,
    PRIMARY KEY (asset, exchange)
);

CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    json TEXT NOT NULL
);
"""


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Enum):
        return obj.value
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    if isinstance(obj, (set, frozenset, tuple)):
        return list(obj)
    if isinstance(obj, (bytes, bytearray)):
        return obj.decode("utf-8", "replace")
    return str(obj)


def _enum_value(v: Any) -> Any:
    return v.value if isinstance(v, Enum) else v


def _utc_day(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d")


# --------------------------------------------------------------------------- #
# deserialisation (the engine restores open trades after a restart)
# --------------------------------------------------------------------------- #
def order_from_dict(data: dict[str, Any]) -> Order:
    """Rebuild an ``Order`` from its JSON form (``save_order`` / ``TradeRecord.to_dict``)."""
    known = {f.name for f in dataclasses.fields(Order)}
    d: dict[str, Any] = {k: v for k, v in data.items() if k in known}
    d["side"] = Side(str(d.get("side") or Side.BUY.value))
    d["type"] = OrderType(str(d.get("type") or OrderType.LIMIT.value))
    d["status"] = OrderStatus(str(d.get("status") or OrderStatus.NEW.value))
    fill_fields = {f.name for f in dataclasses.fields(Fill)}
    fills: list[Fill] = []
    for f in d.get("fills") or []:
        if not isinstance(f, dict):
            continue
        try:
            fills.append(Fill(**{k: v for k, v in f.items() if k in fill_fields}))
        except TypeError:
            continue
    d["fills"] = fills
    for key in ("id", "client_id", "exchange", "symbol"):
        d[key] = str(d.get(key) or "")
    d["amount"] = float(d.get("amount") or 0.0)
    d["filled"] = float(d.get("filled") or 0.0)
    d.setdefault("price", None)
    return Order(**d)


def trade_from_dict(data: dict[str, Any]) -> TradeRecord:
    """Rebuild a ``TradeRecord`` from ``TradeRecord.to_dict()`` (as stored by ``save_trade``)."""
    known = {f.name for f in dataclasses.fields(TradeRecord)}
    d: dict[str, Any] = {k: v for k, v in data.items() if k in known}
    d["mode"] = Mode(str(d.get("mode") or Mode.PAPER.value))
    d["state"] = OppState(str(d.get("state") or OppState.EXECUTING.value))
    for key in ("entry_order", "exit_order"):
        o = d.get(key)
        d[key] = order_from_dict(o) if isinstance(o, dict) else None
    d["events"] = [
        (float(e[0]), str(e[1])) for e in d.get("events") or [] if isinstance(e, (list, tuple)) and len(e) >= 2
    ]
    d["direction"] = int(d.get("direction") or 1)
    return TradeRecord(**d)


class Database:
    """Thread-safe SQLite store.  All public methods take ``self._lock``."""

    def __init__(self, path: str, *, clock: Clock | None = None) -> None:
        self.path = str(path)
        self._clock = clock or Clock()
        self._lock = threading.Lock()
        self._closed = False
        if self.path != ":memory:":
            Path(self.path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        try:
            self.journal_mode = str(self._conn.execute("PRAGMA journal_mode=WAL").fetchone()[0])
        except sqlite3.DatabaseError as exc:  # pragma: no cover - exotic filesystems
            log.warning("WAL not available for %s: %s", self.path, exc)
            self.journal_mode = "unknown"
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=OFF")
        self._conn.executescript(_SCHEMA)
        log.info("database ready at %s (journal=%s)", self.path, self.journal_mode)

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._conn.close()
            except sqlite3.Error as exc:  # pragma: no cover
                log.warning("database close failed: %s", exc)

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # serialisation
    # ------------------------------------------------------------------ #
    def _dump(self, obj: Any) -> str:
        if isinstance(obj, Opportunity):
            data = obj.to_dict(self._clock.mono())
        elif hasattr(obj, "to_dict") and callable(obj.to_dict):
            data = obj.to_dict()
        elif dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            data = dataclasses.asdict(obj)
        else:
            data = obj
        return json.dumps(data, default=_json_default, ensure_ascii=False)

    @staticmethod
    def _load(text: str | None) -> Any:
        if text is None:
            return None
        try:
            return json.loads(text)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for r in rows:
            d = Database._load(r["json"])
            if isinstance(d, dict):
                out.append(d)
        return out

    # ------------------------------------------------------------------ #
    # opportunities
    # ------------------------------------------------------------------ #
    def save_opportunity(self, opp: Opportunity) -> None:
        """Upsert by id (json blob + indexed columns)."""
        payload = self._dump(opp)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO opportunities (id, asset, exchange, direction, state, net_pct, detected_ts, updated_ts, json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    asset=excluded.asset, exchange=excluded.exchange, direction=excluded.direction,
                    state=excluded.state, net_pct=excluded.net_pct, detected_ts=excluded.detected_ts,
                    updated_ts=excluded.updated_ts, json=excluded.json
                """,
                (
                    opp.id,
                    opp.asset,
                    opp.target_exchange,
                    int(opp.direction),
                    _enum_value(opp.state),
                    float(opp.econ.net_pct) if opp.econ is not None else None,
                    float(opp.detected_ts),
                    self._clock.wall(),
                    payload,
                ),
            )

    def list_opportunities(
        self,
        limit: int = 200,
        state: str | None = None,
        asset: str | None = None,
        exchange: str | None = None,
        since_ts: float | None = None,
    ) -> list[dict[str, Any]]:
        """Newest first (by detected_ts).  ``state`` may be an OppState or its value."""
        sql = "SELECT json FROM opportunities"
        where: list[str] = []
        args: list[Any] = []
        if state:
            where.append("state = ?")
            args.append(_enum_value(state))
        if asset:
            where.append("asset = ?")
            args.append(asset)
        if exchange:
            where.append("exchange = ?")
            args.append(exchange)
        if since_ts is not None:
            where.append("detected_ts >= ?")
            args.append(float(since_ts))
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY detected_ts DESC, rowid DESC LIMIT ?"
        args.append(int(limit))
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return self._rows_to_dicts(rows)

    # ------------------------------------------------------------------ #
    # trades
    # ------------------------------------------------------------------ #
    def save_trade(self, trade: TradeRecord) -> None:
        payload = self._dump(trade)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO trades (id, opportunity_id, mode, exchange, asset, state, expected_net_pct,
                                    actual_net_pct, realized_pnl_quote, started_ts, finished_ts, json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    opportunity_id=excluded.opportunity_id, mode=excluded.mode, exchange=excluded.exchange,
                    asset=excluded.asset, state=excluded.state, expected_net_pct=excluded.expected_net_pct,
                    actual_net_pct=excluded.actual_net_pct, realized_pnl_quote=excluded.realized_pnl_quote,
                    started_ts=excluded.started_ts, finished_ts=excluded.finished_ts, json=excluded.json
                """,
                (
                    trade.id,
                    trade.opportunity_id,
                    _enum_value(trade.mode),
                    trade.exchange,
                    trade.asset,
                    _enum_value(trade.state),
                    float(trade.expected_net_pct),
                    None if trade.actual_net_pct is None else float(trade.actual_net_pct),
                    None if trade.realized_pnl_quote is None else float(trade.realized_pnl_quote),
                    float(trade.started_ts),
                    None if trade.finished_ts is None else float(trade.finished_ts),
                    payload,
                ),
            )

    def list_trades(
        self, limit: int = 200, mode: str | None = None, since_ts: float | None = None
    ) -> list[dict[str, Any]]:
        """Newest first (by started_ts).  ``since_ts`` filters on started_ts."""
        sql = "SELECT json FROM trades"
        where: list[str] = []
        args: list[Any] = []
        if mode:
            where.append("mode = ?")
            args.append(_enum_value(mode))
        if since_ts is not None:
            where.append("started_ts >= ?")
            args.append(float(since_ts))
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY started_ts DESC, rowid DESC LIMIT ?"
        args.append(int(limit))
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return self._rows_to_dicts(rows)

    def get_trade(self, trade_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT json FROM trades WHERE id = ?", (trade_id,)).fetchone()
        d = self._load(row["json"]) if row else None
        return d if isinstance(d, dict) else None

    # ------------------------------------------------------------------ #
    # orders
    # ------------------------------------------------------------------ #
    def save_order(self, order: Order, trade_id: str | None) -> None:
        """Upsert by (exchange, id).  An order whose exchange id is still unknown
        (timed out, status SENT) is keyed by its client id until the id is known."""
        data = dataclasses.asdict(order)
        data.pop("raw", None)
        payload = json.dumps(data, default=_json_default, ensure_ascii=False)
        client_key = f"client:{order.client_id}" if order.client_id else ""
        row_id = order.id or client_key
        with self._lock:
            if order.id and client_key:
                self._conn.execute("DELETE FROM orders WHERE exchange = ? AND id = ?", (order.exchange, client_key))
            self._conn.execute(
                """
                INSERT INTO orders (id, trade_id, exchange, symbol, side, status, json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(exchange, id) DO UPDATE SET
                    trade_id=COALESCE(excluded.trade_id, orders.trade_id), symbol=excluded.symbol,
                    side=excluded.side, status=excluded.status, json=excluded.json
                """,
                (
                    row_id,
                    trade_id,
                    order.exchange,
                    order.symbol,
                    _enum_value(order.side),
                    _enum_value(order.status),
                    payload,
                ),
            )

    def list_orders(self, trade_id: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        sql = "SELECT json FROM orders"
        args: list[Any] = []
        if trade_id is not None:
            sql += " WHERE trade_id = ?"
            args.append(trade_id)
        sql += " ORDER BY rowid DESC LIMIT ?"
        args.append(int(limit))
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return self._rows_to_dicts(rows)

    # ------------------------------------------------------------------ #
    # stats
    # ------------------------------------------------------------------ #
    def save_stats(self, stats: ReactionStats) -> None:
        payload = self._dump(stats)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO stats (asset, exchange, json) VALUES (?, ?, ?)
                ON CONFLICT(asset, exchange) DO UPDATE SET json=excluded.json
                """,
                (stats.asset, stats.exchange, payload),
            )

    def load_stats(self) -> list[ReactionStats]:
        with self._lock:
            rows = self._conn.execute("SELECT json FROM stats ORDER BY asset, exchange").fetchall()
        known = {f.name for f in dataclasses.fields(ReactionStats)}
        out: list[ReactionStats] = []
        for r in rows:
            d = self._load(r["json"])
            if not isinstance(d, dict) or "asset" not in d or "exchange" not in d:
                continue
            try:
                out.append(ReactionStats(**{k: v for k, v in d.items() if k in known}))
            except TypeError as exc:
                log.warning("stats row unreadable (%s): %s", exc, d.get("asset"))
        return out

    # ------------------------------------------------------------------ #
    # kv
    # ------------------------------------------------------------------ #
    def kv_get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            row = self._conn.execute("SELECT json FROM kv WHERE key = ?", (key,)).fetchone()
        if row is None:
            return default
        try:
            return json.loads(row["json"])
        except (TypeError, ValueError):
            return default

    def kv_set(self, key: str, value: Any) -> None:
        payload = json.dumps(value, default=_json_default, ensure_ascii=False)
        with self._lock:
            self._conn.execute(
                "INSERT INTO kv (key, json) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET json=excluded.json",
                (key, payload),
            )

    def kv_delete(self, key: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM kv WHERE key = ?", (key,))

    # ------------------------------------------------------------------ #
    # aggregates
    # ------------------------------------------------------------------ #
    def _daily_pnl_locked(self, day: str, mode: str) -> float:
        row = self._conn.execute(
            """
            SELECT COALESCE(SUM(realized_pnl_quote), 0.0) AS pnl FROM trades
            WHERE mode = ? AND finished_ts IS NOT NULL AND realized_pnl_quote IS NOT NULL
              AND strftime('%Y-%m-%d', finished_ts, 'unixepoch') = ?
            """,
            (mode, day),
        ).fetchone()
        return float(row["pnl"] or 0.0)

    def daily_pnl(self, day: str, mode: str) -> float:
        """Sum of realized_pnl_quote of trades finished on ``day`` (UTC 'YYYY-MM-DD')."""
        with self._lock:
            return self._daily_pnl_locked(day, _enum_value(mode))

    def counters_today(self, mode: str, now_ts: float | None = None) -> dict[str, Any]:
        """Dashboard numbers for the current UTC day (of ``now_ts`` or the clock)."""
        mode_v = _enum_value(mode)
        ts = self._clock.wall() if now_ts is None else float(now_ts)
        day = _utc_day(ts)
        day_start = (int(ts) // 86400) * 86400
        day_end = day_start + 86400
        with self._lock:
            opps = self._conn.execute(
                "SELECT COUNT(*) AS n FROM opportunities WHERE detected_ts >= ? AND detected_ts < ?",
                (day_start, day_end),
            ).fetchone()["n"]
            expired = self._conn.execute(
                "SELECT COUNT(*) AS n FROM opportunities WHERE state = ? AND detected_ts >= ? AND detected_ts < ?",
                (OppState.EXPIRED.value, day_start, day_end),
            ).fetchone()["n"]
            trade_rows = self._conn.execute(
                "SELECT state, json FROM trades WHERE mode = ? AND started_ts >= ? AND started_ts < ?",
                (mode_v, day_start, day_end),
            ).fetchall()
            pnl = self._daily_pnl_locked(day, mode_v)
        trades = len(trade_rows)
        failed = sum(1 for r in trade_rows if r["state"] == OppState.FAILED.value)
        closed = sum(1 for r in trade_rows if r["state"] == OppState.CLOSED.value)
        exec_times: list[float] = []
        latencies: list[float] = []
        wins = 0
        for r in trade_rows:
            d = self._load(r["json"])
            if not isinstance(d, dict):
                continue
            et = d.get("execution_time_s")
            if isinstance(et, (int, float)):
                exec_times.append(float(et))
            lat = d.get("entry_latency_ms")
            if isinstance(lat, (int, float)):
                latencies.append(float(lat))
            rp = d.get("realized_pnl_quote")
            if isinstance(rp, (int, float)) and rp > 0:
                wins += 1
        return {
            "day": day,
            "mode": mode_v,
            "opportunities_today": int(opps),
            "trades": trades,
            "closed": closed,
            "failed": failed,
            "wins": wins,
            "expired": int(expired),
            "realized_pnl": pnl,
            "avg_execution_time_s": (sum(exec_times) / len(exec_times)) if exec_times else None,
            "avg_latency_ms": (sum(latencies) / len(latencies)) if latencies else None,
        }
