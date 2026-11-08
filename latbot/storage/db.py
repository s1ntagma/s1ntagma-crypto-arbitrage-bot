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

CREATE TABLE IF NOT EXISTS shadow_trades (
    id TEXT PRIMARY KEY,
    opened_ts REAL,
    closed_ts REAL,
    json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_shadow_opened ON shadow_trades(opened_ts);

CREATE TABLE IF NOT EXISTS basis_trades (
    id TEXT PRIMARY KEY,
    opened_ts REAL,
    closed_ts REAL,
    json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_basis_signal ON basis_trades(opened_ts);

CREATE TABLE IF NOT EXISTS trade_notes (
    trade_id TEXT PRIMARY KEY,
    note TEXT NOT NULL,
    updated_ts REAL
);

CREATE TABLE IF NOT EXISTS trade_screenshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id TEXT NOT NULL,
    path TEXT NOT NULL,
    media_type TEXT,
    created_ts REAL,
    analysis TEXT,
    analysis_ts REAL,
    analysis_model TEXT
);
CREATE INDEX IF NOT EXISTS ix_screenshots_trade ON trade_screenshots(trade_id);

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

-- Phase 3: market context and learning (measurements only)
CREATE TABLE IF NOT EXISTS news_items (
    id TEXT PRIMARY KEY,
    ts REAL,
    json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_news_ts ON news_items(ts);

CREATE TABLE IF NOT EXISTS event_reactions (
    event_key TEXT NOT NULL,
    asset TEXT NOT NULL,
    horizon_s INTEGER NOT NULL,
    event_type TEXT,
    event_ts REAL,
    json TEXT NOT NULL,
    PRIMARY KEY (event_key, asset, horizon_s)
);
CREATE INDEX IF NOT EXISTS ix_reactions_ts ON event_reactions(event_ts);

CREATE TABLE IF NOT EXISTS deriv_samples (
    asset TEXT NOT NULL,
    venue TEXT NOT NULL,
    ts REAL NOT NULL,
    json TEXT NOT NULL,
    PRIMARY KEY (asset, venue, ts)
);
CREATE INDEX IF NOT EXISTS ix_deriv_asset_ts ON deriv_samples(asset, ts);

CREATE TABLE IF NOT EXISTS metalabel_outcomes (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT UNIQUE,
    ts REAL,
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
        self._migrate_shadow_training()
        log.info("database ready at %s (journal=%s)", self.path, self.journal_mode)

    def _migrate_shadow_training(self) -> None:
        """``shadow_trades.training`` (0/1): offered and training trades are queried apart, so thousands
        of training rows never push the offered record (Stats, READY, virtual balance) out of a window."""
        cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(shadow_trades)").fetchall()}
        if "training" not in cols:
            self._conn.execute("ALTER TABLE shadow_trades ADD COLUMN training INTEGER NOT NULL DEFAULT 0")
            for r in self._conn.execute("SELECT id, json FROM shadow_trades").fetchall():
                d = self._load(r["json"])
                if isinstance(d, dict) and d.get("training"):
                    self._conn.execute("UPDATE shadow_trades SET training = 1 WHERE id = ?", (r["id"],))
        self._conn.execute("CREATE INDEX IF NOT EXISTS ix_shadow_training ON shadow_trades(training, opened_ts)")

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
    # trade journal: notes + screenshots (+ their analysis)
    # ------------------------------------------------------------------ #
    def set_trade_note(self, trade_id: str, note: str) -> None:
        now = self._clock.wall() if self._clock is not None else datetime.now(timezone.utc).timestamp()
        with self._lock:
            if note.strip():
                self._conn.execute(
                    "INSERT INTO trade_notes (trade_id, note, updated_ts) VALUES (?, ?, ?) "
                    "ON CONFLICT(trade_id) DO UPDATE SET note=excluded.note, updated_ts=excluded.updated_ts",
                    (trade_id, note, now),
                )
            else:
                self._conn.execute("DELETE FROM trade_notes WHERE trade_id = ?", (trade_id,))

    def get_trade_note(self, trade_id: str) -> str:
        with self._lock:
            row = self._conn.execute("SELECT note FROM trade_notes WHERE trade_id = ?", (trade_id,)).fetchone()
        return str(row["note"]) if row else ""

    def add_screenshot(self, trade_id: str, path: str, media_type: str) -> int:
        now = self._clock.wall() if self._clock is not None else datetime.now(timezone.utc).timestamp()
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO trade_screenshots (trade_id, path, media_type, created_ts) VALUES (?, ?, ?, ?)",
                (trade_id, path, media_type, now),
            )
            return int(cur.lastrowid)

    def get_screenshot(self, screenshot_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM trade_screenshots WHERE id = ?", (int(screenshot_id),)).fetchone()
        return dict(row) if row else None

    def list_screenshots(self, trade_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM trade_screenshots WHERE trade_id = ? ORDER BY id", (trade_id,)
            ).fetchall()
        return [dict(r) for r in rows]

    def set_screenshot_analysis(self, screenshot_id: int, analysis: str, model: str) -> None:
        now = self._clock.wall() if self._clock is not None else datetime.now(timezone.utc).timestamp()
        with self._lock:
            self._conn.execute(
                "UPDATE trade_screenshots SET analysis = ?, analysis_ts = ?, analysis_model = ? WHERE id = ?",
                (analysis, now, model, int(screenshot_id)),
            )

    def save_shadow(self, row: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO shadow_trades (id, opened_ts, closed_ts, training, json) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET opened_ts=excluded.opened_ts, closed_ts=excluded.closed_ts, "
                "training=excluded.training, json=excluded.json",
                (str(row["id"]), row.get("opened_ts"), row.get("closed_ts"), 1 if row.get("training") else 0,
                 json.dumps(row, ensure_ascii=False, default=str)),
            )

    @staticmethod
    def _shadow_where(training: bool | None, since_ts: float | None, open_only: bool,
                      closed: bool | None = None) -> tuple[str, list[Any]]:
        where, args = [], []
        if training is not None:
            where.append("training = ?")
            args.append(1 if training else 0)
        if since_ts is not None:
            where.append("opened_ts >= ?")
            args.append(float(since_ts))
        if open_only or closed is False:
            where.append("closed_ts IS NULL")
        elif closed:
            where.append("closed_ts IS NOT NULL")
        return (" WHERE " + " AND ".join(where)) if where else "", args

    def list_shadow(self, limit: int = 200, *, training: bool | None = None, since_ts: float | None = None,
                    open_only: bool = False) -> list[dict[str, Any]]:
        """Newest first.  ``training``: only training (True) / only offered (False) trades; ``since_ts``:
        opened from then on; ``open_only``: positions not closed yet."""
        where, args = self._shadow_where(training, since_ts, open_only)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT json FROM shadow_trades{where} ORDER BY opened_ts DESC LIMIT ?", (*args, int(limit))
            ).fetchall()
        out = []
        for r in rows:
            d = self._load(r["json"])
            if isinstance(d, dict):
                out.append(d)
        return out

    # ---- spot-futures basis scanner (virtual pairs) ---------------------------
    def save_basis_trade(self, row: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO basis_trades (id, opened_ts, closed_ts, json) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET opened_ts=excluded.opened_ts, closed_ts=excluded.closed_ts, json=excluded.json",
                (str(row["id"]), row.get("signal_ts"), row.get("closed_ts"), json.dumps(row, ensure_ascii=False, default=str)),
            )

    def list_basis_trades(self, limit: int = 200) -> list[dict[str, Any]]:
        """Newest first."""
        return self._json_rows("SELECT json FROM basis_trades ORDER BY opened_ts DESC LIMIT ?", (int(limit),))

    def count_shadow(self, *, training: bool | None = None, closed: bool | None = None) -> int:
        where, args = self._shadow_where(training, None, False, closed)
        with self._lock:
            return int(self._conn.execute(f"SELECT COUNT(*) FROM shadow_trades{where}", args).fetchone()[0])

    # ---- Phase 3: context + learning ----------------------------------------
    def _json_rows(self, sql: str, args: tuple[Any, ...]) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        out = []
        for r in rows:
            d = self._load(r["json"])
            if isinstance(d, dict):
                out.append(d)
        return out

    def save_news(self, item: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO news_items (id, ts, json) VALUES (?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET ts=excluded.ts, json=excluded.json",
                (str(item["id"]), item.get("ts"), json.dumps(item, ensure_ascii=False, default=_json_default)),
            )

    def list_news(self, since_ts: float | None = None, limit: int = 200) -> list[dict[str, Any]]:
        """Newest first."""
        if since_ts is None:
            return self._json_rows("SELECT json FROM news_items ORDER BY ts DESC LIMIT ?", (int(limit),))
        return self._json_rows("SELECT json FROM news_items WHERE ts >= ? ORDER BY ts DESC LIMIT ?", (float(since_ts), int(limit)))

    def save_event_reaction(self, row: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO event_reactions (event_key, asset, horizon_s, event_type, event_ts, json) VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(event_key, asset, horizon_s) DO UPDATE SET event_type=excluded.event_type, "
                "event_ts=excluded.event_ts, json=excluded.json",
                (str(row["event_key"]), str(row["asset"]), int(row["horizon_s"]), row.get("event_type"), row.get("event_ts"),
                 json.dumps(row, ensure_ascii=False, default=_json_default)),
            )

    def load_event_reactions(self, limit: int = 20000) -> list[dict[str, Any]]:
        """The most recent ``limit`` samples, oldest first."""
        rows = self._json_rows("SELECT json FROM event_reactions ORDER BY event_ts DESC, horizon_s DESC LIMIT ?", (int(limit),))
        rows.reverse()
        return rows

    def save_deriv_sample(self, row: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO deriv_samples (asset, venue, ts, json) VALUES (?, ?, ?, ?)",
                (str(row["asset"]), str(row.get("venue") or ""), float(row["ts"]), json.dumps(row, ensure_ascii=False, default=_json_default)),
            )

    def load_deriv_samples(self, asset: str, since_ts: float) -> list[dict[str, Any]]:
        """Oldest first."""
        return self._json_rows("SELECT json FROM deriv_samples WHERE asset = ? AND ts >= ? ORDER BY ts ASC", (str(asset), float(since_ts)))

    def save_metalabel_outcome(self, row: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO metalabel_outcomes (id, ts, json) VALUES (?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET ts=excluded.ts, json=excluded.json",
                (str(row["id"]) if row.get("id") is not None else None, row.get("ts"),
                 json.dumps(row, ensure_ascii=False, default=_json_default)),
            )

    def clear_metalabel_outcomes(self) -> None:
        """Drop every persisted meta-label outcome (one-time cleanup of data measured on broken books)."""
        with self._lock:
            self._conn.execute("DELETE FROM metalabel_outcomes")

    def load_metalabel_outcomes(self, limit: int = 20000) -> list[dict[str, Any]]:
        """The most recent ``limit`` outcomes, oldest first."""
        rows = self._json_rows("SELECT json FROM metalabel_outcomes ORDER BY seq DESC LIMIT ?", (int(limit),))
        rows.reverse()
        return rows

    def prune_context(self, now_ts: float, *, deriv_days: float = 31.0, news_days: float = 30.0,
                      reaction_days: float = 730.0, metalabel_keep: int = 20000) -> dict[str, int]:
        """Phase 3 tables grow every minute: drop what no reader asks for any more
        (derivative samples beyond the percentile window, old news, very old reactions,
        meta-label outcomes beyond the store's own limit).  Returns rows deleted per table."""
        out: dict[str, int] = {}
        with self._lock:
            for table, sql, args in (
                ("deriv_samples", "DELETE FROM deriv_samples WHERE ts < ?", (now_ts - deriv_days * 86400.0,)),
                ("news_items", "DELETE FROM news_items WHERE ts < ?", (now_ts - news_days * 86400.0,)),
                ("event_reactions", "DELETE FROM event_reactions WHERE event_ts < ?", (now_ts - reaction_days * 86400.0,)),
                ("metalabel_outcomes", "DELETE FROM metalabel_outcomes WHERE seq <= (SELECT MAX(seq) FROM metalabel_outcomes) - ?",
                 (max(1, int(metalabel_keep)),)),
            ):
                out[table] = int(self._conn.execute(sql, args).rowcount or 0)
        return out

    def journal_summary(self) -> dict[str, dict[str, Any]]:
        """trade_id -> {note, screenshots, analyzed} for the trades table."""
        out: dict[str, dict[str, Any]] = {}
        with self._lock:
            for r in self._conn.execute("SELECT trade_id, note FROM trade_notes").fetchall():
                out.setdefault(r["trade_id"], {"note": "", "screenshots": 0, "analyzed": 0})["note"] = r["note"]
            for r in self._conn.execute(
                "SELECT trade_id, COUNT(*) AS n, SUM(CASE WHEN analysis IS NOT NULL THEN 1 ELSE 0 END) AS a "
                "FROM trade_screenshots GROUP BY trade_id"
            ).fetchall():
                d = out.setdefault(r["trade_id"], {"note": "", "screenshots": 0, "analyzed": 0})
                d["screenshots"] = int(r["n"] or 0)
                d["analyzed"] = int(r["a"] or 0)
        return out

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
