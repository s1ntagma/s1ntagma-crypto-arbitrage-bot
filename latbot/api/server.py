"""FastAPI server: JSON API + WebSocket hub for the latbot terminal UI.

``create_app(ctx)`` wires every route of ARCHITECTURE §api/server.py on top of
an application context (duck-typed; see ``latbot.app.AppContext``):
``settings, secrets, bus, clock, db, state, stats, opps, pipeline, adapters,
exec_adapters, risk, execution, notifier``.  Every optional member is read with
``getattr(ctx, name, None)`` and guarded, so the server also runs on a partial
context (tests, degraded start-up).

Rules enforced here
-------------------
* Secrets never reach a response: only ``Settings.public()`` is serialised and
  ``Secrets`` / ``ExchangeCredentials`` instances are dropped by ``_jsonable``.
* Errors are always JSON ``{"error": "..."}`` (400 validation, 404 unknown
  opportunity, 503 no execution engine, 500 engine failure).
* ``PUT /api/settings`` accepts only keys that exist on ``Settings``.
* The WebSocket hub coalesces OPP_UPDATE per opportunity id to at most one
  message per ``coalesce_interval_s`` (0.25 s -> <= 4/s per opportunity).
"""
from __future__ import annotations

import asyncio
import datetime as _dt
import functools
import inspect
import json
import logging
import math
import mimetypes
import os
import re
import time
from contextlib import asynccontextmanager
from dataclasses import asdict, fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any, AsyncIterator, Callable

from fastapi import Body, FastAPI, File, HTTPException, Query, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from .. import VERSION, analysis
from ..buildinfo import build_stamp
from ..config import ExchangeCredentials, Secrets, Settings, save_settings
from ..events import BALANCE, DASHBOARD, EMERGENCY, HEALTH, OPP_NEW, OPP_STATE, OPP_UPDATE, SETTINGS, TRADE_UPDATE
from ..logging_setup import redact
from ..models import ExchangeHealth, Light

log = logging.getLogger(__name__)

mimetypes.add_type("font/woff2", ".woff2")  # not known to every Windows registry
UI_DIR: Path = Path(__file__).resolve().parent.parent / "ui"
WS_TOPICS: tuple[str, ...] = (
    OPP_NEW,
    OPP_UPDATE,
    OPP_STATE,
    TRADE_UPDATE,
    HEALTH,
    DASHBOARD,
    EMERGENCY,
    SETTINGS,
    BALANCE,
)
OPP_STATE_FILTERS = ("active", "executing", "history", "all")
_LIGHT_VALUES = {light.value for light in Light}
_SECRET_KEY_RE = re.compile(r"(?i)(api[_-]?key|api[_-]?secret|secret|token|passphrase|password)")
_SETTINGS_KEYS = frozenset(f.name for f in fields(Settings))


# --------------------------------------------------------------------------- #
# JSON helpers
# --------------------------------------------------------------------------- #
def _jsonable(obj: Any) -> Any:
    """Convert models/dataclasses/enums into plain JSON-safe values.

    ``float`` NaN/inf become ``None`` (browsers reject ``NaN`` in JSON).
    ``Secrets`` / ``ExchangeCredentials`` are dropped unconditionally.
    """
    if obj is None or isinstance(obj, (bool, int, str)):
        return obj
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, (Secrets, ExchangeCredentials)):
        return None
    if isinstance(obj, Enum):
        return _jsonable(obj.value)
    if is_dataclass(obj) and not isinstance(obj, type):
        return _jsonable(asdict(obj))
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            key = k.value if isinstance(k, Enum) else k
            out[key if isinstance(key, str) else str(key)] = _jsonable(v)
        return out
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_jsonable(x) for x in obj]
    if isinstance(obj, (Path, _dt.date, _dt.datetime, _dt.time)):
        return str(obj)
    log.debug("non-serialisable object of type %s dropped", type(obj).__name__)
    return None


def _json(data: Any, status_code: int = 200) -> JSONResponse:
    return JSONResponse(content=_jsonable(data), status_code=status_code)


def _error(status_code: int, message: str) -> JSONResponse:
    return JSONResponse(content={"error": redact(str(message))}, status_code=status_code)


def _now(ctx: Any) -> float:
    clock = getattr(ctx, "clock", None)
    if clock is not None and hasattr(clock, "mono"):
        try:
            return float(clock.mono())
        except Exception:  # noqa: BLE001
            log.exception("clock.mono failed; falling back to time.monotonic")
    return time.monotonic()


def _wall(ctx: Any) -> float:
    clock = getattr(ctx, "clock", None)
    if clock is not None and hasattr(clock, "wall"):
        try:
            return float(clock.wall())
        except Exception:  # noqa: BLE001
            log.exception("clock.wall failed; falling back to time.time")
    return time.time()


_CHART_MAX_POINTS = 120
_CHART_MAX_MOVERS = 2


def _decimate(points: list[tuple[float, float]], limit: int) -> list[tuple[float, float]]:
    if len(points) <= limit:
        return points
    stride = -(-len(points) // limit)  # ceil
    out = points[::stride]
    if out[-1] is not points[-1]:
        out.append(points[-1])
    return out


def _finite(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def _chart_lines(settings: Any, opp: Any) -> list[tuple[str, str]]:
    """First mover, then the strongest other REAL movers (moved >= move_threshold_pct in the
    signal's direction), then the target.  ``signal.reacted`` holds every fresh exchange,
    flat and opposite ones included, so it is filtered here."""
    d = 1 if int(getattr(opp, "direction", 1) or 1) >= 0 else -1
    thr = float(getattr(settings, "move_threshold_pct", 0.0) or 0.0)
    reacted = dict(getattr(getattr(opp, "signal", None), "reacted", None) or {})
    skip = {opp.first_mover, opp.target_exchange}
    extra = sorted(
        ((ex, float(m)) for ex, m in reacted.items() if ex not in skip and _finite(m) and float(m) * d >= thr),
        key=lambda kv: (-kv[1] * d, kv[0]),
    )
    lines = [] if opp.first_mover == opp.target_exchange else [(opp.first_mover, "signal")]
    lines += [(ex, "signal") for ex, _ in extra[:_CHART_MAX_MOVERS]]
    lines.append((opp.target_exchange, "target"))
    return lines


def build_chart(ctx: Any, opp: Any, seconds: int) -> dict[str, Any]:
    """Real mid-price history of the movers and the target around an opportunity.

    x = seconds relative to the anchor (now, or the expiry of a finished opportunity).
    y = % change from ONE shared reference: the last real price of each exchange before the
    move started (``first_move_start_mono - move_window_s``), so the vertical gap between the
    movers and the target is the lag the card quotes, even when the move is older than the
    window.  When that reference is no longer in the history (trimmed) every line falls back
    to its first point in the window.  The price in effect at the window's left edge is the
    last real observation before it (carried forward, never interpolated).  A stream with
    fewer than two real points is not a line.
    """
    state = getattr(ctx, "state", None)
    settings = getattr(ctx, "settings", None)
    history_s = int(getattr(settings, "price_history_s", 60) or 60)
    seconds = max(5, min(int(seconds), history_s))
    expired = getattr(opp, "expired_mono", None)
    anchor = float(expired) if getattr(opp, "is_terminal", False) and _finite(expired) else _now(ctx)
    out: dict[str, Any] = {
        "opp_id": opp.id, "asset": opp.asset, "seconds": seconds, "unit": "pct_from_window_start",
        "detected_at_s": round(float(opp.first_seen_mono) - anchor, 3), "move_started_at_s": None,
        "series": [], "has_data": False,
    }
    if state is None or not hasattr(state, "mid_series") or not hasattr(state, "mid_at"):
        return out
    start = getattr(getattr(opp, "signal", None), "first_move_start_mono", None)
    ref = float(start) - float(getattr(settings, "move_window_s", 0.0) or 0.0) if _finite(start) else None
    if _finite(start):
        out["move_started_at_s"] = round(float(start) - anchor, 3)
    lo = anchor - seconds
    raw: list[tuple[str, str, list[tuple[float, float]], float | None]] = []
    for exchange, role in _chart_lines(settings, opp):
        series = list(state.mid_series(exchange, opp.asset))
        window = [(m, p) for m, p in series if lo <= m <= anchor and _finite(p) and p > 0]
        edge = state.mid_at(exchange, opp.asset, lo)
        if _finite(edge) and edge > 0 and (not window or window[0][0] > lo):
            window.insert(0, (lo, float(edge)))
        if len(window) < 2:
            continue
        base = state.mid_at(exchange, opp.asset, ref) if ref is not None else None
        raw.append((exchange, role, window, float(base) if _finite(base) and base > 0 else None))
    shared = bool(raw) and all(b is not None for *_, b in raw)
    if shared:
        out["unit"] = "pct_from_pre_move"
    for exchange, role, window, base in raw:
        b = base if shared and base is not None else window[0][1]
        pts = _decimate([(round(m - anchor, 3), round((p / b - 1.0) * 100.0, 4)) for m, p in window], _CHART_MAX_POINTS)
        out["series"].append({"exchange": exchange, "role": role, "points": [list(p) for p in pts]})
    out["has_data"] = bool(out["series"])
    return out


def _opp_dict(opp: Any, now_mono: float) -> dict[str, Any]:
    if isinstance(opp, dict):
        return _jsonable(opp)
    return _jsonable(opp.to_dict(now_mono))


def _trade_dict(trade: Any) -> dict[str, Any]:
    if isinstance(trade, dict):
        return _jsonable(trade)
    return _jsonable(trade.to_dict())


def _result(obj: Any, now_mono: float) -> Any:
    """Serialise an engine result: TradeRecord/Opportunity -> to_dict, lists recurse."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return _jsonable(obj)
    if isinstance(obj, dict):
        return {str(k): _result(v, now_mono) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_result(x, now_mono) for x in obj]
    to_dict = getattr(obj, "to_dict", None)
    if callable(to_dict):
        try:
            if _supports_kwarg(to_dict, "now_mono"):
                return _jsonable(to_dict(now_mono))
            return _jsonable(to_dict())
        except TypeError:
            return _jsonable(to_dict())
    return _jsonable(obj)


def _supports_kwarg(fn: Callable[..., Any], name: str) -> bool:
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False
    if name in params:
        return True
    return any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())


def _mode_value(settings: Any) -> str:
    try:
        mode = settings.mode()
    except Exception:  # noqa: BLE001
        return "paper"
    return mode.value if isinstance(mode, Enum) else str(mode)


# --------------------------------------------------------------------------- #
# State builders
# --------------------------------------------------------------------------- #
def _health_list(ctx: Any) -> list[dict[str, Any]]:
    """Health of every configured/active exchange with display label + usability."""
    now = _now(ctx)
    settings = ctx.settings
    adapters = getattr(ctx, "adapters", None) or {}
    state = getattr(ctx, "state", None)
    names: list[str] = list(adapters.keys())
    if not names:
        try:
            names = list(settings.enabled_exchanges())
        except Exception:  # noqa: BLE001
            names = []
    out: list[dict[str, Any]] = []
    for name in names:
        adapter = adapters.get(name)
        health = getattr(adapter, "health", None) if adapter is not None else None
        if health is None and state is not None and hasattr(state, "health"):
            try:
                health = state.health(name)
            except Exception:  # noqa: BLE001
                health = None
        if health is None:
            health = ExchangeHealth(exchange=name)
        d = _jsonable(health) if is_dataclass(health) else dict(health)
        d.setdefault("exchange", name)
        try:
            d["usable"] = bool(health.is_usable(now)) if hasattr(health, "is_usable") else bool(d.get("ws_connected"))
        except Exception:  # noqa: BLE001
            d["usable"] = False
        cfg = settings.exchange(name) if hasattr(settings, "exchange") else None
        d["label"] = cfg.display if cfg is not None else name.capitalize()
        interval = float(getattr(settings, "ticker_poll_interval_s", 2.0) or 2.0)
        if hasattr(health, "is_polling"):
            d["polling"] = bool(health.is_polling(now, interval))
        else:
            lp = getattr(health, "last_poll_mono", None)
            d["polling"] = bool(lp is not None and (now - float(lp)) <= max(30.0, 10.0 * interval))
        role = getattr(cfg, "role", None)
        d["role"] = role.value if isinstance(role, Enum) else (str(role) if role else "")
        out.append(d)
    return out


def _dashboard(ctx: Any) -> dict[str, Any]:
    now = _now(ctx)
    pipeline = getattr(ctx, "pipeline", None)
    if pipeline is not None and hasattr(pipeline, "dashboard"):
        try:
            return _jsonable(pipeline.dashboard() or {})
        except Exception:  # noqa: BLE001
            log.exception("pipeline.dashboard failed")
    opps = getattr(ctx, "opps", None)
    if opps is not None and hasattr(opps, "counters"):
        try:
            return _jsonable(opps.counters(now) or {})
        except Exception:  # noqa: BLE001
            log.exception("opps.counters failed")
    return {}


def _best(ctx: Any) -> dict[str, Any] | None:
    opps = getattr(ctx, "opps", None)
    if opps is None or not hasattr(opps, "best"):
        return None
    now = _now(ctx)
    try:
        best = opps.best(now)
    except Exception:  # noqa: BLE001
        log.exception("opps.best failed")
        return None
    return _opp_dict(best, now) if best is not None else None


def _emergency(ctx: Any) -> tuple[bool, str]:
    risk = getattr(ctx, "risk", None)
    if risk is None:
        return False, ""
    return bool(getattr(risk, "emergency", False)), str(getattr(risk, "emergency_reason", "") or "")


def build_state(ctx: Any) -> dict[str, Any]:
    """Payload of ``GET /api/state`` (also embedded in the WS snapshot)."""
    settings = ctx.settings
    emergency, reason = _emergency(ctx)
    return {
        "mode": _mode_value(settings),
        "emergency": emergency,
        "emergency_reason": reason,
        "settings": _jsonable(settings.public()),
        "health": _health_list(ctx),
        "dashboard": _dashboard(ctx),
        "best": _best(ctx),
        "version": VERSION,
        "build": build_stamp(),
        "server_ts": _wall(ctx),
    }


def _live_opps(ctx: Any) -> list[Any]:
    """Active + executing opportunities (deduplicated, net desc)."""
    opps = getattr(ctx, "opps", None)
    if opps is None:
        return []
    items: list[Any] = []
    seen: set[str] = set()
    for getter in ("active", "executing"):
        fn = getattr(opps, getter, None)
        if fn is None:
            continue
        try:
            for opp in fn() or []:
                if opp.id not in seen:
                    seen.add(opp.id)
                    items.append(opp)
        except Exception:  # noqa: BLE001
            log.exception("opps.%s failed", getter)
    return items


def build_snapshot(ctx: Any) -> dict[str, Any]:
    """Full snapshot sent to a WebSocket client on connect."""
    now = _now(ctx)
    return {"state": build_state(ctx), "opportunities": [_opp_dict(o, now) for o in _live_opps(ctx)]}


# --------------------------------------------------------------------------- #
# Event serialisation + WebSocket hub
# --------------------------------------------------------------------------- #
def _looks_like_secrets(payload: Any) -> bool:
    if isinstance(payload, (Secrets, ExchangeCredentials)):
        return True
    if isinstance(payload, dict):
        return any(_SECRET_KEY_RE.search(str(k)) for k in payload)
    return any(hasattr(payload, attr) for attr in ("telegram_bot_token", "api_secret"))


def serialise_event(topic: str, payload: Any, now_mono: float) -> dict[str, Any] | None:
    """Turn a bus event into ``{"topic", "data"}`` for the UI; ``None`` = skip."""
    try:
        if topic in (OPP_NEW, OPP_UPDATE):
            if payload is None:
                return None
            return {"topic": topic, "data": _opp_dict(payload, now_mono)}
        if topic == OPP_STATE:
            if isinstance(payload, dict):
                opp = payload.get("opportunity")
                if opp is None:
                    return None
                data = {
                    "opportunity": _opp_dict(opp, now_mono),
                    "from": _jsonable(payload.get("from")),
                    "to": _jsonable(payload.get("to")),
                    "reason": str(payload.get("reason") or ""),
                }
            else:
                data = {
                    "opportunity": _opp_dict(payload, now_mono),
                    "from": None,
                    "to": _jsonable(getattr(payload, "state", None)),
                    "reason": "",
                }
            return {"topic": topic, "data": data}
        if topic == TRADE_UPDATE:
            if payload is None:
                return None
            return {"topic": topic, "data": _trade_dict(payload)}
        if topic == HEALTH:
            if payload is None:
                return None
            data = _jsonable(payload) if is_dataclass(payload) else _jsonable(dict(payload))
            if hasattr(payload, "is_usable"):
                data["usable"] = bool(payload.is_usable(now_mono))
            return {"topic": topic, "data": data}
        if topic in (DASHBOARD, EMERGENCY):
            return {"topic": topic, "data": _jsonable(dict(payload or {}))}
        if topic == SETTINGS:
            if _looks_like_secrets(payload):
                log.warning("refusing to broadcast a secrets-like payload on topic %s", topic)
                return None
            public = getattr(payload, "public", None)
            if callable(public):
                return {"topic": topic, "data": _jsonable(public())}
            if isinstance(payload, dict):
                return {"topic": topic, "data": _jsonable(payload)}
            return None
        if topic == BALANCE:
            if not isinstance(payload, dict):
                return None
            return {
                "topic": topic,
                "data": {
                    "exchange": str(payload.get("exchange", "")),
                    "mode": _jsonable(payload.get("mode")),
                    "balances": _jsonable(payload.get("balances") or {}),
                },
            }
    except Exception:  # noqa: BLE001
        log.exception("failed to serialise event topic=%s", topic)
    return None


def _opp_id_of(msg: dict[str, Any]) -> str | None:
    data = msg.get("data")
    if not isinstance(data, dict):
        return None
    if msg.get("topic") == OPP_STATE:
        opp = data.get("opportunity")
        return opp.get("id") if isinstance(opp, dict) else None
    oid = data.get("id")
    return str(oid) if oid else None


class WsHub:
    """Fan-out of bus events to WebSocket clients with OPP_UPDATE coalescing.

    ``on_event`` is synchronous (safe as a bus handler); messages go through an
    ``asyncio.Queue`` drained by ``_pump``.  OPP_UPDATE messages are parked in
    ``_pending`` per opportunity id and released by ``flush`` every
    ``coalesce_interval_s`` so a client sees at most ~4 updates/s per
    opportunity.  An OPP_STATE for an id drops its pending update, so a stale
    coalesced update can never regress a state the client already saw.
    """

    def __init__(self, ctx: Any, *, coalesce_interval_s: float = 0.25) -> None:
        self._ctx = ctx
        self._interval = max(0.01, float(coalesce_interval_s))
        self.clients: set[WebSocket] = set()
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._pending: dict[str, dict[str, Any]] = {}
        self._unsubs: list[Callable[[], None]] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._started = False
        self.sent_count = 0

    # ---- lifecycle -------------------------------------------------------
    def start(self) -> None:
        """Subscribe to the bus and spawn the pump/flush tasks (idempotent).
        Must be called from inside a running event loop."""
        if self._started:
            return
        bus = getattr(self._ctx, "bus", None)
        if bus is not None and hasattr(bus, "subscribe"):
            for topic in WS_TOPICS:
                self._unsubs.append(bus.subscribe(topic, functools.partial(self.on_event, topic)))
        loop = asyncio.get_running_loop()
        self._tasks = [loop.create_task(self._pump(), name="ws-hub-pump"), loop.create_task(self._flusher(), name="ws-hub-flush")]
        self._started = True
        log.info("websocket hub started (coalesce %.0f ms)", self._interval * 1000)

    async def stop(self) -> None:
        for unsub in self._unsubs:
            try:
                unsub()
            except Exception:  # noqa: BLE001
                log.exception("unsubscribe failed")
        self._unsubs.clear()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._tasks.clear()
        for ws in list(self.clients):
            try:
                await ws.close()
            except Exception:  # noqa: BLE001
                pass
        self.clients.clear()
        self._pending.clear()
        self._started = False

    # ---- events ----------------------------------------------------------
    def on_event(self, topic: str, payload: Any) -> None:
        msg = serialise_event(topic, payload, _now(self._ctx))
        if msg is None:
            return
        oid = _opp_id_of(msg)
        if topic == OPP_UPDATE and oid:
            self._pending[oid] = msg
            return
        if topic == OPP_STATE and oid:
            self._pending.pop(oid, None)
        self._queue.put_nowait(msg)

    async def flush(self) -> None:
        """Broadcast every pending coalesced OPP_UPDATE (one per opportunity)."""
        if not self._pending:
            return
        pending = list(self._pending.values())
        self._pending.clear()
        for msg in pending:
            await self.broadcast(msg)

    async def broadcast(self, msg: dict[str, Any]) -> None:
        if not self.clients:
            return
        try:
            text = json.dumps(_jsonable(msg), ensure_ascii=False, allow_nan=False, default=str)
        except (TypeError, ValueError):
            log.exception("cannot encode websocket message topic=%s", msg.get("topic"))
            return
        dead: list[WebSocket] = []
        for ws in list(self.clients):
            try:
                await ws.send_text(text)
            except Exception:  # noqa: BLE001
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)
        self.sent_count += 1

    async def _pump(self) -> None:
        while True:
            msg = await self._queue.get()
            try:
                await self.broadcast(msg)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("broadcast failed")

    async def _flusher(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            try:
                await self.flush()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("flush failed")

    # ---- client handling -------------------------------------------------
    async def handle(self, ws: WebSocket) -> None:
        await ws.accept()
        self.start()
        try:
            snapshot = {"topic": "snapshot", "data": build_snapshot(self._ctx)}
            await ws.send_text(json.dumps(_jsonable(snapshot), ensure_ascii=False, allow_nan=False, default=str))
        except Exception:  # noqa: BLE001
            log.exception("failed to send snapshot")
            self.clients.discard(ws)
            return
        self.clients.add(ws)
        try:
            while True:
                text = await ws.receive_text()
                if text.strip().lower() == "ping":
                    await ws.send_text(json.dumps({"topic": "pong", "data": {"ts": time.time()}}))
        except WebSocketDisconnect:
            pass
        except Exception:  # noqa: BLE001
            log.debug("websocket client dropped", exc_info=True)
        finally:
            self.clients.discard(ws)


# --------------------------------------------------------------------------- #
# Request bodies
# --------------------------------------------------------------------------- #
class ManualEntered(BaseModel):
    price: float = Field(gt=0, description="price the user actually got on the entry leg")
    quote_amount: float = Field(gt=0, description="capital used, in quote currency")


class ManualExited(BaseModel):
    price: float = Field(gt=0, description="price the user actually got on the exit leg")
    reported_pnl_quote: float | None = Field(None, description="result the exchange showed (quote currency)")
    note: str = Field("", max_length=2000)


class ManualSelect(BaseModel):
    capital: float | None = Field(None, gt=0, description="amount the owner wants to put in (quote currency)")


class EmergencyStopBody(BaseModel):
    reason: str = "user"


class NoteBody(BaseModel):
    note: str = Field("", max_length=4000)


class CancelBody(BaseModel):
    reason: str = "user"


# --------------------------------------------------------------------------- #
# Engine call helper
# --------------------------------------------------------------------------- #
async def _call(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Call an engine method (sync or async) and map its errors to HTTP errors."""
    try:
        result = fn(*args, **kwargs)
        if inspect.isawaitable(result):
            result = await result
        return result
    except HTTPException:
        raise
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="opportunity not found") from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=redact(str(exc)) or "not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=redact(str(exc)) or "invalid request") from exc
    except Exception as exc:  # noqa: BLE001
        log.exception("engine call %s failed", getattr(fn, "__name__", repr(fn)))
        raise HTTPException(status_code=500, detail=redact(f"{type(exc).__name__}: {exc}")) from exc


# --------------------------------------------------------------------------- #
# Application factory
# --------------------------------------------------------------------------- #
def _trade_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    def stats(group: list[dict[str, Any]]) -> dict[str, Any]:
        done = [r for r in group if str(r.get("state")) in ("CLOSED", "FAILED") and r.get("actual_net_pct") is not None]
        n = len(done)
        wins = sum(1 for r in done if float(r.get("realized_pnl_quote") or 0.0) > 0)
        return {
            "trades": len(group),
            "finished": n,
            "open": sum(1 for r in group if str(r.get("state")) == "EXECUTING"),
            "wins": wins,
            "win_rate": (wins / n) if n else None,
            "avg_expected_net_pct": (sum(float(r.get("expected_net_pct") or 0.0) for r in done) / n) if n else None,
            "avg_actual_net_pct": (sum(float(r.get("actual_net_pct") or 0.0) for r in done) / n) if n else None,
            "pnl_quote": sum(float(r.get("realized_pnl_quote") or 0.0) for r in done),
        }

    out = stats(rows)
    by: dict[str, dict[str, list[dict[str, Any]]]] = {"by_mode": {}, "by_exchange": {}, "by_asset": {}}
    for r in rows:
        by["by_mode"].setdefault(str(r.get("mode") or "?"), []).append(r)
        by["by_exchange"].setdefault(str(r.get("exchange") or "?"), []).append(r)
        by["by_asset"].setdefault(str(r.get("asset") or "?"), []).append(r)
    for k, groups in by.items():
        out[k] = {name: stats(g) for name, g in groups.items()}
    return out


class _NoCacheStatic(StaticFiles):
    """UI assets are revalidated on every load (ETag -> 304 when unchanged), so a
    new version of the bot never runs with a stale app.js from the browser cache."""

    async def get_response(self, path: str, scope: Any) -> Response:
        response = await super().get_response(path, scope)
        response.headers["Cache-Control"] = "no-cache"
        return response


def _asset_version(path: Path) -> str:
    try:
        st = path.stat()
    except OSError:
        return VERSION
    return f"{int(st.st_mtime)}-{st.st_size}"


def _versioned_index(page: Path) -> str:
    """index.html with ``?v=<mtime-size>`` on app.js / style.css (cache busting)."""
    html = page.read_text(encoding="utf-8")
    for name in ("app.js", "style.css"):
        html = html.replace(f"/static/{name}\"", f"/static/{name}?v={_asset_version(page.parent / name)}\"")
    return html


def create_app(ctx: Any, *, config_path: str | os.PathLike[str] | None = None) -> FastAPI:
    """Build the FastAPI app for the given application context.

    ``config_path`` is where ``PUT /api/settings`` persists ``config.json``
    (kwarg -> ``ctx.config_path`` -> ``"config.json"``).
    """
    cfg_path = Path(config_path if config_path is not None else (getattr(ctx, "config_path", None) or "config.json"))
    hub = WsHub(ctx)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        hub.start()
        try:
            yield
        finally:
            await hub.stop()

    app = FastAPI(title="latbot", version=VERSION, lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.hub = hub
    app.state.ctx = ctx

    # ---- error handling ----------------------------------------------------
    @app.exception_handler(RequestValidationError)
    async def _validation_error(_request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = exc.errors() if hasattr(exc, "errors") else []
        if errors:
            first = errors[0]
            loc = ".".join(str(x) for x in first.get("loc", ()) if x != "body") or "request"
            msg = f"{loc}: {first.get('msg', 'invalid value')}"
        else:
            msg = "invalid request"
        return _error(400, msg)

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        detail = exc.detail if isinstance(exc.detail, str) else json.dumps(_jsonable(exc.detail))
        return _error(exc.status_code, detail)

    @app.exception_handler(ValueError)
    async def _value_error(_request: Request, exc: ValueError) -> JSONResponse:
        return _error(400, str(exc) or "invalid value")

    @app.exception_handler(LookupError)
    async def _lookup_error(_request: Request, exc: LookupError) -> JSONResponse:
        return _error(404, "opportunity not found" if isinstance(exc, KeyError) else (str(exc) or "not found"))

    @app.exception_handler(Exception)
    async def _unexpected(_request: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled error in API")
        return _error(500, f"{type(exc).__name__}: {exc}")

    # ---- helpers bound to ctx ---------------------------------------------
    def _settings() -> Any:
        return ctx.settings

    def _require_execution() -> Any:
        execution = getattr(ctx, "execution", None)
        if execution is None:
            raise HTTPException(status_code=503, detail="execution engine not available")
        return execution

    def _require_opp(opp_id: str) -> Any:
        opps = getattr(ctx, "opps", None)
        if opps is None or not hasattr(opps, "get"):
            return None
        opp = opps.get(opp_id)
        if opp is None:
            raise HTTPException(status_code=404, detail="opportunity not found")
        return opp

    def _require_opp_or_manual_position(opp_id: str) -> Any:
        """Manual routes: the opportunity - or, after a restart, the open MANUAL
        position the engine restored for an opportunity that no longer exists
        (``execution.active_trade``), so its exit can still be reported."""
        opps = getattr(ctx, "opps", None)
        if opps is None or not hasattr(opps, "get"):
            return None
        opp = opps.get(opp_id)
        if opp is not None:
            return opp
        active_trade = getattr(getattr(ctx, "execution", None), "active_trade", None)
        if callable(active_trade):
            try:
                trade = active_trade(opp_id)
            except Exception:  # noqa: BLE001
                log.exception("execution.active_trade failed")
                trade = None
            if trade is not None and getattr(trade, "entry_order", None) is None:
                return trade
        raise HTTPException(status_code=404, detail="opportunity not found")

    async def _publish(topic: str, payload: Any) -> None:
        bus = getattr(ctx, "bus", None)
        if bus is None or not hasattr(bus, "publish"):
            return
        try:
            await bus.publish(topic, payload)
        except Exception:  # noqa: BLE001
            log.exception("publish %s failed", topic)

    # ---- UI --------------------------------------------------------------
    if UI_DIR.is_dir():
        app.mount("/static", _NoCacheStatic(directory=str(UI_DIR)), name="static")
    else:  # pragma: no cover - packaging problem, not a runtime path
        log.error("UI directory missing: %s", UI_DIR)

    @app.get("/", include_in_schema=False)
    async def index() -> Response:
        page = UI_DIR / "index.html"
        if not page.is_file():
            return _error(404, "ui not installed")
        return Response(_versioned_index(page), media_type="text/html; charset=utf-8", headers={"Cache-Control": "no-cache"})

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon() -> Response:
        return Response(status_code=204)

    # ---- state -----------------------------------------------------------
    @app.get("/api/state")
    async def api_state() -> JSONResponse:
        return _json(build_state(ctx))

    @app.get("/api/health")
    async def api_health() -> JSONResponse:
        emergency, reason = _emergency(ctx)
        return _json(
            {
                "ok": True,
                "mode": _mode_value(_settings()),
                "emergency": emergency,
                "emergency_reason": reason,
                "ws_clients": len(hub.clients),
                "exchanges": _health_list(ctx),
                "version": VERSION,
                "build": build_stamp(),
                "server_ts": _wall(ctx),
            }
        )

    # ---- opportunities ---------------------------------------------------
    @app.get("/api/opportunities")
    async def api_opportunities(
        state: str = Query("active"),
        asset: str | None = Query(None),
        exchange: str | None = Query(None),
        min_net: float | None = Query(None),
        min_window: float | None = Query(None),
        human: str | None = Query(None),
        bot: str | None = Query(None),
        limit: int = Query(200, ge=1, le=5000),
    ) -> JSONResponse:
        state_f = (state or "active").strip().lower()
        if state_f not in OPP_STATE_FILTERS:
            raise HTTPException(status_code=400, detail=f"state must be one of {', '.join(OPP_STATE_FILTERS)}")
        human_f = (human or "").strip().lower() or None
        bot_f = (bot or "").strip().lower() or None
        for name, value in (("human", human_f), ("bot", bot_f)):
            if value is not None and value not in _LIGHT_VALUES:
                raise HTTPException(status_code=400, detail=f"{name} must be one of green, yellow, red")
        opps = getattr(ctx, "opps", None)
        now = _now(ctx)
        items: list[Any] = []
        if opps is not None:
            seen: set[str] = set()

            def _extend(values: Any) -> None:
                for o in values or []:
                    if o.id not in seen:
                        seen.add(o.id)
                        items.append(o)

            if state_f in ("active", "all") and hasattr(opps, "active"):
                _extend(opps.active())
            if state_f in ("executing", "all") and hasattr(opps, "executing"):
                _extend(opps.executing())
            if state_f in ("history", "all") and hasattr(opps, "history"):
                _extend(opps.history(limit))
        asset_f = (asset or "").strip().upper() or None
        exchange_f = (exchange or "").strip().lower() or None
        out: list[dict[str, Any]] = []
        for o in items:
            if asset_f and str(o.asset).upper() != asset_f:
                continue
            if exchange_f and str(o.target_exchange).lower() != exchange_f:
                continue
            if min_net is not None and float(o.econ.net_pct) < min_net:
                continue
            if min_window is not None and float(o.remaining_s(now)) < min_window:
                continue
            if human_f and _jsonable(o.human.light) != human_f:
                continue
            if bot_f and _jsonable(o.bot.light) != bot_f:
                continue
            out.append(_opp_dict(o, now))
            if len(out) >= limit:
                break
        return _json(out)

    @app.get("/api/opportunities/{opp_id}")
    async def api_opportunity(opp_id: str) -> JSONResponse:
        opps = getattr(ctx, "opps", None)
        opp = opps.get(opp_id) if opps is not None and hasattr(opps, "get") else None
        if opp is None:
            raise HTTPException(status_code=404, detail="opportunity not found")
        return _json(_opp_dict(opp, _now(ctx)))

    @app.get("/api/opportunities/{opp_id}/chart")
    async def api_opportunity_chart(opp_id: str, seconds: int = Query(30)) -> JSONResponse:
        opps = getattr(ctx, "opps", None)
        opp = opps.get(opp_id) if opps is not None and hasattr(opps, "get") else None
        if opp is None:
            raise HTTPException(status_code=404, detail="opportunity not found")
        return _json(build_chart(ctx, opp, seconds))

    # ---- manual flow -----------------------------------------------------
    @app.post("/api/opportunities/{opp_id}/manual")
    async def api_manual_select(opp_id: str, body: ManualSelect | None = Body(None)) -> JSONResponse:
        _require_opp_or_manual_position(opp_id)
        execution = _require_execution()
        capital = body.capital if body is not None else None
        if capital is not None and _supports_kwarg(execution.select_manual, "capital"):
            return _json(_result(await _call(execution.select_manual, opp_id, capital=capital), _now(ctx)))
        return _json(_result(await _call(execution.select_manual, opp_id), _now(ctx)))

    @app.get("/api/opportunities/{opp_id}/manual/support")
    async def api_manual_support(opp_id: str, capital: float | None = Query(None, gt=0)) -> JSONResponse:
        _require_opp_or_manual_position(opp_id)
        execution = _require_execution()
        if capital is not None and _supports_kwarg(execution.manual_support, "capital"):
            return _json(_result(await _call(execution.manual_support, opp_id, capital=capital), _now(ctx)))
        return _json(_result(await _call(execution.manual_support, opp_id), _now(ctx)))

    @app.post("/api/opportunities/{opp_id}/manual/entered")
    async def api_manual_entered(opp_id: str, body: ManualEntered) -> JSONResponse:
        _require_opp(opp_id)
        execution = _require_execution()
        result = await _call(execution.manual_entered, opp_id, body.price, body.quote_amount)
        return _json(_result(result, _now(ctx)))

    @app.post("/api/opportunities/{opp_id}/manual/exited")
    async def api_manual_exited(opp_id: str, body: ManualExited) -> JSONResponse:
        _require_opp_or_manual_position(opp_id)  # a restored position has no opportunity any more
        execution = _require_execution()
        if (body.reported_pnl_quote is not None or body.note) and _supports_kwarg(execution.manual_exited, "note"):
            result = await _call(execution.manual_exited, opp_id, body.price,
                                 reported_pnl_quote=body.reported_pnl_quote, note=body.note)
        else:
            result = await _call(execution.manual_exited, opp_id, body.price)
        return _json(_result(result, _now(ctx)))

    # ---- auto flow -------------------------------------------------------
    @app.post("/api/opportunities/{opp_id}/auto")
    async def api_auto_select(opp_id: str) -> JSONResponse:
        _require_opp(opp_id)
        execution = _require_execution()
        kwargs: dict[str, Any] = {}
        # With AUTO_CONFIRM the engine executes right away: never hold the HTTP
        # response for the whole trade (entry + exit + hold can take minutes).
        # The trade runs as a tracked task and the UI follows TRADE_UPDATE.
        if _supports_kwarg(execution.select_auto, "wait"):
            kwargs["wait"] = False
        return _json(_result(await _call(execution.select_auto, opp_id, **kwargs), _now(ctx)))

    @app.post("/api/opportunities/{opp_id}/auto/confirm")
    async def api_auto_confirm(opp_id: str) -> JSONResponse:
        _require_opp(opp_id)
        execution = _require_execution()
        kwargs: dict[str, Any] = {}
        # Non-blocking when the engine supports it: the trade runs as a tracked
        # task and the UI follows it through TRADE_UPDATE events.
        if _supports_kwarg(execution.confirm_auto, "wait"):
            kwargs["wait"] = False
        return _json(_result(await _call(execution.confirm_auto, opp_id, **kwargs), _now(ctx)))

    @app.post("/api/opportunities/{opp_id}/auto/deselect")
    async def api_auto_deselect(opp_id: str) -> JSONResponse:
        """Back out of the AUTO confirmation: AUTO_SELECTED -> VALID.  Nothing is
        cancelled - the opportunity stays on the board (distinct from /cancel)."""
        _require_opp(opp_id)
        execution = _require_execution()
        deselect = getattr(execution, "deselect_auto", None)
        if not callable(deselect):
            raise HTTPException(status_code=501, detail="auto/deselect is not supported by this execution engine")
        return _json(_result(await _call(deselect, opp_id), _now(ctx)))

    @app.post("/api/opportunities/{opp_id}/cancel")
    async def api_cancel(opp_id: str, body: CancelBody | None = None) -> JSONResponse:
        _require_opp(opp_id)
        execution = _require_execution()
        reason = (body.reason if body is not None else "user") or "user"
        return _json(_result(await _call(execution.cancel, opp_id, reason=reason), _now(ctx)))

    # ---- emergency -------------------------------------------------------
    @app.post("/api/emergency/stop")
    async def api_emergency_stop(body: EmergencyStopBody | None = None) -> JSONResponse:
        reason = (body.reason if body is not None else "user") or "user"
        execution = getattr(ctx, "execution", None)
        risk = getattr(ctx, "risk", None)
        if execution is not None and hasattr(execution, "emergency_stop"):
            result = await _call(execution.emergency_stop, reason)
            data = dict(result) if isinstance(result, dict) else {}
            data.setdefault("ok", True)
            data.setdefault("emergency", True)
            data.setdefault("reason", reason)
            log.warning("EMERGENCY STOP via API (reason=%s)", reason)
            return _json(_result(data, _now(ctx)))
        if risk is not None and hasattr(risk, "set_emergency"):
            await _call(risk.set_emergency, True, reason)
            log.warning("EMERGENCY STOP via API without execution engine (reason=%s)", reason)
            return _json({"ok": True, "emergency": True, "reason": reason})
        raise HTTPException(status_code=503, detail="execution engine not available")

    @app.post("/api/emergency/close")
    async def api_emergency_close() -> JSONResponse:
        execution = _require_execution()
        closed = await _call(execution.emergency_close_positions)
        trades = [_trade_dict(t) for t in (closed or [])]
        log.warning("EMERGENCY CLOSE via API: %d position(s) closed", len(trades))
        return _json({"ok": True, "closed": trades})

    @app.post("/api/emergency/resume")
    async def api_emergency_resume() -> JSONResponse:
        execution = getattr(ctx, "execution", None)
        risk = getattr(ctx, "risk", None)
        if execution is not None and hasattr(execution, "resume"):
            await _call(execution.resume)
        elif risk is not None and hasattr(risk, "set_emergency"):
            await _call(risk.set_emergency, False, "")
        else:
            raise HTTPException(status_code=503, detail="execution engine not available")
        log.info("emergency resumed via API")
        return _json({"ok": True, "emergency": False})

    # ---- settings --------------------------------------------------------
    @app.get("/api/settings")
    async def api_settings() -> JSONResponse:
        return _json(_settings().public())

    @app.put("/api/settings")
    async def api_settings_put(patch: dict[str, Any] = Body(...)) -> JSONResponse:
        if not isinstance(patch, dict) or not patch:
            raise HTTPException(status_code=400, detail="empty settings patch")
        secretish = sorted(k for k in patch if _SECRET_KEY_RE.search(str(k)))
        if secretish:
            raise HTTPException(status_code=400, detail="secrets cannot be set through the API: " + ", ".join(secretish))
        unknown = sorted(str(k) for k in patch if k not in _SETTINGS_KEYS)
        if unknown:
            raise HTTPException(status_code=400, detail="unknown settings keys: " + ", ".join(unknown))
        settings = _settings()
        before = {f.name: getattr(settings, f.name) for f in fields(Settings)}
        try:
            changed = settings.update(patch)
        except (ValueError, TypeError) as exc:
            for name, value in before.items():  # keep the live settings consistent
                setattr(settings, name, value)
            raise HTTPException(status_code=400, detail=redact(str(exc)) or "invalid settings") from exc
        saved = True
        try:
            save_settings(settings, cfg_path)
        except OSError:
            saved = False
            log.exception("could not persist settings to %s", cfg_path)
        public = settings.public()
        await _publish(SETTINGS, public)
        if changed:
            log.info("settings changed via API: %s", ", ".join(changed))
        return _json({"ok": True, "changed": changed, "saved": saved, "settings": public})

    # ---- trades / stats / balances ---------------------------------------
    @app.get("/api/trades")
    async def api_trades(limit: int = Query(200, ge=1, le=5000)) -> JSONResponse:
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        execution = getattr(ctx, "execution", None)
        if execution is not None and hasattr(execution, "trades"):
            try:
                for t in execution.trades(limit) or []:
                    d = _trade_dict(t)
                    seen.add(str(d.get("id")))
                    out.append(d)
            except Exception:  # noqa: BLE001
                log.exception("execution.trades failed")
        db = getattr(ctx, "db", None)
        if len(out) < limit and db is not None and hasattr(db, "list_trades"):
            try:
                for row in db.list_trades(limit) or []:
                    d = _jsonable(row)
                    if isinstance(d, dict) and d.get("id") and str(d["id"]) not in seen:
                        seen.add(str(d["id"]))
                        out.append(d)
            except Exception:  # noqa: BLE001
                log.exception("db.list_trades failed")
        if db is not None and hasattr(db, "journal_summary"):
            try:
                journal = db.journal_summary()
            except Exception:  # noqa: BLE001
                log.exception("db.journal_summary failed")
                journal = {}
            for d in out:
                j = journal.get(str(d.get("id")))
                d["journal"] = j or {"note": "", "screenshots": 0, "analyzed": 0}
        return _json(out[:limit])

    # ---- trade journal: notes, screenshots, Claude review ------------------
    def _data_dir() -> Path:
        settings = _settings()
        db_path = str(getattr(settings, "db_path", "") or "")
        if db_path and db_path != ":memory:":
            return Path(db_path).expanduser().parent
        return Path("data")

    def _require_db() -> Any:
        db = getattr(ctx, "db", None)
        if db is None or not hasattr(db, "journal_summary"):
            raise HTTPException(status_code=503, detail="database not available")
        return db

    def _trade_or_404(trade_id: str) -> dict[str, Any]:
        execution = getattr(ctx, "execution", None)
        if execution is not None and hasattr(execution, "trade"):
            t = execution.trade(trade_id)
            if t is not None:
                return _trade_dict(t)
        db = getattr(ctx, "db", None)
        if db is not None and hasattr(db, "get_trade"):
            d = db.get_trade(trade_id)
            if isinstance(d, dict):
                return _jsonable(d)
        raise HTTPException(status_code=404, detail="trade not found")

    def _screenshot_dict(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "trade_id": row["trade_id"],
            "url": f"/api/screenshots/{int(row['id'])}",
            "media_type": row.get("media_type"),
            "created_ts": row.get("created_ts"),
            "analysis": row.get("analysis"),
            "analysis_ts": row.get("analysis_ts"),
            "analysis_model": row.get("analysis_model"),
        }

    def _analysis_available() -> bool:
        return analysis.available(getattr(ctx, "secrets", None))

    @app.get("/api/trades/{trade_id}/journal")
    async def api_trade_journal(trade_id: str) -> JSONResponse:
        _trade_or_404(trade_id)
        db = _require_db()
        return _json({
            "trade_id": trade_id,
            "note": db.get_trade_note(trade_id),
            "screenshots": [_screenshot_dict(r) for r in db.list_screenshots(trade_id)],
            "analysis_available": _analysis_available(),
            "analysis_model": str(getattr(_settings(), "analysis_model", "") or ""),
        })

    @app.post("/api/trades/{trade_id}/note")
    async def api_trade_note(trade_id: str, body: NoteBody) -> JSONResponse:
        _trade_or_404(trade_id)
        db = _require_db()
        db.set_trade_note(trade_id, body.note)
        return _json({"ok": True, "trade_id": trade_id, "note": db.get_trade_note(trade_id)})

    @app.post("/api/trades/{trade_id}/screenshot")
    async def api_trade_screenshot(trade_id: str, file: UploadFile = File(...)) -> JSONResponse:
        _trade_or_404(trade_id)
        db = _require_db()
        media_type = (file.content_type or "").lower()
        if media_type not in analysis.MEDIA_TYPES:
            return _error(400, f"only PNG/JPEG/WEBP/GIF screenshots are accepted (got {media_type or 'unknown'})")
        data = await file.read()
        if not data:
            return _error(400, "empty file")
        if len(data) > analysis.MAX_IMAGE_BYTES:
            return _error(413, "screenshot larger than 8 MB")
        ext = {"image/png": "png", "image/jpeg": "jpg", "image/webp": "webp", "image/gif": "gif"}[media_type]
        folder = _data_dir() / "screenshots"
        folder.mkdir(parents=True, exist_ok=True)
        safe_id = re.sub(r"[^A-Za-z0-9_.-]", "_", trade_id)[:80]
        path = folder / f"{safe_id}_{int(time.time() * 1000)}.{ext}"
        path.write_bytes(data)
        sid = db.add_screenshot(trade_id, str(path), media_type)
        row = db.get_screenshot(sid)
        log.info("screenshot %d stored for trade %s (%d bytes)", sid, trade_id, len(data))
        return _json(_screenshot_dict(row) if row else {"id": sid})

    @app.get("/api/screenshots/{screenshot_id}")
    async def api_screenshot(screenshot_id: int) -> Response:
        db = _require_db()
        row = db.get_screenshot(screenshot_id)
        if row is None or not Path(row["path"]).exists():
            raise HTTPException(status_code=404, detail="screenshot not found")
        return FileResponse(row["path"], media_type=row.get("media_type") or "application/octet-stream")

    @app.post("/api/screenshots/{screenshot_id}/analyze")
    async def api_screenshot_analyze(screenshot_id: int) -> JSONResponse:
        db = _require_db()
        row = db.get_screenshot(screenshot_id)
        if row is None:
            raise HTTPException(status_code=404, detail="screenshot not found")
        if not _analysis_available():
            return _error(409, "NOT AVAILABLE: добавь ANTHROPIC_API_KEY в .env, чтобы включить анализ скриншотов")
        path = Path(row["path"])
        if not path.exists():
            raise HTTPException(status_code=404, detail="screenshot file is missing")
        trade = _trade_or_404(row["trade_id"])
        model = str(getattr(_settings(), "analysis_model", "") or "claude-opus-5-5")
        try:
            text = await analysis.analyze_screenshot(
                getattr(ctx, "secrets", None), model, trade, path.read_bytes(),
                row.get("media_type") or "image/png", note=db.get_trade_note(row["trade_id"]),
                client=getattr(ctx, "analysis_client", None),
            )
        except RuntimeError as exc:
            return _error(502, str(exc))
        db.set_screenshot_analysis(screenshot_id, text, model)
        fresh = db.get_screenshot(screenshot_id)
        return _json(_screenshot_dict(fresh) if fresh else {"id": screenshot_id, "analysis": text})

    @app.get("/api/shadow")
    async def api_shadow(limit: int = Query(200, ge=1, le=5000)) -> JSONResponse:
        """Shadow trades (virtual, real book, no orders) + strategy statistics."""
        shadow = getattr(ctx, "shadow", None)
        if shadow is None:
            return _json({"enabled": False, "trades": [], "summary": {}})
        try:
            ml = getattr(ctx, "metalabel", None)
            learning = ml.snapshot() if ml is not None and hasattr(ml, "snapshot") else None
            return _json({"enabled": bool(shadow.enabled), "trades": shadow.trades(limit), "summary": shadow.summary(),
                          "learning": learning})
        except Exception:  # noqa: BLE001
            log.exception("shadow listing failed")
            return _error(500, "shadow listing failed")

    @app.post("/api/stats/period")
    async def api_stats_new_period() -> JSONResponse:
        """Stats count virtual trades from now on (history stays in the DB)."""
        shadow = getattr(ctx, "shadow", None)
        if shadow is None or not hasattr(shadow, "new_period"):
            return _error(409, "shadow trading is not running")
        try:
            return _json({"ok": True, "period_start_ts": shadow.new_period()})
        except Exception:  # noqa: BLE001
            log.exception("new stats period failed")
            return _error(500, "new stats period failed")

    # ---- Phase 3: market context + narrow pool --------------------------------
    @app.get("/api/context")
    async def api_context() -> JSONResponse:
        """Macro calendar, news / Telegram, US markets, BTC/ETH regime, derivatives, sources (Market tab)."""
        hub = getattr(ctx, "context", None)
        pool = getattr(ctx, "pool", None)
        if hub is None:
            return _json({"enabled": False, "reason": "контекст выключен (context_enabled = false) или не создан",
                          "pool": pool.snapshot() if pool is not None and hasattr(pool, "snapshot") else None})
        try:
            return _json({"enabled": True, **hub.snapshot()})
        except Exception:  # noqa: BLE001
            log.exception("context snapshot failed")
            return _error(500, "context snapshot failed")

    @app.get("/api/pool")
    async def api_pool() -> JSONResponse:
        pool = getattr(ctx, "pool", None)
        if pool is None or not hasattr(pool, "snapshot"):
            return _json({"enabled": False})
        try:
            return _json(pool.snapshot())
        except Exception:  # noqa: BLE001
            log.exception("pool snapshot failed")
            return _error(500, "pool snapshot failed")

    @app.get("/api/trades/summary")
    async def api_trades_summary() -> JSONResponse:
        """Real (PAPER/LIVE) trade statistics: expected vs actual NET, win rate, P&L."""
        db = getattr(ctx, "db", None)
        rows: list[dict[str, Any]] = []
        if db is not None and hasattr(db, "list_trades"):
            try:
                rows = [r for r in (db.list_trades(5000) or []) if isinstance(r, dict)]
            except Exception:  # noqa: BLE001
                log.exception("db.list_trades failed")
        return _json(_trade_summary(rows))

    @app.get("/api/diagnostics")
    async def api_diagnostics() -> JSONResponse:
        """Signal funnel + "why there are no signals" (engine.diagnostics)."""
        pipeline = getattr(ctx, "pipeline", None)
        if pipeline is None or not hasattr(pipeline, "diagnostics"):
            return _json({"error": "diagnostics unavailable", "funnel": {}, "exchanges": {}, "why": ["Конвейер не запущен."]})
        try:
            d = pipeline.diagnostics()
            shadow = getattr(ctx, "shadow", None)
            if shadow is not None and isinstance(d.get("funnel"), dict):
                d["funnel"]["shadow_opened"] = int(getattr(shadow, "opened", 0))
                d["funnel"]["shadow_closed"] = int(getattr(shadow, "closed", 0))
                d["funnel"]["shadow_skipped"] = int(sum((getattr(shadow, "skipped", {}) or {}).values()))
            return _json(d)
        except Exception:  # noqa: BLE001
            log.exception("diagnostics failed")
            return _json({"error": "diagnostics failed", "funnel": {}, "exchanges": {}, "why": ["Ошибка диагностики, смотри лог."]})

    @app.get("/api/stats")
    async def api_stats() -> JSONResponse:
        stats = getattr(ctx, "stats", None)
        if stats is None or not hasattr(stats, "summary"):
            return _json([])
        try:
            return _json(list(stats.summary() or []))
        except Exception:  # noqa: BLE001
            log.exception("stats.summary failed")
            return _json([])

    @app.get("/api/balances")
    async def api_balances() -> JSONResponse:
        settings = _settings()
        mode = _mode_value(settings)
        exec_adapters = getattr(ctx, "exec_adapters", None) or {}
        adapters: dict[str, Any] = {}
        for key, value in exec_adapters.items():
            key_value = key.value if isinstance(key, Enum) else str(key)
            if key_value == mode:
                adapters = dict(value or {})
                break
        balances: dict[str, Any] = {}
        errors: dict[str, str] = {}
        for name, adapter in adapters.items():
            try:
                raw = await adapter.fetch_balances()
                balances[name] = {str(asset): _jsonable(b) for asset, b in (raw or {}).items()}
            except Exception as exc:  # noqa: BLE001
                errors[name] = redact(f"{type(exc).__name__}: {exc}")
                log.warning("fetch_balances failed for %s: %s", name, errors[name])
        return _json({"mode": mode, "balances": balances, "errors": errors})

    # ---- websocket -------------------------------------------------------
    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await hub.handle(websocket)

    return app
