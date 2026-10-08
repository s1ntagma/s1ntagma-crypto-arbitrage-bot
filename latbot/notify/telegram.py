"""Telegram notifier - alerts, trade / emergency / link notices and owner commands.

Discipline (carried over from the previous bot):

* only the owner chat is served: ``secrets.telegram_chat_id`` or the first
  private chat that sends ``/start`` (persisted in db kv ``telegram_chat_id``);
  every other chat is ignored,
* one alert per opportunity id - later updates EDIT the message (at most one
  edit per ``EDIT_THROTTLE_S``), a final edit says EXPIRED / CLOSED; the first
  alert fires on the VALID transition (never while DETECTED / VALIDATING) and
  moves the opportunity VALID -> NOTIFIED,
* only important events: alerts with NET >= ``telegram_profit_threshold_pct``
  that are executable, trade open / close, emergency on / off, exchange link
  lost / restored (``LINK_GRACE_S`` grace, once per transition), start / stop,
* HTML parse mode with a plain-text fallback when Telegram rejects the markup,
  the backlog is skipped on start (``getUpdates(offset=-1)``), updates older
  than the start time are ignored, the offset is persisted in kv
  ``telegram_offset``,
* the bot token never appears in logs: only Telegram method names are logged,
* every failure is caught and logged - the notifier never crashes the app.

Replies are in Russian; the alert is the Russian trading PLAN
(``engine.explain.build_plan``) plus a HUMAN / BOT line.
"""
from __future__ import annotations

import asyncio
import html
import logging
import re
from typing import Any

from .. import events
from ..buildinfo import build_stamp
from ..clock import Clock
from ..config import Secrets, Settings, save_settings
from ..engine.explain import build_plan, fmt_money
from ..links import split_symbol
from ..logging_setup import register_secret
from ..models import ExchangeHealth, Light, Opportunity, OppState, TradeRecord

try:  # aiohttp is a hard dependency of the app, but the notifier must import without it
    import aiohttp
except ImportError:  # pragma: no cover
    aiohttp = None  # type: ignore[assignment]


log = logging.getLogger(__name__)

API_HOST = "https://api.telegram.org"
EDIT_THROTTLE_S = 3.0
LINK_GRACE_S = 30.0
POLL_TIMEOUT_S = 20
POLL_IDLE_S = 0.2
MAX_TEXT = 3900
_KEEP_MESSAGES = 1000
_KEEP_TRADES = 500
_LIGHT = {Light.GREEN: "🟢", Light.YELLOW: "🟡", Light.RED: "🔴"}
# Not yet confirmed on the real book: never alert these (OPP_NEW arrives while DETECTED).
_NOT_YET_VALIDATED = frozenset({OppState.DETECTED, OppState.VALIDATING})
_TAG_RE = re.compile(r"<[^>]+>")

HELP_TEXT = (
    "Команды:\n"
    "/status - состояние бота\n"
    "/stop - аварийная остановка (с подтверждением)\n"
    "/resume - снять аварийную остановку\n"
    "/human <сек> - время ручного исполнения (1-120)\n"
    "/threshold <%> - порог NET для Telegram (0-50)\n"
    "/mode - текущий режим (PAPER/LIVE)\n"
    "/diag - диагностика: почему нет сигналов\n"
    "/help - эта справка"
)


def _strip_tags(text: str) -> str:
    return html.unescape(_TAG_RE.sub("", text))


_money = fmt_money  # ``$102,140`` / ``€0.5234`` / ``+$4.82`` - shared with the plan


def _num(text: str) -> float | None:
    try:
        return float(text.strip().replace(",", "."))
    except (TypeError, ValueError):
        return None


def _is_parse_error(data: dict[str, Any] | None) -> bool:
    desc = str((data or {}).get("description", "")).lower()
    return "parse" in desc or "entit" in desc or "tag" in desc


def _is_button_error(data: dict[str, Any] | None) -> bool:
    desc = str((data or {}).get("description", "")).lower()
    return "button" in desc or "reply_markup" in desc or "url" in desc


def _not_modified(data: dict[str, Any] | None) -> bool:
    return "not modified" in str((data or {}).get("description", "")).lower()


class TelegramNotifier:
    """Owner-only Telegram bot: opportunity alerts, notices and commands."""

    def __init__(
        self,
        settings: Settings,
        secrets: Secrets,
        bus: events.EventBus,
        opps: Any,
        execution: Any | None,
        risk: Any | None,
        clock: Clock,
        session: Any | None = None,
        db: Any | None = None,
        config_path: str = "config.json",
        pipeline: Any | None = None,
    ) -> None:
        self._settings = settings
        self._bus = bus
        self._opps = opps
        self._execution = execution
        self._pipeline = pipeline
        self._risk = risk
        self._clock = clock
        self._session = session
        self._own_session = False
        self._db = db
        self._config_path = config_path
        self._token = str(getattr(secrets, "telegram_bot_token", "") or "").strip()
        self._base = f"{API_HOST}/bot{self._token}" if self._token else ""
        if self._token:
            register_secret(self._token)
        chat = str(getattr(secrets, "telegram_chat_id", "") or "").strip()
        stored = self._kv_get("telegram_chat_id", None)
        self._chat_id: str | None = chat or (str(stored) if stored else None)
        try:
            self._offset = int(self._kv_get("telegram_offset", 0) or 0)
        except (TypeError, ValueError):
            self._offset = 0
        self._started = False
        self._started_ts = 0.0
        self._poll_task: asyncio.Task[None] | None = None
        self._unsubs: list[Any] = []
        # alerts
        self._messages: dict[str, int] = {}  # opp id -> telegram message id
        self._alert_text: dict[str, str] = {}
        self._last_edit: dict[str, float] = {}
        self._final: set[str] = set()
        # trades / emergency / links
        self._trade_seen: dict[str, str] = {}
        self._health: dict[str, ExchangeHealth] = {}
        self._link_up: dict[str, bool] = {}
        self._down_since: dict[str, float] = {}
        self._down_notified: set[str] = set()
        self._emergency_state: bool | None = bool(getattr(risk, "emergency", False)) if risk is not None else None

    # ------------------------------------------------------------------ #
    # small helpers
    # ------------------------------------------------------------------ #
    @property
    def enabled(self) -> bool:
        return bool(self._token)

    @property
    def chat_id(self) -> str | None:
        return self._chat_id

    @property
    def offset(self) -> int:
        return self._offset

    def message_id_for(self, opp_id: str) -> int | None:
        return self._messages.get(opp_id)

    def _kv_get(self, key: str, default: Any = None) -> Any:
        if self._db is None or not hasattr(self._db, "kv_get"):
            return default
        try:
            return self._db.kv_get(key, default)
        except Exception:  # noqa: BLE001
            log.exception("kv_get %s failed", key)
            return default

    def _kv_set(self, key: str, value: Any) -> None:
        if self._db is None or not hasattr(self._db, "kv_set"):
            return
        try:
            self._db.kv_set(key, value)
        except Exception:  # noqa: BLE001
            log.exception("kv_set %s failed", key)

    def _set_offset(self, offset: int) -> None:
        if offset > self._offset:
            self._offset = offset
            self._kv_set("telegram_offset", offset)

    def _display(self, exchange: str) -> str:
        cfg = self._settings.exchange(exchange)
        return cfg.display if cfg is not None else str(exchange).capitalize()

    def _names(self) -> dict[str, str]:
        return {e.name: e.display for e in self._settings.exchanges}

    def _mode_name(self) -> str:
        return self._settings.mode().value.upper()

    def _base_url(self) -> str:
        public = str(self._settings.public_url or "").strip().rstrip("/")
        return public or f"http://{self._settings.host}:{self._settings.port}"

    def _emergency_active(self) -> bool:
        if self._risk is not None:
            return bool(getattr(self._risk, "emergency", False))
        return bool(self._emergency_state)

    @staticmethod
    def _side(direction: int) -> str:
        return "LONG" if direction > 0 else "SHORT"

    def _prune(self) -> None:
        while len(self._messages) > _KEEP_MESSAGES:
            old = next(iter(self._messages))
            for d in (self._messages, self._alert_text, self._last_edit):
                d.pop(old, None)
            self._final.discard(old)
        while len(self._trade_seen) > _KEEP_TRADES:
            self._trade_seen.pop(next(iter(self._trade_seen)), None)

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    async def start(self, poll: bool = True) -> None:
        """Subscribe to the bus, start the long-poll task, announce start.
        No-op without a token.  ``poll=False`` skips the getUpdates task (tests)."""
        if not self._token:
            log.info("telegram disabled (no TELEGRAM_BOT_TOKEN)")
            return
        if self._started:
            return
        if self._session is None:
            if aiohttp is None:
                log.error("telegram disabled: aiohttp is not installed")
                return
            self._session = aiohttp.ClientSession()
            self._own_session = True
        self._started = True
        self._started_ts = self._clock.wall()
        self._unsubs = [
            self._bus.subscribe(events.OPP_NEW, self._on_opp),
            self._bus.subscribe(events.OPP_UPDATE, self._on_opp),
            self._bus.subscribe(events.OPP_STATE, self._on_opp_state),
            self._bus.subscribe(events.TRADE_UPDATE, self._on_trade),
            self._bus.subscribe(events.EMERGENCY, self._on_emergency),
            self._bus.subscribe(events.HEALTH, self._on_health),
        ]
        if poll:
            self._poll_task = asyncio.create_task(self._poll_loop(), name="latbot-telegram")
        log.info("telegram started (owner chat %s)", "set" if self._chat_id else "not yet known")
        await self.send(f"🚀 latbot запущен ({self._mode_name()})")

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        try:
            await self.send("⏹ latbot остановлен")
        except Exception:  # noqa: BLE001
            log.exception("telegram stop notice failed")
        task, self._poll_task = self._poll_task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        for unsub in self._unsubs:
            try:
                unsub()
            except Exception:  # noqa: BLE001
                pass
        self._unsubs = []
        if self._own_session and self._session is not None:
            try:
                await self._session.close()
            except Exception:  # noqa: BLE001
                log.debug("telegram session close failed", exc_info=True)
            self._session = None
            self._own_session = False
        log.info("telegram stopped")

    # ------------------------------------------------------------------ #
    # HTTP (the token lives only in the URL; never log it)
    # ------------------------------------------------------------------ #
    async def _api(self, method: str, **params: Any) -> dict[str, Any] | None:
        """POST ``method``; returns the decoded response (``ok`` may be False) or None."""
        if not self._base or self._session is None:
            return None
        timeout_s = float(params.pop("_timeout", 15.0))
        kwargs: dict[str, Any] = {"json": params}
        if aiohttp is not None and isinstance(self._session, aiohttp.ClientSession):
            kwargs["timeout"] = aiohttp.ClientTimeout(total=timeout_s)
        try:
            async with self._session.post(f"{self._base}/{method}", **kwargs) as resp:
                data = await resp.json(content_type=None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("telegram %s failed: %s", method, type(exc).__name__)
            return None
        if not isinstance(data, dict):
            log.warning("telegram %s: unexpected response", method)
            return None
        if not data.get("ok"):
            log.warning("telegram %s: %s", method, str(data.get("description", "error"))[:200])
        return data

    @staticmethod
    def _ok(data: dict[str, Any] | None) -> bool:
        return bool(data and data.get("ok"))

    async def send(self, text: str, buttons: list[list[dict[str, Any]]] | None = None) -> int | None:
        """Send ``text`` (HTML) to the owner chat; returns the message id or None.
        Falls back to plain text / no buttons when Telegram rejects them."""
        if not self._token or not self._chat_id:
            return None
        params: dict[str, Any] = {
            "chat_id": self._chat_id,
            "text": text[:MAX_TEXT],
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if buttons:
            params["reply_markup"] = {"inline_keyboard": buttons}
        try:
            data = await self._api("sendMessage", **params)
            if not self._ok(data) and data is not None and _is_parse_error(data):
                params.pop("parse_mode", None)
                params["text"] = _strip_tags(text)[:MAX_TEXT]
                data = await self._api("sendMessage", **params)
            if not self._ok(data) and data is not None and _is_button_error(data) and "reply_markup" in params:
                params.pop("reply_markup", None)
                data = await self._api("sendMessage", **params)
        except Exception:  # noqa: BLE001
            log.exception("telegram send failed")
            return None
        if not self._ok(data):
            return None
        result = data.get("result") if data else None
        mid = result.get("message_id") if isinstance(result, dict) else None
        return int(mid) if mid is not None else None

    async def _edit(self, message_id: int, text: str, buttons: list[list[dict[str, Any]]] | None = None) -> bool:
        if not self._token or not self._chat_id:
            return False
        params: dict[str, Any] = {
            "chat_id": self._chat_id,
            "message_id": message_id,
            "text": text[:MAX_TEXT],
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if buttons:
            params["reply_markup"] = {"inline_keyboard": buttons}
        try:
            data = await self._api("editMessageText", **params)
            if not self._ok(data) and data is not None and _is_parse_error(data):
                params.pop("parse_mode", None)
                params["text"] = _strip_tags(text)[:MAX_TEXT]
                data = await self._api("editMessageText", **params)
            if not self._ok(data) and data is not None and _is_button_error(data) and "reply_markup" in params:
                params.pop("reply_markup", None)
                data = await self._api("editMessageText", **params)
        except Exception:  # noqa: BLE001
            log.exception("telegram edit failed")
            return False
        return self._ok(data) or _not_modified(data)

    # ------------------------------------------------------------------ #
    # alerts
    # ------------------------------------------------------------------ #
    def should_alert(self, opp: Opportunity) -> bool:
        s = self._settings
        if not opp.is_active:
            return False
        if opp.state in _NOT_YET_VALIDATED:
            # OPP_NEW is published while the opp is still DETECTED; the pipeline
            # moves it VALIDATING -> VALID right after.  Alert only on a state the
            # real book has confirmed (the alert then fires from the VALID OPP_STATE).
            return False
        if opp.econ.net_pct < s.telegram_profit_threshold_pct:
            return False
        if s.telegram_require_executable and opp.bot.light is Light.RED and opp.human.light is Light.RED:
            return False
        if s.alert_human_only and opp.human.light is Light.RED:
            return False
        quality = getattr(opp, "quality", None)
        if isinstance(quality, dict) and quality and quality.get("enforced", True) and not quality.get("passed", False):
            return False  # Phase 3: only signals that passed the quality check (score + no veto); simple mode: info only
        if opp.id in self._messages or opp.telegram_message_id is not None:
            return False
        return True

    def format_alert(self, opp: Opportunity, now_mono: float) -> str:
        """The Russian trading plan (``build_plan``) as the alert card (HTML;
        ``<b>`` on the header and the BUY line only), then a short
        ``HUMAN 🟡 ~7 с · BOT 🟢 0.4 с`` line and the update time.  The plan
        is rebuilt here so "держится" is current at every edit."""
        plan = build_plan(opp, self._settings, self._names(), now_mono, self._clock.wall())
        esc = html.escape
        lights = (
            f"HUMAN {_LIGHT.get(opp.human.light, '')} ~{opp.human.est_execution_s:.0f} с · "
            f"BOT {_LIGHT.get(opp.bot.light, '')} {opp.bot.est_execution_s:.1f} с"
        )
        lines = [
            f"<b>{esc(plan['header'])}</b>",
            esc(plan["movers"]),
            f"<b>{esc(plan['buy'])}</b>",
            esc(plan["sell"]),
            esc(plan["timing"]),
            esc(plan["caveat"]),
            lights,
            esc(_quality_line(opp)),
            esc(plan["updated"]),
        ]
        return "\n".join(line for line in lines if line)

    def _buttons(self, opp: Opportunity, final: bool = False) -> list[list[dict[str, Any]]]:
        rows: list[list[dict[str, Any]]] = []
        if opp.pair_url and not final:
            rows.append([{"text": f"OPEN ON {self._display(opp.target_exchange)}", "url": opp.pair_url}])
        rows.append([{"text": "OPEN OPPORTUNITY", "url": f"{self._base_url()}/#opp/{opp.id}"}])
        return rows

    async def _on_opp(self, opp: Opportunity) -> None:
        try:
            await self._handle_opp(opp)
        except Exception:  # noqa: BLE001
            log.exception("telegram alert handling failed")

    async def _handle_opp(self, opp: Opportunity) -> None:
        now = self._clock.mono()
        mid = self._messages.get(opp.id)
        if mid is not None:
            if opp.id in self._final or not opp.is_active:
                return
            if now - self._last_edit.get(opp.id, -1e9) < EDIT_THROTTLE_S:
                return
            text = self.format_alert(opp, now)
            if await self._edit(mid, text, self._buttons(opp)):
                self._alert_text[opp.id] = text
                self._last_edit[opp.id] = now
            return
        if not self.should_alert(opp):
            return
        text = self.format_alert(opp, now)
        mid = await self.send(text, self._buttons(opp))
        if mid is None:
            return
        self._messages[opp.id] = mid
        self._alert_text[opp.id] = text
        self._last_edit[opp.id] = now
        self._prune()
        diag = getattr(self._pipeline, "diag", None)
        if diag is not None and hasattr(diag, "alert"):
            diag.alert()
        wall = self._clock.wall()
        opp.telegram_message_id = mid
        opp.notified_ts = wall
        managed = self._opps.get(opp.id) if self._opps is not None and hasattr(self._opps, "get") else None
        if managed is not None and managed is not opp:
            managed.telegram_message_id = mid
            managed.notified_ts = wall
        target = managed if managed is not None else opp
        if target.state is OppState.VALID and hasattr(self._opps, "transition"):
            try:
                await self._opps.transition(target.id, OppState.NOTIFIED, "telegram alert sent")
            except Exception as exc:  # noqa: BLE001
                log.debug("NOTIFIED transition skipped for %s: %s", target.id, exc)
        log.info("telegram alert %s %s %s net %+.2f%%", opp.id, opp.asset, opp.target_exchange, opp.econ.net_pct)

    async def _final_edit(self, opp: Opportunity, suffix: str) -> None:
        mid = self._messages.get(opp.id)
        if mid is None or opp.id in self._final:
            return
        self._final.add(opp.id)
        base = self._alert_text.get(opp.id) or self.format_alert(opp, self._clock.mono())
        text = f"{base}\n{suffix}"
        if await self._edit(mid, text, self._buttons(opp, final=True)):
            self._alert_text[opp.id] = text
        self._last_edit[opp.id] = self._clock.mono()

    async def _on_opp_state(self, payload: Any) -> None:
        try:
            if not isinstance(payload, dict):
                return
            opp = payload.get("opportunity")
            if opp is None:
                return
            to = payload.get("to")
            to = str(getattr(to, "value", to) or "")
            if to == OppState.VALID.value:
                # First validation on the real book (DETECTED -> VALIDATING -> VALID
                # in Pipeline.evaluate): this is where a new opportunity is alerted,
                # so the VALID -> NOTIFIED transition is recorded in its history.
                if opp.id not in self._messages:
                    await self._handle_opp(opp)
                return
            if opp.id not in self._messages or opp.id in self._final:
                return
            reason = str(payload.get("reason") or "")
            if to == OppState.EXPIRED.value:
                dur = opp.actual_duration_s
                if dur is None:
                    dur = opp.age_s(self._clock.mono())
                await self._final_edit(opp, f"… истекла через {dur:.1f} с")
            elif to in (OppState.CANCELLED.value, OppState.FAILED.value):
                word = "отменена" if to == OppState.CANCELLED.value else "не удалась"
                await self._final_edit(opp, f"… {word}: {html.escape(reason)}" if reason else f"… {word}")
            elif to == OppState.CLOSED.value and not opp.execution_id:
                await self._final_edit(opp, "… закрыта")
        except Exception:  # noqa: BLE001
            log.exception("telegram OPP_STATE handling failed")

    # ------------------------------------------------------------------ #
    # trades / emergency / links
    # ------------------------------------------------------------------ #
    async def _on_trade(self, trade: TradeRecord) -> None:
        try:
            await self._handle_trade(trade)
        except Exception:  # noqa: BLE001
            log.exception("telegram TRADE_UPDATE handling failed")

    async def _handle_trade(self, trade: TradeRecord) -> None:
        tid = trade.id
        state = trade.state
        seen = self._trade_seen.get(tid)
        where = f"{html.escape(trade.asset)} {self._side(trade.direction)} на {html.escape(self._display(trade.exchange))}"
        mode = getattr(trade.mode, "value", str(trade.mode)).upper()
        quote = split_symbol(trade.symbol or "")[1]
        opp = self._opps.get(trade.opportunity_id) if self._opps is not None and hasattr(self._opps, "get") and trade.opportunity_id else None
        if state is OppState.EXECUTING:
            if seen is None:
                self._trade_seen[tid] = "open"
                self._prune()
                await self.send(f"📈 Сделка открыта: {where}, {_money(trade.capital_quote, quote)} ({mode})")
        elif state is OppState.CLOSED:
            if seen == "closed":
                return
            self._trade_seen[tid] = "closed"
            pnl = float(trade.realized_pnl_quote or 0.0)
            net = trade.actual_net_pct
            net_s = f"{net:+.2f}%" if net is not None else "n/a"
            icon = "🟢" if pnl >= 0 else "🔴"
            await self.send(f"{icon} Сделка закрыта: {where}, NET {net_s} ({_money(pnl, quote, signed=True)}) ({mode})")
            if opp is not None:
                realized = f"{net:+.1f}%" if net is not None else _money(pnl, quote, signed=True)
                await self._final_edit(opp, f"… закрыта: факт {realized}")
        elif state is OppState.FAILED:
            if seen == "failed":
                return
            self._trade_seen[tid] = "failed"
            err = html.escape(str(trade.error or "неизвестная ошибка"))
            await self.send(f"❌ Сделка не удалась: {where}: {err} ({mode})")
            if opp is not None:
                await self._final_edit(opp, f"… не удалась: {err}")
        elif state is OppState.CANCELLED:
            if seen == "cancelled":
                return
            self._trade_seen[tid] = "cancelled"
            err = html.escape(str(trade.error or ""))
            await self.send(f"⚠️ Сделка отменена: {where}: {err} ({mode})")

    async def _on_emergency(self, payload: Any) -> None:
        try:
            if not isinstance(payload, dict):
                return
            active = bool(payload.get("active"))
            reason = str(payload.get("reason") or "")
            if self._emergency_state is not None and self._emergency_state == active:
                return
            self._emergency_state = active
            if active:
                await self.send(f"🛑 EMERGENCY STOP включён: {html.escape(reason) or 'без причины'}")
            else:
                await self.send("✅ EMERGENCY STOP снят")
        except Exception:  # noqa: BLE001
            log.exception("telegram EMERGENCY handling failed")

    async def _on_health(self, h: ExchangeHealth) -> None:
        try:
            if not isinstance(h, ExchangeHealth):
                return
            self._health[h.exchange] = h
            now = self._clock.mono()
            up = bool(h.ws_connected) and not h.stale and not h.maintenance
            prev = self._link_up.get(h.exchange)
            self._link_up[h.exchange] = up
            if up:
                if h.exchange in self._down_notified:
                    self._down_notified.discard(h.exchange)
                    await self.send(f"✅ Связь с биржей {html.escape(self._display(h.exchange))} восстановлена")
                self._down_since.pop(h.exchange, None)
            elif prev is not False or h.exchange not in self._down_since:
                self._down_since.setdefault(h.exchange, now)
            await self.check_links(now)
        except Exception:  # noqa: BLE001
            log.exception("telegram HEALTH handling failed")

    async def check_links(self, now_mono: float) -> list[str]:
        """Notify once per lost link after the grace period.  Returns the exchanges notified."""
        notified: list[str] = []
        for ex, since in list(self._down_since.items()):
            if ex in self._down_notified or now_mono - since < LINK_GRACE_S:
                continue
            self._down_notified.add(ex)
            notified.append(ex)
            try:
                await self.send(f"⚠️ Связь с биржей {html.escape(self._display(ex))} потеряна")
            except Exception:  # noqa: BLE001
                log.exception("telegram link notice failed")
        return notified

    # ------------------------------------------------------------------ #
    # long poll
    # ------------------------------------------------------------------ #
    async def _skip_backlog(self) -> None:
        data = await self._api("getUpdates", offset=-1, timeout=0, _timeout=10.0)
        result = data.get("result") if self._ok(data) else None
        if isinstance(result, list) and result:
            last = result[-1].get("update_id")
            if isinstance(last, int):
                self._set_offset(last + 1)
                log.info("telegram backlog skipped")

    async def _poll_loop(self) -> None:
        backoff = 5.0
        try:
            await self._skip_backlog()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("telegram backlog skip failed")
        while True:
            try:
                data = await self._api(
                    "getUpdates",
                    offset=self._offset,
                    timeout=POLL_TIMEOUT_S,
                    allowed_updates=["message", "callback_query"],
                    _timeout=POLL_TIMEOUT_S + 15.0,
                )
                if not self._ok(data):
                    desc = str((data or {}).get("description", "")).lower()
                    if "conflict" in desc:
                        await asyncio.sleep(10.0)
                    else:
                        await asyncio.sleep(backoff)
                        backoff = min(30.0, backoff * 2.0)
                    continue
                backoff = 5.0
                for update in data.get("result") or []:
                    if not isinstance(update, dict):
                        continue
                    uid = update.get("update_id")
                    if isinstance(uid, int):
                        self._set_offset(uid + 1)
                    try:
                        await self.handle_update(update)
                    except asyncio.CancelledError:
                        raise
                    except Exception:  # noqa: BLE001
                        log.exception("telegram update handling failed")
                await self.check_links(self._clock.mono())
                await asyncio.sleep(POLL_IDLE_S)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("telegram poll loop error")
                await asyncio.sleep(5.0)

    # ------------------------------------------------------------------ #
    # updates: messages and callbacks
    # ------------------------------------------------------------------ #
    async def handle_update(self, update: dict[str, Any]) -> None:
        """Process one Telegram update (owner-only; never raises)."""
        try:
            if "message" in update:
                await self._on_message(update["message"])
            elif "callback_query" in update:
                await self._on_callback(update["callback_query"])
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("telegram update failed")

    async def _on_message(self, msg: Any) -> None:
        if not isinstance(msg, dict):
            return
        chat = msg.get("chat") or {}
        chat_id = str(chat.get("id", "") or "")
        text = str(msg.get("text") or "").strip()
        if not chat_id or not text:
            return
        if self._chat_id is None:
            if text.startswith("/start") and str(chat.get("type", "")) == "private":
                self._chat_id = chat_id
                self._kv_set("telegram_chat_id", chat_id)
                log.info("telegram owner chat registered")
            else:
                log.debug("telegram: message from a non-owner chat ignored")
                return
        elif chat_id != self._chat_id:
            log.debug("telegram: message from a non-owner chat ignored")
            return
        date = msg.get("date")
        if isinstance(date, (int, float)) and date < self._started_ts - 1.0:
            log.debug("telegram: stale message skipped")
            return
        await self._on_command(text)

    async def _on_command(self, text: str) -> None:
        parts = text.split()
        cmd = parts[0].lower().split("@", 1)[0]
        args = parts[1:]
        s = self._settings
        if cmd == "/start":
            await self.send(f"{HELP_TEXT}\n\n{self._status_text()}")
        elif cmd == "/status":
            await self.send(self._status_text())
        elif cmd == "/help":
            await self.send(HELP_TEXT)
        elif cmd == "/diag":
            await self.send(self._diag_text())
        elif cmd == "/mode":
            mode = s.mode().value
            what = "PAPER (бумажный)" if mode == "paper" else "LIVE (реальные ордера!)"
            await self.send(f"Режим: {what}. Переключение - только в терминале.")
        elif cmd == "/stop":
            buttons = [[
                {"text": "🛑 Да, остановить", "callback_data": "stop:yes"},
                {"text": "Отмена", "callback_data": "stop:no"},
            ]]
            await self.send("Остановить все AUTO-сделки и отменить ордера?", buttons)
        elif cmd == "/resume":
            await self._do_resume()
            await self.send("▶️ Возобновлено")
        elif cmd == "/human":
            v = _num(args[0]) if args else None
            if v is None or not (1.0 <= v <= 120.0):
                await self.send(f"Использование: /human <сек> (1-120). Сейчас: {s.human_execution_time_s:g} с")
                return
            await self._apply_setting("human_execution_time_s", v)
            await self.send(f"HUMAN_EXECUTION_TIME = {s.human_execution_time_s:g} с")
        elif cmd == "/threshold":
            v = _num(args[0]) if args else None
            if v is None or not (0.0 <= v <= 50.0):
                await self.send(f"Использование: /threshold <%> (0-50). Сейчас: {s.telegram_profit_threshold_pct:g}%")
                return
            await self._apply_setting("telegram_profit_threshold_pct", v)
            await self.send(f"Порог Telegram = {s.telegram_profit_threshold_pct:g}%")
        else:
            await self.send(HELP_TEXT)

    async def _apply_setting(self, key: str, value: float) -> None:
        changed = self._settings.update({key: value})
        if changed:
            try:
                save_settings(self._settings, self._config_path)
            except Exception:  # noqa: BLE001
                log.exception("save_settings failed after telegram command")
            try:
                await self._bus.publish(events.SETTINGS, self._settings.public())
            except Exception:  # noqa: BLE001
                log.exception("SETTINGS publish failed")
        log.info("telegram: %s = %s", key, value)

    async def _do_emergency_stop(self, reason: str = "telegram") -> bool:
        prev = self._emergency_state
        self._emergency_state = True  # our own notice replaces the bus one
        try:
            if self._execution is not None and hasattr(self._execution, "emergency_stop"):
                await self._execution.emergency_stop(reason=reason)
            elif self._risk is not None and hasattr(self._risk, "set_emergency"):
                await self._risk.set_emergency(True, reason)
            else:
                self._emergency_state = prev
                return False
            return True
        except Exception:  # noqa: BLE001
            self._emergency_state = prev
            log.exception("emergency stop from telegram failed")
            return False

    async def _do_resume(self) -> bool:
        prev = self._emergency_state
        self._emergency_state = False
        try:
            if self._execution is not None and hasattr(self._execution, "resume"):
                await self._execution.resume()
            elif self._risk is not None and hasattr(self._risk, "set_emergency"):
                await self._risk.set_emergency(False, "telegram")
            else:
                self._emergency_state = prev
                return False
            return True
        except Exception:  # noqa: BLE001
            self._emergency_state = prev
            log.exception("resume from telegram failed")
            return False

    async def _on_callback(self, cq: Any) -> None:
        if not isinstance(cq, dict):
            return
        cq_id = cq.get("id")
        msg = cq.get("message") or {}
        chat_id = str((msg.get("chat") or {}).get("id", "") or "")
        message_id = msg.get("message_id")
        data = str(cq.get("data") or "")
        if self._chat_id is None or chat_id != self._chat_id:
            log.debug("telegram: callback from a non-owner chat ignored")
            await self._api("answerCallbackQuery", callback_query_id=cq_id)
            return
        if data == "stop:yes":
            ok = await self._do_emergency_stop("telegram")
            await self._api("answerCallbackQuery", callback_query_id=cq_id, text="EMERGENCY STOP" if ok else "Ошибка")
            text = "🛑 EMERGENCY STOP включён" if ok else "❌ Не удалось включить EMERGENCY STOP"
            if message_id is not None:
                await self._edit(int(message_id), text)
            else:
                await self.send(text)
        elif data == "stop:no":
            await self._api("answerCallbackQuery", callback_query_id=cq_id, text="Отменено")
            if message_id is not None:
                await self._edit(int(message_id), "Отменено")
        else:
            await self._api("answerCallbackQuery", callback_query_id=cq_id)

    # ------------------------------------------------------------------ #
    # status
    # ------------------------------------------------------------------ #
    def _diag_text(self) -> str:
        """Funnel summary for /diag (plain text, numbers from Pipeline.diagnostics)."""
        pipeline = self._pipeline
        if pipeline is None or not hasattr(pipeline, "diagnostics"):
            return "Диагностика недоступна (конвейер не подключён)."
        try:
            d = pipeline.diagnostics()
        except Exception:  # noqa: BLE001
            log.exception("diagnostics failed")
            return "Диагностика: ошибка, смотри лог."
        f = d["funnel"]
        lines = [
            "🔎 Диагностика",
            f"Бирж подключено: {f['exchanges_connected']}/{f['exchanges_enabled']} · монет: {f['assets_scanned']} · тиков: {f['ticks_received']}",
            f"Движений: {f['episodes_detected']} · отброшено: {f['rejected_total']} · сигналов: {f['signals']} · возможностей: {f['opportunities']} · алертов: {f['alerts']}",
        ]
        dead = [n for n, x in d["exchanges"].items() if not x["connected"]]
        if dead:
            lines.append("Не подключены: " + ", ".join(dead))
        lines.append("")
        lines.extend(d["why"])
        return "\n".join(lines)

    def _status_text(self) -> str:
        s = self._settings
        now = self._clock.mono()
        active: list[Opportunity] = []
        best: Opportunity | None = None
        if self._opps is not None:
            try:
                active = list(self._opps.active())
                best = self._opps.best(now)
            except Exception:  # noqa: BLE001
                log.exception("status: opportunity lookup failed")
        if best is not None:
            best_line = (
                f"{html.escape(best.asset)} {self._side(best.direction)} на {html.escape(self._display(best.target_exchange))} "
                f"NET {best.econ.net_pct:+.2f}% HUMAN {_LIGHT.get(best.human.light, '')} BOT {_LIGHT.get(best.bot.light, '')}"
            )
        else:
            best_line = "нет"
        if self._health:
            links = ", ".join(
                f"{html.escape(self._display(ex))} {'✅' if self._link_up.get(ex) else '❌'}" for ex in sorted(self._health)
            )
        else:
            links = "нет данных"
        em = "🛑 включён" if self._emergency_active() else "выключен"
        return (
            f"📊 latbot: {self._mode_name()} · build {build_stamp()}\n"
            f"EMERGENCY STOP: {em}\n"
            f"Активных возможностей: {len(active)}\n"
            f"Лучшая: {best_line}\n"
            f"Биржи: {links}\n"
            f"Порог Telegram: {s.telegram_profit_threshold_pct:g}%\n"
            f"HUMAN_EXECUTION_TIME: {s.human_execution_time_s:g} с"
        )


def _quality_line(opp: Opportunity) -> str:
    """"Качество 78/100 (B) · плечо до 3×" - empty when the quality was not computed."""
    q = getattr(opp, "quality", None)
    if not isinstance(q, dict) or q.get("score") is None:
        return ""
    line = f"Качество {int(round(float(q['score'])))}/100 ({q.get('grade') or '?'})"
    lev = getattr(opp, "leverage", None)
    if isinstance(lev, dict):
        try:
            cap = float(lev.get("max_by_signal") or 0.0)
        except (TypeError, ValueError):
            cap = 0.0
        if cap >= 2.0:
            line += f" · плечо до {cap:g}×"
        elif cap > 0:
            line += " · без плеча"
    return line
