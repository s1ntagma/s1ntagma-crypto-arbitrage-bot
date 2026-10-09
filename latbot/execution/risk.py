"""RiskManager - hard limits checked before every order.

``check`` can not be bypassed by any flag: the execution engine calls it inside
``revalidate`` (before the entry order) and refuses to send anything while
``emergency`` is set.  Limits (all from ``Settings``):

* ``max_trade_quote``                       - absolute cap per trade
* ``total_capital_quote * max_risk_per_trade_pct / 100`` - position size cap
* ``max_exposure_per_exchange_quote``        - open quote per exchange
* ``max_exposure_per_asset_quote``           - open quote per asset
* ``exchange_budget_quote[exchange]``        - the owner's budget on one venue (open quote)
* ``max_simultaneous_trades``               - open round trips
* ``max_daily_loss_quote``                  - realized P&L of the UTC day
* emergency stop

Daily P&L combines the in-memory closes of this process with ``db.daily_pnl``
(trades persisted by the engine), de-duplicated by trade id.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from .. import events
from ..clock import Clock
from ..config import Settings
from ..models import Mode

if TYPE_CHECKING:  # pragma: no cover
    from ..storage.db import Database

log = logging.getLogger(__name__)

_DAY_S = 86400.0


def _utc_day(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d")


def _day_start(ts: float) -> float:
    return float(int(ts // _DAY_S) * _DAY_S)


def _mode_value(mode: Mode | str | None, default: Mode) -> str:
    if mode is None:
        return default.value
    return mode.value if isinstance(mode, Mode) else str(mode)


@dataclass(slots=True)
class _OpenTrade:
    trade_id: str
    exchange: str
    asset: str
    quote: float
    mode: str
    opened_ts: float


@dataclass(slots=True)
class _ClosedTrade:
    trade_id: str
    ts: float
    mode: str
    pnl: float


class RiskManager:
    """Capital, exposure, daily-loss and emergency guard."""

    def __init__(
        self,
        settings: Settings,
        db: "Database | None",
        clock: Clock,
        bus: events.EventBus | None = None,
    ) -> None:
        self._settings = settings
        self._db = db
        self._clock = clock
        self._bus = bus
        self.emergency: bool = False
        self.emergency_reason: str = ""
        self._open: dict[str, _OpenTrade] = {}
        self._closed: dict[str, _ClosedTrade] = {}

    # ------------------------------------------------------------------ #
    # bookkeeping
    # ------------------------------------------------------------------ #
    def register_open(self, trade_id: str, exchange: str, asset: str, quote: float, mode: Mode | str | None = None) -> None:
        """Track an opened round trip (exposure counts until ``register_close``)."""
        m = _mode_value(mode, self._settings.mode())
        self._open[trade_id] = _OpenTrade(
            trade_id=trade_id,
            exchange=exchange,
            asset=asset,
            quote=max(0.0, float(quote)),
            mode=m,
            opened_ts=self._clock.wall(),
        )
        log.info("risk: open %s %s/%s %.2f (%s); open trades=%d", trade_id, exchange, asset, quote, m, len(self._open))

    def register_close(self, trade_id: str, realized_pnl_quote: float) -> None:
        """Release exposure and record the realized P&L for the daily limit."""
        opened = self._open.pop(trade_id, None)
        mode = opened.mode if opened is not None else self._settings.mode().value
        now = self._clock.wall()
        self._closed[trade_id] = _ClosedTrade(trade_id=trade_id, ts=now, mode=mode, pnl=float(realized_pnl_quote or 0.0))
        self._prune(now)
        log.info("risk: close %s pnl=%+.2f (%s); open trades=%d", trade_id, float(realized_pnl_quote or 0.0), mode, len(self._open))

    def _prune(self, now: float) -> None:
        cutoff = _day_start(now) - _DAY_S
        for tid in [t for t, c in self._closed.items() if c.ts < cutoff]:
            self._closed.pop(tid, None)

    def open_trades(self) -> int:
        return len(self._open)

    def exposure(self, exchange: str | None = None, asset: str | None = None) -> float:
        """Open quote matching both filters (None = any)."""
        total = 0.0
        for t in self._open.values():
            if exchange is not None and t.exchange != exchange:
                continue
            if asset is not None and t.asset != asset:
                continue
            total += t.quote
        return total

    def open_positions(self) -> list[dict[str, Any]]:
        """Snapshot of the tracked open trades (for the UI / Telegram)."""
        return [
            {"trade_id": t.trade_id, "exchange": t.exchange, "asset": t.asset, "quote": t.quote, "mode": t.mode, "opened_ts": t.opened_ts}
            for t in self._open.values()
        ]

    # ------------------------------------------------------------------ #
    # daily P&L
    # ------------------------------------------------------------------ #
    def daily_pnl(self, mode: Mode | str) -> float:
        """Realized P&L of the current UTC day for ``mode`` (db + this process)."""
        mode_v = _mode_value(mode, self._settings.mode())
        now = self._clock.wall()
        day = _utc_day(now)
        start = _day_start(now)
        end = start + _DAY_S
        total = 0.0
        db_ids: set[str] = set()
        if self._db is not None:
            try:
                total += float(self._db.daily_pnl(day, mode_v) or 0.0)
                for row in self._db.list_trades(limit=10_000, mode=mode_v, since_ts=start - _DAY_S):
                    fin = row.get("finished_ts")
                    if row.get("realized_pnl_quote") is None or not isinstance(fin, (int, float)):
                        continue
                    if start <= float(fin) < end and row.get("id"):
                        db_ids.add(str(row["id"]))
            except Exception:  # noqa: BLE001
                log.exception("risk: daily_pnl from db failed; using in-memory closes only")
        for c in self._closed.values():
            if c.mode != mode_v or not (start <= c.ts < end) or c.trade_id in db_ids:
                continue
            total += c.pnl
        return total

    # ------------------------------------------------------------------ #
    # the check
    # ------------------------------------------------------------------ #
    def check(self, exchange: str, asset: str, capital_quote: float, mode: Mode | str) -> tuple[bool, str, float]:
        """Return ``(ok, reason, allowed_capital)``.

        ``allowed_capital`` is ``min(requested, max_trade_quote,
        total_capital * max_risk_per_trade_pct / 100, remaining exchange
        exposure, remaining asset exposure)``; the check fails when that is
        below ``min_trade_quote`` or when a hard limit (simultaneous trades,
        daily loss, emergency) is hit.  Reasons are plain English."""
        s = self._settings
        mode_v = _mode_value(mode, s.mode())
        if self.emergency:
            why = f": {self.emergency_reason}" if self.emergency_reason else ""
            return False, f"Emergency stop is active{why}", 0.0
        requested = float(capital_quote or 0.0)
        if requested <= 0:
            return False, "Requested capital must be positive", 0.0
        open_n = self.open_trades()
        if open_n >= int(s.max_simultaneous_trades):
            return False, f"Too many simultaneous trades ({open_n} of {int(s.max_simultaneous_trades)} open)", 0.0
        pnl = self.daily_pnl(mode_v)
        limit = float(s.max_daily_loss_quote)
        if pnl <= -limit:
            return False, f"Daily loss limit reached: {pnl:+.2f} <= -{limit:.2f} ({mode_v})", 0.0

        caps: list[tuple[str, float]] = [
            ("requested capital", requested),
            ("max trade size", float(s.max_trade_quote)),
            ("max risk per trade", float(s.total_capital_quote) * float(s.max_risk_per_trade_pct) / 100.0),
            ("exchange exposure", float(s.max_exposure_per_exchange_quote) - self.exposure(exchange=exchange)),
            ("asset exposure", float(s.max_exposure_per_asset_quote) - self.exposure(asset=asset)),
        ]
        budget = float((getattr(s, "exchange_budget_quote", None) or {}).get(str(exchange).lower(), 0.0) or 0.0)
        if budget > 0:
            caps.append((f"budget on {exchange}", budget - self.exposure(exchange=exchange)))
        binding, allowed = min(caps, key=lambda c: c[1])
        allowed = max(0.0, allowed)
        if allowed < float(s.min_trade_quote):
            return (
                False,
                f"Allowed capital ${allowed:.2f} is below the minimum trade size ${float(s.min_trade_quote):.2f} (limited by {binding})",
                allowed,
            )
        if allowed < requested:
            return True, f"Capital reduced from ${requested:.2f} to ${allowed:.2f} by {binding}", allowed
        return True, "OK", allowed

    # ------------------------------------------------------------------ #
    # emergency
    # ------------------------------------------------------------------ #
    async def set_emergency(self, active: bool, reason: str = "") -> None:
        """Set / clear the emergency stop and publish EMERGENCY."""
        self.emergency = bool(active)
        self.emergency_reason = reason if active else ""
        if active:
            log.warning("EMERGENCY STOP active: %s", reason or "no reason given")
        else:
            log.warning("emergency stop cleared (%s)", reason or "resume")
        if self._bus is not None:
            await self._bus.publish(events.EMERGENCY, {"active": self.emergency, "reason": reason})
