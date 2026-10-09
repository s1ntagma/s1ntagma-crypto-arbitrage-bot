"""Autopilot: AUTO without a click (owner: "скинуть ключи - и он всё делает сам").

Every opportunity that reaches VALID (or improves while VALID) and passes the same gates as a
Telegram alert - proven pair, quality when enforced, NET >= ``min_net_pct_auto``, BOT light not
red - is handed ONCE to ``ExecutionEngine.confirm_auto``.  Nothing else is bypassed: the engine
revalidates on the latest book, ``RiskManager.check`` runs before the entry order, LIVE needs the
Stats READY checklist (``live_requires_ready``), and the emergency stop blocks everything.
Off by default (``Settings.autopilot``); PAPER autopilot trades the paper adapters.
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from .. import events
from ..config import Settings
from ..models import Light, Mode, OppState, Opportunity

if TYPE_CHECKING:  # pragma: no cover
    from .engine import ExecutionEngine
    from .risk import RiskManager

log = logging.getLogger(__name__)

_READY_STATES = (OppState.VALID, OppState.NOTIFIED)
_MAX_TRIED = 5000
_READY_TTL_S = 30.0  # the READY checklist reads the shadow history: reuse its answer this long


class Autopilot:
    def __init__(self, settings: Settings, bus: events.EventBus, engine: "ExecutionEngine", risk: "RiskManager") -> None:
        self._settings = settings
        self._engine = engine
        self._risk = risk
        self._tried: dict[str, None] = {}  # opportunity ids handed to the engine (insertion ordered)
        bus.subscribe(events.OPP_STATE, self._on_state)
        bus.subscribe(events.OPP_UPDATE, self._on_update)

    async def _on_state(self, payload: Any) -> None:
        if isinstance(payload, dict) and payload.get("to") in (OppState.VALID.value, OppState.NOTIFIED.value):
            opp = payload.get("opportunity")
            if isinstance(opp, Opportunity):
                await self.consider(opp)

    async def _on_update(self, opp: Any) -> None:
        if isinstance(opp, Opportunity):
            await self.consider(opp)  # the NET may have risen above min_net_pct_auto

    async def consider(self, opp: Opportunity) -> str | None:
        """Start AUTO for ``opp`` now; returns why not (None = handed to the engine)."""
        s = self._settings
        if not getattr(s, "autopilot", False):
            return "autopilot off"
        if self._risk.emergency:
            return "emergency stop"
        if opp.id in self._tried:
            return "already tried"
        if opp.state not in _READY_STATES:
            return f"state {opp.state.value}"
        if opp.mode != s.mode():
            return "mode changed since detection"
        proven = getattr(opp, "proven", None)
        if isinstance(proven, dict) and proven and proven.get("enforced") and not proven.get("proven"):
            return "pair not proven"
        quality = getattr(opp, "quality", None)
        if isinstance(quality, dict) and quality and quality.get("enforced", True) and not quality.get("passed", False):
            return "quality not passed"
        if opp.bot.light is Light.RED:
            return "bot light red"
        if float(opp.econ.net_pct) < float(s.min_net_pct_auto):
            return f"net {opp.econ.net_pct:+.2f}% below {float(s.min_net_pct_auto):.2f}%"
        if s.mode() == Mode.LIVE and getattr(s, "live_requires_ready", True):
            missing = self._engine.not_ready(max_age_s=_READY_TTL_S)
            if missing is not None:
                return f"not ready for LIVE: {missing}"  # no attempt, no cancelled trade row per signal
        self._tried[opp.id] = None  # one attempt per opportunity, before any await (re-entrant events)
        while len(self._tried) > _MAX_TRIED:
            self._tried.pop(next(iter(self._tried)))
        try:
            res = await self._engine.confirm_auto(opp.id, wait=False)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("autopilot: confirm_auto(%s) failed", opp.id)
            return "engine error"
        if res.get("ok"):
            log.info("autopilot: AUTO started for %s %s on %s (net %+.2f%%)", opp.id, opp.asset,
                     opp.target_exchange, opp.econ.net_pct)
            return None
        log.info("autopilot: %s not started: %s", opp.id, res.get("reason"))
        return str(res.get("reason") or "not started")


__all__ = ["Autopilot"]
