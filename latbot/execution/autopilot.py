"""Autopilot: AUTO without a click (owner: "скинуть ключи - и он всё делает сам").

An opportunity the pipeline has just validated (VALIDATING -> VALID), that Telegram announced
(NOTIFIED) or that improved while VALID is considered, never inside the publisher that reported it:
the attempt runs as its own task and re-checks everything when it runs.  It is handed ONCE to
``ExecutionEngine.confirm_auto`` when

* the autopilot is on, no emergency stop, the card's mode is the current mode;
* the owner has not chosen it (it was never MANUAL_SELECTED / AUTO_SELECTED: their choice wins);
* the pair is PROVEN on the bot's own virtual trades (whenever ``proven_gate_enabled``, in any
  mode; no assessment = not proven), quality passed when enforced, BOT light not red,
  NET >= ``min_net_pct_auto``;
* nothing else is open on that pair (``ExecutionEngine.pair_busy``), the risk limits allow a trade
  (``RiskManager.check``) and, in LIVE, the Stats READY checklist passes.

A blocked opportunity is not marked as tried and stays on the board (no cancelled trade per
signal); it is reconsidered on its next update.  The engine then revalidates on the latest book and
runs ``RiskManager.check`` again before the entry order - nothing is bypassed.  Off by default
(``Settings.autopilot``); in PAPER the autopilot trades the paper adapters.
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
_OWNER_STATES = (OppState.MANUAL_SELECTED.value, OppState.AUTO_SELECTED.value)
_MAX_TRIED = 5000
_READY_TTL_S = 30.0  # a "not ready" answer is reused this long (the checklist reads the shadow history)


class Autopilot:
    def __init__(self, settings: Settings, bus: events.EventBus, engine: "ExecutionEngine", risk: "RiskManager") -> None:
        self._settings = settings
        self._engine = engine
        self._risk = risk
        self._tried: dict[str, None] = {}  # opportunity ids handed to the engine (insertion ordered)
        self._pending: dict[str, asyncio.Task[Any]] = {}  # scheduled attempts (one per opportunity)
        bus.subscribe(events.OPP_STATE, self._on_state)
        bus.subscribe(events.OPP_UPDATE, self._on_update)

    # ---- events: schedule, never act inside the publisher ----------------------------------------
    def _on_state(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        opp = payload.get("opportunity")
        to, frm = payload.get("to"), payload.get("from")
        validated = to == OppState.VALID.value and frm == OppState.VALIDATING.value  # the pipeline, not the engine
        if isinstance(opp, Opportunity) and (validated or to == OppState.NOTIFIED.value):
            self._schedule(opp)

    def _on_update(self, opp: Any) -> None:
        if isinstance(opp, Opportunity) and opp.state in _READY_STATES:
            self._schedule(opp)  # the NET may have risen above min_net_pct_auto

    def _schedule(self, opp: Opportunity) -> None:
        if not getattr(self._settings, "autopilot", False) or opp.id in self._tried or opp.id in self._pending:
            return
        try:
            task = asyncio.get_running_loop().create_task(self.consider(opp), name=f"autopilot-{opp.id}")
        except RuntimeError:  # no running loop (sync caller)
            return
        self._pending[opp.id] = task
        task.add_done_callback(lambda t, oid=opp.id: self._pending.pop(oid, None) if self._pending.get(oid) is t else None)

    # ---- the decision ------------------------------------------------------------------------------
    def _why_not(self, opp: Opportunity) -> str | None:
        s = self._settings
        if not getattr(s, "autopilot", False):
            return "autopilot off"
        if self._risk.emergency:
            return "emergency stop"
        if opp.id in self._tried:
            return "already tried"
        if any(h[1] in _OWNER_STATES for h in opp.state_history if len(h) > 1):
            return "the owner chose this opportunity"
        if opp.state not in _READY_STATES:
            return f"state {opp.state.value}"
        if opp.mode != s.mode():
            return "mode changed since detection"
        proven = getattr(opp, "proven", None)
        if getattr(s, "proven_gate_enabled", True) and not (isinstance(proven, dict) and proven.get("proven")):
            return "pair not proven"
        quality = getattr(opp, "quality", None)
        if isinstance(quality, dict) and quality and quality.get("enforced", True) and not quality.get("passed", False):
            return "quality not passed"
        if opp.bot.light is Light.RED:
            return "bot light red"
        if float(opp.econ.net_pct) < float(s.min_net_pct_auto):
            return f"net {opp.econ.net_pct:+.2f}% below {float(s.min_net_pct_auto):.2f}%"
        busy = self._engine.pair_busy(opp.target_exchange, opp.target_symbol, opp.id)
        if busy is not None:
            return busy
        capital = float(opp.econ.executable_quote or s.capital_quote)
        ok, why, _allowed = self._risk.check(opp.target_exchange, opp.asset, capital, s.mode())
        if not ok:
            return f"risk: {why}"
        if s.mode() == Mode.LIVE and getattr(s, "live_requires_ready", True):
            missing = self._engine.not_ready(max_age_s=_READY_TTL_S)
            if missing is not None:
                return f"not ready for LIVE: {missing}"
        return None

    async def consider(self, opp: Opportunity) -> str | None:
        """Start AUTO for ``opp`` now; returns why not (None = handed to the engine)."""
        why = self._why_not(opp)
        if why is not None:
            return why
        self._tried[opp.id] = None  # one attempt per opportunity
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
