"""OpportunityManager - ids, state machine, dedupe, expiry, counters.

One live opportunity per ``(asset, target, direction)``.  Repeated signals for
the same key update the existing opportunity (OPP_UPDATE) instead of creating
a new one.  State changes are validated against ``models.TRANSITIONS`` and
published as OPP_STATE.  Opportunities in EXECUTION_STATES belong to the
execution engine and are never expired or updated here.
"""
from __future__ import annotations

import logging
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Deque

from .. import events
from ..clock import Clock
from ..config import Settings
from ..links import pair_url, split_symbol
from ..models import (
    ACTIVE_STATES,
    EXECUTION_STATES,
    TERMINAL_STATES,
    TRANSITIONS,
    ExecAssessment,
    ExecutionEconomics,
    LagSignal,
    Light,
    Mode,
    Opportunity,
    OppState,
    new_id,
)

log = logging.getLogger(__name__)

_NET_HISTORY_MAX = 500
_PERSIST_MIN_INTERVAL_S = 1.0


def _utc_day(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _best_light(opp: Opportunity) -> Light:
    lights = (opp.human.light, opp.bot.light)
    if Light.GREEN in lights:
        return Light.GREEN
    if Light.YELLOW in lights:
        return Light.YELLOW
    return Light.RED


class OpportunityManager:
    """Registry + state machine for opportunities."""

    def __init__(
        self,
        settings: Settings,
        bus: events.EventBus,
        clock: Clock,
        db: Any | None = None,
        stats: Any | None = None,
    ) -> None:
        self._settings = settings
        self._bus = bus
        self._clock = clock
        self._db = db
        self._stats = stats
        self._opps: dict[str, Opportunity] = {}
        self._by_key: dict[str, str] = {}  # key -> id of the non-terminal opp
        self._history: Deque[Opportunity] = deque()
        self._last_persist_mono: dict[str, float] = {}

    # ------------------------------------------------------------------ #
    # keys / lookup
    # ------------------------------------------------------------------ #
    @staticmethod
    def key(asset: str, target: str, direction: int) -> str:
        return f"{asset}:{target}:{'L' if direction > 0 else 'S'}"

    def find_active(self, asset: str, target: str, direction: int) -> Opportunity | None:
        opp_id = self._by_key.get(self.key(asset, target, direction))
        if opp_id is None:
            return None
        opp = self._opps.get(opp_id)
        if opp is None or opp.state not in ACTIVE_STATES:
            return None
        return opp

    def get(self, opp_id: str) -> Opportunity | None:
        return self._opps.get(opp_id)

    def active(self) -> list[Opportunity]:
        out = [o for o in self._opps.values() if o.state in ACTIVE_STATES]
        out.sort(key=lambda o: o.econ.net_pct, reverse=True)
        return out

    def executing(self) -> list[Opportunity]:
        return [o for o in self._opps.values() if o.state in EXECUTION_STATES]

    def history(self, limit: int = 200) -> list[Opportunity]:
        """Terminal opportunities, newest first (in-memory ring)."""
        items = list(self._history)
        items.reverse()
        return items[: max(0, int(limit))]

    def best(self, now_mono: float) -> Opportunity | None:
        best: Opportunity | None = None
        for opp in self.active():
            if opp.human.light is Light.RED and opp.bot.light is Light.RED:
                continue
            if opp.remaining_s(now_mono) <= 0:
                continue
            if best is None or opp.econ.net_pct > best.econ.net_pct:
                best = opp
        return best

    # ------------------------------------------------------------------ #
    # upsert
    # ------------------------------------------------------------------ #
    async def upsert(
        self,
        signal: LagSignal,
        econ: ExecutionEconomics,
        human: ExecAssessment,
        bot: ExecAssessment,
        why: str,
        est_duration_s: float,
        mode: Mode,
        enrich: Callable[[Opportunity], None] | None = None,
    ) -> tuple[Opportunity, bool]:
        """Create or update the opportunity for the signal's key.
        Returns ``(opp, is_new)``; an executing opp is returned untouched.
        ``enrich(opp)`` (optional) runs after the fields are replaced and
        BEFORE OPP_NEW / OPP_UPDATE is published, so derived fields (stats,
        the trading plan) reach the UI and Telegram in the same event."""
        now = self._clock.mono()
        wall = self._clock.wall()
        k = self.key(signal.asset, signal.target, signal.direction)
        existing_id = self._by_key.get(k)
        existing = self._opps.get(existing_id) if existing_id else None
        if existing is not None and existing.state in EXECUTION_STATES:
            return existing, False
        if existing is not None and existing.state in ACTIVE_STATES:
            opp = existing
            opp.signal = signal
            opp.mode = mode  # the card always shows the mode that would execute
            opp.first_mover = signal.first_mover
            if signal.target_symbol:
                opp.target_symbol = signal.target_symbol
            opp.econ = econ
            opp.human = human
            opp.bot = bot
            opp.why = why
            opp.estimated_duration_s = est_duration_s
            opp.last_update_mono = now
            if econ.net_pct >= self._settings.min_net_pct_show:
                opp.last_valid_mono = now
            opp.net_history.append((now, econ.net_pct))
            if len(opp.net_history) > _NET_HISTORY_MAX:
                del opp.net_history[: len(opp.net_history) - _NET_HISTORY_MAX]
            opp.update_count += 1
            opp.max_net_pct = max(opp.max_net_pct, econ.net_pct)
            opp.last_update_ts = wall
            self._enrich(opp, enrich)
            await self._bus.publish(events.OPP_UPDATE, opp)
            self._persist(opp)
            return opp, False

        symbol = signal.target_symbol
        quote = signal.target_quote or (split_symbol(symbol)[1] if symbol else "")
        cfg = self._settings.exchange(signal.target)
        opp = Opportunity(
            id=new_id("opp_"),
            asset=signal.asset,
            target_exchange=signal.target,
            target_symbol=symbol,
            first_mover=signal.first_mover,
            direction=signal.direction,
            mode=mode,
            state=OppState.DETECTED,
            signal=signal,
            econ=econ,
            human=human,
            bot=bot,
            why=why,
            detected_ts=wall,
            first_seen_mono=now,
            last_valid_mono=now,
            last_update_mono=now,
            estimated_duration_s=est_duration_s,
            quote=quote,
            pair_url=pair_url(signal.target, symbol, cfg.pair_url if cfg and cfg.pair_url else None),
            max_net_pct=econ.net_pct,
            last_update_ts=wall,
        )
        opp.state_history.append((wall, OppState.DETECTED.value, "detected"))
        opp.net_history.append((now, econ.net_pct))
        self._enrich(opp, enrich)
        self._opps[opp.id] = opp
        self._by_key[k] = opp.id
        log.info(
            "OPP_NEW %s %s %s on %s net %+.2f%% (lag %+.2f%% from %s)",
            opp.id, opp.asset, "LONG" if opp.direction > 0 else "SHORT", opp.target_exchange,
            econ.net_pct, signal.lag_pct, signal.first_mover,
        )
        await self._bus.publish(events.OPP_NEW, opp)
        self._persist(opp, force=True)
        return opp, True

    @staticmethod
    def _enrich(opp: Opportunity, enrich: Callable[[Opportunity], None] | None) -> None:
        if enrich is None:
            return
        try:
            enrich(opp)
        except Exception:  # noqa: BLE001 - derived fields never block the opportunity itself
            log.exception("opportunity enrich failed for %s", opp.id)

    # ------------------------------------------------------------------ #
    # state machine
    # ------------------------------------------------------------------ #
    async def transition(self, opp_id: str, new_state: OppState, reason: str = "") -> Opportunity:
        opp = self._opps.get(opp_id)
        if opp is None:
            raise KeyError(opp_id)
        new_state = OppState(new_state)
        if opp.state is new_state:
            return opp
        allowed = TRANSITIONS.get(opp.state, frozenset())
        if new_state not in allowed:
            raise ValueError(f"invalid transition {opp.state.value} -> {new_state.value} for {opp_id}")
        old = opp.state
        opp.state = new_state
        opp.state_history.append((self._clock.wall(), new_state.value, reason))
        log.info("OPP_STATE %s %s -> %s %s", opp_id, old.value, new_state.value, reason)
        if new_state in TERMINAL_STATES:
            self._finalize(opp)
        await self._bus.publish(
            events.OPP_STATE, {"opportunity": opp, "from": old.value, "to": new_state.value, "reason": reason}
        )
        self._persist(opp, force=True)
        return opp

    def _finalize(self, opp: Opportunity) -> None:
        """Entering a terminal state: leave the key index, fill Phase 2.4 fields."""
        now = self._clock.mono()
        k = self.key(opp.asset, opp.target_exchange, opp.direction)
        if self._by_key.get(k) == opp.id:
            del self._by_key[k]
        if opp.expired_mono is None:
            opp.expired_mono = now
        if opp.actual_duration_s is None:
            opp.actual_duration_s = max(0.0, opp.expired_mono - opp.first_seen_mono)
        if opp.state in (OppState.EXPIRED, OppState.CLOSED):
            caught = bool(opp.caught_up)
            opp.human_would_make_it = (
                opp.actual_duration_s >= self._settings.human_execution_time_s and caught
            )
            if self._stats is not None and hasattr(self._stats, "record_human_outcome"):
                try:
                    self._stats.record_human_outcome(
                        opp.asset, opp.target_exchange, bool(opp.human_would_make_it), caught
                    )
                except Exception:  # noqa: BLE001
                    log.exception("record_human_outcome failed")
        keep = max(1, int(self._settings.opp_history_keep))
        while len(self._history) >= keep:
            old = self._history.popleft()
            if old.id != opp.id:
                self._opps.pop(old.id, None)
        self._history.append(opp)

    async def expire(self, opp_id: str, reason: str) -> Opportunity | None:
        opp = self._opps.get(opp_id)
        if opp is None:
            return None
        if opp.state in TERMINAL_STATES:
            return None
        if opp.state in EXECUTION_STATES:
            log.warning("refusing to expire executing opportunity %s (%s)", opp_id, reason)
            return None
        now = self._clock.mono()
        opp.expired_mono = now
        opp.actual_duration_s = max(0.0, now - opp.first_seen_mono)
        opp.expire_reason = reason
        await self.transition(opp_id, OppState.EXPIRED, reason)
        if self._stats is not None and hasattr(self._stats, "record_opportunity_duration"):
            try:
                self._stats.record_opportunity_duration(opp.asset, opp.target_exchange, opp.actual_duration_s)
            except Exception:  # noqa: BLE001
                log.exception("record_opportunity_duration failed")
        return opp

    async def sweep(self, now_mono: float) -> list[Opportunity]:
        """Expire active opportunities whose window closed, that stopped
        receiving updates, or whose net fell below the show threshold."""
        expired: list[Opportunity] = []
        s = self._settings
        for opp in list(self._opps.values()):
            if opp.state not in ACTIVE_STATES:
                continue
            reason = ""
            ride = bool(getattr(s, "simple_mode", False) and getattr(s, "ride_enabled", False))
            start = float(getattr(opp.signal, "first_move_start_mono", 0.0) or opp.first_seen_mono)
            if ride and now_mono - start >= float(s.ride_entry_max_age_s):  # from the leader's move, not the card
                reason = f"entry window {float(s.ride_entry_max_age_s):g} s over"  # positions keep riding on their own
            elif not ride and getattr(s, "simple_mode", False) and opp.age_s(now_mono) >= float(s.simple_max_hold_s):
                reason = f"hold limit {float(s.simple_max_hold_s):g} s"  # simple mode: not caught up in time -> out
            elif opp.remaining_s(now_mono) <= 0:
                reason = "window closed"
            elif now_mono - opp.last_update_mono > 2.0 * s.move_window_s:
                reason = "no updates"
            elif opp.econ.net_pct < s.min_net_pct_show:
                reason = "net below threshold"
            if reason:
                r = await self.expire(opp.id, reason)
                if r is not None:
                    expired.append(r)
        return expired

    async def relabel_mode(self, mode: Mode) -> list[Opportunity]:
        """Re-label every active opportunity after a PAPER/LIVE switch (hard
        rule 8: the UI always shows the mode that would execute).  Executing
        opportunities belong to the engine and keep the mode their trade runs
        in."""
        changed: list[Opportunity] = []
        for opp in self.active():
            if opp.mode is mode:
                continue
            opp.mode = mode
            changed.append(opp)
            await self._bus.publish(events.OPP_UPDATE, opp)
            self._persist(opp, force=True)
        return changed

    # ------------------------------------------------------------------ #
    # learning hooks
    # ------------------------------------------------------------------ #
    def mark_caught_up(self, asset: str, target: str, direction: int | None = None) -> int:
        """The target crossed in the signal's direction: flag live opps."""
        n = 0
        for opp_id in list(self._by_key.values()):
            opp = self._opps.get(opp_id)
            if opp is None or opp.asset != asset or opp.target_exchange != target:
                continue
            if direction is not None and opp.direction != direction:
                continue
            if not opp.caught_up:
                opp.caught_up = True
                n += 1
        return n

    # ------------------------------------------------------------------ #
    # counters
    # ------------------------------------------------------------------ #
    def counters(self, now_mono: float) -> dict[str, Any]:
        today = _utc_day(self._clock.wall())
        active = self.active()
        green = yellow = red = 0
        human_ok = bot_ok = 0
        for opp in active:
            bl = _best_light(opp)
            if bl is Light.GREEN:
                green += 1
            elif bl is Light.YELLOW:
                yellow += 1
            else:
                red += 1
            if opp.human.light is not Light.RED:
                human_ok += 1
            if opp.bot.light is not Light.RED:
                bot_ok += 1
        today_count = 0
        alerts = 0
        expired = 0
        for opp in self._opps.values():
            if _utc_day(opp.detected_ts) == today:
                today_count += 1
            if opp.notified_ts is not None and _utc_day(opp.notified_ts) == today:
                alerts += 1
            if opp.state is OppState.EXPIRED and opp.state_history and _utc_day(opp.state_history[-1][0]) == today:
                expired += 1
        return {
            "active": len(active),
            "today": today_count,
            "green": green,
            "yellow": yellow,
            "red": red,
            "human_executable": human_ok,
            "bot_executable": bot_ok,
            "telegram_alerts": alerts,
            "paper_pnl": 0.0,
            "live_pnl": 0.0,
            "realized_pnl": 0.0,
            "failed_trades": 0,
            "expired": expired,
            "avg_latency_ms": 0.0,
            "avg_execution_time_s": 0.0,
            "success_rate": None,
        }

    # ------------------------------------------------------------------ #
    # persistence
    # ------------------------------------------------------------------ #
    def _persist(self, opp: Opportunity, force: bool = False) -> None:
        if self._db is None or not hasattr(self._db, "save_opportunity"):
            return
        now = self._clock.mono()
        last = self._last_persist_mono.get(opp.id)
        if not force and last is not None and now - last < _PERSIST_MIN_INTERVAL_S:
            return
        try:
            self._db.save_opportunity(opp)
            self._last_persist_mono[opp.id] = now
        except Exception:  # noqa: BLE001
            log.exception("save_opportunity failed for %s", opp.id)
        if opp.state in TERMINAL_STATES:
            self._last_persist_mono.pop(opp.id, None)
