"""MoveDetector - first mover / lagging exchange detection.

Per asset, on every book (or ticker):
 1. measure the move over ``move_window_s`` on every *fresh* exchange,
 2. keep a per-(exchange, asset) crossing state with hysteresis
    (cross at ``|move| >= move_threshold_pct``, reset when the move in the
    crossing direction falls below ``threshold / 2``), publish MoveEvent,
 3. maintain a per-asset *episode* (first mover = earliest crosser),
 4. learn reaction delays / catch-up ratios when an execution exchange crosses
    later, false signals when the movers revert without the target reacting,
 5. emit one LagSignal per lagging execution exchange (Phase 2.2: only when at
    least ``min_movers`` counted exchanges moved).

No data -> no signal.  Stale exchanges are not measured; their crossing state
is cleared silently (unknown is not "reverted").
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Callable

from .. import events
from ..clock import Clock
from ..config import Settings
from ..links import split_symbol
from ..market.features import move_over_window, velocity_pct_s, volume_acceleration
from ..market.state import MarketState
from ..models import LagSignal, MoveEvent, OrderBook, Ticker
from .stats import StatsStore

log = logging.getLogger(__name__)

ReactionListener = Callable[[str, str, int, float], None]  # (asset, target, direction, delay_s)

Move = tuple[float, float, float, float]  # move_pct, start_price, end_price, start_mono


@dataclass(slots=True)
class _Crossing:
    crossed: bool = False
    direction: int = 0
    cross_mono: float = 0.0
    move_at_cross: float = 0.0


@dataclass(slots=True)
class _Episode:
    asset: str
    first_mover: str
    direction: int
    start_mono: float
    movers: set[str] = field(default_factory=set)  # every exchange that crossed in this episode
    lagging: set[str] = field(default_factory=set)  # targets with a recorded lag
    reacted: set[str] = field(default_factory=set)  # execution exchanges that crossed later
    reverted: bool = False  # a mover reset because its move reverted (not because it went stale)


class MoveDetector:
    """Detects cross-exchange moves and the exchanges lagging behind them."""

    def __init__(
        self,
        settings: Settings,
        state: MarketState,
        stats: StatsStore,
        clock: Clock,
        bus: events.EventBus | None = None,
    ) -> None:
        self._settings = settings
        self._state = state
        self._stats = stats
        self._clock = clock
        self._bus = bus
        self._cross: dict[tuple[str, str], _Crossing] = {}
        self._episodes: dict[str, _Episode] = {}
        self._tasks: set[asyncio.Task] = set()
        self.reaction_listeners: list[ReactionListener] = []

    # ------------------------------------------------------------------ #
    # public API
    # ------------------------------------------------------------------ #
    def on_book(self, book: OrderBook) -> list[LagSignal]:
        asset = MarketState.asset_of(book)
        if not asset:
            return []
        return self._detect(asset, self._clock.mono(), source="ws", max_age_ms=self._settings.max_data_age_ms)

    def on_ticker(self, ticker: Ticker, source: str = "poll") -> list[LagSignal]:
        """Detection on ticker data.  Polled tickers (``source="poll"``) are one
        poll interval old by construction, so the freshness bound is widened by
        ``ticker_poll_interval_s``; WebSocket tickers keep the strict bound.
        The pipeline still confirms every polled signal on a fresh REST book."""
        asset = MarketState.asset_of(ticker)
        if not asset:
            return []
        max_age = self._settings.max_data_age_ms
        if source == "poll":
            max_age += max(0.0, self._settings.ticker_poll_interval_s) * 1000.0
        return self._detect(asset, self._clock.mono(), source=source, max_age_ms=max_age)

    def allowed_direction(self, exchange: str, direction: int) -> bool:
        """LONG always (enabled exchange); SHORT only when ``allow_short``."""
        cfg = self._settings.exchange(exchange)
        if cfg is None or not cfg.enabled:
            return False
        if direction > 0:
            return True
        return bool(cfg.allow_short)

    def is_crossed(self, exchange: str, asset: str) -> bool:
        c = self._cross.get((exchange, asset))
        return bool(c and c.crossed)

    def crossing_direction(self, exchange: str, asset: str) -> int:
        c = self._cross.get((exchange, asset))
        return c.direction if c and c.crossed else 0

    def current_moves(self, asset: str, now_mono: float, max_age_ms: float | None = None) -> dict[str, Move]:
        """Window move per fresh exchange (step 1) - also used by the pipeline
        to refresh an opportunity against live data."""
        max_age = self._settings.max_data_age_ms if max_age_ms is None else max_age_ms
        moves: dict[str, Move] = {}
        for ex in self._state.exchanges_for(asset):
            if not self._state.is_fresh(ex, asset, now_mono, max_age):
                continue
            r = move_over_window(self._state.mid_series(ex, asset), now_mono, self._settings.move_window_s)
            if r is not None:
                moves[ex] = r
        return moves

    # ------------------------------------------------------------------ #
    # core algorithm
    # ------------------------------------------------------------------ #
    def _detect(self, asset: str, now: float, *, source: str, max_age_ms: float) -> list[LagSignal]:
        s = self._settings
        thr = s.move_threshold_pct
        moves = self.current_moves(asset, now, max_age_ms)

        # ---- step 2: crossing state with hysteresis -----------------------
        new_crossings: list[str] = []
        reverted_now: set[str] = set()
        for ex, (move, start_price, end_price, start_mono) in moves.items():
            c = self._cross.setdefault((ex, asset), _Crossing())
            if c.crossed and move * c.direction < thr / 2.0:
                log.debug("crossing reset %s %s move=%.3f%%", ex, asset, move)
                c.crossed = False
                c.direction = 0
                reverted_now.add(ex)
            if not c.crossed and abs(move) >= thr:
                c.crossed = True
                c.direction = 1 if move > 0 else -1
                c.cross_mono = now
                c.move_at_cross = move
                new_crossings.append(ex)
                log.info("MOVE %s %s %+.2f%% over %.1fs", ex, asset, move, max(0.0, now - start_mono))
                self._publish_move(ex, asset, c.direction, move, start_price, end_price, start_mono, now)
        # exchanges without fresh data cannot be measured: clear silently
        for (ex, a), c in self._cross.items():
            if a == asset and ex not in moves and c.crossed:
                log.debug("crossing cleared (no fresh data) %s %s", ex, asset)
                c.crossed = False
                c.direction = 0

        crossed_now = [ex for ex in moves if self._cross[(ex, asset)].crossed]

        # ---- step 3: episode ----------------------------------------------
        ep = self._episodes.get(asset)
        if ep is not None:
            if ep.movers & reverted_now:
                ep.reverted = True
            same = [ex for ex in crossed_now if self._cross[(ex, asset)].direction == ep.direction]
            if not same:
                self._end_episode(ep)
                ep = None
            elif ep.first_mover not in same:
                ep.first_mover = min(same, key=lambda e: (self._cross[(e, asset)].cross_mono, e))
        if ep is None and crossed_now:
            first = min(crossed_now, key=lambda e: (self._cross[(e, asset)].cross_mono, e))
            c = self._cross[(first, asset)]
            ep = _Episode(asset=asset, first_mover=first, direction=c.direction, start_mono=c.cross_mono)
            self._episodes[asset] = ep
            self._stats.record_first_mover(asset, first)
            log.info("episode start %s first_mover=%s dir=%+d", asset, first, ep.direction)
        if ep is None:
            return []
        direction = ep.direction
        for ex in crossed_now:
            if self._cross[(ex, asset)].direction == direction:
                ep.movers.add(ex)

        # ---- step 4: learning (execution exchanges crossing later) ---------
        exec_exchanges = set(s.execution_exchanges())
        first_move_now = moves[ep.first_mover][0] if ep.first_mover in moves else 0.0
        for ex in new_crossings:
            if ex == ep.first_mover or ex in ep.reacted or ex not in exec_exchanges:
                continue
            if self._cross[(ex, asset)].direction != direction:
                continue
            delay = max(0.0, now - ep.start_mono)
            ref = first_move_now * direction
            if ref > 0:
                ratio = (moves[ex][0] * direction) / ref
            else:
                ratio = 1.0
            ratio = max(0.0, min(1.5, ratio))
            ep.reacted.add(ex)
            self._stats.record_reaction(asset, ex, delay, ratio)
            log.info("reaction %s on %s after %.2fs catch-up ratio %.2f", asset, ex, delay, ratio)
            for cb in list(self.reaction_listeners):
                try:
                    cb(asset, ex, direction, delay)
                except Exception:  # noqa: BLE001
                    log.exception("reaction listener failed")

        # ---- step 5: movers (Phase 2.2 volume gate) ------------------------
        movers = [ex for ex in crossed_now if self._cross[(ex, asset)].direction == direction]
        min_vol = s.min_mover_volume_quote
        counted: list[str] = []
        volumes: dict[str, float] = {}
        for ex in movers:
            vol = self._state.volume24h(ex, asset)
            if vol is not None:
                volumes[ex] = vol
            if min_vol > 0 and (vol is None or vol < min_vol):
                continue
            counted.append(ex)
        if len(counted) < max(1, int(s.min_movers)):
            return []
        first = min(counted, key=lambda e: (self._cross[(e, asset)].cross_mono, e))
        first_move = moves[first][0]
        first_cross_mono = self._cross[(first, asset)].cross_mono

        # ---- step 6: lagging execution exchanges ---------------------------
        signals: list[LagSignal] = []
        reacted_moves = {ex: m[0] for ex, m in moves.items()}
        for target in s.execution_exchanges():
            if target == first or target not in moves:
                continue
            if not self.allowed_direction(target, direction):
                continue
            tvol = self._state.volume24h(target, asset)
            if tvol is not None and s.min_target_volume_quote > 0 and tvol < s.min_target_volume_quote:
                continue
            target_move = moves[target][0]
            lag = first_move * direction - target_move * direction
            if lag < s.min_lag_pct:
                continue
            if target not in ep.lagging:
                ep.lagging.add(target)
                self._stats.record_lagging(asset, target, lag)
            ratio = self._stats.catchup_ratio(asset, target) or s.catchup_ratio_default
            symbol = self._state.symbol(target, asset) or ""
            signals.append(
                LagSignal(
                    asset=asset,
                    direction=direction,
                    first_mover=first,
                    first_move_pct=first_move,
                    first_move_start_mono=first_cross_mono,
                    reacted=reacted_moves,
                    target=target,
                    target_symbol=symbol,
                    target_move_pct=target_move,
                    lag_pct=lag,
                    expected_catchup_pct=lag * ratio,
                    catchup_ratio=ratio,
                    detected_mono=now,
                    detected_ts=self._clock.wall(),
                    movers_count=len(counted),
                    median_reaction_delay_s=self._stats.median_reaction_delay(asset, target),
                    source=source,
                    target_quote=split_symbol(symbol)[1] if symbol else "",
                    mover_volumes_quote={ex: volumes[ex] for ex in counted if ex in volumes},
                )
            )
        signals.sort(key=lambda sg: sg.lag_pct, reverse=True)
        return signals

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    def _end_episode(self, ep: _Episode) -> None:
        self._episodes.pop(ep.asset, None)
        if ep.reverted:
            for target in sorted(ep.lagging - ep.reacted):
                self._stats.record_false_signal(ep.asset, target)
                log.info("false signal %s: %s reverted before %s reacted", ep.asset, ep.first_mover, target)
        log.debug("episode end %s first_mover=%s reverted=%s", ep.asset, ep.first_mover, ep.reverted)

    def _publish_move(
        self,
        exchange: str,
        asset: str,
        direction: int,
        move_pct: float,
        start_price: float,
        end_price: float,
        start_mono: float,
        now: float,
    ) -> None:
        if self._bus is None:
            return
        ev = MoveEvent(
            exchange=exchange,
            asset=asset,
            direction=direction,
            move_pct=move_pct,
            start_price=start_price,
            end_price=end_price,
            start_mono=start_mono,
            end_mono=now,
            velocity_pct_s=velocity_pct_s(move_pct, start_mono, now),
            volume_accel=volume_acceleration(self._state, exchange, asset, now),
        )
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            log.debug("no running loop; MoveEvent not published (%s %s)", exchange, asset)
            return
        task = loop.create_task(self._bus.publish(events.MOVE, ev))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
