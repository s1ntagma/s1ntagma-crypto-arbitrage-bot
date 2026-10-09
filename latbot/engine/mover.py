"""MoveDetector - first mover / lagging exchange detection.

Per asset, on every book (or ticker):
 1. measure the move over ``move_window_s`` on every *fresh* exchange,
 2. keep a per-(exchange, asset) crossing state with hysteresis: cross at
    ``|move| >= move_threshold_pct``.  From then on the exchange is measured
    against its *anchor* (the pre-move price captured at the cross), so a
    genuine move keeps its crossing after the sliding window has moved past
    the jump.  The crossing resets only when the price comes back to within
    ``threshold / 2`` of the anchor (a reversion), when the data go stale, or
    when the episode outlives ``max_opp_duration_s``.  Publish MoveEvent.
 3. maintain a per-asset *episode* (first mover = earliest crosser).
    Exchanges that have not crossed are measured from their price at the
    episode's anchor moment, so the lag is "how much of the move since it
    began has the target not done yet",
 4. learn reaction delays / catch-up ratios when an execution exchange crosses
    later (also a slow catch-up that never crosses inside one window), false
    signals when the movers revert without the target reacting,
 5. emit one LagSignal per lagging execution exchange (Phase 2.2: only when at
    least ``min_movers`` counted exchanges moved).

No data -> no signal.  Stale exchanges are not measured; their crossing state
is cleared silently (unknown is not "reverted").  An episode that merely ages
out is not a false signal either - only a price reversion is.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Callable

from .. import events
from ..clock import Clock
from ..config import Settings
from .diagnostics import Diagnostics
from ..links import split_symbol
from ..market.features import move_over_window, velocity_pct_s, volume_acceleration
from ..market.state import MarketState
from ..models import ExchangeRole, LagSignal, MoveEvent, OrderBook, Ticker
from .stats import StatsStore

log = logging.getLogger(__name__)

ReactionListener = Callable[[str, str, int, float], None]  # (asset, target, direction, delay_s)

Move = tuple[float, float, float, float]  # move_pct, start_price, end_price, start_mono


def _pct(end_price: float, start_price: float) -> float:
    return (end_price / start_price - 1.0) * 100.0 if start_price > 0 else 0.0


@dataclass(slots=True)
class _Crossing:
    crossed: bool = False
    direction: int = 0
    cross_mono: float = 0.0
    move_at_cross: float = 0.0
    anchor_price: float = 0.0  # pre-move price the move is measured against while crossed
    anchor_mono: float = 0.0  # when that price was seen (start of the window at the cross)
    anchor_until: float = 0.0  # after a reversion: keep measuring from the anchor until the window forgot the spike


@dataclass(slots=True)
class _Episode:
    asset: str
    first_mover: str
    direction: int
    start_mono: float
    anchor_mono: float = 0.0  # pre-move moment (the first mover's anchor): others are measured from here
    movers: set[str] = field(default_factory=set)  # every exchange that crossed in this episode
    lagging: set[str] = field(default_factory=set)  # targets with a recorded lag
    reacted: set[str] = field(default_factory=set)  # execution exchanges that crossed later
    reverted: bool = False  # a mover reset because its price reverted (not because it went stale / aged out)
    anchors: dict[str, float] = field(default_factory=dict)  # pre-move price per not-yet-crossed exchange
    # lagging target -> latest (first-mover move, target move) toward the direction: the catch-up it
    # actually delivered is recorded when the episode ends, also when it never followed (no survivor bias)
    last_moves: dict[str, tuple[float, float]] = field(default_factory=dict)
    peak_move: dict[str, float] = field(default_factory=dict)  # mover -> best move toward the direction


def _fmt_vol(v: float | None) -> str:
    if v is None:
        return "?"
    if v >= 1e6:
        return f"{v / 1e6:.1f}M"
    if v >= 1e3:
        return f"{v / 1e3:.0f}k"
    return f"{v:.0f}"


class MoveDetector:
    """Detects cross-exchange moves and the exchanges lagging behind them."""

    def __init__(
        self,
        settings: Settings,
        state: MarketState,
        stats: StatsStore,
        clock: Clock,
        bus: events.EventBus | None = None,
        diag: Diagnostics | None = None,
    ) -> None:
        self._settings = settings
        self._state = state
        self._stats = stats
        self._clock = clock
        self._bus = bus
        self.diag = diag or Diagnostics(settings, clock)
        self._cross: dict[str, dict[str, _Crossing]] = {}  # asset -> exchange -> crossing
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
        return self._detect(asset, self._clock.mono(), source=source, max_age_ms=self.freshness_bound(source))

    def freshness_bound(self, source: str) -> float:
        """Data-age bound (ms) used for signals of ``source`` - the pipeline
        re-measures a signal with the same bound it was detected with."""
        max_age = float(self._settings.max_data_age_ms)
        if source == "poll":
            max_age += max(0.0, float(self._settings.ticker_poll_interval_s)) * 1000.0
        return max_age

    def allowed_direction(self, exchange: str, direction: int) -> bool:
        """LONG always (enabled exchange); SHORT only when ``allow_short``."""
        cfg = self._settings.exchange(exchange)
        if cfg is None or not cfg.enabled:
            return False
        if direction > 0:
            return True
        return bool(cfg.allow_short)

    def is_crossed(self, exchange: str, asset: str) -> bool:
        c = self._cross.get(asset, {}).get(exchange)
        return bool(c and c.crossed)

    def crossing_direction(self, exchange: str, asset: str) -> int:
        c = self._cross.get(asset, {}).get(exchange)
        return c.direction if c and c.crossed else 0

    def current_moves(self, asset: str, now_mono: float, max_age_ms: float | None = None) -> dict[str, Move]:
        """Live move per fresh exchange exactly as the detector sees it:
        anchored for crossed exchanges and, during an episode, measured from
        the episode's pre-move moment for the others; the sliding window
        otherwise.  The pipeline refreshes opportunities with this, so refresh
        and detection always agree."""
        max_age = self._settings.max_data_age_ms if max_age_ms is None else max_age_ms
        raw = self._window_moves(asset, now_mono, max_age)
        return self._effective_moves(asset, raw, self._episodes.get(asset))

    # ------------------------------------------------------------------ #
    # measurement helpers
    # ------------------------------------------------------------------ #
    def _window_moves(self, asset: str, now_mono: float, max_age_ms: float) -> dict[str, Move]:
        """Step 1: the sliding-window move per fresh exchange."""
        moves: dict[str, Move] = {}
        for ex in self._state.exchanges_for(asset):
            if not self._state.is_fresh(ex, asset, now_mono, max_age_ms):
                continue
            r = move_over_window(self._state.mid_series(ex, asset), now_mono, self._settings.move_window_s)
            if r is not None:
                moves[ex] = r
        return moves

    def _episode_anchor(self, ep: _Episode, exchange: str) -> float | None:
        """Pre-move price of ``exchange`` for this episode (cached): its mid at
        the episode's anchor moment, else the oldest point it has."""
        a = ep.anchors.get(exchange)
        if a is not None:
            return a
        a = self._state.mid_at(exchange, ep.asset, ep.anchor_mono)
        if a is None:
            series = self._state.mid_series(exchange, ep.asset)
            a = series[0][1] if series else None
        if a is None or a <= 0:
            return None
        ep.anchors[exchange] = a
        return a

    def _effective_moves(self, asset: str, raw: dict[str, Move], ep: _Episode | None) -> dict[str, Move]:
        cross = self._cross.get(asset, {})
        out: dict[str, Move] = {}
        for ex, r in raw.items():
            end_price = r[2]
            c = cross.get(ex)
            if c is not None and c.crossed and c.anchor_price > 0:
                out[ex] = (_pct(end_price, c.anchor_price), c.anchor_price, end_price, c.anchor_mono)
                continue
            if ep is not None:
                a = self._episode_anchor(ep, ex)
                if a is not None:
                    out[ex] = (_pct(end_price, a), a, end_price, ep.anchor_mono)
                    continue
            out[ex] = r
        return out

    # ------------------------------------------------------------------ #
    # core algorithm
    # ------------------------------------------------------------------ #
    def _detect(self, asset: str, now: float, *, source: str, max_age_ms: float) -> list[LagSignal]:
        s = self._settings
        thr = s.move_threshold_pct
        cap = max(float(s.move_window_s), float(s.max_opp_duration_s))
        raw = self._window_moves(asset, now, max_age_ms)
        cross = self._cross.setdefault(asset, {})
        ep = self._episodes.get(asset)

        # an episode that lasted longer than any opportunity can is over (not a reversion)
        if ep is not None and now - ep.start_mono > cap:
            for ex in ep.movers:
                c = cross.get(ex)
                if c is not None and c.crossed and c.direction == ep.direction:
                    c.crossed = False
                    c.direction = 0
            log.info("episode timed out %s after %.0fs (first_mover=%s)", asset, now - ep.start_mono, ep.first_mover)
            self._end_episode(ep)
            ep = None

        # ---- step 2: crossing state with hysteresis (anchored) -------------
        new_crossings: list[str] = []
        reverted_now: set[str] = set()
        for ex, (move, start_price, end_price, start_mono) in raw.items():
            c = cross.get(ex)
            if c is None:
                c = cross[ex] = _Crossing()
            if c.crossed:
                anchored = _pct(end_price, c.anchor_price)
                if anchored * c.direction < thr / 2.0:
                    log.debug("crossing reset %s %s: %+.3f%% vs anchor %.8g (reverted)", ex, asset, anchored, c.anchor_price)
                    c.crossed = False
                    c.direction = 0
                    c.anchor_until = now + s.move_window_s
                    reverted_now.add(ex)
                elif now - c.cross_mono > cap:
                    log.debug("crossing timed out %s %s after %.0fs", ex, asset, now - c.cross_mono)
                    c.crossed = False
                    c.direction = 0
            if not c.crossed:
                cand: tuple[float, float, float] | None = None  # (move_pct, start_price, start_mono)
                if c.anchor_price > 0 and now < c.anchor_until:
                    # just reverted: the window still holds the spike, so measure from the
                    # pre-move price (a reversion is not a move the other way)
                    m = _pct(end_price, c.anchor_price)
                    if abs(m) >= thr:
                        cand = (m, c.anchor_price, c.anchor_mono)
                elif abs(move) >= thr:
                    cand = (move, start_price, start_mono)
                elif ep is not None:
                    # slow catch-up: moved >= threshold since the episode began
                    a = self._episode_anchor(ep, ex)
                    if a is not None:
                        m = _pct(end_price, a)
                        if m * ep.direction >= thr:
                            cand = (m, a, ep.anchor_mono)
                if cand is not None:
                    m, anchor_price, anchor_mono = cand
                    c.crossed = True
                    c.direction = 1 if m > 0 else -1
                    c.cross_mono = now
                    c.move_at_cross = m
                    c.anchor_price = anchor_price
                    c.anchor_mono = anchor_mono
                    c.anchor_until = 0.0
                    new_crossings.append(ex)
                    log.info("MOVE %s %s %+.2f%% over %.1fs", ex, asset, m, max(0.0, now - anchor_mono))
                    self._publish_move(ex, asset, c.direction, m, anchor_price, end_price, anchor_mono, now)
        # exchanges without fresh data cannot be measured: clear silently
        for ex, c in cross.items():
            if ex not in raw and c.crossed:
                log.debug("crossing cleared (no fresh data) %s %s", ex, asset)
                c.crossed = False
                c.direction = 0

        crossed_now = [ex for ex in raw if cross[ex].crossed]

        # ---- step 3: episode ----------------------------------------------
        if ep is not None:
            if ep.movers & reverted_now:
                ep.reverted = True
            same = [ex for ex in crossed_now if cross[ex].direction == ep.direction]
            if not same:
                self._end_episode(ep)
                ep = None
            elif ep.first_mover not in same:
                ep.first_mover = min(same, key=lambda e: (cross[e].cross_mono, e))
        if ep is None and crossed_now:
            first = min(crossed_now, key=lambda e: (cross[e].cross_mono, e))
            c = cross[first]
            ep = _Episode(
                asset=asset, first_mover=first, direction=c.direction, start_mono=c.cross_mono, anchor_mono=c.anchor_mono
            )
            self._episodes[asset] = ep
            self._stats.record_first_mover(asset, first)
            self.diag.episode(asset, first)
            log.info("episode start %s first_mover=%s dir=%+d", asset, first, ep.direction)
        if ep is None:
            return []
        direction = ep.direction
        for ex in crossed_now:
            if cross[ex].direction == direction:
                ep.movers.add(ex)
        moves = self._effective_moves(asset, raw, ep)

        # ---- step 4: learning (execution exchanges crossing later) ---------
        exec_exchanges = set(s.execution_exchanges())
        if s.simple_mode:
            # the lag is measured against the leader, so the catch-up must be too (same reference)
            ref_ex = self._leader(asset)
            rc = cross.get(ref_ex) if ref_ex is not None else None
            ref_ok = bool(ref_ex in moves and rc is not None and rc.crossed and rc.direction == direction)
            ref_start = rc.cross_mono if ref_ok and rc is not None else ep.start_mono
        else:
            ref_ex, ref_ok, ref_start = ep.first_mover, ep.first_mover in moves, ep.start_mono
        first_move_now = moves[ref_ex][0] if ref_ok else 0.0
        for ex in new_crossings:
            if ex == ep.first_mover or ex == ref_ex or ex in ep.reacted or ex not in exec_exchanges:
                continue
            if cross[ex].direction != direction:
                continue
            if s.simple_mode and not ref_ok:
                continue  # the leader has not moved yet: nothing to measure this reaction against
            delay = max(0.0, now - ref_start)
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
        movers = [ex for ex in crossed_now if cross[ex].direction == direction]
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
        if s.simple_mode:
            # SIMPLE MODE: one venue decides - the one where the coin trades most (24h volume)
            leader = self._leader(asset)
            if leader is None:
                self.diag.reject("leader_unknown", asset, "", direction)
                return []
            if leader not in counted:
                self.diag.reject("leader_not_moved", asset, "", direction, detail=f"лидер {leader}")
                return []
            counted = [leader]
        elif len(counted) < max(1, int(s.min_movers)):
            dropped = [ex for ex in movers if ex not in counted]
            if not dropped:
                self.diag.reject("min_movers", asset, "", direction)
            else:
                unknown = [ex for ex in dropped if ex not in volumes]
                reason = "mover_volume_unknown" if len(unknown) == len(dropped) else "mover_volume"
                detail = " ".join(f"{ex}:{_fmt_vol(volumes.get(ex))}" for ex in dropped)
                self.diag.reject(reason, asset, "", direction, detail=detail)
            return []
        first = min(counted, key=lambda e: (cross[e].cross_mono, e))
        first_move = moves[first][0]
        first_cross_mono = cross[first].cross_mono
        peak = max(ep.peak_move.get(first, 0.0), first_move * direction)
        ep.peak_move[first] = peak  # ride the leader: has its move already turned?

        # ---- step 6: lagging execution exchanges ---------------------------
        signals: list[LagSignal] = []
        reacted_moves = {ex: m[0] for ex, m in moves.items()}
        for target in s.execution_exchanges():
            if target == first:
                continue
            if target not in moves:
                if target in self._state.exchanges_for(asset):
                    self.diag.reject("target_no_data", asset, target, direction)
                continue
            if not self.allowed_direction(target, direction):
                self.diag.reject("direction", asset, target, direction)
                continue
            tvol = self._state.volume24h(target, asset)
            if tvol is not None and s.min_target_volume_quote > 0 and tvol < s.min_target_volume_quote:
                self.diag.reject("target_volume", asset, target, direction)
                continue
            target_move = moves[target][0]
            lag = first_move * direction - target_move * direction
            if target in ep.lagging:
                ep.last_moves[target] = (first_move * direction, target_move * direction)
            if lag < s.min_lag_pct:
                self.diag.reject("min_lag", asset, target, direction)
                continue
            self.diag.signal(asset, target, direction)
            if target not in ep.lagging:
                ep.lagging.add(target)
                self._stats.record_lagging(asset, target, lag)
            ep.last_moves[target] = (first_move * direction, target_move * direction)
            learned = self._stats.catchup_ratio(asset, target)
            ratio = s.catchup_ratio_default if learned is None else learned  # a learned 0.0 is a result, not "no data"
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
                    leader_peak_move_pct=peak,
                )
            )
        signals.sort(key=lambda sg: sg.lag_pct, reverse=True)
        return signals

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    def _leader(self, asset: str) -> str | None:
        """Simple mode: the signal venue with the largest 24h quote volume for ``asset`` (None while
        no signal venue reported a volume - no guess)."""
        best, best_vol = None, 0.0
        for e in self._settings.exchanges:
            if not e.enabled or e.role != ExchangeRole.SIGNAL:
                continue
            vol = self._state.volume24h(e.name, asset)
            if vol is not None and vol > best_vol:
                best, best_vol = e.name, vol
        return best

    def _end_episode(self, ep: _Episode) -> None:
        self._episodes.pop(ep.asset, None)
        if ep.reverted:
            for target in sorted(ep.lagging - ep.reacted):
                self._stats.record_false_signal(ep.asset, target)
                log.info("false signal %s: %s reverted before %s reacted", ep.asset, ep.first_mover, target)
        # a lagging target that never crossed is a measurement too (often ~0): learning only from the
        # targets that did follow would keep the expected catch-up high on pairs that never pay
        for target in sorted(ep.lagging - ep.reacted):
            first, moved = ep.last_moves.get(target, (0.0, 0.0))
            ratio = max(0.0, min(1.5, moved / first)) if first > 0 else 0.0
            self._stats.record_catchup(ep.asset, target, ratio)
            log.info("no reaction %s on %s: catch-up ratio %.2f", ep.asset, target, ratio)
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
