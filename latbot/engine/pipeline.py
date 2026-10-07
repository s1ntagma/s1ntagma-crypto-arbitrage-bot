"""Pipeline - glue from live data to opportunities.

BOOK -> MarketState -> MoveDetector -> (per LagSignal) economics on the
target's latest book -> duration -> human / bot lights -> WHY ->
OpportunityManager.  A sweep loop expires opportunities, refreshes adapter
staleness and publishes DASHBOARD.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
from typing import Any

from .. import events
from ..clock import Clock
from ..config import Settings
from ..links import split_symbol
from ..market.features import move_over_window
from ..market.state import MarketState
from ..models import (
    ACTIVE_STATES,
    ExchangeHealth,
    Fees,
    LagSignal,
    Mode,
    Opportunity,
    OppState,
    OrderBook,
    Ticker,
    Trade,
)
from .duration import DurationEstimator
from .economics import compute_economics
from .executability import assess_bot, assess_human
from .explain import build_why
from .mover import MoveDetector
from .opportunity import OpportunityManager
from .stats import StatsStore

log = logging.getLogger(__name__)

_FEES_TTL_S = 3600.0
_STATS_SAVE_INTERVAL_S = 30.0


class Pipeline:
    """Wires market data, detection, economics and opportunities together."""

    def __init__(
        self,
        settings: Settings,
        bus: events.EventBus,
        clock: Clock,
        state: MarketState,
        detector: MoveDetector,
        stats: StatsStore | None,
        opps: OpportunityManager,
        adapters: dict[str, Any],
        duration: DurationEstimator,
        db: Any | None = None,
    ) -> None:
        self._settings = settings
        self._bus = bus
        self._clock = clock
        self._state = state
        self._detector = detector
        self._stats = stats
        self._opps = opps
        self._adapters = adapters
        self._duration = duration
        self._db = db
        self._fees_cache: dict[tuple[str, str], tuple[float, Fees]] = {}
        self._unsubs: list[Any] = []
        self._sweep_task: asyncio.Task | None = None
        self._last_stats_save_mono = clock.mono()
        self._stale_flags: dict[str, bool] = {}
        self._warned_fees: set[str] = set()
        listeners = getattr(detector, "reaction_listeners", None)
        if isinstance(listeners, list):
            listeners.append(self._on_reaction)

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        if self._unsubs:
            return
        self._unsubs = [
            self._bus.subscribe(events.BOOK, self.on_book),
            self._bus.subscribe(events.TRADE, self._on_trade),
            self._bus.subscribe(events.TICKER, self._on_ticker),
            self._bus.subscribe(events.HEALTH, self._on_health),
        ]
        self._sweep_task = asyncio.create_task(self._sweep_loop(), name="latbot-sweep")
        log.info("pipeline started (sweep every %d ms)", self._settings.ui_push_interval_ms)

    async def stop(self) -> None:
        task, self._sweep_task = self._sweep_task, None
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
        log.info("pipeline stopped")

    async def _sweep_loop(self) -> None:
        interval = max(0.05, self._settings.ui_push_interval_ms / 1000.0)
        while True:
            await asyncio.sleep(interval)
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("sweep failed")

    # ------------------------------------------------------------------ #
    # event handlers
    # ------------------------------------------------------------------ #
    def _on_trade(self, trade: Trade) -> None:
        self._state.on_trade(trade)

    def _on_health(self, h: ExchangeHealth) -> None:
        self._state.on_health(h)

    async def _on_ticker(self, ticker: Ticker) -> None:
        self._state.on_ticker(ticker)
        for sig in self._detector.on_ticker(ticker, source="ws"):
            await self.evaluate(sig)

    def _on_reaction(self, asset: str, target: str, direction: int, delay_s: float) -> None:
        self._opps.mark_caught_up(asset, target, direction)

    async def on_book(self, book: OrderBook) -> list[Opportunity]:
        """Ingest one book: detect, evaluate signals, refresh live opps on the
        target exchange that got no new signal (their net decays naturally)."""
        self._state.on_book(book)
        signals = self._detector.on_book(book)
        results: list[Opportunity] = []
        seen: set[str] = set()
        for sig in signals:
            await self._bus.publish(events.SIGNAL, sig)
            opp = await self.evaluate(sig)
            if opp is not None:
                results.append(opp)
                seen.add(opp.id)
        asset = MarketState.asset_of(book)
        for opp in self._opps.active():
            if opp.asset == asset and opp.target_exchange == book.exchange and opp.id not in seen:
                await self.refresh(opp)
        book.ts.proc_mono = self._clock.mono()
        return results

    # ------------------------------------------------------------------ #
    # evaluation
    # ------------------------------------------------------------------ #
    async def _target_book(self, signal: LagSignal) -> OrderBook | None:
        s = self._settings
        now = self._clock.mono()
        book = self._state.book(signal.target, signal.asset)
        stale = book is None or book.age_ms(now) > s.max_data_age_ms
        if signal.source == "poll" or stale:
            adapter = self._adapters.get(signal.target)
            fetch = getattr(adapter, "fetch_order_book", None) if adapter is not None else None
            symbol = signal.target_symbol or (book.symbol if book else "")
            if not (s.confirm_with_rest_book and fetch is not None and symbol):
                log.debug("no fresh book for %s on %s (source=%s)", signal.asset, signal.target, signal.source)
                return None
            try:
                fresh = await fetch(symbol)
            except Exception as exc:  # noqa: BLE001
                log.warning("REST book fetch failed %s %s: %s", signal.target, symbol, exc)
                return None
            if fresh is None:
                return None
            if not fresh.asset:
                fresh.asset = signal.asset
            self._state.on_book(fresh)
            book = fresh
            now = self._clock.mono()
        if book is None or book.age_ms(now) > s.max_data_age_ms:
            return None
        return book

    async def _fees(self, exchange: str, symbol: str) -> Fees:
        now = self._clock.mono()
        key = (exchange, symbol)
        cached = self._fees_cache.get(key)
        if cached is not None and now - cached[0] < _FEES_TTL_S:
            return cached[1]
        cfg = self._settings.exchange(exchange)
        fallback = Fees(maker=cfg.maker_fee, taker=cfg.taker_fee) if cfg else Fees(maker=0.001, taker=0.001)
        adapter = self._adapters.get(exchange)
        fees = fallback
        if adapter is not None and hasattr(adapter, "fees"):
            try:
                fees = await adapter.fees(symbol)
            except Exception as exc:  # noqa: BLE001
                log.warning("fees lookup failed %s %s: %s; using config fees", exchange, symbol, exc)
                fees = fallback
        elif exchange not in self._warned_fees:
            self._warned_fees.add(exchange)
            log.warning("no adapter for %s; using configured fees", exchange)
        self._fees_cache[key] = (now, fees)
        return fees

    def _health(self, exchange: str) -> ExchangeHealth | None:
        adapter = self._adapters.get(exchange)
        health = getattr(adapter, "health", None) if adapter is not None else None
        if isinstance(health, ExchangeHealth):
            return health
        return self._state.health(exchange)

    def _names(self) -> dict[str, str]:
        return {e.name: e.display for e in self._settings.exchanges}

    async def _assess(
        self, signal: LagSignal, book: OrderBook, capital: float, existing: Opportunity | None
    ) -> tuple[Any, Any, Any, str, float]:
        s = self._settings
        now = self._clock.mono()
        fees = await self._fees(signal.target, book.symbol)
        econ = compute_economics(book, signal.direction, fees, capital, signal.expected_catchup_pct, s, now)
        est = self._duration.estimate(signal, existing, now)
        age = existing.age_s(now) if existing is not None else 0.0
        remaining = max(0.0, est - age)
        rs = None
        if self._stats is not None:
            try:
                rs = self._stats.get(signal.asset, signal.target)
            except Exception:  # noqa: BLE001
                rs = None
        human = assess_human(remaining, econ.net_pct, s, stats=rs)
        bot = assess_bot(remaining, econ, self._health(signal.target), rs, s, now)
        quote = signal.target_quote or split_symbol(book.symbol)[1]
        why = build_why(signal, econ, human, bot, est, names=self._names(), quote=quote, max_slippage_pct=s.max_slippage_pct)
        return econ, human, bot, why, est

    async def evaluate(self, signal: LagSignal, capital_quote: float | None = None) -> Opportunity | None:
        """Turn a LagSignal into an Opportunity (or update the live one)."""
        s = self._settings
        book = await self._target_book(signal)
        if book is None:
            return None
        capital = float(capital_quote) if capital_quote is not None else float(s.capital_quote)
        existing = self._opps.find_active(signal.asset, signal.target, signal.direction)
        econ, human, bot, why, est = await self._assess(signal, book, capital, existing)
        if econ.net_pct < s.min_net_pct_show and existing is None:
            log.debug(
                "no opportunity %s on %s: net %+.2f%% < %.2f%%", signal.asset, signal.target, econ.net_pct, s.min_net_pct_show
            )
            return None
        opp, is_new = await self._opps.upsert(signal, econ, human, bot, why, est, s.mode())
        if is_new:
            await self._opps.transition(opp.id, OppState.VALIDATING, "economics computed")
            await self._opps.transition(opp.id, OppState.VALID, f"net {econ.net_pct:+.2f}% on real book")
        if signal.target_move_pct * signal.direction >= s.move_threshold_pct:
            opp.caught_up = True
        return opp

    def _live_signal(self, signal: LagSignal, now: float) -> LagSignal:
        """Re-measure the signal against live data: current target move and
        current first-mover move (unmeasurable movers count as no move)."""
        s = self._settings
        asset, target, d = signal.asset, signal.target, signal.direction
        moves = {}
        get_moves = getattr(self._detector, "current_moves", None)
        if callable(get_moves):
            moves = get_moves(asset, now)
        else:  # pragma: no cover - duck-typed detector
            for ex in self._state.exchanges_for(asset):
                if self._state.is_fresh(ex, asset, now, s.max_data_age_ms):
                    r = move_over_window(self._state.mid_series(ex, asset), now, s.move_window_s)
                    if r is not None:
                        moves[ex] = r
        target_move = moves[target][0] if target in moves else signal.target_move_pct
        ref_move = 0.0
        if signal.first_mover in moves:
            ref_move = moves[signal.first_mover][0]
        else:
            others = [moves[ex][0] for ex in signal.reacted if ex in moves and ex != target]
            if others:
                ref_move = max(others, key=lambda m: m * d)
        lag = max(0.0, ref_move * d - target_move * d)
        return dataclasses.replace(
            signal,
            first_move_pct=ref_move,
            target_move_pct=target_move,
            lag_pct=lag,
            expected_catchup_pct=lag * signal.catchup_ratio,
            reacted={ex: m[0] for ex, m in moves.items()} or dict(signal.reacted),
            detected_mono=now,
            detected_ts=self._clock.wall(),
        )

    async def refresh(self, opp: Opportunity, capital_quote: float | None = None) -> Opportunity:
        """Recompute economics/lights from the latest target book.
        Active opps are updated in place (OPP_UPDATE); executing / terminal
        opps get an updated shallow copy, the managed object is untouched."""
        s = self._settings
        now = self._clock.mono()
        book = self._state.book(opp.target_exchange, opp.asset)
        if book is None:
            log.debug("refresh %s: no book for %s on %s", opp.id, opp.asset, opp.target_exchange)
            return opp
        capital = float(capital_quote) if capital_quote is not None else float(opp.econ.capital_quote or s.capital_quote)
        live = self._live_signal(opp.signal, now)
        managed = self._opps.get(opp.id)
        if managed is not None and managed.state in ACTIVE_STATES:
            econ, human, bot, why, est = await self._assess(live, book, capital, managed)
            updated, _ = await self._opps.upsert(live, econ, human, bot, why, est, managed.mode)
            if live.target_move_pct * live.direction >= s.move_threshold_pct:
                updated.caught_up = True
            return updated
        econ, human, bot, why, est = await self._assess(live, book, capital, opp)
        copy = dataclasses.replace(
            opp,
            signal=live,
            econ=econ,
            human=human,
            bot=bot,
            why=why,
            estimated_duration_s=max(est, opp.estimated_duration_s if opp.state in ACTIVE_STATES else 0.0),
            last_update_mono=now,
            state_history=list(opp.state_history),
            net_history=list(opp.net_history) + [(now, econ.net_pct)],
        )
        return copy

    # ------------------------------------------------------------------ #
    # sweep / dashboard
    # ------------------------------------------------------------------ #
    async def sweep_once(self) -> list[Opportunity]:
        now = self._clock.mono()
        expired = await self._opps.sweep(now)
        for name, adapter in list(self._adapters.items()):
            check = getattr(adapter, "check_staleness", None)
            if check is None:
                continue
            try:
                stale = bool(check(now, self._settings.stale_exchange_s))
            except Exception:  # noqa: BLE001
                log.exception("check_staleness failed for %s", name)
                continue
            if self._stale_flags.get(name) != stale:
                self._stale_flags[name] = stale
                health = getattr(adapter, "health", None)
                if isinstance(health, ExchangeHealth):
                    self._state.on_health(health)
                    await self._bus.publish(events.HEALTH, health)
                if stale:
                    log.warning("exchange %s is stale (no messages for > %.0fs)", name, self._settings.stale_exchange_s)
                else:
                    log.info("exchange %s is live again", name)
        if self._stats is not None and now - self._last_stats_save_mono >= _STATS_SAVE_INTERVAL_S:
            self._last_stats_save_mono = now
            try:
                self._stats.save()
            except Exception:  # noqa: BLE001
                log.exception("stats save failed")
        await self._bus.publish(events.DASHBOARD, self.dashboard())
        return expired

    def dashboard(self) -> dict[str, Any]:
        now = self._clock.mono()
        mode = self._settings.mode()
        d = self._opps.counters(now)
        d["mode"] = mode.value
        exec_names = set(self._settings.execution_exchanges())
        lat = []
        for name, adapter in self._adapters.items():
            health = getattr(adapter, "health", None)
            if name in exec_names and isinstance(health, ExchangeHealth) and health.order_latency_ms is not None:
                lat.append(float(health.order_latency_ms))
        if lat:
            d["avg_latency_ms"] = sum(lat) / len(lat)
        if self._db is not None and hasattr(self._db, "counters_today"):
            for m in (Mode.PAPER, Mode.LIVE):
                try:
                    c = self._db.counters_today(m.value) or {}
                except Exception:  # noqa: BLE001
                    log.exception("counters_today failed for %s", m.value)
                    continue
                pnl = float(c.get("realized_pnl", 0.0) or 0.0)
                d[f"{m.value}_pnl"] = pnl
                if m is mode:
                    d["realized_pnl"] = pnl
                    d["failed_trades"] = int(c.get("failed", d.get("failed_trades", 0)) or 0)
                    d["today"] = max(int(d.get("today", 0)), int(c.get("opportunities_today", 0) or 0))
                    d["expired"] = max(int(d.get("expired", 0)), int(c.get("expired", 0) or 0))
                    trades = int(c.get("trades", 0) or 0)
                    if trades > 0:
                        d["success_rate"] = (trades - d["failed_trades"]) / trades
                    for extra in ("avg_execution_time_s", "avg_latency_ms"):
                        if c.get(extra) is not None:
                            d[extra] = c[extra]
        return d
