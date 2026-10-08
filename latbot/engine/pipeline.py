"""Pipeline - glue from live data to opportunities.

BOOK -> MarketState -> MoveDetector -> (per LagSignal) economics on the
target's latest FRESH book -> duration -> human / bot lights -> WHY ->
OpportunityManager.  A sweep loop expires opportunities, refreshes adapter
staleness and publishes DASHBOARD.

REST order-book confirmation (polled assets, stale WebSocket books) never
runs inside the BOOK handler: it is scheduled as a bounded background task
(single-flight per (exchange, symbol), at most ``_REST_CONCURRENCY`` requests
per exchange, ``rest_book_timeout_s`` each) and the re-measured signal is
evaluated once the fresh book is there.  ``refresh`` (MANUAL support, AUTO
revalidation) waits for the same single-flight fetch inline, so the 300 ms
modal poll can never hammer the REST API.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
from typing import Any, Callable

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
    median,
)
from .duration import DurationEstimator
from .economics import compute_economics
from .executability import assess_bot, assess_human
from .explain import build_plan, build_why
from .leverage import plan_leverage
from .quality import assess_quality
from ..exchanges.base import RateLimited
from .diagnostics import Diagnostics
from .mover import MoveDetector
from .opportunity import OpportunityManager
from .stats import StatsStore

log = logging.getLogger(__name__)

_FEES_TTL_S = 3600.0
_STATS_SAVE_INTERVAL_S = 30.0
_REST_CONCURRENCY = 2  # concurrent REST book fetches per exchange

BookKey = tuple[str, str]  # (exchange, symbol)
ConfirmKey = tuple[str, str, int]  # (target, asset, direction)


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
        diag: Diagnostics | None = None,
    ) -> None:
        self._settings = settings
        self._bus = bus
        self._clock = clock
        self._state = state
        self._detector = detector
        self.diag: Diagnostics = diag or getattr(detector, "diag", None) or Diagnostics(settings, clock)
        # Phase 3 narrow pool (universe.pool.AssetPool or anything with contains(asset)); None = scan everything.
        self.pool: Any = None
        # Phase 3: context.hub.ContextHub (factors / bars / funding / event windows) and the
        # meta-label store (learned outcomes of comparable situations); None -> not available.
        self.context: Any = None
        self.metalabel: Any = None
        self.pool_skipped: set[str] = set()  # assets seen but not scanned (outside the pool), for DIAG
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
        self._book_fetches: dict[BookKey, asyncio.Future] = {}  # in-flight REST book fetches (single-flight)
        self._last_fetch_mono: dict[BookKey, float] = {}
        self._rest_sems: dict[str, asyncio.Semaphore] = {}
        self._confirm_tasks: dict[ConfirmKey, asyncio.Task] = {}
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
        pending = [t for t in self._confirm_tasks.values() if not t.done()]
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._confirm_tasks.clear()
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

    @property
    def pending_confirmations(self) -> int:
        """REST book confirmations still running in the background."""
        return sum(1 for t in self._confirm_tasks.values() if not t.done())

    async def wait_confirmations(self) -> None:
        """Await every pending REST confirmation (tests, orderly shutdown)."""
        while True:
            pending = [t for t in self._confirm_tasks.values() if not t.done()]
            if not pending:
                return
            await asyncio.gather(*pending, return_exceptions=True)

    # ------------------------------------------------------------------ #
    # event handlers
    # ------------------------------------------------------------------ #
    def _on_trade(self, trade: Trade) -> None:
        self.diag.tick(trade.exchange, "trade")
        self._state.on_trade(trade)

    def _on_health(self, h: ExchangeHealth) -> None:
        self._state.on_health(h)

    def scanned(self, asset: str) -> bool:
        """False for an asset outside the Phase 3 pool: its prices are kept, it is never detected."""
        pool = self.pool
        if pool is None or not asset:
            return True
        try:
            ok = bool(pool.contains(asset))
        except Exception:  # noqa: BLE001
            log.exception("pool.contains failed for %s; scanning it", asset)
            return True
        if not ok and len(self.pool_skipped) < 5000:
            self.pool_skipped.add(asset)
        return ok

    async def _on_ticker(self, ticker: Ticker) -> None:
        self.diag.tick(ticker.exchange, "ticker")
        self._state.on_ticker(ticker)
        if not self.scanned(MarketState.asset_of(ticker)):
            return
        for sig in self._detector.on_ticker(ticker, source="ws"):
            await self.evaluate(sig)

    def _on_reaction(self, asset: str, target: str, direction: int, delay_s: float) -> None:
        self._opps.mark_caught_up(asset, target, direction)

    async def on_book(self, book: OrderBook) -> list[Opportunity]:
        """Ingest one book: detect, evaluate signals, refresh live opps on the
        target exchange that got no new signal (their net decays naturally).
        Never waits on REST: a stale target book is confirmed in the background."""
        self.diag.tick(book.exchange, "book")
        self._state.on_book(book)
        signals = self._detector.on_book(book) if self.scanned(MarketState.asset_of(book)) else []
        results: list[Opportunity] = []
        seen: set[str] = set()
        for sig in signals:
            await self._bus.publish(events.SIGNAL, sig)
            opp = await self.evaluate(sig)
            if opp is not None:
                results.append(opp)
                seen.add(opp.id)
        asset = MarketState.asset_of(book)
        for direction in (1, -1):
            opp = self._opps.find_active(asset, book.exchange, direction)
            if opp is not None and opp.id not in seen:
                await self.refresh(opp)
        book.ts.proc_mono = self._clock.mono()
        return results

    # ------------------------------------------------------------------ #
    # fresh target books (REST confirmation)
    # ------------------------------------------------------------------ #
    def _freshness_bound(self, source: str) -> float:
        """Data-age bound (ms) a signal of ``source`` was detected with."""
        bound = getattr(self._detector, "freshness_bound", None)
        if callable(bound):
            try:
                return float(bound(source))
            except Exception:  # noqa: BLE001 - duck-typed detector
                pass
        max_age = float(self._settings.max_data_age_ms)
        if source == "poll":
            max_age += max(0.0, float(self._settings.ticker_poll_interval_s)) * 1000.0
        return max_age

    def _fetcher(self, target: str) -> Any | None:
        adapter = self._adapters.get(target)
        fetch = getattr(adapter, "fetch_order_book", None) if adapter is not None else None
        if not self._settings.confirm_with_rest_book or fetch is None:
            return None
        return fetch

    async def _fetch_book(self, target: str, asset: str, symbol: str) -> OrderBook | None:
        """REST order book for ``(target, symbol)``: single-flight per key, at
        most ``_REST_CONCURRENCY`` requests per exchange, each bounded by
        ``rest_book_timeout_s``.  A failed attempt is not retried within
        ``max_data_age_ms / 2``.  The snapshot is stored in MarketState."""
        s = self._settings
        fetch = self._fetcher(target)
        if fetch is None or not symbol:
            return None
        key: BookKey = (target, symbol)
        inflight = self._book_fetches.get(key)
        if inflight is not None:
            return await asyncio.shield(inflight)
        now = self._clock.mono()
        last = self._last_fetch_mono.get(key)
        if last is not None and now - last < s.max_data_age_ms / 2000.0:
            log.debug("REST book %s %s: attempted %.2fs ago, not retrying yet", target, symbol, now - last)
            return None
        self._last_fetch_mono[key] = now
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._book_fetches[key] = fut
        fresh: OrderBook | None = None
        try:
            sem = self._rest_sems.get(target)
            if sem is None:
                sem = self._rest_sems[target] = asyncio.Semaphore(_REST_CONCURRENCY)
            try:
                async with sem:
                    fresh = await asyncio.wait_for(fetch(symbol), timeout=float(s.rest_book_timeout_s))
            except asyncio.TimeoutError:
                log.warning("REST book fetch %s %s timed out after %.1fs", target, symbol, s.rest_book_timeout_s)
            except asyncio.CancelledError:
                raise
            except RateLimited as exc:
                log.debug("REST book %s %s skipped: rate limited (%.0f s left)", target, symbol, exc.retry_after_s)
            except Exception as exc:  # noqa: BLE001
                log.warning("REST book fetch failed %s %s: %s", target, symbol, exc)
            if fresh is not None:
                if not fresh.asset:
                    fresh.asset = asset
                self._state.on_book(fresh)
            return fresh
        finally:
            if not fut.done():
                fut.set_result(fresh)
            if self._book_fetches.get(key) is fut:
                del self._book_fetches[key]

    async def _fresh_book(self, target: str, asset: str, symbol: str) -> OrderBook | None:
        """The latest book of ``(target, asset)`` not older than
        ``max_data_age_ms``; when the stored one is missing or older and REST
        confirmation is on, fetch it now (single-flight).  None = no fresh data."""
        s = self._settings
        book = self._state.book(target, asset)
        if book is not None and book.age_ms(self._clock.mono()) <= s.max_data_age_ms:
            return book
        fresh = await self._fetch_book(target, asset, symbol or (book.symbol if book else ""))
        if fresh is None or fresh.age_ms(self._clock.mono()) > s.max_data_age_ms:
            return None
        return fresh

    async def _target_book(self, signal: LagSignal) -> OrderBook | None:
        """Fresh target book for ``evaluate``.  A missing or stale book is
        confirmed through REST in a background task (the BOOK handler never
        waits on a throttled REST call); ``evaluate`` returns None until the
        snapshot is there and the confirmation re-evaluates the signal."""
        s = self._settings
        book = self._state.book(signal.target, signal.asset)
        if book is not None and book.age_ms(self._clock.mono()) <= s.max_data_age_ms:
            return book
        symbol = signal.target_symbol or (book.symbol if book else "")
        if self._fetcher(signal.target) is None or not symbol:
            log.debug("no fresh book for %s on %s (source=%s)", signal.asset, signal.target, signal.source)
            return None
        self._schedule_confirmation(signal, symbol)
        return None

    def _schedule_confirmation(self, signal: LagSignal, symbol: str) -> None:
        key: ConfirmKey = (signal.target, signal.asset, signal.direction)
        task = self._confirm_tasks.get(key)
        if task is not None and not task.done():
            return  # this (target, asset, direction) is already being confirmed
        task = asyncio.create_task(
            self._confirm_later(signal, symbol), name=f"latbot-confirm-{signal.asset}-{signal.target}"
        )
        self._confirm_tasks[key] = task
        task.add_done_callback(lambda t, k=key: self._confirm_tasks.pop(k, None) if self._confirm_tasks.get(k) is t else None)

    async def _confirm_later(self, signal: LagSignal, symbol: str) -> None:
        """Fetch the target book, re-measure the signal on live data and
        evaluate it on the fresh snapshot (no fake signals: lag gone -> nothing)."""
        s = self._settings
        try:
            book = await self._fetch_book(signal.target, signal.asset, symbol)
            if book is None:
                return
            now = self._clock.mono()
            if book.age_ms(now) > s.max_data_age_ms:
                return
            live = self._live_signal(signal, now)
            if live.lag_pct < s.min_lag_pct:
                log.debug("confirmation %s on %s: lag gone (%.2f%%)", signal.asset, signal.target, live.lag_pct)
                return
            await self.evaluate(live)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("REST confirmation failed for %s on %s", signal.asset, signal.target)

    # ------------------------------------------------------------------ #
    # evaluation
    # ------------------------------------------------------------------ #
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

    def _learned(self, asset: str, target: str) -> tuple[float | None, int, int, int, int]:
        """What StatsStore knows about (asset, target) for the plan:
        (median opportunity duration, caught_up_ok, caught_up_total,
        human_window_ok, human_window_total).  Nothing known -> (None, 0, 0, 0, 0)."""
        stats = self._stats
        if stats is None:
            return None, 0, 0, 0, 0
        try:
            rs = stats.get(asset, target)
            typical = None
            med = getattr(stats, "median_duration", None)
            if callable(med):
                typical = med(asset, target)
            elif rs is not None:
                typical = median(list(getattr(rs, "opp_durations_s", []) or []))
            if typical is not None and not typical > 0:
                typical = None
            return (
                float(typical) if typical is not None else None,
                int(getattr(rs, "caught_up_ok", 0) or 0),
                int(getattr(rs, "caught_up_total", 0) or 0),
                int(getattr(rs, "human_window_ok", 0) or 0),
                int(getattr(rs, "human_window_total", 0) or 0),
            )
        except Exception:  # noqa: BLE001 - learning never blocks a signal
            log.exception("stats lookup failed for %s on %s", asset, target)
            return None, 0, 0, 0, 0

    def _enricher(self) -> Callable[[Opportunity], None]:
        """Callback for ``OpportunityManager.upsert`` (runs before OPP_NEW /
        OPP_UPDATE is published): copies the learned history onto the
        opportunity, stamps ``last_update_ts`` and builds the Russian plan."""
        names = self._names()
        settings = self._settings
        clock = self._clock

        def enrich(opp: Opportunity) -> None:
            typical, cu_ok, cu_total, h_ok, h_total = self._learned(opp.asset, opp.target_exchange)
            opp.typical_window_s = typical
            opp.caught_up_ok, opp.caught_up_total = cu_ok, cu_total
            opp.human_ok, opp.human_total = h_ok, h_total
            opp.last_update_ts = clock.wall()
            opp.plan = build_plan(opp, settings, names, clock.mono(), opp.last_update_ts)
            self._score(opp)

        return enrich

    def _ctx(self, name: str, *args: Any) -> Any:
        """Call a ContextHub method; None when there is no hub or it fails (no data, never a guess)."""
        fn = getattr(self.context, name, None) if self.context is not None else None
        if fn is None:
            return None
        try:
            return fn(*args)
        except Exception:  # noqa: BLE001 - context never blocks a signal
            log.exception("context.%s failed", name)
            return None

    def _score(self, opp: Opportunity) -> None:
        """Phase 3: quality report + leverage plan on the latest economics (pure, in-memory)."""
        s = self._settings
        now_wall = self._clock.wall()
        try:
            factors = self._ctx("factors", opp.asset, opp.direction, now_wall) or []
            event_window = bool(self._ctx("event_window", now_wall))
            stats_asset = self._ctx("asset_stats", opp.asset) or {}
            stats_btc = stats_asset if opp.asset == "BTC" else (self._ctx("asset_stats", "BTC") or {})
            btc_pctile = stats_btc.get("vol_pctile") if isinstance(stats_btc, dict) else None
            ml = None
            if self.metalabel is not None:
                try:
                    ml = self.metalabel.estimate(opp.asset, opp.target_exchange, opp.first_mover, opp.signal.lag_pct, btc_pctile)
                except Exception:  # noqa: BLE001
                    log.exception("metalabel estimate failed for %s", opp.id)
            rs = None
            if self._stats is not None:
                try:
                    rs = self._stats.get(opp.asset, opp.target_exchange)
                except Exception:  # noqa: BLE001
                    rs = None
            report = assess_quality(opp.signal, opp.econ, opp.human, opp.bot, rs, factors, s,
                                    remaining_s=opp.remaining_s(self._clock.mono()), metalabel=ml, event_window=event_window)
            q = report.to_dict()
            q["btc_vol_pctile"] = btc_pctile
            q["event_window"] = event_window
            opp.quality = q
            funding = self._ctx("funding", opp.asset)
            rate = interval = next_ts = None
            if isinstance(funding, (tuple, list)) and len(funding) >= 3:
                rate, interval, next_ts = funding[0], funding[1], funding[2]
            sa = stats_asset if isinstance(stats_asset, dict) else {}
            plan = plan_leverage(
                opp.econ, report.score if report.passed else 0.0, s, s.exchange(opp.target_exchange),
                sigma_1m_pct=sa.get("vol_1m_pct"), tail_1m_pct=sa.get("tail_1m_pct"),
                hold_s=float(opp.estimated_duration_s or 0.0), funding_rate_8h=rate, funding_interval_h=interval,
                next_funding_ts=next_ts, now_wall=now_wall,
                p10=(ml or {}).get("p10"), n=(ml or {}).get("n"),
                total_capital_quote=float(getattr(s, "total_capital_quote", 0.0) or 0.0), event_window=event_window,
            )
            opp.leverage = plan.to_dict()
        except Exception:  # noqa: BLE001 - a scoring bug must never hide a real opportunity
            log.exception("quality / leverage scoring failed for %s", opp.id)

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
        human = assess_human(remaining, econ.net_pct, s, stats=rs, econ=econ)
        bot = assess_bot(remaining, econ, self._health(signal.target), rs, s, now)
        quote = signal.target_quote or split_symbol(book.symbol)[1]
        why = build_why(signal, econ, human, bot, est, names=self._names(), quote=quote, max_slippage_pct=s.max_slippage_pct)
        return econ, human, bot, why, est

    async def evaluate(self, signal: LagSignal, capital_quote: float | None = None) -> Opportunity | None:
        """Turn a LagSignal into an Opportunity (or update the live one).
        None when there is no fresh target book yet (a REST confirmation may
        be running in the background), when the NET is below the show
        threshold, or when the book does not support the minimum trade."""
        s = self._settings
        book = await self._target_book(signal)
        if book is None:
            h = self._health(signal.target)
            limited = h is not None and h.rate_limited_until_mono is not None and h.rate_limited_until_mono > self._clock.mono()
            self.diag.reject("no_fresh_book", signal.asset, signal.target, signal.direction,
                             detail="rate limited" if limited else "")
            return None
        capital = float(capital_quote) if capital_quote is not None else float(s.capital_quote)
        existing = self._opps.find_active(signal.asset, signal.target, signal.direction)
        econ, human, bot, why, est = await self._assess(signal, book, capital, existing)
        if existing is None:
            if econ.net_pct < s.min_net_pct_show:
                log.debug(
                    "no opportunity %s on %s: net %+.2f%% < %.2f%%", signal.asset, signal.target, econ.net_pct, s.min_net_pct_show
                )
                reason = "fees" if econ.gross_pct >= s.min_net_pct_show else "below_min_net"
                self.diag.reject(reason, signal.asset, signal.target, signal.direction,
                                 detail=f"gross {econ.gross_pct:+.2f}% net {econ.net_pct:+.2f}%")
                return None
            if econ.executable_quote < s.min_trade_quote:
                log.debug(
                    "no opportunity %s on %s: book supports %.2f < min trade %.2f",
                    signal.asset, signal.target, econ.executable_quote, s.min_trade_quote,
                )
                self.diag.reject("liquidity", signal.asset, signal.target, signal.direction)
                return None
        opp, is_new = await self._opps.upsert(signal, econ, human, bot, why, est, s.mode(), enrich=self._enricher())
        if is_new:
            self.diag.opportunity()
            q = opp.quality or {}
            if q:
                self.diag.quality(bool(q.get("passed")), "" if q.get("passed") else _quality_reason_key(q))
            await self._opps.transition(opp.id, OppState.VALIDATING, "economics computed")
            await self._opps.transition(opp.id, OppState.VALID, f"net {econ.net_pct:+.2f}% on real book")
        if signal.target_move_pct * signal.direction >= s.move_threshold_pct:
            opp.caught_up = True
        return opp

    def _live_signal(self, signal: LagSignal, now: float) -> LagSignal:
        """Re-measure the signal against live data: current target move and
        current first-mover move (unmeasurable movers count as no move), with
        the same freshness bound the detector used for this signal's source."""
        s = self._settings
        asset, target, d = signal.asset, signal.target, signal.direction
        max_age = self._freshness_bound(signal.source)
        moves = {}
        get_moves = getattr(self._detector, "current_moves", None)
        if callable(get_moves):
            moves = get_moves(asset, now, max_age)
        else:  # pragma: no cover - duck-typed detector
            for ex in self._state.exchanges_for(asset):
                if self._state.is_fresh(ex, asset, now, max_age):
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

    async def refresh(self, opp: Opportunity, capital_quote: float | None = None) -> Opportunity | None:
        """Recompute economics/lights from the latest FRESH target book; None
        when there is none (no data -> no numbers).  A book older than
        ``max_data_age_ms`` is re-fetched through REST when
        ``confirm_with_rest_book`` (single-flight, so the 300 ms MANUAL poll
        cannot hammer the API).  Active opps are updated in place
        (OPP_UPDATE); executing / terminal opps get an updated shallow copy,
        the managed object is untouched."""
        s = self._settings
        book = await self._fresh_book(opp.target_exchange, opp.asset, opp.target_symbol or opp.signal.target_symbol)
        if book is None:
            log.debug("refresh %s: no fresh book for %s on %s", opp.id, opp.asset, opp.target_exchange)
            return None
        now = self._clock.mono()
        capital = float(capital_quote) if capital_quote is not None else float(opp.econ.capital_quote or s.capital_quote)
        live = self._live_signal(opp.signal, now)
        managed = self._opps.get(opp.id)
        if managed is not None and managed.state in ACTIVE_STATES:
            econ, human, bot, why, est = await self._assess(live, book, capital, managed)
            updated, _ = await self._opps.upsert(live, econ, human, bot, why, est, s.mode(), enrich=self._enricher())
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
            plan=dict(opp.plan),
        )
        self._enricher()(copy)
        return copy

    # ------------------------------------------------------------------ #
    # sweep / dashboard
    # ------------------------------------------------------------------ #
    async def sweep_once(self) -> list[Opportunity]:
        now = self._clock.mono()
        s = self._settings
        expired: list[Opportunity] = []
        # the card always shows the mode that would execute (PAPER/LIVE switch)
        relabel = getattr(self._opps, "relabel_mode", None)
        if callable(relabel):
            await relabel(s.mode())
        # no fresh target book for longer than twice the freshness bound -> no data, no opportunity
        for opp in self._opps.active():
            book = self._state.book(opp.target_exchange, opp.asset)
            bound = 2.0 * self._freshness_bound(getattr(opp.signal, "source", "ws"))
            if book is None or book.age_ms(now) > bound:
                r = await self._opps.expire(opp.id, "target book stale")
                if r is not None:
                    expired.append(r)
        expired += await self._opps.sweep(now)
        for name, adapter in list(self._adapters.items()):
            check = getattr(adapter, "check_staleness", None)
            if check is None:
                continue
            try:
                stale = bool(check(now, self._settings.stale_exchange_s))
            except Exception:  # noqa: BLE001
                log.exception("check_staleness failed for %s", name)
                continue
            previous = self._stale_flags.get(name)
            if previous != stale:
                self._stale_flags[name] = stale
                health = getattr(adapter, "health", None)
                if isinstance(health, ExchangeHealth):
                    self._state.on_health(health)
                    await self._bus.publish(events.HEALTH, health)
                if not stale:
                    log.info("exchange %s is live", name)
                elif previous is None:
                    log.info("exchange %s: no market data yet", name)  # not connected so far, not a drop
                else:
                    log.warning("exchange %s is stale (no messages for > %.0fs)", name, self._settings.stale_exchange_s)
        if self._stats is not None and now - self._last_stats_save_mono >= _STATS_SAVE_INTERVAL_S:
            self._last_stats_save_mono = now
            try:
                self._stats.save()
            except Exception:  # noqa: BLE001
                log.exception("stats save failed")
        await self._bus.publish(events.DASHBOARD, self.dashboard())
        return expired

    def diagnostics(self) -> dict[str, Any]:
        """Funnel snapshot + "why no signals" (see engine.diagnostics)."""
        return self.diag.snapshot(self._state, self._adapters, self._opps.counters(self._clock.mono()), pool=self._pool_info())

    def _pool_info(self) -> dict[str, Any]:
        pool = self.pool
        skipped = sorted(self.pool_skipped)
        if pool is None:
            return {"enabled": False, "size": 0, "assets": [], "skipped_n": len(skipped), "skipped": skipped[:50]}
        try:
            assets = list(pool.assets()) if hasattr(pool, "assets") else []
        except Exception:  # noqa: BLE001
            log.exception("pool.assets failed")
            assets = []
        return {"enabled": True, "size": len(assets), "assets": assets, "skipped_n": len(skipped), "skipped": skipped[:50]}

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
                    # success = wins / finished trades (CLOSED + FAILED), the same definition as
                    # StatsStore.record_trade(success = pnl > 0); EXECUTING / CANCELLED rows do not count
                    closed = int(c.get("closed", 0) or 0)
                    wins = int(c.get("wins", 0) or 0)
                    finished = closed + d["failed_trades"]
                    d["success_rate"] = (wins / finished) if finished > 0 else None
                    for extra in ("avg_execution_time_s", "avg_latency_ms"):
                        if c.get(extra) is not None:
                            d[extra] = c[extra]
        return d


def _quality_reason_key(q: dict[str, Any]) -> str:
    """DIAG groups quality rejections by cause, not by text: veto texts carry countdowns and
    numbers ("CPI через 90 с"), so the key is the vetoing factor's label or "score below minimum"."""
    vetoes = [str(v) for v in (q.get("vetoes") or [])]
    if vetoes:
        v = vetoes[0]
        if v.startswith("NET"):
            return "вето: NET после издержек ≤ 0"
        return "вето: " + (v.split(":", 1)[0].strip() if ":" in v else v)[:60]
    return f"оценка ниже минимума {q.get('min_score', '')}".strip()
