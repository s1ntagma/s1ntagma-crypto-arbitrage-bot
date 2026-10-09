"""Application assembly and main loop (ARCHITECTURE §app.py).

``build_context``  wires every module into an ``AppContext`` (no network).
``run_app``        starts adapters (in the background, with retry), the
                   pipeline, recorder, Telegram, the HTTP/WebSocket server
                   (uvicorn inside the same event loop) and waits for a stop
                   condition: ``seconds`` elapsed, ``logs/stop.flag`` created,
                   SIGINT/SIGTERM.  Shutdown is graceful and every step is
                   guarded.
``main``           process entry: platform setup (Windows selector loop,
                   UTF-8 console), logging, secrets + settings (``config.json``
                   is created when missing), single-instance lock
                   (``logs/bot.lock``), ``logs/bot.pid``, restart loop with
                   backoff on crash.

Exit codes: 0 clean stop, 1 crash when a duration was given, 3 another
instance is already running.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib
import inspect
import logging
import os
import signal
import sys
import time
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

from . import VERSION, events
from .buildinfo import build_stamp
from .api.server import create_app
from .backtest.recorder import MarketRecorder
from .clock import Clock
from .config import Secrets, Settings, load_secrets, load_settings, save_settings
from .engine.diagnostics import Diagnostics
from .engine.metalabel import MetaLabelStore
from .engine.duration import DurationEstimator
from .engine.shadow import ShadowTrader
from .engine.mover import MoveDetector
from .engine.opportunity import OpportunityManager
from .engine.pipeline import Pipeline
from .engine.stats import StatsStore
from .events import EventBus
from .exchanges.base import ExchangeAdapter
from .exchanges.registry import (
    listing_map,
    AdapterFactory,
    build_execution_adapters,
    build_market_adapters,
    resolve_universe,
    resolve_watchlist,
)
from .execution.engine import ExecutionEngine
from .execution.risk import RiskManager
from .execution.autopilot import Autopilot
from .logging_setup import setup_logging
from .market.state import MarketState
from .models import Mode
from .notify.telegram import TelegramNotifier
from .storage.db import Database

log = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_CRASH = 1
EXIT_ALREADY_RUNNING = 3

_WATCHLIST_TIMEOUT_S = 20.0
_UNIVERSE_TIMEOUT_S = 60.0
_UNIVERSE_RETRY_MIN_S = 15.0
_UNIVERSE_RETRY_MAX_S = 120.0
_UNIVERSE_ATTEMPTS = 20
_POOL_FOLLOW_S = 30.0  # Phase 3: how often the REST poll set is matched to the pool
_LISTINGS_REFRESH_S = 3600.0  # listings (asset -> venues, 24h volume) for the pool's venue step
_LISTINGS_RETRY_S = 300.0
_ADAPTER_START_TIMEOUT_S = 120.0
_ADAPTER_RETRY_MIN_S = 5.0
_ADAPTER_RETRY_MAX_S = 60.0
_SHUTDOWN_STEP_S = 5.0
_EXEC_SHUTDOWN_EXTRA_S = 5.0  # on top of buy_fill_timeout_s: cancel a resting exit + market-exit the remainder
_SERVER_STOP_S = 10.0
_RESTART_BACKOFF_STEP_S = 10.0
_RESTART_BACKOFF_MAX_S = 120.0


# --------------------------------------------------------------------------- #
# Context
# --------------------------------------------------------------------------- #
@dataclass
class AppContext:
    """Everything the running application consists of (duck-typed by the API)."""

    settings: Settings
    secrets: Secrets
    bus: EventBus
    clock: Clock
    db: Database
    state: MarketState
    stats: StatsStore
    detector: MoveDetector
    duration: DurationEstimator
    opps: OpportunityManager
    adapters: dict[str, ExchangeAdapter]  # market-data adapters (name -> adapter)
    pipeline: Pipeline
    exec_adapters: dict[Mode, dict[str, ExchangeAdapter]]
    risk: RiskManager
    execution: ExecutionEngine
    diag: Diagnostics | None = None
    shadow: ShadowTrader | None = None
    notifier: TelegramNotifier | None = None
    recorder: MarketRecorder | None = None
    app: Any = None  # FastAPI application
    config_path: Path = Path("config.json")
    stop_flag: Path | None = None
    stop_flag_poll_s: float = 1.0
    watchlist: list[str] = field(default_factory=list)
    poll_assets: list[str] = field(default_factory=list)
    poller: Any = None  # market.poller.TickerPoller when available
    # Phase 3: narrow pool, market context (context.hub.ContextHub), meta-label store, shared HTTP client
    pool: Any = None
    context: Any = None
    metalabel: Any = None
    http: Any = None
    listings: dict[str, dict[str, float | None]] = field(default_factory=dict)
    autopilot: Autopilot | None = None  # AUTO without a click (Settings.autopilot)
    server: Any = None  # uvicorn Server while serving
    server_port: int | None = None  # bound port (port=0 -> OS assigned)
    stop_reason: str = ""
    request_stop: Callable[[str], None] | None = field(default=None, repr=False)

    @property
    def mode(self) -> Mode:
        return self.settings.mode()


def _build_context_layer(settings: Settings, secrets: Secrets, clock: Clock, db: Any, state: MarketState, stats: Any,
                         metalabel: Any, listings: dict[str, dict[str, float | None]], http: Any) -> tuple[Any, Any, Any]:
    """Phase 3: the asset pool (always - it works offline from its bundled fallback) and, with
    ``context_enabled``, the context collectors + ContextHub.  Best effort: a failing component is
    logged and left out (its factors then say "нет данных")."""
    data = _data_dir(settings)
    signal_venues = [e.name for e in settings.exchanges if e.enabled and e.role.value == "signal"]

    def price_fn(asset: str) -> float | None:
        # USD-quoted signal venues (Binance, KuCoin, ...): median of FRESH mids; None = the minute is not written
        return state.median_mid(asset, clock.mono(), max(float(settings.max_data_age_ms), 5000.0), signal_venues)

    bars = derivs = None
    context_on = bool(getattr(settings, "context_enabled", True))
    if context_on and http is None:
        from .context.base import AiohttpGetter

        http = AiohttpGetter()
    if context_on:
        try:
            from .context.bars import MinuteBars
            from .context.derivs import Derivatives

            bars = MinuteBars(settings, clock, price_fn, http)
            derivs = Derivatives(settings, clock, http, db=db)
        except Exception:  # noqa: BLE001
            log.exception("minute bars / derivatives not created")
    pool = None
    try:
        from .universe.pool import AssetPool

        pool = AssetPool(
            settings, clock, http if context_on else None, cache_path=data / "asset_pool.json",
            api_key=secrets.coingecko_api_key, listings_fn=lambda: listings,
            stability_fn=bars.stability if bars is not None else None,
            stability_pending_fn=getattr(bars, "stability_pending", None) if bars is not None else None,
            funding_interval_fn=derivs.funding_interval_h if derivs is not None else None,
        )
        if derivs is not None and hasattr(derivs, "_assets_fn"):
            derivs._assets_fn = pool.assets
    except Exception:  # noqa: BLE001
        log.exception("asset pool not created; every asset is scanned")
    if not context_on:
        return pool, None, http
    parts: dict[str, Any] = {"bars": bars, "derivs": derivs, "pool": pool, "metalabel": metalabel, "stats": stats, "db": db}
    builders: list[tuple[str, Callable[[], Any]]] = []

    def calendar() -> Any:
        from .context.calendar import BlsActuals, MacroCalendar

        cal = MacroCalendar(settings, clock, http, cache_path=data / "macro_calendar.json")
        parts["actuals"] = BlsActuals(settings, clock, http, cal, api_key=getattr(secrets, "bls_api_key", ""))
        return cal

    def markets() -> Any:
        from .context.markets import TradFiMarkets

        return TradFiMarkets(settings, clock, http)

    def fear_greed() -> Any:
        from .context.derivs import FearGreed

        return FearGreed(settings, clock, http)

    def etf() -> Any:
        from .context.derivs import EtfFlows

        return EtfFlows(settings, clock, http)

    def reactions() -> Any:
        from .context.reactions import EventReactionStore

        return EventReactionStore(settings, clock, price_fn, db)

    def news() -> Any:
        from .context.classify import ClaudeClassifier, HeadlineClassifier, RuleClassifier
        from .context.news import NewsFeed

        claude = ClaudeClassifier(settings, clock, api_key=secrets.anthropic_api_key)
        classifier = HeadlineClassifier(settings, clock, rules=RuleClassifier(), claude=claude)
        return NewsFeed(settings, clock, http, classifier=classifier, db=db,
                        assets_fn=pool.assets if pool is not None else (lambda: []))

    builders = [("calendar", calendar), ("markets", markets), ("fear_greed", fear_greed), ("etf", etf),
                ("reactions", reactions), ("news", news)]
    for name, build in builders:
        try:
            parts[name] = build()
        except Exception:  # noqa: BLE001
            log.exception("context component %s not created; its factors will say 'нет данных'", name)
            parts[name] = None
    try:
        from .context.hub import ContextHub

        hub = ContextHub(settings, clock, **parts)
    except Exception:  # noqa: BLE001
        log.exception("context hub not created; quality uses the engine components only")
        hub = None
    return pool, hub, http


def _data_dir(settings: Settings) -> Path:
    if settings.db_path and settings.db_path != ":memory:":
        return Path(settings.db_path).expanduser().parent
    return Path("data")


def _load_poller_class() -> Any | None:
    """``latbot.market.poller.TickerPoller`` when the module exists (Phase 2.3)."""
    try:
        module = importlib.import_module("latbot.market.poller")
    except ImportError:
        return None
    return getattr(module, "TickerPoller", None)


def build_context(
    settings: Settings,
    secrets: Secrets,
    *,
    bus: EventBus | None = None,
    clock: Clock | None = None,
    db: Database | None = None,
    adapter_factory: AdapterFactory | None = None,
    config_path: str | os.PathLike[str] = "config.json",
    stop_flag: str | os.PathLike[str] | None = None,
    http: Any = None,
) -> AppContext:
    """Wire every module together.  Touches no network (adapters connect in
    ``run_app``).  Optional components (Telegram, recorder, ticker poller, HTTP
    app) are built best effort: a failure is logged and the component is None."""
    bus = bus or EventBus()
    clock = clock or Clock()
    db = db or Database(settings.db_path, clock=clock)
    stats = StatsStore(settings, db)
    state = MarketState(settings, clock)
    diag = Diagnostics(settings, clock)
    detector = MoveDetector(settings, state, stats, clock, bus, diag=diag)
    duration = DurationEstimator(settings, stats)
    opps = OpportunityManager(settings, bus, clock, db=db, stats=stats)
    adapters = build_market_adapters(settings, secrets, bus, clock, adapter_factory=adapter_factory)
    pipeline = Pipeline(settings, bus, clock, state, detector, stats, opps, adapters, duration, db=db, diag=diag)
    metalabel = MetaLabelStore(settings, clock, db=db)
    from .engine.proven import ProvenBook

    proven = ProvenBook(settings, db=db)
    pipeline.proven = proven
    shadow = ShadowTrader(settings, bus, clock, db=db, diag=diag, metalabel=metalabel, market=pipeline.market_view,
                          proven=proven)
    pipeline.sweep_listeners.append(shadow.tick)  # ride the leader: virtual positions checked every sweep
    pipeline.metalabel = metalabel
    listings: dict[str, dict[str, float | None]] = {}
    pool, hub, http = _build_context_layer(settings, secrets, clock, db, state, stats, metalabel, listings, http)
    pipeline.pool = pool if getattr(settings, "pool_enabled", True) else None
    pipeline.context = hub
    exec_adapters = build_execution_adapters(settings, secrets, bus, clock, adapters, db, adapter_factory=adapter_factory)
    risk = RiskManager(settings, db, clock, bus)
    execution = ExecutionEngine(settings, bus, clock, pipeline, opps, exec_adapters, risk, db, stats)
    execution.readiness = shadow.readiness  # LIVE AUTO waits for the Stats READY checklist
    autopilot = Autopilot(settings, bus, execution, risk)  # idle until Settings.autopilot is on
    try:
        restored = execution.restore_from_db()  # positions a crash / restart left behind stay tracked
    except Exception:  # noqa: BLE001
        log.exception("restore of open trades from the database failed")
    else:
        if restored:
            log.error("%d open trade(s) restored from the database - see the trades tab / EMERGENCY CLOSE", len(restored))

    cfg_path = Path(config_path)
    notifier: TelegramNotifier | None = None
    try:
        notifier = TelegramNotifier(settings, secrets, bus, opps, execution, risk, clock, db=db, config_path=str(cfg_path), pipeline=pipeline)
    except Exception:  # noqa: BLE001
        log.exception("telegram notifier not created; continuing without Telegram")

    recorder: MarketRecorder | None = None
    try:
        recorder = MarketRecorder(settings, bus, clock, out_dir=_data_dir(settings) / "market")
    except Exception:  # noqa: BLE001
        log.exception("market recorder not created; continuing without recording")

    poller: Any = None
    poller_cls = _load_poller_class()
    if poller_cls is None:
        log.info("ticker polling unavailable (latbot.market.poller not installed); WebSocket coverage only")
    else:
        try:
            poller = poller_cls(settings, adapters, state, detector, pipeline, clock)
        except Exception:  # noqa: BLE001
            log.exception("ticker poller not created; WebSocket coverage only")

    ctx = AppContext(
        settings=settings,
        autopilot=autopilot,
        secrets=secrets,
        bus=bus,
        clock=clock,
        db=db,
        diag=diag,
        shadow=shadow,
        state=state,
        stats=stats,
        detector=detector,
        duration=duration,
        opps=opps,
        adapters=adapters,
        pipeline=pipeline,
        exec_adapters=exec_adapters,
        risk=risk,
        execution=execution,
        notifier=notifier,
        recorder=recorder,
        config_path=cfg_path,
        stop_flag=Path(stop_flag) if stop_flag is not None else None,
        poller=poller,
        pool=pool,
        context=hub,
        metalabel=metalabel,
        http=http,
        listings=listings,
    )
    try:
        ctx.app = create_app(ctx, config_path=cfg_path)
    except Exception:  # noqa: BLE001
        log.exception("HTTP application not created; continuing without UI")
        ctx.app = None
    log.info(
        "latbot %s context built: mode=%s exchanges=%s execution(paper)=%s execution(live)=%s",
        VERSION,
        settings.mode().value.upper(),
        ",".join(adapters) or "-",
        ",".join(exec_adapters.get(Mode.PAPER, {})) or "-",
        ",".join(exec_adapters.get(Mode.LIVE, {})) or "-",
    )
    return ctx


# --------------------------------------------------------------------------- #
# run_app helpers
# --------------------------------------------------------------------------- #
async def _resolve_watchlist(ctx: AppContext) -> list[str]:
    settings = ctx.settings
    try:
        watchlist = await asyncio.wait_for(resolve_watchlist(settings, ctx.adapters), _WATCHLIST_TIMEOUT_S)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        log.warning("watchlist resolution failed (%s: %s); using configured watchlist", type(exc).__name__, str(exc)[:120])
        watchlist = list(settings.watchlist)
    watchlist = [a.strip().upper() for a in watchlist if a and a.strip()]
    watchlist = list(dict.fromkeys(watchlist))
    if not watchlist:
        watchlist = ["BTC"]
    return watchlist


async def _resolve_poll_assets(ctx: AppContext) -> list[str]:
    """Phase 2.3 breadth: assets covered by REST ticker polling (best effort, [] on failure)."""
    if ctx.poller is None or not ctx.settings.ticker_poll_enabled:
        return []
    try:
        _ws, poll = await asyncio.wait_for(resolve_universe(ctx.settings, ctx.adapters), _UNIVERSE_TIMEOUT_S)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        log.info("poll universe unavailable (%s: %s)", type(exc).__name__, str(exc)[:120])
        return []
    return list(poll)


async def _upgrade_poll_universe(ctx: AppContext) -> None:
    """Background: resolve the common universe (needs the exchanges' REST API)
    and hand watchlist + universe to the poller.  Retries with backoff for a
    while; a bot without network keeps polling the watchlist only."""
    delay = _UNIVERSE_RETRY_MIN_S
    for _attempt in range(_UNIVERSE_ATTEMPTS):
        poll = await _resolve_poll_assets(ctx)
        if poll:  # (pool disabled: with the pool on, _follow_pool owns the poll set)
            ctx.poll_assets = poll
            assets = list(dict.fromkeys([*ctx.watchlist, *poll]))
            set_assets = getattr(ctx.poller, "set_assets", None)
            if callable(set_assets):
                set_assets(assets)
            log.info("ticker polling: %d assets (%d watchlist + %d universe)", len(assets), len(ctx.watchlist), len(poll))
            return
        await asyncio.sleep(delay)
        delay = min(_UNIVERSE_RETRY_MAX_S, delay * 2.0)
    log.info("poll universe not resolved; polling the watchlist only")


async def _refresh_listings(ctx: AppContext) -> bool:
    """asset -> {exchange: 24h volume} for the pool's venue step (best effort)."""
    try:
        fresh = await asyncio.wait_for(listing_map(ctx.settings, ctx.adapters), _UNIVERSE_TIMEOUT_S)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        log.info("listings unavailable (%s)", type(exc).__name__)
        return False
    if not fresh:
        log.info("listings: no exchange answered; the pool keeps its previous venue data")
        return False
    ctx.listings.clear()
    ctx.listings.update(fresh)
    if ctx.pool is not None:
        try:
            ctx.pool.reevaluate()
        except Exception:  # noqa: BLE001
            log.exception("pool reevaluate after listings failed")
    return True


async def _stream_pool(ctx: AppContext, pool_assets: list[str], tried: dict[str, set[str]]) -> None:
    """Assets that entered the pool after start get WebSocket streams (free ``ws_assets`` slots)
    instead of 2-second REST polling.  Each (exchange, asset) is offered once."""
    for name, adapter in ctx.adapters.items():
        add = getattr(adapter, "add_streams", None)
        if not callable(add):
            continue
        seen = tried.setdefault(name, set())
        new = [a for a in pool_assets if a not in seen]
        if not new or not getattr(adapter, "streamed_assets", lambda: [])():
            continue  # nothing new, or the adapter has not started streaming yet (retried next round)
        seen.update(new)
        try:
            n = await add(new)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.info("%s: new pool streams not started (%s)", name, type(exc).__name__)
            continue
        if n:
            log.info("%s: +%d WebSocket streams for new pool assets", name, n)


async def _follow_pool(ctx: AppContext, stop: asyncio.Event) -> None:
    """Phase 3: the pool changes at runtime (CoinGecko answers, stability gets measured, every
    pool_refresh_h).  Every 30 s the REST poll set is matched to watchlist + pool, so an asset that
    enters the pool is polled and can be detected; listings for the pool's venue step are refreshed
    hourly whether or not ticker polling is on.  WebSocket subscriptions stay as started."""
    applied: set[str] | None = None
    ws_tried: dict[str, set[str]] = {}  # exchange -> pool assets already offered a WebSocket stream
    next_listings = 0.0
    while not stop.is_set():
        try:
            mono = ctx.clock.mono()
            if ctx.adapters and mono >= next_listings:
                ok = await _refresh_listings(ctx)
                next_listings = mono + (_LISTINGS_REFRESH_S if ok else _LISTINGS_RETRY_S)
            pool = ctx.pipeline.pool
            set_assets = getattr(ctx.poller, "set_assets", None)
            if pool is not None and callable(set_assets) and ctx.settings.ticker_poll_enabled:
                pool_assets = list(pool.assets())
                wanted = list(dict.fromkeys([*ctx.watchlist, *pool_assets]))
                if set(wanted) != applied:
                    added = sorted(set(wanted) - (applied or set()))
                    removed = sorted((applied or set()) - set(wanted))
                    set_assets(wanted)
                    ctx.poll_assets = [a for a in wanted if a not in ctx.watchlist]
                    applied = set(wanted)
                    log.info("ticker polling follows the pool: %d assets (+%s -%s)", len(wanted),
                             ",".join(added) or "0", ",".join(removed) or "0")
            if pool is not None:
                await _stream_pool(ctx, list(pool.assets()), ws_tried)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("pool follow-up failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=_POOL_FOLLOW_S)
        except asyncio.TimeoutError:
            pass


def _accepts_kwarg(func: Any, name: str) -> bool:
    """True when ``func`` has a parameter ``name`` or ``**kwargs``."""
    try:
        sig = inspect.signature(func)
    except (TypeError, ValueError):
        return False
    return any(p.kind is inspect.Parameter.VAR_KEYWORD or p.name == name for p in sig.parameters.values())


async def _start_adapter(adapter: ExchangeAdapter, assets: list[str], ws_assets: int | None = None) -> None:
    """Start one adapter; on failure log and retry with backoff (5 -> 60 s).
    Never raises (except on cancellation) so a dead exchange cannot stop the app.
    ``ws_assets`` (0 = execution-only, no streams) is passed only to adapters whose
    ``start`` takes it (``CcxtAdapter``); the base signature is ``start(symbols)``."""
    delay = _ADAPTER_RETRY_MIN_S
    kwargs: dict[str, Any] = {}
    if ws_assets is not None and _accepts_kwarg(adapter.start, "ws_assets"):
        kwargs["ws_assets"] = int(ws_assets)
    while True:
        try:
            await asyncio.wait_for(adapter.start(list(assets), **kwargs), _ADAPTER_START_TIMEOUT_S)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            reason = type(exc).__name__ if isinstance(exc, asyncio.TimeoutError) else f"{type(exc).__name__}: {str(exc)[:120]}"
            try:
                adapter._mark_error(type(exc).__name__)
            except Exception:  # noqa: BLE001
                pass
            log.warning("%s: start failed (%s); retry in %.0f s", adapter.name, reason, delay)
            await asyncio.sleep(delay)
            delay = min(_ADAPTER_RETRY_MAX_S, delay * 2.0)
            continue
        log.info("%s: started (%d assets%s)", adapter.name, len(assets), ", execution-only" if ws_assets == 0 else "")
        return


def _adapters_to_start(ctx: AppContext) -> list[tuple[ExchangeAdapter, int | None]]:
    """``(adapter, ws_assets)`` for every adapter ``run_app`` starts and stops.

    Market adapters stream the watchlist (``None`` = the full ``settings.ws_assets``
    budget).  Real execution adapters - LIVE, and the Bybit demo adapter used as
    the paper venue - are started **execution-only** (``0``): they load markets,
    resolve symbols and assert the demo host, but open no streams of their own;
    their books and health come from the market adapter of the same exchange
    (``market_source``).  One stream set, one health object, one HEALTH/BOOK
    publisher per exchange.  ``PaperAdapter`` wraps the market adapter and needs
    no start of its own."""
    out: list[tuple[ExchangeAdapter, int | None]] = [(a, None) for a in ctx.adapters.values()]
    seen = {id(a) for a, _ in out}
    for adapter in ctx.exec_adapters.get(Mode.LIVE, {}).values():
        if id(adapter) not in seen:
            out.append((adapter, 0))
            seen.add(id(adapter))
    # a real demo adapter used as the paper venue (paper_engine == "bybit_demo")
    for name, adapter in ctx.exec_adapters.get(Mode.PAPER, {}).items():
        if id(adapter) in seen or getattr(adapter, "inner", None) is not None:
            continue
        if isinstance(adapter, ExchangeAdapter):
            out.append((adapter, 0))
            seen.add(id(adapter))
    return out


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, request_stop: Callable[[str], None]) -> Callable[[], None]:
    """SIGINT/SIGTERM -> request_stop("signal").  Returns a restore function."""
    restores: list[Callable[[], None]] = []
    sigs = [signal.SIGINT, signal.SIGTERM]
    if sys.platform == "win32":
        for name in ("SIGBREAK",):
            sig = getattr(signal, name, None)
            if sig is not None:
                sigs.append(sig)
        for sig in sigs:
            try:
                prev = signal.signal(sig, lambda *_args, _l=loop: _l.call_soon_threadsafe(request_stop, "signal"))
            except (ValueError, OSError, RuntimeError):  # not the main thread
                continue
            restores.append(lambda s=sig, p=prev: signal.signal(s, p))
    else:
        for sig in sigs:
            try:
                loop.add_signal_handler(sig, request_stop, "signal")
            except (NotImplementedError, RuntimeError, ValueError):
                continue
            restores.append(lambda s=sig, _l=loop: _l.remove_signal_handler(s))

    def restore() -> None:
        for fn in reversed(restores):
            try:
                fn()
            except Exception:  # noqa: BLE001
                pass

    return restore


def _make_server(ctx: AppContext) -> Any:
    """uvicorn Server for ``ctx.app`` that leaves signal handling to the app."""
    import uvicorn  # lazy: not needed for tests that run with serve=False

    class _QuietServer(uvicorn.Server):
        @contextlib.contextmanager
        def capture_signals(self):  # type: ignore[override]
            yield

    settings = ctx.settings
    config = uvicorn.Config(
        ctx.app,
        host=settings.host,
        port=int(settings.port),
        log_config=None,
        access_log=False,
        lifespan="on",
        loop="none",
        ws="auto",
        timeout_graceful_shutdown=5,
    )
    return _QuietServer(config)


async def _serve(ctx: AppContext, server: Any) -> None:
    settings = ctx.settings
    try:
        await server.serve()
    except asyncio.CancelledError:
        raise
    except (SystemExit, KeyboardInterrupt):
        log.error(
            "HTTP server could not start on %s:%s (port busy?) - running without the UI", settings.host, settings.port
        )
    except Exception:  # noqa: BLE001
        log.exception("HTTP server crashed - running without the UI")


async def _announce_server(ctx: AppContext, server: Any, server_task: asyncio.Task[None]) -> None:
    for _ in range(200):  # <= 10 s
        if getattr(server, "started", False):
            port: int | None = None
            try:
                servers = getattr(server, "servers", None) or []
                sockets = getattr(servers[0], "sockets", None) or [] if servers else []
                if sockets:
                    port = int(sockets[0].getsockname()[1])
            except Exception:  # noqa: BLE001
                port = None
            if port is None:
                port = int(ctx.settings.port)
            ctx.server_port = port
            log.info("UI: http://%s:%d/", ctx.settings.host, port)
            return
        if server_task.done():
            return
        await asyncio.sleep(0.05)


async def _sleep_then_stop(seconds: float, request_stop: Callable[[str], None]) -> None:
    await asyncio.sleep(max(0.0, float(seconds)))
    request_stop("seconds")


async def _watch_stop_flag(path: Path, poll_s: float, request_stop: Callable[[str], None]) -> None:
    interval = max(0.02, float(poll_s))
    while True:
        await asyncio.sleep(interval)
        try:
            exists = path.exists()
        except OSError:
            exists = False
        if exists:
            try:
                path.unlink()
            except OSError:
                pass
            request_stop("stop.flag")
            return


async def _guarded(what: str, coro: Any, timeout_s: float = _SHUTDOWN_STEP_S) -> None:
    """Await ``coro`` with a timeout; log (never raise) on failure."""
    try:
        await asyncio.wait_for(coro, timeout_s)
    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError:
        log.warning("%s: timed out after %.0f s", what, timeout_s)
    except Exception:  # noqa: BLE001
        log.exception("%s failed", what)


async def _cancel_tasks(tasks: Iterable[asyncio.Task[Any]]) -> None:
    pending = [t for t in tasks if not t.done()]
    for t in pending:
        t.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


# --------------------------------------------------------------------------- #
# run_app
# --------------------------------------------------------------------------- #
async def run_app(ctx: AppContext, seconds: float | None = None, *, serve: bool = True) -> int:
    """Run the application until ``seconds`` elapse, ``ctx.stop_flag`` appears
    or SIGINT/SIGTERM arrives.  Returns the process exit code (0)."""
    loop = asyncio.get_running_loop()
    settings = ctx.settings
    stop = asyncio.Event()
    ctx.stop_reason = ""
    ctx.server = None
    ctx.server_port = None

    def request_stop(reason: str) -> None:
        if stop.is_set():
            return
        ctx.stop_reason = reason
        log.info("stop requested (%s)", reason)
        stop.set()

    ctx.request_stop = request_stop
    helpers: list[asyncio.Task[Any]] = []
    server: Any = None
    server_task: asyncio.Task[None] | None = None
    restore_signals = _install_signal_handlers(loop, request_stop)
    log.info("latbot %s (build %s) starting in %s mode", VERSION, build_stamp(), settings.mode().value.upper())
    try:
        # ---- universe --------------------------------------------------
        ctx.watchlist = await _resolve_watchlist(ctx)
        if ctx.pipeline.pool is not None:
            narrowed = [a for a in ctx.watchlist if ctx.pipeline.scanned(a)]
            dropped = [a for a in ctx.watchlist if a not in narrowed]
            if dropped:
                log.info("watchlist: outside the narrow pool, not streamed: %s", ", ".join(dropped))
            # free WebSocket slots go to the next pool assets (the rest of the pool is REST-polled)
            try:
                extra = [a for a in ctx.pipeline.pool.assets() if a not in narrowed]
            except Exception:  # noqa: BLE001
                extra = []
            narrowed += extra[: max(0, int(settings.ws_assets) - len(narrowed))]
            ctx.watchlist = narrowed or ["BTC"]
        log.info("watchlist: %s", ", ".join(ctx.watchlist))

        # ---- adapters (background, never block the UI) -------------------
        for adapter, ws_assets in _adapters_to_start(ctx):
            helpers.append(
                asyncio.create_task(_start_adapter(adapter, ctx.watchlist, ws_assets), name=f"start-{adapter.name}")
            )

        # ---- engine ------------------------------------------------------
        await ctx.pipeline.start()
        if ctx.recorder is not None:
            await _guarded("recorder start", ctx.recorder.start())
        if ctx.notifier is not None:
            await _guarded("telegram start", ctx.notifier.start(), timeout_s=30.0)
        if ctx.execution is not None and hasattr(ctx.execution, "announce_open_positions"):
            await _guarded("announce open positions", ctx.execution.announce_open_positions())
        if ctx.poller is not None:
            # Poll the watchlist right away (REST tickers carry the 24h volumes the
            # Phase-2 gates need); widen to the common universe once exchanges answer.
            try:
                await _maybe_await(ctx.poller.start(list(ctx.watchlist)))
                if settings.ticker_poll_enabled and ctx.pipeline.pool is None:
                    helpers.append(asyncio.create_task(_upgrade_poll_universe(ctx), name="latbot-universe"))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("ticker poller start failed; WebSocket coverage only")
        if ctx.pool is not None:  # listings for the pool + (pool enabled) the REST poll set follows it
            helpers.append(asyncio.create_task(_follow_pool(ctx, stop), name="latbot-pool-follow"))
        if ctx.context is not None:
            helpers.append(asyncio.create_task(ctx.context.run(stop), name="latbot-context"))
        try:
            await ctx.bus.publish(events.DASHBOARD, ctx.pipeline.dashboard())
        except Exception:  # noqa: BLE001
            log.exception("initial dashboard publish failed")

        # ---- HTTP / WebSocket server -------------------------------------
        if serve and ctx.app is not None:
            try:
                server = _make_server(ctx)
            except Exception:  # noqa: BLE001
                log.exception("HTTP server not created - running without the UI")
                server = None
            if server is not None:
                ctx.server = server
                server_task = asyncio.create_task(_serve(ctx, server), name="latbot-http")
                helpers.append(asyncio.create_task(_announce_server(ctx, server, server_task), name="latbot-http-announce"))
        elif serve:
            log.warning("no HTTP application; running without the UI")

        # ---- stop conditions -----------------------------------------------
        if seconds is not None:
            helpers.append(asyncio.create_task(_sleep_then_stop(float(seconds), request_stop), name="latbot-timer"))
        if ctx.stop_flag is not None:
            helpers.append(
                asyncio.create_task(_watch_stop_flag(Path(ctx.stop_flag), ctx.stop_flag_poll_s, request_stop), name="latbot-stop-flag")
            )
        log.info("latbot running (stop: %s)", ", ".join(_stop_sources(ctx, seconds)))
        await stop.wait()
        log.info("latbot stopping (%s)", ctx.stop_reason or "unknown")
    finally:
        # ---- graceful shutdown (every step guarded) -------------------------
        await _cancel_tasks(helpers)
        if server is not None and server_task is not None:
            server.should_exit = True
            try:
                await asyncio.wait_for(asyncio.shield(server_task), _SERVER_STOP_S)
            except asyncio.TimeoutError:
                log.warning("HTTP server did not stop in %.0f s; forcing", _SERVER_STOP_S)
                server.force_exit = True
                await _cancel_tasks([server_task])
            except Exception:  # noqa: BLE001
                log.exception("HTTP server shutdown failed")
        ctx.server = None
        if ctx.execution is not None and hasattr(ctx.execution, "shutdown"):
            # running trades first: abort -> market exit while the adapters, Telegram and the db are still up
            drain_s = float(settings.buy_fill_timeout_s) + _EXEC_SHUTDOWN_EXTRA_S
            await _guarded("execution shutdown", ctx.execution.shutdown(timeout_s=drain_s), timeout_s=drain_s + _SHUTDOWN_STEP_S)
        if ctx.notifier is not None:
            await _guarded("telegram stop", ctx.notifier.stop(), timeout_s=15.0)
        if ctx.poller is not None and hasattr(ctx.poller, "stop"):
            await _guarded("ticker poller stop", _maybe_await(ctx.poller.stop()))
        if ctx.recorder is not None:
            await _guarded("recorder stop", ctx.recorder.stop())
        await _guarded("pipeline stop", ctx.pipeline.stop())
        stops = [_guarded(f"{a.name} stop", a.stop(), timeout_s=_SERVER_STOP_S) for a, _ in _adapters_to_start(ctx)]
        if stops:
            await asyncio.gather(*stops, return_exceptions=True)
        if ctx.http is not None and hasattr(ctx.http, "close"):
            await _guarded("context http close", ctx.http.close())
        try:
            ctx.stats.save()
        except Exception:  # noqa: BLE001
            log.exception("stats save failed")
        try:
            ctx.db.close()
        except Exception:  # noqa: BLE001
            log.exception("database close failed")
        restore_signals()
        ctx.request_stop = None
        log.info("latbot stopped (%s)", ctx.stop_reason or "shutdown")
    return EXIT_OK


def _stop_sources(ctx: AppContext, seconds: float | None) -> list[str]:
    out = ["signal"]
    if seconds is not None:
        out.append(f"after {float(seconds):g} s")
    if ctx.stop_flag is not None:
        out.append(str(ctx.stop_flag))
    return out


async def _maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value) or isinstance(value, asyncio.Future):
        return await value
    return value


# --------------------------------------------------------------------------- #
# process plumbing: platform, lock, pid, restart loop
# --------------------------------------------------------------------------- #
def _prepare_platform() -> None:
    """Windows: selector event loop (ccxt.pro / aiohttp need it) + UTF-8 console."""
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    if sys.platform == "win32":
        policy = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
        if policy is not None:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                try:
                    asyncio.set_event_loop_policy(policy())
                except Exception:  # noqa: BLE001
                    pass
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            pass


class InstanceLock:
    """Single-instance lock on a file (msvcrt on Windows, fcntl elsewhere).
    The handle stays open until ``release``; the lock dies with the process."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self._fh: Any = None

    @property
    def held(self) -> bool:
        return self._fh is not None

    def acquire(self) -> bool:
        if self._fh is not None:
            return True
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fh = open(self.path, "a+", encoding="utf-8")
        except OSError as exc:
            log.error("cannot open lock file %s: %s", self.path, exc)
            return False
        try:
            if sys.platform == "win32":
                import msvcrt

                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fh.close()
            return False
        try:
            fh.seek(0)
            fh.truncate()
            fh.write(f"{os.getpid()}\n")
            fh.flush()
        except OSError:
            pass
        self._fh = fh
        return True

    def release(self) -> None:
        fh, self._fh = self._fh, None
        if fh is None:
            return
        try:
            if sys.platform == "win32":
                import msvcrt

                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            fh.close()
        except OSError:
            pass
        try:
            self.path.unlink()
        except OSError:
            pass

    def __enter__(self) -> "InstanceLock":
        if not self.acquire():
            raise RuntimeError(f"lock {self.path} is held by another process")
        return self

    def __exit__(self, *exc: Any) -> None:
        self.release()


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="latbot", description="latbot - cross-exchange latency terminal")
    parser.add_argument("seconds", nargs="?", type=float, default=None, help="run for N seconds, then exit (smoke test)")
    parser.add_argument("--seconds", dest="seconds_opt", type=float, default=None, help="same as the positional SECONDS")
    parser.add_argument("--config", default="config.json", help="path to config.json (default: config.json)")
    parser.add_argument("--env", default=".env", help="path to .env (default: .env)")
    parser.add_argument("--no-server", action="store_true", help="do not start the HTTP/WebSocket server (debug)")
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.seconds is None and args.seconds_opt is not None:
        args.seconds = args.seconds_opt
    return args


async def _run_once(
    settings: Settings,
    secrets: Secrets,
    seconds: float | None,
    config_path: str | os.PathLike[str],
    stop_flag: Path | None,
    serve: bool,
) -> int:
    ctx = build_context(settings, secrets, config_path=config_path, stop_flag=stop_flag)
    return await run_app(ctx, seconds, serve=serve)


def main(argv: list[str] | None = None) -> int:
    """Process entry point (``python run.py [SECONDS]`` / ``python -m latbot``)."""
    args = _parse_args(argv)
    _prepare_platform()
    config_path = Path(args.config)
    secrets = load_secrets(args.env)
    settings = load_settings(config_path)
    setup_logging(settings.log_dir, settings.log_level)
    if not config_path.exists():
        try:
            config_path.parent.mkdir(parents=True, exist_ok=True)
            save_settings(settings, config_path)
            log.info("%s created with every parameter (edit it or use the UI settings)", config_path)
        except OSError as exc:
            log.error("could not write %s: %s", config_path, exc)
    log_dir = Path(settings.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    lock = InstanceLock(log_dir / "bot.lock")
    if not lock.acquire():
        log.error("latbot is already running (%s is locked); exit code %d", lock.path, EXIT_ALREADY_RUNNING)
        return EXIT_ALREADY_RUNNING
    pid_path = log_dir / "bot.pid"
    stop_flag = log_dir / "stop.flag"
    serve = not args.no_server
    try:
        try:
            pid_path.write_text(f"{os.getpid()}\n", encoding="utf-8")
        except OSError as exc:
            log.warning("could not write %s: %s", pid_path, exc)
        # A stale flag belongs to a bot that already exited (the lock proves it).
        try:
            if stop_flag.exists():
                stop_flag.unlink()
                log.info("stale %s removed", stop_flag)
        except OSError:
            pass
        failures = 0
        while True:
            try:
                rc = asyncio.run(_run_once(settings, secrets, args.seconds, config_path, stop_flag, serve))
            except KeyboardInterrupt:
                log.info("interrupted")
                return EXIT_OK
            except Exception:  # noqa: BLE001
                log.exception("latbot crashed")
                failures += 1
            else:
                return int(rc)
            if args.seconds is not None:
                return EXIT_CRASH
            if stop_flag.exists():
                try:
                    stop_flag.unlink()
                except OSError:
                    pass
                log.info("stop.flag present after crash; not restarting")
                return EXIT_OK
            delay = min(_RESTART_BACKOFF_MAX_S, _RESTART_BACKOFF_STEP_S * failures)
            log.warning("restart %d in %.0f s", failures, delay)
            time.sleep(delay)
            secrets = load_secrets(args.env)  # pick up edits made while we were down
            settings = load_settings(config_path)
    finally:
        try:
            pid_path.unlink()
        except OSError:
            pass
        lock.release()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(sys.argv[1:]))
