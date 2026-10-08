"""Adapter construction and watchlist resolution.

* ``build_market_adapters``    - one public (credential-less) adapter per enabled exchange.
* ``build_execution_adapters`` - PAPER: ``PaperAdapter`` wrapping the market adapter
  (or the real Bybit Demo adapter when ``paper_engine == "bybit_demo"``);
  LIVE: real adapters with credentials for execution exchanges (others omitted).
  A real execution adapter gets ``market_source = <market adapter>`` (when the
  adapter class has that attribute) so it shares the exchange's books and health
  instead of opening a second stream set; ``app.run_app`` starts it with
  ``ws_assets=0``.
* ``resolve_watchlist``        - ``settings.watchlist`` or, with ``watchlist_auto``,
  the most liquid assets on ``settings.watchlist_exchange`` (BTC always first).
* ``resolve_universe``         - (ws_assets, poll_assets) for Phase 2 coverage.

``adapter_factory(cfg, creds) -> ExchangeAdapter`` lets tests inject fakes; the
default builds ``CcxtAdapter`` (imported lazily - ccxt.pro is slow to import).
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable

from ..clock import Clock
from ..config import ExchangeConfig, ExchangeCredentials, Secrets, Settings
from ..events import EventBus
from ..links import split_symbol
from ..models import Mode, Ticker
from .base import ExchangeAdapter
from .paper import PaperAdapter

log = logging.getLogger(__name__)

AdapterFactory = Callable[[ExchangeConfig, "ExchangeCredentials | None"], ExchangeAdapter]

_YOUNG_MARKET_DAYS = 365.0


def _default_factory(settings: Settings, bus: EventBus, clock: Clock) -> AdapterFactory:
    def make(cfg: ExchangeConfig, creds: ExchangeCredentials | None) -> ExchangeAdapter:
        from .ccxt_adapter import CcxtAdapter  # lazy: ccxt.pro import is ~1 s

        return CcxtAdapter(cfg, bus, clock, settings, creds)

    return make


def build_market_adapters(
    settings: Settings,
    secrets: Secrets,
    bus: EventBus,
    clock: Clock,
    adapter_factory: AdapterFactory | None = None,
) -> dict[str, ExchangeAdapter]:
    """One market-data adapter (no credentials) per enabled exchange.
    Construction errors are logged and the exchange is skipped."""
    factory = adapter_factory or _default_factory(settings, bus, clock)
    out: dict[str, ExchangeAdapter] = {}
    for cfg in settings.exchanges:
        if not cfg.enabled:
            continue
        try:
            out[cfg.name] = factory(cfg, None)
        except Exception as exc:  # noqa: BLE001
            log.error("market adapter %s not created: %s: %s", cfg.name, type(exc).__name__, str(exc)[:200])
    return out


def _share_market_data(adapter: ExchangeAdapter, inner: ExchangeAdapter | None) -> None:
    """Point an execution-only adapter at the market adapter of its exchange
    (``CcxtAdapter.market_source``); adapters without the attribute are left alone."""
    if inner is None or inner is adapter or not hasattr(adapter, "market_source"):
        return
    try:
        adapter.market_source = inner  # type: ignore[attr-defined]
    except Exception as exc:  # noqa: BLE001
        log.warning("%s: market data not shared with the execution adapter: %s", adapter.name, type(exc).__name__)


def build_execution_adapters(
    settings: Settings,
    secrets: Secrets,
    bus: EventBus,
    clock: Clock,
    market: dict[str, ExchangeAdapter],
    db: Any | None,
    adapter_factory: AdapterFactory | None = None,
) -> dict[Mode, dict[str, ExchangeAdapter]]:
    """PAPER and LIVE execution adapters per execution exchange (see module doc)."""
    factory = adapter_factory or _default_factory(settings, bus, clock)
    result: dict[Mode, dict[str, ExchangeAdapter]] = {Mode.PAPER: {}, Mode.LIVE: {}}
    for name in settings.execution_exchanges():
        cfg = settings.exchange(name)
        if cfg is None:
            continue
        creds = secrets.creds(name)
        inner = market.get(name)
        # ---- PAPER ---------------------------------------------------------------
        paper: ExchangeAdapter | None = None
        if name == "bybit" and settings.paper_engine == "bybit_demo" and cfg.demo and creds.present:
            try:
                paper = factory(cfg, creds)
                _share_market_data(paper, inner)
                log.info("paper venue for bybit: Bybit Demo Trading (real adapter)")
            except Exception as exc:  # noqa: BLE001
                log.error("bybit demo adapter not created (%s: %s); falling back to internal paper", type(exc).__name__, str(exc)[:200])
        if paper is None:
            if inner is None:
                log.warning("no market adapter for %s; paper adapter skipped", name)
            else:
                paper = PaperAdapter(inner, settings, bus, clock, db=db, quote_assets=settings.quote_assets)
        if paper is not None:
            result[Mode.PAPER][name] = paper
        # ---- LIVE ------------------------------------------------------------------
        if not creds.present:
            continue
        if cfg.demo:
            log.info("%s: demo-configured; no LIVE adapter (set demo=false for real trading)", name)
            continue
        try:
            live = factory(cfg, creds)
        except Exception as exc:  # noqa: BLE001
            log.error("live adapter %s not created: %s: %s", name, type(exc).__name__, str(exc)[:200])
            continue
        _share_market_data(live, inner)
        result[Mode.LIVE][name] = live
    return result


# --------------------------------------------------------------------------- #
# Watchlist / universe
# --------------------------------------------------------------------------- #
def _ticker_asset_volume(key: str, t: Any) -> tuple[str, float | None]:
    """(asset, 24h quote volume) from a models.Ticker or a raw ccxt ticker dict."""
    if isinstance(t, Ticker):
        asset = t.asset or split_symbol(t.symbol)[0]
        return asset.upper(), t.quote_volume
    if isinstance(t, dict):
        sym = str(t.get("symbol") or key)
        asset = split_symbol(sym)[0] if "/" in sym else sym
        vol = t.get("quoteVolume")
        try:
            return asset.upper(), float(vol) if vol is not None else None
        except (TypeError, ValueError):
            return asset.upper(), None
    return str(key).upper(), None


def _market_age_days(market: dict[str, Any] | None, now: float) -> float | None:
    """Age in days from ``market['info']['launchTime']`` (ms or s); None if unknown."""
    if not isinstance(market, dict):
        return None
    info = market.get("info") or {}
    raw = info.get("launchTime") if isinstance(info, dict) else None
    if raw in (None, "", 0, "0"):
        return None
    try:
        ts = float(raw)
    except (TypeError, ValueError):
        return None
    if ts > 1e11:  # milliseconds
        ts /= 1000.0
    if ts <= 0:
        return None
    return (now - ts) / 86400.0


async def _ranked_assets(adapter: ExchangeAdapter, now: float) -> list[tuple[str, float]]:
    """(asset, volume) desc for every asset with a market on ``adapter``."""
    fetch = getattr(adapter, "fetch_tickers", None)
    if fetch is None:
        raise AttributeError("adapter has no fetch_tickers")
    tickers = await fetch()
    symbol_for = getattr(adapter, "symbol_for", None)
    market_of = getattr(adapter, "market", None)
    rows: dict[str, float] = {}
    for key, t in (tickers or {}).items():
        asset, vol = _ticker_asset_volume(str(key), t)
        if not asset:
            continue
        if symbol_for is not None and not symbol_for(asset):
            continue
        age = _market_age_days(market_of(asset), now) if market_of is not None else None
        if age is not None and age < _YOUNG_MARKET_DAYS:
            log.info("watchlist: %s skipped (market %.0f days old)", asset, age)
            continue
        rows[asset] = max(rows.get(asset, 0.0), vol or 0.0)
    return sorted(rows.items(), key=lambda r: r[1], reverse=True)


async def resolve_watchlist(settings: Settings, market: dict[str, ExchangeAdapter]) -> list[str]:
    """``settings.watchlist``, or with ``watchlist_auto`` the top assets by 24h quote
    volume on ``settings.watchlist_exchange`` (BTC first, young markets excluded,
    ``watchlist_size`` entries).  Falls back to ``settings.watchlist`` on any failure."""
    fallback = list(settings.watchlist)
    if not settings.watchlist_auto:
        return fallback
    adapter = market.get(settings.watchlist_exchange)
    if adapter is None:
        log.warning("watchlist_auto: exchange %s not available; using configured watchlist", settings.watchlist_exchange)
        return fallback
    try:
        ranked = await _ranked_assets(adapter, time.time())
    except Exception as exc:  # noqa: BLE001
        log.warning("watchlist_auto: %s fetch_tickers failed (%s: %s); using configured watchlist",
                    settings.watchlist_exchange, type(exc).__name__, str(exc)[:120])
        return fallback
    assets = [a for a, _ in ranked]
    if not assets:
        log.warning("watchlist_auto: no tickers from %s; using configured watchlist", settings.watchlist_exchange)
        return fallback
    symbol_for = getattr(adapter, "symbol_for", None)
    if "BTC" in assets:
        assets.remove("BTC")
        assets.insert(0, "BTC")
    elif symbol_for is None or symbol_for("BTC"):
        assets.insert(0, "BTC")
    result = assets[: max(1, int(settings.watchlist_size))]
    log.info("watchlist_auto (%s): %s", settings.watchlist_exchange, ", ".join(result))
    return result


async def listing_map(settings: Settings, adapters: dict[str, ExchangeAdapter]) -> dict[str, dict[str, float | None]]:
    """``asset -> {exchange: 24h quote volume}`` for every market on an ENABLED exchange
    (REST tickers; young markets skipped).  An adapter whose tickers fail is left out
    (best effort).  Used by the universe and by the Phase 3 asset pool."""
    out: dict[str, dict[str, float | None]] = {}
    now = time.time()
    for name, adapter in adapters.items():
        cfg = settings.exchange(name)
        if cfg is None or not cfg.enabled or not cfg.poll_tickers:
            continue
        try:
            ranked = await _ranked_assets(adapter, now)
        except Exception as exc:  # noqa: BLE001
            log.info("universe: %s tickers unavailable (%s)", name, type(exc).__name__)
            continue
        for asset, vol in ranked:
            out.setdefault(asset, {})[name] = vol
    return out


async def resolve_universe(settings: Settings, adapters: dict[str, ExchangeAdapter]) -> tuple[list[str], list[str]]:
    """Phase 2 coverage: ``(ws_assets, poll_assets)``.

    ws   = resolved watchlist (first ``settings.ws_assets``);
    poll = assets listed on >= 2 enabled exchanges with >= 1 EXECUTION venue,
           sorted by total 24h quote volume, capped at ``poll_assets_max``
           (watchlist assets excluded - they are streamed already).  Best effort:
           an adapter whose tickers fail is skipped.
    """
    ws = (await resolve_watchlist(settings, adapters))[: max(1, int(settings.ws_assets))]
    listings = await listing_map(settings, adapters)
    listed: dict[str, set[str]] = {a: set(v) for a, v in listings.items()}
    volume: dict[str, float] = {a: sum((x or 0.0) for x in v.values()) for a, v in listings.items()}
    exec_names = {n for n in settings.execution_exchanges()}
    poll = [
        a for a, exs in listed.items()
        if len(exs) >= 2 and (exs & exec_names) and a not in ws
    ]
    poll.sort(key=lambda a: volume.get(a, 0.0), reverse=True)
    return ws, poll[: max(0, int(settings.poll_assets_max))]


__all__ = [
    "AdapterFactory",
    "build_market_adapters",
    "build_execution_adapters",
    "listing_map",
    "resolve_watchlist",
    "resolve_universe",
]
