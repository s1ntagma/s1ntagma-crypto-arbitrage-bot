"""Live data for the basis scanner: per venue a SPOT and a PERP ``CcxtAdapter`` (public, no keys) on a
PRIVATE event bus - the latency bot's pipeline, poller and diagnostics never see these books.

Perp books count contracts: their sizes are scaled to base units (``contract_size``) before the scanner
sees them.  Funding rates are polled every ``basis_funding_poll_s`` (then the funding carry reviews its positions);
the simulator ticks 4 times a second.
Research only: the runner never calls ``place_order``.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
from typing import Any, Callable

from .. import events
from ..config import ExchangeConfig, Settings
from ..events import EventBus
from ..models import BookLevel, ExchangeRole, OrderBook
from .scanner import BasisScanner

log = logging.getLogger(__name__)

_TICK_S = 0.25
_ASSETS_REFRESH_S = 60.0
_HISTORY_REFRESH_S = 4 * 3600.0  # settled funding history of every pair (carry rate estimate, interval)  # the pool answers from its cache first, the full top-N a little later
# venue -> (spot ccxt id, perp ccxt id, spot taker, perp taker, quote).  Both legs use the SAME quote (no FX
# inside the basis): Kraken Futures perps settle in USD, so Kraken spot is BTC/USD, not BTC/USDT.
# Fees are replaced by the venue's own (adapter.fees) when known.
# Default taker fees are the published base tier (conservative: the higher of this and the library's
# market fee is charged, the account's own fee wins when keys are set).
VENUES: dict[str, tuple[str, str, float, float, str]] = {
    "binance": ("binance", "binance", 0.001, 0.0005, "USDT"),
    "okx": ("okx", "okx", 0.001, 0.0005, "USDT"),
    "mexc": ("mexc", "mexc", 0.0005, 0.0002, "USDT"),
    "gate": ("gate", "gate", 0.002, 0.0005, "USDT"),
    "bitget": ("bitget", "bitget", 0.001, 0.0006, "USDT"),
    "bybit": ("bybit", "bybit", 0.001, 0.00055, "USDT"),
    "htx": ("htx", "htx", 0.002, 0.0005, "USDT"),
    "kraken": ("kraken", "krakenfutures", 0.004, 0.0005, "USD"),
}

LegFactory = Callable[[ExchangeConfig, Any], Any]


def leg_name(venue: str, leg: str) -> str:
    return f"{venue}.{leg}"


def leg_configs(venue: str) -> dict[str, ExchangeConfig] | None:
    spec = VENUES.get(venue)
    if spec is None:
        return None
    spot_id, perp_id, spot_fee, perp_fee, _quote = spec
    common = dict(role=ExchangeRole.SIGNAL, stream="book", book_depth=20, poll_tickers=False)
    return {
        "spot": ExchangeConfig(name=leg_name(venue, "spot"), ccxt_id=spot_id, market_type="spot",
                               taker_fee=spot_fee, maker_fee=spot_fee, label=f"{venue} spot", **common),
        "perp": ExchangeConfig(name=leg_name(venue, "perp"), ccxt_id=perp_id, market_type="swap",
                               taker_fee=perp_fee, maker_fee=perp_fee, label=f"{venue} perp", **common),
    }


class BasisRunner:
    def __init__(self, settings: Settings, clock: Any, db: Any | None = None, *,
                 leg_factory: LegFactory | None = None, asset_source: Callable[[], list[str]] | None = None) -> None:
        self._settings = settings
        self._asset_source = asset_source  # the bot's pool (market-cap order) when basis_assets is empty
        self._clock = clock
        self.bus = EventBus()  # private: these books never reach the latency pipeline
        self.scanner = BasisScanner(settings, clock, db=db)
        self._factory = leg_factory or self._default_factory
        self.legs: dict[str, dict[str, Any]] = {}  # venue -> {"spot": adapter, "perp": adapter}
        self._route: dict[str, tuple[str, str]] = {}  # adapter name -> (venue, leg)
        self._tasks: list[asyncio.Task] = []
        self._known: set[str] = set()  # assets already offered to the legs
        self._unsub = self.bus.subscribe(events.BOOK, self._on_book)
        for venue in dict.fromkeys(str(v).strip().lower() for v in settings.basis_venues):
            cfgs = leg_configs(venue)
            if cfgs is None:
                log.warning("basis: unknown venue %s (known: %s)", venue, ", ".join(VENUES))
                continue
            legs: dict[str, Any] = {}
            for leg, cfg in cfgs.items():
                try:
                    legs[leg] = self._factory(cfg, None)
                    self._route[cfg.name] = (venue, leg)
                except Exception as exc:  # noqa: BLE001
                    log.error("basis: %s adapter not created: %s: %s", cfg.name, type(exc).__name__, str(exc)[:160])
            if len(legs) == 2:
                self.legs[venue] = legs

    def _default_factory(self, cfg: ExchangeConfig, creds: Any) -> Any:
        from ..exchanges.ccxt_adapter import CcxtAdapter  # lazy: ccxt.pro import is slow

        venue = cfg.name.split(".", 1)[0]
        quote = VENUES[venue][4]
        # each leg resolves exactly the venue's quote, so both legs share it; the stream budget covers basis_top_n
        top_n = int(getattr(self._settings, "basis_top_n", 100) or 100)
        legs_settings = dataclasses.replace(self._settings, signal_quote_assets=[quote], quote_assets=[quote],
                                            ws_assets=max(int(self._settings.ws_assets), top_n))
        return CcxtAdapter(cfg, self.bus, self._clock, legs_settings, None)

    @property
    def assets(self) -> list[str]:
        """``basis_assets`` when set, else the first ``basis_top_n`` coins of the bot's pool."""
        explicit = [str(a).strip().upper() for a in (self._settings.basis_assets or []) if str(a).strip()]
        if explicit:
            return list(dict.fromkeys(explicit))
        source: list[str] = []
        if self._asset_source is not None:
            try:
                source = [str(a).upper() for a in (self._asset_source() or [])]
            except Exception:  # noqa: BLE001
                log.exception("basis: asset source failed")
        n = max(1, int(getattr(self._settings, "basis_top_n", 100) or 100))
        return list(dict.fromkeys(source))[:n]

    # ------------------------------------------------------------------ lifecycle
    async def start(self, wait_s: float = 120.0) -> None:
        assets = self.assets
        waited = 0.0
        while not assets and self._asset_source is not None and waited < wait_s:
            await asyncio.sleep(2.0)  # the pool answers a little after start (CoinGecko / cache)
            waited += 2.0
            assets = self.assets
        if not assets:
            log.warning("basis: no assets (basis_assets empty and the pool gave none); scanner idle")
        else:
            log.info("basis: %d assets (%s%s)", len(assets), ", ".join(assets[:10]), ", ..." if len(assets) > 10 else "")
        await asyncio.gather(*(self._start_venue(v, legs, assets) for v, legs in self.legs.items()),
                             return_exceptions=True)
        self._tasks = [asyncio.create_task(self._tick_loop(), name="basis-tick"),
                       asyncio.create_task(self._funding_loop(), name="basis-funding"),
                       asyncio.create_task(self._assets_loop(), name="basis-assets"),
                       asyncio.create_task(self._history_loop(), name="basis-funding-history")]

    async def _start_venue(self, venue: str, legs: dict[str, Any], assets: list[str]) -> None:
        self._known.update(assets)
        for leg, adapter in legs.items():
            try:
                await adapter.start(assets, ws_assets=len(assets))
            except Exception as exc:  # noqa: BLE001
                log.warning("basis: %s did not start: %s: %s", adapter.name, type(exc).__name__, str(exc)[:160])
        await self._register(venue, legs, assets)
        log.info("basis: %s ready (%s)", venue, ", ".join(p["asset"] for p in self.scanner.pairs() if p["venue"] == venue))

    async def refresh_assets_once(self) -> int:
        """Offer assets that entered the pool after start to every venue; running streams are kept.
        Returns the number of new assets offered."""
        new = [a for a in self.assets if a not in self._known]
        if not new:
            return 0
        self._known.update(new)
        for venue, legs in self.legs.items():
            for adapter in legs.values():
                try:
                    if getattr(adapter, "streamed_assets", lambda: [])():
                        await adapter.add_streams(new)
                    else:  # did not stream yet (start failed / empty pool): start with everything known
                        await adapter.start(sorted(self._known), ws_assets=len(self._known))
                except Exception as exc:  # noqa: BLE001
                    log.warning("basis: %s new streams failed: %s: %s", adapter.name, type(exc).__name__, str(exc)[:160])
            await self._register(venue, legs, new)
        log.info("basis: +%d assets (%s)", len(new), ", ".join(new[:10]))
        return len(new)

    async def _assets_loop(self) -> None:
        while True:
            await asyncio.sleep(_ASSETS_REFRESH_S)
            try:
                await self.refresh_assets_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("basis asset refresh failed")

    async def _register(self, venue: str, legs: dict[str, Any], assets: list[str]) -> None:
        spot, perp = legs["spot"], legs["perp"]
        for asset in assets:
            ss, ps = _symbol(spot, asset), _symbol(perp, asset)
            if not ss or not ps:
                continue
            if self.scanner.register_pair(venue, asset, ss, ps):
                for leg, adapter, sym in (("spot", spot, ss), ("perp", perp, ps)):
                    try:
                        fees = await adapter.fees(sym)
                        self.scanner.set_fee(venue, asset, leg, fees.taker)
                    except Exception as exc:  # noqa: BLE001
                        log.debug("basis: fees %s %s: %s", adapter.name, sym, exc)

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        stops = [a.stop() for legs in self.legs.values() for a in legs.values() if hasattr(a, "stop")]
        await asyncio.gather(*stops, return_exceptions=True)
        self.scanner.flush()

    # ------------------------------------------------------------------ data
    def _on_book(self, book: OrderBook) -> None:
        route = self._route.get(book.exchange)
        if route is None:
            return
        venue, leg = route
        if leg == "perp":
            adapter = self.legs.get(venue, {}).get("perp")
            size = _contract_size(adapter, book.symbol)
            if size != 1.0:
                book = dataclasses.replace(
                    book, bids=[BookLevel(lv.price, lv.size * size) for lv in book.bids],
                    asks=[BookLevel(lv.price, lv.size * size) for lv in book.asks])
        self.scanner.on_book(venue, leg, book)

    async def _tick_loop(self) -> None:
        while True:
            try:
                self.scanner.tick()
            except Exception:  # noqa: BLE001
                log.exception("basis tick failed")
            await asyncio.sleep(_TICK_S)

    async def poll_funding_once(self) -> int:
        """Funding of every pair: all venues at once, one batch call per venue where it has one."""
        counts = await asyncio.gather(*(self._poll_venue_funding(v, legs["perp"]) for v, legs in self.legs.items()),
                                      return_exceptions=True)
        n = sum(c for c in counts if isinstance(c, int))
        try:
            self.scanner.carry_review()  # funding carry decides on fresh rates
        except Exception:  # noqa: BLE001
            log.exception("basis: carry review failed")
        return n

    async def _poll_venue_funding(self, venue: str, perp: Any) -> int:
        fetch = getattr(perp, "fetch_funding_rate", None)
        pairs = [p for p in self.scanner.pairs() if p["venue"] == venue and p.get("quote_ok")]
        if fetch is None or not pairs:
            return 0
        batch: dict[str, Any] = {}
        many = getattr(perp, "fetch_funding_rates", None)
        if many is not None:
            try:
                batch = await many([p["perp_symbol"] for p in pairs]) or {}
            except Exception as exc:  # noqa: BLE001
                log.debug("basis: funding batch %s: %s", venue, exc)
        n = 0
        for p in pairs:
            r = batch.get(p["perp_symbol"])
            if r is None:
                try:
                    r = await fetch(p["perp_symbol"])
                except Exception as exc:  # noqa: BLE001
                    log.debug("basis: funding %s %s: %s", venue, p["perp_symbol"], exc)
                    continue
            self.scanner.set_funding(venue, p["asset"], r.get("rate"), r.get("interval_h"), r.get("next_ts"))
            n += 1
        return n

    async def poll_history_once(self) -> int:
        """Settled funding history of every pair (all venues at once, one pair after another per venue)."""
        async def venue_history(venue: str, perp: Any) -> int:
            fetch = getattr(perp, "fetch_funding_history", None)
            if fetch is None:
                return 0
            n = 0
            for p in [p for p in self.scanner.pairs() if p["venue"] == venue and p.get("quote_ok")]:
                try:
                    rows = await fetch(p["perp_symbol"], int(getattr(self._settings, "carry_history_n", 21) or 21))
                except Exception as exc:  # noqa: BLE001
                    log.debug("basis: funding history %s %s: %s", venue, p["perp_symbol"], exc)
                    continue
                if rows:
                    self.scanner.set_funding_history(venue, p["asset"], rows)
                    n += 1
            return n

        counts = await asyncio.gather(*(venue_history(v, legs["perp"]) for v, legs in self.legs.items()),
                                      return_exceptions=True)
        return sum(c for c in counts if isinstance(c, int))

    async def _history_loop(self) -> None:
        while True:
            try:
                n = await self.poll_history_once()
                log.info("basis: funding history of %d pairs", n)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("basis funding history failed")
            await asyncio.sleep(_HISTORY_REFRESH_S)

    async def _funding_loop(self) -> None:
        while True:
            try:
                await self.poll_funding_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("basis funding poll failed")
            await asyncio.sleep(max(30.0, float(self._settings.basis_funding_poll_s)))

    def snapshot(self, history_limit: int = 300) -> dict[str, Any]:
        pairs = self.scanner.pairs()
        return {
            "enabled": True,
            "venues": sorted(self.legs),
            "assets": self.assets,
            "pairs": pairs,
            "stats": self.scanner.stats(),
            "trades": self.scanner.trades(200),
            "history": {f"{p['venue']}:{p['asset']}": self.scanner.history(p["venue"], p["asset"], history_limit)
                        for p in pairs},
            "settings": {k: getattr(self._settings, k) for k in (
                "basis_mode", "basis_notional_quote", "basis_fut_upper_pct", "basis_spot_lower_pct",
                "basis_ref_window_s", "basis_exit_basis_pct", "basis_min_convergence_pct", "basis_max_hold_s", "basis_latency_ms",
                "basis_max_skew_ms", "basis_max_age_ms", "basis_max_slippage_pct", "carry_enabled",
                "carry_notional_quote", "carry_horizon_h", "carry_min_edge_pct", "carry_exit_rate",
                "carry_confirm_polls", "carry_max_hold_h", "carry_max_positions", "carry_leverage",
                "carry_maint_margin", "carry_liq_guard_pct", "carry_history_n", "carry_min_positive_share",
                "carry_exit_horizon_h")},
        }


def _symbol(adapter: Any, asset: str) -> str | None:
    fn = getattr(adapter, "symbol_for", None)
    try:
        return fn(asset) if callable(fn) else None
    except Exception:  # noqa: BLE001
        return None


def _contract_size(adapter: Any, symbol: str) -> float:
    fn = getattr(adapter, "contract_size", None)
    try:
        size = float(fn(symbol)) if callable(fn) else 1.0
    except Exception:  # noqa: BLE001
        return 1.0
    return size if size > 0 else 1.0


__all__ = ["BasisRunner", "VENUES", "leg_configs", "leg_name"]
