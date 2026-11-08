"""Live data for the basis scanner: per venue a SPOT and a PERP ``CcxtAdapter`` (public, no keys) on a
PRIVATE event bus - the latency bot's pipeline, poller and diagnostics never see these books.

Perp books count contracts: their sizes are scaled to base units (``contract_size``) before the scanner
sees them.  Funding rates are polled every ``basis_funding_poll_s``; the simulator ticks 4 times a second.
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
# venue -> (spot ccxt id, perp ccxt id, spot taker, perp taker, quote).  Both legs use the SAME quote (no FX
# inside the basis): Kraken Futures perps settle in USD, so Kraken spot is BTC/USD, not BTC/USDT.
# Fees are replaced by the venue's own (adapter.fees) when known.
VENUES: dict[str, tuple[str, str, float, float, str]] = {
    "okx": ("okx", "okx", 0.001, 0.0005, "USDT"),
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
                 leg_factory: LegFactory | None = None) -> None:
        self._settings = settings
        self._clock = clock
        self.bus = EventBus()  # private: these books never reach the latency pipeline
        self.scanner = BasisScanner(settings, clock, db=db)
        self._factory = leg_factory or self._default_factory
        self.legs: dict[str, dict[str, Any]] = {}  # venue -> {"spot": adapter, "perp": adapter}
        self._route: dict[str, tuple[str, str]] = {}  # adapter name -> (venue, leg)
        self._tasks: list[asyncio.Task] = []
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
        # each leg resolves exactly the venue's quote, so both legs share it
        legs_settings = dataclasses.replace(self._settings, signal_quote_assets=[quote], quote_assets=[quote])
        return CcxtAdapter(cfg, self.bus, self._clock, legs_settings, None)

    @property
    def assets(self) -> list[str]:
        return list(dict.fromkeys(str(a).strip().upper() for a in self._settings.basis_assets if str(a).strip()))

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        assets = self.assets
        await asyncio.gather(*(self._start_venue(v, legs, assets) for v, legs in self.legs.items()),
                             return_exceptions=True)
        self._tasks = [asyncio.create_task(self._tick_loop(), name="basis-tick"),
                       asyncio.create_task(self._funding_loop(), name="basis-funding")]

    async def _start_venue(self, venue: str, legs: dict[str, Any], assets: list[str]) -> None:
        for leg, adapter in legs.items():
            try:
                await adapter.start(assets, ws_assets=len(assets))
            except Exception as exc:  # noqa: BLE001
                log.warning("basis: %s did not start: %s: %s", adapter.name, type(exc).__name__, str(exc)[:160])
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
        log.info("basis: %s ready (%s)", venue, ", ".join(p["asset"] for p in self.scanner.pairs() if p["venue"] == venue))

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
        n = 0
        for venue, legs in self.legs.items():
            perp = legs["perp"]
            fetch = getattr(perp, "fetch_funding_rate", None)
            if fetch is None:
                continue
            for p in self.scanner.pairs():
                if p["venue"] != venue or not p.get("quote_ok"):
                    continue
                try:
                    r = await fetch(p["perp_symbol"])
                except Exception as exc:  # noqa: BLE001
                    log.debug("basis: funding %s %s: %s", venue, p["perp_symbol"], exc)
                    continue
                self.scanner.set_funding(venue, p["asset"], r.get("rate"), r.get("interval_h"), r.get("next_ts"))
                n += 1
        return n

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
                "basis_max_skew_ms", "basis_max_age_ms", "basis_max_slippage_pct")},
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
