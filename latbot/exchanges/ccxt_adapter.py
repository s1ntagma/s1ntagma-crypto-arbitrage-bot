"""Real exchange adapter on top of ccxt.pro.

Two ccxt instances per exchange:

* ``public``  - no credentials; WebSocket order books / trades / tickers and
  REST snapshots.
* ``private`` - created only when ``creds.present`` and the exchange is an
  EXECUTION venue; used for orders, balances and fee lookups.  A demo
  configured adapter (``cfg.demo``) enables Bybit Demo Trading on the private
  instance and refuses to send an order through a non-demo host.

An execution-only adapter (the LIVE adapter, or the Bybit demo paper venue)
is started with ``ws_assets=0`` and shares the market adapter's books and
health through ``market_source`` (set by ``registry.build_execution_adapters``):
one WebSocket stream set per exchange, one health object, one IP budget.  Its
own ``public`` instance is kept for REST snapshots (revalidation fallback).

Tests inject a fake ccxt instance through ``exchange_factory`` so nothing here
needs the network.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import re
import zlib
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Iterable

import ccxt
import ccxt.pro as ccxtpro

from .. import events
from ..clock import Clock
from ..config import ExchangeConfig, ExchangeCredentials, Settings
from ..events import EventBus
from ..links import split_symbol
from ..models import (
    Balance,
    BookLevel,
    ExchangeHealth,
    ExchangeRole,
    Fees,
    Fill,
    Order,
    OrderBook,
    OrderRequest,
    OrderStatus,
    OrderType,
    Side,
    Ticker,
    TimeInForce,
    Timestamps,
    Trade,
)
from .base import ExchangeAdapter, ExchangeError, InsufficientBalance, OrderRejected, RateLimited

log = logging.getLogger(__name__)

# WebSocket order-book depths each venue accepts (None = do not pass a limit).
# A wrong value is rejected by the exchange ("limit can be one of: [...]"),
# so we snap the configured book_depth to the nearest allowed value.
_WS_BOOK_LIMITS: dict[str, list[int | None]] = {
    "bybit": [1, 50, 200, 1000],
    "kraken": [10, 25, 100, 500, 1000],
    "gate": [5, 10, 20, 50, 100],
    "gateio": [5, 10, 20, 50, 100],
    "binance": [5, 10, 20],
    "cryptocom": [10, 50],
    "kucoin": [5, 50],
    "bitget": [1, 5, 15],
    "okx": [None],
    "myokx": [None],
    "coinbase": [None],
    "bitstamp": [None],
    "bitvavo": [None],
    "htx": [None],
    "mexc": [5, 10, 20],
    "revolutx": [None],
}
_LIMIT_LIST_RE = re.compile(r"\[\s*([0-9]+(?:\s*,\s*[0-9]+)*)\s*\]")


def snap_depth(requested: int | None, allowed: list[int | None]) -> int | None:
    """Smallest allowed depth >= requested, else the largest allowed; None if the venue takes no limit."""
    if not allowed or allowed == [None]:
        return None
    nums = sorted(x for x in allowed if isinstance(x, int))
    if not nums:
        return None
    if not requested:
        return nums[0]
    for n in nums:
        if n >= requested:
            return n
    return nums[-1]


def allowed_depths_from_error(message: str) -> list[int] | None:
    """Parse "limit can be one of: [1,50,200,1000]" style messages."""
    m = _LIMIT_LIST_RE.search(message or "")
    if not m:
        return None
    try:
        return [int(x) for x in m.group(1).replace(" ", "").split(",")]
    except ValueError:
        return None

ExchangeFactory = Callable[[str, dict[str, Any]], Any]

_DEMO_MARKERS = ("api-demo", "demo", "testnet", "sandbox", "test")
_TRADES_MAX_FAILURES = 3
_CROSSED_REST_S = 5.0  # a crossed WS book is replaced by at most one REST snapshot per symbol this often
_CROSSED_RESUB_S = 30.0  # ... and the WS book is re-subscribed (fresh snapshot) at most this often
_SWEEP_SETTLE_S = 0.25  # a print outside the top of book is a stale level only if the next book, this much later, still shows it

# Rate limits: floors per response kind, escalation on repeated hits (see _rate_limited)
_RL_FLOOR_429_S = 1.0  # RateLimitExceeded (HTTP 429 / Binance -1003)
_RL_FLOOR_DDOS_S = 5.0  # DDoSProtection (Kraken "EAPI:Rate limit exceeded", abuse responses)
_RL_FLOOR_418_S = 120.0  # HTTP 418 = Binance IP ban: 2 min minimum, escalating to days
_RL_ESCALATION_WINDOW_S = 60.0  # hits closer than this count as "still knocking"
_RL_PENALTY_MAX_S = 300.0  # cap of the escalated (heuristic) penalty; a Retry-After header may say more
_RL_HEADER_MAX_S = 3 * 86400.0  # sanity cap on Retry-After (Binance bans top out at 3 days)


def _default_factory(name: str, opts: dict[str, Any]) -> Any:
    """ccxt.pro class when the venue streams; otherwise the REST-only
    ``ccxt.async_support`` class (e.g. Revolut X), covered by ticker polling
    and REST order books instead of WebSocket streams."""
    cls = getattr(ccxtpro, name, None)
    if cls is None:
        import ccxt.async_support as ccxt_async

        cls = getattr(ccxt_async, name, None)
    if cls is None:
        raise ExchangeError(f"unknown ccxt exchange '{name}'")
    return cls(opts)


def is_rest_only(instance: Any) -> bool:
    """True when the ccxt instance cannot stream (no watch_order_book)."""
    return not callable(getattr(instance, "watch_order_book", None))


def _num(v: Any) -> float | None:
    """float(v) for ints/floats/numeric strings, else None (no bools)."""
    if v is None or isinstance(v, bool):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _rest_base(inst: Any) -> str:
    """Best-effort REST base URL of a ccxt instance ('' if unknown)."""
    urls = getattr(inst, "urls", None) or {}
    api = urls.get("api") if isinstance(urls, dict) else None
    if isinstance(api, str):
        return api
    if isinstance(api, dict):
        for key in ("private", "public"):
            v = api.get(key)
            if isinstance(v, str):
                return v
        for v in api.values():
            if isinstance(v, str):
                return v
    return ""


def _http_status(inst: Any) -> int | None:
    """HTTP status of the latest REST response on ``inst`` (recorded by the
    ``on_rest_response`` hook, see ``_install_status_hook``), else None."""
    code = getattr(inst, "last_http_status_code", None) if inst is not None else None
    try:
        return int(code) if code is not None else None
    except (TypeError, ValueError):
        return None


def _retry_after_s(inst: Any, now_wall: float, ms: bool = False) -> float | None:
    """Seconds from the ``Retry-After`` header of the latest response on
    ``inst`` (delta-seconds or an HTTP date), capped; None when absent.
    ``ms``: the venue sends milliseconds instead of HTTP seconds (Revolut X)."""
    headers = getattr(inst, "last_response_headers", None) if inst is not None else None
    if not isinstance(headers, dict):
        return None
    raw = next((v for k, v in headers.items() if str(k).lower() == "retry-after"), None)
    if raw is None:
        return None
    text = str(raw).strip().split(",", 1)[0].strip()
    value = _num(text)
    if value is not None and ms:
        value = value / 1000.0
    if value is None:
        try:
            value = parsedate_to_datetime(text).timestamp() - now_wall
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
    return min(_RL_HEADER_MAX_S, max(0.0, float(value)))


def _map_status(status: Any, filled: float, amount: float) -> OrderStatus:
    s = str(status or "").lower()
    if s == "open":
        return OrderStatus.PARTIALLY_FILLED if filled > 0 else OrderStatus.ACKNOWLEDGED
    if s == "closed":
        return OrderStatus.FILLED
    if s in ("canceled", "cancelled"):
        return OrderStatus.CANCELLED
    if s == "rejected":
        return OrderStatus.REJECTED
    if s == "expired":
        return OrderStatus.EXPIRED
    if amount > 0 and filled >= amount - 1e-12:
        return OrderStatus.FILLED
    if filled > 0:
        return OrderStatus.PARTIALLY_FILLED
    return OrderStatus.ACKNOWLEDGED


class CcxtAdapter(ExchangeAdapter):
    """ExchangeAdapter backed by ccxt.pro (see module docstring)."""

    def __init__(
        self,
        cfg: ExchangeConfig,
        bus: EventBus,
        clock: Clock,
        settings: Settings,
        creds: ExchangeCredentials | None = None,
        *,
        exchange_factory: ExchangeFactory | None = None,
        backoff_min_s: float = 1.0,
        backoff_max_s: float = 30.0,
        health_interval_s: float = 2.0,
    ) -> None:
        super().__init__(cfg, bus, clock)
        self.settings = settings
        self._factory: ExchangeFactory = exchange_factory or _default_factory
        self._backoff_min = backoff_min_s
        self._backoff_max = backoff_max_s
        self._ws_depth: int | None = snap_depth(cfg.book_depth, _WS_BOOK_LIMITS.get(cfg.ccxt_name, [cfg.book_depth]))
        self._err_log_mono: dict[str, float] = {}
        self._err_counts: dict[str, int] = {}
        self.crossed_books = 0  # WS books dropped as crossed (bid >= ask): shown in DIAG
        self._crossed_rest_mono: dict[str, float] = {}
        self._crossed_resub_mono: dict[str, float] = {}
        self._crossed_log_mono: dict[str, float] = {}
        self._suspect: dict[str, tuple[float, float]] = {}  # asset -> (print price outside the cached top of book, recv_mono)
        self._health_interval = health_interval_s
        opts = self._build_opts()
        self.public: Any = self._factory(cfg.ccxt_name, opts)
        self.private: Any = None
        if creds is not None and creds.present and cfg.role == ExchangeRole.EXECUTION:
            popts = dict(opts)
            popts["apiKey"] = creds.api_key
            popts["secret"] = creds.api_secret
            popts["privateKey"] = creds.api_secret  # venues signing with a private key (Revolut X)
            if creds.passphrase:
                popts["password"] = creds.passphrase
            self.private = self._factory(cfg.ccxt_name, popts)
            if cfg.demo:
                self._enable_demo(self.private)
        for inst in (self.public, self.private):
            self._install_status_hook(inst)
        self._rest_only = cfg.stream == "rest" or is_rest_only(self.public)
        self._rl_hits = 0  # consecutive rate-limit hits (public + private: one IP budget)
        self._rl_last_mono: float | None = None
        self._symbols: dict[str, str] = {}  # asset -> symbol
        self._assets: dict[str, str] = {}  # symbol -> asset
        self._books: dict[str, OrderBook] = {}  # by asset
        self._streamed: set[str] = set()  # symbols with a WS task
        self._tasks: list[asyncio.Task[None]] = []
        self._skipped: set[str] = set()
        self._orders: dict[str, Order] = {}  # by exchange id
        self._orders_by_client: dict[str, Order] = {}
        self._fee_fetched: set[str] = set()
        self._markets_loaded = False
        self._universe_resolved = False  # resolve_assets(None) ran on the current markets
        self._tickers_all_only = False  # venue refuses fetchTickers(symbols) with several symbols (Crypto.com)
        self._market_source: ExchangeAdapter | None = None  # see the ``market_source`` property

    # ---- construction helpers ---------------------------------------------------
    def _build_opts(self) -> dict[str, Any]:
        opts: dict[str, Any] = {
            "enableRateLimit": True,
            "options": {"defaultType": self.cfg.market_type},
        }
        if self.cfg.hostname:
            opts["hostname"] = self.cfg.hostname
        if self.cfg.ccxt_name == "htx":
            # default watchTicker channel 'detail' is 24h statistics (no bid/ask);
            # 'ticker' carries bid/ask/last and does not flap
            opts["options"]["watchTicker"] = {"name": "market.{marketId}.ticker"}
        return opts

    @staticmethod
    def _install_status_hook(inst: Any) -> None:
        """Remember the HTTP status of the latest REST response as
        ``inst.last_http_status_code``.  ccxt keeps the response headers but
        not the status, and a Binance IP ban (HTTP 418, body code -1003) is
        raised as the very same ``RateLimitExceeded`` as a plain 429."""
        if inst is None:
            return
        original = getattr(inst, "on_rest_response", None)
        if not callable(original):
            return

        def on_rest_response(code: Any, reason: Any, url: Any, method: Any, response_headers: Any, response_body: Any,
                             request_headers: Any, request_body: Any) -> Any:
            try:
                inst.last_http_status_code = int(code)
            except (TypeError, ValueError):
                inst.last_http_status_code = None
            return original(code, reason, url, method, response_headers, response_body, request_headers, request_body)

        try:
            inst.last_http_status_code = None
            inst.on_rest_response = on_rest_response
        except Exception as exc:  # noqa: BLE001
            log.debug("status hook not installed: %s", type(exc).__name__)

    def _enable_demo(self, inst: Any) -> None:
        name = self.cfg.ccxt_name
        if name == "bybit" or hasattr(inst, "enable_demo_trading"):
            inst.enable_demo_trading(True)
        elif hasattr(inst, "set_sandbox_mode"):
            inst.set_sandbox_mode(True)
        else:
            raise ExchangeError(f"{self.name}: demo mode requested but '{name}' has no demo/sandbox endpoint")
        self._assert_demo_host()
        log.info("%s: demo trading enabled (%s)", self.name, _rest_base(inst))

    def _assert_demo_host(self) -> None:
        """Raise unless the private REST base is a demo host (demo adapters only)."""
        if not self.cfg.demo or self.private is None:
            return
        base = _rest_base(self.private).lower()
        if self.cfg.ccxt_name == "bybit":
            ok = "api-demo" in base
        else:
            ok = any(m in base for m in _DEMO_MARKERS)
        if not ok:
            raise ExchangeError(f"{self.name}: demo mode configured but REST host is not a demo endpoint ({base or 'unknown'})")

    # ---- market source (execution-only adapters) -----------------------------------
    @property
    def market_source(self) -> ExchangeAdapter | None:
        """The market-data adapter of the same exchange whose cached books and
        health this (execution-only) adapter shares; None when it streams itself."""
        return self._market_source

    @market_source.setter
    def market_source(self, adapter: ExchangeAdapter | None) -> None:
        self._market_source = adapter
        health = getattr(adapter, "health", None) if adapter is not None else None
        if isinstance(health, ExchangeHealth):
            # One health per exchange: ws liveness / staleness come from the market
            # stream, order and REST latency EWMAs from this adapter's calls.
            self.health = health

    # ---- identity ------------------------------------------------------------------
    def symbol_for(self, asset: str) -> str | None:
        return self._symbols.get(asset.upper())

    def asset_for(self, symbol: str) -> str | None:
        return self._assets.get(symbol)

    def market(self, asset: str) -> dict[str, Any] | None:
        """Raw ccxt market dict for a resolved asset (None if unknown)."""
        sym = self._symbols.get(asset.upper())
        markets = getattr(self.public, "markets", None) or {}
        return markets.get(sym) if sym else None

    @staticmethod
    def _asset_key(item: str) -> str:
        """'btc' / 'BTC/USDT' / 'BTC/USDT:USDT' -> 'BTC'."""
        return item.split("/", 1)[0].split(":", 1)[0].strip().upper()

    # ---- markets -------------------------------------------------------------------
    async def _load_markets(self) -> None:
        if self._markets_loaded and getattr(self.public, "markets", None):
            return
        await self.public.load_markets()
        self._markets_loaded = True
        self._universe_resolved = False
        if self.private is not None and hasattr(self.private, "set_markets"):
            try:
                self.private.set_markets(self.public.markets, getattr(self.public, "currencies", None))
            except Exception as exc:  # noqa: BLE001
                log.debug("%s: set_markets on private instance failed: %s", self.name, type(exc).__name__)

    def _market_ok(self, m: dict[str, Any]) -> bool:
        if m.get("active") is False:
            return False
        if self.cfg.market_type == "swap":
            return bool(m.get("swap")) and bool(m.get("linear"))
        return bool(m.get("spot"))

    def resolve_assets(self, assets: Iterable[str] | None = None) -> dict[str, str]:
        """Resolve assets -> market symbols (``None`` = the whole universe).

        Rule: ``market['base'] == asset``, active, matching ``cfg.market_type``
        (spot; or swap + linear), quote in ``settings.quote_assets`` - the best
        preference index wins.  Unresolvable assets are logged once.
        """
        markets: dict[str, dict[str, Any]] = getattr(self.public, "markets", None) or {}
        quotes = [q.upper() for q in self.settings.quote_assets]
        by_base: dict[str, list[dict[str, Any]]] = {}
        for m in markets.values():
            if not isinstance(m, dict) or not m.get("symbol"):
                continue
            if not self._market_ok(m):
                continue
            base = str(m.get("base") or "").upper()
            if base:
                by_base.setdefault(base, []).append(m)
        targets = [self._asset_key(a) for a in assets] if assets is not None else sorted(by_base)
        out: dict[str, str] = {}
        for asset in targets:
            if not asset:
                continue
            best: dict[str, Any] | None = None
            best_rank = len(quotes)
            for m in by_base.get(asset, []):
                q = str(m.get("quote") or "").upper()
                if q not in quotes:
                    continue
                rank = quotes.index(q)
                if rank < best_rank:
                    best, best_rank = m, rank
            if best is None:
                if asset not in self._skipped:
                    self._skipped.add(asset)
                    log.info("%s: no %s market for %s in quotes %s; skipped", self.name, self.cfg.market_type, asset, quotes)
                continue
            sym = str(best["symbol"])
            self._symbols[asset] = sym
            self._assets[sym] = asset
            out[asset] = sym
        if assets is None:
            self._universe_resolved = True
        return out

    # ---- lifecycle --------------------------------------------------------------------
    async def start(self, assets: Iterable[str], ws_assets: int | None = None) -> None:  # noqa: D401
        """Load markets, resolve ``assets`` and start one WS task per streamed symbol.

        ``ws_assets=0`` (execution-only adapter): markets are loaded, symbols
        resolved and the demo host asserted, but no stream and no health loop
        are started - the market adapter of the same exchange (``market_source``)
        owns the data and the health object."""
        wanted = [self._asset_key(a) for a in assets]
        wanted = list(dict.fromkeys(a for a in wanted if a))
        self._assert_demo_host()
        await self._load_markets()
        resolved = self.resolve_assets(wanted)
        limit = self.settings.ws_assets if ws_assets is None else int(ws_assets)
        if self.cfg.max_ws_subscriptions and self.cfg.max_ws_subscriptions > 0:
            limit = min(limit, self.cfg.max_ws_subscriptions)
        streamed = [a for a in wanted if a in resolved][: max(0, limit)]
        if limit <= 0:
            log.info("%s: execution-only (markets resolved for %d/%d assets, no streams)", self.name, len(resolved), len(wanted))
            return
        if self._rest_only:
            log.info(
                "%s: REST-only venue (no WebSocket): %d/%d assets resolved; prices via ticker polling, order book via REST",
                self.name, len(resolved), len(wanted),
            )
            return
        has = getattr(self.public, "has", None) or {}
        for asset in streamed:
            symbol = resolved[asset]
            if symbol in self._streamed:
                continue
            self._streamed.add(symbol)
            if self.cfg.stream == "bbo":
                self._tasks.append(asyncio.create_task(self._bbo_loop(asset, symbol), name=f"{self.name}:bbo:{symbol}"))
            else:
                self._tasks.append(asyncio.create_task(self._book_loop(asset, symbol), name=f"{self.name}:book:{symbol}"))
                if has.get("watchTrades"):
                    self._tasks.append(asyncio.create_task(self._trades_loop(asset, symbol), name=f"{self.name}:trades:{symbol}"))
        if self._market_source is None and not any(t.get_name() == f"{self.name}:health" for t in self._tasks):
            self._tasks.append(asyncio.create_task(self._health_loop(), name=f"{self.name}:health"))
        log.info("%s: streaming %d/%d assets (%s)", self.name, len(streamed), len(wanted), self.cfg.stream)

    async def stop(self) -> None:
        tasks, self._tasks = self._tasks, []
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._market_source is None:  # a shared health object is the market adapter's to flip
            self.health.ws_connected = False
        for inst in (self.public, self.private):
            if inst is None:
                continue
            close = getattr(inst, "close", None)
            if close is None:
                continue
            try:
                r = close()
                if asyncio.iscoroutine(r):
                    await r
            except Exception as exc:  # noqa: BLE001
                log.debug("%s: close failed: %s", self.name, type(exc).__name__)

    def supported_symbols(self) -> set[str]:
        return set(self._streamed)

    def streamed_assets(self) -> list[str]:
        """Assets with a live WebSocket stream."""
        return sorted({self._assets.get(sym, sym) for sym in self._streamed})

    def ws_budget(self) -> int:
        """How many assets may stream (``ws_assets`` capped by ``max_ws_subscriptions``; 0 = REST-only)."""
        if self._rest_only:
            return 0
        limit = int(self.settings.ws_assets)
        if self.cfg.max_ws_subscriptions and self.cfg.max_ws_subscriptions > 0:
            limit = min(limit, self.cfg.max_ws_subscriptions)
        return max(0, limit)

    async def add_streams(self, assets: Iterable[str]) -> int:
        """Stream assets that entered the pool after start (Phase 3) within ``ws_budget``;
        streams that run are kept.  Only for an adapter that already streams (started OK).
        Returns the number of new streams."""
        have = self.streamed_assets()
        if not have:
            return 0
        new = [a for a in dict.fromkeys(self._asset_key(x) for x in assets) if a and a not in have]
        if not new or self.ws_budget() <= len(have):
            return 0
        before = len(self._streamed)
        await self.start([*have, *new])  # running streams first: they keep their slots
        return len(self._streamed) - before

    # ---- streaming loops -------------------------------------------------------------
    def _timestamps(self, exchange_ms: Any) -> Timestamps:
        ts = _num(exchange_ms)
        return Timestamps(
            recv_ts=self.clock.wall(),
            recv_mono=self.clock.mono(),
            exchange_ts=ts / 1000.0 if ts else None,
        )

    def _normalise_book(self, asset: str, symbol: str, ob: dict[str, Any]) -> OrderBook:
        depth = self.cfg.book_depth or None
        bids = [BookLevel(float(p), float(s)) for p, s, *_ in (ob.get("bids") or []) if _num(s) and float(s) > 0]
        asks = [BookLevel(float(p), float(s)) for p, s, *_ in (ob.get("asks") or []) if _num(s) and float(s) > 0]
        bids.sort(key=lambda lv: lv.price, reverse=True)
        asks.sort(key=lambda lv: lv.price)
        if depth:
            bids, asks = bids[:depth], asks[:depth]
        nonce = ob.get("nonce")
        return OrderBook(
            exchange=self.name,
            symbol=symbol,
            bids=bids,
            asks=asks,
            ts=self._timestamps(ob.get("timestamp")),
            asset=asset,
            seq=int(nonce) if isinstance(nonce, (int, float)) else None,
        )

    def _on_stream_error(self, what: str, symbol: str, exc: BaseException, backoff: float) -> None:
        """Log one WARNING per (stream kind) per 60 s with a repeat counter; the rest at DEBUG."""
        key = what
        now = self.clock.mono()
        self._err_counts[key] = self._err_counts.get(key, 0) + 1
        last = self._err_log_mono.get(key)
        if last is None or now - last >= 60.0:
            log.warning(
                "%s: %s stream failed (%s: %s) [%d failures, last %s]; retry in %.0f s",
                self.name, what, type(exc).__name__, str(exc)[:100], self._err_counts[key], symbol, backoff,
            )
            self._err_log_mono[key] = now
        else:
            log.debug("%s: %s stream %s failed (%s: %s)", self.name, what, symbol, type(exc).__name__, str(exc)[:100])
        self._mark_error(type(exc).__name__)
        self.health.ws_connected = False

    def _drop_cached_book(self, symbol: str) -> None:
        """ccxt.pro keeps its order book across reconnects and applies new deltas on top of stale levels:
        after a connection-level error (the client is gone, the re-watch subscribes again) the cache is
        dropped so the next watch rebuilds it from a fresh snapshot.  Never while a subscription is alive:
        the venue's handler then raises on the missing book and the receive loop dies with it."""
        cache = getattr(self.public, "orderbooks", None)
        if isinstance(cache, dict):
            cache.pop(symbol, None)

    def _can_resubscribe(self) -> bool:
        has = getattr(self.public, "has", None) or {}
        return getattr(self.public, "un_watch_order_book", None) is not None and bool(has.get("unWatchOrderBook"))

    async def _resubscribe(self, symbol: str, why: str) -> bool:
        """Fresh snapshot for ``symbol`` where the venue supports un-watching (the library drops its
        book with the subscription and the next watch loads a new snapshot).  Returns False where it
        cannot: the broken flag and the throttled REST snapshot keep the price honest until a sane book."""
        if not self._can_resubscribe():
            return False
        try:
            await self.public.un_watch_order_book(symbol)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.debug("%s: un_watch_order_book %s (%s) failed: %s", self.name, symbol, why, exc)
            return False
        return True

    def _next_resub_mono(self, symbol: str, *, first: bool) -> float:
        """When the periodic re-subscribe of ``symbol`` is due (inf = never: off or unsupported).
        The first one is staggered per symbol so all streams do not reconnect in the same second."""
        resub = float(getattr(self.settings, "ws_book_resubscribe_s", 0.0) or 0.0)
        if resub <= 0 or not self._can_resubscribe():
            return float("inf")
        spread = (zlib.crc32(symbol.encode()) % 1000) / 1000.0 * resub if first else 0.0
        return self.clock.mono() + resub + spread

    async def _book_loop(self, asset: str, symbol: str) -> None:
        backoff = self._backoff_min
        resub_due = self._next_resub_mono(symbol, first=True)
        while True:
            try:
                if self.clock.mono() >= resub_due:
                    await self._resubscribe(symbol, "periodic")  # a silent reconnect cannot keep stale levels
                    resub_due = self._next_resub_mono(symbol, first=False)
                if self._ws_depth is None:
                    ob = await self.public.watch_order_book(symbol)
                else:
                    ob = await self.public.watch_order_book(symbol, self._ws_depth)
                book = self._normalise_book(asset, symbol, ob)
                if book.is_crossed:
                    backoff = self._backoff_min
                    await self._on_broken(asset, symbol, book, "crossed")
                    continue
                if await self._print_contradicts(asset, symbol, book):
                    backoff = self._backoff_min
                    continue
                self._books[asset] = book
                self._mark_msg(book.ts.exchange_ts, book.ts.recv_ts, book.ts.recv_mono)
                backoff = self._backoff_min
                await self.bus.publish(events.BOOK, book)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                allowed = allowed_depths_from_error(str(exc)) if isinstance(exc, ccxt.BadRequest) else None
                if allowed:
                    new_depth = snap_depth(self.cfg.book_depth, allowed)
                    if new_depth != self._ws_depth:
                        log.info("%s: order book depth %s rejected, using %s", self.name, self._ws_depth, new_depth)
                        self._ws_depth = new_depth
                        continue
                self._on_stream_error("book", symbol, exc, backoff)
                if isinstance(exc, ccxt.NetworkError):
                    self._drop_cached_book(symbol)  # the client is gone: never re-watch on top of the old levels
                self._suspect.pop(asset, None)
                resub_due = self._next_resub_mono(symbol, first=False)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, self._backoff_max)

    async def _on_broken(self, asset: str, symbol: str, book: OrderBook, why: str) -> None:
        """A broken WS book - crossed (bid >= ask) or contradicted by a print outside its top of book -
        is a stale cache, typically levels left behind after a reconnect (hard rule 1).  It is published
        flagged ``broken`` so MarketState invalidates the stream at once; the venue has no price for
        ``asset`` until a sane book arrives: one REST snapshot is tried (throttled) and the WS book is
        re-subscribed so the library loads a fresh snapshot."""
        self.crossed_books += 1
        self._books.pop(asset, None)  # the last book is not current any more
        self._suspect.pop(asset, None)
        book = dataclasses.replace(book, broken=why)  # never mutate an object that was already published
        await self.bus.publish(events.BOOK, book)
        now = self.clock.mono()
        last_log = self._crossed_log_mono.get(symbol)
        if last_log is None or now - last_log >= 60.0:
            log.warning("%s: broken order book for %s dropped (%s; bid %.10g ask %.10g; %d so far)",
                        self.name, symbol, why, book.best_bid or 0.0, book.best_ask or 0.0, self.crossed_books)
            self._crossed_log_mono[symbol] = now
        last_resub = self._crossed_resub_mono.get(symbol)
        if last_resub is None or now - last_resub >= _CROSSED_RESUB_S:
            self._crossed_resub_mono[symbol] = now
            await self._resubscribe(symbol, why)
        last = self._crossed_rest_mono.get(symbol)
        if last is not None and now - last < _CROSSED_REST_S:
            return
        self._crossed_rest_mono[symbol] = now
        try:
            rest = await self.fetch_order_book(symbol)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.debug("%s: REST book for %s after a crossed WS book failed: %s", self.name, symbol, exc)
            return
        if rest.is_crossed or not rest.bids or not rest.asks:
            self.crossed_books += 1 if rest.is_crossed else 0
            return
        self._books[asset] = rest
        self._mark_msg(rest.ts.exchange_ts, rest.ts.recv_ts, rest.ts.recv_mono)
        await self.bus.publish(events.BOOK, rest)

    def _ticker_from(self, asset: str, symbol: str, t: dict[str, Any]) -> Ticker:
        return Ticker(
            exchange=self.name,
            symbol=symbol,
            bid=_num(t.get("bid")),
            ask=_num(t.get("ask")),
            last=_num(t.get("last")),
            ts=self._timestamps(t.get("timestamp")),
            asset=asset,
            base_volume=_num(t.get("baseVolume")),
            quote_volume=_num(t.get("quoteVolume")),
        )

    async def _bbo_loop(self, asset: str, symbol: str) -> None:
        backoff = self._backoff_min
        has = getattr(self.public, "has", None) or {}
        use_bids_asks = bool(has.get("watchBidsAsks"))
        failures = 0
        while True:
            try:
                if use_bids_asks:
                    res = await self.public.watch_bids_asks([symbol])
                    raw = res.get(symbol) if isinstance(res, dict) else None
                    if raw is None and isinstance(res, dict) and res:
                        raw = next(iter(res.values()))
                else:
                    raw = await self.public.watch_ticker(symbol)
                if not raw:
                    continue
                ticker = self._ticker_from(asset, symbol, raw)
                self._mark_msg(ticker.ts.exchange_ts, ticker.ts.recv_ts, ticker.ts.recv_mono)
                backoff = self._backoff_min
                failures = 0
                await self.bus.publish(events.TICKER, ticker)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                failures += 1
                if use_bids_asks and failures >= 3:
                    use_bids_asks = False  # watchBidsAsks unreliable on this venue: plain ticker stream
                    log.info("%s: bbo stream %s falling back to watch_ticker", self.name, symbol)
                if failures >= 8 and self.cfg.poll_tickers:
                    log.info("%s: bbo stream %s gave up after %d failures; REST ticker polling covers it",
                             self.name, symbol, failures)
                    return
                self._on_stream_error("bbo", symbol, exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, self._backoff_max)

    def _note_print(self, asset: str, trade: Trade) -> None:
        """A print outside the cached top of book is a *suspicion* of a stale level - the trade channel
        normally runs ahead of the aggregated book channel - resolved by ``_print_contradicts`` on the
        next book.  Needs the venue's own timestamps: a print not newer than the book proves nothing."""
        cached = self._books.get(asset)
        if cached is None or trade.ts.exchange_ts is None or asset in self._suspect:
            return
        book_ts = cached.ts.exchange_ts if cached.ts.exchange_ts is not None else cached.ts.recv_ts - 0.5
        if trade.ts.exchange_ts <= book_ts:
            return
        price = trade.price
        if (cached.best_ask is not None and price > cached.best_ask) or (
                cached.best_bid is not None and price < cached.best_bid):
            self._suspect[asset] = (price, trade.ts.recv_mono)

    async def _print_contradicts(self, asset: str, symbol: str, book: OrderBook) -> bool:
        """Resolve a print suspicion with ``book``: a book received ``_SWEEP_SETTLE_S`` after the print
        that still shows the level the taker would have swept is a stale cache -> broken; a consistent
        book clears the suspicion; an earlier book waits for the next one."""
        suspect = self._suspect.get(asset)
        if suspect is None:
            return False
        price, since = suspect
        outside = (book.best_ask is not None and price > book.best_ask) or (
            book.best_bid is not None and price < book.best_bid)
        if not outside:
            self._suspect.pop(asset, None)
            return False
        if book.ts.recv_mono - since < _SWEEP_SETTLE_S:
            return False
        await self._on_broken(asset, symbol, book, f"print {price:.10g} outside the top of book persisted")
        return True

    async def _trades_loop(self, asset: str, symbol: str) -> None:
        failures = 0
        backoff = self._backoff_min
        first_batch = True  # the first batch after a (re)subscription is history, not a live print
        while True:
            try:
                trades = await self.public.watch_trades(symbol)
                failures = 0
                backoff = self._backoff_min
                for t in trades or []:
                    price = _num(t.get("price"))
                    amount = _num(t.get("amount"))
                    if price is None or amount is None:
                        continue
                    side_raw = str(t.get("side") or "").lower()
                    side = Side.BUY if side_raw == "buy" else Side.SELL if side_raw == "sell" else None
                    trade = Trade(
                        exchange=self.name,
                        symbol=symbol,
                        price=price,
                        amount=amount,
                        side=side,
                        ts=self._timestamps(t.get("timestamp")),
                        asset=asset,
                        trade_id=str(t["id"]) if t.get("id") is not None else None,
                    )
                    if not first_batch:
                        self._note_print(asset, trade)
                    await self.bus.publish(events.TRADE, trade)
                first_batch = False
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                failures += 1
                first_batch = True
                if failures >= _TRADES_MAX_FAILURES:
                    log.warning("%s: trades stream %s disabled after %d failures (%s: %s)",
                                self.name, symbol, failures, type(exc).__name__, str(exc)[:100])
                    return
                log.info("%s: trades stream %s failed (%s: %s); retry in %.0f s",
                         self.name, symbol, type(exc).__name__, str(exc)[:100], backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, self._backoff_max)

    async def _health_loop(self) -> None:
        while True:
            try:
                await self.bus.publish(events.HEALTH, self.health)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("%s: health publish failed", self.name)
            await asyncio.sleep(self._health_interval)

    # ---- market data (REST) ------------------------------------------------------------
    def _symbol_arg(self, symbol_or_asset: str) -> tuple[str, str]:
        """Return (symbol, asset) for a symbol or an asset key."""
        if symbol_or_asset in self._assets:
            return symbol_or_asset, self._assets[symbol_or_asset]
        key = symbol_or_asset.upper()
        if key in self._symbols:
            return self._symbols[key], key
        return symbol_or_asset, self._asset_key(symbol_or_asset)

    def order_book(self, symbol: str) -> OrderBook | None:
        _, asset = self._symbol_arg(symbol)
        book = self._books.get(asset)
        if book is None and self._market_source is not None:
            return self._market_source.order_book(symbol)
        return book

    async def _public_call(self, fn: Callable[..., Any], *args: Any) -> Any:
        t0 = self.clock.mono()
        until = self.health.rate_limited_until_mono
        if until is not None and t0 < until:
            # Banned: do not knock (every request during the ban escalates it).
            # Orders are NOT gated here - an exit must always be attempted.
            raise RateLimited(retry_after_s=until - t0, msg=f"{self.name}: rate limited, {until - t0:.0f} s left")
        try:
            res = await fn(*args)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._mark_error(type(exc).__name__)
            raise self._translate(exc, self.public) from exc
        self._mark_rest_latency((self.clock.mono() - t0) * 1000.0)
        if self._rest_only:
            # no WebSocket exists: a successful REST call is the liveness signal
            self._mark_msg(None, self.clock.wall(), self.clock.mono())
        return res

    async def fetch_order_book(self, symbol: str, limit: int = 20) -> OrderBook:
        symbol, asset = self._symbol_arg(symbol)
        ob = await self._public_call(self.public.fetch_order_book, symbol, limit)
        return self._normalise_book(asset, symbol, ob)

    async def fetch_ticker(self, symbol: str) -> Ticker:
        symbol, asset = self._symbol_arg(symbol)
        raw = await self._public_call(self.public.fetch_ticker, symbol)
        return self._ticker_from(asset, symbol, raw)

    async def fetch_tickers(self, assets: Iterable[str] | None = None) -> dict[str, Ticker]:
        """asset -> Ticker in ONE REST call: for ``assets``, or (``None``) for
        EVERY listed market - whatever ``start()`` / earlier calls resolved.
        ``registry.resolve_universe`` relies on the ``None`` form returning the
        whole universe after the adapter was started with the watchlist."""
        await self._load_markets()
        if assets is not None:
            wanted = self.resolve_assets(assets)
        else:
            if not self._universe_resolved:
                self.resolve_assets(None)
            wanted = dict(self._symbols)
        if not wanted:
            return {}
        symbols = list(wanted.values())
        raw: Any = None
        if assets is not None and len(symbols) <= 50 and not self._tickers_all_only:
            try:
                raw = await self._public_call(self.public.fetch_tickers, symbols)
            except RateLimited:
                raise
            except ExchangeError as exc:
                refused = (ccxt.BadRequest, ccxt.NotSupported, ccxt.ArgumentsRequired)
                if len(symbols) < 2 or not isinstance(exc.__cause__, refused):
                    raise
                self._tickers_all_only = True  # one call for every market from now on, filtered below
                log.info("%s: fetchTickers takes no symbol list here; polling every market instead", self.name)
        if raw is None:
            raw = await self._public_call(self.public.fetch_tickers)
        out: dict[str, Ticker] = {}
        for asset, sym in wanted.items():
            t = (raw or {}).get(sym)
            if t:
                out[asset] = self._ticker_from(asset, sym, t)
        return out

    # ---- account -------------------------------------------------------------------------
    def _require_private(self) -> Any:
        if self.private is None:
            raise ExchangeError(f"{self.name}: no API credentials (role={self.cfg.role.value})")
        return self.private

    async def fetch_balances(self) -> dict[str, Balance]:
        if self.private is None:
            return {}
        raw = await self._private_call(self.private.fetch_balance)
        free = raw.get("free") or {}
        total = raw.get("total") or {}
        out: dict[str, Balance] = {}
        for asset in set(free) | set(total):
            f = _num(free.get(asset)) or 0.0
            t = _num(total.get(asset)) or 0.0
            if f or t:
                out[str(asset)] = Balance(asset=str(asset), free=f, total=t)
        return out

    async def fees(self, symbol: str) -> Fees:
        symbol, _ = self._symbol_arg(symbol)
        cached = self._fees_cache.get(symbol)
        if cached is not None:
            return cached
        maker, taker = self.cfg.maker_fee, self.cfg.taker_fee
        market = (getattr(self.public, "markets", None) or {}).get(symbol) or {}
        m_taker, m_maker = _num(market.get("taker")), _num(market.get("maker"))
        if m_taker is not None:
            taker = m_taker
        if m_maker is not None:
            maker = m_maker
        if self.private is not None and symbol not in self._fee_fetched and hasattr(self.private, "fetch_trading_fee"):
            self._fee_fetched.add(symbol)
            try:
                f = await self.private.fetch_trading_fee(symbol)
                f_taker, f_maker = _num((f or {}).get("taker")), _num((f or {}).get("maker"))
                if f_taker is not None:
                    taker = f_taker
                if f_maker is not None:
                    maker = f_maker
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.debug("%s: fetch_trading_fee(%s) failed: %s", self.name, symbol, type(exc).__name__)
        fees = Fees(maker=maker, taker=taker)
        self._fees_cache[symbol] = fees
        return fees

    # ---- error mapping ----------------------------------------------------------------------
    def _rate_limited(self, exc: BaseException, inst: Any) -> float:
        """Record a rate-limit hit; returns how long to stay away (seconds).

        Floors: 1 s for a plain 429 (``RateLimitExceeded``), 5 s for a DDoS /
        abuse response, 2 min for an HTTP 418 IP ban.  The exchange's
        ``Retry-After`` header wins when it says more.  Hits closer than
        ``_RL_ESCALATION_WINDOW_S`` double the penalty (capped at
        ``_RL_PENALTY_MAX_S``) - the exchanges' own contract: keep knocking
        and the ban grows - and a quiet period resets it.  The public and
        the private instance share the counter (one IP budget), and
        ``health.rate_limited_until_mono`` never moves earlier.  The header
        and status are read off the instance right after it raised; with
        concurrent requests on one instance they may belong to a neighbouring
        response, in which case only the floors / escalation apply.
        """
        now = self.clock.mono()
        status = _http_status(inst)
        header = _retry_after_s(inst, self.clock.wall(), ms=bool(getattr(self.cfg, "retry_after_ms", False)))
        floor = _RL_FLOOR_429_S if isinstance(exc, ccxt.RateLimitExceeded) else _RL_FLOOR_DDOS_S
        if status == 418:
            floor = max(floor, _RL_FLOOR_418_S)
        if self._rl_last_mono is not None and now - self._rl_last_mono <= _RL_ESCALATION_WINDOW_S:
            self._rl_hits += 1
        else:
            self._rl_hits = 1
        self._rl_last_mono = now
        penalty = min(_RL_PENALTY_MAX_S, floor * (2.0 ** (self._rl_hits - 1)))
        retry = max(floor, penalty, header or 0.0)
        until = now + retry
        prev = self.health.rate_limited_until_mono
        if prev is not None and prev > until:
            until = prev
        self.health.rate_limited_until_mono = until
        self.health.reason = f"rate limited (HTTP {status or '?'}, hit {self._rl_hits}): backing off {until - now:.0f} s"
        log.warning(
            "%s: rate limited (%s, HTTP %s, Retry-After %s, hit %d) - no requests for %.0f s",
            self.name, type(exc).__name__, status or "?", f"{header:.0f}" if header is not None else "-", self._rl_hits, until - now,
        )
        return until - now

    def _translate(self, exc: BaseException, inst: Any = None) -> ExchangeError:
        """Map a ccxt exception to the typed errors of ``base.py``.  ``inst``
        is the ccxt instance that raised (for the rate-limit response details)."""
        msg = f"{type(exc).__name__}: {str(exc)[:200]}"
        if isinstance(exc, ExchangeError):
            return exc
        if isinstance(exc, ccxt.InsufficientFunds):
            return InsufficientBalance(msg)
        if isinstance(exc, (ccxt.RateLimitExceeded, ccxt.DDoSProtection)):
            retry = self._rate_limited(exc, inst)
            return RateLimited(retry_after_s=retry, msg=msg)
        if isinstance(exc, ccxt.InvalidOrder):
            return OrderRejected(msg)
        if isinstance(exc, ccxt.ExchangeNotAvailable):  # includes OnMaintenance
            self.health.maintenance = True
            return ExchangeError(msg)
        return ExchangeError(msg)

    async def _private_call(self, fn: Callable[..., Any], *args: Any) -> Any:
        try:
            return await fn(*args)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._mark_error(type(exc).__name__)
            raise self._translate(exc, self.private) from exc

    # ---- orders ---------------------------------------------------------------------------------
    @property
    def _client_key(self) -> str:
        return "orderLinkId" if self.cfg.ccxt_name == "bybit" else "clientOrderId"

    def _order_params(self, req: OrderRequest, reduce_only: bool) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if req.type == OrderType.LIMIT:
            if req.tif == TimeInForce.IOC:
                params["timeInForce"] = "IOC"
            elif req.tif == TimeInForce.FOK:
                params["timeInForce"] = "FOK"
            if req.post_only:
                params["postOnly"] = True
        if req.client_id:
            params[self._client_key] = req.client_id
        if reduce_only and self.cfg.market_type == "swap":
            params["reduceOnly"] = True
        return params

    def _cache(self, order: Order) -> None:
        if order.id:
            self._orders[order.id] = order
        if order.client_id:
            self._orders_by_client[order.client_id] = order

    def _apply_ccxt_order(self, order: Order, raw: dict[str, Any] | None) -> Order:
        """Fill ``order`` from a ccxt order structure (status, fills, fees)."""
        raw = raw or {}
        order.raw = raw
        if raw.get("id") is not None and str(raw["id"]):
            order.id = str(raw["id"])
        if raw.get("clientOrderId") and not order.client_id:
            order.client_id = str(raw["clientOrderId"])
        filled = _num(raw.get("filled")) or 0.0
        amount = _num(raw.get("amount")) or order.amount
        if amount and amount > 0:
            order.amount = amount
        avg = _num(raw.get("average"))
        price = _num(raw.get("price"))
        if price is not None and order.price is None:
            order.price = price
        base, quote = split_symbol(order.symbol)
        fills: list[Fill] = []
        trades = raw.get("trades") or []
        for t in trades:
            if not isinstance(t, dict):
                continue
            tp, ta = _num(t.get("price")), _num(t.get("amount"))
            if tp is None or ta is None:
                continue
            fee = t.get("fee") or {}
            fts = _num(t.get("timestamp"))
            fills.append(
                Fill(
                    order_id=order.id,
                    price=tp,
                    amount=ta,
                    fee_cost=_num(fee.get("cost")) or 0.0,
                    fee_asset=str(fee.get("currency") or quote),
                    ts=fts / 1000.0 if fts else self.clock.wall(),
                    trade_id=str(t["id"]) if t.get("id") is not None else None,
                )
            )
        if not fills and filled > 0:
            fee = raw.get("fee") or {}
            fts = _num(raw.get("lastTradeTimestamp")) or _num(raw.get("timestamp"))
            fill_price = avg if avg is not None else (price if price is not None else (order.price or 0.0))
            fills.append(
                Fill(
                    order_id=order.id,
                    price=fill_price,
                    amount=filled,
                    fee_cost=_num(fee.get("cost")) or 0.0,
                    fee_asset=str(fee.get("currency") or quote),
                    ts=fts / 1000.0 if fts else self.clock.wall(),
                )
            )
        if fills:
            order.fills = fills
            fsum = sum(f.amount for f in fills)
            if filled <= 0 and fsum > 0:
                filled = fsum
            fee_quote = 0.0
            for f in fills:
                if f.fee_asset.upper() == base.upper() and f.fee_asset.upper() != quote.upper():
                    fee_quote += f.fee_cost * f.price
                else:
                    fee_quote += f.fee_cost
            order.fee_cost_quote = fee_quote
            if avg is None and fsum > 0:
                avg = sum(f.price * f.amount for f in fills) / fsum
        order.filled = filled
        order.avg_price = avg if filled > 0 else None
        order.status = _map_status(raw.get("status"), filled, order.amount)
        now = self.clock.mono()
        if filled > 0 and order.first_fill_mono is None:
            order.first_fill_mono = now
        if order.is_done and order.done_mono is None:
            order.done_mono = now
        self._cache(order)
        return order

    def _order_from_raw(self, raw: dict[str, Any], symbol: str) -> Order:
        """Build/refresh an Order from a ccxt structure, reusing the cache so
        sent/ack timestamps survive ``fetch_order``."""
        oid = str(raw.get("id") or "")
        cid = str(raw.get("clientOrderId") or "")
        order = self._orders.get(oid) or self._orders_by_client.get(cid)
        if order is None:
            side_raw = str(raw.get("side") or "buy").lower()
            type_raw = str(raw.get("type") or "limit").lower()
            order = Order(
                id=oid,
                client_id=cid,
                exchange=self.name,
                symbol=str(raw.get("symbol") or symbol),
                side=Side.SELL if side_raw == "sell" else Side.BUY,
                type=OrderType.MARKET if type_raw == "market" else OrderType.LIMIT,
                amount=_num(raw.get("amount")) or 0.0,
                price=_num(raw.get("price")),
            )
        return self._apply_ccxt_order(order, raw)

    async def place_order(self, req: OrderRequest, *, reduce_only: bool = False) -> Order:
        """Submit ``req``.  ``reduce_only=True`` (extra kwarg, swap only) marks a
        position-closing order so the exchange never flips the position."""
        priv = self._require_private()
        self._assert_demo_host()
        params = self._order_params(req, reduce_only)
        order = Order(
            id="",
            client_id=req.client_id,
            exchange=self.name,
            symbol=req.symbol,
            side=req.side,
            type=req.type,
            amount=req.amount,
            price=req.price,
            status=OrderStatus.SENT,
            sent_ts=self.clock.wall(),
            sent_mono=self.clock.mono(),
        )
        self._cache(order)
        # MARKET orders pass req.price (if any) only as a reference price: ccxt
        # uses it to derive the quote cost on venues that require it (Bybit spot).
        try:
            raw = await priv.create_order(req.symbol, req.type.value, req.side.value, req.amount, req.price, params)
        except asyncio.CancelledError:
            raise
        except ccxt.RequestTimeout as exc:
            self._mark_error("RequestTimeout")
            order.error = f"timeout: status unknown, reconcile with fetch_order(client_id={req.client_id})"
            log.warning("%s: create_order timed out (%s); order %s status unknown", self.name, str(exc)[:100], req.client_id)
            return order
        except Exception as exc:  # noqa: BLE001
            self._mark_error(type(exc).__name__)
            err = self._translate(exc, priv)
            order.status = OrderStatus.REJECTED if isinstance(err, (OrderRejected, InsufficientBalance)) else OrderStatus.FAILED
            order.error = str(err)[:200]
            order.done_mono = self.clock.mono()
            raise err from exc
        order.ack_mono = self.clock.mono()
        self._mark_order_latency((order.ack_mono - (order.sent_mono or order.ack_mono)) * 1000.0)
        self._apply_ccxt_order(order, raw)
        if order.status == OrderStatus.SENT:
            order.status = OrderStatus.ACKNOWLEDGED
        return order

    async def fetch_order(self, order_id: str, symbol: str, *, client_id: str | None = None) -> Order:
        priv = self._require_private()
        params: dict[str, Any] = {}
        oid: Any = order_id or None
        if not order_id:
            cached = self._orders_by_client.get(client_id or "")
            if cached is not None and cached.id:
                oid = cached.id
            elif client_id:
                params[self._client_key] = client_id
            else:
                raise ExchangeError(f"{self.name}: fetch_order needs an order id or client id")
        raw = await self._private_call(priv.fetch_order, oid, symbol, params)
        order = self._order_from_raw(raw or {}, symbol)
        if client_id and not order.client_id:
            order.client_id = client_id
            self._cache(order)
        return order

    async def cancel_order(self, order_id: str, symbol: str) -> Order:
        priv = self._require_private()
        try:
            raw = await priv.cancel_order(order_id, symbol)
        except asyncio.CancelledError:
            raise
        except ccxt.OrderNotFound as exc:
            try:
                return await self.fetch_order(order_id, symbol)
            except ExchangeError:
                raise OrderRejected(f"{self.name}: order {order_id} not found") from exc
        except Exception as exc:  # noqa: BLE001
            self._mark_error(type(exc).__name__)
            raise self._translate(exc, priv) from exc
        raw = raw if isinstance(raw, dict) else {}
        if not raw.get("id"):
            raw = dict(raw, id=order_id)
        order = self._order_from_raw(raw, symbol)
        if not order.is_done:
            order.status = OrderStatus.CANCELLED
            order.done_mono = self.clock.mono()
        return order

    async def cancel_all(self, symbol: str | None = None) -> int:
        if self.private is None:
            return 0
        if hasattr(self.private, "cancel_all_orders"):
            try:
                res = await self.private.cancel_all_orders(symbol)
                if isinstance(res, list):
                    for raw in res:
                        if isinstance(raw, dict) and raw.get("id"):
                            self._order_from_raw(raw, symbol or str(raw.get("symbol") or ""))
                    return len(res)
                return 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.info("%s: cancel_all_orders failed (%s); cancelling one by one", self.name, type(exc).__name__)
        count = 0
        for o in await self.open_orders(symbol):
            try:
                await self.cancel_order(o.id, o.symbol)
                count += 1
            except ExchangeError as exc:
                log.warning("%s: cancel %s failed: %s", self.name, o.id, exc)
        return count

    async def open_orders(self, symbol: str | None = None) -> list[Order]:
        if self.private is None:
            return []
        raw = await self._private_call(self.private.fetch_open_orders, symbol)
        out: list[Order] = []
        for r in raw or []:
            if isinstance(r, dict):
                out.append(self._order_from_raw(r, symbol or str(r.get("symbol") or "")))
        return out
