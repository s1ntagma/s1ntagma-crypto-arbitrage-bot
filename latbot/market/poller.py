"""TickerPoller - REST breadth coverage (ARCHITECTURE §2.3).

Every ``settings.ticker_poll_interval_s`` the poller calls ``fetch_tickers``
on every market adapter whose ``ExchangeConfig.poll_tickers`` is on (one REST
call per exchange), then:

1. ``state.on_ticker(ticker)`` for every ticker - above all the 24h quote
   volume that the Phase-2 gates (``min_mover_volume_quote`` /
   ``min_target_volume_quote``) need; WebSocket book/bbo streams carry no
   volume, so this is what makes the watchlist assets eligible at all.  For
   assets the same exchange streams over WebSocket the ticker updates volume
   / ticker / liveness only (``append_mid=False``): a REST price is one
   interval old and must never become the newest point of the live series.
2. ``detector.on_ticker(ticker)`` (``source="poll"``) ONCE per asset per
   round (detection is asset-wide), and only for assets at least one polled
   exchange does not stream (streamed assets are detected on their stream).
3. every ``LagSignal`` -> ``pipeline.evaluate`` (the pipeline confirms polled
   signals on a fresh REST order book - in the background - before creating
   an opportunity).

A round never blocks the event loop for long: it yields every few hundred
tickers, and it does not wait for REST book confirmations.  Per-exchange
backoff on errors (interval -> 60 s, ``RateLimited.retry_after_s`` honoured).
``set_assets`` narrows the polled universe at runtime (the app starts with the
watchlist and upgrades to ``registry.resolve_universe`` once the exchanges
answer).
"""
from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Iterable

from ..clock import Clock
from ..config import Settings
from ..exchanges.base import RateLimited
from ..models import Ticker

log = logging.getLogger(__name__)

_BACKOFF_MAX_S = 60.0
_MIN_INTERVAL_S = 0.2
_YIELD_EVERY_TICKERS = 200  # give the loop back to WebSocket frames / UI / fill polling
_YIELD_EVERY_ASSETS = 100


class TickerPoller:
    """Polls REST tickers for breadth coverage (see module docstring)."""

    def __init__(
        self,
        settings: Settings,
        adapters: dict[str, Any],
        state: Any,
        detector: Any,
        pipeline: Any,
        clock: Clock,
    ) -> None:
        self._settings = settings
        self._adapters = adapters
        self._state = state
        self._detector = detector
        self._pipeline = pipeline
        self._clock = clock
        self._assets: set[str] | None = None  # None = everything the adapter lists
        self._assets_order: list[str] = []
        self._task: asyncio.Task[None] | None = None
        self._next_allowed: dict[str, float] = {}
        self._backoff: dict[str, float] = {}
        self._failing: set[str] = set()
        self.polls = 0
        self.tickers_seen = 0
        self.signals_seen = 0
        self.evaluated = 0  # signals that produced or refreshed an opportunity on an already-fresh book

    # ------------------------------------------------------------------ #
    # configuration / lifecycle
    # ------------------------------------------------------------------ #
    @property
    def interval_s(self) -> float:
        return max(_MIN_INTERVAL_S, float(self._settings.ticker_poll_interval_s))

    @property
    def assets(self) -> list[str] | None:
        return None if self._assets is None else list(self._assets_order)

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def set_assets(self, assets: Iterable[str] | None) -> None:
        """Restrict polling to ``assets`` (None = every listed market)."""
        if assets is None:
            self._assets = None
            self._assets_order = []
            return
        cleaned = [str(a).strip().upper() for a in assets if str(a).strip()]
        self._assets_order = list(dict.fromkeys(cleaned))
        self._assets = set(self._assets_order)

    async def start(self, assets: Iterable[str] | None = None) -> None:
        """Start the poll loop (no-op when ``settings.ticker_poll_enabled`` is off)."""
        self.set_assets(assets)
        if not self._settings.ticker_poll_enabled:
            log.info("ticker polling disabled (ticker_poll_enabled=false)")
            return
        if self.running:
            return
        self._task = asyncio.create_task(self._loop(), name="latbot-ticker-poll")
        log.info(
            "ticker polling every %.1f s on %s (%s)",
            self.interval_s,
            ", ".join(name for name, _ in self._targets()) or "no exchange",
            "all listed assets" if self._assets is None else f"{len(self._assets)} assets",
        )

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass

    async def _loop(self) -> None:
        while True:
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("ticker poll failed")
            await asyncio.sleep(self.interval_s)

    # ------------------------------------------------------------------ #
    # one round
    # ------------------------------------------------------------------ #
    def _targets(self) -> list[tuple[str, Any]]:
        out: list[tuple[str, Any]] = []
        for name, adapter in self._adapters.items():
            cfg = self._settings.exchange(name)
            if cfg is None or not cfg.enabled or not cfg.poll_tickers:
                continue
            if not callable(getattr(adapter, "fetch_tickers", None)):
                continue
            out.append((name, adapter))
        return out

    def _streamed_assets(self, adapter: Any) -> set[str]:
        """Assets ``adapter`` streams over WebSocket (detected on the stream, not here)."""
        symbols = getattr(adapter, "supported_symbols", None)
        if not callable(symbols):
            return set()
        try:
            streamed = symbols() or set()
        except Exception:  # noqa: BLE001
            return set()
        asset_for = getattr(adapter, "asset_for", None)
        out: set[str] = set()
        for sym in streamed:
            asset = asset_for(sym) if callable(asset_for) else None
            out.add((asset or str(sym).split("/", 1)[0].split(":", 1)[0]).upper())
        return out

    async def _fetch(self, name: str, adapter: Any) -> dict[str, Ticker]:
        """fetch_tickers with per-exchange backoff; never raises."""
        now = self._clock.mono()
        try:
            fetch = adapter.fetch_tickers
            if self._assets is not None and _accepts_argument(fetch):
                res = await fetch(list(self._assets_order))
            else:
                res = await fetch()
        except asyncio.CancelledError:
            raise
        except RateLimited as exc:
            wait = max(self.interval_s, float(getattr(exc, "retry_after_s", 1.0) or 1.0))
            self._next_allowed[name] = now + wait
            log.info("%s: tickers rate limited; next poll in %.0f s", name, wait)
            return {}
        except Exception as exc:  # noqa: BLE001
            prev = self._backoff.get(name, 0.0)
            wait = min(_BACKOFF_MAX_S, max(self.interval_s, prev * 2.0))
            self._backoff[name] = wait
            self._next_allowed[name] = now + wait
            if name not in self._failing:
                self._failing.add(name)
                log.warning("%s: fetch_tickers failed (%s: %s); retry in %.0f s", name, type(exc).__name__, str(exc)[:120], wait)
            else:
                log.debug("%s: fetch_tickers still failing (%s); retry in %.0f s", name, type(exc).__name__, wait)
            return {}
        self._backoff.pop(name, None)
        self._next_allowed.pop(name, None)
        if name in self._failing:
            self._failing.discard(name)
            log.info("%s: tickers available again", name)
        out: dict[str, Ticker] = {}
        for key, t in (res or {}).items():
            if not isinstance(t, Ticker):
                continue
            asset = (t.asset or str(key)).upper()
            if not t.asset:
                t.asset = asset
            out[asset] = t
        return out

    async def poll_once(self) -> int:
        """One round over every eligible exchange.  Returns the number of tickers processed."""
        now = self._clock.mono()
        targets = [(n, a) for n, a in self._targets() if now >= self._next_allowed.get(n, 0.0)]
        if not targets:
            return 0
        self.polls += 1
        results = await asyncio.gather(*(self._fetch(n, a) for n, a in targets))
        count = 0
        # pass A: every ticker into MarketState (volume!), remember one ticker per asset
        # from an exchange that does not stream it (detection runs once per asset)
        pending: dict[str, Ticker] = {}
        processed = 0
        for (name, adapter), tickers in zip(targets, results):
            if not tickers:
                continue
            streamed = self._streamed_assets(adapter)
            for asset, ticker in tickers.items():
                if self._assets is not None and asset not in self._assets:
                    continue
                count += 1
                is_streamed = asset in streamed
                try:
                    self._state.on_ticker(ticker, append_mid=not is_streamed)
                except Exception:  # noqa: BLE001
                    log.exception("state.on_ticker failed for %s %s", name, asset)
                    continue
                if not is_streamed and asset not in pending:
                    pending[asset] = ticker
                processed += 1
                if processed % _YIELD_EVERY_TICKERS == 0:
                    await asyncio.sleep(0)
        # pass B: asset-wide detection, once per asset, on all freshly stored tickers
        detected = 0
        for asset, ticker in pending.items():
            try:
                signals = self._detector.on_ticker(ticker, source="poll") or []
            except Exception:  # noqa: BLE001
                log.exception("detector.on_ticker failed for %s %s", ticker.exchange, asset)
                signals = []
            for sig in signals:
                self.signals_seen += 1
                try:
                    opp = await self._pipeline.evaluate(sig)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001
                    log.exception("pipeline.evaluate failed for polled signal %s on %s", sig.asset, sig.target)
                    continue
                if opp is not None:
                    self.evaluated += 1
            detected += 1
            if detected % _YIELD_EVERY_ASSETS == 0:
                await asyncio.sleep(0)
        self.tickers_seen += count
        return count


def _accepts_argument(fn: Any) -> bool:
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False
    return any(
        p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.VAR_POSITIONAL)
        for p in params.values()
    )


__all__ = ["TickerPoller"]
