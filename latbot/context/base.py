"""Shared contracts of the context package.

* ``HttpGetter``   - the only way collectors reach the network (tests inject a fake).
* ``PollingCollector`` - interval polling with exponential backoff, Retry-After
  handling and an honest ``SourceStatus`` (ok / error / blocked / no key).
* ``MacroEvent`` / ``NewsItem`` - normalised items every source produces.
* ``Factor`` - one objective input to the signal-quality model, always relative
  to the TRADE DIRECTION (+ supports the trade, - argues against it).  A factor
  without data is ``available=False`` and is never counted as support.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import re
import unicodedata
from dataclasses import asdict, dataclass, field
from typing import Any, Protocol

from ..logging_setup import redact

log = logging.getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36 latbot"

MACRO_EVENT_TYPES: tuple[str, ...] = (
    "CPI", "CORE_CPI", "PPI", "CORE_PPI", "NFP", "UNEMPLOYMENT", "EARNINGS",
    "FOMC_RATE", "FOMC_STATEMENT", "FOMC_PRESSER", "FOMC_MINUTES", "FED_SPEECH",
    "GDP", "PCE", "CORE_PCE", "RETAIL_SALES", "JOBLESS_CLAIMS", "ISM_MANUFACTURING",
    "ISM_SERVICES", "JOLTS", "CONSUMER_SENTIMENT", "OTHER",
)

NEWS_CATEGORIES: tuple[str, ...] = (
    "fed_rates", "macro_data", "regulation", "etf", "hack", "exchange", "listing", "delisting",
    "whale_transfer", "liquidation", "unlock", "upgrade", "partnership", "geopolitics",
    "market_move", "other",
)

NEWS_SPEEDS: tuple[str, ...] = ("seconds", "minutes", "hours", "unknown")


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class HttpResponse:
    status: int
    text: str
    headers: dict[str, str] = field(default_factory=dict)  # lower-case keys

    def json(self) -> Any:
        return json.loads(self.text)


class HttpGetter(Protocol):
    async def get(
        self, url: str, *, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None,
        timeout_s: float = 10.0,
    ) -> HttpResponse: ...

    async def close(self) -> None: ...


class AiohttpGetter:
    """Real HTTP client: one lazily created aiohttp session (proxy env honoured),
    a browser-like User-Agent, bounded concurrency.  Network errors propagate
    (the collector turns them into a SourceStatus error)."""

    def __init__(self, *, user_agent: str = USER_AGENT, max_concurrency: int = 8) -> None:
        self._ua = user_agent
        self._sem = asyncio.Semaphore(max(1, int(max_concurrency)))
        self._session: Any = None

    async def _ensure(self) -> Any:
        if self._session is None or self._session.closed:
            import aiohttp

            self._session = aiohttp.ClientSession(trust_env=True, headers={"User-Agent": self._ua})
        return self._session

    async def get(
        self, url: str, *, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None,
        timeout_s: float = 10.0,
    ) -> HttpResponse:
        import aiohttp

        session = await self._ensure()
        async with self._sem:
            async with session.get(
                url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=float(timeout_s)),
                allow_redirects=True,
            ) as r:
                text = await r.text(errors="replace")
                return HttpResponse(r.status, text, {k.lower(): v for k, v in r.headers.items()})

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None


class SourceError(Exception):
    """A source answered with an error (HTTP >= 400, bad payload)."""


class RateLimitedSource(SourceError):
    def __init__(self, retry_after_s: float, message: str = "rate limited") -> None:
        super().__init__(message)
        self.retry_after_s = max(1.0, float(retry_after_s))


def check_response(resp: HttpResponse, source: str, *, default_retry_s: float = 60.0) -> None:
    """Raise RateLimitedSource on 429/418 (Retry-After seconds when given), SourceError on other errors."""
    if resp.status in (418, 429):
        ra = resp.headers.get("retry-after", "")
        try:
            wait = float(ra)
        except ValueError:
            wait = default_retry_s
        raise RateLimitedSource(wait, f"{source}: HTTP {resp.status} (rate limited)")
    if resp.status in (401, 403, 451):
        raise SourceError(f"{source}: blocked (HTTP {resp.status})")
    if resp.status >= 400:
        raise SourceError(f"{source}: HTTP {resp.status}")


# --------------------------------------------------------------------------- #
# items
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class SourceStatus:
    name: str
    enabled: bool = True
    ok: bool = False
    last_ok_ts: float | None = None
    last_attempt_ts: float | None = None
    last_error: str = ""
    items: int = 0
    note: str = ""  # e.g. "no API key", "blocked (HTTP 451)", "disabled in settings"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _finite(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


@dataclass(slots=True)
class Factor:
    """One objective input to the quality model, relative to the trade direction."""

    key: str  # stable id, e.g. "macro_event", "news", "funding"
    label: str  # short English UI label
    available: bool  # False -> no data: neutral, never support
    score: float = 0.0  # -1..+1: + supports the trade direction, - argues against it
    weight: float = 1.0  # relative importance (from settings)
    detail: str = ""  # Russian one-liner WITH the numbers behind it
    veto: bool = False  # hard block (e.g. CPI release in 60 s)
    data: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not _finite(self.score):
            self.score, self.available = 0.0, False
        self.score = max(-1.0, min(1.0, float(self.score)))
        self.weight = max(0.0, float(self.weight)) if _finite(self.weight) else 0.0
        if not self.available:
            self.score = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class MacroEvent:
    id: str
    title: str
    country: str
    ts: float  # scheduled release time, wall epoch seconds (UTC)
    impact: str  # "high" | "medium" | "low" | "holiday"
    event_type: str  # one of MACRO_EVENT_TYPES
    forecast: str = ""
    previous: str = ""
    actual: str = ""
    surprise: int | None = None  # +1 actual above forecast, -1 below, 0 in line, None unknown
    source: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class NewsItem:
    id: str  # stable: source + native id (or hash of the normalised title)
    ts: float  # publication time, wall epoch seconds
    source: str  # "telegram" | "rss"
    channel: str  # channel handle / feed name
    title: str
    text: str = ""
    url: str = ""
    assets: list[str] = field(default_factory=list)  # tickers; ["*"] = whole crypto market
    category: str = "other"  # one of NEWS_CATEGORIES
    importance: float = 0.0  # 0..1
    direction: int = 0  # +1 bullish, -1 bearish, 0 unclear (for the listed assets)
    speed: str = "unknown"  # one of NEWS_SPEEDS
    confidence: float = 0.0  # 0..1
    classifier: str = "none"  # "rules" | "claude" | "none"
    reason: str = ""
    dedup_key: str = ""
    data: dict[str, Any] = field(default_factory=dict)  # parsed extras (whale transfer, ETF flow...)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def stable_id(*parts: Any) -> str:
    h = hashlib.sha1("\x1f".join(str(p) for p in parts).encode("utf-8")).hexdigest()
    return h[:16]


_WS = re.compile(r"\s+")
_NON_WORD = re.compile(r"[^\w%$.]+", re.UNICODE)


def normalise_title(text: str) -> str:
    """Lower-case, accents/emoji/punctuation stripped, whitespace collapsed (dedup across sources)."""
    t = unicodedata.normalize("NFKD", text or "")
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    t = _NON_WORD.sub(" ", t.lower())
    return _WS.sub(" ", t).strip()


# --------------------------------------------------------------------------- #
# polling
# --------------------------------------------------------------------------- #
class PollingCollector:
    """Base class: ``poll_once()`` every ``interval_s`` with exponential backoff.

    Subclasses implement ``poll_once() -> int`` (items updated) and raise
    ``SourceError`` / ``RateLimitedSource`` (see ``check_response``) on bad
    answers.  ``step()`` is what tests call; ``run()`` is the production loop.
    """

    name = "collector"
    max_backoff_s = 900.0

    def __init__(self, settings: Any, clock: Any, http: HttpGetter | None, *, interval_s: float, enabled: bool = True) -> None:
        self._settings = settings
        self._clock = clock
        self._http = http
        self.interval_s = max(1.0, float(interval_s))
        self.status = SourceStatus(name=self.name, enabled=bool(enabled))
        if not enabled:
            self.status.note = "disabled in settings"
        self._next_mono = 0.0
        self._failures = 0

    async def poll_once(self) -> int:  # pragma: no cover - abstract
        raise NotImplementedError

    def due(self, now_mono: float | None = None) -> bool:
        now = self._clock.mono() if now_mono is None else now_mono
        return self.status.enabled and now >= self._next_mono

    async def step(self) -> bool:
        """One guarded poll; True when it succeeded."""
        now = self._clock.mono()
        self.status.last_attempt_ts = self._clock.wall()
        try:
            n = await self.poll_once()
        except RateLimitedSource as e:
            self._fail(str(e), now, wait=e.retry_after_s)
            return False
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - a source must never take the bot down
            msg = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
            self._fail(msg, now)
            return False
        self._failures = 0
        self.status.ok = True
        self.status.last_ok_ts = self._clock.wall()
        self.status.last_error = ""
        if isinstance(n, int) and n >= 0:
            self.status.items = n
        self._next_mono = now + self.interval_s
        return True

    def _fail(self, message: str, now: float, *, wait: float | None = None) -> None:
        self._failures += 1
        backoff = min(self.max_backoff_s, self.interval_s * (2 ** min(self._failures - 1, 6)))
        if wait is not None:
            backoff = max(backoff, wait)
        self.status.ok = False
        self.status.last_error = redact(message)[:240]
        self._next_mono = now + backoff
        level = logging.WARNING if self._failures in (1, 5, 20) else logging.DEBUG
        log.log(level, "context source %s failed (%d in a row, next try in %.0f s): %s",
                self.name, self._failures, backoff, self.status.last_error)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            if self.due():
                await self.step()
            try:
                await asyncio.wait_for(stop.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass


__all__ = [
    "AiohttpGetter", "Factor", "HttpGetter", "HttpResponse", "MACRO_EVENT_TYPES", "MacroEvent",
    "NEWS_CATEGORIES", "NEWS_SPEEDS", "NewsItem", "PollingCollector", "RateLimitedSource",
    "SourceError", "SourceStatus", "USER_AGENT", "check_response", "normalise_title", "stable_id",
]
