"""US macro calendar (Forex Factory weekly export) + optional BLS actual values.

``MacroCalendar`` (collector ``"macro_calendar"``)
    * Source: ``ff_calendar_thisweek.xml`` (times in UTC, stable FF event-type id
      ``ebaseId`` in ``<url>``, declared cp1252); ``ff_calendar_thisweek.json`` is the
      fallback (ISO dates with an explicit New-York offset).  No tz database is used.
    * The export never contains actual values - only schedule, forecast, previous.
    * Polled at most every 30 min (the file changes at most hourly; FF allows 2
      downloads / 5 min / IP / file type).  HTTP 429 or the HTML "Request Denied"
      page -> ``RateLimitedSource`` (>= 300 s) and the last good copy stays in use.
    * The last good copy is kept in ``cache_path`` (``fetched_ts`` + raw body +
      actual/surprise values set later) and loaded at construction, so the
      calendar works offline and across restarts; its age is always reported.
    * Times 'All Day' / 'Tentative' / 'Day N' / 'Aug 15th' -> time UNKNOWN: such an
      event is listed (``events(include_undated=True)``, ``snapshot()``) but never
      takes part in timed windows (``upcoming``/``recent``/``next_event``/
      ``event_window``).  Its ``ts`` is 00:00 UTC of the listed date and is only a
      grouping key - never a release time.
    * ``surprise`` stays ``None`` until a real actual value arrives
      (``set_actual``: BLS, manual) or a squawk reports the sign (``set_surprise``).

``BlsActuals`` (collector ``"bls_actuals"``, optional)
    For a few minutes after a mapped US release (CPI, Core CPI, PPI, NFP,
    Unemployment Rate) polls the BLS Atom feeds and parses the headline numbers
    with strict regexes; with a BLS API key, values still missing (incl. Average
    Hourly Earnings, which the Atom headline lacks) are filled later from the BLS
    API v2 (documented ~1 day lag).  Nothing found -> nothing set (honest "no data").

Note: several FF rows describe ONE release (CPI m/m + CPI y/y + Core CPI at 12:30;
NFP + Unemployment Rate + AHE): consumers that learn per release should group by
``(event_type family, ts)`` to avoid counting one market reaction several times.
"""
from __future__ import annotations

import html
import json
import logging
import math
import os
import re
from calendar import timegm
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from ..logging_setup import register_secret
from .base import (
    USER_AGENT,
    MacroEvent,
    PollingCollector,
    RateLimitedSource,
    SourceError,
    check_response,
    stable_id,
)

log = logging.getLogger(__name__)

FF_XML_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"
FF_JSON_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
FF_MIN_INTERVAL_S = 1800.0  # never poll Forex Factory more often than every 30 min
FF_RATE_LIMIT_S = 300.0  # back-off after 429 / "Request Denied"
SOURCE = "forexfactory"
CACHE_VERSION = 1

IMPACT_RANK: dict[str, int] = {"holiday": 0, "low": 1, "medium": 2, "high": 3}

DEFAULT_COUNTRIES: tuple[str, ...] = ("USD",)
DEFAULT_IMPACTS: tuple[str, ...] = ("high", "medium")

# Stable Forex Factory event-type ids (ebaseId, from <url> of the XML export), verified on
# FF data Jul-Oct 2026.  These ids are global (US-only events), so no country check is needed.
EBASE_TYPES: dict[int, str] = {
    78: "CPI", 884: "CPI",  # CPI m/m, CPI y/y
    79: "CORE_CPI", 933: "CORE_CPI",  # Core CPI m/m, y/y
    86: "PPI", 87: "CORE_PPI",  # PPI m/m, Core PPI m/m
    66: "NFP", 56: "UNEMPLOYMENT", 159: "EARNINGS",  # Non-Farm Employment Change, Unemployment Rate, AHE m/m
    1: "FOMC_RATE", 4: "FOMC_STATEMENT", 527: "FOMC_STATEMENT",  # Federal Funds Rate, Statement, Economic Projections
    512: "FOMC_PRESSER", 304: "FOMC_MINUTES",
    129: "FED_SPEECH", 931: "FED_SPEECH",  # Fed Chair speaks / testifies
    2: "GDP", 27: "GDP", 28: "GDP",  # Advance / Prelim / Final GDP q/q
    85: "CORE_PCE",
    102: "RETAIL_SALES", 112: "RETAIL_SALES",
    11: "JOBLESS_CLAIMS",
    252: "ISM_MANUFACTURING", 255: "ISM_MANUFACTURING",  # ISM Manufacturing PMI / Prices
    253: "ISM_SERVICES",
    578: "JOLTS",
    53: "CONSUMER_SENTIMENT", 54: "CONSUMER_SENTIMENT",  # Prelim / Revised UoM Consumer Sentiment
    236: "CONSUMER_SENTIMENT", 238: "CONSUMER_SENTIMENT",  # Prelim / Revised UoM Inflation Expectations
    208: "CONSUMER_SENTIMENT",  # CB Consumer Confidence
}

# Exact FF titles (case-insensitive, whitespace-collapsed) -> type; used when no ebaseId is known
# (JSON fallback) or the id is not in the map.
TITLE_TYPES: dict[str, str] = {
    "cpi m/m": "CPI", "cpi y/y": "CPI",
    "core cpi m/m": "CORE_CPI", "core cpi y/y": "CORE_CPI",
    "ppi m/m": "PPI", "ppi y/y": "PPI",
    "core ppi m/m": "CORE_PPI", "core ppi y/y": "CORE_PPI",
    "non-farm employment change": "NFP",
    "unemployment rate": "UNEMPLOYMENT",
    "average hourly earnings m/m": "EARNINGS", "average hourly earnings y/y": "EARNINGS",
    "federal funds rate": "FOMC_RATE",
    "fomc statement": "FOMC_STATEMENT", "fomc economic projections": "FOMC_STATEMENT",
    "fomc press conference": "FOMC_PRESSER",
    "fomc meeting minutes": "FOMC_MINUTES",
    "advance gdp q/q": "GDP", "prelim gdp q/q": "GDP", "final gdp q/q": "GDP",
    "core pce price index m/m": "CORE_PCE", "core pce price index y/y": "CORE_PCE",
    "pce price index m/m": "PCE", "pce price index y/y": "PCE",
    "retail sales m/m": "RETAIL_SALES", "core retail sales m/m": "RETAIL_SALES",
    "unemployment claims": "JOBLESS_CLAIMS",
    "ism manufacturing pmi": "ISM_MANUFACTURING", "ism manufacturing prices": "ISM_MANUFACTURING",
    "ism services pmi": "ISM_SERVICES", "ism services prices": "ISM_SERVICES",
    "jolts job openings": "JOLTS",
    "prelim uom consumer sentiment": "CONSUMER_SENTIMENT", "revised uom consumer sentiment": "CONSUMER_SENTIMENT",
    "prelim uom inflation expectations": "CONSUMER_SENTIMENT",
    "revised uom inflation expectations": "CONSUMER_SENTIMENT",
    "cb consumer confidence": "CONSUMER_SENTIMENT",
}

_FED_SPEECH_RE = re.compile(
    r"^(?:fomc member|fed chair(?:man|woman)?|fed vice chair(?:man|woman)?(?: for supervision)?)"
    r"(?: [a-z][a-z.'-]*){1,2} (?:speaks|testifies)$",
    re.I,
)
_WS_RE = re.compile(r"\s+")


def _clean_title(title: str) -> str:
    return _WS_RE.sub(" ", (title or "").strip()).lower()


def classify_title(title: str, ebase_id: int | None = None) -> str:
    """FF title (+ optional ebaseId) -> one of ``MACRO_EVENT_TYPES`` ("OTHER" when unknown).

    The ebaseId map wins; otherwise exact titles; Fed speakers by a strict regex."""
    if ebase_id is not None and ebase_id in EBASE_TYPES:
        return EBASE_TYPES[ebase_id]
    t = _clean_title(title)
    if t in TITLE_TYPES:
        return TITLE_TYPES[t]
    if _FED_SPEECH_RE.match(t):
        return "FED_SPEECH"
    return "OTHER"


# --------------------------------------------------------------------------- #
# numbers / surprise
# --------------------------------------------------------------------------- #
_NUM_RE = re.compile(r"^([+-]?)(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?|\.\d+)([kmbt%]?)$", re.I)
_SCALE = {"": 1.0, "%": 1.0, "k": 1e3, "m": 1e6, "b": 1e9, "t": 1e12}


def _parse_with_unit(text: str | None) -> tuple[float, str] | None:
    t = (text or "").strip().replace("−", "-").replace(" ", "")
    t = t.replace(" ", "")
    m = _NUM_RE.match(t)
    if not m:
        return None
    sign, digits, suffix = m.groups()
    try:
        v = float(digits.replace(",", ""))
    except ValueError:  # pragma: no cover - regex guarantees a number
        return None
    suffix = suffix.lower()
    v *= _SCALE[suffix]
    if sign == "-":
        v = -v
    kind = "pct" if suffix == "%" else ("scaled" if suffix else "plain")
    return (v, kind) if math.isfinite(v) else None


def parse_number(text: str | None) -> float | None:
    """FF display string -> number: "0.3%" -> 0.3, "175K" -> 175000.0, "-1.2B" -> -1.2e9.

    Anything ambiguous ("4.83|2.7", "<0.1%", "", text) -> None."""
    p = _parse_with_unit(text)
    return None if p is None else p[0]


def surprise_of(actual: str, forecast: str, tolerance: float = 0.0) -> int | None:
    """+1 actual above forecast, -1 below, 0 within ``tolerance`` (same units as
    ``parse_number``), None when either side is missing or the units differ."""
    a, f = _parse_with_unit(actual), _parse_with_unit(forecast)
    if a is None or f is None:
        return None
    if a[1] != f[1]:
        return None
    diff = a[0] - f[0]
    eps = 1e-9 * max(1.0, abs(a[0]), abs(f[0]))
    if abs(diff) <= max(0.0, float(tolerance)) + eps:
        return 0
    return 1 if diff > 0 else -1


# --------------------------------------------------------------------------- #
# Forex Factory parsing
# --------------------------------------------------------------------------- #
_EVENT_RE = re.compile(r"<event>(.*?)</event>", re.S | re.I)
_CDATA_RE = re.compile(r"^<!\[CDATA\[(.*?)\]\]>$", re.S)
_XML_DATE_RE = re.compile(r"^(\d{1,2})-(\d{1,2})-(\d{4})$")
_XML_TIME_RE = re.compile(r"^(\d{1,2}):(\d{2})\s*([ap]m)$", re.I)
_EBASE_RE = re.compile(r"/calendar/(\d+)-")


def _tag(block: str, name: str) -> str:
    m = re.search(rf"<{name}\s*/>|<{name}>(.*?)</{name}>", block, re.S | re.I)
    if not m or m.group(1) is None:
        return ""
    v = m.group(1).strip()
    c = _CDATA_RE.match(v)
    v = c.group(1) if c else html.unescape(v)
    return _WS_RE.sub(" ", v).strip()


def _repair_cp1252(text: str) -> str:
    """A cp1252 body decoded as latin-1 shows C1 control characters; re-decode it.

    (A body already decoded as UTF-8 with replacement characters cannot be repaired -
    such characters stay U+FFFD; US titles are ASCII.)"""
    if any("\x80" <= ch <= "\x9f" for ch in text):
        try:
            return text.encode("latin-1").decode("cp1252")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return text
    return text


def _utc_hhmm(ts: float) -> str:
    hh, mm = divmod(int(ts) % 86400 // 60, 60)
    return f"{hh:02d}:{mm:02d} UTC"


def _norm_impact(raw: str) -> str:
    v = (raw or "").strip().lower()
    return v or "low"


def _row(title: str, country: str, ts: float, time_known: bool, time_label: str, impact: str,
         forecast: str, previous: str, ebase_id: int | None, fmt: str) -> dict[str, Any]:
    return {
        "title": title, "country": country, "ts": float(ts), "time_known": time_known,
        "time_label": time_label, "impact": _norm_impact(impact), "forecast": forecast,
        "previous": previous, "ebase_id": ebase_id, "format": fmt,
    }


def parse_ff_xml(data: str | bytes) -> list[dict[str, Any]]:
    """Parse ``ff_calendar_thisweek.xml`` (dates/times are UTC).  Bytes are decoded as cp1252.

    Returns rows: title, country, ts, time_known, time_label, impact, forecast, previous,
    ebase_id, format.  Malformed events are skipped (never guessed)."""
    text = data.decode("cp1252", errors="replace") if isinstance(data, (bytes, bytearray)) else _repair_cp1252(data)
    rows: list[dict[str, Any]] = []
    for m in _EVENT_RE.finditer(text):
        block = m.group(1)
        title, country = _tag(block, "title"), _tag(block, "country").upper()
        dm = _XML_DATE_RE.match(_tag(block, "date"))
        if not title or not country or not dm:
            continue
        mon, day, year = (int(x) for x in dm.groups())
        if not (1 <= mon <= 12 and 1 <= day <= 31):
            continue
        time_label = _tag(block, "time")
        tm = _XML_TIME_RE.match(time_label)
        ts: float
        known = False
        try:
            if tm:
                hh, mm, ap = int(tm.group(1)), int(tm.group(2)), tm.group(3).lower()
                if 1 <= hh <= 12 and 0 <= mm <= 59:
                    hh = (0 if hh == 12 else hh) + (12 if ap == "pm" else 0)
                    ts, known = float(timegm((year, mon, day, hh, mm, 0))), True
            if not known:
                ts = float(timegm((year, mon, day, 0, 0, 0)))
            datetime(year, mon, day)  # validates the date (timegm does not)
        except (ValueError, OverflowError):
            continue
        eb = _EBASE_RE.search(_tag(block, "url"))
        rows.append(_row(title, country, ts, known, _utc_hhmm(ts) if known else (time_label or "время неизвестно"),
                         _tag(block, "impact"), _tag(block, "forecast"), _tag(block, "previous"),
                         int(eb.group(1)) if eb else None, "xml"))
    return rows


def parse_ff_json(data: str | bytes | list[Any]) -> list[dict[str, Any]]:
    """Parse ``ff_calendar_thisweek.json`` (ISO dates with an explicit offset).

    The JSON cannot mark 'All Day'/'Tentative': a local 00:00 time or a missing offset is
    treated as time UNKNOWN (never as a midnight release)."""
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8", errors="replace")
    items = json.loads(data) if isinstance(data, str) else data
    if not isinstance(items, list):
        raise ValueError("FF JSON: expected a list")
    rows: list[dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        title = _WS_RE.sub(" ", str(it.get("title") or "")).strip()
        country = str(it.get("country") or "").strip().upper()
        raw_date = str(it.get("date") or "").strip()
        if not title or not country or not raw_date:
            continue
        try:
            dt = datetime.fromisoformat(raw_date)
        except ValueError:
            continue
        if dt.tzinfo is not None and dt.utcoffset() is not None and (dt.hour, dt.minute, dt.second) != (0, 0, 0):
            ts, known = dt.timestamp(), True
            label = _utc_hhmm(ts)
        else:
            ts = float(timegm((dt.year, dt.month, dt.day, 0, 0, 0)))
            known, label = False, "время неизвестно"
        rows.append(_row(title, country, ts, known, label, str(it.get("impact") or ""),
                         str(it.get("forecast") or "").strip(), str(it.get("previous") or "").strip(), None, "json"))
    return rows


def _looks_html(text: str) -> bool:
    head = text.lstrip()[:400].lower()
    return head.startswith("<!doctype html") or head.startswith("<html") or "<html" in head[:200]


def _check_ff_body(resp: Any, what: str) -> str:
    """HTTP status + body sanity for an FF export; raises RateLimitedSource / SourceError."""
    text = resp.text or ""
    is_html = _looks_html(text)
    if is_html:
        low = text.lower()
        if "request denied" in low or "exceeded the limit" in low:  # FF export limit page (any status)
            raise RateLimitedSource(FF_RATE_LIMIT_S, f"{what}: Request Denied (FF export limit)")
    try:
        check_response(resp, what, default_retry_s=FF_RATE_LIMIT_S)
    except RateLimitedSource as e:
        raise RateLimitedSource(max(FF_RATE_LIMIT_S, e.retry_after_s), str(e)) from None
    if is_html:
        raise SourceError(f"{what}: HTML instead of a calendar")
    if not text.strip():
        raise SourceError(f"{what}: empty body")
    return text


def _num_setting(settings: Any, name: str, default: float) -> float:
    v = getattr(settings, name, default)
    try:
        f = float(v)
    except (TypeError, ValueError):
        return float(default)
    return f if math.isfinite(f) else float(default)


@dataclass(slots=True)
class _Meta:
    ebase_id: int | None
    time_known: bool
    time_label: str
    surprise_source: str = ""  # "actual" | "squawk" | "" (unknown)


class MacroCalendar(PollingCollector):
    """Forex Factory weekly calendar (XML preferred, JSON fallback) with an on-disk last good copy."""

    name = "macro_calendar"

    def __init__(self, settings: Any, clock: Any, http: Any, *, cache_path: Path | None = None) -> None:
        interval = max(FF_MIN_INTERVAL_S, _num_setting(settings, "macro_refresh_s", 3600.0))
        super().__init__(settings, clock, http, interval_s=interval,
                         enabled=bool(getattr(settings, "macro_enabled", True)))
        self._cache_path = Path(cache_path) if cache_path else None
        self._all: dict[str, MacroEvent] = {}
        self._meta: dict[str, _Meta] = {}
        self._overrides: dict[str, dict[str, Any]] = {}
        self.fetched_ts: float | None = None  # wall time of the last good download (or of the cached one)
        self.format: str = ""  # "xml" | "json" | ""
        self.from_cache: bool = False
        self._body: str = ""
        self._last_modified: dict[str, str] = {}
        self._load_cache()

    # ------------------------------------------------------------------ settings
    def _countries(self) -> set[str]:
        v = getattr(self._settings, "macro_countries", DEFAULT_COUNTRIES)
        return {str(c).strip().upper() for c in (v or ()) if str(c).strip()}

    def _impacts(self) -> set[str]:
        v = getattr(self._settings, "macro_impacts", DEFAULT_IMPACTS)
        return {str(c).strip().lower() for c in (v or ()) if str(c).strip()}

    # ------------------------------------------------------------------ polling
    async def poll_once(self) -> int:
        if self._http is None:
            raise SourceError("no HTTP client")
        errors: list[str] = []
        rate_wait: float | None = None
        for fmt, url in (("xml", FF_XML_URL), ("json", FF_JSON_URL)):
            what = f"ForexFactory {fmt.upper()}"
            headers = {"User-Agent": USER_AGENT, "Accept": "application/xml,text/xml,application/json;q=0.9,*/*;q=0.5"}
            lm = self._last_modified.get(fmt, "")
            if lm and self.format == fmt and self._all:
                headers["If-Modified-Since"] = lm
            try:
                resp = await self._http.get(url, headers=headers, timeout_s=20.0)
                if resp.status == 304 and self.format == fmt and self._all:
                    self.fetched_ts = self._clock.wall()
                    self.from_cache = False
                    self.status.note = "не изменился (304)"
                    self._save_cache()
                    return len(self._all)
                body = _check_ff_body(resp, what)
                rows = parse_ff_xml(body) if fmt == "xml" else parse_ff_json(body)
                if not rows:
                    raise SourceError(f"{what}: no events parsed")
            except RateLimitedSource as e:
                rate_wait = max(rate_wait or 0.0, e.retry_after_s, FF_RATE_LIMIT_S)
                errors.append(str(e))
                continue
            except (SourceError, ValueError) as e:
                errors.append(str(e) or type(e).__name__)
                continue
            except Exception as e:  # noqa: BLE001 - network error: try the other format, then report
                errors.append(f"{what}: {type(e).__name__}: {e}" if str(e) else f"{what}: {type(e).__name__}")
                continue
            self._install(rows, fmt, body, self._clock.wall(), from_cache=False)
            self._last_modified[fmt] = (resp.headers or {}).get("last-modified", "")
            self.status.note = "" if fmt == "xml" else "XML недоступен, используется JSON"
            self._save_cache()
            log.info("macro calendar: %d events (%s, %d in filter)", len(self._all), fmt, len(self.events(True)))
            return len(self._all)
        if self._all:
            self.status.note = "последняя удачная копия" + (" (кэш)" if self.from_cache else "")
        msg = "; ".join(errors) or "no data"
        if rate_wait is not None:
            raise RateLimitedSource(rate_wait, msg)
        raise SourceError(msg)

    # ------------------------------------------------------------------ install / cache
    def _install(self, rows: Iterable[dict[str, Any]], fmt: str, body: str, fetched_ts: float | None,
                 *, from_cache: bool) -> None:
        events: dict[str, MacroEvent] = {}
        meta: dict[str, _Meta] = {}
        for r in rows:
            key_time = int(r["ts"]) if r["time_known"] else "date:" + _utc_date(r["ts"])
            eid = stable_id("ff", r["country"], r["title"], key_time)
            if eid in events:
                continue
            ev = MacroEvent(
                id=eid, title=r["title"], country=r["country"], ts=float(r["ts"]), impact=r["impact"],
                event_type=classify_title(r["title"], r.get("ebase_id")), forecast=r["forecast"],
                previous=r["previous"], source=SOURCE,
            )
            m = _Meta(ebase_id=r.get("ebase_id"), time_known=bool(r["time_known"]), time_label=str(r["time_label"]))
            ov = self._overrides.get(eid)
            if ov:
                ev.actual = str(ov.get("actual") or "")
                s = ov.get("surprise")
                ev.surprise = int(s) if s in (-1, 0, 1) else None
                m.surprise_source = str(ov.get("surprise_source") or "") if ev.surprise is not None else ""
            events[eid] = ev
            meta[eid] = m
        self._all, self._meta = events, meta
        self._overrides = {k: v for k, v in self._overrides.items() if k in events}
        self.format, self._body, self.fetched_ts, self.from_cache = fmt, body, fetched_ts, from_cache
        self.status.items = len(events)

    def _load_cache(self) -> None:
        p = self._cache_path
        if p is None or not p.is_file():
            return
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if not isinstance(data, dict) or data.get("source") != SOURCE:
                raise ValueError("unexpected cache content")
            fmt, body = str(data.get("format") or ""), str(data.get("body") or "")
            rows = parse_ff_xml(body) if fmt == "xml" else parse_ff_json(body) if fmt == "json" else []
            if not rows:
                raise ValueError("cache has no events")
            ov = data.get("overrides") or {}
            self._overrides = {str(k): dict(v) for k, v in ov.items() if isinstance(v, dict)}
            fetched = data.get("fetched_ts")
            fetched_ts = float(fetched) if isinstance(fetched, (int, float)) and math.isfinite(fetched) else None
            lm = data.get("last_modified") or {}
            self._last_modified = {str(k): str(v) for k, v in lm.items()} if isinstance(lm, dict) else {}
        except (OSError, ValueError, TypeError, AttributeError) as e:
            log.warning("macro calendar cache %s ignored: %s", p, e)
            return
        self._install(rows, fmt, body, fetched_ts, from_cache=True)
        self.status.note = "из кэша"
        if fetched_ts is not None:
            age = self._clock.wall() - fetched_ts
            if 0 <= age < self.interval_s:  # do not hammer FF after a restart
                self._next_mono = self._clock.mono() + (self.interval_s - age)
        log.info("macro calendar: %d events loaded from cache (%s)", len(self._all), fmt)

    def _save_cache(self) -> None:
        p = self._cache_path
        if p is None or not self._body:
            return
        data = {
            "version": CACHE_VERSION, "source": SOURCE, "format": self.format, "fetched_ts": self.fetched_ts,
            "last_modified": self._last_modified, "body": self._body, "overrides": self._overrides,
        }
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_name(p.name + ".tmp")
            tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            os.replace(tmp, p)
        except OSError as e:
            log.warning("macro calendar cache %s not written: %s", p, e)

    # ------------------------------------------------------------------ queries
    def events(self, include_undated: bool = False) -> list[MacroEvent]:
        """Events of ``macro_countries`` with impact in ``macro_impacts``, sorted by ts.

        Only events with a known release time unless ``include_undated``."""
        countries, impacts = self._countries(), self._impacts()
        out = [
            e for e in self._all.values()
            if e.country in countries and e.impact in impacts
            and (include_undated or self._meta[e.id].time_known)
        ]
        out.sort(key=lambda e: (e.ts, e.title))
        return out

    def get(self, event_id: str) -> MacroEvent | None:
        return self._all.get(event_id)

    def is_time_known(self, event_id: str) -> bool:
        m = self._meta.get(event_id)
        return bool(m and m.time_known)

    def event_info(self, event_id: str) -> dict[str, Any] | None:
        """MacroEvent.to_dict() + ebase_id, time_known, time_label, surprise_source."""
        ev, m = self._all.get(event_id), self._meta.get(event_id)
        if ev is None or m is None:
            return None
        d = ev.to_dict()
        d.update(ebase_id=m.ebase_id, time_known=m.time_known, time_label=m.time_label,
                 surprise_source=m.surprise_source)
        return d

    def _timed(self, min_impact: str) -> list[MacroEvent]:
        rank = IMPACT_RANK.get(str(min_impact).lower(), IMPACT_RANK["high"])
        return [e for e in self.events() if IMPACT_RANK.get(e.impact, -1) >= rank]

    def upcoming(self, now_wall: float, within_s: float, min_impact: str = "high") -> list[MacroEvent]:
        """Timed events with now < ts <= now + within_s (oldest first)."""
        return [e for e in self._timed(min_impact) if now_wall < e.ts <= now_wall + within_s]

    def recent(self, now_wall: float, within_s: float, min_impact: str = "high") -> list[MacroEvent]:
        """Timed events with now - within_s <= ts <= now (oldest first)."""
        return [e for e in self._timed(min_impact) if now_wall - within_s <= e.ts <= now_wall]

    def next_event(self, now_wall: float, min_impact: str = "high") -> MacroEvent | None:
        for e in self._timed(min_impact):
            if e.ts > now_wall:
                return e
        return None

    def event_window(self, now_wall: float) -> list[tuple[MacroEvent, str]]:
        """High-impact timed events whose window contains ``now_wall``:
        ``"before"`` within ``macro_block_before_s`` (300) ahead of the release,
        ``"after"`` within ``macro_window_after_s`` (3600) after it."""
        before = max(0.0, _num_setting(self._settings, "macro_block_before_s", 300.0))
        after = max(0.0, _num_setting(self._settings, "macro_window_after_s", 3600.0))
        out: list[tuple[MacroEvent, str]] = []
        for e in self._timed("high"):
            if e.ts - before <= now_wall < e.ts:
                out.append((e, "before"))
            elif e.ts <= now_wall <= e.ts + after:
                out.append((e, "after"))
        return out

    # ------------------------------------------------------------------ data validity
    def coverage(self) -> tuple[float, float] | None:
        """(start, end) wall seconds of the FF week the loaded file describes
        (Sunday 04:00 UTC = 00:00 New-York EDT, + 7 days)."""
        if not self._all:
            return None
        lo = min(e.ts for e in self._all.values())
        hi = max(e.ts for e in self._all.values())
        day = int(lo // 86400)
        if lo - day * 86400 < 4 * 3600:  # before Sunday 00:00 ET -> belongs to the previous UTC day
            day -= 1
        sunday = day - ((day + 4) % 7)  # 1970-01-01 was a Thursday
        start = sunday * 86400.0 + 4 * 3600.0
        end = start + 7 * 86400.0
        return start, max(end, hi + 3600.0)

    def available(self, now_wall: float | None = None) -> bool:
        """True when the loaded calendar describes the week containing ``now_wall``."""
        cov = self.coverage()
        if cov is None:
            return False
        now = self._clock.wall() if now_wall is None else now_wall
        return cov[0] <= now < cov[1]

    def age_s(self, now_wall: float | None = None) -> float | None:
        if self.fetched_ts is None:
            return None
        now = self._clock.wall() if now_wall is None else now_wall
        return max(0.0, now - self.fetched_ts)

    # ------------------------------------------------------------------ actual / surprise
    def set_actual(self, event_id: str, actual: str, *, source: str = "") -> bool:
        """Record the released value; the surprise is computed against the forecast
        (``macro_surprise_tolerance``, default 0).  A squawk surprise is kept when the
        actual cannot be compared (no forecast / different units)."""
        ev = self._all.get(event_id)
        actual = (actual or "").strip()
        if ev is None or not actual:
            return False
        tol = _num_setting(self._settings, "macro_surprise_tolerance", 0.0)
        s = surprise_of(actual, ev.forecast, tol)
        ev.actual = actual
        m = self._meta[event_id]
        if s is not None:
            ev.surprise, m.surprise_source = s, "actual"
        self._remember(ev, m)
        log.info("macro actual %s %s: %s (forecast %s) surprise=%s%s", ev.country, ev.title, actual,
                 ev.forecast or "-", ev.surprise, f" via {source}" if source else "")
        return True

    def set_surprise(self, event_id: str, surprise: int | None, *, source: str = "squawk") -> bool:
        """Surprise sign from a non-numeric source (news squawks).  An actual-based
        surprise is authoritative and is never overwritten."""
        ev = self._all.get(event_id)
        if ev is None or surprise not in (-1, 0, 1, None):
            return False
        m = self._meta[event_id]
        if m.surprise_source == "actual":
            return False
        ev.surprise = surprise
        m.surprise_source = (source or "squawk") if surprise is not None else ""
        self._remember(ev, m)
        return True

    def _remember(self, ev: MacroEvent, m: _Meta) -> None:
        self._overrides[ev.id] = {"actual": ev.actual, "surprise": ev.surprise, "surprise_source": m.surprise_source}
        self._save_cache()

    # ------------------------------------------------------------------ UI
    def snapshot(self, now_wall: float | None = None) -> dict[str, Any]:
        now = self._clock.wall() if now_wall is None else now_wall
        listed = [self.event_info(e.id) for e in self.events(include_undated=True)]
        nxt = self.next_event(now)
        cov = self.coverage()
        return {
            "enabled": self.status.enabled,
            "source": SOURCE,
            "format": self.format,
            "fetched_ts": self.fetched_ts,
            "age_s": self.age_s(now),
            "from_cache": self.from_cache,
            "available": self.available(now),
            "coverage": list(cov) if cov else None,
            "countries": sorted(self._countries()),
            "impacts": sorted(self._impacts()),
            "events": [d for d in listed if d and d["time_known"]],
            "time_unknown": [d for d in listed if d and not d["time_known"]],
            "next_high": self.event_info(nxt.id) if nxt else None,
            "window": [{"event": self.event_info(e.id), "phase": ph} for e, ph in self.event_window(now)],
            "status": self.status.to_dict(),
            "note": "Forex Factory: без фактических значений; surprise только из BLS / сквошей",
        }


def _utc_date(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d")


# --------------------------------------------------------------------------- #
# BLS actual values (optional)
# --------------------------------------------------------------------------- #
BLS_FEEDS: dict[str, str] = {
    "cpi": "https://www.bls.gov/feed/cpi.rss",
    "empsit": "https://www.bls.gov/feed/empsit.rss",
    "ppi": "https://www.bls.gov/feed/ppi.rss",
}
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/{series}"

# kind -> (Atom feed or None, BLS API series or None, API value mode)
BLS_KINDS: dict[str, tuple[str | None, str | None, str | None]] = {
    "cpi_mm": ("cpi", "CUSR0000SA0", "pct"),
    "cpi_yy": ("cpi", None, None),  # official y/y needs the NSA index (series id not verified)
    "core_cpi_mm": ("cpi", "CUSR0000SA0L1E", "pct"),
    "core_cpi_yy": ("cpi", None, None),
    "ppi_mm": ("ppi", "WPSFD4", "pct"),
    "ppi_yy": ("ppi", None, None),
    "nfp": ("empsit", "CES0000000001", "diff_k"),
    "unemployment": ("empsit", "LNS14000000", "level_pct"),
    "ahe_mm": (None, "CES0500000003", "pct"),  # not in the Atom headline
}


def bls_kind(event: MacroEvent) -> str | None:
    """Which BLS headline number an FF event corresponds to (None: not a BLS-mapped release)."""
    if event.country != "USD":
        return None
    t = _clean_title(event.title)
    mm, yy = t.endswith("m/m"), t.endswith("y/y")
    et = event.event_type
    if et == "CPI":
        return "cpi_mm" if mm else "cpi_yy" if yy else None
    if et == "CORE_CPI":
        return "core_cpi_mm" if mm else "core_cpi_yy" if yy else None
    if et == "PPI":
        return "ppi_mm" if mm else "ppi_yy" if yy else None
    if et == "NFP":
        return "nfp"
    if et == "UNEMPLOYMENT":
        return "unemployment"
    if et == "EARNINGS":
        return "ahe_mm" if mm else None
    return None


_ENTRY_RE = re.compile(r"<entry\b[^>]*>(.*?)</entry>", re.S | re.I)
_TAG_STRIP_RE = re.compile(r"<[^>]+>")


def _atom_text(block: str, name: str) -> str:
    m = re.search(rf"<{name}\b[^>]*?(?:/>|>(.*?)</{name}>)", block, re.S | re.I)
    if not m or m.group(1) is None:
        return ""
    v = m.group(1).strip()
    c = _CDATA_RE.match(v)
    v = c.group(1) if c else html.unescape(v)
    v = html.unescape(_TAG_STRIP_RE.sub(" ", v)) if "<" in v else v
    return _WS_RE.sub(" ", v).strip()


def parse_bls_feed(text: str) -> list[dict[str, Any]]:
    """BLS Atom feed -> entries: title, content, published_ts (None when unparsable), link, id."""
    out: list[dict[str, Any]] = []
    for m in _ENTRY_RE.finditer(text or ""):
        block = m.group(1)
        raw = _atom_text(block, "published") or _atom_text(block, "updated")
        ts: float | None = None
        try:
            dt = datetime.fromisoformat(raw) if raw else None
            if dt is not None and dt.tzinfo is not None:
                ts = dt.timestamp()
        except ValueError:
            ts = None
        link = re.search(r"<link\b[^>]*\bhref=\"([^\"]+)\"", block, re.I)
        out.append({
            "title": _atom_text(block, "title"), "content": _atom_text(block, "content") or _atom_text(block, "summary"),
            "published_ts": ts, "link": link.group(1) if link else "", "id": _atom_text(block, "id"),
        })
    return out


_UP = (r"(?:rose|increased|advanced|moved up|edged up|climbed|gained|rises|increases|advances|moves up|"
       r"edges up|climbs|gains)")
_DOWN = r"(?:fell|declined|decreased|moved down|edged down|dropped|falls|declines|decreases|moves down|edges down|drops)"
_DOWN_RE = re.compile(rf"^{_DOWN}$", re.I)
_MOVE = rf"(?P<verb>{_UP}|{_DOWN})"
_NUM = r"(?P<num>\d+(?:\.\d+)?)"
_MONTH = r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
_JOBS = r"(?P<num>\d{1,3}(?:,\d{3})*)"
_FILLER = r"(?:[^.]|(?<=\d)\.(?=\d))*?"  # within one sentence (decimal points allowed)


def _rx(*patterns: str) -> tuple[re.Pattern[str], ...]:
    return tuple(re.compile(p, re.I) for p in patterns)


_BLS_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "cpi_mm": _rx(
        rf"Consumer Price Index for All Urban Consumers {_MOVE} {_NUM} percent(?: in {_MONTH})?, seasonally adjusted",
        rf"Consumer Price Index for All Urban Consumers (?:was )?unchanged(?: in {_MONTH})?, seasonally adjusted",
        rf"^CPI for all items {_MOVE} {_NUM}% in {_MONTH}\b",
        rf"^CPI for all items (?:is )?unchanged in {_MONTH}\b",
    ),
    "cpi_yy": _rx(
        rf"Consumer Price Index for All Urban Consumers {_FILLER}\b{_MOVE} {_NUM} percent over the last 12 months, "
        r"not seasonally adjusted",
        rf"^CPI for all items {_MOVE} {_NUM}% over (?:the last )?12 months\b",
    ),
    "core_cpi_mm": _rx(
        rf"index for all items less food and energy {_MOVE} {_NUM} percent in {_MONTH} \(SA\)",
        rf"index for all items less food and energy (?:was )?unchanged in {_MONTH} \(SA\)",
    ),
    "core_cpi_yy": _rx(
        rf"index for all items less food and energy {_FILLER}\(SA\)[;,] (?P<dir>up|down) {_NUM} percent over the year \(NSA\)",
        rf"index for all items less food and energy {_MOVE} {_NUM} percent over the year \(NSA\)",
    ),
    "ppi_mm": _rx(
        rf"Producer Price Index for final demand {_MOVE} {_NUM} percent in {_MONTH}",
        rf"Producer Price Index for final demand (?:was )?unchanged in {_MONTH}",
        rf"^PPI for final demand {_MOVE} {_NUM}% in {_MONTH}\b",
        rf"^PPI for final demand (?:is )?unchanged in {_MONTH}\b",
    ),
    "ppi_yy": _rx(
        rf"(?:index for|prices for) final demand {_MOVE} {_NUM} percent for the 12 months ended in {_MONTH}",
    ),
    "nfp": _rx(
        rf"payroll employment \((?P<sign>[+-]){_JOBS}\)",
        rf"payroll employment (?:changed little|was little changed|{_UP}|{_DOWN}) in {_MONTH} \((?P<sign>[+-]){_JOBS}\)",
        rf"payroll employment {_MOVE}(?: by)? {_JOBS} in {_MONTH}",
    ),
    "unemployment": _rx(
        rf"unemployment rate \({_NUM}(?:%| percent)\)",
        rf"unemployment rate (?:was unchanged|unchanged|changed little|changes little|was little changed|ticked up|"
        rf"ticked down|ticks up|ticks down|{_UP}|{_DOWN})(?: in {_MONTH})? (?:at|to) {_NUM}(?:%| percent)",
        rf"unemployment rate was {_NUM} percent",
    ),
}


def _fmt_pct(v: float) -> str:
    v = round(v, 1)
    return f"{0.0 if v == 0 else v:.1f}%"


def _fmt_k(jobs: float) -> str:
    k = jobs / 1000.0
    return f"{int(round(k))}K" if abs(k - round(k)) < 1e-9 else f"{k:.1f}K"


def bls_headline(kind: str, title: str, content: str) -> str | None:
    """Headline number of a BLS release entry in FF display format ("0.4%", "29K", "4.2%").

    Every pattern match in title + content must agree; no match or a disagreement -> None."""
    pats = _BLS_PATTERNS.get(kind)
    if not pats:
        return None
    found: set[str] = set()
    for text in (_WS_RE.sub(" ", title or "").strip(), _WS_RE.sub(" ", content or "").strip()):
        for p in pats:
            for m in p.finditer(text):
                g = m.groupdict()
                num = g.get("num")
                if num is None:
                    v = 0.0  # "unchanged"
                else:
                    v = float(num.replace(",", ""))
                    if g.get("sign") == "-" or (g.get("dir") or "").lower() == "down" or (
                            g.get("verb") and _DOWN_RE.match(g["verb"])):
                        v = -v
                found.add(_fmt_k(v) if kind == "nfp" else _fmt_pct(v))
    return found.pop() if len(found) == 1 else None


def _prev_month(y: int, m: int) -> tuple[int, int]:
    return (y - 1, 12) if m == 1 else (y, m - 1)


def bls_api_value(payload: Any, mode: str, release_ts: float) -> tuple[str | None, str]:
    """BLS API v1/v2 single-series payload -> (FF-style value, state).

    state: "ok", "not_yet" (reference month not published yet), "too_late" (a newer month is
    already there), "bad" (gap / unparsable).  The reference month is the month before the
    release date.  Raises SourceError / RateLimitedSource for an API-level failure."""
    if not isinstance(payload, dict):
        raise SourceError("BLS API: unexpected payload")
    status = str(payload.get("status") or "")
    if status != "REQUEST_SUCCEEDED":
        msg = " ".join(str(x) for x in (payload.get("message") or []))[:200]
        if "threshold" in msg.lower():
            raise RateLimitedSource(6 * 3600.0, "BLS API: daily threshold reached")
        raise SourceError(f"BLS API: {status or 'no status'} {msg}".strip())
    try:
        data = payload["Results"]["series"][0]["data"]
    except (KeyError, IndexError, TypeError):
        return None, "bad"
    rows: list[tuple[int, int, float]] = []
    for r in data or []:
        try:
            period = str(r.get("period") or "")
            if not re.fullmatch(r"M(0[1-9]|1[0-2])", period):
                continue
            v = float(str(r.get("value")).replace(",", ""))
            if math.isfinite(v):
                rows.append((int(r.get("year")), int(period[1:]), v))
        except (TypeError, ValueError, AttributeError):
            continue
    if not rows:
        return None, "bad"
    rows.sort(key=lambda x: (x[0], x[1]), reverse=True)
    d = datetime.fromtimestamp(float(release_ts), tz=timezone.utc)
    ref = _prev_month(d.year, d.month)
    latest = (rows[0][0], rows[0][1])
    if latest < ref:
        return None, "not_yet"
    if latest > ref:
        return None, "too_late"
    v = rows[0][2]
    if mode == "level_pct":
        return _fmt_pct(v), "ok"
    if len(rows) < 2 or (rows[1][0], rows[1][1]) != _prev_month(*ref) or rows[1][2] == 0:
        return None, "bad"
    prev = rows[1][2]
    if mode == "pct":
        return _fmt_pct((v / prev - 1.0) * 100.0), "ok"
    if mode == "diff_k":
        return _fmt_k((v - prev) * 1000.0), "ok"
    return None, "bad"


class BlsActuals(PollingCollector):
    """Fills ``actual`` of released NFP / CPI / Core CPI / PPI / Unemployment events (and
    Average Hourly Earnings with an API key) from official BLS publications.

    Atom feeds (keyless) are polled every ``bls_poll_s`` for ``bls_window_s`` after the
    scheduled release; the matching entry (published within ``bls_match_s`` of the release)
    is parsed with strict regexes.  With ``api_key`` missing values are retried on the BLS
    API v2 from ``bls_api_delay_s`` to ``bls_api_max_age_s`` after the release.  Each found
    value goes to ``calendar.set_actual``; nothing found -> nothing set."""

    name = "bls_actuals"
    max_results = 50

    def __init__(self, settings: Any, clock: Any, http: Any, calendar: MacroCalendar, *, api_key: str = "") -> None:
        super().__init__(settings, clock, http, interval_s=max(2.0, _num_setting(settings, "bls_poll_s", 10.0)),
                         enabled=bool(getattr(settings, "bls_enabled", True)))
        self._calendar = calendar
        self._api_key = (api_key or "").strip()
        if self._api_key:
            register_secret(self._api_key)
        self._attempted: set[str] = set()
        self._atom_done: set[str] = set()
        self._done: set[str] = set()
        self._api_next: dict[str, float] = {}
        self.results: list[dict[str, Any]] = []
        if self.status.enabled and not self._api_key:
            self.status.note = "без BLS_API_KEY: только Atom-ленты BLS"

    @property
    def has_api_key(self) -> bool:
        return bool(self._api_key)

    def _headers(self) -> dict[str, str]:
        contact = str(getattr(self._settings, "bls_contact", "") or "").strip()
        return {"User-Agent": USER_AGENT + (f" ({contact})" if contact else ""),
                "Accept": "application/atom+xml,application/xml;q=0.9,*/*;q=0.5"}

    def _record(self, ev: MacroEvent, kind: str, value: str | None, source: str, now: float, note: str = "") -> None:
        self.results.append({
            "event_id": ev.id, "title": ev.title, "event_type": ev.event_type, "kind": kind, "actual": value,
            "source": source, "found_ts": now if value else None, "delay_s": (now - ev.ts) if value else None,
            "note": note,
        })
        del self.results[:-self.max_results]

    def _apply(self, ev: MacroEvent, kind: str, value: str, source: str, now: float) -> None:
        self._calendar.set_actual(ev.id, value, source=source)
        self._done.add(ev.id)
        self._record(ev, kind, value, source, now)
        log.info("BLS actual %s = %s (%s, %.0f s after the scheduled release)", ev.title, value, source, now - ev.ts)

    async def poll_once(self) -> int:
        now = self._clock.wall()
        window = max(0.0, _num_setting(self._settings, "bls_window_s", 600.0))
        api_delay = max(0.0, _num_setting(self._settings, "bls_api_delay_s", 3600.0))
        api_max_age = max(api_delay, _num_setting(self._settings, "bls_api_max_age_s", 172800.0))
        atom_need: dict[str, list[tuple[MacroEvent, str]]] = {}
        api_need: list[tuple[MacroEvent, str, str, str]] = []
        for ev in self._calendar.events():
            if ev.id in self._done:
                continue
            kind = bls_kind(ev)
            if kind is None or not self._calendar.is_time_known(ev.id):
                continue
            if ev.actual:  # already known (another source / manual)
                self._done.add(ev.id)
                continue
            age = now - ev.ts
            if age < 0:
                continue
            feed, series, mode = BLS_KINDS[kind]
            if feed and ev.id not in self._atom_done:
                if age <= window:
                    atom_need.setdefault(feed, []).append((ev, kind))
                    continue
                self._atom_done.add(ev.id)
                if ev.id in self._attempted:
                    self._record(ev, kind, None, "bls_atom", now, "нет данных в ленте BLS за окно")
                    log.info("BLS: no headline for %s within %.0f s - surprise stays unknown", ev.title, window)
            if self._api_key and series and mode:
                if age > api_max_age:
                    self._done.add(ev.id)
                    if ev.id in self._attempted:
                        self._record(ev, kind, None, "bls_api", now, "нет данных в BLS API")
                elif age >= api_delay and now >= self._api_next.get(ev.id, 0.0):
                    api_need.append((ev, kind, series, mode))
                continue
            self._done.add(ev.id)

        errors: list[str] = []
        found = 0
        for feed, items in atom_need.items():
            try:
                resp = await self._http.get(BLS_FEEDS[feed], headers=self._headers(), timeout_s=10.0)
                check_response(resp, f"BLS {feed}")
                entries = parse_bls_feed(resp.text)
            except RateLimitedSource:
                raise
            except Exception as e:  # noqa: BLE001 - reported via status, retried next poll
                errors.append(f"BLS {feed}: {type(e).__name__}: {e}" if str(e) else f"BLS {feed}: {type(e).__name__}")
                continue
            match_s = max(0.0, _num_setting(self._settings, "bls_match_s", 10800.0))
            for ev, kind in items:
                self._attempted.add(ev.id)
                cands = [e for e in entries if e["published_ts"] is not None and abs(e["published_ts"] - ev.ts) <= match_s]
                if len(cands) != 1:
                    continue  # not published yet (or ambiguous) - keep polling inside the window
                value = bls_headline(kind, cands[0]["title"], cands[0]["content"])
                if value is not None:
                    self._apply(ev, kind, value, "bls_atom", now)
                    found += 1

        retry = max(60.0, _num_setting(self._settings, "bls_api_retry_s", 3600.0))
        for ev, kind, series, mode in api_need:
            self._attempted.add(ev.id)
            self._api_next[ev.id] = now + retry
            try:
                resp = await self._http.get(BLS_API_URL.format(series=series), params={"registrationkey": self._api_key},
                                            headers=self._headers(), timeout_s=15.0)
                check_response(resp, "BLS API")
                value, state = bls_api_value(resp.json(), mode, ev.ts)
            except RateLimitedSource as e:
                for k in list(self._api_next):
                    self._api_next[k] = max(self._api_next[k], now + e.retry_after_s)
                errors.append(str(e))
                break
            except Exception as e:  # noqa: BLE001
                errors.append(f"BLS API {series}: {type(e).__name__}: {e}" if str(e) else f"BLS API {series}: error")
                continue
            if value is not None:
                self._apply(ev, kind, value, "bls_api", now)
                found += 1
            elif state in ("too_late", "bad"):
                self._done.add(ev.id)
                self._record(ev, kind, None, "bls_api", now, f"BLS API: {state}")
        if errors and not found:
            raise SourceError("; ".join(errors))
        notes = [] if self._api_key else ["без BLS_API_KEY: только Atom-ленты BLS"]
        if atom_need:
            notes.append(f"ждёт релиз: {sum(len(v) for v in atom_need.values())}")
        self.status.note = "; ".join(notes)
        return sum(1 for r in self.results if r["actual"])

    def snapshot(self) -> dict[str, Any]:
        return {
            "enabled": self.status.enabled,
            "api_key": self.has_api_key,  # presence only, never the key
            "results": list(self.results),
            "status": self.status.to_dict(),
        }


__all__ = [
    "BLS_FEEDS", "BLS_KINDS", "EBASE_TYPES", "FF_JSON_URL", "FF_XML_URL", "IMPACT_RANK", "TITLE_TYPES",
    "BlsActuals", "MacroCalendar", "bls_api_value", "bls_headline", "bls_kind", "classify_title",
    "parse_bls_feed", "parse_ff_json", "parse_ff_xml", "parse_number", "surprise_of",
]
