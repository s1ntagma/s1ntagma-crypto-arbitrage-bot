"""News feed: public Telegram channels (t.me/s/<channel>) + RSS/Atom feeds
(ARCHITECTURE P3.5 + P3.12 "News").

* Telegram: the first poll is ``GET https://t.me/s/<ch>``, then
  ``GET https://t.me/s/<ch>?after=<max_seen_id>`` (~19 KB when nothing is new);
  a ``data-after`` / ``<link rel=next>`` cursor is followed to catch up.  Fast
  squawks every 20 s, others every 60 s (per-source due times inside
  ``poll_once``; the collector itself ticks every 20 s).  A redirect / page
  without the channel history is an error, never "no posts".
* RSS 2.0 / RSS 1.0 / Atom with tolerant parsing (BOM, bare '&', HTML entities)
  and robust dates (RFC-822 incl. GMT / odd spacing, ISO-8601).  Conditional
  GET (ETag / Last-Modified) when the server supports it.
* De-duplication: sha1 of the normalised headline or token Jaccard >= 0.5 within
  ``news_dedup_window_s``; macro prints by (event type, actual, forecast) within
  5 min.  A duplicate from another channel corroborates the first story.
* Fake-news guard: ETF / hack / exchange-insolvency / Fed-decision items get
  importance > 0.85 only when corroborated (>= 2 independent channels, or the
  official SEC/Fed feed, or the exchange's own announcement channel).
* Relevance: only pool assets (``assets_fn``) or market-wide (``["*"]``) items.

Every item is a factor input, never a BUY/SELL by itself.  Items without a
publication time are skipped (no invented timestamps)."""
from __future__ import annotations

import asyncio
import collections
import html as html_lib
import json
import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.entities import name2codepoint
from html.parser import HTMLParser
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from ..logging_setup import redact
from .base import (
    HttpGetter,
    NewsItem,
    PollingCollector,
    RateLimitedSource,
    SourceError,
    SourceStatus,
    check_response,
    stable_id,
)
from .classify import (
    HeadlineClassifier,
    dedup_key,
    headline,
    jaccard,
    macro_surprise,
    parse_whale_alert,
    tokens,
)

log = logging.getLogger(__name__)

DEFAULT_TELEGRAM_CHANNELS: list[str] = [
    "WatcherGuru", "whale_alert_io", "wublockchainenglish", "cointelegraph", "WalterBloomberg", "FinancialJuice",
    "markettwits", "unfolded", "BWEnews", "binance_announcements", "okxannouncements", "Bybit_Announcements",
    "kucoin_news",
]
DEFAULT_TELEGRAM_FAST: list[str] = [
    "FinancialJuice", "WalterBloomberg", "markettwits", "WatcherGuru", "whale_alert_io", "wublockchainenglish",
]
DEFAULT_RSS: list[str] = [
    "https://cointelegraph.com/rss",
    "https://www.coindesk.com/arc/outboundfeeds/rss",
    "https://decrypt.co/feed",
    "https://www.theblock.co/rss.xml",
    "https://bitcoinmagazine.com/feed",
    "https://www.federalreserve.gov/feeds/press_all.xml",
    "https://www.federalreserve.gov/feeds/speeches.xml",
    "https://www.sec.gov/news/pressreleases.rss",
]
KNOWN_FEEDS: dict[str, str] = {
    "cointelegraph.com/rss": "cointelegraph",
    "www.coindesk.com/arc/outboundfeeds/rss": "coindesk",
    "decrypt.co/feed": "decrypt",
    "www.theblock.co/rss.xml": "theblock",
    "bitcoinmagazine.com/feed": "bitcoinmagazine",
    "www.federalreserve.gov/feeds/press_all.xml": "fed_press",
    "www.federalreserve.gov/feeds/press_monetary.xml": "fed_monetary",
    "www.federalreserve.gov/feeds/speeches.xml": "fed_speeches",
    "www.sec.gov/news/pressreleases.rss": "sec_press",
    "www.sec.gov/news/statements.rss": "sec_statements",
}

# fake-news guard (P3.12)
GUARD_CAP = 0.85
GUARDED_CATEGORIES = frozenset({"etf", "hack", "exchange", "fed_rates"})
# official sources: who may confirm which categories on their own
OFFICIAL_RSS_HOSTS: dict[str, frozenset[str]] = {
    "federalreserve.gov": frozenset({"fed_rates", "macro_data"}),
    "sec.gov": frozenset({"etf", "regulation"}),
}
OFFICIAL_TELEGRAM: dict[str, frozenset[str]] = {
    ch: frozenset({"hack", "exchange", "listing", "delisting"})
    for ch in ("binance_announcements", "okxannouncements", "bybit_announcements", "kucoin_news")
}
# one publisher = one voice: its Telegram channel and its RSS feed are not independent
PUBLISHER_ALIASES: dict[str, str] = {
    "coindeskglobal": "coindesk", "the_block_crypto": "theblock", "fed_press": "fed", "fed_monetary": "fed",
    "fed_speeches": "fed", "sec_press": "sec", "sec_statements": "sec", "walter_bloomberg": "walterbloomberg",
}
DEDUP_JACCARD = 0.5  # measured: >= 0.5 had no false match on 682 cross-source headlines
DEDUP_MIN_TOKENS = 3
MACRO_DEDUP_S = 300.0
CORROBORATION_CONF_STEP = 0.1
CORROBORATION_CONF_MAX = 0.9
TELEGRAM_HOST = "t.me"


# --------------------------------------------------------------------------- #
# dates
# --------------------------------------------------------------------------- #
_WS = re.compile(r"\s+")


def parse_date(text: str | None) -> float | None:
    """RFC-822 (RSS: "Wed, 7 Oct 2026 18:00:00 GMT", "Fri,  2 Oct 2026 08:32:11 -0400")
    or ISO-8601 (Atom / Telegram) -> epoch seconds UTC.  A missing zone is read as
    UTC.  Unparseable -> None."""
    s = _WS.sub(" ", (text or "").strip())
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
    except (TypeError, ValueError, IndexError):
        dt = None
    if dt is None:
        iso = s.replace("Z", "+00:00").replace("z", "+00:00")
        if re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}(:\d{2}(\.\d+)?)?( ?[+-]\d{2}:?\d{2})?", iso):
            iso = iso.replace(" ", "T", 1).replace(" ", "")
        try:
            dt = datetime.fromisoformat(iso)
        except ValueError:
            dt = None
    if dt is None:
        for fmt in ("%d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M %z", "%Y/%m/%d %H:%M:%S", "%d %b %Y %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        return dt.timestamp()
    except (OverflowError, OSError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# Telegram public preview
# --------------------------------------------------------------------------- #
class _TgPageParser(HTMLParser):
    """t.me/s/<channel> HTML -> posts + paging cursors (stdlib only)."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.posts: list[dict[str, Any]] = []
        self.cur: dict[str, Any] | None = None
        self.skip = False  # current message is a service message
        self.msg_depth = 0  # <div> depth inside the current message
        self.text_depth = 0  # > 0 inside the OUTERMOST message_text div
        self.text_parts: list[str] = []
        self.text_done = False
        self.in_views = False
        self.in_date = False
        self.before: int | None = None
        self.after: int | None = None
        self.is_channel = False

    @staticmethod
    def _cursor(value: str | None) -> int | None:
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        a = {k: (v or "") for k, v in attrs}
        classes = a.get("class", "").split()
        if tag == "section" and "tgme_channel_history" in classes:
            self.is_channel = True
        if tag == "link" and a.get("rel") in ("next", "prev") and a.get("href"):
            q = parse_qs(urlparse(a["href"]).query)
            if a["rel"] == "next" and q.get("after"):
                self.after = self.after or self._cursor(q["after"][0])
            if a["rel"] == "prev" and q.get("before"):
                self.before = self.before or self._cursor(q["before"][0])
        if tag == "a" and "tme_messages_more" in classes:
            if a.get("data-before"):
                self.before = self._cursor(a["data-before"]) or self.before
            if a.get("data-after"):
                self.after = self._cursor(a["data-after"]) or self.after
        if tag == "div" and "tgme_widget_message" in classes and a.get("data-post") and self.cur is None:
            post = a["data-post"]
            ch, _, pid = post.rpartition("/")
            post_id = self._cursor(pid)
            self.cur = {"id": post_id, "post": post, "channel": ch, "ts": None, "datetime": "", "text": "",
                        "url": "", "views": "", "edited": False, "forwarded_from": "", "links": []}
            self.skip = "service_message" in classes or post_id is None
            self.msg_depth = 1
            self.text_depth = 0
            self.text_done = False
            self.text_parts = []
            return
        if self.cur is None:
            return
        if tag == "div":
            self.msg_depth += 1
            if self.text_depth:
                self.text_depth += 1
            elif "tgme_widget_message_text" in classes and not self.text_done:
                self.text_depth = 1
                self.text_parts = []
        if self.text_depth:
            if tag == "br":
                self.text_parts.append("\n")
            elif tag == "a" and a.get("href"):
                self.cur["links"].append(a["href"])
        if tag == "a" and "tgme_widget_message_date" in classes:
            self.cur["url"] = a.get("href", "")
            self.in_date = True
        if tag == "time" and a.get("datetime") and (self.in_date or not self.cur["datetime"]):
            self.cur["datetime"] = a["datetime"]
        if tag == "span" and "tgme_widget_message_views" in classes:
            self.in_views = True
        if tag == "a" and "tgme_widget_message_forwarded_from_name" in classes:
            self.cur["forwarded_from"] = a.get("href", "") or "?"

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "div":  # "<div/>" never opens a scope
            return
        self.handle_starttag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        if self.cur is None:
            return
        if tag == "span":
            self.in_views = False
        elif tag == "a":
            self.in_date = False
        elif tag == "div":
            if self.text_depth:
                self.text_depth -= 1
                if self.text_depth == 0:
                    self.text_done = True
                    self.cur["text"] = _tidy("".join(self.text_parts))
            self.msg_depth -= 1
            if self.msg_depth <= 0:
                if not self.skip:
                    self.cur["ts"] = parse_date(self.cur["datetime"]) if self.cur["datetime"] else None
                    self.posts.append(self.cur)
                self.cur = None
                self.skip = False

    def handle_data(self, data: str) -> None:
        if self.cur is None:
            return
        if self.text_depth:
            self.text_parts.append(data)
        elif self.in_views:
            self.cur["views"] = data.strip()
        elif data.strip() == "edited":
            self.cur["edited"] = True


def _tidy(text: str) -> str:
    lines = [re.sub(r"[ \t ]+", " ", ln).strip() for ln in text.replace("\r", "").split("\n")]
    out = "\n".join(lines)
    return re.sub(r"\n{3,}", "\n\n", out).strip()


def telegram_page_info(html: str, channel: str = "") -> dict[str, Any]:
    """Posts plus paging cursors of a t.me/s page:
    ``{"posts": [...], "before": int|None, "after": int|None, "is_channel": bool}``.
    ``after`` is set only when newer posts exist beyond this page (catch-up)."""
    doc = html or ""
    if doc.startswith('"'):  # XHR variant: a JSON-encoded string of the HTML fragment
        try:
            doc = json.loads(doc)
        except ValueError:
            pass
    p = _TgPageParser()
    try:
        p.feed(doc)
        p.close()
    except Exception as exc:  # noqa: BLE001 - malformed HTML must not take the feed down
        log.debug("telegram page %s: parser stopped early: %s", channel, type(exc).__name__)
    posts = sorted((x for x in p.posts if x["id"] is not None), key=lambda x: x["id"])
    for x in posts:
        if channel and not x["channel"]:
            x["channel"] = channel
        if not x["url"] and x["channel"]:
            x["url"] = f"https://t.me/{x['channel']}/{x['id']}"
    return {"posts": posts, "before": p.before, "after": p.after, "is_channel": p.is_channel or bool(posts)}


def parse_telegram_page(html: str, channel: str) -> list[dict[str, Any]]:
    """Posts of a t.me/s/<channel> page, oldest first: ``id`` (int, from data-post),
    ``ts`` (UTC epoch from <time datetime>, None if absent), ``text`` (outermost
    message text, <br> -> newline), ``url``; extras ``channel``, ``post``,
    ``datetime``, ``views``, ``edited``, ``forwarded_from``, ``links``.  Service
    messages (pins...) are skipped."""
    return telegram_page_info(html, channel)["posts"]


# --------------------------------------------------------------------------- #
# RSS / Atom
# --------------------------------------------------------------------------- #
_BARE_AMP = re.compile(r"&(?!(?:[a-zA-Z][a-zA-Z0-9]*|#[0-9]+|#x[0-9a-fA-F]+);)")
_NAMED_ENTITY = re.compile(r"&([a-zA-Z][a-zA-Z0-9]*);")
_XML_ENTITIES = frozenset({"amp", "lt", "gt", "quot", "apos"})
_HTML_TAG = re.compile(r"<[^>]+>")


def _local(tag: Any) -> str:
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


def _xml_root(xml: str, feed: str) -> ET.Element:
    s = (xml or "").lstrip("﻿ \t\r\n")
    if not s:
        raise SourceError(f"{feed}: empty feed")
    attempts = [s]
    fixed = _BARE_AMP.sub("&amp;", s)
    attempts.append(fixed)
    attempts.append(_NAMED_ENTITY.sub(
        lambda m: m.group(0) if m.group(1) in _XML_ENTITIES or m.group(1) not in name2codepoint
        else f"&#{name2codepoint[m.group(1)]};", fixed))
    err: Exception | None = None
    for doc in attempts:
        try:
            return ET.fromstring(doc)
        except ET.ParseError as e:
            err = e
    raise SourceError(f"{feed}: malformed XML ({err})")


def _child(el: ET.Element, *names: str) -> ET.Element | None:
    for name in names:
        for c in el:
            if _local(c.tag) == name:
                return c
    return None


def _text_of(el: ET.Element | None) -> str:
    if el is None:
        return ""
    return "".join(el.itertext()).strip()


def strip_html(text: str) -> str:
    t = _HTML_TAG.sub(" ", text or "")
    t = html_lib.unescape(t)
    return _WS.sub(" ", t).strip()


def _entry_link(el: ET.Element) -> str:
    best = ""
    for c in el:
        if _local(c.tag) != "link":
            continue
        href = c.attrib.get("href")
        if href:
            if c.attrib.get("rel", "alternate") == "alternate":
                return href.strip()
            best = best or href.strip()
        elif (c.text or "").strip():
            return (c.text or "").strip()
    return best


def parse_rss(xml: str, feed: str) -> list[dict[str, Any]]:
    """RSS 2.0 / RSS 1.0 / Atom -> ``[{"id", "ts", "title", "text", "url", "feed"}]``
    in document order.  ``ts`` is None when the item has no parseable date.
    Raises ``SourceError`` when the document is not XML even after repair."""
    root = _xml_root(xml, feed)
    out: list[dict[str, Any]] = []
    for el in root.iter():
        if _local(el.tag) not in ("item", "entry"):
            continue
        title = strip_html(_text_of(_child(el, "title")))
        url = _entry_link(el)
        native = _text_of(_child(el, "guid", "id"))
        date = _text_of(_child(el, "pubDate", "published", "date", "issued", "updated", "modified"))
        desc = _text_of(_child(el, "description", "summary", "encoded", "content"))
        text = strip_html(desc)
        if not title and not text:
            continue
        out.append({
            "id": native or url or stable_id(feed, title),
            "ts": parse_date(date),
            "title": title,
            "text": text[:1500],
            "url": url,
            "feed": feed,
        })
    return out


def feed_name(url: str) -> str:
    """Short stable name of an RSS feed URL ("coindesk", "fed_press", ...)."""
    p = urlparse(url.strip())
    key = (p.netloc + p.path).rstrip("/")
    if key in KNOWN_FEEDS:
        return KNOWN_FEEDS[key]
    host = p.netloc.lower().removeprefix("www.")
    base = host.split(".")[0] if host else "feed"
    stem = re.sub(r"\.(xml|rss|html?)$", "", p.path.rstrip("/").rsplit("/", 1)[-1].lower())
    stem = re.sub(r"[^a-z0-9]+", "_", stem).strip("_")
    return f"{base}_{stem}" if stem and stem not in ("rss", "feed", "") else base


# --------------------------------------------------------------------------- #
# feed collector
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class _Source:
    key: str  # "tg:<channel>" | "rss:<name>"
    kind: str  # "telegram" | "rss"
    name: str  # channel handle / feed name
    url: str
    interval_s: float
    status: SourceStatus
    official: frozenset[str] = frozenset()
    next_due_mono: float = 0.0
    failures: int = 0
    max_id: int | None = None  # telegram
    etag: str = ""  # rss
    last_modified: str = ""
    seen: collections.OrderedDict[str, None] = field(default_factory=collections.OrderedDict)
    stored: int = 0


def _norm_channel(ch: str) -> str:
    c = str(ch or "").strip()
    c = re.sub(r"^(?:https?://)?(?:t\.me|telegram\.me)/(?:s/)?", "", c, flags=re.I)
    return c.strip("@/ ").split("/")[0].split("?")[0]


def publisher_of(source_key: str) -> str:
    """Corroboration identity of a source key ("tg:cointelegraph" and "rss:cointelegraph" -> "cointelegraph")."""
    name = source_key.split(":", 1)[-1].lower()
    return PUBLISHER_ALIASES.get(name, name)


def _host_of(url: str) -> str:
    return urlparse(url).netloc.lower().removeprefix("www.")


class NewsFeed(PollingCollector):
    """RSS + public Telegram channels -> classified, de-duplicated ``NewsItem``s."""

    name = "news"

    def __init__(self, settings: Any, clock: Any, http: HttpGetter | None, *,
                 classifier: HeadlineClassifier | None = None, db: Any = None,
                 assets_fn: Callable[[], list[str]] = lambda: []) -> None:
        fast_s = float(getattr(settings, "news_fast_poll_s", 20.0))
        slow_s = float(getattr(settings, "news_poll_s", 60.0))
        enabled = bool(getattr(settings, "news_enabled", True))
        super().__init__(settings, clock, http, interval_s=min(fast_s, slow_s), enabled=enabled)
        self._classifier: Any = classifier if classifier is not None else HeadlineClassifier(settings, clock)
        self._db = db
        self._assets_fn = assets_fn
        self._items: list[NewsItem] = []  # newest first
        self._by_id: dict[str, NewsItem] = {}
        self._tokens: dict[str, frozenset[str]] = {}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._refine_sem = asyncio.Semaphore(2)
        self._restored = False
        self._tg_hold_until_mono = 0.0  # t.me rate limit (per client, not per channel)
        self._dirty_order = False
        self.skipped_no_ts = 0
        self.duplicates = 0
        self._src: dict[str, _Source] = {}
        self.sources: dict[str, SourceStatus] = {}

        fast = {_norm_channel(c).lower() for c in getattr(settings, "news_telegram_fast", DEFAULT_TELEGRAM_FAST) or []}
        for raw in getattr(settings, "news_telegram_channels", DEFAULT_TELEGRAM_CHANNELS) or []:
            ch = _norm_channel(raw)
            if not ch:
                continue
            key = f"tg:{ch.lower()}"
            if key in self._src:
                continue
            is_fast = ch.lower() in fast
            interval = fast_s if is_fast else slow_s
            st = SourceStatus(name=key, enabled=enabled, note=f"telegram, every {interval:.0f} s"
                              + (" (fast squawk)" if is_fast else ""))
            off = OFFICIAL_TELEGRAM.get(ch.lower(), frozenset())
            if off:
                st.note += ", official exchange channel"
            self._src[key] = _Source(key, "telegram", ch, f"https://{TELEGRAM_HOST}/s/{ch}", interval, st, off)
        for raw in getattr(settings, "news_rss", DEFAULT_RSS) or []:
            if isinstance(raw, dict):
                url, nm = str(raw.get("url", "")).strip(), str(raw.get("name", "")).strip()
            else:
                url, nm = str(raw).strip(), ""
            if not url:
                continue
            nm = nm or feed_name(url)
            key = f"rss:{nm.lower()}"
            if key in self._src:
                continue
            host = _host_of(url)
            off = next((cats for h, cats in OFFICIAL_RSS_HOSTS.items() if host == h or host.endswith("." + h)),
                       frozenset())
            st = SourceStatus(name=key, enabled=enabled, note=f"rss, every {slow_s:.0f} s" + (", official" if off else ""))
            self._src[key] = _Source(key, "rss", nm, url, slow_s, st, off)
        self.sources = {k: s.status for k, s in self._src.items()}
        if enabled:
            n_tg = sum(1 for x in self._src.values() if x.kind == "telegram")
            self.status.note = f"{n_tg} telegram channels, {len(self._src) - n_tg} rss feeds"

    # ------------------------------------------------------------------ polling
    async def poll_once(self) -> int:
        if self._http is None:
            raise SourceError("news: no HTTP client")
        if not self._restored:
            self._restore()
        now = self._clock.mono()
        due = [s for s in self._src.values() if s.status.enabled and now >= s.next_due_mono]
        tg = sorted((s for s in due if s.kind == "telegram"), key=lambda s: s.next_due_mono)
        if now < self._tg_hold_until_mono:
            tg = []
        tg = tg[: max(1, int(getattr(self._settings, "news_tg_max_per_poll", 13)))]
        rss = [s for s in due if s.kind == "rss"]
        if not tg and not rss:
            return len(self._items)
        results = await asyncio.gather(self._poll_telegram_group(tg), self._poll_rss_group(rss))
        oks = [ok for group in results for ok in group]
        if self._dirty_order:
            self._items.sort(key=lambda it: it.ts, reverse=True)
            self._dirty_order = False
        self._prune()
        if oks and not any(oks):
            errors = "; ".join(f"{s.key}: {s.status.last_error}" for s in (tg + rss) if s.status.last_error)
            raise SourceError(f"all {len(oks)} news sources failed: {errors}"[:400])
        return len(self._items)

    async def _poll_telegram_group(self, sources: list[_Source]) -> list[bool]:
        out = []
        for s in sources:  # sequential: keep t.me traffic low (~1 request at a time)
            if self._clock.mono() < self._tg_hold_until_mono:
                break  # rate limited during this poll: the rest waits too
            out.append(await self._guarded(s, self._poll_telegram))
        return out

    async def _poll_rss_group(self, sources: list[_Source]) -> list[bool]:
        if not sources:
            return []
        return list(await asyncio.gather(*(self._guarded(s, self._poll_rss) for s in sources)))

    async def _guarded(self, s: _Source, fn: Callable[[_Source], Any]) -> bool:
        now = self._clock.mono()
        s.status.last_attempt_ts = self._clock.wall()
        try:
            await fn(s)
        except asyncio.CancelledError:
            raise
        except RateLimitedSource as e:
            self._source_failed(s, str(e), now, wait=e.retry_after_s)
            if s.kind == "telegram":  # t.me limits per client, not per channel
                self._tg_hold_until_mono = max(self._tg_hold_until_mono, now + e.retry_after_s)
            return False
        except Exception as e:  # noqa: BLE001 - one source never takes the feed down
            msg = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
            self._source_failed(s, msg, now)
            return False
        s.failures = 0
        s.status.ok = True
        s.status.last_ok_ts = self._clock.wall()
        s.status.last_error = ""
        s.status.items = s.stored
        s.next_due_mono = now + s.interval_s
        return True

    def _source_failed(self, s: _Source, message: str, now: float, *, wait: float | None = None) -> None:
        s.failures += 1
        backoff = min(self.max_backoff_s, s.interval_s * (2 ** min(s.failures - 1, 6)))
        if wait is not None:
            backoff = max(backoff, wait)
        s.status.ok = False
        s.status.last_error = redact(message)[:240]
        s.next_due_mono = now + backoff
        level = logging.WARNING if s.failures in (1, 5, 20) else logging.DEBUG
        log.log(level, "news source %s failed (%d in a row, next try in %.0f s): %s",
                s.key, s.failures, backoff, s.status.last_error)

    async def _poll_telegram(self, s: _Source) -> None:
        assert self._http is not None
        pages = 0
        max_pages = 1 + max(0, int(getattr(self._settings, "news_tg_catchup_pages", 3)))
        cursor = s.max_id
        while pages < max_pages:
            params = {"after": cursor} if cursor is not None else None
            resp = await self._http.get(s.url, params=params, timeout_s=10.0)
            check_response(resp, f"t.me/s/{s.name}")
            if resp.status != 200:
                raise SourceError(f"t.me/s/{s.name}: not a public channel (HTTP {resp.status})")
            info = telegram_page_info(resp.text, s.name)
            if not info["is_channel"]:
                raise SourceError(f"t.me/s/{s.name}: not a public channel preview")
            pages += 1
            start_max = s.max_id
            for post in info["posts"]:
                pid = post["id"]
                if start_max is not None and pid <= start_max:
                    continue
                if str(pid) in s.seen:
                    continue
                self._remember(s, str(pid))
                await self._ingest_telegram(s, post)
            ids = [p["id"] for p in info["posts"]]
            if ids:
                s.max_id = max(ids + ([s.max_id] if s.max_id is not None else []))
            nxt = info["after"]
            if cursor is None or nxt is None or s.max_id is None or nxt < s.max_id or nxt == cursor:
                break  # first look at the head, or nothing newer beyond this page
            cursor = nxt
        s.status.note = re.sub(r"(, last id \d+)?$", f", last id {s.max_id}" if s.max_id is not None else "",
                               s.status.note, count=1)

    async def _poll_rss(self, s: _Source) -> None:
        assert self._http is not None
        headers: dict[str, str] = {}
        if s.etag:
            headers["If-None-Match"] = s.etag
        if s.last_modified:
            headers["If-Modified-Since"] = s.last_modified
        resp = await self._http.get(s.url, headers=headers or None, timeout_s=15.0)
        if resp.status == 304:
            return
        check_response(resp, f"rss {s.name}")
        if resp.status != 200:
            raise SourceError(f"rss {s.name}: HTTP {resp.status}")
        entries = parse_rss(resp.text, s.name)
        s.etag = resp.headers.get("etag", "") or s.etag
        s.last_modified = resp.headers.get("last-modified", "") or s.last_modified
        for e in sorted(entries, key=lambda x: x["ts"] or 0.0):
            nid = str(e["id"])
            if nid in s.seen:
                continue
            self._remember(s, nid)
            await self._ingest(s, native_id=nid, ts=e["ts"], title=e["title"], text=e["text"], url=e["url"])

    @staticmethod
    def _remember(s: _Source, native_id: str) -> None:
        s.seen[native_id] = None
        while len(s.seen) > 2000:
            s.seen.popitem(last=False)

    async def _ingest_telegram(self, s: _Source, post: dict[str, Any]) -> None:
        extra: dict[str, Any] = {"post_id": post["id"]}
        if post.get("edited"):
            extra["edited"] = True
        if post.get("views"):
            extra["views"] = post["views"]
        fwd = post.get("forwarded_from") or ""
        if fwd:
            extra["forwarded_from"] = fwd
        await self._ingest(s, native_id=str(post["id"]), ts=post.get("ts"), title="", text=post.get("text", ""),
                           url=post.get("url", ""), extra=extra)

    # ------------------------------------------------------------------ ingest
    def _origin_key(self, s: _Source, extra: dict[str, Any] | None) -> str:
        """Corroboration key: a forwarded post counts for the ORIGINAL channel."""
        fwd = (extra or {}).get("forwarded_from") or ""
        if fwd and fwd != "?":
            ch = _norm_channel(fwd)
            if ch:
                return f"tg:{ch.lower()}"
        return s.key

    async def _ingest(self, s: _Source, *, native_id: str, ts: float | None, title: str, text: str, url: str,
                      extra: dict[str, Any] | None = None) -> NewsItem | None:
        head = headline(title or text)
        if not head:
            return None
        if ts is None:
            self.skipped_no_ts += 1
            log.debug("news %s: item %s has no publication time - skipped", s.key, native_id)
            return None
        item_id = stable_id(s.kind, s.name.lower(), native_id)
        if item_id in self._by_id:
            return None
        origin = self._origin_key(s, extra)
        dkey = dedup_key(head)
        toks = tokens(head)
        macro = macro_surprise(f"{head}\n{text}") if re.search(r"\d", head) else None
        if macro is not None and macro["event_type"] == "OTHER":
            macro = None  # not a recognised data release: no event key, no surprise
        dup = self._find_duplicate(dkey, toks, float(ts), macro, origin)
        if dup is not None:
            self._corroborate(dup, origin, float(ts))
            return None
        body = text.strip()
        item = NewsItem(
            id=item_id, ts=float(ts), source=s.kind, channel=s.name, title=head[:300],
            text=body[:2000] if body != head else "", url=url, dedup_key=dkey,
        )
        item.data["sources"] = [origin]
        item.data["native_id"] = native_id
        if extra:
            item.data.update(extra)
        if macro is not None:
            item.data["macro"] = macro
        pool = self._pool()
        await self._classify(item, pool)
        if "whale" not in item.data and s.name.lower().startswith("whale_alert"):
            w = parse_whale_alert(item.text or item.title)
            if w:
                item.data["whale"] = w
        self._finalize(item)
        self._store(item, toks)
        s.stored += 1
        self._save(item)
        self._maybe_refine(item, pool)
        return item

    def _pool(self) -> list[str]:
        try:
            return [str(a).upper() for a in (self._assets_fn() or [])]
        except Exception as exc:  # noqa: BLE001
            log.debug("news: assets_fn failed: %s", type(exc).__name__)
            return []

    async def _classify(self, item: NewsItem, pool: list[str]) -> None:
        c = self._classifier
        try:
            if hasattr(c, "apply_rules"):
                c.apply_rules(item, pool)
            else:
                await c.classify(item, pool)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - an unclassified item stays neutral
            log.warning("news: classification failed for %s: %s", item.id, type(exc).__name__)
            item.category, item.direction, item.importance, item.confidence = "other", 0, 0.0, 0.0
            item.classifier, item.reason = "none", "классификация не удалась"
        item.data.setdefault("classified", {"importance": item.importance, "confidence": item.confidence})

    def _maybe_refine(self, item: NewsItem, pool: list[str]) -> None:
        c = self._classifier
        if not (hasattr(c, "wants_claude") and hasattr(c, "refine")):
            return
        try:
            wanted = c.wants_claude(item, self._clock.wall())
        except Exception:  # noqa: BLE001
            wanted = False
        if not wanted:
            return
        task = asyncio.create_task(self._refine(item, pool))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _refine(self, item: NewsItem, pool: list[str]) -> None:
        async with self._refine_sem:
            if not self._classifier.wants_claude(item, self._clock.wall()):
                return
            try:
                changed = await self._classifier.refine(item, pool)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("news: refine failed for %s: %s", item.id, type(exc).__name__)
                return
        if changed and item.id in self._by_id:
            self._finalize(item)
            self._save(item)

    async def drain(self, timeout_s: float | None = None) -> None:
        """Wait for pending Claude refinements (tests / shutdown)."""
        pending = [t for t in self._tasks if not t.done()]
        if pending:
            await asyncio.wait(pending, timeout=timeout_s)

    # ------------------------------------------------------------------ dedup / guard
    def _find_duplicate(self, dkey: str, toks: frozenset[str], ts: float, macro: dict[str, Any] | None,
                        origin: str = "") -> NewsItem | None:
        """Same story within ``news_dedup_window_s``: identical normalised headline;
        the same macro print (event type, actual, forecast) within 5 min; or token
        Jaccard >= 0.5 across DIFFERENT channels when the headlines' numbers do not
        contradict each other (squawk templates like "CONTINUED / INITIAL JOBLESS
        CLAIMS ACTUAL ..." share most words but are different prints)."""
        window = float(getattr(self._settings, "news_dedup_window_s", 21600.0))
        nums = _numbers(toks)
        for it in self._items:
            if abs(it.ts - ts) > window:
                continue
            if it.dedup_key == dkey:
                return it
            other_macro = it.data.get("macro")
            if macro and other_macro:
                if other_macro.get("event_type") == macro["event_type"] \
                        and abs(it.ts - ts) <= MACRO_DEDUP_S \
                        and _same_number(other_macro.get("actual"), macro["actual"]) \
                        and _same_number(other_macro.get("forecast"), macro["forecast"]):
                    return it
                continue  # two different prints never merge on wording
            if len(toks) < DEDUP_MIN_TOKENS or origin in (it.data.get("sources") or []):
                continue
            ot = self._tokens.get(it.id)
            if not ot or len(ot) < DEDUP_MIN_TOKENS:
                continue
            onums = _numbers(ot)
            if nums and onums and not (nums & onums):
                continue  # different figures -> different stories
            if jaccard(toks, ot) >= DEDUP_JACCARD:
                return it
        return None

    def _corroborate(self, item: NewsItem, origin: str, ts: float) -> None:
        self.duplicates += 1
        srcs: list[str] = item.data.setdefault("sources", [])
        changed = False
        if origin not in srcs:
            srcs.append(origin)
            changed = True
        if ts < item.ts:  # the story time is the earliest report
            item.ts = ts
            self._dirty_order = True
            changed = True
        if changed:
            self._finalize(item)
            self._save(item)

    def _is_official(self, source_key: str, category: str) -> bool:
        s = self._src.get(source_key)
        if s is not None:
            return category in s.official
        if source_key.startswith("tg:"):
            return category in OFFICIAL_TELEGRAM.get(source_key[3:], frozenset())
        return False

    def _finalize(self, item: NewsItem) -> None:
        """Corroboration bonus on confidence and the fake-news guard on importance,
        both from the classified values (re-applied after every change)."""
        cls = item.data.get("classified") or {"importance": item.importance, "confidence": item.confidence}
        claimed = float(cls.get("importance", item.importance))
        conf = float(cls.get("confidence", item.confidence))
        srcs = list(dict.fromkeys(item.data.get("sources") or []))
        n = len({publisher_of(k) for k in srcs})  # independent publishers
        official = any(self._is_official(k, item.category) for k in srcs)
        corroborated = official or n >= 2
        item.data["n_sources"] = n
        item.data["official"] = official
        item.data["corroborated"] = corroborated
        if n >= 2:
            conf = min(CORROBORATION_CONF_MAX, max(conf, conf + CORROBORATION_CONF_STEP * (n - 1)))
        if item.direction == 0:
            conf = min(conf, 0.5)
        item.confidence = round(conf, 3)
        if item.category in GUARDED_CATEGORIES and claimed > GUARD_CAP and not corroborated:
            item.importance = GUARD_CAP
            item.data["importance_claimed"] = claimed
            item.data["guard"] = (f"не подтверждено: важность ограничена {GUARD_CAP:.2f} до второго независимого "
                                  f"источника или официального канала")
        else:
            item.importance = round(max(0.0, min(1.0, claimed)), 3)
            item.data.pop("importance_claimed", None)
            item.data.pop("guard", None)

    # ------------------------------------------------------------------ storage
    def _store(self, item: NewsItem, toks: frozenset[str] | None = None) -> None:
        self._by_id[item.id] = item
        self._tokens[item.id] = toks if toks is not None else tokens(item.title)
        if self._items and item.ts < self._items[0].ts:
            self._dirty_order = True
        self._items.insert(0, item)

    def _prune(self) -> None:
        keep = max(1, int(getattr(self._settings, "news_keep", 500)))
        if len(self._items) <= keep:
            return
        for it in self._items[keep:]:
            self._by_id.pop(it.id, None)
            self._tokens.pop(it.id, None)
        del self._items[keep:]

    def _save(self, item: NewsItem) -> None:
        save = getattr(self._db, "save_news", None) if self._db is not None else None
        if save is None:
            return
        try:
            save(item.to_dict())
        except Exception as exc:  # noqa: BLE001 - persistence must not stop the feed
            log.warning("news: save_news failed: %s", type(exc).__name__)

    def _restore(self) -> None:
        self._restored = True
        load = getattr(self._db, "list_news", None) if self._db is not None else None
        if load is None:
            return
        window = float(getattr(self._settings, "news_dedup_window_s", 21600.0))
        keep = max(1, int(getattr(self._settings, "news_keep", 500)))
        try:
            rows = load(since_ts=self._clock.wall() - window, limit=keep)
        except Exception as exc:  # noqa: BLE001
            log.warning("news: list_news failed: %s", type(exc).__name__)
            return
        n = 0
        for row in rows or []:
            item = _item_from_row(row)
            if item is None or item.id in self._by_id:
                continue
            self._by_id[item.id] = item
            self._tokens[item.id] = tokens(item.title)
            self._items.append(item)
            n += 1
        self._items.sort(key=lambda it: it.ts, reverse=True)
        if n:
            log.info("news: restored %d items from the database", n)

    # ------------------------------------------------------------------ queries
    def items(self, since_wall: float | None = None, limit: int = 100) -> list[NewsItem]:
        """Newest first."""
        out: list[NewsItem] = []
        n = max(0, int(limit))
        for it in self._items:
            if len(out) >= n or (since_wall is not None and it.ts < since_wall):
                break
            out.append(it)
        return out

    def is_pool_relevant(self, item: NewsItem, pool: set[str] | None = None) -> bool:
        assets = {a.upper() for a in item.assets}
        if "*" in assets:
            return True
        pool = {p.upper() for p in self._pool()} if pool is None else pool
        if not pool:
            return bool(assets)
        return bool(assets & pool)

    def relevant(self, asset: str, now_wall: float, within_s: float, min_importance: float = 0.5) -> list[NewsItem]:
        """Items about ``asset`` (or the whole market, ``"*"``) published in
        ``[now - within_s, now]`` with importance >= ``min_importance``, newest first.
        A non-pool asset only gets market-wide items."""
        a = (asset or "").upper()
        pool = {p.upper() for p in self._pool()}
        in_pool = not pool or a in pool
        lo = now_wall - max(0.0, float(within_s))
        out: list[NewsItem] = []
        for it in self._items:
            if it.ts > now_wall + 60.0:
                continue
            if it.ts < lo:
                break
            if it.importance < min_importance:
                continue
            assets = {x.upper() for x in it.assets}
            if "*" in assets or (in_pool and a in assets):
                out.append(it)
        return out

    def whale_flows(self, asset: str, now_wall: float, within_s: float) -> list[dict[str, Any]]:
        """Parsed Whale Alert transfers of ``asset`` (``"*"`` = all) in the window, newest first."""
        a = (asset or "").upper()
        lo = now_wall - max(0.0, float(within_s))
        out: list[dict[str, Any]] = []
        for it in self._items:
            if it.ts > now_wall + 60.0:
                continue
            if it.ts < lo:
                break
            w = it.data.get("whale")
            if not w or (a != "*" and str(w.get("asset", "")).upper() != a):
                continue
            out.append({**w, "ts": it.ts, "id": it.id, "channel": it.channel})
        return out

    def macro_surprises(self, now_wall: float, within_s: float) -> list[dict[str, Any]]:
        """Actual-vs-forecast squawks in the window, newest first (surprise sign source for the calendar)."""
        lo = now_wall - max(0.0, float(within_s))
        out: list[dict[str, Any]] = []
        for it in self._items:
            if it.ts > now_wall + 60.0:
                continue
            if it.ts < lo:
                break
            m = it.data.get("macro")
            if m and m.get("event_type", "OTHER") != "OTHER":
                out.append({**m, "ts": it.ts, "id": it.id, "channel": it.channel,
                            "sources": list(it.data.get("sources") or [])})
        return out

    def snapshot(self) -> dict[str, Any]:
        pool = {p.upper() for p in self._pool()}
        items = []
        for it in self._items[:60]:
            d = it.to_dict()
            d["pool_relevant"] = self.is_pool_relevant(it, pool)
            items.append(d)
        by_cat: dict[str, int] = {}
        for it in self._items:
            by_cat[it.category] = by_cat.get(it.category, 0) + 1
        statuses = [s.to_dict() for s in self.sources.values()]
        snap = {
            "enabled": self.status.enabled,
            "status": self.status.to_dict(),
            "sources": statuses,
            "sources_ok": sum(1 for s in self.sources.values() if s.ok),
            "sources_total": len(self.sources),
            "count": len(self._items),
            "duplicates": self.duplicates,
            "skipped_no_ts": self.skipped_no_ts,
            "by_category": by_cat,
            "items": items,
        }
        if hasattr(self._classifier, "snapshot"):
            try:
                snap["classifier"] = self._classifier.snapshot()
            except Exception:  # noqa: BLE001
                pass
        return snap


def _numbers(toks: frozenset[str]) -> frozenset[str]:
    return frozenset(t for t in toks if any(c.isdigit() for c in t))


def _same_number(a: Any, b: Any) -> bool:
    try:
        x, y = float(a), float(b)
    except (TypeError, ValueError):
        return False
    return abs(x - y) <= 1e-6 * max(1.0, abs(x), abs(y))


def _item_from_row(row: Any) -> NewsItem | None:
    """A ``db.list_news`` row (dict) -> NewsItem; unknown keys ignored."""
    if not isinstance(row, dict) or not row.get("id") or row.get("ts") is None:
        return None

    def _json(v: Any, default: Any) -> Any:
        if isinstance(v, str):
            try:
                return json.loads(v)
            except ValueError:
                return default
        return v if v is not None else default

    try:
        item = NewsItem(
            id=str(row["id"]), ts=float(row["ts"]), source=str(row.get("source") or ""),
            channel=str(row.get("channel") or ""), title=str(row.get("title") or ""),
            text=str(row.get("text") or ""), url=str(row.get("url") or ""),
            assets=[str(a) for a in _json(row.get("assets"), []) or []],
            category=str(row.get("category") or "other"), importance=float(row.get("importance") or 0.0),
            direction=int(row.get("direction") or 0), speed=str(row.get("speed") or "unknown"),
            confidence=float(row.get("confidence") or 0.0), classifier=str(row.get("classifier") or "none"),
            reason=str(row.get("reason") or ""), dedup_key=str(row.get("dedup_key") or ""),
            data=dict(_json(row.get("data"), {}) or {}),
        )
    except (TypeError, ValueError):
        return None
    if not item.dedup_key and item.title:
        item.dedup_key = dedup_key(item.title)
    return item


__all__ = [
    "DEFAULT_RSS", "DEFAULT_TELEGRAM_CHANNELS", "DEFAULT_TELEGRAM_FAST", "GUARD_CAP", "NewsFeed", "feed_name",
    "macro_surprise", "parse_date", "parse_rss", "parse_telegram_page", "parse_whale_alert", "publisher_of",
    "strip_html", "telegram_page_info",
]
