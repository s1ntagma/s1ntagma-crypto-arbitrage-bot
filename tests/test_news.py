"""context/news.py: Telegram preview + RSS parsing, polling, dedup, corroboration guard, relevance."""
from __future__ import annotations

import html
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from latbot.clock import SimClock
from latbot.context.base import HttpResponse, SourceError
from latbot.context.classify import HeadlineClassifier
from latbot.context.news import (
    GUARD_CAP,
    NewsFeed,
    feed_name,
    macro_surprise,
    parse_date,
    parse_rss,
    parse_telegram_page,
    parse_whale_alert,
    telegram_page_info,
)

FIX = Path(__file__).parent / "fixtures" / "context" / "news"
NOW = datetime(2026, 10, 8, 12, 45, tzinfo=timezone.utc).timestamp()


def fx(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


def utc(*a: int) -> float:
    return datetime(*a, tzinfo=timezone.utc).timestamp()


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def tg_page(channel: str, posts: list[tuple[int, float, str]], *, after: int | None = None) -> str:
    msgs = []
    for pid, ts, text in posts:
        body = html.escape(text).replace("\n", "<br/>")
        msgs.append(
            '<div class="tgme_widget_message_wrap js-widget_message_wrap"><div class="tgme_widget_message '
            f'text_not_supported_wrap js-widget_message" data-post="{channel}/{pid}"><div class="tgme_widget_message_bubble">'
            f'<div class="tgme_widget_message_text js-message_text" dir="auto">{body}</div>'
            '<div class="tgme_widget_message_footer"><div class="tgme_widget_message_info">'
            '<span class="tgme_widget_message_views">1K</span><span class="tgme_widget_message_meta">'
            f'<a class="tgme_widget_message_date" href="https://t.me/{channel}/{pid}"><time datetime="{iso(ts)}" '
            'class="time">x</time></a></span></div></div></div></div></div>')
    more = (f'<div class="tgme_widget_message_centered js-messages_more_wrap"><a href="/s/{channel}?after={after}" '
            f'class="tme_messages_more js-messages_more" data-after="{after}"></a></div>') if after else ""
    return ('<!DOCTYPE html><html><head></head><body><main><section class="tgme_channel_history js-message_history">'
            + "".join(msgs) + more + "</section></main></body></html>")


def rss_doc(items: list[tuple[str, str, str | None]]) -> str:
    parts = []
    for guid, title, date in items:
        d = f"<pubDate>{date}</pubDate>" if date else ""
        parts.append(f"<item><title>{html.escape(title)}</title><link>https://ex.com/{guid}</link>"
                     f"<guid>{guid}</guid>{d}<description>&lt;p&gt;{html.escape(title)} body&lt;/p&gt;</description></item>")
    return '<?xml version="1.0" encoding="utf-8"?><rss version="2.0"><channel><title>x</title>' + "".join(parts) + \
        "</channel></rss>"


class FakeHttp:
    """Routes: url -> HttpResponse | Exception | callable(params, headers) -> HttpResponse."""

    def __init__(self, routes: dict | None = None) -> None:
        self.routes = dict(routes or {})
        self.calls: list[tuple[str, dict, dict]] = []

    async def get(self, url, *, params=None, headers=None, timeout_s=10.0):
        self.calls.append((url, dict(params or {}), dict(headers or {})))
        r = self.routes.get(url)
        if r is None:
            return HttpResponse(404, "not found")
        if callable(r):
            r = r(dict(params or {}), dict(headers or {}))
        if isinstance(r, BaseException):
            raise r
        return r

    async def close(self) -> None:
        pass

    def urls(self) -> list[str]:
        return [c[0] for c in self.calls]


def ok(text: str, **headers: str) -> HttpResponse:
    return HttpResponse(200, text, {k.replace("_", "-"): v for k, v in headers.items()})


def settings(**kw):
    base = dict(news_enabled=True, news_telegram_channels=[], news_telegram_fast=[], news_rss=[], news_poll_s=60.0,
                news_fast_poll_s=20.0, news_keep=500, news_dedup_window_s=21600.0, news_claude_enabled=True,
                news_claude_max_per_hour=30, news_claude_min_importance=0.4, news_claude_max_age_s=1800.0,
                news_claude_timeout_s=5.0, news_tg_catchup_pages=3, analysis_model="claude-opus-5-5")
    base.update(kw)
    return SimpleNamespace(**base)


def make_feed(st, http, *, clock=None, pool=("BTC", "ETH", "SOL", "XRP"), db=None, classifier=None):
    clock = clock or SimClock(wall=NOW, mono=1000.0)
    return NewsFeed(st, clock, http, classifier=classifier or HeadlineClassifier(st, clock), db=db,
                    assets_fn=lambda: list(pool)), clock


# --------------------------------------------------------------------------- #
# Telegram parsing
# --------------------------------------------------------------------------- #
def test_parse_telegram_page_album_br_time_views():
    posts = parse_telegram_page(fx("WatcherGuru.html"), "WatcherGuru")
    assert len(posts) == 17
    assert [p["id"] for p in posts] == sorted(p["id"] for p in posts)
    first = posts[0]
    assert first["id"] == 15380 and first["ts"] == utc(2026, 10, 7, 8, 49, 10)
    assert first["url"] == "https://t.me/WatcherGuru/15380" and first["views"] == "72.8K"
    assert first["text"].startswith("JUST IN: 🇷🇺 Russia's largest bank Sberbank")
    album = next(p for p in posts if p["id"] == 15381)  # text div nested twice: taken once
    assert album["text"].count("Robinhood") == 1
    assert album["text"] == ("JUST IN: Robinhood $HOOD announces it added $25,000,000 worth of Bitcoin to its balance "
                             "sheet.\n\n@WatcherGuru")
    assert all(p["ts"] is not None for p in posts)


def test_parse_telegram_page_skips_service_messages():
    ids = [p["id"] for p in parse_telegram_page(fx("cointelegraph.html"), "cointelegraph")]
    assert 72555 not in ids and ids == [72552, 72554, 72556]


def test_parse_telegram_forwarded_and_case_insensitive_post_ids():
    posts = parse_telegram_page(fx("forklog.html"), "forklog")
    fwd = posts[0]
    assert fwd["forwarded_from"] == "https://t.me/forklogAI/7702"
    assert any(p["text"] == "" for p in posts)  # media-only post: no text, still an id
    okx = parse_telegram_page(tg_page("OKXAnnouncements", [(5, NOW, "OKX will delist ABC")]), "okxannouncements")
    assert okx[0]["id"] == 5 and okx[0]["channel"] == "OKXAnnouncements"


def test_telegram_cursors_and_empty_and_non_channel_pages():
    older = telegram_page_info(fx("WatcherGuru_before15380.html"), "WatcherGuru")
    assert older["after"] == 15379 and older["before"] == 15360
    head = telegram_page_info(fx("WatcherGuru.html"), "WatcherGuru")
    assert head["after"] is None and head["before"] == 15380 and head["is_channel"]
    after = telegram_page_info(fx("WatcherGuru_after15390.html"), "WatcherGuru")
    assert after["after"] is None and min(p["id"] for p in after["posts"]) == 15390
    empty = telegram_page_info(fx("whale_alert_io_after_head_empty.html"), "whale_alert_io")
    assert empty["posts"] == [] and empty["is_channel"] and empty["after"] is None
    redirected = telegram_page_info('<html><body><div class="tgme_page"><div class="tgme_page_title">X</div></div>'
                                    "</body></html>", "x")
    assert redirected["posts"] == [] and not redirected["is_channel"]


# --------------------------------------------------------------------------- #
# RSS
# --------------------------------------------------------------------------- #
def test_parse_rss_fed_gmt_dates_and_cdata():
    items = parse_rss(fx("fed_press_all.xml"), "fed_press")
    assert len(items) == 6
    assert items[0]["ts"] == utc(2026, 10, 7, 18, 0, 0)
    assert items[0]["title"].startswith("Minutes of the Federal Open Market Committee")
    assert items[0]["url"].startswith("https://www.federalreserve.gov/newsevents/pressreleases/")
    assert items[0]["id"] == items[0]["url"]  # guid


def test_parse_rss_odd_spacing_malformed_and_atom():
    bls = parse_rss(fx("bls_latest.xml"), "bls_latest")
    assert bls[0]["ts"] == utc(2026, 10, 2, 12, 32, 11)  # "Fri,  2 Oct 2026 08:32:11 -0400"
    assert "Consumer Price Index" in bls[0]["text"] and "<" not in bls[0]["text"]
    ecb = parse_rss(fx("ecb_pub.xml"), "ecb_pub")  # bare '&' (R&D): strict XML fails, tolerant parse works
    assert len(ecb) == 3 and any("R&D" in e["text"] for e in ecb)
    atom = parse_rss(fx("bls_cpi.xml"), "bls_cpi")
    assert len(atom) == 4
    assert atom[0]["ts"] == pytest.approx(utc(2026, 9, 11, 11, 50, 40) + 0.968)
    assert atom[0]["url"] == "https://www.bls.gov/news.release/archives/cpi_09112026.htm"
    assert atom[0]["id"] == "cpi-2026_09_11__07_50_40"
    cd = parse_rss(fx("coindesk.xml"), "coindesk")
    assert len(cd) == 5 and cd[0]["id"] == "f9ecdaa6-45f1-4455-b874-bcb478300ac3"
    assert cd[0]["ts"] == utc(2026, 10, 8, 13, 6, 24)


def test_parse_rss_rejects_non_xml():
    with pytest.raises(SourceError):
        parse_rss("<html><body>Request Denied<p>oops</body>", "x")
    with pytest.raises(SourceError):
        parse_rss("", "x")


def test_parse_date_variants():
    assert parse_date("Wed, 7 Oct 2026 18:00:00 GMT") == utc(2026, 10, 7, 18)
    assert parse_date("Fri,  2 Oct 2026 08:32:11 -0400") == utc(2026, 10, 2, 12, 32, 11)
    assert parse_date("Thu, 08 Oct 2026 12:58:01 +0000") == utc(2026, 10, 8, 12, 58, 1)
    assert parse_date("Thu, 08 Oct 2026 08:58:01 EDT") == utc(2026, 10, 8, 12, 58, 1)
    assert parse_date("2026-10-08T12:30:29+00:00") == utc(2026, 10, 8, 12, 30, 29)
    assert parse_date("2026-10-08T12:30:29Z") == utc(2026, 10, 8, 12, 30, 29)
    assert parse_date("2026-10-08 08:30:29 -0400") == utc(2026, 10, 8, 12, 30, 29)
    assert parse_date("  2026-10-08T12:30:29.500-00:00 ") == pytest.approx(utc(2026, 10, 8, 12, 30, 29) + 0.5)
    assert parse_date("yesterday") is None and parse_date("") is None and parse_date(None) is None


def test_feed_names():
    assert feed_name("https://www.coindesk.com/arc/outboundfeeds/rss") == "coindesk"
    assert feed_name("https://www.federalreserve.gov/feeds/press_all.xml") == "fed_press"
    assert feed_name("https://www.ecb.europa.eu/rss/press.html") == "ecb_press"


def test_news_reexports_text_parsers():
    assert parse_whale_alert("1,000 $ETH (2,500,000 USD) transferred from #Binance to unknown wallet\nDetails")["flow"] == \
        "from_exchange"
    assert macro_surprise("US CPI ACTUAL 0.4% (FORECAST 0.3%)")["surprise"] == 1


# --------------------------------------------------------------------------- #
# polling
# --------------------------------------------------------------------------- #
async def test_telegram_first_get_then_after_and_catch_up():
    ch = "WatcherGuru"
    url = f"https://t.me/s/{ch}"
    pages = {
        None: tg_page(ch, [(100, NOW - 300, "JUST IN: SEC sues Binance over staking"),
                           (101, NOW - 200, "JUST IN: Robinhood buys $25M of Bitcoin")]),
        101: tg_page(ch, [(102, NOW - 50, "JUST IN: Tether mints $1B USDT on Tron network"),
                          (103, NOW - 40, "JUST IN: Coinbase lists XRP perpetuals")], after=103),
        103: tg_page(ch, [(104, NOW - 30, "JUST IN: Kraken halts withdrawals for Ethereum")]),
    }

    def route(params, headers):
        return ok(pages[int(params["after"])] if "after" in params else pages[None])

    http = FakeHttp({url: route})
    feed, clock = make_feed(settings(news_telegram_channels=[ch], news_telegram_fast=[ch]), http)
    assert await feed.step()
    assert http.calls[0][1] == {}  # first poll: plain GET of the head
    assert len(feed.items()) == 2
    clock.advance(20)
    assert await feed.step()
    assert [c[1] for c in http.calls[1:]] == [{"after": 101}, {"after": 103}]  # followed data-after
    assert [it.data["post_id"] for it in feed.items()] == [104, 103, 102, 101, 100]
    st = feed.sources["tg:watcherguru"]
    assert st.ok and st.items == 5 and "last id 104" in st.note


async def test_fast_and_slow_sources_have_own_due_times():
    fast, slow = "FinancialJuice", "cointelegraph"
    http = FakeHttp({f"https://t.me/s/{fast}": ok(tg_page(fast, [])), f"https://t.me/s/{slow}": ok(tg_page(slow, [])),
                     "https://cointelegraph.com/rss": ok(rss_doc([]))})
    st = settings(news_telegram_channels=[fast, slow], news_telegram_fast=[fast], news_rss=["https://cointelegraph.com/rss"])
    feed, clock = make_feed(st, http)
    assert feed.interval_s == 20.0
    await feed.step()
    assert len(http.calls) == 3
    for _ in range(2):  # t = 20 s, 40 s: only the fast squawk is due
        clock.advance(20)
        await feed.step()
    assert http.urls()[3:] == [f"https://t.me/s/{fast}", f"https://t.me/s/{fast}"]
    clock.advance(20)  # t = 60 s: everything
    await feed.step()
    assert sorted(http.urls()[5:]) == sorted([f"https://t.me/s/{fast}", f"https://t.me/s/{slow}",
                                              "https://cointelegraph.com/rss"])


async def test_one_broken_source_does_not_fail_the_feed():
    good, bad = "WatcherGuru", "nosuchchannel"
    http = FakeHttp({
        f"https://t.me/s/{good}": ok(tg_page(good, [(1, NOW - 10, "JUST IN: SEC sues Binance")])),
        # aiohttp follows the 302 to t.me/<handle>: a page without the channel history
        f"https://t.me/s/{bad}": ok('<html><body><div class="tgme_page">Join</div></body></html>'),
    })
    feed, clock = make_feed(settings(news_telegram_channels=[good, bad]), http)
    assert await feed.step()
    assert feed.status.ok and len(feed.items()) == 1
    assert feed.sources["tg:watcherguru"].ok
    badst = feed.sources["tg:nosuchchannel"]
    assert not badst.ok and "not a public channel" in badst.last_error
    # the broken source backs off (60 s, then 120 s) while the good one keeps its 60 s schedule
    for _ in range(3):
        clock.advance(60)
        await feed.step()
    assert http.urls().count(f"https://t.me/s/{good}") == 4
    assert http.urls().count(f"https://t.me/s/{bad}") == 3  # t = 0, 60, 180


async def test_all_sources_failing_fails_the_step_and_429_holds_telegram():
    a, b = "WatcherGuru", "markettwits"
    http = FakeHttp({f"https://t.me/s/{a}": HttpResponse(429, "", {"retry-after": "300"}),  # first in the queue
                     f"https://t.me/s/{b}": ConnectionError("reset")})
    feed, clock = make_feed(settings(news_telegram_channels=[a, b], news_telegram_fast=[a, b]), http)
    assert not await feed.step()
    assert not feed.status.ok and "news sources failed" in feed.status.last_error
    assert "rate limited" in feed.sources["tg:watcherguru"].last_error
    assert len(http.calls) == 1  # after the 429 the rest of the t.me queue waits
    # 429 on t.me holds every Telegram source for Retry-After (300 s), not just the channel that got it
    for _ in range(11):
        clock.advance(25)
        await feed.step()
    assert len(http.calls) == 1
    clock.advance(30)
    await feed.step()
    # after Retry-After: the most overdue source first (markettwits never ran), then the 429 one again
    assert http.urls()[1:] == ["https://t.me/s/markettwits", "https://t.me/s/WatcherGuru"]


async def test_rss_conditional_get_304_and_undated_items_skipped():
    url = "https://www.theblock.co/rss.xml"
    seen_headers = []

    def route(params, headers):
        seen_headers.append(headers)
        if headers.get("If-None-Match") == '"v1"':
            return HttpResponse(304, "")
        return ok(rss_doc([("a1", "SEC sues Kraken over staking program", "Thu, 08 Oct 2026 12:00:00 +0000"),
                           ("a2", "Undated headline without a publication time", None)]), etag='"v1"')

    http = FakeHttp({url: route})
    feed, clock = make_feed(settings(news_rss=[url]), http)
    assert await feed.step()
    assert [it.title for it in feed.items()] == ["SEC sues Kraken over staking program"]
    assert feed.skipped_no_ts == 1  # never an invented timestamp
    it = feed.items()[0]
    assert it.source == "rss" and it.channel == "theblock" and it.ts == utc(2026, 10, 8, 12)
    clock.advance(60)
    assert await feed.step()
    assert seen_headers[1] == {"If-None-Match": '"v1"'}
    assert feed.sources["rss:theblock"].ok and len(feed.items()) == 1


# --------------------------------------------------------------------------- #
# dedup / corroboration / guard
# --------------------------------------------------------------------------- #
async def test_macro_print_from_three_squawks_is_one_story():
    http = FakeHttp({
        "https://t.me/s/FinancialJuice": ok(fx("FinancialJuice_p2.html")),
        "https://t.me/s/markettwits": ok(fx("markettwits.html")),
        "https://t.me/s/WalterBloomberg": ok(fx("WalterBloomberg.html")),
    })
    feed, _ = make_feed(settings(news_telegram_channels=["FinancialJuice", "markettwits", "WalterBloomberg"]), http)
    assert await feed.step()
    claims = [it for it in feed.items(limit=500)
              if (it.data.get("macro") or {}).get("event_type") == "JOBLESS_CLAIMS"
              and it.data["macro"]["actual"] == 197000.0]
    assert len(claims) == 1
    story = claims[0]
    assert sorted(story.data["sources"]) == ["tg:financialjuice", "tg:markettwits", "tg:walterbloomberg"]
    assert story.ts == utc(2026, 10, 8, 12, 30, 12)  # earliest report (markettwits)
    assert story.data["macro"]["surprise"] == -1 and story.direction == 0  # claims: sign learned, not guessed
    # the continuing-claims print from the same channel is a different story
    cont = [it for it in feed.items(limit=500) if (it.data.get("macro") or {}).get("actual") == 1716000.0]
    assert len(cont) == 1
    surprises = feed.macro_surprises(utc(2026, 10, 8, 12, 45), 3600)
    assert {(s["event_type"], s["actual"]) for s in surprises} >= {("JOBLESS_CLAIMS", 197000.0),
                                                                   ("JOBLESS_CLAIMS", 1716000.0)}


async def test_cross_source_jaccard_dedup_and_window():
    a, b = "WatcherGuru", "wublockchainenglish"
    pages = {a: tg_page(a, [(1, NOW - 600, "Greece prepares to levy 10% crypto capital gains tax")]),
             b: tg_page(b, [(7, NOW - 300, "Greece plans 10% crypto capital gains tax"),
                            (8, NOW - 200, "Greece plans 15% crypto capital gains tax")])}
    http = FakeHttp({f"https://t.me/s/{c}": ok(p) for c, p in pages.items()})
    feed, _ = make_feed(settings(news_telegram_channels=[a, b]), http)
    await feed.step()
    titles = [it.title for it in feed.items()]
    assert titles == ["Greece plans 15% crypto capital gains tax",  # different figure -> different story
                      "Greece prepares to levy 10% crypto capital gains tax"]
    assert feed.items()[1].data["sources"] == ["tg:watcherguru", "tg:wublockchainenglish"]
    assert feed.duplicates == 1


async def test_same_headline_outside_dedup_window_is_new():
    a, b = "WatcherGuru", "unfolded"
    http = FakeHttp({f"https://t.me/s/{a}": ok(tg_page(a, [(1, NOW - 30000, "Binance lists XRP perpetuals")])),
                     f"https://t.me/s/{b}": ok(tg_page(b, [(2, NOW - 10, "Binance lists XRP perpetuals")]))})
    feed, _ = make_feed(settings(news_telegram_channels=[a, b]), http)
    await feed.step()
    assert len(feed.items()) == 2


async def test_guard_caps_uncorroborated_etf_decision_until_second_channel():
    a, b = "WatcherGuru", "BWEnews"
    pages = {a: [(1, NOW - 100, "JUST IN: SEC approves first U.S. spot Solana ETFs")], b: []}
    http = FakeHttp({f"https://t.me/s/{c}": (lambda c: lambda p, h: ok(tg_page(c, pages[c])))(c) for c in (a, b)})
    feed, clock = make_feed(settings(news_telegram_channels=[a, b], news_telegram_fast=[a, b]), http)
    await feed.step()
    it = feed.items()[0]
    assert it.category == "etf" and it.direction == 1 and it.assets == ["SOL"]
    assert it.importance == GUARD_CAP and it.data["importance_claimed"] > GUARD_CAP
    assert not it.data["corroborated"] and "не подтверждено" in it.data["guard"]
    pages[b] = [(9, NOW - 50, "BREAKING: SEC approves first US spot Solana ETFs")]
    clock.advance(20)
    await feed.step()
    assert len(feed.items()) == 1
    assert it.data["corroborated"] and it.importance > GUARD_CAP and "guard" not in it.data
    assert it.confidence > it.data["classified"]["confidence"]  # corroboration raises confidence


async def test_official_feed_is_corroboration_by_itself():
    url = "https://www.sec.gov/news/pressreleases.rss"
    http = FakeHttp({url: ok(rss_doc([("r1", "SEC approves first U.S. spot Solana ETFs",
                                       "Thu, 08 Oct 2026 12:40:00 +0000")]))})
    feed, _ = make_feed(settings(news_rss=[url]), http)
    await feed.step()
    it = feed.items()[0]
    assert it.channel == "sec_press" and it.data["official"] and it.importance > GUARD_CAP


async def test_publisher_telegram_and_rss_are_one_voice():
    url = "https://cointelegraph.com/rss"
    http = FakeHttp({
        url: ok(rss_doc([("c1", "SEC approves first U.S. spot Solana ETFs", "Thu, 08 Oct 2026 12:40:00 +0000")])),
        "https://t.me/s/cointelegraph": ok(tg_page("cointelegraph", [
            (9, NOW - 120, "🇺🇸 ETF: SEC approves first U.S. spot Solana ETFs")])),
    })
    feed, _ = make_feed(settings(news_telegram_channels=["cointelegraph"], news_rss=[url]), http)
    await feed.step()
    assert len(feed.items()) == 1
    it = feed.items()[0]
    assert sorted(it.data["sources"]) == ["rss:cointelegraph", "tg:cointelegraph"]
    assert it.data["n_sources"] == 1 and not it.data["corroborated"] and it.importance == GUARD_CAP


async def test_forwarded_copy_is_not_independent_corroboration():
    a = "WatcherGuru"
    fwd_page = tg_page("forklog", [(5, NOW - 50, "JUST IN: SEC approves first U.S. spot Solana ETFs")]).replace(
        '<div class="tgme_widget_message_text', '<a class="tgme_widget_message_forwarded_from_name" '
        'href="https://t.me/WatcherGuru/1">Watcher Guru</a><div class="tgme_widget_message_text')
    http = FakeHttp({f"https://t.me/s/{a}": ok(tg_page(a, [(1, NOW - 100, "JUST IN: SEC approves first U.S. spot "
                                                                           "Solana ETFs")])),
                     "https://t.me/s/forklog": ok(fwd_page)})
    feed, _ = make_feed(settings(news_telegram_channels=[a, "forklog"]), http)
    await feed.step()
    it = feed.items()[0]
    assert it.data["sources"] == ["tg:watcherguru"] and it.importance == GUARD_CAP


# --------------------------------------------------------------------------- #
# relevance / helpers / persistence
# --------------------------------------------------------------------------- #
async def test_relevant_pool_and_market_wide_items():
    ch = "WatcherGuru"
    http = FakeHttp({f"https://t.me/s/{ch}": ok(tg_page(ch, [
        (1, NOW - 120, "JUST IN: Bybit hacked: $1.5 billion in ETH stolen from cold wallet"),
        (2, NOW - 90, "FED CUTS RATES BY 50 BPS TO 3.25%-3.50%"),
        (3, NOW - 60, "UPBIT LISTING: 폰즈(PONS) 신규 거래지원 안내 (KRW, BTC, USDT 마켓)"),
        (4, NOW - 5000, "SEC sues Ripple over XRP sales again"),
        (5, NOW - 30, "Weekly community quiz winners"),
    ]))})
    feed, _ = make_feed(settings(news_telegram_channels=[ch]), http, pool=("BTC", "ETH"))
    await feed.step()
    eth = feed.relevant("ETH", NOW, 1800, min_importance=0.5)
    assert [it.data["post_id"] for it in eth] == [2, 1]  # Fed (market-wide) + the ETH hack, newest first
    assert [it.data["post_id"] for it in feed.relevant("BTC", NOW, 1800)] == [2]
    assert [it.data["post_id"] for it in feed.relevant("SOL", NOW, 1800)] == [2]  # non-pool: market-wide only
    assert feed.relevant("ETH", NOW, 60) == []  # window
    assert feed.items(limit=0) == [] and len(feed.items(limit=2)) == 2
    assert [it.data["post_id"] for it in feed.items(since_wall=NOW - 100)] == [5, 3, 2]
    assert [it.data["post_id"] for it in feed.relevant("XRP", NOW, 7200, min_importance=0.0)] == [2]  # XRP not in pool
    snap = feed.snapshot()
    by_id = {d["data"]["post_id"]: d for d in snap["items"]}
    assert by_id[3]["pool_relevant"] is False and by_id[1]["pool_relevant"] is True


async def test_whale_flows_from_real_page():
    ch = "whale_alert_io"
    clock = SimClock(wall=utc(2026, 10, 8, 3, 0), mono=1000.0)
    http = FakeHttp({f"https://t.me/s/{ch}": ok(fx("whale_alert_io_before103800.html"))})
    feed, _ = make_feed(settings(news_telegram_channels=[ch]), http, clock=clock, pool=("BTC", "ETH", "DOGE"))
    await feed.step()
    flows = feed.whale_flows("BTC", clock.wall(), 86400)
    assert flows and all(f["asset"] == "BTC" for f in flows)
    kinds = {f["flow"] for f in flows}
    assert {"to_exchange", "from_exchange", "other"} <= kinds
    usdc = feed.whale_flows("*", clock.wall(), 86400)
    assert any(f["flow"] == "mint" and f["asset"] == "USDC" for f in usdc)
    doge = feed.whale_flows("DOGE", clock.wall(), 86400)
    assert doge[0]["flow"] == "to_exchange" and doge[0]["to"] == "#Binance"
    story = next(it for it in feed.items(limit=100) if "Read Analysis" in it.text or "drops to" in it.title)
    assert "whale" not in story.data


class FakeDb:
    def __init__(self, rows=None):
        self.saved: list[dict] = []
        self.rows = rows or []
        self.list_calls = []

    def save_news(self, d: dict) -> None:
        self.saved.append(json.loads(json.dumps(d)))  # must be JSON-serialisable

    def list_news(self, since_ts=None, limit=200):
        self.list_calls.append((since_ts, limit))
        return [r for r in self.rows if since_ts is None or r["ts"] >= since_ts][:limit]


async def test_persistence_save_and_restore():
    restored = {"id": "old1", "ts": NOW - 600, "source": "telegram", "channel": "WatcherGuru",
                "title": "SEC sues Binance over staking", "assets": json.dumps(["BNB"]), "category": "regulation",
                "importance": 0.75, "direction": -1, "data": json.dumps({"sources": ["tg:watcherguru"]})}
    db = FakeDb(rows=[restored])
    ch = "unfolded"
    http = FakeHttp({f"https://t.me/s/{ch}": ok(tg_page(ch, [(3, NOW - 60, "SEC sues Binance over staking"),
                                                             (4, NOW - 30, "Kraken halts withdrawals")]))})
    feed, _ = make_feed(settings(news_telegram_channels=[ch]), http, db=db)
    await feed.step()
    assert db.list_calls == [(NOW - 21600.0, 500)]
    ids = [it.id for it in feed.items()]
    assert "old1" in ids and len(ids) == 2  # the restored story absorbed the duplicate
    old = next(it for it in feed.items() if it.id == "old1")
    assert old.data["sources"] == ["tg:watcherguru", "tg:unfolded"]
    saved_ids = [d["id"] for d in db.saved]
    assert saved_ids.count("old1") == 1 and len(saved_ids) == 2  # corroboration update + new item


async def test_claude_refinement_runs_in_background_and_is_saved():
    class FakeMessages:
        def __init__(self):
            self.calls = []

        async def create(self, **kw):
            self.calls.append(kw)
            text = json.dumps({"category": "exchange", "assets": ["*"], "direction": -1, "importance": 0.8,
                               "speed": "minutes", "confidence": 0.7, "reason": "крупная биржа остановила вывод"})
            return SimpleNamespace(stop_reason="end_turn", content=[SimpleNamespace(type="text", text=text)])

    client = SimpleNamespace(messages=FakeMessages())
    st = settings(news_telegram_channels=["WatcherGuru"])
    clock = SimClock(wall=NOW, mono=1000.0)
    db = FakeDb()
    http = FakeHttp({"https://t.me/s/WatcherGuru": ok(tg_page("WatcherGuru", [
        (1, NOW - 30, "JUST IN: Kraken halts withdrawals"),
        (2, NOW - 7200, "JUST IN: Bybit halts withdrawals"),  # too old for Claude
        (3, NOW - 20, "Weekly community quiz winners"),  # rule importance < 0.4
    ]))})
    feed = NewsFeed(st, clock, http, classifier=HeadlineClassifier(st, clock, client=client), db=db,
                    assets_fn=lambda: ["BTC", "ETH"])
    await feed.step()
    await feed.drain()
    assert len(client.messages.calls) == 1
    kraken = next(it for it in feed.items() if it.data["post_id"] == 1)
    assert kraken.classifier == "claude" and kraken.reason == "крупная биржа остановила вывод"
    assert kraken.data["rules"]["category"] == "exchange"
    assert [d["classifier"] for d in db.saved if d["id"] == kraken.id] == ["rules", "claude"]
    snap = feed.snapshot()
    assert snap["classifier"]["claude"]["used_last_hour"] == 1


async def test_snapshot_shape_and_disabled_feed():
    ch = "WatcherGuru"
    http = FakeHttp({f"https://t.me/s/{ch}": ok(tg_page(ch, [(1, NOW - 30, "JUST IN: SEC sues Binance")]))})
    feed, _ = make_feed(settings(news_telegram_channels=[ch], news_rss=["https://decrypt.co/feed"]), http)
    await feed.step()
    snap = feed.snapshot()
    assert set(snap) >= {"enabled", "status", "sources", "items", "count", "sources_ok", "sources_total"}
    assert snap["sources_total"] == 2 and snap["count"] == 1
    assert {s["name"] for s in snap["sources"]} == {"tg:watcherguru", "rss:decrypt"}
    json.dumps(snap)  # JSON-serialisable for /api/context
    off, _ = make_feed(settings(news_enabled=False, news_telegram_channels=[ch]), FakeHttp())
    assert not off.due() and off.status.note == "disabled in settings"
    assert all(not s.enabled for s in off.sources.values())


def test_default_sources_match_the_contract():
    feed = NewsFeed(SimpleNamespace(), SimClock(), FakeHttp(), classifier=None)
    names = set(feed.sources)
    assert len([n for n in names if n.startswith("tg:")]) == 13 and len([n for n in names if n.startswith("rss:")]) == 8
    assert {"tg:financialjuice", "tg:whale_alert_io", "rss:coindesk", "rss:fed_press", "rss:sec_press"} <= names
    assert "fast squawk" in feed.sources["tg:financialjuice"].note
    assert "fast squawk" not in feed.sources["tg:binance_announcements"].note
    assert feed.interval_s == 20.0
