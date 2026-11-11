"""Offline tests for latbot.context.calendar: MacroCalendar (Forex Factory weekly export,
XML preferred / JSON fallback, cache, rate limits, windows) and BlsActuals.

Fixtures (tests/fixtures/context/calendar-markets/, captured 2026-10-08): the FF weekly
export (XML + JSON, week 2026-10-04..10), the real nginx 404 of ``ff_calendar_nextweek``,
a SYNTHETIC "Request Denied" page built from the documented FF text, the BLS Atom feeds
(cpi / empsit / ppi) and trimmed BLS API v1 answers.
"""
from __future__ import annotations

import json
import logging
from calendar import timegm
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from latbot.clock import SimClock
from latbot.context.base import MACRO_EVENT_TYPES, HttpResponse, RateLimitedSource, SourceError
from latbot.context.calendar import (
    FF_JSON_URL,
    FF_XML_URL,
    BlsActuals,
    MacroCalendar,
    bls_api_value,
    bls_headline,
    bls_kind,
    classify_title,
    parse_bls_feed,
    parse_ff_json,
    parse_ff_xml,
    parse_number,
    surprise_of,
)

FIX = Path(__file__).parent / "fixtures" / "context" / "calendar-markets"

MINUTES_TS = float(timegm((2026, 10, 7, 18, 0, 0)))  # FOMC Meeting Minutes, High (XML "6:00pm" UTC)
WALLER_TS = float(timegm((2026, 10, 8, 8, 30, 0)))  # FOMC Member Waller Speaks, Medium
CLAIMS_TS = float(timegm((2026, 10, 8, 12, 30, 0)))  # Unemployment Claims, Medium
IN_WEEK = float(timegm((2026, 10, 7, 12, 0, 0)))


def fx_text(name: str, encoding: str = "utf-8") -> str:
    return (FIX / name).read_bytes().decode(encoding)


class FakeHttp:
    """url -> list of responses (the last one repeats) or an Exception to raise."""

    def __init__(self, routes: dict[str, Any] | None = None) -> None:
        self.routes: dict[str, Any] = dict(routes or {})
        self.calls: list[tuple[str, dict | None, dict | None]] = []

    async def get(self, url: str, *, params: dict | None = None, headers: dict | None = None,
                  timeout_s: float = 10.0) -> HttpResponse:
        self.calls.append((url, params, headers))
        r = self.routes.get(url)
        if r is None:
            return HttpResponse(404, "<html><body>404 Not Found</body></html>", {})
        if isinstance(r, list):
            r = r.pop(0) if len(r) > 1 else r[0]
        if isinstance(r, Exception):
            raise r
        return r

    async def close(self) -> None:
        pass

    def urls(self) -> list[str]:
        return [c[0] for c in self.calls]


def ok(text: str, **headers: str) -> HttpResponse:
    return HttpResponse(200, text, {k.lower().replace("_", "-"): v for k, v in headers.items()})


def settings(**over: Any) -> SimpleNamespace:
    base: dict[str, Any] = dict(macro_enabled=True, macro_refresh_s=3600, macro_countries=["USD"],
                                macro_impacts=["high", "medium"], macro_block_before_s=300,
                                macro_window_after_s=3600, bls_enabled=True, bls_poll_s=10.0, bls_window_s=600.0)
    base.update(over)
    return SimpleNamespace(**base)


def xml_http() -> FakeHttp:
    return FakeHttp({FF_XML_URL: ok(fx_text("ff_calendar_thisweek.xml", "cp1252"),
                                    last_modified="Thu, 08 Oct 2026 12:42:18 GMT")})


async def loaded_calendar(clock: SimClock | None = None, **over: Any) -> tuple[MacroCalendar, FakeHttp, SimClock]:
    clock = clock or SimClock(wall=IN_WEEK, mono=1000.0)
    http = xml_http()
    cal = MacroCalendar(settings(**over), clock, http)
    assert await cal.step() is True
    return cal, http, clock


def mini_xml(events: list[tuple[str, str, str, str, str, str, int]]) -> str:
    """(title, country, MM-DD-YYYY, time, impact, forecast, ebaseId) -> FF-style XML."""
    parts = ['<?xml version="1.0" encoding="windows-1252"?>\n<weeklyevents>']
    for title, country, date, time_, impact, forecast, ebase in events:
        fc = f"<forecast><![CDATA[{forecast}]]></forecast>" if forecast else "<forecast />"
        parts.append(
            f"<event><title>{title}</title><country>{country}</country><date><![CDATA[{date}]]></date>"
            f"<time><![CDATA[{time_}]]></time><impact><![CDATA[{impact}]]></impact>{fc}<previous />"
            f"<url><![CDATA[https://www.forexfactory.com/calendar/{ebase}-us-x]]></url></event>")
    parts.append("</weeklyevents>")
    return "\n".join(parts)


# --------------------------------------------------------------------------- #
# parsing
# --------------------------------------------------------------------------- #
def test_parse_ff_xml_fixture_utc_times_and_ebase_ids() -> None:
    rows = parse_ff_xml((FIX / "ff_calendar_thisweek.xml").read_bytes())
    assert len(rows) == 83
    minutes = [r for r in rows if r["title"] == "FOMC Meeting Minutes"]
    assert len(minutes) == 1
    m = minutes[0]
    assert m["country"] == "USD" and m["impact"] == "high" and m["ebase_id"] == 304
    assert m["ts"] == MINUTES_TS and m["time_known"] is True and m["time_label"] == "18:00 UTC"
    claims = next(r for r in rows if r["title"] == "Unemployment Claims")
    assert claims["ts"] == CLAIMS_TS  # 08:30 New York == 12:30 UTC (verified against the JSON offset)
    assert (claims["forecast"], claims["previous"]) == ("200K", "197K")
    bond = next(r for r in rows if r["title"] == "10-y Bond Auction" and r["country"] == "USD")
    assert bond["previous"] == "4.83|2.7" and bond["forecast"] == ""


def test_xml_and_json_exports_agree_on_every_event() -> None:
    x = parse_ff_xml((FIX / "ff_calendar_thisweek.xml").read_bytes())
    j = parse_ff_json(fx_text("ff_calendar_thisweek.json"))
    assert len(j) == 83
    assert {(r["title"], r["country"], r["ts"]) for r in x} == {(r["title"], r["country"], r["ts"]) for r in j}
    assert all(r["ebase_id"] is None for r in j)  # the JSON has no event-type id


def test_time_unknown_labels_are_never_midnight_releases() -> None:
    xml = mini_xml([
        ("Bank Holiday", "USD", "10-05-2026", "All Day", "Holiday", "", 370),
        ("FOMC Member Daly Speaks", "USD", "10-06-2026", "Tentative", "High", "", 807),
        ("G20 Meetings", "USD", "10-07-2026", "Day 1", "High", "", 999),
        ("Jackson Hole", "USD", "10-07-2026", "Aug 15th", "High", "", 998),
        ("CPI m/m", "USD", "10-07-2026", "12:30pm", "High", "0.3%", 78),
    ])
    rows = parse_ff_xml(xml)
    assert [r["time_known"] for r in rows] == [False, False, False, False, True]
    assert rows[1]["time_label"] == "Tentative" and rows[2]["time_label"] == "Day 1"


def test_json_local_midnight_or_missing_offset_is_time_unknown() -> None:
    data = json.dumps([
        {"title": "Bank Holiday", "country": "USD", "date": "2026-10-12T00:00:00-04:00", "impact": "Holiday",
         "forecast": "", "previous": ""},
        {"title": "CPI m/m", "country": "USD", "date": "2026-10-14T08:30:00-04:00", "impact": "High",
         "forecast": "0.3%", "previous": "0.4%"},
        {"title": "Naive", "country": "USD", "date": "2026-10-14T08:30:00", "impact": "High", "forecast": "",
         "previous": ""},
        {"title": "Broken", "country": "USD", "date": "not a date", "impact": "High"},
    ])
    rows = parse_ff_json(data)
    assert [r["title"] for r in rows] == ["Bank Holiday", "CPI m/m", "Naive"]
    assert [r["time_known"] for r in rows] == [False, True, False]
    assert rows[1]["ts"] == timegm((2026, 10, 14, 12, 30, 0))


def test_cp1252_bytes_and_latin1_misdecoded_text_are_repaired() -> None:
    raw = mini_xml([("ECB President Lagarde Speaks – Paris", "EUR", "10-05-2026", "9:00am", "High", "", 1)])
    data = raw.encode("cp1252")
    assert parse_ff_xml(data)[0]["title"] == "ECB President Lagarde Speaks – Paris"
    assert parse_ff_xml(data.decode("latin-1"))[0]["title"] == "ECB President Lagarde Speaks – Paris"


def test_classify_title_ebase_first_then_exact_titles() -> None:
    assert classify_title("CPI m/m") == "CPI"
    assert classify_title("CPI y/y", 884) == "CPI"
    assert classify_title("Core CPI m/m") == "CORE_CPI"
    assert classify_title("Renamed by FF", 78) == "CPI"  # ebaseId wins over the title
    assert classify_title("Non-Farm Employment Change") == "NFP"
    assert classify_title("ADP Non-Farm Employment Change") == "OTHER"  # not NFP
    assert classify_title("ADP Non-Farm Employment Change", 75) == "OTHER"
    assert classify_title("Federal Funds Rate", 1) == "FOMC_RATE"
    assert classify_title("FOMC Press Conference") == "FOMC_PRESSER"
    assert classify_title("FOMC Member Waller Speaks") == "FED_SPEECH"
    assert classify_title("Fed Chair Powell Testifies") == "FED_SPEECH"
    assert classify_title("Fed Chairman Warsh Speaks", 129) == "FED_SPEECH"
    assert classify_title("President Trump Speaks", 645) == "OTHER"
    assert classify_title("  unemployment   claims ") == "JOBLESS_CLAIMS"
    assert classify_title("Average Hourly Earnings m/m") == "EARNINGS"
    for t in ("CPI m/m", "Core PCE Price Index m/m", "ISM Services PMI", "JOLTS Job Openings", "x", ""):
        assert classify_title(t) in MACRO_EVENT_TYPES


@pytest.mark.parametrize("text,expected", [
    ("0.3%", 0.3), ("-0.4%", -0.4), ("175K", 175000.0), ("-1.2B", -1.2e9), ("1.9M", 1.9e6),
    ("53.3", 53.3), ("4.00%", 4.0), ("-23K", -23000.0), ("+29,000", 29000.0), ("1,234.5K", 1234500.0),
    ("−0.1%", -0.1), ("2.1T", 2.1e12),
    ("", None), (None, None), ("4.83|2.7", None), ("<0.1%", None), ("n/a", None), ("1.2.3", None),
])
def test_parse_number(text: str | None, expected: float | None) -> None:
    v = parse_number(text)
    if expected is None:
        assert v is None
    else:
        assert v == pytest.approx(expected)


def test_surprise_of() -> None:
    assert surprise_of("0.4%", "0.3%") == 1
    assert surprise_of("98K", "118K") == -1
    assert surprise_of("0.3%", "0.3%") == 0
    assert surprise_of("0.3%", "0.30%") == 0
    assert surprise_of("1.9M", "1850K") == 1  # both scaled
    assert surprise_of("53.3", "53.8", tolerance=0.5) == 0
    assert surprise_of("53.3", "53.8", tolerance=0.4) == -1
    assert surprise_of("0.3%", "") is None
    assert surprise_of("", "0.3%") is None
    assert surprise_of("0.3%", "175K") is None  # units differ
    assert surprise_of("53.3", "53K") is None


# --------------------------------------------------------------------------- #
# collector
# --------------------------------------------------------------------------- #
async def test_poll_xml_filters_usd_high_medium_sorted() -> None:
    cal, http, _ = await loaded_calendar()
    assert http.urls() == [FF_XML_URL]
    headers = http.calls[0][2]
    assert "Mozilla" in headers["User-Agent"]
    assert cal.status.ok and cal.format == "xml" and cal.status.items == 83
    evs = cal.events()
    assert evs and all(e.country == "USD" and e.impact in ("high", "medium") for e in evs)
    assert [e.ts for e in evs] == sorted(e.ts for e in evs)
    titles = [e.title for e in evs]
    assert "FOMC Meeting Minutes" in titles and "Unemployment Claims" in titles and "Trade Balance" not in titles
    minutes = next(e for e in evs if e.title == "FOMC Meeting Minutes")
    assert minutes.event_type == "FOMC_MINUTES" and minutes.source == "forexfactory" and minutes.surprise is None
    info = cal.event_info(minutes.id)
    assert info is not None and info["ebase_id"] == 304 and info["time_known"] is True
    # stable ids: the same event gets the same id from the JSON export
    cal2 = MacroCalendar(settings(), SimClock(wall=IN_WEEK), FakeHttp({
        FF_XML_URL: HttpResponse(429, "Edge: Too Many Requests", {}),
        FF_JSON_URL: ok(fx_text("ff_calendar_thisweek.json")),
    }))
    assert await cal2.step() is True
    assert {e.id for e in cal2.events()} == {e.id for e in evs}


async def test_interval_is_never_below_30_minutes_and_next_poll_respects_it() -> None:
    cal, http, clock = await loaded_calendar(macro_refresh_s=60)
    assert cal.interval_s == 1800.0
    assert not cal.due(clock.mono() + 1799)
    assert cal.due(clock.mono() + 1800)


async def test_settings_object_from_config_works_with_defaults() -> None:
    from latbot.config import Settings

    cal = MacroCalendar(Settings(), SimClock(wall=IN_WEEK), xml_http())
    assert cal.interval_s == 3600.0
    assert await cal.step() is True
    assert all(e.country == "USD" for e in cal.events())


async def test_request_denied_html_falls_back_to_json() -> None:
    http = FakeHttp({
        FF_XML_URL: ok(fx_text("ff_request_denied_synthetic.html")),
        FF_JSON_URL: ok(fx_text("ff_calendar_thisweek.json")),
    })
    cal = MacroCalendar(settings(), SimClock(wall=IN_WEEK), http)
    assert await cal.step() is True
    assert http.urls() == [FF_XML_URL, FF_JSON_URL]
    assert cal.format == "json" and "JSON" in cal.status.note
    assert len(cal.events()) > 5


async def test_rate_limited_keeps_last_good_copy_and_backs_off_at_least_300s() -> None:
    cal, http, clock = await loaded_calendar()
    before = [e.id for e in cal.events()]
    http.routes = {
        FF_XML_URL: HttpResponse(429, "Too Many Requests", {"retry-after": "120"}),
        FF_JSON_URL: ok(fx_text("ff_request_denied_synthetic.html")),
    }
    clock.advance(3600)
    assert cal.due()
    assert await cal.step() is False
    assert cal.status.ok is False
    assert "rate limited" in cal.status.last_error.lower() and "request denied" in cal.status.last_error.lower()
    assert [e.id for e in cal.events()] == before  # last good copy stays
    assert not cal.due(clock.mono() + 299)


async def test_request_denied_page_with_an_error_status_is_a_rate_limit() -> None:
    denied = fx_text("ff_request_denied_synthetic.html")
    http = FakeHttp({FF_XML_URL: HttpResponse(403, denied, {}), FF_JSON_URL: HttpResponse(503, denied, {})})
    clock = SimClock(wall=IN_WEEK, mono=50.0)
    cal = MacroCalendar(settings(), clock, http)
    assert await cal.step() is False
    assert "Request Denied" in cal.status.last_error
    assert not cal.due(clock.mono() + 299)


async def test_html_or_404_is_an_error_not_an_empty_calendar() -> None:
    http = FakeHttp({
        FF_XML_URL: ok(fx_text("ff_nextweek_404.html")),
        FF_JSON_URL: HttpResponse(404, fx_text("ff_nextweek_404.html"), {}),
    })
    cal = MacroCalendar(settings(), SimClock(wall=IN_WEEK), http)
    assert await cal.step() is False
    assert "HTML" in cal.status.last_error and "404" in cal.status.last_error
    assert cal.events() == [] and cal.available() is False


async def test_network_error_on_both_formats_is_reported() -> None:
    http = FakeHttp({FF_XML_URL: OSError("connection reset"), FF_JSON_URL: TimeoutError()})
    cal = MacroCalendar(settings(), SimClock(wall=IN_WEEK), http)
    assert await cal.step() is False
    assert "OSError" in cal.status.last_error and "TimeoutError" in cal.status.last_error


async def test_not_modified_keeps_events_and_refreshes_age() -> None:
    cal, http, clock = await loaded_calendar()
    n = len(cal.events())
    http.routes = {FF_XML_URL: HttpResponse(304, "", {})}
    clock.advance(3600)
    assert await cal.step() is True
    assert http.calls[-1][2]["If-Modified-Since"] == "Thu, 08 Oct 2026 12:42:18 GMT"
    assert len(cal.events()) == n and cal.fetched_ts == clock.wall() and cal.age_s() == 0.0


async def test_cache_survives_restart_and_works_offline(tmp_path: Path) -> None:
    path = tmp_path / "ctx" / "macro_calendar.json"
    clock = SimClock(wall=IN_WEEK, mono=1000.0)
    cal = MacroCalendar(settings(), clock, xml_http(), cache_path=path)
    assert await cal.step() is True
    assert path.is_file()
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["fetched_ts"] == IN_WEEK and data["format"] == "xml" and "<weeklyevents>" in data["body"]
    minutes = next(e for e in cal.events() if e.title == "FOMC Meeting Minutes")
    assert cal.set_surprise(minutes.id, -1)

    clock.advance(600)
    dead = FakeHttp({FF_XML_URL: OSError("offline"), FF_JSON_URL: OSError("offline")})
    cal2 = MacroCalendar(settings(), clock, dead, cache_path=path)
    assert cal2.from_cache and cal2.format == "xml" and cal2.fetched_ts == IN_WEEK
    assert [e.id for e in cal2.events()] == [e.id for e in cal.events()]
    assert cal2.get(minutes.id).surprise == -1  # overrides persisted
    assert cal2.age_s() == 600.0
    assert not cal2.due()  # a fresh cache: no immediate re-download after a restart
    assert cal2.due(clock.mono() + 3000)
    assert cal2.snapshot()["from_cache"] is True


def test_corrupt_cache_is_ignored(tmp_path: Path) -> None:
    path = tmp_path / "macro_calendar.json"
    path.write_text("{not json", encoding="utf-8")
    cal = MacroCalendar(settings(), SimClock(wall=IN_WEEK), FakeHttp(), cache_path=path)
    assert cal.events() == [] and cal.fetched_ts is None and cal.due()


async def test_windows_upcoming_recent_next() -> None:
    cal, _, _ = await loaded_calendar()
    minutes = next(e for e in cal.events() if e.title == "FOMC Meeting Minutes")
    assert cal.event_window(MINUTES_TS - 301) == []
    assert cal.event_window(MINUTES_TS - 200) == [(minutes, "before")]
    assert cal.event_window(MINUTES_TS) == [(minutes, "after")]
    assert cal.event_window(MINUTES_TS + 3600) == [(minutes, "after")]
    assert cal.event_window(MINUTES_TS + 3601) == []
    # medium events never open a window
    assert cal.event_window(CLAIMS_TS + 10) == []

    assert cal.next_event(MINUTES_TS - 10) is minutes
    assert cal.next_event(MINUTES_TS) is None  # no other high-impact US event this week
    assert [e.title for e in cal.upcoming(MINUTES_TS - 100, 600)] == ["FOMC Meeting Minutes"]
    assert cal.upcoming(MINUTES_TS, 600) == []
    assert [e.title for e in cal.recent(MINUTES_TS + 60, 600)] == ["FOMC Meeting Minutes"]
    med = cal.upcoming(WALLER_TS - 60, 4 * 3600 + 60, "medium")
    assert [e.title for e in med] == ["FOMC Member Waller Speaks", "Unemployment Claims"]
    nxt = cal.next_event(MINUTES_TS + 1, "medium")
    assert nxt is not None and nxt.title == "FOMC Member Waller Speaks"


async def test_custom_window_settings() -> None:
    cal, _, _ = await loaded_calendar(macro_block_before_s=120, macro_window_after_s=600)
    assert cal.event_window(MINUTES_TS - 200) == []
    assert cal.event_window(MINUTES_TS - 100)[0][1] == "before"
    assert cal.event_window(MINUTES_TS + 601) == []


async def test_undated_events_are_listed_but_excluded_from_timed_queries() -> None:
    xml = mini_xml([
        ("FOMC Member Daly Speaks", "USD", "10-07-2026", "Tentative", "High", "", 807),
        ("CPI m/m", "USD", "10-07-2026", "12:30pm", "High", "0.3%", 78),
    ])
    clock = SimClock(wall=float(timegm((2026, 10, 7, 0, 0, 0))) + 60)
    cal = MacroCalendar(settings(), clock, FakeHttp({FF_XML_URL: ok(xml)}))
    assert await cal.step() is True
    assert [e.title for e in cal.events()] == ["CPI m/m"]
    undated = [e for e in cal.events(include_undated=True) if e.title == "FOMC Member Daly Speaks"]
    assert len(undated) == 1 and not cal.is_time_known(undated[0].id)
    midnight = float(timegm((2026, 10, 7, 0, 0, 0)))
    assert cal.event_window(midnight) == [] and cal.recent(midnight + 120, 600) == []
    assert [e.title for e in cal.upcoming(midnight, 86400)] == ["CPI m/m"]
    snap = cal.snapshot()
    assert [d["title"] for d in snap["time_unknown"]] == ["FOMC Member Daly Speaks"]
    assert snap["time_unknown"][0]["time_label"] == "Tentative"
    assert [d["title"] for d in snap["events"]] == ["CPI m/m"]


async def test_set_actual_and_set_surprise() -> None:
    xml = mini_xml([
        ("CPI m/m", "USD", "10-07-2026", "12:30pm", "High", "0.3%", 78),
        ("Core CPI m/m", "USD", "10-07-2026", "12:30pm", "High", "", 79),
    ])
    cal = MacroCalendar(settings(), SimClock(wall=IN_WEEK), FakeHttp({FF_XML_URL: ok(xml)}))
    assert await cal.step()
    by_title = {e.title: e for e in cal.events()}
    cpi, core = by_title["CPI m/m"], by_title["Core CPI m/m"]
    assert cal.set_actual("nope", "0.4%") is False
    assert cal.set_actual(cpi.id, "") is False

    # squawk first, then the real number overrides it
    assert cal.set_surprise(cpi.id, -1) is True and cpi.surprise == -1
    assert cal.set_actual(cpi.id, "0.4%") is True
    assert cpi.actual == "0.4%" and cpi.surprise == 1 and cal.event_info(cpi.id)["surprise_source"] == "actual"
    assert cal.set_surprise(cpi.id, -1) is False and cpi.surprise == 1  # actual is authoritative

    # no forecast: the actual is recorded, a squawk surprise is kept, never guessed
    assert cal.set_surprise(core.id, 1, source="FinancialJuice") is True
    assert cal.set_actual(core.id, "0.3%") is True
    assert core.actual == "0.3%" and core.surprise == 1
    assert cal.event_info(core.id)["surprise_source"] == "FinancialJuice"
    assert cal.set_surprise(core.id, None) is True and core.surprise is None
    assert cal.set_surprise(core.id, 5) is False  # type: ignore[arg-type]


async def test_actual_survives_a_refresh_of_the_same_week() -> None:
    cal, http, clock = await loaded_calendar()
    claims = next(e for e in cal.events() if e.title == "Unemployment Claims")
    cal.set_actual(claims.id, "210K")
    clock.advance(3600)
    assert await cal.step()
    again = cal.get(claims.id)
    assert again is not None and again is not claims
    assert again.actual == "210K" and again.surprise == 1


async def test_available_only_inside_the_covered_week() -> None:
    cal, _, _ = await loaded_calendar()
    start, end = cal.coverage()
    assert start == timegm((2026, 10, 4, 4, 0, 0)) and end == timegm((2026, 10, 11, 4, 0, 0))
    assert cal.available(IN_WEEK) is True
    assert cal.available(float(timegm((2026, 10, 12, 12, 0, 0)))) is False  # last week's file
    assert cal.available(float(timegm((2026, 10, 3, 12, 0, 0)))) is False
    assert MacroCalendar(settings(), SimClock(), FakeHttp()).available() is False


async def test_country_and_impact_filters_from_settings() -> None:
    cal, _, _ = await loaded_calendar(macro_countries=["USD", "EUR"], macro_impacts=["high"])
    assert {e.country for e in cal.events()} <= {"USD", "EUR"}
    assert all(e.impact == "high" for e in cal.events())


async def test_disabled_calendar_is_never_due() -> None:
    cal = MacroCalendar(settings(macro_enabled=False), SimClock(), FakeHttp())
    assert cal.due() is False and cal.status.note == "disabled in settings"


async def test_snapshot_shape() -> None:
    cal, _, _ = await loaded_calendar()
    snap = cal.snapshot(MINUTES_TS - 100)
    json.dumps(snap)  # serialisable
    assert snap["available"] is True and snap["format"] == "xml" and snap["age_s"] is not None
    assert snap["next_high"]["title"] == "FOMC Meeting Minutes"
    assert snap["window"] == [{"event": snap["next_high"], "phase": "before"}]
    assert all(d["time_known"] for d in snap["events"])


# --------------------------------------------------------------------------- #
# BLS
# --------------------------------------------------------------------------- #
CPI_RELEASE = float(timegm((2026, 9, 11, 12, 30, 0)))
NFP_RELEASE = float(timegm((2026, 10, 2, 12, 30, 0)))
BLS_CPI = "https://www.bls.gov/feed/cpi.rss"
BLS_EMPSIT = "https://www.bls.gov/feed/empsit.rss"


def test_bls_headlines_from_real_feeds_match_ff_actuals() -> None:
    cpi = parse_bls_feed(fx_text("bls_cpi.rss"))
    assert len(cpi) == 12 and cpi[0]["published_ts"] == pytest.approx(timegm((2026, 9, 11, 11, 50, 40)) + 0.968)
    e = cpi[0]
    assert bls_headline("cpi_mm", e["title"], e["content"]) == "0.4%"
    assert bls_headline("core_cpi_mm", e["title"], e["content"]) == "0.3%"
    assert bls_headline("cpi_yy", e["title"], e["content"]) == "3.4%"
    assert bls_headline("core_cpi_yy", e["title"], e["content"]) == "2.4%"
    june = cpi[2]  # "falls 0.4% in June", core "was unchanged"
    assert bls_headline("cpi_mm", june["title"], june["content"]) == "-0.4%"
    assert bls_headline("core_cpi_mm", june["title"], june["content"]) == "0.0%"
    nov25 = next(x for x in cpi if "12 months ending in November" in x["title"])
    assert bls_headline("cpi_mm", nov25["title"], nov25["content"]) is None  # only a 12-month number
    assert bls_headline("cpi_yy", nov25["title"], nov25["content"]) == "2.7%"

    emp = parse_bls_feed(fx_text("bls_empsit.rss"))
    got = [(bls_headline("nfp", x["title"], x["content"]), bls_headline("unemployment", x["title"], x["content"]))
           for x in emp[:4]]
    assert got == [("29K", "4.2%"), ("162K", "4.1%"), ("-23K", "4.1%"), ("57K", "4.2%")]
    feb = next(x for x in emp if "edges down 92,000" in x["title"])
    assert bls_headline("nfp", feb["title"], feb["content"]) == "-92K"

    ppi = parse_bls_feed(fx_text("bls_ppi.rss"))
    assert [bls_headline("ppi_mm", x["title"], x["content"]) for x in ppi[:3]] == ["0.4%", "0.0%", "-0.3%"]
    assert bls_headline("ppi_yy", ppi[0]["title"], ppi[0]["content"]) == "5.4%"


def test_bls_headline_is_strict() -> None:
    # title and content disagree -> nothing
    assert bls_headline("cpi_mm", "CPI for all items increases 0.4% in August",
                        "the Consumer Price Index for All Urban Consumers rose 0.5 percent, seasonally adjusted") is None
    assert bls_headline("cpi_mm", "Something else entirely", "No numbers here.") is None
    assert bls_headline("ahe_mm", "x", "Average hourly earnings rose 0.3 percent") is None  # not an Atom kind
    assert bls_headline("unknown", "x", "y") is None


def test_bls_kind_mapping() -> None:
    from latbot.context.base import MacroEvent

    def ev(title: str, et: str, country: str = "USD") -> MacroEvent:
        return MacroEvent(id="x", title=title, country=country, ts=0.0, impact="high", event_type=et)

    assert bls_kind(ev("CPI m/m", "CPI")) == "cpi_mm"
    assert bls_kind(ev("CPI y/y", "CPI")) == "cpi_yy"
    assert bls_kind(ev("Core CPI y/y", "CORE_CPI")) == "core_cpi_yy"
    assert bls_kind(ev("PPI m/m", "PPI")) == "ppi_mm"
    assert bls_kind(ev("Core PPI m/m", "CORE_PPI")) is None
    assert bls_kind(ev("Non-Farm Employment Change", "NFP")) == "nfp"
    assert bls_kind(ev("Unemployment Rate", "UNEMPLOYMENT")) == "unemployment"
    assert bls_kind(ev("Average Hourly Earnings m/m", "EARNINGS")) == "ahe_mm"
    assert bls_kind(ev("CPI m/m", "CPI", "CAD")) is None
    assert bls_kind(ev("FOMC Statement", "FOMC_STATEMENT")) is None


def test_bls_api_value() -> None:
    cpi = json.loads(fx_text("bls_api_CUSR0000SA0.json"))
    assert bls_api_value(cpi, "pct", CPI_RELEASE) == ("0.4%", "ok")  # 334.131 / 332.813
    assert bls_api_value(cpi, "pct", float(timegm((2026, 8, 12, 12, 30, 0))))[1] == "too_late"
    assert bls_api_value(cpi, "pct", float(timegm((2026, 10, 14, 12, 30, 0)))) == (None, "not_yet")
    nfp = json.loads(fx_text("bls_api_CES0000000001.json"))
    assert bls_api_value(nfp, "diff_k", NFP_RELEASE) == ("29K", "ok")  # 159044 - 159015
    ahe = json.loads(fx_text("bls_api_CES0500000003.json"))
    assert bls_api_value(ahe, "pct", NFP_RELEASE) == ("0.1%", "ok")  # 37.81 / 37.76
    unrate = {"status": "REQUEST_SUCCEEDED", "Results": {"series": [{"data": [
        {"year": "2026", "period": "M09", "value": "4.2"}, {"year": "2026", "period": "M08", "value": "4.1"}]}]}}
    assert bls_api_value(unrate, "level_pct", NFP_RELEASE) == ("4.2%", "ok")
    gap = {"status": "REQUEST_SUCCEEDED", "Results": {"series": [{"data": [
        {"year": "2026", "period": "M09", "value": "37.81"}, {"year": "2026", "period": "M07", "value": "37.64"}]}]}}
    assert bls_api_value(gap, "pct", NFP_RELEASE) == (None, "bad")  # never bridges a missing month
    with pytest.raises(RateLimitedSource):
        bls_api_value(json.loads(fx_text("bls_api_not_processed.json")), "pct", NFP_RELEASE)
    with pytest.raises(SourceError):
        bls_api_value({"status": "REQUEST_FAILED", "message": ["bad series"]}, "pct", NFP_RELEASE)


def cpi_week_xml() -> str:
    return mini_xml([
        ("CPI m/m", "USD", "09-11-2026", "12:30pm", "High", "0.3%", 78),
        ("Core CPI m/m", "USD", "09-11-2026", "12:30pm", "High", "0.3%", 79),
        ("CPI y/y", "USD", "09-11-2026", "12:30pm", "High", "3.3%", 884),
        ("Core PPI m/m", "USD", "09-11-2026", "12:30pm", "High", "0.2%", 87),
    ])


async def bls_setup(xml: str, wall: float, **over: Any) -> tuple[MacroCalendar, SimClock]:
    clock = SimClock(wall=wall, mono=5000.0)
    cal = MacroCalendar(settings(**over), clock, FakeHttp({FF_XML_URL: ok(xml)}))
    assert await cal.step()
    return cal, clock


async def test_bls_actuals_fill_cpi_after_release() -> None:
    cal, clock = await bls_setup(cpi_week_xml(), CPI_RELEASE - 30)
    http = FakeHttp({BLS_CPI: ok(fx_text("bls_cpi.rss"))})
    bls = BlsActuals(settings(), clock, http, cal)
    assert bls.interval_s == 10.0 and "BLS_API_KEY" in bls.status.note
    assert await bls.step() is True
    assert http.calls == []  # nothing before the scheduled release

    clock.advance(45)
    assert await bls.step() is True
    assert http.urls() == [BLS_CPI]  # one fetch serves CPI m/m, Core CPI and CPI y/y
    by_title = {e.title: e for e in cal.events()}
    assert (by_title["CPI m/m"].actual, by_title["CPI m/m"].surprise) == ("0.4%", 1)
    assert (by_title["Core CPI m/m"].actual, by_title["Core CPI m/m"].surprise) == ("0.3%", 0)
    assert (by_title["CPI y/y"].actual, by_title["CPI y/y"].surprise) == ("3.4%", 1)
    assert by_title["Core PPI m/m"].actual == "" and by_title["Core PPI m/m"].surprise is None
    assert bls.status.items == 3
    res = {r["title"]: r for r in bls.results}
    assert res["CPI m/m"]["delay_s"] == pytest.approx(15.0) and res["CPI m/m"]["source"] == "bls_atom"

    clock.advance(20)
    assert await bls.step() is True
    assert len(http.calls) == 1  # done: no more requests


async def test_bls_feed_not_updated_within_window_leaves_surprise_unknown() -> None:
    cal, clock = await bls_setup(cpi_week_xml(), CPI_RELEASE + 5)
    old_feed = fx_text("bls_cpi.rss").replace("2026-09-11T07:50:40.968-04:00", "2026-08-12T07:51:16.21-04:00")
    http = FakeHttp({BLS_CPI: ok(old_feed)})
    bls = BlsActuals(settings(), clock, http, cal)
    for _ in range(3):
        assert await bls.step() is True
        clock.advance(10)
    assert len(http.calls) == 3
    assert all(e.actual == "" and e.surprise is None for e in cal.events())
    clock.advance(600)
    assert await bls.step() is True
    n = len(http.calls)
    assert {r["title"] for r in bls.results if r["actual"] is None} == {"CPI m/m", "Core CPI m/m", "CPI y/y"}
    clock.advance(10)
    assert await bls.step() is True
    assert len(http.calls) == n  # window closed, no key -> stops polling


async def test_bls_old_events_at_startup_are_not_polled() -> None:
    cal, clock = await bls_setup(cpi_week_xml(), CPI_RELEASE + 7200)
    http = FakeHttp({BLS_CPI: ok(fx_text("bls_cpi.rss"))})
    bls = BlsActuals(settings(), clock, http, cal)
    assert await bls.step() is True
    assert http.calls == [] and bls.results == []


async def test_bls_feed_error_is_reported() -> None:
    cal, clock = await bls_setup(cpi_week_xml(), CPI_RELEASE + 5)
    http = FakeHttp({BLS_CPI: HttpResponse(403, "Access Denied", {})})
    bls = BlsActuals(settings(), clock, http, cal)
    assert await bls.step() is False
    assert "403" in bls.status.last_error
    assert all(e.actual == "" for e in cal.events())


async def test_bls_contact_goes_into_user_agent() -> None:
    cal, clock = await bls_setup(cpi_week_xml(), CPI_RELEASE + 5)
    http = FakeHttp({BLS_CPI: ok(fx_text("bls_cpi.rss"))})
    bls = BlsActuals(settings(bls_contact="owner@example.com"), clock, http, cal)
    await bls.step()
    assert http.calls[0][2]["User-Agent"].endswith("(owner@example.com)")


async def test_bls_api_fills_earnings_and_never_leaks_the_key(caplog: pytest.LogCaptureFixture) -> None:
    xml = mini_xml([
        ("Non-Farm Employment Change", "USD", "10-02-2026", "12:30pm", "High", "50K", 66),
        ("Average Hourly Earnings m/m", "USD", "10-02-2026", "12:30pm", "High", "0.3%", 159),
    ])
    cal, clock = await bls_setup(xml, NFP_RELEASE + 5)
    key = "SECRETKEY1234567"
    api = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    http = FakeHttp({
        BLS_EMPSIT: ok(fx_text("bls_empsit.rss")),
        api + "CES0500000003": ok(fx_text("bls_api_CES0500000003.json")),
    })
    bls = BlsActuals(settings(bls_api_delay_s=3600, bls_api_retry_s=1800), clock, http, cal, api_key=key)
    assert bls.has_api_key and bls.status.note == ""
    caplog.set_level(logging.DEBUG)
    assert await bls.step() is True
    by_title = {e.title: e for e in cal.events()}
    assert by_title["Non-Farm Employment Change"].actual == "29K"
    assert by_title["Non-Farm Employment Change"].surprise == -1
    assert by_title["Average Hourly Earnings m/m"].actual == ""  # Atom has no AHE; API only after the delay
    assert http.urls() == [BLS_EMPSIT]

    clock.advance(3600)
    assert await bls.step() is True
    assert http.urls()[-1] == api + "CES0500000003"
    assert http.calls[-1][1] == {"registrationkey": key}
    ahe = by_title["Average Hourly Earnings m/m"]
    assert (ahe.actual, ahe.surprise) == ("0.1%", -1)
    assert any(r["source"] == "bls_api" and r["actual"] == "0.1%" for r in bls.results)
    blob = json.dumps(bls.snapshot()) + json.dumps(cal.snapshot()) + caplog.text
    assert key not in blob


async def test_bls_api_not_yet_published_is_retried_later() -> None:
    xml = mini_xml([("Average Hourly Earnings m/m", "USD", "10-02-2026", "12:30pm", "High", "0.3%", 159)])
    cal, clock = await bls_setup(xml, NFP_RELEASE + 4000)
    api = "https://api.bls.gov/publicAPI/v2/timeseries/data/CES0500000003"
    aug_only = {"status": "REQUEST_SUCCEEDED", "Results": {"series": [{"data": [
        {"year": "2026", "period": "M08", "value": "37.76"}, {"year": "2026", "period": "M07", "value": "37.64"}]}]}}
    http = FakeHttp({api: [ok(json.dumps(aug_only)), ok(fx_text("bls_api_CES0500000003.json"))]})
    bls = BlsActuals(settings(bls_api_retry_s=1800), clock, http, cal, api_key="k" * 20)
    assert await bls.step() is True
    assert len(http.calls) == 1 and cal.events()[0].actual == ""
    clock.advance(60)
    assert await bls.step() is True
    assert len(http.calls) == 1  # waits bls_api_retry_s
    clock.advance(1800)
    assert await bls.step() is True
    assert len(http.calls) == 2 and cal.events()[0].actual == "0.1%"
