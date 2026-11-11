"""context/classify.py: rule classifier, Claude refinement (fake client), combined classifier."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from latbot.clock import SimClock
from latbot.context.base import NEWS_CATEGORIES, NEWS_SPEEDS, NewsItem
from latbot.context.classify import (
    CLAUDE_SCHEMA,
    ClaudeClassifier,
    HeadlineClassifier,
    RuleClassifier,
    dedup_key,
    extract_assets,
    headline,
    macro_surprise,
    parse_json_answer,
    parse_whale_alert,
    similarity,
)

FIX = Path(__file__).parent / "fixtures" / "context" / "news"
POOL = ["BTC", "ETH", "SOL", "XRP", "LINK", "ADA", "AAVE", "DOGE", "BCH", "ETC"]


def _settings(**kw):
    base = dict(news_claude_enabled=True, news_claude_max_per_hour=30, news_claude_min_importance=0.4,
                news_claude_max_age_s=1800.0, news_claude_timeout_s=5.0, analysis_model="claude-opus-5-5")
    base.update(kw)
    return SimpleNamespace(**base)


def _item(title: str, text: str = "", ts: float = 1_700_000_000.0, **kw) -> NewsItem:
    return NewsItem(id="x1", ts=ts, source="telegram", channel="WatcherGuru", title=title, text=text, **kw)


# --------------------------------------------------------------------------- #
# rules
# --------------------------------------------------------------------------- #
def test_rules_accuracy_on_labelled_headlines():
    """In-sample sanity check on the research fixture (not an accuracy estimate):
    the port must keep the prototype's 31/32 per field; the threshold leaves room
    for the documented misses (RU negation, tokenised-stocks partnership)."""
    rc = RuleClassifier()
    rows = [json.loads(line) for line in (FIX / "headlines_labelled.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()]
    assert len(rows) == 32
    hits = {"category": 0, "direction": 0, "speed": 0}
    for row in rows:
        r = rc.classify(row["title"], row["title"], [])
        assert r["category"] in NEWS_CATEGORIES and r["speed"] in NEWS_SPEEDS
        assert r["direction"] in (-1, 0, 1) and 0.0 <= r["importance"] <= 1.0 and 0.0 <= r["confidence"] <= 1.0
        for k in hits:
            hits[k] += int(r[k] == row["exp"][k])
        if "max_importance" in row["exp"]:
            assert r["importance"] <= row["exp"]["max_importance"], row["title"]
        if "assets" in row["exp"]:
            assert r["assets"] == row["exp"]["assets"], row["title"]
    for k, v in hits.items():
        assert v >= 29, (k, v)  # >= 90 %


def test_market_move_direction_forced_zero():
    rc = RuleClassifier()
    r = rc.classify("Bitcoin plunges 8% below $80,000 as sellers dump", "", [])
    assert r["category"] == "market_move" and r["direction"] == 0
    assert r["confidence"] <= 0.5


def test_no_spurious_signal_from_routine_or_non_crypto_releases():
    rc = RuleClassifier()
    fed = rc.classify("Federal Reserve Board announces approval of application by Isabella Bank Corporation", "", POOL)
    assert fed["direction"] == 0 and fed["importance"] < 0.4  # bank paperwork is not monetary policy
    sec = rc.classify("SEC Seeks Final Judgment Against Former Western Asset Co-CIO in Cherry-Picking Case", "", POOL)
    assert sec["category"] == "regulation" and sec["assets"] == [] and sec["direction"] == 0  # not crypto-wide
    ripple = rc.classify("SEC sues Ripple over XRP sales", "", POOL)  # a named asset is crypto context
    assert ripple["assets"] == ["XRP"] and ripple["direction"] == -1 and not ripple["data"].get("no_crypto_context")
    report = rc.classify("📉 📉 Bitcoin drops to $82,734, erasing October gains ahead of CPI data", "", POOL)
    assert report["category"] == "market_move" and report["direction"] == 0
    assert rc.classify("Fed hikes rates by 25 bps", "", POOL)["direction"] == -1
    assert rc.classify("Fed approves bank merger after review", "", POOL)["direction"] == 0  # only rate words count


def test_negation_makes_word_direction_unclear():
    rc = RuleClassifier()
    r = rc.classify("Powell: rate cuts are not on the table", "", POOL)
    assert r["direction"] == 0 and "отрицание" in r["reason"] and r["confidence"] <= 0.5
    # a category sign is not affected by a negation elsewhere in the headline
    assert rc.classify("Exchange XYZ hacked, says user funds not affected", "", POOL)["direction"] == -1


def test_unclassifiable_is_neutral_and_low_confidence():
    r = RuleClassifier().classify("Weekly community quiz: winners announced", "", [])
    assert r["category"] == "other" and r["direction"] == 0
    assert r["confidence"] <= 0.2 and r["importance"] <= 0.2
    assert "ни одно правило" in r["reason"]


def test_url_only_or_hashtag_only_post_returns_empty():
    rc = RuleClassifier()
    assert rc.classify("", "https://example.com/x", []) == {}
    assert rc.classify("", "#btc #крипто\nhttps://t.me/x", []) == {}


def test_hedge_reduces_importance_and_confidence():
    rc = RuleClassifier()
    firm = rc.classify("SEC sues Binance over unregistered securities", "", [])
    hedged = rc.classify("SEC reportedly may sue Binance over unregistered securities", "", [])
    assert firm["category"] == hedged["category"] == "regulation"
    assert hedged["importance"] == pytest.approx(firm["importance"] - 0.1)
    assert hedged["confidence"] < firm["confidence"]
    assert hedged["data"].get("hedge") is True


def test_category_sign_and_word_direction():
    rc = RuleClassifier()
    assert rc.classify("Bybit hacked: $1.5 billion in ETH stolen from cold wallet", "", POOL)["direction"] == -1
    assert rc.classify("Binance will list Solana perpetuals", "", POOL)["direction"] == +1
    etf = rc.classify("SEC approves first U.S. spot bitcoin ETFs", "", POOL)
    assert etf["category"] == "etf" and etf["direction"] == +1 and etf["speed"] == "seconds"
    assert etf["importance"] >= 0.85


def test_inflation_surprise_sets_direction_labour_does_not():
    rc = RuleClassifier()
    hot = rc.classify("U.S. CPI: +3.8% YEAR-OVER-YEAR (EST. +3.7%)", "", [])
    assert hot["direction"] == -1 and hot["assets"] == ["*"] and hot["data"]["macro"]["surprise"] == 1
    cool = rc.classify("US CPI ACTUAL 0.1% (FORECAST 0.3%, PREVIOUS 0.4%)", "", [])
    assert cool["direction"] == +1
    claims = rc.classify("US INITIAL JOBLESS CLAIMS ACTUAL 230K (FORECAST 200K, PREVIOUS 197K)", "", [])
    assert claims["direction"] == 0 and claims["data"]["macro"]["surprise"] == 1


def test_assets_by_names_tickers_and_pool():
    assert extract_assets("Ethereum and Solana lead the rally; $DOGE follows", None) == ["DOGE", "ETH", "SOL"]
    # bare pool tickers count only when unambiguous and the text is not all caps
    assert extract_assets("Whales add AAVE and XRP to their bags", POOL) == ["XRP", "AAVE"]
    assert extract_assets("Click the link to read more about ADA Lovelace", POOL) == []
    assert extract_assets("Chainlink price feeds go live", POOL) == ["LINK"]
    # Bitcoin Cash / Ethereum Classic do not leak into BTC / ETH
    assert extract_assets("Bitcoin Cash upgrade scheduled", None) == ["BCH"]
    assert extract_assets("Ethereum Classic network update", None) == ["ETC"]
    # stablecoins are not assets; pool restricts
    assert extract_assets("$USDT and $PEPE flows", POOL) == []


def test_market_wide_categories_use_star():
    rc = RuleClassifier()
    assert rc.classify("FED CUTS RATES BY 25 BPS TO 3.75%-4.00%", "", POOL)["assets"] == ["*"]
    assert rc.classify("Trump announces 100% tariffs on China starting November 1", "", POOL)["assets"] == ["*"]
    # a named crypto asset in a macro item stays specific
    r = rc.classify("Powell: no view on bitcoin reserve", "", POOL)
    assert r["category"] == "fed_rates" and r["assets"] == ["BTC"]


def test_listing_assets_and_pool_restriction():
    rc = RuleClassifier()
    r = rc.classify("UPBIT LISTING: 폰즈(PONS) 신규 거래지원 안내 (KRW, BTC, USDT 마켓)", "", POOL)
    assert r["category"] == "listing" and r["assets"] == []  # not a pool asset
    assert "PONS" in r["data"]["mentioned"]
    r2 = rc.classify("Coinbase will list Aave (AAVE) perpetuals", "", POOL)
    assert r2["assets"] == ["AAVE"] and r2["direction"] == 1


def test_whale_transfers_direction():
    rc = RuleClassifier()
    to_ex = rc.classify("", "🚨 2,050 $BTC (170,834,648 USD) transferred from unknown wallet to Coinbase Institutional\nDetails",
                        POOL)
    assert to_ex["category"] == "whale_transfer" and to_ex["direction"] == -1 and to_ex["assets"] == ["BTC"]
    assert to_ex["data"]["whale"]["flow"] == "to_exchange"
    mint = rc.classify("", "💵 250,000,000 $USDC (250,092,500 USD) minted at USDC Treasury\nDetails", POOL)
    assert mint["direction"] == 1 and mint["assets"] == ["*"]


def test_old_hack_conviction_is_regulation_low_importance():
    r = RuleClassifier().classify("Uranium Finance Hacker Who Bought Pokémon Cards Convicted Over $50M Theft", "", [])
    assert r["category"] == "regulation" and r["importance"] <= 0.3 and r["speed"] == "hours"


def test_headline_cleaning_and_dedup_helpers():
    assert headline("❗️🇺🇸#сша #экономика\nСША - Initial Jobless Claims = 197 тыс") == "США - Initial Jobless Claims = 197 тыс"
    assert headline("JUST IN: Robinhood adds $25,000,000 of Bitcoin.\n\n@WatcherGuru") == \
        "JUST IN: Robinhood adds $25,000,000 of Bitcoin."
    assert headline("Effective Fed Funds Rate 3.88%|FJ") == "Effective Fed Funds Rate 3.88%"
    assert headline("AggrNews: Binance lists XYZ") == "Binance lists XYZ"
    assert dedup_key("BREAKING: SEC approves spot ETH ETFs 🚨") == dedup_key("SEC approves spot ETH ETFs")
    assert similarity("Greece prepares to levy 10% crypto capital gains tax",
                      "Greece plans 10% crypto capital gains tax") >= 0.5
    assert similarity("BoE Gov. Bailey: inflation to target", "Fed's Waller: rate cuts ahead") < 0.5


def test_macro_surprise_formats():
    fj = macro_surprise("❗ US INITIAL JOBLESS CLAIMS ACTUAL 197K (FORECAST 200K, PREVIOUS 197K) $MACRO|FJ")
    assert fj == {"event_type": "JOBLESS_CLAIMS", "actual": 197000.0, "forecast": 200000.0, "surprise": -1}
    tree = macro_surprise("U.S. CPI: +3.8% YEAR-OVER-YEAR (EST. +3.7%)")
    assert tree["event_type"] == "CPI" and tree["actual"] == pytest.approx(3.8) and tree["surprise"] == 1
    walter = macro_surprise("US JOBLESS CLAIMS -2K TO 197K IN OCT-3 WK; SURVEY 200K")
    assert walter["actual"] == 197000.0 and walter["forecast"] == 200000.0 and walter["surprise"] == -1
    ru = macro_surprise("США - Initial Jobless Claims = 197 тыс (ожид 200 тыс / ранее 197 тыс)")
    assert ru["event_type"] == "JOBLESS_CLAIMS" and ru["actual"] == 197000.0 and ru["surprise"] == -1
    inline = macro_surprise("U.S. CPI: +3.4% YEAR-OVER-YEAR (EST. +3.4%)")
    assert inline["surprise"] == 0
    assert macro_surprise("US CONTINUED JOBLESS CLAIMS ACTUAL 1.716M (FORECAST 1.7M, PREVIOUS 1.701M)")["actual"] == \
        pytest.approx(1_716_000.0)
    assert macro_surprise("Bitcoin rallies after the Fed meeting") is None
    assert macro_surprise("") is None


def test_parse_whale_alert_flows():
    w = parse_whale_alert("🚨 🚨 973 $BTC (81,197,515 USD) transferred from unknown wallet to Coinbase Institutional\nDetails")
    assert w == {"amount": 973.0, "asset": "BTC", "usd": 81197515.0, "from": "unknown wallet",
                 "to": "Coinbase Institutional", "flow": "to_exchange"}
    assert parse_whale_alert("975 $BTC (80,759,839 USD) transferred from #Kraken to unknown wallet\nDetails")["flow"] == \
        "from_exchange"
    assert parse_whale_alert("3,000 $BTC (249,390,131 USD) transferred from unknown wallet to unknown wallet")["flow"] == "other"
    assert parse_whale_alert("133,797,325 $USDC (133,837,464 USD) transferred from #Aave to Unknown Whale 1")["flow"] == "other"
    assert parse_whale_alert("5,000 $ETH (12,000,000 USD) transferred from #Binance to #OKX\nDetails")["flow"] == \
        "exchange_to_exchange"
    mint = parse_whale_alert("💵 250,000,000 $USDC (250,092,500 USD) minted at USDC Treasury\nDetails")
    assert mint["flow"] == "mint" and mint["from"] is None and mint["to"] == "USDC Treasury"
    burn = parse_whale_alert("🔥 100,000,000 $USDT (100,010,000 USD) burned at Tether Treasury\nDetails")
    assert burn["flow"] == "burn" and burn["asset"] == "USDT"
    assert parse_whale_alert("📉 📉 📉 Bitcoin drops to $82,734 ahead of CPI data\nRead Analysis") is None


# --------------------------------------------------------------------------- #
# Claude (fake client - never the real API)
# --------------------------------------------------------------------------- #
class _Block:
    def __init__(self, type_: str, text: str = "") -> None:
        self.type = type_
        self.text = text


class _Resp:
    def __init__(self, text: str, stop_reason: str = "end_turn") -> None:
        self.content = [_Block("thinking", ""), _Block("text", text)]
        self.stop_reason = stop_reason


class FakeMessages:
    def __init__(self, answers):
        self.answers = list(answers)
        self.calls: list[dict] = []

    async def create(self, **kw):
        self.calls.append(kw)
        a = self.answers.pop(0) if self.answers else _Resp("{}")
        if isinstance(a, BaseException):
            raise a
        if callable(a):
            return await a()
        return a


class FakeClient:
    def __init__(self, answers):
        self.messages = FakeMessages(answers)


def _answer(**kw) -> _Resp:
    d = {"category": "fed_rates", "assets": ["*"], "direction": 1, "importance": 0.7, "speed": "minutes",
         "confidence": 0.8, "reason": "вероятность повышения ставки упала: голубиный сигнал"}
    d.update(kw)
    return _Resp(json.dumps(d, ensure_ascii=False))


async def test_claude_request_shape_and_parsing():
    client = FakeClient([_answer(importance=1.7, confidence=-2, assets=["btc", "PEPE", "*"])])
    clock = SimClock()
    cc = ClaudeClassifier(_settings(analysis_model="claude-haiku-5-5"), clock, client=client)
    res = await cc.classify("Rate hike odds collapse to 19%", "text", POOL, source="telegram:markettwits",
                            published_ts=1_791_462_629.0)
    assert res == {"category": "fed_rates", "assets": ["*"], "direction": 1, "importance": 1.0, "speed": "minutes",
                   "confidence": 0.0, "reason": "вероятность повышения ставки упала: голубиный сигнал"}
    call = client.messages.calls[0]
    assert call["model"] == "claude-haiku-5-5"
    assert call["output_config"]["effort"] == "low"
    assert call["output_config"]["format"] == {"type": "json_schema", "schema": CLAUDE_SCHEMA}
    assert call["system"][0]["cache_control"] == {"type": "ephemeral"}
    user = call["messages"][0]["content"]
    assert "POOL: AAVE, ADA, BCH, BTC" in user and "HEADLINE: Rate hike odds collapse to 19%" in user
    assert "PUBLISHED_UTC: 2026-" in user and "SOURCE: telegram:markettwits" in user
    assert "temperature" not in call and "thinking" not in call


async def test_claude_failures_return_none():
    clock = SimClock()
    answers = [_Resp("not json at all"), _Resp("", stop_reason="refusal"), _Resp("{}", stop_reason="max_tokens"),
               RuntimeError("boom"), _answer(category="price_action"), _Resp('```json\n{"category": "x"}\n```')]
    cc = ClaudeClassifier(_settings(), clock, client=FakeClient(answers))
    for _ in range(len(answers)):
        assert await cc.classify("Some headline", "", POOL) is None
    assert cc.calls_total == len(answers) and cc.ok_total == 0


async def test_claude_direction_without_assets_becomes_zero():
    cc = ClaudeClassifier(_settings(), SimClock(), client=FakeClient([_answer(assets=["PEPE"], direction=-1)]))
    res = await cc.classify("PEPE whale dumps", "", POOL)
    assert res["assets"] == [] and res["direction"] == 0


async def test_claude_timeout_is_none_without_retry():
    async def slow():
        await asyncio.sleep(5)
        return _answer()

    cc = ClaudeClassifier(_settings(news_claude_timeout_s=0.05), SimClock(), client=FakeClient([slow]))
    assert await cc.classify("Headline", "", POOL) is None
    assert cc.calls_total == 1


async def test_claude_budget_per_rolling_hour():
    clock = SimClock()
    client = FakeClient([_answer() for _ in range(5)])
    cc = ClaudeClassifier(_settings(news_claude_max_per_hour=2), clock, client=client)
    assert await cc.classify("a", "", POOL) is not None
    assert await cc.classify("b", "", POOL) is not None
    assert await cc.classify("c", "", POOL) is None  # budget exhausted: no API call
    assert len(client.messages.calls) == 2
    clock.advance(3601)
    assert await cc.classify("d", "", POOL) is not None


async def test_claude_rate_limit_holds_further_calls():
    class RateLimited(Exception):
        status_code = 429
        response = SimpleNamespace(headers={"retry-after": "120"})

    clock = SimClock()
    client = FakeClient([RateLimited("slow down"), _answer()])
    cc = ClaudeClassifier(_settings(), clock, client=client)
    assert await cc.classify("a", "", POOL) is None
    assert not cc.can_call()
    clock.advance(121)
    assert await cc.classify("b", "", POOL) is not None


async def test_claude_client_created_lazily_from_key(monkeypatch, caplog):
    import anthropic

    created = {}

    class FakeAsyncAnthropic:
        def __init__(self, **kw):
            created.update(kw)
            self.messages = FakeMessages([RuntimeError("network down")])

    monkeypatch.setattr(anthropic, "AsyncAnthropic", FakeAsyncAnthropic)
    key = "sk-ant-test-SECRET-123"
    cc = ClaudeClassifier(_settings(), SimClock(), api_key=key)
    assert created == {}  # lazy: nothing created before the first call
    with caplog.at_level("DEBUG"):
        assert await cc.classify("a", "", POOL) is None
    assert created["api_key"] == key and created["max_retries"] == 0
    assert key not in caplog.text and key not in json.dumps(cc.snapshot())


def test_parse_json_answer_is_defensive():
    assert parse_json_answer('{"a": 1}') == {"a": 1}
    assert parse_json_answer('```json\n{"a": 2}\n```') == {"a": 2}
    assert parse_json_answer('Here you go: {"a": 3} thanks') == {"a": 3}
    assert parse_json_answer("[1, 2]") is None
    assert parse_json_answer("") is None


# --------------------------------------------------------------------------- #
# HeadlineClassifier
# --------------------------------------------------------------------------- #
async def test_headline_classifier_rules_only_without_key():
    clock = SimClock(wall=1_700_000_000.0)
    hc = HeadlineClassifier(_settings(), clock)
    it = await hc.classify(_item("SEC approves first U.S. spot bitcoin ETFs", ts=clock.wall()), POOL)
    assert it.classifier == "rules" and it.category == "etf" and it.direction == 1 and it.assets == ["BTC"]
    assert hc.claude is None and hc.snapshot()["claude"]["available"] is False


async def test_headline_classifier_claude_refines_negation():
    clock = SimClock(wall=1_700_000_000.0)
    client = FakeClient([_answer()])
    hc = HeadlineClassifier(_settings(), clock, client=client)
    title = "Оценка игроками вероятности повышения ставки ФРС на заседании 28 октября рухнула - до 19%"
    it = await hc.classify(_item(title, ts=clock.wall()), POOL)
    assert it.data["rules"]["direction"] == -1  # the rule result is kept for transparency
    # rules (-1) and Claude (+1) disagree -> no direction (no fake signal)
    assert it.classifier == "claude" and it.direction == 0 and it.confidence <= 0.5
    assert "расходятся" in it.reason


async def test_headline_classifier_claude_fills_unclear_direction():
    clock = SimClock(wall=1_700_000_000.0)
    hc = HeadlineClassifier(_settings(), clock, client=FakeClient([_answer(category="etf", assets=["BTC"],
                                                                                  direction=1, importance=0.8)]))
    it = await hc.classify(_item("BlackRock spot bitcoin ETF sees record inflows", ts=clock.wall()), POOL)
    assert it.classifier == "claude" and it.direction == 1 and it.importance == 0.8 and it.assets == ["BTC"]
    assert it.data["classified"]["importance"] == 0.8


async def test_headline_classifier_gates_claude():
    clock = SimClock(wall=1_700_000_000.0)
    client = FakeClient([_answer() for _ in range(3)])
    hc = HeadlineClassifier(_settings(), clock, client=client)
    # low rule importance -> no Claude call
    low = await hc.classify(_item("Weekly community quiz: winners announced", ts=clock.wall()), POOL)
    assert low.classifier == "rules"
    # too old -> no Claude call
    old = await hc.classify(_item("SEC sues Binance", ts=clock.wall() - 7200), POOL)
    assert old.classifier == "rules"
    # no crypto context -> no Claude call
    eq = await hc.classify(_item("SEC charges hedge fund adviser with cherry-picking trades", ts=clock.wall()), POOL)
    assert eq.classifier == "rules" and eq.data.get("no_crypto_context")
    assert client.messages.calls == []
    # disabled in settings -> no Claude call even with a key
    off = HeadlineClassifier(_settings(news_claude_enabled=False), clock, client=client)
    it = await off.classify(_item("SEC sues Binance", ts=clock.wall()), POOL)
    assert it.classifier == "rules" and client.messages.calls == []


async def test_headline_classifier_keeps_rules_when_claude_fails():
    clock = SimClock(wall=1_700_000_000.0)
    hc = HeadlineClassifier(_settings(), clock, client=FakeClient([RuntimeError("down")]))
    it = await hc.classify(_item("Bybit hacked: $1.5 billion in ETH stolen", ts=clock.wall()), POOL)
    assert it.classifier == "rules" and it.category == "hack" and it.direction == -1
