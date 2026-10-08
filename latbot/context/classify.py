"""Headline classification for the news feed (ARCHITECTURE P3.5 / P3.12).

* ``RuleClassifier``     - stdlib rules, always on.  Ordered first-match category
  rules on the HEADLINE (bodies are noisy), a base importance / reaction speed
  per category, direction from the category sign or bull/bear words, price
  reports forced to direction 0, hedges ("reportedly", "may") cut importance
  and confidence.  Assets by cashtags, pool tickers AND project names; macro /
  Fed / geopolitics are market-wide (``["*"]``).
* ``ClaudeClassifier``   - optional refinement through the existing ``anthropic``
  SDK (JSON-only answer, hourly budget).  Any failure -> ``None`` and the rule
  result stays.  The API key is never logged.
* ``HeadlineClassifier`` - rules always; Claude refines when enabled + key.

Rule weights are PRIORS, not fitted to past trades (CLAUDE.md rule 10): how far
and how fast the market really reacts is measured by ``context.reactions``.
A classified item is a factor input, never a BUY/SELL by itself.

Text parsers shared with ``context.news``: ``headline``, ``dedup_title``,
``dedup_key``, ``similarity``, ``macro_surprise``, ``parse_whale_alert``.
"""
from __future__ import annotations

import asyncio
import collections
import hashlib
import json
import logging
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any

from .base import NEWS_CATEGORIES, NEWS_SPEEDS, NewsItem, normalise_title

log = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# text cleaning / headline / dedup
# --------------------------------------------------------------------------- #
_CHANNEL_TAILS = [re.compile(p, re.I) for p in (
    r"@\w+\s*$",  # "@WatcherGuru" footer
    r"\|\s*FJ\s*$",  # FinancialJuice suffix
    r"[—-]\s*link\s*(?:\|\s*AI comment)?\s*$",  # unfolded / WuBlockchain
    r"\bRead Analysis\s*$", r"\bDetails\s*$",  # Whale Alert
    r"News\s*\|\s*Markets\s*\|\s*YouTube\s*$",  # Cointelegraph TG footer
    r"\(@\w+\)\s*$",  # "(@WalterBloomberg)"
)]
_FLAG_RE = re.compile("[\U0001F1E6-\U0001F1FF]{2}")
_EMOJI_RE = re.compile("[\U0001F000-\U0001FAFF☀-➿⬀-⯿️‍]+")
_PREFIX_RE = re.compile(
    r"^\s*(?:\*+\s*)?(?:BREAKING|JUST IN|URGENT|FLASH|ALERT|UPDATE|СРОЧНО|МОЛНИЯ)\s*[:!\-–—]\s*", re.I)
_HEADLINE_FLAG_RE = re.compile(r"\b(?:BREAKING|JUST IN|URGENT|FLASH)\b|СРОЧНО|МОЛНИЯ|❗|🚨", re.I)
_SOURCE_PREFIX_RE = re.compile(
    r"^\s*(?:AggrNews|Binance EN|Bloomberg|Reuters|WSJ|FT|CNBC|AFP|Tree News)\s*:\s*", re.I)
_URL_RE = re.compile(r"https?://\S+")
_TAG_RE = re.compile(r"#(\w+)")


def clean_text(text: str) -> str:
    """NFKC + channel footers removed ("@WatcherGuru", "|FJ", "Details"...)."""
    t = unicodedata.normalize("NFKC", text or "").strip()
    for rx in _CHANNEL_TAILS:
        t = rx.sub("", t.strip())
    return t.strip()


def _is_noise_line(line: str) -> bool:
    """Hashtag-only ("#btc #крипто"), emoji-only or URL-only lines."""
    t = _FLAG_RE.sub(" ", _EMOJI_RE.sub(" ", line))
    t = _URL_RE.sub(" ", t)
    t = _TAG_RE.sub(" ", t)
    return len(re.sub(r"[\W_]+", "", t)) < 3


def hashtags(text: str) -> list[str]:
    return list(dict.fromkeys(h.lower() for h in _TAG_RE.findall(text or "")))


def headline(text: str) -> str:
    """First meaningful line of a Telegram post / RSS title, cleaned ("" when none)."""
    t = clean_text(text)
    first = next((ln for ln in t.splitlines() if ln.strip() and not _is_noise_line(ln)), "")
    return _SOURCE_PREFIX_RE.sub("", first.strip()).strip()


def dedup_title(title: str) -> str:
    """Normalised headline for de-duplication across sources: flags/emoji,
    BREAKING/JUST IN prefixes, URLs, @/# tags and thousands separators removed,
    then ``base.normalise_title``."""
    t = unicodedata.normalize("NFKC", title or "")
    t = _FLAG_RE.sub(" ", t)
    t = _EMOJI_RE.sub(" ", t)
    t = _PREFIX_RE.sub("", t)
    t = _URL_RE.sub(" ", t)
    t = re.sub(r"[@#]\w+", " ", t)
    t = re.sub(r"(?<=\d),(?=\d{3}\b)", "", t)  # 3,100,000 -> 3100000
    return normalise_title(t)


def dedup_key(title: str) -> str:
    """sha1 of the normalised headline (16 hex chars)."""
    return hashlib.sha1(dedup_title(title).encode("utf-8")).hexdigest()[:16]


_STOP = frozenset(
    "the a an of to in on for and or is are was were be by with at from as its it this that will has have "
    "after over into than amid says said".split())


def tokens(title: str) -> frozenset[str]:
    """Content tokens of a headline (for the Jaccard similarity)."""
    return frozenset(w.strip(".") for w in dedup_title(title).split()
                     if w.strip(".") and w.strip(".") not in _STOP and len(w.strip(".")) > 1)


def jaccard(a: frozenset[str] | set[str], b: frozenset[str] | set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def similarity(a: str, b: str) -> float:
    """Token Jaccard of two headlines (0..1).  >= 0.5 within 6 h == same story
    (no false match on 682 real cross-source headlines; 0.4-0.5 has false matches)."""
    return jaccard(tokens(a), tokens(b))


# --------------------------------------------------------------------------- #
# assets
# --------------------------------------------------------------------------- #
# project names / unambiguous tickers -> ticker.  Ambiguous English words (link, near,
# op, ton, dot, one, gas, sui, sand...) only via the project name or a $CASHTAG.
ASSET_ALIASES: dict[str, str] = {
    r"\bbitcoin\b(?! cash)|\bbtc\b|биткоин|биткойн|\bбтк\b": "BTC",
    r"\bethereum\b(?! classic)|\bether\b|\beth\b|эфириум|\bэфир": "ETH",
    r"\bethereum classic\b": "ETC",
    r"\bsolana\b|\bsol\b|солан": "SOL",
    r"\bxrp\b|\bripple\b|рипл": "XRP",
    r"\bbnb\b|binance coin": "BNB",
    r"\bdogecoin\b|\bdoge\b|догикоин": "DOGE",
    r"\bcardano\b": "ADA",
    r"\bchainlink\b": "LINK",
    r"\bavalanche\b|\bavax\b": "AVAX",
    r"\bpolkadot\b": "DOT",
    r"\blitecoin\b|\bltc\b": "LTC",
    r"\btron\b|\btrx\b": "TRX",
    r"\btoncoin\b|\bton network\b|\bthe open network\b": "TON",
    r"\bsui network\b|\bsui\b(?= (?:blockchain|token|price|network|foundation|ecosystem))": "SUI",
    r"\baptos\b": "APT",
    r"\barbitrum\b": "ARB",
    r"\boptimism\b": "OP",
    r"\bhyperliquid\b": "HYPE",
    r"\bstellar\b|\bxlm\b": "XLM",
    r"\bhedera\b|\bhbar\b": "HBAR",
    r"\buniswap\b": "UNI",
    r"\baave\b": "AAVE",
    r"\bnear protocol\b": "NEAR",
    r"\bbitcoin cash\b|\bbch\b": "BCH",
    r"\brender network\b|\brender token\b": "RENDER",
    r"\balgorand\b": "ALGO",
    r"\bcosmos (?:hub|network|ecosystem)\b": "ATOM",
    r"\bpolygon\b|\bmatic\b": "POL",
    r"\bcurve finance\b|\bcurve dao\b": "CRV",
    r"\bbittensor\b": "TAO",
    r"\bondo finance\b": "ONDO",
    r"\binjective\b": "INJ",
}
_ALIAS_RES = [(re.compile(p), t) for p, t in ASSET_ALIASES.items()]
_KNOWN_TICKERS = frozenset(ASSET_ALIASES.values())
_CASHTAG_RE = re.compile(r"(?<![\w$])\$([A-Z][A-Z0-9]{1,9})\b")  # $BTC, not $3,100
STABLES = frozenset({"USDT", "USDC", "DAI", "FDUSD", "USDE", "PYUSD", "RLUSD", "USD1", "TUSD", "USDS"})
# bare uppercase pool tickers that are also common words / names: never matched bare
AMBIGUOUS_TICKERS = frozenset({
    "LINK", "NEAR", "OP", "TON", "DOT", "ONE", "GAS", "SUI", "SAND", "ETC", "ADA", "UNI", "ALGO", "ATOM",
    "POL", "APT", "TAO", "INJ", "CRV", "ONDO", "RENDER", "HYPE", "ARB", "AI", "B", "M", "LUNA", "IT", "US",
    "ME", "SO", "AT", "ON", "GO", "UP", "HOLD", "SAFE", "REAL", "TRUMP", "MOVE", "S", "T", "W", "G",
})
_WORD_RE = re.compile(r"(?<![\w$])([A-Z][A-Z0-9]{1,9})(?![\w])")


def _mostly_upper(text: str) -> bool:
    letters = [c for c in text if c.isalpha()]
    if len(letters) < 12:
        return False
    return sum(1 for c in letters if c.isupper()) / len(letters) >= 0.6


def extract_assets(text: str, pool: list[str] | None = None) -> list[str]:
    """Tickers mentioned in ``text``: cashtags, project names, and (when a pool is
    given) bare uppercase pool tickers that are not ambiguous words.  Stablecoins
    are dropped.  With ``pool`` the result is restricted to it."""
    pool_set = {p.upper() for p in pool} if pool else None
    found: list[str] = [m.group(1) for m in _CASHTAG_RE.finditer(text or "")]
    low = (text or "").lower()
    for rx, tic in _ALIAS_RES:
        if rx.search(low):
            found.append(tic)
    if pool_set and not _mostly_upper(text or ""):
        for m in _WORD_RE.finditer(text or ""):
            t = m.group(1)
            if t in pool_set and t not in AMBIGUOUS_TICKERS:
                found.append(t)
    out: list[str] = []
    for f in found:
        if f in STABLES or f in out:
            continue
        if pool_set is not None and f not in pool_set:
            continue
        out.append(f)
    return out


# --------------------------------------------------------------------------- #
# categories (ordered: first match wins) - category, pattern, base importance, speed
# --------------------------------------------------------------------------- #
_CATEGORY_RULES_SRC: list[tuple[str, str, float, str]] = [
    # a price report ("Bitcoin drops to $82,734 ... ahead of CPI") is market_move whatever else it mentions
    ("market_move", r"^(?:bitcoin|btc|ether(?:eum)?|eth|solana|sol|xrp|crypto(?: market)?|биткоин)\b.{0,25}?"
                    r"\b(?:surges?|soars?|jumps?|rall(?:y|ies)|plunges?|plummets?|tumbles?|crash(?:es)?|drops?|falls?|"
                    r"slides?|dips?|spikes?|rises?|climbs?|breaks? (?:below|above)|reclaims?|hits?)\b.{0,15}?\$\s?\d",
     0.25, "minutes"),
    # routine Fed board business (bank applications, enforcement, comment periods) is not monetary policy
    ("fed_rates", r"application (?:by|from)|enforcement action|comment period|termination of (?:an )?enforcement|"
                  r"announces approval of", 0.1, "unknown"),
    # legal aftermath of an old hack is NOT a fresh hack
    ("regulation", r"(?:hacker|exploiter|thief).{0,60}(?:convicted|sentenced|arrested|extradited|pleads?|charged|jailed)|"
                   r"(?:convicted|sentenced|arrested|extradited|charged).{0,60}(?:hack|exploit)", 0.3, "hours"),
    ("hack", r"\bhack(?:ed|er|ers|s)?\b|exploit(?:ed)?\b|drain(?:ed)?\b|stolen|breach|compromised|rug ?pull|"
             r"взлом|хакер|украл|похищ|эксплойт", 0.75, "seconds"),
    # exchange stress (insolvency, withdrawals halted, outage)
    ("exchange", r"(?:halt|suspend|pause|freez)\w* (?:all )?(?:withdrawal|deposit|trading)|insolven|bankrupt|"
                 r"chapter 11|outage|down for|proof of reserves|liquidity (?:crunch|crisis)|bank run|"
                 r"withdrawal (?:surge|spike|run)|приостанов\w* вывод|банкрот|неплатеж", 0.7, "minutes"),
    # delisting before listing ("delist" contains "list")
    ("delisting", r"\bdelist\w*|will remove|removal of (?:spot )?trading|делистинг|исключ\w* из листинга", 0.65, "minutes"),
    ("listing", r"\b(?:will )?list(?:s|ing|ed)?\b|new listing|adds? .{0,30}(?:trading|spot|perpetual|market)|"
                r"launchpool|launchpad|roadmap|листинг|добав\w* .{0,20}(?:торг|пар)", 0.6, "seconds"),
    # Fed / rates (before macro_data)
    ("fed_rates", r"effective fed funds|fed funds rate \d|repo operation|reverse repo", 0.1, "unknown"),  # routine prints
    ("fed_rates", r"(?:fed|fomc|federal reserve).{0,40}(?:cuts?|raises?|hikes?|holds?|keeps?|leaves?|lowers?)\b.{0,30}rates?|"
                  r"rate decision|fomc statement|dot plot|"
                  r"(?:фрс|фед).{0,30}(?:снизил|повысил|сохранил|оставил)\w*.{0,20}ставк", 0.85, "seconds"),
    ("fed_rates", r"\bpowell\b|\bfed chair|пауэлл", 0.6, "seconds"),
    ("fed_rates", r"\bfomc\b|\bfed(?:eral reserve)?\b|warsh|waller|jefferson|bowman|\bbarr\b|logan|goolsbee|"
                  r"musalem|hammack|kashkari|daly|schmid|williams|rate cuts?|rate hikes?|basis points|\bbps\b|фрс|"
                  r"ключев\w* ставк|процентн\w* ставк|ставк\w* (?:фрс|цб|ецб)|(?:фрс|цб|ецб|банк).{0,30}ставк", 0.45, "minutes"),
    ("fed_rates", r"\becb\b|lagarde|\bboj\b|ueda|\bboe\b|bailey|\bpboc\b|ецб|банк японии|банк англии", 0.3, "minutes"),
    ("macro_data", r"\bcpi\b|\bppi\b|\bpce\b|inflation|nonfarm|non-farm|payrolls|\bnfp\b|jobless claims|"
                   r"unemployment rate|\bgdp\b|retail sales|\bism\b|\bjolts\b|consumer (?:confidence|sentiment)|"
                   r"инфляц|ипц|безработиц|нонфарм|ввп|розничн\w* продаж", 0.7, "seconds"),
    ("etf", r"\betfs?\b|\betps?\b|\bibit\b|\bfbtc\b|\bgbtc\b|\betha\b|spot (?:bitcoin|ether|solana|xrp) funds?|"
            r"(?:fund|etf|etp)s?\W.{0,40}(?:in|out)flows?|(?:in|out)flows?.{0,40}\b(?:etf|etp|fund)s?\b|"
            r"биржев\w* фонд|\bетф\b", 0.6, "minutes"),
    ("regulation", r"\bsec\b|\bcftc\b|\bdoj\b|\bofac\b|\besma\b|\bmica\b|\bfca\b|regulat|lawsuit|\bsues?\b|"
                   r"charged?|indict|settle(?:ment|s|d)?\b|\bban(?:s|ned)?\b|crackdown|sanction|legislat|bill\b|"
                   r"senate|congress|executive order|stablecoin (?:law|act|rules)|genius act|clarity act|"
                   r"tax\b|kyc|aml\b|license|licence|approv|court|judge|ruling|"
                   r"регулир|запрет|санкц|закон|суд|иск\b|налог|лиценз|цб рф|минфин|госдум", 0.55, "minutes"),
    ("geopolitics", r"\bwar\b|missile|airstrike|strikes? on|attack|invasion|invade|troops|military|nuclear|"
                    r"ceasefire|truce|peace (?:deal|talks|plan)|tariff|trade war|embargo|sanctions? on|"
                    r"войн|ракет|удар|атак|вторжен|перемир|мирн\w* (?:сделк|переговор)|пошлин", 0.6, "seconds"),
    ("liquidation", r"liquidat|ликвидац", 0.5, "minutes"),
    ("whale_transfer", r"transferred from|minted at|burned at|whale|кит\w*\b", 0.35, "minutes"),
    ("unlock", r"\bunlock|token release|vesting|разблокир|анлок", 0.45, "hours"),
    ("upgrade", r"upgrade|hard ?fork|mainnet|testnet|network update|protocol update|обновлени|хардфорк", 0.4, "hours"),
    ("partnership", r"partner(?:s|ship)?\b|collaborat|integrat|teams up|joins forces|acquir|acquisition|merger|"
                    r"партнерств|партнёрств|сотруднич|поглощ", 0.35, "hours"),
    ("market_move", r"(?:surges?|soars?|jumps?|rall(?:y|ies)|plunges?|plummets?|tumbles?|crash(?:es)?|"
                    r"drops?|falls?|slides?|dips?|spikes?)\b|all[- ]time high|\bath\b|[<>]\s*\$\s?\d|"
                    r"\bабсолютн\w* максимум|обвал|рост\b|падени", 0.25, "minutes"),
]
CATEGORY_RULES: list[tuple[str, re.Pattern[str], float, str]] = [
    (c, re.compile(p), imp, spd) for c, p, imp, spd in _CATEGORY_RULES_SRC
]

# direction words (relative to the asset / crypto market)
_BULL_RE = re.compile(
    r"\bapprov(?:e|es|ed|al)\b|green ?light|\bwins?\b|dismiss(?:es|ed)? (?:the )?(?:case|lawsuit)|"
    r"drops? (?:the )?(?:case|lawsuit|charges)|\bcuts? (?:interest )?rates?\b|rate cut|dovish|"
    r"record inflows?|inflows?\b|"
    r"ceasefire|truce|peace deal|tariff (?:pause|cut|relief|exemption|delay)|trade deal|"
    r"buys?\b|bought|accumulat|adopt|legal tender|strategic (?:bitcoin |crypto )?reserve|bitcoin reserve|"
    r"lists?\b|listing|launch(?:es|ed)? (?:spot|crypto)|"
    r"одобр|снизил\w*(?: \w+)? ставк|снижени\w*(?: \w+)? ставк|перемир|приток|покупа|купил")
_BEAR_RE = re.compile(
    r"\breject(?:s|ed|ion)?\b|\bden(?:y|ies|ied)\b|\bdelay(?:s|ed)?\b|\bsues?\b|sued|lawsuit|charged?|"
    r"\bban(?:s|ned)?\b|crackdown|sanction|\bhikes? rates?\b|rate hike|hawkish|outflows?\b|hack|exploit|stolen|"
    r"drain|insolven|bankrupt|halt|suspend|outage|delist|missile|attack|invasion|strikes? on|escalat|"
    r"tariffs? (?:on|of|hike)|impos\w* tariff|sell(?:s|ing)?\b|sold|dump|"
    r"отклон|запрет|иск\b|повысил\w*(?: \w+)? ставк|повышени\w*(?: \w+)? ставк|взлом|войн|ракет|удар|атак|"
    r"отток|продал|пошлин")
_LEAD_NOISE_RE = re.compile(r"^[^\w$#]+")
# rules cannot parse negation: a word-derived sign next to "not / never / rules out" is unclear
_NEGATION_RE = re.compile(r"\bnot\b|n't\b|\bnever\b|\brules? out\b|\bruled out\b|\bno (?:plans?|intention|rush|hurry)\b|"
                          r"\bне (?:будет|планиру|собира)|\bне \w+ет\b")
# for central-bank items only rate words carry a sign ("approval of a bank application" is not dovish)
_BULL_RATES_RE = re.compile(r"\bcuts? (?:interest )?rates?\b|rate cuts?\b|\blowers? rates?\b|dovish|"
                            r"снизил\w*(?: \w+)? ставк|снижени\w*(?: \w+)? ставк")
_BEAR_RATES_RE = re.compile(r"\b(?:hikes?|raises?) (?:interest )?rates?\b|rate hikes?\b|hawkish|"
                            r"повысил\w*(?: \w+)? ставк|повышени\w*(?: \w+)? ставк")
_HEDGE_RE = re.compile(
    r"\breportedly\b|\brumou?r|sources? (?:say|said)|\bcould\b|\bmay\b(?!\s+\d)|\bmight\b|consider|weighs?\b|"
    r"\bplans? to\b|\bexpected to\b|\?|по данным|возможно|рассматрива|планиру|слух")
_TIER1_RE = re.compile(
    r"binance|coinbase|\bsec\b|\bfed\b|powell|blackrock|\btrump\b|tether|circle|upbit|kraken|okx|bybit|"
    r"\bcme\b|treasury|white house|фрс|трамп")
_BIG_USD_RE = re.compile(
    r"\$\s?(\d+(?:[.,]\d+)?)\s*(billion|bn|b|million|mn|m|млрд|млн)\b|(\d[\d,]{8,})\s*usd", re.I)
_CRYPTO_CTX_RE = re.compile(
    r"crypto|bitcoin|ether|token|coin|defi|protocol|bridge|exchange|wallet|chain|stablecoin|\$[A-Z]{2,}|"
    r"binance|coinbase|kraken|okx|bybit|bitget|upbit|tether|usdt|usdc|circle|blockchain|digital asset|"
    r"крипт|бирж|кошел|блокчейн|цифров\w* актив", re.I)
# strict crypto context for regulation headlines ("Securities and Exchange Commission" is not crypto)
_CRYPTO_STRICT_RE = re.compile(
    r"crypto|bitcoin|\bether\b|ethereum|stablecoin|defi\b|blockchain|digital asset|\btokens?\b|tokeni[sz]|"
    r"\$[A-Z]{2,}|\bbtc\b|\beth\b|binance|coinbase|kraken|\bokx\b|bybit|tether|\busdt\b|\busdc\b|"
    r"крипт|блокчейн|биткоин|цифров\w* (?:актив|валют|рубл)", re.I)
_TOP_EXCHANGE_RE = re.compile(r"binance|coinbase|kraken|okx|bybit|bitget|upbit|bitfinex|kucoin|gate\.io|htx|crypto\.com")
_ETF_DECISION_RE = re.compile(r"\bapprov|\breject|\bden(?:y|ies|ied)\b|одобр|отклон")
_ETF_WORD_RE = re.compile(r"\betfs?\b|\betps?\b|spot")
_INFL_HOT_RE = re.compile(r"hotter|above (?:expectations|estimates|forecast)|выше (?:ожидан|прогноз)")
_INFL_COOL_RE = re.compile(r"cooler|softer|below (?:expectations|estimates|forecast)|ниже (?:ожидан|прогноз)")
_MACRO_INFL_RE = re.compile(r"\bcpi\b|\bppi\b|\bpce\b|inflation|инфляц|ипц")
_LISTING_PAREN_RE = re.compile(r"\(([A-Z][A-Z0-9]{1,9})\)")
_LISTING_QUOTES = frozenset({"KRW", "USDT", "USDC", "USD", "EUR", "BTC", "ETH", "TRY", "BRL"})

CATEGORY_SIGN = {"hack": -1, "exchange": -1, "delisting": -1, "listing": +1, "unlock": -1}
MARKET_WIDE_CATEGORIES = frozenset({"fed_rates", "macro_data", "geopolitics"})
INFLATION_EVENTS = frozenset({"CPI", "CORE_CPI", "PPI", "CORE_PPI", "PCE", "CORE_PCE"})


def _usd_amount(text: str) -> float:
    best = 0.0
    for m in _BIG_USD_RE.finditer(text):
        try:
            if m.group(3):
                v = float(m.group(3).replace(",", ""))
            else:
                v = float(m.group(1).replace(",", "."))
                unit = m.group(2).lower()
                v *= 1e9 if unit in ("billion", "bn", "b", "млрд") else 1e6
        except ValueError:
            continue
        best = max(best, v)
    return best


# --------------------------------------------------------------------------- #
# macro squawk: actual vs forecast
# --------------------------------------------------------------------------- #
_NUM = r"[-+]?\d[\d.,]*"
_UNIT = r"%|тыс|млн|млрд|[KMB](?![A-Za-zА-Яа-я])"
_SURPRISE_RES = [
    # FinancialJuice: "... ACTUAL 197K (FORECAST 200K, PREVIOUS 197K)"
    re.compile(rf"actual\s+(?P<a>{_NUM})\s*(?P<u>{_UNIT})?\s*\(\s*forecast\s+(?P<e>{_NUM})\s*(?P<eu>{_UNIT})?", re.I),
    # WalterBloomberg: "... -2K TO 197K IN OCT-3 WK; SURVEY 200K"
    re.compile(rf"\bto\s+(?P<a>{_NUM})\s*(?P<u>{_UNIT})?[^;\n]{{0,40}};\s*survey\s+(?P<e>{_NUM})\s*(?P<eu>{_UNIT})?", re.I),
    # MarketTwits (RU): "= 197 тыс (ожид 200 тыс / ранее 197 тыс)"
    re.compile(rf"=\s*(?P<a>{_NUM})\s*(?P<u>{_UNIT})?\s*\(\s*ожид\w*\.?\s+(?P<e>{_NUM})\s*(?P<eu>{_UNIT})?", re.I),
    # Tree / Bloomberg style: "+3.8% YEAR-OVER-YEAR (EST. +3.7%)"
    re.compile(rf"(?P<a>{_NUM})\s*(?P<u>{_UNIT})?[^()\n]{{0,40}}\((?:est\.?|exp\.?|survey|consensus|f/?c(?:ast)?\.?)\s*"
               rf"(?P<e>{_NUM})\s*(?P<eu>{_UNIT})?", re.I),
]
_MACRO_TYPE_RULES: list[tuple[str, re.Pattern[str]]] = [(t, re.compile(p, re.I)) for t, p in (
    ("CORE_CPI", r"core cpi|core consumer price|базов\w* (?:ипц|инфляц)"),
    ("CPI", r"\bcpi\b|consumer price|\bипц\b|потребительск\w* цен"),
    ("CORE_PPI", r"core ppi|core producer price"),
    ("PPI", r"\bppi\b|producer price|цен\w* производител"),
    ("CORE_PCE", r"core pce"),
    ("PCE", r"\bpce\b|personal consumption expenditure"),
    ("NFP", r"non-?farm|payrolls|\bnfp\b|нонфарм"),
    ("UNEMPLOYMENT", r"unemployment rate|безработиц"),
    ("JOBLESS_CLAIMS", r"jobless claims|initial claims|continu\w* claims|заявк\w* (?:на|по) пособи"),
    ("EARNINGS", r"hourly earnings|почасов\w* заработ"),
    ("GDP", r"\bgdp\b|\bввп\b"),
    ("RETAIL_SALES", r"retail sales|розничн\w* продаж"),
    ("ISM_MANUFACTURING", r"\bism\b.{0,20}manufactur"),
    ("ISM_SERVICES", r"\bism\b.{0,20}(?:services|non-?manufactur)"),
    ("JOLTS", r"\bjolts\b|job openings"),
    ("CONSUMER_SENTIMENT", r"consumer sentiment|michigan|consumer confidence"),
    ("FOMC_RATE", r"fed funds target|rate decision|interest rate decision"),
)]
_SCALE = {"k": 1e3, "тыс": 1e3, "m": 1e6, "млн": 1e6, "b": 1e9, "млрд": 1e9}


def macro_event_type(text: str) -> str:
    """Macro event type (``base.MACRO_EVENT_TYPES``) named in a squawk line, else "OTHER"."""
    for t, rx in _MACRO_TYPE_RULES:
        if rx.search(text or ""):
            return t
    return "OTHER"


def _num(raw: str, unit: str | None) -> float | None:
    s = raw.strip()
    if "," in s and "." not in s and re.fullmatch(r"[-+]?\d{1,3}(?:,\d{3})+", s):
        s = s.replace(",", "")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")  # "3,8" (RU decimal comma)
    else:
        s = s.replace(",", "")
    s = s.rstrip(".")
    try:
        v = float(s)
    except ValueError:
        return None
    return v * _SCALE.get((unit or "").lower(), 1.0)


def macro_surprise(text: str) -> dict[str, Any] | None:
    """Actual vs forecast from a data-release squawk (FinancialJuice "ACTUAL/FORECAST",
    Tree "EST.", WalterBloomberg "SURVEY", MarketTwits "ожид").

    -> ``{"event_type", "actual", "forecast", "surprise"}`` (surprise +1 above the
    forecast, -1 below, 0 in line; numbers scaled K/M/B/тыс/млн) or ``None``."""
    t = unicodedata.normalize("NFKC", text or "")
    if not re.search(r"\d", t):
        return None
    for rx in _SURPRISE_RES:
        m = rx.search(t)
        if not m:
            continue
        unit = m.group("u")
        eunit = m.group("eu") or unit
        actual = _num(m.group("a"), unit)
        forecast = _num(m.group("e"), eunit)
        if actual is None or forecast is None:
            return None
        line_start = t.rfind("\n", 0, m.start()) + 1
        line_end = t.find("\n", m.end())
        line = t[line_start: line_end if line_end >= 0 else len(t)]
        tol = 1e-9 * max(1.0, abs(forecast))
        surprise = 0 if abs(actual - forecast) <= tol else (1 if actual > forecast else -1)
        return {"event_type": macro_event_type(line), "actual": actual, "forecast": forecast, "surprise": surprise}
    return None


# --------------------------------------------------------------------------- #
# Whale Alert
# --------------------------------------------------------------------------- #
_WHALE_RE = re.compile(
    r"(?P<amount>\d[\d,]*(?:\.\d+)?)\s+\$?(?P<asset>[A-Z][A-Z0-9]{1,11})\s+\(\s*(?P<usd>\d[\d,]*(?:\.\d+)?)\s+USD\s*\)\s+"
    r"(?:transferred from\s+(?P<frm>.+?)\s+to\s+(?P<to>.+?)|(?P<op>minted|burned|burnt)\s+at\s+(?P<at>.+?))"
    r"\s*(?:\n|Details|$)", re.S | re.I)
_EXCHANGES_RE = re.compile(
    r"binance|coinbase|kraken|\bokx\b|\bokex\b|bybit|bitfinex|bitstamp|kucoin|\bhtx\b|huobi|\bgate(?:\.io)?\b|mexc|"
    r"bitget|upbit|bithumb|crypto\.com|gemini|robinhood|bitvavo|deribit|poloniex|bitflyer|coincheck|bitso|"
    r"bitpanda|\bbingx\b|whitebit|lbank|cex\.io", re.I)


def parse_whale_alert(text: str) -> dict[str, Any] | None:
    """Parse a Whale Alert transfer / mint / burn post.

    -> ``{"amount", "asset", "usd", "from", "to", "flow"}`` with flow one of
    to_exchange | from_exchange | exchange_to_exchange | mint | burn | other,
    or ``None`` (stories, unknown formats)."""
    t = _EMOJI_RE.sub(" ", unicodedata.normalize("NFKC", text or "")).strip()
    m = _WHALE_RE.search(t)
    if not m:
        return None
    try:
        amount = float(m.group("amount").replace(",", ""))
        usd = float(m.group("usd").replace(",", ""))
    except ValueError:
        return None
    d: dict[str, Any] = {"amount": amount, "asset": m.group("asset").upper(), "usd": usd}
    if m.group("op"):
        op = m.group("op").lower()
        d.update({"from": None, "to": m.group("at").strip(), "flow": "mint" if op == "minted" else "burn"})
        return d
    frm, to = m.group("frm").strip(), m.group("to").strip()
    fe, te = bool(_EXCHANGES_RE.search(frm)), bool(_EXCHANGES_RE.search(to))
    flow = "exchange_to_exchange" if fe and te else "to_exchange" if te else "from_exchange" if fe else "other"
    d.update({"from": frm, "to": to, "flow": flow})
    return d


# --------------------------------------------------------------------------- #
# rule classifier
# --------------------------------------------------------------------------- #
def _clamp01(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    if x != x:  # NaN
        return default
    return max(0.0, min(1.0, x))


class RuleClassifier:
    """Ordered first-match rules on the headline (port of the research prototype)."""

    name = "rules"

    def classify(self, title: str, text: str = "", assets: list[str] | None = None) -> dict[str, Any]:
        """``assets`` is the scanned pool (restricts the output tickers; ``[]``/None = no
        restriction).  Returns {} for empty / URL-only posts."""
        pool = [a.upper() for a in assets] if assets else None
        head = headline(title or text)
        if not head:
            return {}
        body = clean_text(text)
        full = f"{head}\n{body}" if body and body != head else head
        hlow = head.lower()
        hmatch = _PREFIX_RE.sub("", _LEAD_NOISE_RE.sub("", hlow))  # "📉 📉 JUST IN: Bitcoin..." -> "bitcoin..."

        category, base, speed, rule_hit = "other", 0.1, "unknown", ""
        for cat, rx, imp, spd in CATEGORY_RULES:
            m = rx.search(hmatch)  # categories from the HEADLINE only (bodies are noisy)
            if m:
                category, base, speed, rule_hit = cat, imp, spd, m.group(0)
                break
        reasons: list[str] = [f"правило {category}: «{rule_hit.strip()}»" if rule_hit else "ни одно правило не подошло"]

        mentioned = extract_assets(full, None)
        found = extract_assets(full, pool)
        if category in ("listing", "delisting"):
            par = [t for t in _LISTING_PAREN_RE.findall(head) if t not in _LISTING_QUOTES]
            cash = [m.group(1) for m in _CASHTAG_RE.finditer(head)]
            listed = list(dict.fromkeys(par + cash))
            if listed:
                mentioned = list(dict.fromkeys(listed + mentioned))
                found = [t for t in listed if pool is None or t in pool]
        data: dict[str, Any] = {"mentioned": mentioned, "rule": rule_hit}
        tags = hashtags(text or title)
        if tags:
            data["hashtags"] = tags
        direction = 0
        bull = bear = 0
        if category == "whale_transfer":
            w = parse_whale_alert(full)
            if w:
                data["whale"] = w
                if w["asset"] in STABLES:
                    direction = +1 if w["flow"] in ("to_exchange", "mint") else (-1 if w["flow"] == "burn" else 0)
                    found = ["*"]
                else:
                    direction = -1 if w["flow"] == "to_exchange" else (+1 if w["flow"] == "from_exchange" else 0)
                    found = [w["asset"]] if (pool is None or w["asset"] in pool) else []
                usd = w["usd"]
                base = 0.25 + (0.15 if usd >= 1e8 else 0.0) + (0.15 if usd >= 5e8 else 0.0)
                reasons.append(f"перевод {w['flow']} на ${usd:,.0f}")
        else:
            sign = CATEGORY_SIGN.get(category, 0)
            bull_re, bear_re = (_BULL_RATES_RE, _BEAR_RATES_RE) if category == "fed_rates" else (_BULL_RE, _BEAR_RE)
            bull = len(bull_re.findall(hlow))
            bear = len(bear_re.findall(hlow))
            if sign:
                direction = sign
                reasons.append("знак по категории")
            elif bull > bear:
                direction = +1
            elif bear > bull:
                direction = -1
            if bull or bear:
                reasons.append(f"бычьих слов {bull}, медвежьих {bear}")
            if direction and not sign and _NEGATION_RE.search(hlow):
                direction = 0
                reasons.append("отрицание в заголовке: направление неясно")
            if category == "market_move":
                direction = 0  # the price already moved: report, don't trade on it
                reasons.append("сообщение о движении цены: направление 0")
            if category == "macro_data":
                direction = 0
                sur = macro_surprise(full)
                if sur is not None:
                    data["macro"] = sur
                    reasons.append(f"{sur['event_type']}: факт {sur['actual']:g} vs прогноз {sur['forecast']:g}")
                    if (sur["event_type"] in INFLATION_EVENTS or _MACRO_INFL_RE.search(hlow)) and sur["surprise"]:
                        direction = -1 if sur["surprise"] > 0 else +1  # hotter inflation -> hawkish -> risk-off
                        base += 0.1
                        reasons.append("инфляция выше прогноза" if sur["surprise"] > 0 else "инфляция ниже прогноза")
                    # labour / growth surprises: the sign is regime-dependent -> 0, reactions learn it
                elif _MACRO_INFL_RE.search(hlow) and _INFL_HOT_RE.search(hlow):
                    direction = -1
                elif _MACRO_INFL_RE.search(hlow) and _INFL_COOL_RE.search(hlow):
                    direction = +1
            if category in ("etf", "regulation") and _ETF_DECISION_RE.search(hlow) and _ETF_WORD_RE.search(hlow):
                category, speed, base = "etf", "seconds", max(base, 0.8)  # approval / denial decision
                reasons.append("решение по ETF")
            if category == "hack" and not _CRYPTO_CTX_RE.search(full):
                base -= 0.4
                direction = 0
                reasons.append("нет крипто-контекста")
            if category == "regulation" and not mentioned and not _CRYPTO_STRICT_RE.search(head):
                base -= 0.2
                direction = 0
                data["no_crypto_context"] = True
                reasons.append("нет крипто-контекста")

        if category in MARKET_WIDE_CATEGORIES:
            named = [a for a in found if a in _KNOWN_TICKERS]
            found = named or ["*"]  # macro moves the whole market; drop $MACRO-style cashtags
        elif not found and not mentioned and category in ("etf", "regulation") and not data.get("no_crypto_context"):
            found = ["*"]  # no specific asset named: crypto-wide law / ETF news
        elif not found and not mentioned and category in ("hack", "exchange") and _TOP_EXCHANGE_RE.search(hlow):
            found = ["*"]  # a top exchange in trouble moves the whole market

        importance = base
        if _HEADLINE_FLAG_RE.search(full):
            importance += 0.1
        if _TIER1_RE.search(hlow):
            importance += 0.1
        usd_amt = _usd_amount(full)
        if usd_amt >= 1e9:
            importance += 0.15
        elif usd_amt >= 1e8:
            importance += 0.05
        confidence = 0.6 if category != "other" else 0.2
        if _HEDGE_RE.search(hlow):
            importance -= 0.1
            confidence *= 0.7
            data["hedge"] = True
            reasons.append("оговорка (слухи / возможно): важность −0.1, уверенность ×0.7")
        if direction == 0:
            confidence = min(confidence, 0.5)
        importance = max(0.0, min(1.0, round(importance, 2)))
        data["bull"], data["bear"] = bull, bear
        return {
            "category": category, "assets": found, "direction": int(direction), "importance": importance,
            "speed": speed, "confidence": round(confidence, 2), "reason": "; ".join(reasons),
            "dedup_key": dedup_key(head), "data": data, "headline": head,
        }


# --------------------------------------------------------------------------- #
# Claude classifier
# --------------------------------------------------------------------------- #
CLAUDE_SYSTEM_PROMPT = (
    "You classify ONE crypto/macro news item for a spot-crypto trader whose positions last seconds to an hour. "
    "Reply only with the JSON object required by the schema.\n\n"
    "Fields\n"
    "- category: fed_rates | macro_data | regulation | etf | hack | exchange | listing | delisting | whale_transfer | "
    "liquidation | unlock | upgrade | partnership | geopolitics | market_move | other.\n"
    "- assets: tickers from POOL that this item directly affects. [\"*\"] when it moves the whole crypto market "
    "(Fed/ECB decisions, US macro data, war, tariffs, US crypto law, spot-BTC ETF flows). [] when no POOL asset is "
    "affected. Never output a ticker that is not in POOL or \"*\".\n"
    "- direction: expected price effect on those assets within 60 minutes: 1 up, -1 down, 0 unclear.\n"
    "- importance 0..1: probability-like weight that this item moves the listed assets by at least 0.5% within an "
    "hour. Above 0.85 only for: Fed/ECB rate decisions that differ from expectations, CPI/PCE surprises, spot-ETF "
    "approval or denial, a hack or insolvency of a top-20 exchange or over $100M, military escalation between major "
    "powers, new tariffs on China/EU, delisting of a POOL asset by Binance or Coinbase.\n"
    "- speed: seconds (reaction within 60 s: scheduled data, rate decisions, hacks, top-exchange listings, missile "
    "strikes, tariff announcements), minutes (1-30 min: lawsuits, outages, large whale or ETF flows), hours (slow: "
    "unlocks, upgrades, partnerships, proposals, policy plans).\n"
    "- confidence 0..1: confidence in category AND direction together.\n"
    "- reason: at most 20 words IN RUSSIAN, the causal chain (for example \"CPI 3.8% против прогноза 3.7%: "
    "инфляция выше, ястребиная ФРС, риск-офф\").\n\n"
    "Rules\n"
    "1. direction 0 when: unclear, mixed, a question, opinion, analysis or recap, a rumor without a named credible "
    "source, a speaker quote with no new policy, a price report (\"BTC falls below $80k\" is market_move with 0), an "
    "outcome that was fully expected, or the item is old news retold. Unclear is always better than wrong.\n"
    "2. Data releases: compare actual with forecast. Inflation above forecast -1, below +1, in line 0. Jobs, claims, "
    "GDP, retail sales: 0 unless the text itself states the market reading.\n"
    "3. Hedges (reportedly, sources say, considering, may, plans to, weighs): confidence at most 0.5.\n"
    "4. Legal aftermath of an old hack (arrest, conviction, settlement), anniversaries, explainers: importance at "
    "most 0.3.\n"
    "5. Whale transfers: wallet to exchange -1, exchange to wallet +1, wallet to wallet 0, stablecoin mint +1; "
    "importance at most 0.5 unless above $500M.\n"
    "6. Negations flip meaning (\"probability of a hike collapsed\" is dovish, +1).\n"
    "7. Use only the given text. Never invent numbers, sources, assets or events.\n"
    "8. If POOL says ANY, use plain market tickers (BTC, ETH, SOL...) for the assets the item names directly."
)

CLAUDE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "category": {"type": "string", "enum": list(NEWS_CATEGORIES)},
        "assets": {"type": "array", "items": {"type": "string"}},
        "direction": {"type": "integer", "enum": [-1, 0, 1]},
        "importance": {"type": "number"},
        "speed": {"type": "string", "enum": ["seconds", "minutes", "hours"]},
        "confidence": {"type": "number"},
        "reason": {"type": "string"},
    },
    "required": ["category", "assets", "direction", "importance", "speed", "confidence", "reason"],
    "additionalProperties": False,
}


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def parse_json_answer(text: str) -> dict[str, Any] | None:
    """The first JSON object in a model answer (code fences / prose tolerated)."""
    s = (text or "").strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z]*\s*", "", s)
        s = re.sub(r"\s*```\s*$", "", s)
    try:
        obj = json.loads(s)
    except (ValueError, TypeError):
        i = s.find("{")
        if i < 0:
            return None
        try:
            obj, _ = json.JSONDecoder().raw_decode(s[i:])
        except ValueError:
            return None
    return obj if isinstance(obj, dict) else None


def normalise_answer(raw: dict[str, Any], pool: list[str] | None) -> dict[str, Any] | None:
    """Validate / clamp a Claude answer.  None when the essentials are unusable."""
    category = raw.get("category")
    if category not in NEWS_CATEGORIES:
        return None
    try:
        direction = int(raw.get("direction", 0))
    except (TypeError, ValueError):
        direction = 0
    if direction not in (-1, 0, 1):
        direction = 0
    speed = raw.get("speed")
    if speed not in NEWS_SPEEDS:
        speed = "unknown"
    pool_set = {p.upper() for p in pool} if pool else None
    assets: list[str] = []
    for a in raw.get("assets") or []:
        if not isinstance(a, str):
            continue
        t = a.strip().upper().lstrip("$")
        if not t or t in assets:
            continue
        if t != "*" and (t in STABLES or (pool_set is not None and t not in pool_set)):
            continue
        assets.append(t)
    if "*" in assets:
        assets = ["*"]
    if direction != 0 and not assets:
        direction = 0
    reason = str(raw.get("reason") or "").strip().replace("\n", " ")[:200]
    return {
        "category": category, "assets": assets, "direction": direction,
        "importance": round(_clamp01(raw.get("importance")), 3), "speed": speed,
        "confidence": round(_clamp01(raw.get("confidence")), 3), "reason": reason,
    }


class ClaudeClassifier:
    """JSON-only headline classification through ``anthropic.AsyncAnthropic``.

    The client is created lazily from ``api_key`` (never logged) or injected
    (tests).  Budget: ``news_claude_max_per_hour`` calls in any rolling hour
    (attempts count, failed ones too).  Any failure -> ``None``."""

    name = "claude"

    def __init__(self, settings: Any, clock: Any, *, api_key: str = "", client: Any = None) -> None:
        self._settings = settings
        self._clock = clock
        self._api_key = api_key or ""
        self._client = client
        self._calls: collections.deque[float] = collections.deque()
        self._hold_until_mono = 0.0
        self.last_error = ""
        self.calls_total = 0
        self.ok_total = 0

    # ---- state -------------------------------------------------------------
    @property
    def available(self) -> bool:
        return bool(self._client is not None or self._api_key)

    @property
    def enabled(self) -> bool:
        return bool(getattr(self._settings, "news_claude_enabled", True)) and self.available

    @property
    def model(self) -> str:
        return str(getattr(self._settings, "analysis_model", "") or "claude-opus-5-5")

    def budget(self) -> int:
        return max(0, int(getattr(self._settings, "news_claude_max_per_hour", 30)))

    def _trim(self, now: float) -> None:
        while self._calls and now - self._calls[0] >= 3600.0:
            self._calls.popleft()

    def used_last_hour(self) -> int:
        self._trim(self._clock.mono())
        return len(self._calls)

    def remaining(self) -> int:
        return max(0, self.budget() - self.used_last_hour())

    def can_call(self) -> bool:
        return self.enabled and self.remaining() > 0 and self._clock.mono() >= self._hold_until_mono

    def snapshot(self) -> dict[str, Any]:
        return {
            "enabled": bool(getattr(self._settings, "news_claude_enabled", True)),
            "available": self.available, "model": self.model, "budget_per_hour": self.budget(),
            "used_last_hour": self.used_last_hour(), "calls_total": self.calls_total, "ok_total": self.ok_total,
            "last_error": self.last_error,
        }

    def _ensure_client(self) -> Any:
        if self._client is None:
            import anthropic  # local import: optional at runtime

            timeout = float(getattr(self._settings, "news_claude_timeout_s", 15.0))
            self._client = anthropic.AsyncAnthropic(api_key=self._api_key, max_retries=0, timeout=timeout)
        return self._client

    @staticmethod
    def user_message(title: str, text: str, pool: list[str] | None, *, source: str = "",
                     published_ts: float | None = None) -> str:
        pool_csv = ", ".join(sorted({p.upper() for p in pool})) if pool else "ANY"
        iso = (datetime.fromtimestamp(published_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
               if published_ts is not None else "unknown")
        body = re.sub(r"\s+", " ", text or "").strip()[:600]
        return (f"POOL: {pool_csv}\nSOURCE: {source or 'unknown'}\nPUBLISHED_UTC: {iso}\n"
                f"HEADLINE: {(title or '').strip()[:300]}\nTEXT: {body}")

    # ---- call ----------------------------------------------------------------
    async def classify(self, title: str, text: str, assets: list[str], *, source: str = "",
                       published_ts: float | None = None) -> dict[str, Any] | None:
        if not self.can_call():
            return None
        now = self._clock.mono()
        self._calls.append(now)
        self.calls_total += 1
        timeout = float(getattr(self._settings, "news_claude_timeout_s", 15.0))
        try:
            client = self._ensure_client()
            response = await asyncio.wait_for(client.messages.create(
                model=self.model,
                max_tokens=2048,
                system=[{"type": "text", "text": CLAUDE_SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
                messages=[{"role": "user", "content": self.user_message(
                    title, text, assets, source=source, published_ts=published_ts)}],
                output_config={"effort": "low", "format": {"type": "json_schema", "schema": CLAUDE_SCHEMA}},
            ), timeout=timeout)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - never raise, never leak the key
            name = type(exc).__name__
            status = getattr(exc, "status_code", None)
            if status == 429:
                retry_after = 60.0
                try:
                    retry_after = float(exc.response.headers.get("retry-after", "60"))  # type: ignore[attr-defined]
                except Exception:  # noqa: BLE001
                    pass
                self._hold_until_mono = now + max(1.0, retry_after)
            self.last_error = f"{name}" + (f" (HTTP {status})" if status else "")
            log.warning("news: Claude classification failed: %s", self.last_error)
            return None
        stop = _get(response, "stop_reason")
        if stop in ("refusal", "max_tokens"):
            self.last_error = f"stop_reason={stop}"
            log.info("news: Claude answer skipped (%s)", self.last_error)
            return None
        text_out = ""
        for block in _get(response, "content", []) or []:
            if _get(block, "type") == "text" and _get(block, "text"):
                text_out = str(_get(block, "text"))
                break
        raw = parse_json_answer(text_out)
        if raw is None:
            self.last_error = "invalid JSON answer"
            log.info("news: Claude answer was not JSON")
            return None
        out = normalise_answer(raw, assets)
        if out is None:
            self.last_error = "answer outside the schema"
            return None
        self.last_error = ""
        self.ok_total += 1
        return out


# --------------------------------------------------------------------------- #
# combined classifier
# --------------------------------------------------------------------------- #
class HeadlineClassifier:
    """Rules always; Claude refines (when enabled + key) items whose rule importance
    is >= ``news_claude_min_importance`` (0.4), that are at most ``news_claude_max_age_s``
    old and within the hourly budget."""

    def __init__(self, settings: Any, clock: Any, *, api_key: str = "", client: Any = None,
                 rules: RuleClassifier | None = None, claude: ClaudeClassifier | None = None) -> None:
        self._settings = settings
        self._clock = clock
        self.rules = rules or RuleClassifier()
        self.claude = claude if claude is not None else (
            ClaudeClassifier(settings, clock, api_key=api_key, client=client) if (api_key or client is not None) else None)

    # ---- rules -------------------------------------------------------------
    def apply_rules(self, item: NewsItem, assets: list[str]) -> NewsItem:
        res = self.rules.classify(item.title, item.text, assets)
        if not res:
            item.category, item.assets, item.direction = "other", [], 0
            item.importance, item.confidence, item.speed = 0.0, 0.0, "unknown"
            item.classifier, item.reason = "rules", "нет текста для классификации"
            item.data["classified"] = {"importance": 0.0, "confidence": 0.0}
            return item
        item.category = res["category"]
        item.assets = list(res["assets"])
        item.direction = int(res["direction"])
        item.importance = float(res["importance"])
        item.speed = res["speed"]
        item.confidence = float(res["confidence"])
        item.classifier = "rules"
        item.reason = res["reason"]
        if not item.dedup_key:
            item.dedup_key = res["dedup_key"]
        for k, v in res["data"].items():
            item.data[k] = v
        item.data["rules"] = {k: res[k] for k in ("category", "assets", "direction", "importance", "speed", "confidence")}
        item.data["classified"] = {"importance": item.importance, "confidence": item.confidence}
        return item

    # ---- Claude --------------------------------------------------------------
    def wants_claude(self, item: NewsItem, now_wall: float | None = None) -> bool:
        if self.claude is None or not self.claude.can_call():
            return False
        rules = item.data.get("rules") or {}
        if float(rules.get("importance", item.importance)) < float(getattr(self._settings, "news_claude_min_importance", 0.4)):
            return False
        if item.data.get("no_crypto_context"):
            return False  # e.g. SEC actions against equity advisers: nothing for the crypto pool
        now = self._clock.wall() if now_wall is None else now_wall
        max_age = float(getattr(self._settings, "news_claude_max_age_s", 1800.0))
        return (now - float(item.ts)) <= max_age

    async def refine(self, item: NewsItem, assets: list[str]) -> bool:
        """Claude refinement merged into ``item``; False (item unchanged) on any failure."""
        if self.claude is None:
            return False
        try:
            res = await self.claude.classify(item.title, item.text, assets,
                                             source=f"{item.source}:{item.channel}", published_ts=item.ts)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("news: Claude refine failed: %s", type(exc).__name__)
            return False
        if res is None:
            return False
        self.merge(item, res)
        return True

    @staticmethod
    def merge(item: NewsItem, res: dict[str, Any]) -> NewsItem:
        """Claude result over the rule result.  Opposite non-zero directions -> 0
        (no fake signal) with the lower confidence."""
        rules = item.data.get("rules") or {}
        r_dir = int(rules.get("direction", item.direction) or 0)
        c_dir = int(res["direction"])
        confidence = float(res["confidence"])
        direction = c_dir
        note = ""
        if r_dir and c_dir and r_dir != c_dir:
            direction = 0
            confidence = min(confidence, float(rules.get("confidence", confidence)))
            note = " (правила и Claude расходятся по направлению → 0)"
        if direction == 0:
            confidence = min(confidence, 0.5)
        item.category = res["category"]
        if res["assets"] or not item.assets:
            item.assets = list(res["assets"])
        item.direction = direction
        item.importance = float(res["importance"])
        if res["speed"] in NEWS_SPEEDS and res["speed"] != "unknown":
            item.speed = res["speed"]
        item.confidence = round(confidence, 3)
        item.classifier = "claude"
        item.reason = (res["reason"] or "Claude") + note
        item.data["claude"] = dict(res)
        item.data["classified"] = {"importance": item.importance, "confidence": item.confidence}
        return item

    async def classify(self, item: NewsItem, assets: list[str]) -> NewsItem:
        self.apply_rules(item, assets)
        if self.wants_claude(item):
            await self.refine(item, assets)
        return item

    def snapshot(self) -> dict[str, Any]:
        claude = self.claude.snapshot() if self.claude is not None else {
            "enabled": bool(getattr(self._settings, "news_claude_enabled", True)), "available": False,
            "model": str(getattr(self._settings, "analysis_model", "") or "claude-opus-5-5"),
            "budget_per_hour": int(getattr(self._settings, "news_claude_max_per_hour", 30)), "used_last_hour": 0,
            "calls_total": 0, "ok_total": 0, "last_error": "", "note": "no API key",
        }
        return {"rules": True, "claude": claude}


__all__ = [
    "AMBIGUOUS_TICKERS", "ASSET_ALIASES", "CATEGORY_RULES", "CLAUDE_SCHEMA", "CLAUDE_SYSTEM_PROMPT", "ClaudeClassifier",
    "HeadlineClassifier", "RuleClassifier", "STABLES", "clean_text", "dedup_key", "dedup_title", "extract_assets",
    "hashtags", "headline", "jaccard", "macro_event_type", "macro_surprise", "normalise_answer", "parse_json_answer",
    "parse_whale_alert", "similarity", "tokens",
]
