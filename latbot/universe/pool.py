"""Asset pool: which assets the bot scans at all (ARCHITECTURE P3.1 + P3.12).

The pool is objective and narrow.  Every candidate goes through the same steps;
a missing input means "нет данных" and the asset stays OUT (only ``pool_include``
- BTC, ETH by default - is always in):

1. **Category** exclusion by CoinGecko *id* (meme, stablecoins, wrapped, liquid
   staking, bridged, tokenized gold / commodities / RWA, fan tokens): the frozen
   id lists of ``data/fallback_pool.json`` plus a live refresh of the main
   categories.  Tickers are never excluded by symbol (bridged / wrapped / meme
   coins reuse BTC, ETH, SOL, TAO ...), except the futures meme aliases
   (1000PEPE ...) - and never the ambiguous tickers AI, BABY, B, M, LION, DOG,
   LUNA.  CoinGecko market data (market cap, global volume, 24 h / 7 d change)
   is checked here too (hype pumps / dumps).
2. **Venues**: listed on >= ``pool_min_signal_venues`` enabled exchanges, of
   which >= ``pool_min_target_venues`` are execution (target) venues.
3. **Target turnover** stays per signal (``min_target_volume_quote`` on the
   venue's own ticker); the pool only shows the volumes it was given.
4. **Stability** (``stability_fn`` = ``context/bars.py`` on OKX 1H candles over
   30 days): correlation with BTC, residual hourly jumps, max 7-day move,
   history length.  Also excluded: perp funding interval < 8 h
   (``funding_interval_fn``) - exchanges move erratic coins to 4 h / 1 h.

The pool is usable right after construction (last cache file, else the bundled
fallback) so ``contains()`` works before the first network refresh.  Network
only through the injected ``HttpGetter``.
"""
from __future__ import annotations

import json
import logging
import math
import os
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

from ..context.base import HttpGetter, HttpResponse, PollingCollector, RateLimitedSource, SourceError, check_response

log = logging.getLogger(__name__)

COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"
FALLBACK_PATH = Path(__file__).resolve().parent / "data" / "fallback_pool.json"
CG_PER_PAGE = 250

# Exchange ticker -> cross-exchange asset key (Kraken legacy names).
SYMBOL_MAP: dict[str, str] = {"XBT": "BTC", "XDG": "DOGE"}

# Every CoinGecko category id excluded by default (same ids as the groups in
# data/fallback_pool.json "exclude_categories"; verified against
# /coins/categories/list on 2026-10-08).
MEME_CATEGORIES: tuple[str, ...] = (
    "meme-token", "solana-meme-coins", "base-meme-coins", "bitcoin-meme", "sui-meme", "ton-meme-coins",
    "tron-meme", "chinese-meme", "ai-meme-coins", "tiktok-meme", "politifi", "trump-affiliated-tokens",
    "parody-meme-coins", "celebrity-themed-coins", "elon-musk-inspired-coins", "dog-themed-coins",
    "cat-themed-coins", "frog-themed-coins", "four-meme-ecosystem", "pump-fun", "pump-fun-creator",
    "gmci-meme-index", "desci-meme", "ip-meme", "country-themed-meme-coins", "4chan-themed", "zoo-themed",
    "mascot-themed", "sticker-themed-coin",
)
DEFAULT_EXCLUDE_CATEGORIES: tuple[str, ...] = MEME_CATEGORIES + (
    # stablecoins
    "stablecoins", "usd-stablecoin", "eur-stablecoin", "fiat-backed-stablecoin", "crypto-backed-stablecoin",
    "algorithmic-stablecoin", "yield-bearing-stablecoins", "synthetic-dollar", "bridged-stablecoins",
    # wrapped
    "wrapped-tokens",
    # liquid staking / restaking receipts (NOT "liquid-staking": that one holds the protocols' governance tokens)
    "liquid-staking-tokens", "liquid-staked-eth", "liquid-staked-sol", "liquid-restaking-tokens",
    "liquid-restaked-eth",
    # bridged
    "bridged-tokens", "bridged-wbtc", "bridged-weth", "bridged-usdc", "bridged-usdt",
    # tokenized gold / commodities
    "tokenized-gold", "gold", "tokenized-commodities", "tokenized-silver", "commodity-backed-stablecoin",
    # fan tokens
    "fan-token",
    # tokenized stocks / treasuries / other RWA receipts
    "tokenized-products", "tokenized-stock", "tokenized-treasuries",
)

# Broad parent categories refreshed live (cheap: about one call each); the
# narrow sub-categories rely on the frozen lists.  An excluded category without
# a frozen list (owner-added) is refreshed live as well.
DEFAULT_LIVE_CATEGORIES: tuple[str, ...] = (
    "meme-token", "stablecoins", "wrapped-tokens", "liquid-staking-tokens", "liquid-restaking-tokens",
    "bridged-tokens", "tokenized-gold", "tokenized-commodities", "fan-token", "tokenized-products",
)

# Settings read with getattr(settings, name, default) - the integration adds the same fields to Settings.
POOL_DEFAULTS: dict[str, Any] = {
    "pool_enabled": True,
    "pool_max_assets": 25,  # <= 0: no cap
    "pool_stability_filters": True,  # False: the top coins by market cap as they are (no stability / funding / spike steps)
    "pool_min_market_cap_usd": 1e9,
    "pool_min_volume_24h_usd": 2e7,  # global CoinGecko volume (target turnover is checked per signal)
    "pool_max_abs_change_24h_pct": 15.0,
    "pool_max_abs_change_7d_pct": 100.0,  # CoinGecko 7 d change and the stability max 7-day move
    "pool_exclude_categories": list(DEFAULT_EXCLUDE_CATEGORIES),
    "pool_live_categories": list(DEFAULT_LIVE_CATEGORIES),
    "pool_include": ["BTC", "ETH"],
    "pool_exclude": [],
    "pool_min_signal_venues": 3,
    "pool_min_target_venues": 1,
    "pool_min_btc_corr": 0.55,
    "pool_jump_hour_pct": 3.0,  # what context/bars.py counts as an hourly residual jump (shown in criteria)
    "pool_max_hour_jumps_30d": 20,
    "pool_min_history_days": 20,
    "pool_min_funding_interval_h": 8.0,
    "stability_max_age_h": 72.0,  # shared with context/bars.py: a stability measurement older than this is no data
    "pool_spike_pct": 2.0,  # bars_fn (P3.1 compat): 1-minute spike size
    "pool_max_spikes": 3,
    "pool_jump_min_minutes": 120,
    "pool_refresh_h": 12.0,
    "pool_cg_calls_per_min": 0,  # 0 = auto: 5 keyless, 60 with a Demo key
    "pool_category_max_pages": 4,
}

_GROUP_LABELS: dict[str, str] = {
    "meme": "мем-монета",
    "stablecoin": "стейблкоин",
    "wrapped": "обёрнутый токен",
    "liquid_staking": "токен ликвидного стейкинга",
    "bridged": "мостовой токен",
    "gold_commodities": "токенизированное золото/сырьё",
    "fan_token": "фан-токен",
    "tokenized_rwa": "токенизированный актив (RWA)",
}
_BLOCKLIST_GROUPS = {"meme-blocklist": "meme", "stable-blocklist": "stablecoin"}

_KEYLESS_CALLS_PER_MIN = 5
_KEY_CALLS_PER_MIN = 60
_SAVE_MIN_INTERVAL_S = 300.0
_STAB_KEYS = ("days", "btc_corr", "hour_jumps", "max_7d_move_pct", "beta", "jump_hour_pct")


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def normalise_asset(asset: Any) -> str:
    """Exchange ticker -> cross-exchange asset key ("xbt" -> "BTC")."""
    s = str(asset or "").strip().upper()
    return SYMBOL_MAP.get(s, s)


def _num(v: Any) -> float | None:
    if isinstance(v, bool) or v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _int(v: Any) -> int | None:
    f = _num(v)
    return int(f) if f is not None and f > 0 else None


def _str_list(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, str):
        v = v.split(",")
    try:
        return [str(x).strip() for x in v if str(x).strip()]
    except TypeError:
        return []


def _usd(v: float) -> str:
    a = abs(v)
    if a >= 1e12:
        return f"${v / 1e12:.2f} трлн"
    if a >= 1e9:
        return f"${v / 1e9:.1f} млрд"
    if a >= 1e6:
        return f"${v / 1e6:.1f} млн"
    if a >= 1e3:
        return f"${v / 1e3:.0f} тыс."
    return f"${v:.0f}"


def _age(seconds: float | None) -> str:
    if seconds is None:
        return "?"
    s = max(0.0, seconds)
    if s < 120:
        return f"{s:.0f} с"
    if s < 7200:
        return f"{s / 60:.0f} мин"
    if s < 172800:
        return f"{s / 3600:.0f} ч"
    return f"{s / 86400:.0f} дн"


def _percentile(sorted_vals: list[float], q: float) -> float:
    """Linear-interpolated percentile (numpy default) of an already sorted list."""
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    pos = (len(sorted_vals) - 1) * q / 100.0
    lo = math.floor(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (pos - lo)


def jumpiness(closes: list[float], spike_pct: float) -> dict:
    """1-minute jump profile of a close series (oldest -> newest).

    ``minutes`` = number of 1-minute returns, ``spikes`` = returns with
    |r| >= spike_pct (percent), ``max_1m_pct`` = max |r|, ``tail_ratio`` =
    |p95| / |p5| of the returns (None when p5 is 0).  Non-positive / non-finite
    closes break the chain (no gap filling)."""
    rets: list[float] = []
    prev: float | None = None
    for c in closes or ():
        f = _num(c)
        if f is None or f <= 0:
            prev = None
            continue
        if prev is not None:
            rets.append((f / prev - 1.0) * 100.0)
        prev = f
    if not rets:
        return {"minutes": 0, "spikes": 0, "max_1m_pct": None, "tail_ratio": None}
    thr = abs(_num(spike_pct) or 0.0)
    spikes = sum(1 for r in rets if abs(r) >= thr) if thr > 0 else 0
    srt = sorted(rets)
    p95, p5 = _percentile(srt, 95.0), _percentile(srt, 5.0)
    tail = abs(p95) / abs(p5) if p5 != 0 else None
    return {
        "minutes": len(rets),
        "spikes": spikes,
        "max_1m_pct": round(max(abs(r) for r in rets), 4),
        "tail_ratio": round(tail, 3) if tail is not None else None,
    }


def load_fallback(path: Path | None = None) -> dict:
    """The bundled offline pool (tiers, frozen category ids, blocklists); {} when unreadable."""
    p = Path(path) if path is not None else FALLBACK_PATH
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        log.error("asset pool: bundled fallback %s unreadable: %s", p, e)
        return {}
    return doc if isinstance(doc, dict) else {}


def _parse_market_row(r: Any) -> dict | None:
    """CoinGecko /coins/markets row (or a cached row) -> compact dict."""
    if not isinstance(r, dict):
        return None
    cid = str(r.get("id") or "").strip()
    sym = normalise_asset(r.get("symbol"))
    if not cid or not sym:
        return None
    cap = _num(r.get("market_cap"))
    vol = _num(r.get("total_volume", r.get("volume")))
    ch24 = _num(r.get("price_change_percentage_24h", r.get("ch24")))
    if ch24 is None:
        ch24 = _num(r.get("price_change_percentage_24h_in_currency"))
    ch7 = _num(r.get("price_change_percentage_7d_in_currency", r.get("ch7d")))
    if ch7 is None:
        ch7 = _num(r.get("price_change_percentage_7d"))
    return {
        "id": cid,
        "symbol": sym,
        "rank": _int(r.get("market_cap_rank", r.get("rank"))),
        "market_cap": cap if cap is not None and cap > 0 else None,  # CoinGecko reports 0 for "unknown"
        "volume": vol if vol is not None and vol >= 0 else None,
        "ch24": ch24,
        "ch7d": ch7,
    }


# --------------------------------------------------------------------------- #
# model
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class PoolAsset:
    asset: str
    rank: int | None = None
    market_cap_usd: float | None = None
    volume_24h_usd: float | None = None
    change_24h_pct: float | None = None
    change_7d_pct: float | None = None
    categories: list[str] = field(default_factory=list)  # excluded CoinGecko categories the id belongs to
    venues_signal: list[str] = field(default_factory=list)  # every enabled venue that lists the asset
    venues_target: list[str] = field(default_factory=list)  # the execution venues among them
    jump: dict = field(default_factory=dict)  # jumpiness() result or {}
    included: bool = False
    reasons: list[str] = field(default_factory=list)  # Russian: why excluded / "в пуле: ..."
    source: str = "fallback"  # "coingecko" | "cache" | "fallback"
    # P3.12 additions
    cg_id: str = ""
    tier: str = ""  # "A" / "B": research tier of the bundled fallback list (informational)
    target_volumes: dict = field(default_factory=dict)  # venue -> 24 h quote turnover as given by listings_fn
    stability: dict = field(default_factory=dict)  # measurement used (+ "origin", "age_s")
    funding_interval_h: float | None = None
    forced: bool = False  # in pool_include

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class _Eval:
    pa: PoolAsset
    fails: list[str]
    notes: list[str]
    pre_ok: bool  # passed every step before stability / funding (needs stability data)


# --------------------------------------------------------------------------- #
# pool
# --------------------------------------------------------------------------- #
class AssetPool(PollingCollector):
    name = "asset_pool"

    def __init__(
        self, settings: Any, clock: Any, http: HttpGetter | None, *, cache_path: Path | None = None,
        api_key: str = "",
        listings_fn: Callable[[], dict[str, dict[str, float | None]]] | None = None,
        bars_fn: Callable[[str], list[float]] | None = None,
        stability_fn: Callable[[str], dict | None] | None = None,
        funding_interval_fn: Callable[[str], float | None] | None = None,
        fallback_path: Path | None = None,
        stability_pending_fn: Callable[[str], bool] | None = None,
    ) -> None:
        refresh_h = _num(getattr(settings, "pool_refresh_h", POOL_DEFAULTS["pool_refresh_h"]))
        super().__init__(settings, clock, http, interval_s=max(60.0, (refresh_h or 12.0) * 3600.0))
        self._cache_path = Path(cache_path) if cache_path is not None else None
        self._api_key = (api_key or "").strip()
        self._listings_fn = listings_fn
        self._bars_fn = bars_fn
        self._stability_fn = stability_fn
        self._stability_pending_fn = stability_pending_fn
        self._funding_interval_fn = funding_interval_fn
        self._fallback = load_fallback(fallback_path)
        fb = self._fallback
        self._fb_pool = [normalise_asset(a) for a in fb.get("pool") or []]
        self._tiers: dict[str, str] = {}
        for t, key in (("A", "tier_a"), ("B", "tier_b")):
            for a in fb.get(key) or []:
                self._tiers.setdefault(normalise_asset(a), t)
        self._fb_ids = {normalise_asset(k): str(v) for k, v in (fb.get("cg_ids") or {}).items() if v}
        self._fb_ranks = {normalise_asset(k): _int(v) for k, v in (fb.get("ranks") or {}).items()}
        self._cat_group: dict[str, str] = {}
        for group, cats in (fb.get("exclude_categories") or {}).items():
            for c in cats or []:
                self._cat_group[str(c)] = str(group)
        self._frozen: dict[str, frozenset[str]] = {
            str(c): frozenset(str(i) for i in ids or []) for c, ids in (fb.get("category_members") or {}).items()
        }
        self._meme_ids = self._blocklist(fb.get("meme_blocklist"))  # CoinGecko id -> ticker (display only)
        self._stable_ids = self._blocklist(fb.get("stable_blocklist"))
        self._ambiguous = {normalise_asset(s) for s in fb.get("ambiguous_symbols") or []}
        self._aliases: dict[str, dict] = {}
        for k, v in (fb.get("symbol_aliases") or {}).items():
            key = normalise_asset(k)
            if key and key not in self._ambiguous:
                self._aliases[key] = v if isinstance(v, dict) else {"base": str(v)}

        # data
        self._rows: list[dict] = []  # CoinGecko markets rows (live or cached)
        self._markets_ts: float | None = None  # wall ts the market rows were fetched
        self._source = "fallback"
        self._live_cats: dict[str, dict] = {}  # cat -> {"fetched_ts", "attempt_ts", "ids", "error"}
        self._stab_cache: dict[str, dict] = {}
        self._dirty = False
        self._last_save_mono: float | None = None
        self._calls: deque[float] = deque()

        # evaluation
        self._assets: dict[str, PoolAsset] = {}
        self._included: list[str] = []
        self._included_set: frozenset[str] = frozenset()
        self._candidates: list[str] = []
        self._steps: dict[str, str] = {}
        self._updated_ts: float | None = None

        if self._load_cache() and self._markets_ts is not None:
            age = self._clock.wall() - self._markets_ts
            if 0 <= age < self.interval_s:  # fresh cache: no CoinGecko call right after a restart
                self._next_mono = self._clock.mono() + (self.interval_s - age)
        self.reevaluate()
        self._update_note()

    @staticmethod
    def _blocklist(entries: Any) -> dict[str, str]:
        out: dict[str, str] = {}
        for e in entries or []:
            if isinstance(e, dict) and e.get("id"):
                out[str(e["id"])] = str(e.get("symbol") or "")
        return out

    # ------------------------------------------------------------------ #
    # settings
    # ------------------------------------------------------------------ #
    def _s(self, name: str) -> Any:
        return getattr(self._settings, name, POOL_DEFAULTS[name])

    def _f(self, name: str) -> float:
        v = _num(self._s(name))
        return float(POOL_DEFAULTS[name]) if v is None else v

    def _exclude_categories(self) -> list[str]:
        v = getattr(self._settings, "pool_exclude_categories", None)
        return _str_list(DEFAULT_EXCLUDE_CATEGORIES if v is None else v)

    def _live_categories(self) -> list[str]:
        excl = self._exclude_categories()
        excl_set = set(excl)
        v = getattr(self._settings, "pool_live_categories", None)
        live = [c for c in _str_list(DEFAULT_LIVE_CATEGORIES if v is None else v) if c in excl_set]
        known = set(self._cat_group) | set(self._frozen)
        live += [c for c in excl if c not in known and c not in live]  # owner-added: no frozen list
        return live

    def _calls_per_min(self) -> int:
        v = _num(self._s("pool_cg_calls_per_min")) or 0
        if v > 0:
            return int(v)
        return _KEY_CALLS_PER_MIN if self._api_key else _KEYLESS_CALLS_PER_MIN

    def criteria(self) -> dict[str, Any]:
        """The thresholds in force (for the UI / snapshot)."""
        out: dict[str, Any] = {}
        for k in POOL_DEFAULTS:
            if k in ("pool_exclude_categories", "pool_live_categories"):
                continue
            v = self._s(k)
            out[k] = _str_list(v) if k in ("pool_include", "pool_exclude") else v
        out["pool_exclude_categories"] = self._exclude_categories()
        out["pool_live_categories"] = self._live_categories()
        out["min_target_volume_quote"] = getattr(self._settings, "min_target_volume_quote", 500_000.0)
        return out

    # ------------------------------------------------------------------ #
    # public API
    # ------------------------------------------------------------------ #
    def contains(self, asset: str) -> bool:
        if not self._s("pool_enabled"):
            return True
        return asset in self._included_set or normalise_asset(asset) in self._included_set

    def assets(self) -> list[str]:
        """Included assets: pool_include first, then by market-cap rank."""
        return list(self._included)

    def candidates(self) -> list[str]:
        """Assets that passed every step before stability (they need stability data:
        the integration fetches their 1H candles), pool_include first."""
        return list(self._candidates)

    def get(self, asset: str) -> PoolAsset | None:
        return self._assets.get(normalise_asset(asset))

    def blocked_symbol(self, asset: str) -> str | None:
        """Reason when an exchange ticker is a futures meme alias (1000PEPE ...), else None."""
        a = normalise_asset(asset)
        if a in self._ambiguous:
            return None
        al = self._aliases.get(a)
        if al is None:
            return None
        return f"мем-тикер фьючерса: {a} = {al.get('base') or '?'} (мем-монета)"

    @property
    def source(self) -> str:
        return self._source

    def snapshot(self) -> dict:
        now = self._clock.wall()
        inc = [self._assets[a].to_dict() for a in self._included if a in self._assets]
        exc = []
        for pa in sorted((p for p in self._assets.values() if not p.included),
                         key=lambda p: (p.rank is None, p.rank or 0, p.asset)):
            exc.append({"asset": pa.asset, "rank": pa.rank, "cg_id": pa.cg_id, "reasons": list(pa.reasons)})
        cats = {}
        for c in self._live_categories():
            rec = self._live_cats.get(c) or {}
            cats[c] = {
                "fetched_ts": rec.get("fetched_ts"), "live_members": len(rec.get("ids") or ()),
                "frozen_members": len(self._frozen.get(c, ())), "error": rec.get("error", ""),
            }
        return {
            "enabled": bool(self._s("pool_enabled")),
            "updated_ts": self._updated_ts,
            "source": self._source,
            "data_ts": self._markets_ts,
            "data_age_s": (now - self._markets_ts) if self._markets_ts is not None else None,
            "fallback_generated": self._fallback.get("generated", ""),
            "api_key": bool(self._api_key),
            "criteria": self.criteria(),
            "steps": dict(self._steps),
            "size": len(inc),
            "included": inc,
            "excluded": exc,
            "candidates": list(self._candidates),
            "categories": cats,
            "pending_categories": self._due_categories(now),
            "blocked_symbols": sorted(self._aliases),
            "status": self.status.to_dict(),
        }

    # ------------------------------------------------------------------ #
    # evaluation (no network)
    # ------------------------------------------------------------------ #
    def reevaluate(self) -> None:
        """Re-apply the criteria to the latest market data, listings_fn(),
        stability_fn(), funding_interval_fn() and bars_fn() (no network)."""
        now = self._clock.wall()
        self.interval_s = max(60.0, self._f("pool_refresh_h") * 3600.0)  # owner may change it at runtime
        include = []
        for a in _str_list(self._s("pool_include")):
            k = normalise_asset(a)
            if k and k not in include:
                include.append(k)
        exclude = {normalise_asset(a) for a in _str_list(self._s("pool_exclude"))}
        excl_cats = self._exclude_categories()
        id_cats = self._excluded_ids(excl_cats)
        listings = self._listings()
        targets = self._target_exchanges()

        rows = self._rows_by_asset()
        if self._source == "fallback":
            cand = [a for a in self._fb_pool]
        else:
            cand = list(rows)
        for a in include:
            if a not in cand:
                cand.append(a)
        # Stability source down (OKX unreachable, first minute after start): no candidate has a measurement.
        # Then the step is skipped and noted - otherwise an outage would empty the pool to BTC/ETH.
        self._skip_stability = bool(self._stability_fn is not None and cand
                                    and not any(self._stability(a, now)[0] is not None for a in cand if a not in include))

        evals: dict[str, _Eval] = {}
        for a in cand:
            evals[a] = self._evaluate(a, rows.get(a), now, include=include, exclude=exclude, id_cats=id_cats,
                                      listings=listings, targets=targets)

        def order_key(e: _Eval) -> tuple:
            pa = e.pa
            forced_pos = include.index(pa.asset) if pa.forced else len(include)
            return (forced_pos, pa.rank is None, pa.rank or 0, pa.tier or "Z", pa.asset)

        ok = sorted((e for e in evals.values() if e.pa.forced or not e.fails), key=order_key)
        cap = int(_num(self._s("pool_max_assets")) or 0)
        n_forced = sum(1 for e in ok if e.pa.forced)
        room = max(0, cap - n_forced) if cap > 0 else len(ok)
        chosen: list[_Eval] = []
        taken = 0
        for e in ok:
            if e.pa.forced:
                chosen.append(e)
            elif taken < room:
                chosen.append(e)
                taken += 1
            else:
                rk = f"ранг #{e.pa.rank}" if e.pa.rank else "без ранга"
                e.fails.append(f"лимит пула: {cap} монет, все места заняты более крупными ({rk})")
        chosen_set = {e.pa.asset for e in chosen}

        for e in evals.values():
            pa = e.pa
            pa.included = pa.asset in chosen_set
            if pa.included:
                pa.reasons = [self._included_summary(pa)] + e.notes
                if pa.forced and e.fails:
                    pa.reasons += [f"не прошла бы отбор: {f}" for f in e.fails]
            else:
                pa.reasons = e.fails + e.notes

        old = self._included_set
        self._assets = {a: e.pa for a, e in evals.items()}
        self._included = [e.pa.asset for e in sorted(chosen, key=order_key)]
        self._included_set = frozenset(self._included)
        self._candidates = [e.pa.asset for e in sorted(evals.values(), key=order_key) if e.pa.forced or e.pre_ok]
        self._updated_ts = now
        self._steps = self._describe_steps(now, listings, targets, evals)
        if old != self._included_set:
            added = sorted(self._included_set - old)
            removed = sorted(old - self._included_set)
            log.info("asset pool (%s): %d assets%s%s", self._source, len(self._included),
                     f"; +{','.join(added)}" if added and old else "", f"; -{','.join(removed)}" if removed else "")
        self._save_cache()

    def _evaluate(
        self, asset: str, row: dict | None, now: float, *, include: list[str], exclude: set[str],
        id_cats: dict[str, list[str]], listings: dict[str, dict[str, float | None]] | None,
        targets: set[str] | None,
    ) -> _Eval:
        fails: list[str] = []
        notes: list[str] = []
        forced = asset in include and asset not in exclude
        cg_id = row["id"] if row else self._fb_ids.get(asset, "")
        rank = row["rank"] if row else (self._fb_ranks.get(asset) if self._source == "fallback" else None)
        pa = PoolAsset(asset=asset, rank=rank, cg_id=cg_id, tier=self._tiers.get(asset, ""), source=self._source,
                       forced=forced)
        if row:
            pa.market_cap_usd, pa.volume_24h_usd = row["market_cap"], row["volume"]
            pa.change_24h_pct, pa.change_7d_pct = row["ch24"], row["ch7d"]

        # owner
        if asset in exclude:
            fails.append("исключена владельцем (pool_exclude)")

        # (1) categories, by CoinGecko id - never by an ambiguous ticker
        alias = self.blocked_symbol(asset)
        if alias:
            fails.append(alias)
        if cg_id:
            cats = id_cats.get(cg_id, [])
            pa.categories = list(cats)
            if cats:
                fails.append(self._category_reason(cats))
        elif row is None and not forced:
            fails.append("нет id CoinGecko: категорию проверить нельзя")

        # (1b) market data
        if row is None:
            if self._source == "fallback" and asset in self._fb_pool:
                notes.append(f"рыночные данные: офлайн-список ({self._fallback.get('generated', '?')}), "
                             "капитализация и изменения цены не проверялись")
            else:
                fails.append("нет данных CoinGecko (нет в топ-250 по капитализации)")
        else:
            fails += self._market_fails(row)

        # (2) venues
        if listings is not None:
            venues = listings.get(asset, {})
            pa.venues_signal = sorted(venues)
            min_sig = int(self._f("pool_min_signal_venues"))
            if targets is not None:
                pa.venues_target = sorted(v for v in venues if v in targets)
                pa.target_volumes = {v: venues[v] for v in pa.venues_target}
            if not venues:
                fails.append("нет ни на одной включённой бирже")
            else:
                if len(pa.venues_signal) < min_sig:
                    fails.append(f"мало площадок: {len(pa.venues_signal)} < {min_sig} ({', '.join(pa.venues_signal)})")
                min_tgt = int(self._f("pool_min_target_venues"))
                if targets is not None and len(pa.venues_target) < min_tgt:
                    fails.append(f"мало целевых площадок: {len(pa.venues_target)} < {min_tgt}")
        pre_ok = not fails
        if not (pre_ok or forced):
            return _Eval(pa=pa, fails=fails, notes=notes, pre_ok=False)  # already out: no stability noise

        # (4) stability
        filters = bool(self._s("pool_stability_filters"))
        if not filters:
            notes.append("стабильность не проверяется: берутся топ-монеты по капитализации как есть")
        elif self._stability_fn is not None and getattr(self, "_skip_stability", False):
            notes.append("стабильность: источник пока не дал данных ни по одной монете - шаг временно пропущен")
        elif self._stability_fn is not None:
            st, age, origin = self._stability(asset, now)
            pending = st is None and self._stability_pending(asset)
            if pending and asset in self._included_set:
                # already in the pool, not measured yet (first start, refresh running): it stays until
                # measured - otherwise the pool collapses to BTC/ETH while OKX is queried one by one
                notes.append("стабильность: замер ещё не выполнялся - монета остаётся до замера")
            elif pending:
                fails.append("стабильность ещё не измерена (замер в очереди)")  # a NEW asset enters only measured
            elif st is None:
                fails.append("нет данных о стабильности")
            else:
                pa.stability = dict(st, origin=origin, age_s=round(age, 1) if age is not None else None)
                if origin == "cache":
                    notes.append(f"стабильность из кэша (замер {_age(age)} назад)")
                fails += self._stability_fails(asset, st)
        if filters and self._funding_interval_fn is not None:
            iv = None
            try:
                iv = _num(self._funding_interval_fn(asset))
            except Exception as e:  # noqa: BLE001 - a callback failure is "no data"
                log.debug("asset pool: funding_interval_fn(%s) failed: %s", asset, e)
            if iv is None or iv <= 0:
                notes.append("интервал фандинга: нет данных")
            else:
                pa.funding_interval_h = iv
                min_iv = self._f("pool_min_funding_interval_h")
                if iv < min_iv - 1e-9:
                    fails.append(f"фандинг каждые {iv:g} ч (< {min_iv:g} ч): биржа считает монету волатильной")

        # bars_fn (P3.1 compat): 1-minute spikes
        if filters and self._bars_fn is not None:
            closes: list[float] = []
            try:
                closes = list(self._bars_fn(asset) or [])
            except Exception as e:  # noqa: BLE001
                log.debug("asset pool: bars_fn(%s) failed: %s", asset, e)
            spike = self._f("pool_spike_pct")
            pa.jump = jumpiness(closes, spike)
            need = int(self._f("pool_jump_min_minutes"))
            if pa.jump["minutes"] < need:
                notes.append(f"1-мин. скачки не оценены: {pa.jump['minutes']} мин < {need}")
            else:
                mx = int(self._f("pool_max_spikes"))
                if pa.jump["spikes"] > mx:
                    fails.append(f"1-мин. скачков ≥{spike:g}%: {pa.jump['spikes']} за {pa.jump['minutes']} мин > {mx}")
        return _Eval(pa=pa, fails=fails, notes=notes, pre_ok=pre_ok)

    def _market_fails(self, row: dict) -> list[str]:
        out: list[str] = []
        min_cap = self._f("pool_min_market_cap_usd")
        min_vol = self._f("pool_min_volume_24h_usd")
        cap, vol = row["market_cap"], row["volume"]
        if cap is None:
            out.append("нет данных: капитализация")
        elif cap < min_cap:
            out.append(f"капитализация {_usd(cap)} < {_usd(min_cap)}")
        if vol is None:
            out.append("нет данных: объём за 24 ч")
        elif vol < min_vol:
            out.append(f"объём за 24 ч {_usd(vol)} < {_usd(min_vol)}")
        for key, label, setting in (("ch24", "24 ч", "pool_max_abs_change_24h_pct"),
                                    ("ch7d", "7 дн", "pool_max_abs_change_7d_pct")):
            v, lim = row[key], self._f(setting)
            if v is None:
                out.append(f"нет данных: изменение цены за {label}")
            elif abs(v) > lim:
                out.append(f"изменение цены за {label} {v:+.1f}% (лимит ±{lim:g}%): памп/дамп")
        return out

    def _stability_fails(self, asset: str, st: dict) -> list[str]:
        out: list[str] = []
        days = _num(st.get("days"))
        corr = _num(st.get("btc_corr"))
        jumps = _num(st.get("hour_jumps"))
        mv = _num(st.get("max_7d_move_pct"))
        min_days = self._f("pool_min_history_days")
        if days is None:
            out.append("нет данных о стабильности: длина истории")
        elif days < min_days:
            out.append(f"мало истории свечей: {days:.0f} дн < {min_days:.0f} дн")
        if asset != "BTC":
            min_corr = self._f("pool_min_btc_corr")
            if corr is None:
                out.append("нет данных о стабильности: корреляция с BTC")
            elif corr < min_corr:
                out.append(f"слабая связь с BTC: корреляция {corr:.2f} < {min_corr:.2f}")
        mj = self._f("pool_max_hour_jumps_30d")
        jh = _num(st.get("jump_hour_pct")) or self._f("pool_jump_hour_pct")
        if jumps is None:
            out.append("нет данных о стабильности: часовые скачки")
        elif jumps > mj:
            span = f"{days:.0f} дн" if days is not None else "30 дн"
            out.append(f"собственных часовых скачков ≥{jh:g}%: {jumps:.0f} за {span} > {mj:g}")
        lim = self._f("pool_max_abs_change_7d_pct")
        if mv is None:
            out.append("нет данных о стабильности: макс. движение за 7 дн")
        elif abs(mv) > lim:
            out.append(f"памп/дамп: макс. движение за 7 дн {abs(mv):.0f}% > {lim:g}%")
        return out

    def _stability_pending(self, asset: str) -> bool:
        """The stability source has not tried this asset yet (first start, new candidate):
        the step is deferred, not failed - otherwise the pool collapses while it is measured."""
        if self._stability_pending_fn is None:
            return False
        try:
            return bool(self._stability_pending_fn(asset))
        except Exception as e:  # noqa: BLE001
            log.debug("asset pool: stability_pending_fn(%s) failed: %s", asset, e)
            return False

    def _stability(self, asset: str, now: float) -> tuple[dict | None, float | None, str]:
        """(measurement, age_s, "live" | "cache"): stability_fn first, else the measurement
        persisted in the cache file while younger than stability_max_age_h (restart before
        bars has re-measured); (None, None, "") = no data."""
        live: Any = None
        try:
            live = self._stability_fn(asset) if self._stability_fn is not None else None
        except Exception as e:  # noqa: BLE001 - a callback failure is "no data"
            log.debug("asset pool: stability_fn(%s) failed: %s", asset, e)
        if isinstance(live, dict):
            rec = {k: _num(live.get(k)) for k in _STAB_KEYS if _num(live.get(k)) is not None}
            if any(k in rec for k in ("days", "btc_corr", "hour_jumps")):
                measured = _num(live.get("measured_ts")) or _num(live.get("ts")) or now  # bars.py: "ts"
                rec["measured_ts"] = measured
                old = self._stab_cache.get(asset)
                if old is None or {k: v for k, v in old.items() if k != "measured_ts"} != {
                        k: v for k, v in rec.items() if k != "measured_ts"}:
                    self._dirty = True
                self._stab_cache[asset] = rec
                return rec, max(0.0, now - measured), "live"
        cached = self._stab_cache.get(asset)
        if cached is not None:
            age = now - (_num(cached.get("measured_ts")) or 0.0)
            if 0 <= age <= self._f("stability_max_age_h") * 3600.0:
                return cached, age, "cache"
        return None, None, ""

    def _included_summary(self, pa: PoolAsset) -> str:
        parts: list[str] = []
        if pa.forced:
            parts.append("включена владельцем (pool_include)")
        if pa.rank:
            parts.append(f"ранг #{pa.rank}")
        if pa.market_cap_usd is not None:
            parts.append(f"кап. {_usd(pa.market_cap_usd)}")
        if pa.change_7d_pct is not None:
            parts.append(f"7 дн {pa.change_7d_pct:+.1f}%")
        if pa.venues_signal:
            parts.append(f"площадок {len(pa.venues_signal)} (целевых {len(pa.venues_target)})")
        st = pa.stability
        if st:
            bits = []
            if st.get("btc_corr") is not None:
                bits.append(f"корр. с BTC {st['btc_corr']:.2f}")
            if st.get("hour_jumps") is not None:
                bits.append(f"часовых скачков {st['hour_jumps']:.0f}")
            if bits:
                parts.append(", ".join(bits))
        if pa.funding_interval_h is not None:
            parts.append(f"фандинг {pa.funding_interval_h:g} ч")
        if pa.tier:
            parts.append(f"tier {pa.tier}")
        return "в пуле: " + (", ".join(parts) if parts else "критерии пройдены")

    def _category_reason(self, cats: list[str]) -> str:
        labels: list[str] = []
        ids: list[str] = []
        for c in cats:
            group = _BLOCKLIST_GROUPS.get(c) or self._cat_group.get(c, "")
            lab = _GROUP_LABELS.get(group, "исключённая категория")
            if lab not in labels:
                labels.append(lab)
            if c not in _BLOCKLIST_GROUPS:
                ids.append(c)
        shown = ", ".join(ids[:4]) + (f" и ещё {len(ids) - 4}" if len(ids) > 4 else "")
        if any(c in _BLOCKLIST_GROUPS for c in cats):
            shown = f"{shown}; список блокировки" if shown else "список блокировки"
        return f"категория CoinGecko: {', '.join(labels)} ({shown})"

    def _excluded_ids(self, excl_cats: list[str]) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {}
        for c in excl_cats:
            ids = set(self._frozen.get(c, ())) | set((self._live_cats.get(c) or {}).get("ids") or ())
            for i in ids:
                out.setdefault(i, []).append(c)
        if "meme-token" in excl_cats:
            for i in self._meme_ids:
                out.setdefault(i, []).append("meme-blocklist")
        if "stablecoins" in excl_cats:
            for i in self._stable_ids:
                out.setdefault(i, []).append("stable-blocklist")
        return out

    def _rows_by_asset(self) -> dict[str, dict]:
        """One market row per ticker: the bundled id override when present, else the highest market cap."""
        by_asset: dict[str, dict] = {}
        for r in sorted(self._rows, key=lambda r: (r["rank"] is None, r["rank"] or 0)):
            a = r["symbol"]
            if a in by_asset:
                if self._fb_ids.get(a) == r["id"]:
                    by_asset[a] = r
                continue
            by_asset[a] = r
        return by_asset

    def _listings(self) -> dict[str, dict[str, float | None]] | None:
        if self._listings_fn is None:
            return None
        try:
            raw = self._listings_fn()
        except Exception as e:  # noqa: BLE001
            log.warning("asset pool: listings_fn failed: %s", e)
            return None
        if not isinstance(raw, dict) or not raw:
            return None
        out: dict[str, dict[str, float | None]] = {}
        for a, venues in raw.items():
            k = normalise_asset(a)
            if not k or not isinstance(venues, dict):
                continue
            d = out.setdefault(k, {})
            for ex, vol in venues.items():
                name = str(ex).strip().lower()
                if not name:
                    continue
                v = _num(vol)
                prev = d.get(name)
                d[name] = v if prev is None else (max(prev, v) if v is not None else prev)
        return out or None

    def _target_exchanges(self) -> set[str] | None:
        exs = getattr(self._settings, "exchanges", None)
        if not exs:
            return None
        out: set[str] = set()
        for e in exs:
            if not getattr(e, "enabled", True):
                continue
            role = getattr(e, "role", "")
            role = getattr(role, "value", role)
            if str(role).lower() == "execution":
                out.add(str(getattr(e, "name", "")).strip().lower())
        return out

    def _describe_steps(self, now: float, listings: dict | None, targets: set[str] | None,
                        evals: dict[str, _Eval]) -> dict[str, str]:
        steps: dict[str, str] = {}
        if self._source == "fallback":
            steps["market"] = (f"офлайн-список ({self._fallback.get('generated', '?')}): "
                               "CoinGecko ещё не ответил, рыночные критерии не проверялись")
        else:
            src = "CoinGecko" if self._source == "coingecko" else "кэш CoinGecko"
            age = (now - self._markets_ts) if self._markets_ts is not None else None
            steps["market"] = f"{src}: {len(self._rows)} монет, возраст {_age(age)}"
        live = self._live_categories()
        fresh = sum(1 for c in live if (self._live_cats.get(c) or {}).get("fetched_ts"))
        steps["categories"] = (f"id CoinGecko: офлайн-список ({sum(len(v) for v in self._frozen.values())} id) "
                               f"+ живое обновление {fresh}/{len(live)} категорий")
        if listings is None:
            steps["venues"] = "нет данных о площадках: шаг пропущен"
        elif targets is None:
            steps["venues"] = f"листинги {len(listings)} монет; роли бирж неизвестны: целевые площадки не проверены"
        else:
            steps["venues"] = f"листинги {len(listings)} монет на включённых биржах, целевых бирж {len(targets)}"
        steps["turnover"] = "оборот целевой площадки проверяется на каждом сигнале (min_target_volume_quote)"
        if self._stability_fn is None:
            steps["stability"] = "источник стабильности не подключён: шаг пропущен"
        elif getattr(self, "_skip_stability", False):
            steps["stability"] = "OKX 1H: данных пока нет ни по одной монете - шаг временно пропущен"
        else:
            have = sum(1 for e in evals.values() if e.pa.stability)
            steps["stability"] = f"OKX 1H за 30 дн: данные есть для {have} из {len(evals)}"
        steps["funding"] = ("интервал фандинга не подключён: шаг пропущен" if self._funding_interval_fn is None
                            else f"интервал фандинга < {self._f('pool_min_funding_interval_h'):g} ч = исключена; "
                                 "нет данных = не оценивается")
        steps["jumps_1m"] = ("1-мин. бары не подключены" if self._bars_fn is None
                             else "1-мин. скачки по нашим барам")
        return steps

    # ------------------------------------------------------------------ #
    # network
    # ------------------------------------------------------------------ #
    async def step(self) -> bool:
        ok = await super().step()
        if ok and self._due_categories(self._clock.wall()):
            # call budget ran out mid-refresh: continue in a minute, not in pool_refresh_h
            self._next_mono = min(self._next_mono, self._clock.mono() + 61.0)
        return ok

    async def poll_once(self) -> int:
        """CoinGecko markets (top 250 by market cap, 24 h / 7 d change) + the excluded
        category lists (within the call budget) -> cache file -> reevaluate()."""
        if self._http is None:
            raise SourceError("coingecko: HTTP-клиент не передан")
        now = self._clock.wall()
        if self._markets_ts is None or now - self._markets_ts >= 0.75 * self.interval_s:
            await self._fetch_markets()  # a continuation poll (pending categories) keeps the fresh rows
        rate_limited: RateLimitedSource | None = None
        min_cap = self._interest_cap()
        for cat in self._due_categories(now):
            if self._budget_wait() > 0:
                break
            try:
                await self._fetch_category(cat, min_cap)
            except RateLimitedSource as e:
                rate_limited = e
                break
            except SourceError as e:
                rec = self._live_cats.setdefault(cat, {})
                rec["attempt_ts"] = self._clock.wall()
                rec["error"] = str(e)[:200]
                log.warning("asset pool: category %s refresh failed: %s", cat, e)
        self.reevaluate()
        self._save_cache(force=True)
        self._update_note()
        if rate_limited is not None:
            raise rate_limited
        return len(self._included)

    async def _fetch_markets(self) -> None:
        params = {
            "vs_currency": "usd", "order": "market_cap_desc", "per_page": CG_PER_PAGE, "page": 1,
            "price_change_percentage": "24h,7d", "sparkline": "false",
        }
        data, _ = await self._cg(params, what="markets")
        rows = [r for r in (_parse_market_row(x) for x in data) if r]
        if not rows:
            raise SourceError("coingecko: пустой список монет")
        self._rows = rows
        self._markets_ts = self._clock.wall()
        self._source = "coingecko"
        self._dirty = True

    async def _fetch_category(self, cat: str, min_cap: float) -> None:
        """Members of one category, market cap descending; further pages only while
        they can still hold candidates (last row's cap >= min_cap) and ``total`` says
        there are more.  Out of call budget mid-way: the ids so far are merged and
        the category stays due (continued by the next step)."""
        ids: set[str] = set()
        max_pages = max(1, int(self._f("pool_category_max_pages")))
        page = 1
        state = "complete"
        while True:
            params = {"vs_currency": "usd", "category": cat, "order": "market_cap_desc",
                      "per_page": CG_PER_PAGE, "page": page, "sparkline": "false"}
            data, resp = await self._cg(params, what=f"category {cat}")
            for r in data:
                if isinstance(r, dict) and r.get("id"):
                    ids.add(str(r["id"]))
            total = _int(resp.headers.get("total"))
            last_cap = _num(data[-1].get("market_cap")) if data and isinstance(data[-1], dict) else None
            more = (len(data) >= CG_PER_PAGE and (total is None or page * CG_PER_PAGE < total)
                    and last_cap is not None and last_cap >= min_cap)
            if not more:
                break
            if page >= max_pages:
                state = "truncated"
                break
            if self._budget_wait() > 0:
                state = "budget"
                break
            page += 1
        now = self._clock.wall()
        rec = self._live_cats.setdefault(cat, {})
        rec["error"] = ""
        if state == "budget":
            rec["ids"] = sorted(ids | set(rec.get("ids") or ()))
        else:
            rec["ids"] = sorted(ids)
            rec["fetched_ts"] = rec["attempt_ts"] = now
            if state == "truncated":
                rec["error"] = f"только первые {page} стр. (pool_category_max_pages)"
        self._dirty = True

    async def _cg(self, params: dict[str, Any], *, what: str) -> tuple[list, HttpResponse]:
        wait = self._budget_wait()
        if wait > 0:
            raise RateLimitedSource(wait, f"coingecko: бюджет {self._calls_per_min()} запросов/мин исчерпан")
        self._calls.append(self._clock.mono())
        headers = {"accept": "application/json"}
        if self._api_key:
            headers["x-cg-demo-api-key"] = self._api_key
        resp = await self._http.get(COINGECKO_MARKETS_URL, params=params, headers=headers, timeout_s=15.0)
        check_response(resp, f"coingecko {what}", default_retry_s=60.0)
        try:
            data = resp.json()
        except ValueError:
            raise SourceError(f"coingecko {what}: ответ не JSON") from None
        if not isinstance(data, list):
            code = ""
            if isinstance(data, dict):
                st = data.get("status")
                code = str(st.get("error_code", "")) if isinstance(st, dict) else str(data.get("error", ""))[:60]
            raise SourceError(f"coingecko {what}: неожиданный ответ {code}".rstrip())
        return data, resp

    def _budget_wait(self) -> float:
        """Seconds until the per-minute call budget has room (0 = call now)."""
        now = self._clock.mono()
        while self._calls and now - self._calls[0] >= 60.0:
            self._calls.popleft()
        if len(self._calls) < self._calls_per_min():
            return 0.0
        return max(1.0, 60.0 - (now - self._calls[0]))

    def _interest_cap(self) -> float:
        """Category pages are followed only while they still hold coins that could be candidates."""
        caps = [r["market_cap"] for r in self._rows if r["market_cap"] is not None]
        floor = self._f("pool_min_market_cap_usd")
        return min([floor] + caps) if caps else floor

    def _due_categories(self, now: float) -> list[str]:
        refresh_s = self.interval_s
        out = []
        for c in self._live_categories():
            rec = self._live_cats.get(c) or {}
            last = max(_num(rec.get("fetched_ts")) or 0.0, _num(rec.get("attempt_ts")) or 0.0)
            if not last or now - last >= 0.75 * refresh_s:
                out.append(c)
        return out

    def _update_note(self) -> None:
        key = "с ключом" if self._api_key else "без ключа"
        src = {"coingecko": "CoinGecko", "cache": "кэш", "fallback": "офлайн-список"}.get(self._source, self._source)
        pending = len(self._due_categories(self._clock.wall()))
        note = f"CoinGecko {key}; источник: {src}; в пуле {len(self._included)}"
        if pending:
            note += f"; категорий в очереди: {pending}"
        self.status.note = note

    # ------------------------------------------------------------------ #
    # cache file
    # ------------------------------------------------------------------ #
    def _load_cache(self) -> bool:
        if self._cache_path is None:
            return False
        try:
            doc = json.loads(self._cache_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return False
        except (OSError, ValueError) as e:
            log.warning("asset pool: cache %s unreadable (%s): using the bundled fallback", self._cache_path, e)
            return False
        if not isinstance(doc, dict):
            return False
        cats = doc.get("categories")
        if isinstance(cats, dict):
            for c, rec in cats.items():
                if isinstance(rec, dict):
                    self._live_cats[str(c)] = {
                        "fetched_ts": _num(rec.get("fetched_ts")), "attempt_ts": _num(rec.get("attempt_ts")),
                        "ids": [str(i) for i in rec.get("ids") or []], "error": str(rec.get("error") or ""),
                    }
        stab = doc.get("stability")
        if isinstance(stab, dict):
            for a, rec in stab.items():
                if isinstance(rec, dict) and _num(rec.get("measured_ts")) is not None:
                    keep = _STAB_KEYS + ("measured_ts",)
                    self._stab_cache[normalise_asset(a)] = {
                        k: _num(v) for k, v in rec.items() if k in keep and _num(v) is not None}
        rows = [r for r in (_parse_market_row(x) for x in doc.get("markets") or []) if r]
        if not rows:
            return False
        self._rows = rows
        self._markets_ts = _num(doc.get("fetched_ts"))
        self._source = "cache"
        return True

    def _save_cache(self, *, force: bool = False) -> None:
        if self._cache_path is None or not self._dirty:
            return
        now_m = self._clock.mono()
        if not force and self._last_save_mono is not None and now_m - self._last_save_mono < _SAVE_MIN_INTERVAL_S:
            return
        doc = {
            "version": 1,
            "fetched_ts": self._markets_ts if self._source != "fallback" else None,
            "saved_ts": self._clock.wall(),
            "markets": self._rows if self._source != "fallback" else [],
            "categories": self._live_cats,
            "stability": self._stab_cache,
        }
        tmp = self._cache_path.with_name(self._cache_path.name + ".tmp")
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps(doc, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
            os.replace(tmp, self._cache_path)
        except OSError as e:
            log.warning("asset pool: cannot write cache %s: %s", self._cache_path, e)
            return
        self._dirty = False
        self._last_save_mono = now_m


__all__ = [
    "AssetPool", "COINGECKO_MARKETS_URL", "DEFAULT_EXCLUDE_CATEGORIES", "MEME_CATEGORIES", "DEFAULT_LIVE_CATEGORIES", "FALLBACK_PATH",
    "POOL_DEFAULTS", "PoolAsset", "SYMBOL_MAP", "jumpiness", "load_fallback", "normalise_asset",
]
