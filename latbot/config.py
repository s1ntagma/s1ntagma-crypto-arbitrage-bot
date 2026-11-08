"""Configuration.

Two layers:
  1. ``.env``          - secrets + machine settings (never shown in UI / logs).
  2. ``config.json``   - user-tunable runtime parameters (editable from the UI;
                         Settings.update() persists it).

Secrets are kept in ``Secrets`` and are NEVER included in ``Settings.public()``.

Exchange API keys use slot placeholders (see .env.example):
    EXCHANGE_1_NAME=bybit
    EXCHANGE_1_API_KEY=
    EXCHANGE_1_API_SECRET=
    EXCHANGE_1_PASSPHRASE=        (okx / coinbase only)
"""
from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from .logging_setup import register_secret
from .models import ExchangeRole, Mode

log = logging.getLogger(__name__)

MAX_EXCHANGE_SLOTS = 8


# --------------------------------------------------------------------------- #
# Secrets (from .env only)
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class ExchangeCredentials:
    name: str
    api_key: str = ""
    api_secret: str = ""
    passphrase: str = ""

    @property
    def present(self) -> bool:
        return bool(self.api_key and self.api_secret)


@dataclass(slots=True)
class Secrets:
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    coingecko_api_key: str = ""
    anthropic_api_key: str = ""  # optional: screenshot / trade analysis through the Claude API
    bls_api_key: str = ""  # optional: BLS API v2 key (US jobs / CPI actuals); the public feeds work without it
    exchanges: dict[str, ExchangeCredentials] = field(default_factory=dict)

    def creds(self, exchange: str) -> ExchangeCredentials:
        return self.exchanges.get(exchange, ExchangeCredentials(name=exchange))


def load_secrets(env_path: str | os.PathLike[str] | None = ".env") -> Secrets:
    if env_path and Path(env_path).exists():
        load_dotenv(env_path, override=False)
    s = Secrets(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        coingecko_api_key=os.getenv("COINGECKO_API_KEY", "").strip(),
        anthropic_api_key=os.getenv("ANTHROPIC_API_KEY", "").strip(),
        bls_api_key=os.getenv("BLS_API_KEY", "").strip(),
    )
    register_secret(s.bls_api_key)
    register_secret(s.telegram_bot_token)
    register_secret(s.coingecko_api_key)
    register_secret(s.anthropic_api_key)
    for i in range(1, MAX_EXCHANGE_SLOTS + 1):
        name = os.getenv(f"EXCHANGE_{i}_NAME", "").strip().lower()
        if not name:
            continue
        c = ExchangeCredentials(
            name=name,
            api_key=os.getenv(f"EXCHANGE_{i}_API_KEY", "").strip(),
            api_secret=os.getenv(f"EXCHANGE_{i}_API_SECRET", "").strip(),
            passphrase=os.getenv(f"EXCHANGE_{i}_PASSPHRASE", "").strip(),
        )
        register_secret(c.api_key)
        register_secret(c.api_secret)
        register_secret(c.passphrase)
        s.exchanges[name] = c
    # Named fallbacks (BYBIT_API_KEY=...) for convenience.
    for name in ("binance", "bybit", "okx", "kraken", "coinbase", "gate", "bitstamp", "bitvavo", "cryptocom", "revolutx"):
        key = os.getenv(f"{name.upper()}_API_KEY", "").strip()
        sec = os.getenv(f"{name.upper()}_API_SECRET", "").strip()
        if key and sec and name not in s.exchanges:
            c = ExchangeCredentials(
                name=name, api_key=key, api_secret=sec, passphrase=os.getenv(f"{name.upper()}_PASSPHRASE", "").strip()
            )
            register_secret(key)
            register_secret(sec)
            register_secret(c.passphrase)
            s.exchanges[name] = c
    return s


# --------------------------------------------------------------------------- #
# Exchange configuration (public)
# --------------------------------------------------------------------------- #
@dataclass
class ExchangeConfig:
    name: str  # internal name, also ccxt id unless ccxt_id set
    enabled: bool = True
    role: ExchangeRole = ExchangeRole.EXECUTION
    ccxt_id: str = ""  # e.g. "myokx" for OKX EEA; defaults to name
    hostname: str = ""  # optional ccxt hostname override (e.g. EU domains)
    market_type: str = "spot"  # "spot" | "swap" (linear USDT perpetuals)
    allow_short: bool = False  # SHORT opportunities (direction -1); swap only
    taker_fee: float = 0.001  # fraction; overridden by live fee info when available
    maker_fee: float = 0.001
    book_depth: int = 10
    demo: bool = False  # use exchange demo/testnet endpoint (Bybit Demo Trading)
    rate_limit_per_s: float = 10.0
    weight: float = 1.0  # tie-breaker for target selection (1 = neutral)
    # Streaming: "book" = full order book (needed for execution targets),
    # "bbo" = best bid/ask only (cheap; enough for move detection on signal sources),
    # "rest" = venue has no WebSocket: ticker polling + REST order book (Revolut X).
    stream: str = "book"
    max_ws_subscriptions: int = 0  # 0 = no limit; Coinbase breaks above ~4 book streams
    poll_tickers: bool = True  # broad coverage via REST fetch_tickers (all common assets)
    pair_url: str = ""  # override template for the "open pair" button (see market/links.py)
    retry_after_ms: bool = False  # the venue's Retry-After header is in milliseconds (Revolut X), not HTTP seconds
    # Phase 3 leverage plan (advice only, no leveraged orders): what YOUR account allows on this venue
    max_leverage: float = 1.0  # 1 = spot only
    margin_kind: str = "spot"  # "spot" | "margin" | "perp"
    maintenance_margin_rate: float = 0.005
    borrow_rate_daily_pct: float = 0.0  # margin borrow cost per day, %
    margin_open_fee_pct: float = 0.0  # margin opening fee, %
    label: str = ""  # display name ("OKX EEA", "Bybit EU")

    @property
    def ccxt_name(self) -> str:
        return self.ccxt_id or self.name

    @property
    def display(self) -> str:
        return self.label or self.name.capitalize()


# Manual-first default universe (owner trades by hand on EU-licensed venues):
#   EXECUTION (targets) = exchanges where the owner has an account.
#   SIGNAL = fast venues used only to see that "the market already moved".
# Prices are compared as PERCENT moves per exchange, so EUR / USDC / USD quoted
# markets are comparable without an FX conversion.
DEFAULT_EXCHANGES: list[ExchangeConfig] = [
    # ---- targets (manual trading venues) ----------------------------------
    ExchangeConfig(name="kraken", role=ExchangeRole.EXECUTION, taker_fee=0.004, maker_fee=0.0025, label="Kraken"),
    ExchangeConfig(
        name="coinbase", role=ExchangeRole.EXECUTION, taker_fee=0.006, maker_fee=0.004, max_ws_subscriptions=4, label="Coinbase"
    ),
    ExchangeConfig(name="bitvavo", role=ExchangeRole.EXECUTION, taker_fee=0.0025, maker_fee=0.0015, label="Bitvavo"),
    ExchangeConfig(name="bitstamp", role=ExchangeRole.EXECUTION, taker_fee=0.004, maker_fee=0.003, label="Bitstamp"),
    ExchangeConfig(name="cryptocom", role=ExchangeRole.EXECUTION, taker_fee=0.0075, maker_fee=0.0025, label="Crypto.com"),
    ExchangeConfig(name="okx", ccxt_id="myokx", role=ExchangeRole.EXECUTION, taker_fee=0.001, maker_fee=0.0008, label="OKX EEA"),
    # Bybit: Demo Trading (api-demo.bybit.com) is the paper venue carried over from the previous bot.
    ExchangeConfig(
        name="bybit", role=ExchangeRole.EXECUTION, market_type="spot", demo=True,
        taker_fee=0.001, maker_fee=0.001, label="Bybit EU",
    ),
    ExchangeConfig(name="gate", role=ExchangeRole.EXECUTION, taker_fee=0.002, maker_fee=0.002, label="Gate EU"),
    # Revolut X: REST API only (no WebSocket) -> prices by ticker polling, book by REST before a signal.
    ExchangeConfig(name="revolutx", role=ExchangeRole.EXECUTION, stream="rest", taker_fee=0.0009, maker_fee=0.0, label="Revolut X",
                   retry_after_ms=True),
    # ---- signal sources only (not MiCA venues; never targets) ----------------
    ExchangeConfig(name="binance", role=ExchangeRole.SIGNAL, stream="bbo", taker_fee=0.001, maker_fee=0.001, label="Binance"),
    ExchangeConfig(name="kucoin", role=ExchangeRole.SIGNAL, stream="bbo", taker_fee=0.001, maker_fee=0.001, label="KuCoin"),
    ExchangeConfig(name="mexc", role=ExchangeRole.SIGNAL, stream="bbo", taker_fee=0.001, maker_fee=0.0, label="MEXC"),
    ExchangeConfig(name="bitget", role=ExchangeRole.SIGNAL, stream="bbo", taker_fee=0.001, maker_fee=0.001, label="Bitget"),
    ExchangeConfig(name="htx", role=ExchangeRole.SIGNAL, stream="bbo", taker_fee=0.002, maker_fee=0.002, label="HTX"),
]


# --------------------------------------------------------------------------- #
# Runtime settings (public, user-tunable)
# --------------------------------------------------------------------------- #
# Bumped when a saved config.json needs a one-time upgrade (see Settings._migrate).
CONFIG_VERSION = 7

# owner (2026-11-08): "топ-50 - топ-100 монет в мире" - only coins that are not a tradable coin of their own
# are left out (stablecoins, wrapped / bridged / staked receipts, tokenized gold / stocks, fan tokens)
TOP_EXCLUDE_CATEGORIES: list[str] = [
    "stablecoins", "usd-stablecoin", "eur-stablecoin", "fiat-backed-stablecoin", "crypto-backed-stablecoin",
    "algorithmic-stablecoin", "yield-bearing-stablecoins", "synthetic-dollar", "bridged-stablecoins",
    "wrapped-tokens", "liquid-staking-tokens", "liquid-staked-eth", "liquid-staked-sol",
    "liquid-restaking-tokens", "liquid-restaked-eth", "bridged-tokens", "bridged-wbtc", "bridged-weth",
    "bridged-usdc", "bridged-usdt", "tokenized-gold", "gold", "tokenized-commodities", "tokenized-silver",
    "commodity-backed-stablecoin", "fan-token", "tokenized-products", "tokenized-stock", "tokenized-treasuries",
]
TOP_N = 100


@dataclass
class Settings:
    config_version: int = CONFIG_VERSION
    # ---- modes -----------------------------------------------------------
    paper_mode: bool = True  # PAPER_MODE: execution engine uses paper adapters
    live_mode: bool = False  # LIVE_MODE: allow real orders (requires keys)
    paper_engine: str = "internal"  # "internal" simulator | "bybit_demo"
    paper_balance_quote: float = 10_000.0
    auto_confirm: bool = False  # AUTO_CONFIRM
    record_market_data: bool = False  # write JSONL for backtests

    # ---- universe --------------------------------------------------------
    # Assets (base currencies).  Each exchange resolves its own market for an
    # asset using market_type + quote_assets preference (USD-equivalents only,
    # so prices are comparable across exchanges without FX).
    watchlist: list[str] = field(
        default_factory=lambda: ["BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "LINK", "AVAX", "DOT", "LTC"]
    )
    watchlist_auto: bool = False  # pick most liquid assets on watchlist_exchange at start
    watchlist_size: int = 15  # 3..20 when watchlist_auto
    watchlist_exchange: str = "kraken"
    # Quote preference per exchange market resolution.  Moves are compared in
    # percent, so EUR / USDC / USD / USDT quoted markets are all comparable.
    quote_assets: list[str] = field(default_factory=lambda: ["EUR", "USDC", "USD", "USDT"])
    # Signal venues only say where the market moves: their deepest market is watched (Binance BTC/USDT
    # trades 200x BTC/EUR; EUR-first made a smaller venue's USDT book the "leader").  Execution venues
    # keep ``quote_assets`` (the owner's money is in EUR).
    signal_quote_assets: list[str] = field(default_factory=lambda: ["USDT", "USDC", "USD", "EUR"])
    # ---- SPOT-FUTURES BASIS SCANNER (research module: virtual pairs only, never an order) -----------
    # Per venue: the spot market and the linear perpetual of the same asset and quote (USDT / USD).
    basis_enabled: bool = False  # library default off (tests); load_settings turns it on for the owner
    basis_venues: list[str] = field(default_factory=lambda: ["okx", "bybit", "htx", "kraken"])
    basis_assets: list[str] = field(default_factory=lambda: ["BTC", "ETH", "SOL", "XRP", "DOGE"])  # [] = the pool
    basis_top_n: int = 100  # with basis_assets empty: the first N coins of the pool (market-cap order)
    basis_notional_quote: float = 1000.0  # virtual size of each leg (quote currency)
    basis_mode: str = "both"  # what opens a pair: fut_upper | spot_lower | any | both
    basis_fut_upper_pct: float = 0.15  # perp best bid >= rolling mean perp mid * (1 + this %)
    basis_spot_lower_pct: float = 0.15  # spot best ask <= rolling mean spot mid * (1 - this %)
    basis_ref_window_s: float = 300.0  # rolling window of the reference means
    basis_exit_basis_pct: float = 0.02  # close when the executable exit basis falls to this %
    basis_min_convergence_pct: float = 0.05  # ... and at least this far below the entry basis
    basis_max_hold_s: float = 3600.0
    basis_latency_ms: float = 300.0  # signal -> fill delay of each leg in the simulation
    basis_max_skew_ms: float = 500.0  # the two books must be this close in time
    basis_max_age_ms: float = 2000.0  # and this fresh
    basis_max_slippage_pct: float = 0.3  # a simulated market order walks the book at most this far
    basis_funding_poll_s: float = 300.0
    basis_history_s: float = 3600.0
    exchanges: list[ExchangeConfig] = field(default_factory=lambda: [ExchangeConfig(**asdict(e)) for e in DEFAULT_EXCHANGES])
    # ---- coverage: WS for the core watchlist, REST ticker polling for breadth --
    ws_assets: int = 25  # WebSocket streams per exchange: the whole Phase 3 pool (REST polling is 1-2 s slower)
    ticker_poll_enabled: bool = True  # fetch_tickers on every exchange for ALL common assets
    ticker_poll_interval_s: float = 2.0
    # a streamed asset whose WebSocket delivered nothing for this long gets its prices from REST polling;
    # a signal venue without a price point that recent cannot be the leader (a silent stream is no stream)
    ws_silent_s: float = 10.0
    # re-subscribe every WebSocket book stream this often (a fresh snapshot: a silent reconnect cannot
    # keep stale levels in the library's cache); 0 = off
    ws_book_resubscribe_s: float = 300.0
    poll_assets_max: int = 600  # cap on polled assets (common to >= 2 exchanges)
    confirm_with_rest_book: bool = True  # for polled assets fetch the target REST book before creating an opportunity
    rest_book_timeout_s: float = 2.0  # REST book confirmation: per-request timeout (single-flight, bounded per exchange)
    price_sample_min_interval_ms: float = 100.0  # throttle mid-price series appends per stream
    tick_outlier_pct: float = 0.5  # a lone ticker tick this far from the series waits for a confirming tick (0 = off)
    # ---- signal quality --------------------------------------------------
    # ---- SIMPLE MODE (owner): the coin moved on its most liquid venue, the target has not moved yet ->
    # buy there, sell when it catches up or after simple_max_hold_s.  Quality / context are info only.
    # The library default is off (tests, 'full' mode); load_settings turns it on for the owner.
    simple_mode: bool = False
    simple_entry_deadline_s: float = 3.0  # no new entry later than this after the leader moved
    simple_max_hold_s: float = 15.0  # window, shadow trade and AUTO exit cap (without ride)
    # RIDE THE LEADER (simple mode): enter while the leader's move is intact, hold while it keeps going,
    # sell when the move is over (engine/exit_policy.py).  Replaces the 3 s / 15 s limits above.
    ride_enabled: bool = True
    ride_entry_max_age_s: float = 60.0  # no new entry later than this after the leader's move began
    ride_reversal_pct: float = 0.3  # the leader fell this far from its peak -> the move is over
    ride_stop_pct: float = 1.0  # NET on the position <= -this -> sell
    ride_lag_closed_pct: float = 0.1  # the target caught up to within this ...
    ride_stall_s: float = 5.0  # ... and the leader made no new high for this long -> take the profit
    ride_trail_arm_pct: float = 0.3  # once NET >= this ...
    ride_trail_pct: float = 0.3  # ... a pull-back of the target by this from its peak -> sell
    ride_max_hold_s: float = 300.0  # safety: sell at market after this
    ride_stale_s: float = 30.0  # no fresh price for the position this long -> close as "no data"
    min_movers: int = 2  # at least this many exchanges must have moved (single-venue spikes revert)
    min_mover_volume_quote: float = 1_000_000.0  # 24h quote volume for a mover exchange to count
    min_target_volume_quote: float = 500_000.0  # 24h quote volume on the target market (owner: >= ~500k)
    alert_human_only: bool = True  # Telegram only when HUMAN light != RED (owner trades by hand)

    # ---- human / bot execution ------------------------------------------
    human_execution_time_s: float = 7.0  # HUMAN_EXECUTION_TIME
    human_green_margin: float = 1.5  # GREEN if remaining >= time * margin
    bot_green_margin: float = 3.0
    bot_yellow_margin: float = 1.2
    default_api_latency_ms: float = 250.0  # MAX_API_LATENCY baseline when no stats
    max_api_latency_ms: float = 1500.0  # bot RED above this
    bot_fixed_overhead_ms: float = 150.0  # processing + revalidation

    # ---- detection -------------------------------------------------------
    move_window_s: float = 5.0  # window over which a "move" is measured
    move_threshold_pct: float = 0.35  # first mover must move at least this
    min_lag_pct: float = 0.15  # gross lag to consider
    catchup_ratio_default: float = 0.6  # expected fraction of lag recovered (learned)
    default_opp_duration_s: float = 8.0  # when no history
    min_opp_duration_s: float = 0.5
    max_opp_duration_s: float = 120.0
    price_history_s: float = 60.0  # per exchange/symbol rolling window

    # ---- economics / thresholds -----------------------------------------
    capital_quote: float = 100.0  # default trade size shown in cards
    min_trade_quote: float = 10.0
    min_net_pct_show: float = 0.1  # show inside bot if net >= this (NOT 3 %)
    min_net_pct_bot: float = 0.3  # bot RED below this
    min_net_pct_auto: float = 0.5  # AUTO execution requires at least this at revalidation
    telegram_profit_threshold_pct: float = 3.0  # TELEGRAM_PROFIT_THRESHOLD
    shadow_trading: bool = True  # virtually trade every approved opportunity on the real book (no orders)
    analysis_model: str = "claude-opus-5-5"  # Claude model for screenshot / trade review (needs ANTHROPIC_API_KEY)
    telegram_require_executable: bool = True
    max_slippage_pct: float = 0.3  # MAX_SLIPPAGE: liquidity counted only within this
    min_liquidity_quote: float = 50.0  # MIN_LIQUIDITY on both sides within max_slippage
    min_fill_ratio: float = 0.5  # executable/capital below this -> flag liquidity_reason
    execution_loss_pct: float = 0.05  # adverse move during bot execution (per round trip)
    revalidate_tolerance: float = 0.5  # current net must be >= expected * this at execution
    max_data_age_ms: float = 1500.0  # MAX_DATA_AGE: stale book -> not usable
    stale_exchange_s: float = 10.0  # no messages for this long -> exchange stale

    # ---- capital / risk ---------------------------------------------------
    total_capital_quote: float = 1_000.0
    max_trade_quote: float = 200.0  # MAX_TRADE_SIZE
    max_simultaneous_trades: int = 2
    max_risk_per_trade_pct: float = 2.0  # MAX_RISK (of total capital)
    max_daily_loss_quote: float = 50.0  # MAX_DAILY_LOSS
    max_exposure_per_exchange_quote: float = 500.0
    max_exposure_per_asset_quote: float = 300.0
    # what AUTO may keep in open trades on ONE venue ("сколько я закину на каждую биржу"); a venue
    # without an entry is limited by max_exposure_per_exchange_quote and its real free balance only
    exchange_budget_quote: dict[str, float] = field(default_factory=dict)
    live_requires_ready: bool = True  # LIVE AUTO only while the Stats READY checklist says yes
    autopilot: bool = False  # AUTO without a click (execution/autopilot.py); every gate still applies
    allow_downsize: bool = True  # if balance < capital -> use max available

    # ---- execution behaviour ---------------------------------------------
    buy_limit_slippage_pct: float = 0.2  # IOC limit = ask_vwap * (1 + this)
    buy_fill_timeout_s: float = 3.0
    sell_timeout_s: float = 20.0  # wait for catch-up sell this long
    max_hold_s: float = 60.0  # after this, exit at market
    exit_on_timeout: str = "market"  # "market" | "hold"
    min_partial_fill_ratio: float = 0.2  # below this, cancel and abort

    # ---- Phase 3: selective, context-aware signals (ARCHITECTURE "Phase 3") ----
    # Training: every opportunity with NET >= this is traded virtually (shadow), whatever its quality.
    shadow_min_net_pct: float = 0.1
    shadow_balance_quote: float = 1000.0  # virtual training balance: stakes come out of it, results go back in
    shadow_train_all_signals: bool = True  # ride mode: virtually enter every priced signal the model rejects (training only, never offered)
    shadow_unlimited: bool = True  # training on unlimited virtual money: no free-balance check, best size per offer
    best_size_multipliers: list[float] = field(default_factory=lambda: [0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 50.0])
    # PROVEN PAIRS (simple mode): only (asset, venue) pairs whose own virtual trades proved themselves are
    # offered on the board, in Telegram and to AUTO; everything else keeps training (engine/proven.py)
    proven_gate_enabled: bool = True
    proven_min_trades: int = 30
    proven_min_p10: float = 0.60  # worst-case win probability (the proven_quantile of Beta(1+wins, 1+losses))
    proven_quantile: float = 0.05  # 5th percentile: strict, the verdict is re-checked after every trade
    proven_min_avg_net_pct: float = 0.05
    # LIVE readiness checklist (Stats)
    ready_min_trades: int = 200
    ready_max_drawdown_pct: float = 5.0  # on the cumulative NET % curve (size-independent)
    ready_min_proven_win_rate: float = 0.60
    shadow_stop_pct: float = 1.0  # shadow stop barrier when the leverage plan has no stop
    # quality score (engine/quality.py) - fixed, documented weights; never fitted to past trades
    min_quality_score: int = 60  # below: hidden by default, no Telegram, no AUTO (still shadow-traded)
    quality_event_window_bonus: int = 10  # required score is higher around high-impact US releases
    quality_history_k: float = 10.0
    quality_weights: dict = field(default_factory=lambda: {
        "edge": 0.20, "breadth": 0.10, "depth": 0.10, "history": 0.20, "timing": 0.10, "context": 0.30})
    context_weights: dict = field(default_factory=lambda: {
        "macro_event": 1.0, "news": 1.0, "whale": 0.4, "funding": 0.6, "open_interest": 0.5,
        "liquidations": 0.5, "btc_regime": 0.8, "tradfi": 0.3, "sentiment": 0.2, "event_reaction": 0.8,
        "erratic": 1.0})
    # meta-label table (engine/metalabel.py): outcomes of comparable situations
    metalabel_min_n: int = 20
    metalabel_kappa: float = 10.0
    metalabel_keep: int = 20_000
    # leverage plan (engine/leverage.py): advice + virtual P&L only, no leveraged orders are sent
    max_leverage: float = 5.0
    leverage_tiers: list = field(default_factory=lambda: [[70, 2], [80, 3], [90, 5]])
    leverage_liq_buffer: float = 3.0  # liquidation distance >= this x stop distance
    leverage_risk_frac_pct: float = 0.5  # loss at stop <= this % of total capital (risk-first sizing)
    leverage_min_net_pct: float = 0.4
    leverage_min_rr: float = 1.5
    leverage_kelly_fraction: float = 0.25
    leverage_min_stop_pct: float = 0.3
    leverage_stop_sigma: float = 2.5
    # market context (context/*)
    context_enabled: bool = True
    macro_block_before_s: float = 300.0  # high-impact US release ahead -> veto
    macro_block_after_s: float = 120.0
    macro_window_after_s: float = 3600.0
    reaction_horizons_s: list = field(default_factory=lambda: [10, 60, 300, 1800])
    reaction_min_n: int = 5
    erratic_min_1m_pct: float = 1.0
    erratic_max_5m_pct: float = 3.0
    pool_jump_hour_pct: float = 3.0
    # narrow pool (universe/pool.py): objective data + measured stability, no meme / hype / erratic coins
    pool_enabled: bool = True
    pool_max_assets: int = 25
    pool_stability_filters: bool = True  # False: the top coins by market cap as they are
    pool_exclude_categories: list | None = None  # None: the pool's default (memes excluded too)
    pool_min_market_cap_usd: float = 1e9
    pool_min_volume_24h_usd: float = 2e7  # global 24h turnover (CoinGecko); the target venue's own >= min_target_volume_quote
    pool_max_abs_change_24h_pct: float = 15.0  # hype pump / dump today
    pool_max_abs_change_7d_pct: float = 100.0
    pool_include: list = field(default_factory=lambda: ["BTC", "ETH"])
    pool_exclude: list = field(default_factory=list)
    pool_min_signal_venues: int = 3
    pool_min_target_venues: int = 1
    pool_min_btc_corr: float = 0.40  # 30-day HOURLY correlation with BTC (0.55 was calibrated on daily data)
    pool_max_hour_jumps_30d: int = 20
    pool_min_history_days: int = 20
    pool_refresh_h: float = 12.0

    # ---- ui / server -----------------------------------------------------
    host: str = "127.0.0.1"
    port: int = 8787
    public_url: str = ""  # for Telegram "OPEN" button, e.g. http://127.0.0.1:8787
    ui_push_interval_ms: int = 250
    log_level: str = "INFO"
    db_path: str = "data/latbot.sqlite3"
    log_dir: str = "logs"
    stats_max_samples: int = 500
    opp_history_keep: int = 500
    single_instance_port: int = 47614

    # ------------------------------------------------------------------ #
    def ride_active(self) -> bool:
        """Simple mode with "ride the leader" exits."""
        return bool(self.simple_mode and self.ride_enabled)

    def effective_max_hold_s(self) -> float:
        """Exit at market after this (ride: ride_max_hold_s; simple mode: simple_max_hold_s)."""
        if self.ride_active():
            return float(self.ride_max_hold_s)
        return min(float(self.max_hold_s), float(self.simple_max_hold_s)) if self.simple_mode else float(self.max_hold_s)

    def effective_sell_timeout_s(self) -> float:
        if self.ride_active():
            return float(self.ride_max_hold_s)
        return min(float(self.sell_timeout_s), float(self.simple_max_hold_s)) if self.simple_mode else float(self.sell_timeout_s)

    def mode(self) -> Mode:
        return Mode.LIVE if (self.live_mode and not self.paper_mode) else Mode.PAPER

    def exchange(self, name: str) -> ExchangeConfig | None:
        for e in self.exchanges:
            if e.name == name:
                return e
        return None

    def execution_exchanges(self) -> list[str]:
        return [e.name for e in self.exchanges if e.enabled and e.role == ExchangeRole.EXECUTION]

    def enabled_exchanges(self) -> list[str]:
        return [e.name for e in self.exchanges if e.enabled]

    # ---- serialisation ----------------------------------------------------
    def public(self) -> dict[str, Any]:
        """Dict safe for UI / logs (no secrets live here anyway)."""
        d = asdict(self)
        d["mode"] = self.mode().value
        for e in d["exchanges"]:
            e["role"] = e["role"].value if isinstance(e["role"], ExchangeRole) else e["role"]
        return d

    def to_json(self) -> str:
        d = asdict(self)
        for e in d["exchanges"]:
            e["role"] = e["role"].value if isinstance(e["role"], ExchangeRole) else e["role"]
        return json.dumps(d, indent=2, ensure_ascii=False)

    @staticmethod
    def _migrate(data: dict[str, Any]) -> dict[str, Any]:
        """One-time upgrade of a config.json written by an older build."""
        data = dict(data)
        version = int(data.get("config_version") or 1)
        if version < 3:
            # Phase 3 (owner): target turnover >= ~500k (only the old default is raised, a custom value stays);
            # training = every opportunity with NET >= 0.1 % is shadow-traded.
            if float(data.get("min_target_volume_quote", 100_000.0)) == 100_000.0:
                data["min_target_volume_quote"] = 500_000.0
            data["min_net_pct_show"] = 0.1
        if version < 7:
            # owner: the whole bot on the top-100 coins by market cap (not a handful of "predictable" ones)
            top = _top100_settings()
            if int(data.get("ws_assets", 25) or 25) not in (10, 25):
                top.pop("ws_assets")  # a value the owner chose himself stays
            data.update(top)
        if version < 6:
            # owner: "Spot-Futures Arbitrage Scanner" - research module on (virtual only)
            data["basis_enabled"] = True
        if version < 5:
            # owner: "логика должна быть максимально простая" - simple mode on once (a later choice stays)
            data["simple_mode"] = True
        if version < 4:
            # owner: faster signals - every pool asset on WebSocket (only the old default 10 is raised)
            if int(data.get("ws_assets", 10) or 10) == 10:
                data["ws_assets"] = 25
        data["config_version"] = CONFIG_VERSION
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Settings":
        known = {f.name for f in fields(cls)}
        clean: dict[str, Any] = {}
        for k, v in cls._migrate(data).items():
            if k not in known:
                continue
            if k == "exchanges":
                clean[k] = [_exchange_from_dict(x) for x in v]
            else:
                clean[k] = v
        s = cls(**clean)
        s.validate()
        return s

    def update(self, patch: dict[str, Any]) -> list[str]:
        """Apply a partial update (from UI / Telegram).  Returns changed keys.
        Unknown keys are ignored; types are coerced to the field's current type."""
        changed: list[str] = []
        known = {f.name: f for f in fields(self)}
        for k, v in patch.items():
            if k not in known or k in ("exchanges",) and not isinstance(v, list):
                continue
            cur = getattr(self, k)
            if k == "exchanges":
                new = _merge_default_exchanges([_exchange_from_dict(x) for x in v])
            elif isinstance(cur, dict):
                new = _budgets(v, self.exchanges, strict=True)
            elif isinstance(cur, bool) or known[k].type in ("bool", bool):
                new = _to_bool(v)
            elif isinstance(cur, int) and not isinstance(cur, bool):
                new = int(float(v))
            elif isinstance(cur, float):
                new = float(v)
            elif isinstance(cur, list):
                new = list(v) if isinstance(v, (list, tuple)) else [x.strip() for x in str(v).split(",") if x.strip()]
            else:
                new = type(cur)(v) if cur is not None else v
            if new != cur:
                setattr(self, k, new)
                changed.append(k)
        self.validate()
        return changed

    def validate(self) -> None:
        if self.human_execution_time_s <= 0:
            raise ValueError("human_execution_time_s must be > 0")
        self.watchlist = [w.strip().upper().removesuffix("/USDT").removesuffix("USDT") if len(w.strip()) > 4 and w.strip().upper().endswith("USDT") else w.strip().upper() for w in self.watchlist if w.strip()]
        self.watchlist = list(dict.fromkeys(self.watchlist))[:20]
        self.watchlist_size = max(3, min(20, int(self.watchlist_size)))
        for e in self.exchanges:
            if e.market_type not in ("spot", "swap"):
                raise ValueError(f"exchange {e.name}: market_type must be spot|swap")
            if e.market_type == "spot":
                e.allow_short = False
            if e.stream not in ("book", "bbo", "rest"):
                raise ValueError(f"exchange {e.name}: stream must be book|bbo|rest")
            if e.role == ExchangeRole.EXECUTION and e.stream == "bbo":
                e.stream = "book"  # targets always need depth
        self.min_movers = max(1, int(self.min_movers))
        self.simple_entry_deadline_s = float(self.simple_entry_deadline_s) if self.simple_entry_deadline_s > 0 else 3.0
        self.simple_max_hold_s = max(1.0, float(self.simple_max_hold_s))
        for name, lo in (("ride_entry_max_age_s", 1.0), ("ride_reversal_pct", 0.01), ("ride_stop_pct", 0.05),
                         ("ride_lag_closed_pct", 0.0), ("ride_stall_s", 0.5), ("ride_trail_arm_pct", 0.0),
                         ("ride_trail_pct", 0.01), ("ride_max_hold_s", 5.0), ("ride_stale_s", 2.0)):
            setattr(self, name, max(lo, float(getattr(self, name))))
        self.ws_assets = max(1, min(100, int(self.ws_assets)))
        self.ws_book_resubscribe_s = max(0.0, float(self.ws_book_resubscribe_s))
        self.exchange_budget_quote = _budgets(self.exchange_budget_quote, self.exchanges, strict=False)
        for f in fields(self):  # a hand-edited config.json may hold "false" / "0": never truthy strings
            if f.type in ("bool", bool) and not isinstance(getattr(self, f.name), bool):
                setattr(self, f.name, _to_bool(getattr(self, f.name)))
        if self.capital_quote <= 0 or self.max_trade_quote <= 0:
            raise ValueError("capital must be > 0")
        if self.rest_book_timeout_s <= 0:
            raise ValueError("rest_book_timeout_s must be > 0")
        if self.max_slippage_pct < 0 or self.max_slippage_pct > 10:
            raise ValueError("max_slippage_pct out of range")
        if self.telegram_profit_threshold_pct < 0:
            raise ValueError("telegram_profit_threshold_pct must be >= 0")
        if self.paper_engine not in ("internal", "bybit_demo"):
            raise ValueError("paper_engine must be 'internal' or 'bybit_demo'")
        if self.exit_on_timeout not in ("market", "hold"):
            raise ValueError("exit_on_timeout must be 'market' or 'hold'")
        if self.live_mode and self.paper_mode:
            # paper wins; live requires explicitly turning paper off
            self.live_mode = False


def _budget_key(name: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def _budgets(v: Any, exchanges: list[ExchangeConfig], *, strict: bool) -> dict[str, float]:
    """Exchange budgets from a dict or the UI text "kraken=200, OKX EEA=500".  A venue may be named by
    its internal name, ccxt id or the label the UI shows; it is stored under the internal name.
    0 = nothing on that venue.  ``strict`` (a patch from the UI / API / Telegram): an unreadable part,
    an unknown venue or a negative amount is a ValueError (the API answers 400, never a false "Saved");
    otherwise (config.json on load) such entries are dropped with a warning."""
    names: dict[str, str] = {}
    for e in exchanges:
        for alias in (e.name, e.ccxt_name, e.label):
            if alias:
                names.setdefault(_budget_key(alias), e.name)
    items: list[tuple[Any, Any]] = []
    problems: list[str] = []
    if isinstance(v, dict):
        items = list(v.items())
    else:
        for part in (p.strip() for p in str(v or "").split(",")):
            if not part:
                continue
            if "=" not in part:
                problems.append(f"'{part}' (use name=amount, e.g. kraken=200)")
                continue
            name, amount = part.split("=", 1)
            items.append((name, amount))
    out: dict[str, float] = {}
    for name, amount in items:
        key = names.get(_budget_key(name))
        try:
            x = float(amount if isinstance(amount, (int, float)) else str(amount).strip())
        except (TypeError, ValueError):
            x = None
        if key is None:
            problems.append(f"unknown exchange '{str(name).strip()}'")
        elif x is None or x != x or x in (float("inf"), float("-inf")):
            problems.append(f"'{str(amount).strip()}' for {str(name).strip()} is not a number")
        elif x < 0:
            problems.append(f"{str(name).strip()} must be 0 or more")
        else:
            out[key] = x
    if problems:
        if strict:
            raise ValueError("exchange budget: " + "; ".join(problems))
        log.warning("exchange budget entries ignored: %s", "; ".join(problems))
    return out


def _to_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _merge_default_exchanges(saved: list[ExchangeConfig]) -> list[ExchangeConfig]:
    """Venues added to ``DEFAULT_EXCHANGES`` after the user's config.json was
    written are appended (with their defaults); the saved entries are kept as is."""
    have = {e.name for e in saved}
    return saved + [ExchangeConfig(**asdict(e)) for e in DEFAULT_EXCHANGES if e.name not in have]


def _exchange_from_dict(x: dict[str, Any]) -> ExchangeConfig:
    x = dict(x)
    if "role" in x and not isinstance(x["role"], ExchangeRole):
        x["role"] = ExchangeRole(str(x["role"]).lower())
    known = {f.name for f in fields(ExchangeConfig)}
    # A field the saved config.json does not have yet (added in a newer version)
    # takes the built-in default of that venue, not the generic dataclass default.
    base = next((e for e in DEFAULT_EXCHANGES if e.name == x.get("name")), None)
    if base is not None:
        for f in known:
            if f not in x:
                x[f] = getattr(base, f)
    return ExchangeConfig(**{k: v for k, v in x.items() if k in known})


# --------------------------------------------------------------------------- #
# Persistence
# --------------------------------------------------------------------------- #
def _top100_settings() -> dict[str, Any]:
    """The owner's universe: the top-100 coins by market cap for the whole bot (latency + basis)."""
    return {
        "pool_max_assets": TOP_N, "pool_stability_filters": False, "pool_exclude_categories": list(TOP_EXCLUDE_CATEGORIES),
        # ~25 of the top 100 are stablecoins / tokenized assets: 100 TRADABLE coins reach about rank 140
        "pool_min_market_cap_usd": 1e8, "pool_min_volume_24h_usd": 5e6,
        "pool_max_abs_change_24h_pct": 100.0, "pool_max_abs_change_7d_pct": 1000.0,
        "ws_assets": 50, "basis_assets": [], "basis_top_n": TOP_N,
    }


def load_settings(path: str | os.PathLike[str] = "config.json") -> Settings:
    p = Path(path)
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            s = Settings.from_dict(data)
            s.exchanges = _merge_default_exchanges(s.exchanges)  # venues added after this config.json was written
            _apply_env_overrides(s)
            return s
        except Exception as exc:  # noqa: BLE001
            log.error("config.json invalid (%s); using defaults", exc)
    s = Settings(simple_mode=True, basis_enabled=True, **_top100_settings())  # fresh install: the owner's choices
    _apply_env_overrides(s)
    return s


def save_settings(settings: Settings, path: str | os.PathLike[str] = "config.json") -> None:
    p = Path(path)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(settings.to_json(), encoding="utf-8")
    os.replace(tmp, p)


_ENV_OVERRIDES = {
    "HUMAN_EXECUTION_TIME": "human_execution_time_s",
    "TELEGRAM_PROFIT_THRESHOLD": "telegram_profit_threshold_pct",
    "MAX_TRADE_SIZE": "max_trade_quote",
    "MAX_RISK": "max_risk_per_trade_pct",
    "MAX_DAILY_LOSS": "max_daily_loss_quote",
    "AUTO_CONFIRM": "auto_confirm",
    "PAPER_MODE": "paper_mode",
    "LIVE_MODE": "live_mode",
    "MIN_LIQUIDITY": "min_liquidity_quote",
    "MAX_SLIPPAGE": "max_slippage_pct",
    "MAX_DATA_AGE": "max_data_age_ms",
    "MAX_API_LATENCY": "max_api_latency_ms",
    "CAPITAL": "capital_quote",
    "HOST": "host",
    "PORT": "port",
    "PUBLIC_URL": "public_url",
    "LOG_LEVEL": "log_level",
    "WATCHLIST": "watchlist",
    "WATCHLIST_SIZE": "watchlist_size",
    "PAPER_BALANCE": "paper_balance_quote",
    "ALERT_HUMAN_ONLY": "alert_human_only",
    "MIN_MOVERS": "min_movers",
}


def _apply_env_overrides(s: Settings) -> None:
    patch: dict[str, Any] = {}
    for env_key, attr in _ENV_OVERRIDES.items():
        v = os.getenv(env_key)
        if v is not None and v.strip() != "":
            patch[attr] = v.strip()
    # Legacy: WATCHLIST=auto -> watchlist_auto
    if str(patch.get("watchlist", "")).strip().lower() == "auto":
        patch.pop("watchlist")
        patch["watchlist_auto"] = True
    mode = (os.getenv("MODE") or "").strip().upper()
    if mode == "LIVE":
        patch["paper_mode"] = False
        patch["live_mode"] = True
    elif mode == "PAPER":
        patch["paper_mode"] = True
    if patch:
        s.update(patch)
