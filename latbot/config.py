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
    )
    register_secret(s.telegram_bot_token)
    register_secret(s.coingecko_api_key)
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
    for name in ("binance", "bybit", "okx", "kraken", "coinbase", "gate", "bitstamp", "bitvavo"):
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
    book_depth: int = 20
    demo: bool = False  # use exchange demo/testnet endpoint (Bybit Demo Trading)
    rate_limit_per_s: float = 10.0
    weight: float = 1.0  # tie-breaker for target selection (1 = neutral)

    @property
    def ccxt_name(self) -> str:
        return self.ccxt_id or self.name


DEFAULT_EXCHANGES: list[ExchangeConfig] = [
    # Binance is a signal source only (not MiCA-licensed for EU execution).
    ExchangeConfig(name="binance", role=ExchangeRole.SIGNAL, market_type="spot", taker_fee=0.001, maker_fee=0.001),
    # Bybit: USDT linear perpetuals on Demo Trading (api-demo.bybit.com), as in the previous bot.
    ExchangeConfig(
        name="bybit", role=ExchangeRole.EXECUTION, market_type="swap", allow_short=True, demo=True,
        taker_fee=0.00055, maker_fee=0.0002,
    ),
    ExchangeConfig(name="okx", role=ExchangeRole.EXECUTION, market_type="spot", taker_fee=0.001, maker_fee=0.0008),
    ExchangeConfig(name="kraken", role=ExchangeRole.EXECUTION, market_type="spot", taker_fee=0.004, maker_fee=0.0025),
    ExchangeConfig(name="coinbase", role=ExchangeRole.EXECUTION, market_type="spot", taker_fee=0.006, maker_fee=0.004),
    ExchangeConfig(name="gate", role=ExchangeRole.EXECUTION, market_type="spot", taker_fee=0.002, maker_fee=0.002),
    ExchangeConfig(name="bitstamp", role=ExchangeRole.EXECUTION, enabled=False, taker_fee=0.004, maker_fee=0.003),
    ExchangeConfig(name="bitvavo", role=ExchangeRole.EXECUTION, enabled=False, taker_fee=0.0025, maker_fee=0.0015),
]


# --------------------------------------------------------------------------- #
# Runtime settings (public, user-tunable)
# --------------------------------------------------------------------------- #
@dataclass
class Settings:
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
    watchlist_exchange: str = "bybit"
    quote_assets: list[str] = field(default_factory=lambda: ["USDT", "USDC", "USD"])
    exchanges: list[ExchangeConfig] = field(default_factory=lambda: [ExchangeConfig(**asdict(e)) for e in DEFAULT_EXCHANGES])

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
    allow_downsize: bool = True  # if balance < capital -> use max available

    # ---- execution behaviour ---------------------------------------------
    buy_limit_slippage_pct: float = 0.2  # IOC limit = ask_vwap * (1 + this)
    buy_fill_timeout_s: float = 3.0
    sell_timeout_s: float = 20.0  # wait for catch-up sell this long
    max_hold_s: float = 60.0  # after this, exit at market
    exit_on_timeout: str = "market"  # "market" | "hold"
    min_partial_fill_ratio: float = 0.2  # below this, cancel and abort

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

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Settings":
        known = {f.name for f in fields(cls)}
        clean: dict[str, Any] = {}
        for k, v in data.items():
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
                new = [_exchange_from_dict(x) for x in v]
            elif isinstance(cur, bool):
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
        if self.capital_quote <= 0 or self.max_trade_quote <= 0:
            raise ValueError("capital must be > 0")
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


def _to_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _exchange_from_dict(x: dict[str, Any]) -> ExchangeConfig:
    x = dict(x)
    if "role" in x and not isinstance(x["role"], ExchangeRole):
        x["role"] = ExchangeRole(str(x["role"]).lower())
    known = {f.name for f in fields(ExchangeConfig)}
    return ExchangeConfig(**{k: v for k, v in x.items() if k in known})


# --------------------------------------------------------------------------- #
# Persistence
# --------------------------------------------------------------------------- #
def load_settings(path: str | os.PathLike[str] = "config.json") -> Settings:
    p = Path(path)
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            s = Settings.from_dict(data)
            _apply_env_overrides(s)
            return s
        except Exception as exc:  # noqa: BLE001
            log.error("config.json invalid (%s); using defaults", exc)
    s = Settings()
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
