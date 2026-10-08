"""Core data model shared by every module.

Conventions
-----------
* Percentages are stored as PERCENT values (1.5 means 1.5 %), never fractions.
  Fee rates are the only exception: Fees.maker / Fees.taker are fractions
  (0.001 == 0.1 %) because that is what exchanges report.
* Prices are in quote currency, amounts ("base_amount") in base currency,
  capital / profit ("*_quote") in quote currency.
* Time: ``*_ts`` = wall clock seconds (time.time()), ``*_mono`` = monotonic
  seconds (time.monotonic()).  Durations/ages must be computed from mono.
* ``asset`` is the canonical cross-exchange key (base currency, e.g. "BTC").
  ``symbol`` is the exchange's own ccxt market symbol for that asset
  ("BTC/USDT" spot on Binance, "BTC/USDT:USDT" linear swap on Bybit, "BTC/USD"
  on Kraken).  Signals are compared per asset across USD-equivalent quotes.
* ``direction`` is +1 (price moved UP -> target lags -> go LONG: BUY then SELL)
  or -1 (moved DOWN -> go SHORT: SELL then BUY; only on swap markets that allow
  shorting).  Economics use entry/exit naming; the UI maps them to BUY/SELL.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    LIMIT = "limit"
    MARKET = "market"


class TimeInForce(str, Enum):
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"


class ExchangeRole(str, Enum):
    """SIGNAL exchanges only feed price discovery; EXECUTION exchanges can be
    chosen as the target exchange (they also act as signal sources)."""

    SIGNAL = "signal"
    EXECUTION = "execution"


class Mode(str, Enum):
    PAPER = "paper"
    LIVE = "live"


class Light(str, Enum):
    RED = "red"
    YELLOW = "yellow"
    GREEN = "green"


class OppState(str, Enum):
    DETECTED = "DETECTED"
    VALIDATING = "VALIDATING"
    VALID = "VALID"
    NOTIFIED = "NOTIFIED"
    MANUAL_SELECTED = "MANUAL_SELECTED"
    AUTO_SELECTED = "AUTO_SELECTED"
    EXECUTING = "EXECUTING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CLOSED = "CLOSED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


ACTIVE_STATES = frozenset(
    {
        OppState.DETECTED,
        OppState.VALIDATING,
        OppState.VALID,
        OppState.NOTIFIED,
        OppState.MANUAL_SELECTED,
        OppState.AUTO_SELECTED,
    }
)
EXECUTION_STATES = frozenset(
    {OppState.EXECUTING, OppState.PARTIALLY_FILLED, OppState.FILLED}
)
TERMINAL_STATES = frozenset(
    {OppState.CLOSED, OppState.EXPIRED, OppState.CANCELLED, OppState.FAILED}
)

# Allowed state transitions (from -> set(to)).  OpportunityManager enforces.
TRANSITIONS: dict[OppState, frozenset[OppState]] = {
    OppState.DETECTED: frozenset({OppState.VALIDATING, OppState.EXPIRED, OppState.CANCELLED}),
    OppState.VALIDATING: frozenset({OppState.VALID, OppState.EXPIRED, OppState.CANCELLED, OppState.DETECTED}),
    OppState.VALID: frozenset(
        {OppState.NOTIFIED, OppState.MANUAL_SELECTED, OppState.AUTO_SELECTED, OppState.VALIDATING, OppState.EXPIRED, OppState.CANCELLED}
    ),
    OppState.NOTIFIED: frozenset(
        {OppState.MANUAL_SELECTED, OppState.AUTO_SELECTED, OppState.VALIDATING, OppState.EXPIRED, OppState.CANCELLED}
    ),
    OppState.MANUAL_SELECTED: frozenset(
        {OppState.EXECUTING, OppState.CLOSED, OppState.EXPIRED, OppState.CANCELLED, OppState.VALID, OppState.NOTIFIED}
    ),
    OppState.AUTO_SELECTED: frozenset(
        {OppState.EXECUTING, OppState.EXPIRED, OppState.CANCELLED, OppState.FAILED, OppState.VALID, OppState.NOTIFIED}
    ),
    OppState.EXECUTING: frozenset(
        {OppState.PARTIALLY_FILLED, OppState.FILLED, OppState.CLOSED, OppState.CANCELLED, OppState.FAILED}
    ),
    OppState.PARTIALLY_FILLED: frozenset({OppState.FILLED, OppState.CLOSED, OppState.CANCELLED, OppState.FAILED}),
    OppState.FILLED: frozenset({OppState.CLOSED, OppState.FAILED}),
    OppState.CLOSED: frozenset(),
    OppState.EXPIRED: frozenset(),
    OppState.CANCELLED: frozenset(),
    OppState.FAILED: frozenset(),
}


class OrderStatus(str, Enum):
    NEW = "NEW"  # created locally, not sent
    SENT = "SENT"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"


ORDER_DONE = frozenset(
    {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED, OrderStatus.FAILED}
)


# --------------------------------------------------------------------------- #
# Market data
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class Timestamps:
    """exchange_ts: exchange-reported wall time (s) or None.
    recv_ts / recv_mono: when the bot received the message.
    proc_mono: when the bot finished processing (set by pipeline)."""

    recv_ts: float
    recv_mono: float
    exchange_ts: float | None = None
    proc_mono: float | None = None

    @property
    def transport_latency_ms(self) -> float | None:
        if self.exchange_ts is None:
            return None
        return (self.recv_ts - self.exchange_ts) * 1000.0

    @classmethod
    def now(cls, exchange_ts: float | None = None) -> "Timestamps":
        return cls(recv_ts=time.time(), recv_mono=time.monotonic(), exchange_ts=exchange_ts)


@dataclass(slots=True)
class BookLevel:
    price: float
    size: float  # base amount


@dataclass(slots=True)
class OrderBook:
    exchange: str
    symbol: str  # exchange market symbol
    bids: list[BookLevel]  # sorted by price DESC
    asks: list[BookLevel]  # sorted by price ASC
    ts: Timestamps
    asset: str = ""  # canonical key (base), filled by adapter
    seq: int | None = None

    @property
    def best_bid(self) -> float | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0].price if self.asks else None

    @property
    def mid(self) -> float | None:
        if not self.bids or not self.asks:
            return None
        return (self.bids[0].price + self.asks[0].price) / 2.0

    @property
    def spread_pct(self) -> float | None:
        if not self.bids or not self.asks:
            return None
        m = self.mid
        if not m:
            return None
        return (self.asks[0].price - self.bids[0].price) / m * 100.0

    def age_ms(self, now_mono: float) -> float:
        return (now_mono - self.ts.recv_mono) * 1000.0


@dataclass(slots=True)
class Trade:
    exchange: str
    symbol: str
    price: float
    amount: float
    side: Side | None
    ts: Timestamps
    asset: str = ""
    trade_id: str | None = None


@dataclass(slots=True)
class Ticker:
    exchange: str
    symbol: str
    bid: float | None
    ask: float | None
    last: float | None
    ts: Timestamps
    asset: str = ""
    base_volume: float | None = None
    quote_volume: float | None = None


@dataclass(slots=True)
class Fees:
    """Fractions: 0.001 == 0.1 %."""

    maker: float
    taker: float

    @property
    def taker_pct(self) -> float:
        return self.taker * 100.0

    @property
    def maker_pct(self) -> float:
        return self.maker * 100.0


@dataclass(slots=True)
class Balance:
    asset: str
    free: float
    total: float


@dataclass(slots=True)
class ExchangeHealth:
    exchange: str
    ws_connected: bool = False
    last_msg_mono: float | None = None
    ws_latency_ms: float | None = None  # exchange_ts -> recv (EWMA)
    rest_latency_ms: float | None = None  # EWMA
    order_latency_ms: float | None = None  # send -> ack EWMA
    fill_latency_ms: float | None = None  # send -> first fill EWMA
    error_count: int = 0
    rate_limited_until_mono: float | None = None
    maintenance: bool = False
    stale: bool = False
    reason: str = ""
    last_poll_mono: float | None = None  # last successful REST ticker poll (breadth coverage)

    def is_polling(self, now_mono: float, poll_interval_s: float = 2.0) -> bool:
        """REST ticker polling delivered recently.  Tolerant window (>= 30 s): a
        venue polled every few seconds with an occasional failed round is
        covered, not offline."""
        lp = self.last_poll_mono
        return lp is not None and now_mono - float(lp) <= max(30.0, 10.0 * float(poll_interval_s or 2.0))

    def is_usable(self, now_mono: float) -> bool:
        if self.maintenance or self.stale or not self.ws_connected:
            return False
        if self.rate_limited_until_mono and now_mono < self.rate_limited_until_mono:
            return False
        return True


# --------------------------------------------------------------------------- #
# Orders
# --------------------------------------------------------------------------- #
def new_id(prefix: str = "") -> str:
    return f"{prefix}{uuid.uuid4().hex[:12]}"


@dataclass(slots=True)
class OrderRequest:
    exchange: str
    symbol: str
    side: Side
    amount: float  # base
    type: OrderType = OrderType.LIMIT
    price: float | None = None  # required for LIMIT
    tif: TimeInForce = TimeInForce.IOC
    client_id: str = field(default_factory=lambda: new_id("o"))
    post_only: bool = False
    opportunity_id: str | None = None


@dataclass(slots=True)
class Fill:
    order_id: str
    price: float
    amount: float  # base
    fee_cost: float  # in fee_asset
    fee_asset: str
    ts: float  # wall
    trade_id: str | None = None


@dataclass(slots=True)
class Order:
    id: str  # exchange order id (or paper id)
    client_id: str
    exchange: str
    symbol: str
    side: Side
    type: OrderType
    amount: float
    price: float | None
    status: OrderStatus = OrderStatus.NEW
    filled: float = 0.0
    avg_price: float | None = None
    fills: list[Fill] = field(default_factory=list)
    fee_cost_quote: float = 0.0  # total fees converted to quote
    sent_ts: float | None = None
    sent_mono: float | None = None
    ack_mono: float | None = None
    first_fill_mono: float | None = None
    done_mono: float | None = None
    error: str | None = None
    raw: Any = None

    @property
    def remaining(self) -> float:
        return max(0.0, self.amount - self.filled)

    @property
    def is_done(self) -> bool:
        return self.status in ORDER_DONE

    @property
    def filled_quote(self) -> float:
        return sum(f.price * f.amount for f in self.fills) if self.fills else (
            (self.avg_price or 0.0) * self.filled
        )

    def latency_ms(self) -> float | None:
        if self.sent_mono is None or self.ack_mono is None:
            return None
        return (self.ack_mono - self.sent_mono) * 1000.0


# --------------------------------------------------------------------------- #
# Detection
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class MoveEvent:
    """A significant price move observed on one exchange."""

    exchange: str
    asset: str
    direction: int  # +1 up, -1 down
    move_pct: float
    start_price: float
    end_price: float
    start_mono: float
    end_mono: float
    velocity_pct_s: float  # move_pct / duration
    volume_accel: float | None = None  # recent volume / baseline volume


@dataclass(slots=True)
class LagSignal:
    """Output of the first-mover / lagging-exchange engine.

    lag_pct is the GROSS gap: first_move_pct - target_move_pct (same direction).
    expected_catchup_pct is lag_pct * learned catch-up ratio (what we expect the
    target to actually recover inside the window)."""

    asset: str
    direction: int
    first_mover: str
    first_move_pct: float
    first_move_start_mono: float
    reacted: dict[str, float]  # exchange -> move_pct over the same window
    target: str
    target_symbol: str  # target exchange market symbol
    target_move_pct: float
    lag_pct: float
    expected_catchup_pct: float
    catchup_ratio: float
    detected_mono: float
    detected_ts: float
    movers_count: int = 1  # how many exchanges already moved >= threshold
    median_reaction_delay_s: float | None = None  # historical for target
    source: str = "ws"  # "ws" (order book / bbo stream) | "poll" (REST tickers; needs book confirmation)
    target_quote: str = ""  # quote currency of the target market (EUR / USDC / USD / USDT)
    mover_volumes_quote: dict[str, float] = field(default_factory=dict)  # 24h quote volume per mover


# --------------------------------------------------------------------------- #
# Economics / executability
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class WalkResult:
    vwap: float | None
    filled_base: float
    filled_quote: float
    levels_used: int
    exhausted: bool  # book did not contain enough size
    worst_price: float | None = None


@dataclass(slots=True)
class ExecutionEconomics:
    """Executable economics of ONE round trip on the target exchange.

    direction +1 (LONG):  entry = BUY walking asks, exit = SELL walking bids.
    direction -1 (SHORT): entry = SELL walking bids, exit = BUY walking asks.
    ``exit_target`` = exit_vwap_now moved by expected catch-up in our favour.
    Slippage is already embedded in the VWAPs; fees + execution loss are the
    explicit costs.  net_pct is THE number shown to the user."""

    direction: int
    capital_quote: float  # requested
    executable_quote: float  # what the book really supports (<= capital)
    base_amount: float
    entry_best: float
    entry_vwap: float
    exit_best: float
    exit_vwap_now: float
    exit_target: float
    spread_pct: float
    entry_fee_pct: float
    exit_fee_pct: float
    entry_slippage_pct: float  # |entry_vwap - entry_best| / entry_best
    exit_slippage_pct: float  # |exit_best - exit_vwap_now| / exit_best
    execution_loss_pct: float  # adverse move during execution
    gross_pct: float  # (exit_target / entry_vwap - 1) * direction
    total_cost_pct: float  # fees + execution loss
    net_pct: float
    net_quote: float
    liquidity_ok: bool
    liquidity_reason: str = ""
    book_age_ms: float = 0.0
    bid_depth_quote: float = 0.0  # within MAX_SLIPPAGE
    ask_depth_quote: float = 0.0
    # Worst entry price at which NET is still >= settings.min_net_pct_show
    # (LONG: the highest buy price, SHORT: the lowest sell price).  The manual
    # trader's limit: "купить <= X".  0.0 when the book is empty / invalid.
    entry_cap_price: float = 0.0

    # --- display helpers: what the human actually does ----------------------
    @property
    def buy_price(self) -> float:
        return self.entry_vwap if self.direction > 0 else self.exit_target

    @property
    def sell_price(self) -> float:
        return self.exit_target if self.direction > 0 else self.entry_vwap


@dataclass(slots=True)
class ExecAssessment:
    light: Light
    label: str  # "HIGH" | "MEDIUM" | "LOW" | "NOT EXECUTABLE"
    est_execution_s: float
    margin_s: float  # remaining_window - est_execution
    probability: float  # 0..1 rough success probability
    reasons: list[str] = field(default_factory=list)
    stats_rate: float | None = None  # learned success rate for this (asset, exchange) if known
    stats_n: int = 0  # number of samples behind stats_rate


# --------------------------------------------------------------------------- #
# Opportunity
# --------------------------------------------------------------------------- #
@dataclass
class Opportunity:
    id: str
    asset: str
    target_exchange: str
    target_symbol: str
    first_mover: str
    direction: int  # +1 LONG, -1 SHORT
    mode: Mode
    state: OppState
    signal: LagSignal
    econ: ExecutionEconomics
    human: ExecAssessment
    bot: ExecAssessment
    why: str
    detected_ts: float
    first_seen_mono: float
    last_valid_mono: float
    last_update_mono: float
    estimated_duration_s: float
    expired_mono: float | None = None
    actual_duration_s: float | None = None
    telegram_message_id: int | None = None
    notified_ts: float | None = None
    execution_id: str | None = None
    state_history: list[tuple[float, str, str]] = field(default_factory=list)  # (ts, state, reason)
    net_history: list[tuple[float, float]] = field(default_factory=list)  # (mono, net_pct)
    update_count: int = 0
    expire_reason: str = ""
    quote: str = ""  # quote currency of the target market (for display: €, $)
    pair_url: str = ""  # direct link to the pair page on the target exchange
    caught_up: bool | None = None  # set at expiry: did the target actually catch up?
    human_would_make_it: bool | None = None  # set at expiry: window >= human time AND caught_up
    max_net_pct: float = 0.0  # best NET seen during the opportunity's life
    # Learned history of this (asset, target) copied from StatsStore on every
    # update (the trading PLAN quotes them): median opportunity duration, how
    # often the target caught up, how often a human had time.
    typical_window_s: float | None = None
    caught_up_ok: int = 0
    caught_up_total: int = 0
    human_ok: int = 0
    human_total: int = 0
    last_update_ts: float = 0.0  # wall clock of the last upsert / refresh
    plan: dict = field(default_factory=dict)  # Russian trading plan (explain.build_plan), key -> line
    # Phase 3: signal quality (engine.quality.QualityReport.to_dict) and leverage plan
    # (engine.leverage.LeveragePlan.to_dict); empty when not computed.
    quality: dict = field(default_factory=dict)
    leverage: dict = field(default_factory=dict)

    def age_s(self, now_mono: float) -> float:
        return max(0.0, now_mono - self.first_seen_mono)

    def remaining_s(self, now_mono: float) -> float:
        return max(0.0, self.estimated_duration_s - self.age_s(now_mono))

    @property
    def is_active(self) -> bool:
        return self.state in ACTIVE_STATES

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def to_dict(self, now_mono: float | None = None) -> dict[str, Any]:
        now_mono = time.monotonic() if now_mono is None else now_mono
        d = asdict(self)
        d["age_s"] = round(self.age_s(now_mono), 2)
        d["remaining_s"] = round(self.remaining_s(now_mono), 2)
        d["state"] = self.state.value
        d["mode"] = self.mode.value
        d["human"]["light"] = self.human.light.value
        d["bot"]["light"] = self.bot.light.value
        d["state_history"] = [list(x) for x in self.state_history]
        d["net_history"] = [list(x) for x in self.net_history[-50:]]
        d["side"] = "LONG" if self.direction > 0 else "SHORT"
        d["buy_price"] = self.econ.buy_price
        d["sell_price"] = self.econ.sell_price
        d["plan"] = dict(self.plan)  # as built by explain.build_plan (asdict already copied it; keep it plain)
        return d


# --------------------------------------------------------------------------- #
# Trades (round trips) and realized P&L
# --------------------------------------------------------------------------- #
@dataclass
class TradeRecord:
    """One round trip (entry + exit) on the target exchange, paper or live."""

    id: str
    opportunity_id: str
    mode: Mode
    exchange: str
    asset: str
    symbol: str
    direction: int
    capital_quote: float
    expected_net_pct: float
    expected_profit_quote: float
    expected_entry_price: float
    expected_exit_price: float
    state: OppState = OppState.EXECUTING
    entry_order: Order | None = None
    exit_order: Order | None = None
    actual_entry_price: float | None = None
    actual_exit_price: float | None = None
    actual_fees_quote: float = 0.0
    actual_slippage_pct: float | None = None  # vs expected buy/sell prices
    actual_net_pct: float | None = None
    realized_pnl_quote: float | None = None
    execution_time_s: float | None = None  # entry sent -> exit done
    entry_latency_ms: float | None = None
    diff_reason: str = ""  # explanation of expected vs actual gap
    error: str | None = None
    started_ts: float = field(default_factory=time.time)
    finished_ts: float | None = None
    events: list[tuple[float, str]] = field(default_factory=list)  # (ts, message)
    # MANUAL trade session: what the owner did by hand, next to what was expected
    manual: bool = False
    entered_in_window: bool | None = None  # entry reported while the opportunity was still alive
    time_to_entry_s: float | None = None  # "Я ЗАШЁЛ" -> "Я КУПИЛ"
    user_reported_pnl_quote: float | None = None  # the result the exchange showed the owner
    user_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["mode"] = self.mode.value
        d["state"] = self.state.value
        d["side"] = "LONG" if self.direction > 0 else "SHORT"
        for key in ("entry_order", "exit_order"):
            o = d.get(key)
            if o:
                o["side"] = o["side"].value if isinstance(o["side"], Side) else o["side"]
                o["type"] = o["type"].value if isinstance(o["type"], OrderType) else o["type"]
                o["status"] = o["status"].value if isinstance(o["status"], OrderStatus) else o["status"]
                o.pop("raw", None)
        d["events"] = [list(x) for x in self.events]
        return d


# --------------------------------------------------------------------------- #
# Statistics (learned per symbol+exchange)
# --------------------------------------------------------------------------- #
@dataclass
class ReactionStats:
    """Rolling statistics for one (asset, exchange).  Lists are bounded by
    stats.StatsStore (max_samples)."""

    asset: str
    exchange: str
    reaction_delays_s: list[float] = field(default_factory=list)
    first_mover_count: int = 0
    lagging_count: int = 0
    lag_sizes_pct: list[float] = field(default_factory=list)
    catchup_ratios: list[float] = field(default_factory=list)
    opp_durations_s: list[float] = field(default_factory=list)
    success_count: int = 0
    fail_count: int = 0
    false_signal_count: int = 0
    realized_returns_pct: list[float] = field(default_factory=list)
    slippages_pct: list[float] = field(default_factory=list)
    exec_latencies_ms: list[float] = field(default_factory=list)
    # Learned "would a human have made it": per expired/closed opportunity we
    # record whether the window lasted >= human_execution_time_s AND the target
    # actually caught up (so the exit would have been there).
    human_window_total: int = 0
    human_window_ok: int = 0
    caught_up_total: int = 0
    caught_up_ok: int = 0
    # Real MANUAL trades of the owner (stronger evidence than the estimate above):
    # entered while the window was open / closed with a profit.
    manual_total: int = 0
    manual_in_window: int = 0
    manual_wins: int = 0

    @property
    def success_rate(self) -> float | None:
        n = self.success_count + self.fail_count
        return None if n == 0 else self.success_count / n

    @property
    def human_success_rate(self) -> float | None:
        return None if self.human_window_total == 0 else self.human_window_ok / self.human_window_total

    @property
    def manual_success_rate(self) -> float | None:
        return None if self.manual_total == 0 else self.manual_wins / self.manual_total

    @property
    def catchup_rate(self) -> float | None:
        return None if self.caught_up_total == 0 else self.caught_up_ok / self.caught_up_total


def percentile(values: list[float], p: float) -> float | None:
    """Nearest-rank percentile, p in [0, 100]."""
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


def median(values: list[float]) -> float | None:
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def mean(values: list[float]) -> float | None:
    return None if not values else sum(values) / len(values)
