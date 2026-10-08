# latbot — architecture & module contracts

Real-time cross-exchange **latency** trading terminal.  Other exchanges are used
to detect that a price move has started; the **target exchange** is the one
that lags.  Both legs (entry and exit) are executed on the target exchange.
No transfers between exchanges, ever.

```
LIVE MARKET DATA (ccxt.pro WebSocket order books)         exchanges/*
  ↓ normalise → OrderBook(asset, symbol, bids, asks, Timestamps)
MarketState (latest books, mid-price series, health)      market/state.py
  ↓
MoveDetector: first mover / lagging exchange → LagSignal  engine/mover.py (+ engine/stats.py learning)
  ↓ per lagging execution exchange
compute_economics (book walk, fees, slippage, NET)        engine/economics.py
DurationEstimator (history + live decay)                  engine/duration.py
assess_human / assess_bot → 🔴🟡🟢                        engine/executability.py
build_why                                                 engine/explain.py
OpportunityManager (ids, state machine, dedupe, expiry)   engine/opportunity.py
  ↓ events OPP_NEW / OPP_UPDATE / OPP_STATE
UI (FastAPI + WS)   Telegram (NET ≥ 3 % and executable)   api/, notify/
  ↓ user picks MANUAL or AUTO
ExecutionEngine: FINAL REVALIDATION → entry → fills → exit → realized P&L   execution/
  ↓
Database (SQLite) + StatsStore (learning)                 storage/, engine/stats.py
```

Everything is `asyncio`, single process.  Python ≥ 3.11.  Dependencies:
`ccxt` (incl. `ccxt.pro`), `fastapi`, `uvicorn`, `aiohttp`, `python-dotenv`;
tests: `pytest`, `pytest-asyncio` (asyncio_mode = auto), offline only.

## Shared foundation (already written — read these first)

* `latbot/models.py` — all dataclasses/enums.  **Do not change field names**;
  add fields only with a default value, and only if you really must.
* `latbot/config.py` — `Settings` (public, user-tunable, persisted to
  `config.json`), `Secrets` (from `.env`, never exposed), `ExchangeConfig`.
* `latbot/events.py` — `EventBus` + topic constants.
* `latbot/clock.py` — `Clock`, `SimClock`, `Ewma`.
* `latbot/logging_setup.py` — secret-redacting logging.
* `latbot/exchanges/base.py` — `ExchangeAdapter` ABC + typed errors.

Conventions: percentages are percent values (1.5 == 1.5 %), fee rates are
fractions; `*_mono` for durations, `*_ts` for wall time; `asset` = base ("BTC")
is the cross-exchange key, `symbol` = exchange market symbol.  Logging via
`logging.getLogger(__name__)`; never log secrets.  Type hints everywhere.
Keep functions pure where possible (easy offline tests).  Comments in English
or Russian are both fine; user-facing text in Telegram is Russian except the
alert card format (see §Telegram).

## Module contracts

Each bullet is a **public** API other modules rely on.  Implement exactly these
names/signatures (you may add private helpers and extra optional kwargs).

### market/state.py — `MarketState`
```python
class MarketState:
    def __init__(self, settings: Settings, clock: Clock): ...
    def on_book(self, book: OrderBook) -> None      # store latest; append (recv_mono, mid) to series; trim > settings.price_history_s
    def on_trade(self, trade: Trade) -> None        # bounded deque of recent trades per (exchange, asset)
    def on_ticker(self, ticker: Ticker) -> None
    def on_health(self, h: ExchangeHealth) -> None
    def book(self, exchange: str, asset: str) -> OrderBook | None
    def books_for(self, asset: str) -> dict[str, OrderBook]          # exchange -> latest book
    def health(self, exchange: str) -> ExchangeHealth | None
    def is_fresh(self, exchange: str, asset: str, now_mono: float, max_age_ms: float) -> bool
    def mid_series(self, exchange: str, asset: str) -> Sequence[tuple[float, float]]  # (mono, mid) oldest→newest
    def mid_at(self, exchange: str, asset: str, mono: float) -> float | None          # last mid at or before mono
    def assets(self) -> set[str]
    def exchanges_for(self, asset: str) -> list[str]
    def recent_quote_volume(self, exchange: str, asset: str, window_s: float, now_mono: float) -> float
```

### market/features.py
```python
def move_over_window(series, now_mono: float, window_s: float) -> tuple[float, float, float, float] | None
    # -> (move_pct, start_price, end_price, start_mono) using the oldest point within [now-window, now]; None if < 2 points
def velocity_pct_s(move_pct: float, start_mono: float, end_mono: float) -> float
def volume_acceleration(state: MarketState, exchange: str, asset: str, now_mono: float, short_s: float = 5, long_s: float = 60) -> float | None
```

### engine/stats.py — `StatsStore` (learning)
```python
class StatsStore:
    def __init__(self, settings: Settings, db: "Database | None" = None): ...   # loads from db if given
    def get(self, asset: str, exchange: str) -> ReactionStats                   # creates empty if missing
    def record_first_mover(self, asset, exchange) -> None
    def record_lagging(self, asset, exchange, lag_pct: float) -> None
    def record_reaction(self, asset, exchange, delay_s: float, catchup_ratio: float) -> None
    def record_false_signal(self, asset, exchange) -> None
    def record_opportunity_duration(self, asset, exchange, duration_s: float) -> None
    def record_trade(self, asset, exchange, success: bool, realized_pct: float | None, slippage_pct: float | None, exec_latency_ms: float | None) -> None
    def median_reaction_delay(self, asset, exchange) -> float | None
    def reaction_percentile(self, asset, exchange, p: float) -> float | None
    def median_duration(self, asset, exchange) -> float | None
    def catchup_ratio(self, asset, exchange) -> float | None                    # median of catchup_ratios, None if < 3 samples
    def first_mover_frequency(self, asset, exchange) -> float | None            # first / (first + lagging)
    def summary(self) -> list[dict]                                             # one row per (asset, exchange) for UI
    def save(self) -> None                                                      # persist all to db (no-op without db)
```
Lists are capped at `settings.stats_max_samples` (drop oldest).

### engine/mover.py — `MoveDetector`
```python
class MoveDetector:
    def __init__(self, settings: Settings, state: MarketState, stats: StatsStore, clock: Clock, bus: EventBus | None = None): ...
    def on_book(self, book: OrderBook) -> list[LagSignal]
    def allowed_direction(self, exchange: str, direction: int) -> bool   # SHORT only if ExchangeConfig.allow_short
```
Algorithm (per asset, on every book):
1. For every exchange with fresh data (`is_fresh` with `max_data_age_ms`),
   `move_over_window(mid_series, now, settings.move_window_s)` → move_pct.
2. Track a per-(exchange, asset) *crossing* state with hysteresis: when
   `|move| >= move_threshold_pct` and not crossed → crossed, record
   `cross_mono = now`, direction = sign, the *anchor* (pre-move price = start
   of the window), publish `MoveEvent` on bus (topic MOVE) and
   `stats.record_first_mover` only for the earliest crosser.  From then on the
   exchange is measured against its anchor, so a genuine move keeps its
   crossing after the window slid past the jump; reset when the anchored move
   falls below `move_threshold_pct / 2` (a reversion — the anchor is kept for
   one more window so the reversion is not read as a move the other way),
   silently when the data go stale, or when the episode outlives
   `max_opp_duration_s` (a timeout, not a reversion).  Exchanges that have not
   crossed are measured from their price at the episode's anchor moment (a
   slow catch-up ≥ threshold since the move began also counts as a cross).
   `current_moves(asset, now, max_age_ms)` exposes these effective moves; the
   pipeline refreshes opportunities with them, so refresh and detection agree.
3. Movers = crossed exchanges with the same direction.  If none → `[]`.
   First mover = earliest `cross_mono`.  `movers_count` = len(movers).
4. For each **execution** exchange T ≠ first mover, fresh, `allowed_direction`:
   `lag_pct = first_move_pct·dir − move_T·dir`.  If `lag_pct >= min_lag_pct`
   → `LagSignal(target=T, target_symbol=book-of-T.symbol, expected_catchup_pct =
   lag_pct * ratio, catchup_ratio=ratio, median_reaction_delay_s=stats…)` where
   `ratio = stats.catchup_ratio(asset, T) or settings.catchup_ratio_default`.
   Call `stats.record_lagging(asset, T, lag_pct)` once per crossing episode.
5. Learning: when T later crosses in the same direction, record
   `reaction delay = T.cross_mono − first.cross_mono` and
   `catchup_ratio = (move_T·dir) / first_move_pct` (clamp 0..1.5).  When the
   movers' crossings reset by *reversion* without T crossing →
   `record_false_signal` (a stale clear or a timed-out episode is not one).
Return one `LagSignal` per lagging target (the pipeline evaluates each; the
best by NET is shown on top — dynamic target selection).

### engine/economics.py
```python
def walk_book(levels: list[BookLevel], *, quote_amount: float | None = None, base_amount: float | None = None,
              limit_price: float | None = None) -> WalkResult
    # consume levels in order until quote_amount (or base_amount) is filled; stop at limit_price if given
def depth_within(levels: list[BookLevel], max_slippage_pct: float) -> float     # quote depth within best*(1±slip)
def compute_economics(book: OrderBook, direction: int, fees: Fees, capital_quote: float,
                      expected_catchup_pct: float, settings: Settings, now_mono: float) -> ExecutionEconomics
```
`compute_economics` (dir +1: entry=asks, exit=bids; dir −1: entry=bids, exit=asks):
* `executable_quote = min(capital, depth_within(entry), depth_within(exit))`
  (both within `settings.max_slippage_pct`); if `< settings.min_trade_quote`
  → `liquidity_ok=False`, reason "book supports only $X".  Also
  `liquidity_ok=False` if either depth `< settings.min_liquidity_quote` or
  `executable_quote < capital * settings.min_fill_ratio` (reason explains; the
  card then shows CAPITAL = executable_quote, not the requested capital).
* `entry_vwap` = walk entry for `executable_quote` → `base_amount`.
* `exit_vwap_now` = walk exit for `base_amount`.  `exit_target = exit_vwap_now * (1 + dir*expected_catchup_pct/100)`.
* `gross_pct = (exit_target/entry_vwap − 1)*100*dir`; fees taker both legs
  (`fees.taker_pct`); `execution_loss_pct = settings.execution_loss_pct`;
  `total_cost_pct = entry_fee + exit_fee + execution_loss`; `net_pct = gross − total_cost`;
  `net_quote = executable_quote * net_pct / 100`.  Spread is reported in
  `spread_pct` but NOT added again (already inside the VWAPs).
* Never raise on an empty side: return `liquidity_ok=False`, zeros.

### engine/duration.py — `DurationEstimator`
```python
class DurationEstimator:
    def __init__(self, settings: Settings, stats: StatsStore): ...
    def estimate(self, signal: LagSignal, opp: Opportunity | None, now_mono: float) -> float   # total seconds from first_seen
```
Base = `stats.median_duration(asset, target)` or `median_reaction_delay` or
`settings.default_opp_duration_s`.  If `opp` has ≥ 3 `net_history` points
spanning ≥ 1 s, fit a linear decay of net_pct; if decaying, `remaining = net_now/rate`,
`live = age + remaining`, result = `0.5*base + 0.5*live`.  Clamp to
`[min_opp_duration_s, max_opp_duration_s]`.  Never below current age + 0.1.

### engine/executability.py
```python
def assess_human(remaining_s: float, net_pct: float, settings: Settings, stats: ReactionStats | None = None,
                 econ: ExecutionEconomics | None = None) -> ExecAssessment
def assess_bot(remaining_s: float, econ: ExecutionEconomics, health: ExchangeHealth | None,
               stats: ReactionStats | None, settings: Settings, now_mono: float) -> ExecAssessment
```
Human: `est = settings.human_execution_time_s`; GREEN if `remaining ≥ est*human_green_margin`,
YELLOW if `remaining ≥ est`, else RED ("too fast for human").  Labels:
GREEN→"HIGH", YELLOW→"MEDIUM"/"LOW" (≥ est*1.2 → MEDIUM), RED→"NOT EXECUTABLE".
With `econ`: `executable_quote < min_trade_quote` → RED "NOT EXECUTABLE"
("not executable at size …") whatever the window — a size nobody would trade
is not an opportunity (other `liquidity_ok=False` cases keep the time light).
Bot: `est_s = (ws_latency + order_latency(default settings.default_api_latency_ms)
+ fill_latency(if known) + bot_fixed_overhead_ms)/1000`.  RED when: health
missing/not usable, `econ.book_age_ms > max_data_age_ms`, order latency
`> max_api_latency_ms`, `not econ.liquidity_ok`, `econ.net_pct < min_net_pct_bot`,
or `remaining < est*bot_yellow_margin`.  YELLOW when `remaining < est*bot_green_margin`
or stats.success_rate < 0.5 with ≥ 5 trades.  Else GREEN.  Always fill `reasons`.
`probability`: rough 0..1 (GREEN 0.8, YELLOW 0.5, RED 0.1, adjusted by success_rate).

### engine/explain.py
```python
def build_why(signal: LagSignal, econ: ExecutionEconomics, human: ExecAssessment, bot: ExecAssessment,
              est_duration_s: float) -> str
```
≤ 6 short lines, e.g.
```
Binance moved +4.1% 1.8 s ago. Bybit moved only +1.2% (lag +2.9%).
Historical median reaction delay on Bybit: 3.4 s. Expected catch-up: +1.7% (ratio 0.6).
Target book supports $250 within 0.3% slippage.
Costs: fees 0.11% + execution 0.05%. Net after costs: +1.54%.
HUMAN 🟡 (7 s needed, ~8 s window). BOT 🟢 (est. 0.4 s).
```

### engine/opportunity.py — `OpportunityManager`
```python
class OpportunityManager:
    def __init__(self, settings: Settings, bus: EventBus, clock: Clock, db: "Database | None" = None): ...
    @staticmethod
    def key(asset: str, target: str, direction: int) -> str
    def find_active(self, asset: str, target: str, direction: int) -> Opportunity | None
    async def upsert(self, signal: LagSignal, econ: ExecutionEconomics, human: ExecAssessment, bot: ExecAssessment,
                     why: str, est_duration_s: float, mode: Mode) -> tuple[Opportunity, bool]   # (opp, is_new)
    async def transition(self, opp_id: str, new_state: OppState, reason: str = "") -> Opportunity  # enforces models.TRANSITIONS, appends state_history, publishes OPP_STATE, persists
    async def expire(self, opp_id: str, reason: str) -> Opportunity | None
    async def sweep(self, now_mono: float) -> list[Opportunity]   # expire active opps: remaining_s==0, or no update for > 2*move_window_s, or net < min_net_pct_show
    def get(self, opp_id: str) -> Opportunity | None
    def active(self) -> list[Opportunity]                          # ACTIVE_STATES, sorted by net desc
    def executing(self) -> list[Opportunity]
    def history(self, limit: int = 200) -> list[Opportunity]       # terminal, newest first (in-memory ring + db)
    def best(self, now_mono: float) -> Opportunity | None          # highest net among active where bot or human light != RED
    def counters(self, now_mono: float) -> dict                    # dashboard numbers (see §Dashboard)
    async def relabel_mode(self, mode: Mode) -> list[Opportunity]  # PAPER/LIVE switch: active cards show the mode that would execute
```
New opp: id `new_id("opp_")`, state DETECTED → (pipeline moves it) VALIDATING → VALID.
Update: replace econ/human/bot/why/estimated_duration/mode, `last_update_mono`,
`last_valid_mono` (if net ≥ min_net_pct_show), append `net_history`, bump
`update_count`, publish OPP_UPDATE.  Expiry sets `expired_mono`,
`actual_duration_s`, `expire_reason`, state EXPIRED, publishes OPP_STATE, and
calls `stats.record_opportunity_duration` (pass stats via constructor kwarg
`stats: StatsStore | None = None`).  Opps in EXECUTION_STATES are never expired
by sweep (the execution engine owns them).

### engine/pipeline.py — `Pipeline`
```python
class Pipeline:
    def __init__(self, settings, bus, clock, state: MarketState, detector: MoveDetector, stats: StatsStore,
                 opps: OpportunityManager, adapters: dict[str, ExchangeAdapter], duration: DurationEstimator,
                 db: "Database | None" = None): ...
    async def start(self) -> None     # subscribe BOOK/TRADE/TICKER/HEALTH, start sweep loop (every settings.ui_push_interval_ms)
    async def stop(self) -> None
    async def on_book(self, book: OrderBook) -> list[Opportunity]
    async def evaluate(self, signal: LagSignal, capital_quote: float | None = None) -> Opportunity | None
    async def refresh(self, opp: Opportunity, capital_quote: float | None = None) -> Opportunity | None   # recompute from the latest FRESH book; None = no fresh data
    def dashboard(self) -> dict
```
`evaluate`: book = state.book(target) (must be younger than `max_data_age_ms`;
a missing / stale book is confirmed through REST in a bounded background task
— single-flight per (exchange, symbol), ≤ 2 requests per exchange,
`rest_book_timeout_s` each — and `evaluate` returns None until that task
re-measures the signal on live data and evaluates it on the fresh snapshot;
the BOOK handler never waits on REST) → fees = `await adapters[target].fees(symbol)`
(cache) → `compute_economics` → if no active opp and (`econ.net_pct <
settings.min_net_pct_show` or `econ.executable_quote < min_trade_quote`) → None
(no fake signals) → duration → human/bot → why → `opps.upsert(…, settings.mode())`
→ transitions DETECTED→VALIDATING→VALID for new opps → return.
`refresh`: same freshness rule, but inline (the single-flight fetch is awaited,
so the 300 ms MANUAL poll cannot hammer the API); the first mover is
re-measured with the freshness bound of the signal's source (poll signals get
`+ ticker_poll_interval_s`); None when there is no fresh book.  Sweep loop:
`opps.relabel_mode(settings.mode())`, expire active opps whose target book is
older than 2× their freshness bound ("target book stale"), `opps.sweep`,
`adapter.check_staleness`, publish DASHBOARD.  `pending_confirmations` /
`wait_confirmations()` expose the background confirmations (tests, shutdown).

### exchanges/ccxt_adapter.py — `CcxtAdapter(ExchangeAdapter)`
```python
class CcxtAdapter(ExchangeAdapter):
    def __init__(self, cfg: ExchangeConfig, bus: EventBus, clock: Clock, settings: Settings,
                 creds: ExchangeCredentials | None = None): ...
    def symbol_for(self, asset: str) -> str | None
    def asset_for(self, symbol: str) -> str | None
```
* Two ccxt.pro instances: **public** (no keys) for market data, **private**
  (keys) only when `creds.present` and `cfg.role == EXECUTION`.
* `market_source: ExchangeAdapter | None` (set by `registry.build_execution_adapters`
  on the LIVE adapter and on the Bybit demo paper adapter): the market adapter of
  the same exchange.  When set, `health` **is** the market adapter's object (one
  per exchange: stream liveness / `stale` from the market stream, order / REST
  latency EWMAs and rate-limit back-off from both) and `order_book()` falls back
  to its cached book, so `revalidate` reads the live book without a second stream.
  `start(assets, ws_assets=0)` = execution-only (no `_book_loop` / `_trades_loop`
  / `_health_loop`; `fetch_order_book` on the own public instance still works).
* `cfg.demo` + bybit → `exchange.enable_demo_trading(True)` on the private
  instance; assert the resulting REST base contains `api-demo` or raise.  Never
  send a live order from a demo-configured adapter to a non-demo host.
* `start(assets)`: `load_markets`, resolve each asset to a market with
  `cfg.market_type` (spot: `market['spot']`; swap: `market['swap'] and market['linear']`)
  and quote in `settings.quote_assets` preference order; skip unknown (log once).
  One `watch_order_book` task per symbol (limit `cfg.book_depth`), reconnect
  with exponential backoff (1→30 s), set health, publish BOOK (OrderBook with
  `asset`, `Timestamps(exchange_ts=ob['timestamp']/1000 if present)`).  Trades via
  `watch_trades` if supported (best effort).
* `fees(symbol)`: from `market['taker']/['maker']` else cfg; try
  `fetch_trading_fee` once if private.
* `place_order`: map `OrderRequest` → `create_order` with `timeInForce` (IOC) /
  `postOnly`; `params['clientOrderId']`/`orderLinkId` = `req.client_id` for
  idempotency; swap SHORT entry = sell, SHORT exit = buy with `reduceOnly`; set
  `sent_mono`, `ack_mono`, `_mark_order_latency`.  Map ccxt errors:
  `InsufficientFunds`→`InsufficientBalance`, `RateLimitExceeded`/`DDoSProtection`
  →`RateLimited` (+ set `health.rate_limited_until_mono`), `InvalidOrder`→`OrderRejected`,
  `ExchangeNotAvailable`/`OnMaintenance`→ set `health.maintenance` & raise `ExchangeError`.
* `fetch_order` fills `Order.fills` from `order['trades']` if present else from
  `filled/average/fee`.

Cross-module conventions settled during integration (all adapters + engine):
* An IOC limit order that fills only partially ends with status `CANCELLED` and
  `filled > 0` (`fills` kept) - callers branch on `order.filled`, never on the
  status alone (the execution engine does).
* `place_order(req, *, reduce_only=False)` on every adapter; the engine passes
  `reduce_only=True` only for swap exits and only when the signature accepts it.
* `fetch_order(order_id, symbol, *, client_id=None)` exists on `CcxtAdapter`
  only; the engine's timeout reconcile inspects the signature.
* `Database` stores `Mode.value` / `OppState.value`; callers pass `.value`
  (`Pipeline.dashboard`, `RiskManager.daily_pnl`) and the db also accepts enums.
* `confirm_auto(opp_id, wait=True)`: the HTTP route passes `wait=False` (the UI
  follows `TRADE_UPDATE` over the WebSocket); engine business refusals are
  HTTP 200 `{ok: false, reason}`.
* Risk cap: `max_risk_per_trade_pct × total_capital_quote` is a hard position
  cap - with the defaults (2 % × 1000) every AUTO trade is downsized to $20;
  raise either value in `config.json` for larger trades.
* `exchange X: no market data yet` (INFO) is logged on the first sweep for
  exchanges that have not connected; `stale` (WARNING) only after a real drop.

### exchanges/paper.py — `PaperAdapter(ExchangeAdapter)`
```python
class PaperAdapter(ExchangeAdapter):
    def __init__(self, inner: ExchangeAdapter, settings: Settings, bus: EventBus, clock: Clock,
                 db: "Database | None" = None, initial_quote: float | None = None, quote_asset: str = "USDT"): ...
```
Wraps a market-data adapter (`inner`), shares its books/health (`order_book`,
`fetch_order_book`, `fetch_ticker`, `supported_symbols`, `symbol_for` delegate).
Virtual balances (persisted via db kv if given; reset method `reset(initial)`).
`place_order`: `await asyncio.sleep(latency)` where latency = inner order
latency EWMA or `settings.default_api_latency_ms` (+ jitter via `random` —
fine in runtime code, keep deterministic with `seed` kwarg for tests), then
walk the **current** inner book with `walk_book(limit_price=req.price)`;
market orders walk fully; IOC limit: partial fill if the book is thinner, rest
CANCELLED; fees = inner taker; swap positions (SHORT allowed) tracked in
`self.positions[symbol] = {"side", "amount", "entry"}`; balances updated;
`Order.fills` populated; publish ORDER_UPDATE and BALANCE.  `cancel_order`
on a filled order is a no-op returning the order.  `name` = inner.name (same
exchange name — mode distinguishes).

### exchanges/fake.py — `FakeAdapter(ExchangeAdapter)` (tests + backtest replay)
```python
class FakeAdapter(ExchangeAdapter):
    def __init__(self, cfg: ExchangeConfig, bus: EventBus, clock: Clock, symbols: dict[str, str] | None = None): ...  # asset -> symbol
    async def push_book(self, asset: str, bids: list[tuple[float, float]], asks: list[tuple[float, float]],
                        exchange_ts: float | None = None) -> OrderBook   # stores + publishes BOOK + marks health
    fill_latency_s: float = 0.0; reject_next: bool = False; partial_ratio: float | None = None  # knobs
```
Trading fills against the stored book like PaperAdapter (reuse `walk_book`).

### exchanges/registry.py
```python
def build_market_adapters(settings: Settings, secrets: Secrets, bus: EventBus, clock: Clock) -> dict[str, ExchangeAdapter]
def build_execution_adapters(settings, secrets, bus, clock, market: dict[str, ExchangeAdapter], db) -> dict[Mode, dict[str, ExchangeAdapter]]
    # PAPER: PaperAdapter(inner=market[name]) per execution exchange; if settings.paper_engine == "bybit_demo" and bybit has demo creds → the real CcxtAdapter(demo=True) is used for bybit instead
    # LIVE: real CcxtAdapter with creds for execution exchanges that have creds.present (others omitted)
async def resolve_watchlist(settings: Settings, market: dict[str, ExchangeAdapter]) -> list[str]
    # settings.watchlist, or when watchlist_auto: top-N by 24h quote volume on settings.watchlist_exchange (fetch_tickers), BTC always first
```

### storage/db.py — `Database` (sqlite3, sync, thread-safe, WAL)
```python
class Database:
    def __init__(self, path: str): ...        # creates schema; ":memory:" ok for tests
    def close(self) -> None
    def save_opportunity(self, opp: Opportunity) -> None          # upsert by id (json blob + indexed columns)
    def list_opportunities(self, limit: int = 200, state: str | None = None, asset: str | None = None, exchange: str | None = None, since_ts: float | None = None) -> list[dict]
    def save_trade(self, trade: TradeRecord) -> None
    def list_trades(self, limit: int = 200, mode: str | None = None, since_ts: float | None = None) -> list[dict]
    def save_order(self, order: Order, trade_id: str | None) -> None
    def save_stats(self, stats: ReactionStats) -> None;  def load_stats(self) -> list[ReactionStats]
    def kv_get(self, key: str, default=None);  def kv_set(self, key: str, value) -> None   # json values
    def daily_pnl(self, day: str, mode: str) -> float      # sum realized_pnl_quote of trades finished that day (UTC 'YYYY-MM-DD')
    def counters_today(self, mode: str) -> dict             # opportunities_today, trades, failed, expired, realized_pnl
```

### execution/risk.py — `RiskManager`
```python
class RiskManager:
    def __init__(self, settings: Settings, db: Database | None, clock: Clock, bus: EventBus | None = None): ...
    emergency: bool; emergency_reason: str
    def check(self, exchange: str, asset: str, capital_quote: float, mode: Mode) -> tuple[bool, str, float]   # (ok, reason, allowed_capital ≤ requested; respects max_trade_quote, max_risk_per_trade_pct*total_capital, exposure limits, max_simultaneous_trades, max_daily_loss, emergency)
    def register_open(self, trade_id: str, exchange: str, asset: str, quote: float) -> None
    def register_close(self, trade_id: str, realized_pnl_quote: float) -> None
    def open_trades(self) -> int;  def exposure(self, exchange: str | None = None, asset: str | None = None) -> float
    def daily_pnl(self, mode: Mode) -> float
    async def set_emergency(self, active: bool, reason: str = "") -> None   # publishes EMERGENCY
```

### execution/engine.py — `ExecutionEngine`
```python
class ExecutionEngine:
    def __init__(self, settings, bus, clock, pipeline: Pipeline, opps: OpportunityManager,
                 exec_adapters: dict[Mode, dict[str, ExchangeAdapter]], risk: RiskManager,
                 db: Database | None, stats: StatsStore): ...
    def adapter_for(self, opp: Opportunity) -> ExchangeAdapter | None       # by settings.mode()
    async def select_manual(self, opp_id: str) -> dict                       # → MANUAL_SELECTED; returns manual_support()
    async def manual_support(self, opp_id: str) -> dict                      # live payload (see below)
    async def manual_entered(self, opp_id: str, price: float, quote_amount: float) -> dict   # user did the entry by hand → TradeRecord(mode) with entry filled; returns exit target info
    async def manual_exited(self, opp_id: str, price: float) -> TradeRecord  # realized P&L, CLOSED
    async def select_auto(self, opp_id: str, wait: bool = True) -> dict      # → AUTO_SELECTED; if settings.auto_confirm: execute now (wait=False: start in the background like confirm_auto); else {"confirmation_required": True, "snapshot": {...}}
    async def confirm_auto(self, opp_id: str, wait: bool = True) -> dict     # revalidate + execute (wait=False → {"ok": True, "started": True, "state": "EXECUTING"}; follow TRADE_UPDATE)
    async def deselect_auto(self, opp_id: str, reason: str = "...") -> dict  # back out of the confirmation: AUTO_SELECTED → VALID, nothing cancelled; any other state → ok False, untouched
    async def cancel(self, opp_id: str, reason: str = "user") -> dict
    async def revalidate(self, opp: Opportunity) -> tuple[bool, str, ExecutionEconomics | None]
    async def execute(self, opp: Opportunity) -> TradeRecord
    async def emergency_stop(self, reason: str = "user") -> dict             # risk.set_emergency(True); cancel pending orders on the exec adapters of the CURRENT mode (plus adapters holding a pending exit / open position); adapters of the other mode are never touched; abort EXECUTING trades before entry; keep history
    async def emergency_close_positions(self) -> list[TradeRecord]           # resolve unknown orders (fetch by id / client id, cancel what still rests), then market-exit every open position (paper + live adapters of current mode)
    async def resume(self) -> None
    async def shutdown(self, timeout_s: float) -> list[dict]                 # abort running trades (market-exit, like cancel()), wait up to timeout_s, cancel the rest; returns the positions still open
    def restore_from_db(self) -> list[TradeRecord]                           # after a restart: EXECUTING/PARTIALLY_FILLED/FILLED/FAILED-with-position rows -> tracked open positions + risk exposure (never auto-traded)
    async def announce_open_positions(self) -> None                          # TRADE_UPDATE for every tracked open position (UI / Telegram)
    def trades(self, limit: int = 200) -> list[TradeRecord]
```
* `manual_support` payload: `{opp_id, valid: bool, status, side, exchange, symbol,
  entry_price_now, exit_target_now, net_now_pct, expected_slippage_pct,
  remaining_s, human: {light,label,est_execution_s}, capital, base_amount,
  expired: bool, message}` — recomputed from the **latest** book via
  `pipeline.refresh`.  If the opp is no longer active → `valid=False,
  message="⚠️ OPPORTUNITY EXPIRED"`.  While a manual position is open the
  payload is `status="in_position"` with `trade_id, entry_price, quote_amount,
  exit_side` (also from `select_manual`), so a reloaded page / second tab can
  resume the exit step instead of starting a new entry.  A MANUAL position
  restored after a restart has no opportunity any more (`OpportunityManager`
  starts empty): `select_manual` / `manual_support` still answer
  `in_position` for its id (live numbers `None`, `fresh=False`) and
  `manual_exited` closes it; the manual API routes accept such an id through
  `execution.active_trade` (a new entry still needs a live opportunity).
* `revalidate`: book age ≤ max_data_age_ms (else `fetch_order_book` REST once);
  recompute econ with the current catch-up; require `econ.net_pct ≥ max(settings.min_net_pct_auto,
  opp.econ.net_pct * settings.revalidate_tolerance)`, `liquidity_ok`, bot light
  ≠ RED, balance ≥ needed where needed = capital × (1 + taker fee + `buy_limit_slippage_pct`)
  (what the entry order really locks; downsize to free / that factor if `allow_downsize`,
  else fail), `risk.check` ok.  Returns reason strings a human can read.
* `execute`: AUTO flow → `EXECUTING`; place entry (LIMIT IOC at
  `entry_vwap*(1±buy_limit_slippage_pct)`), wait ≤ `buy_fill_timeout_s` polling
  `fetch_order` (or returned status); no fill → CANCELLED; partial below
  `min_partial_fill_ratio` → cancel rest, exit what was filled at market, FAILED
  reason; otherwise `PARTIALLY_FILLED`/`FILLED`; exit leg: LIMIT GTC at
  `exit_target` (reduceOnly for swap), wait ≤ `sell_timeout_s`; if unfilled and
  `exit_on_timeout == "market"` → cancel + market exit; else hold until
  `max_hold_s` then market.  Build `TradeRecord` with every timestamp, call
  `pnl.finalize`, `risk.register_close`, `stats.record_trade`, opp → CLOSED
  (or FAILED), publish TRADE_UPDATE at every step, persist.
* Never execute if `risk.emergency`; re-check right before each order.
* Hard rule 4 covers both legs: right before the exit order the engine refreshes
  the book (REST when the WS book is older than `max_data_age_ms`) and reprices
  the exit from the fresh economics; when the net on the held position is ≤ 0 the
  position is exited at market immediately (the planned target is used only when
  the refresh is unavailable - a position is never left unmanaged).
* Unknown order state is never guessed: a timed-out `place_order` (asyncio timeout,
  `ccxt.RequestTimeout`, or the adapter's `SENT`/empty-id return) is reconciled by
  `fetch_order(client_id=…)`; a cancel whose confirmation and follow-up fetch both
  fail leaves the order open.  An unresolved entry/exit makes the trade FAILED with
  the position tracked (`open_positions()`, risk exposure kept, `unresolved: true`)
  - never "entry not filled", never a second exit for the same quantity.
  `emergency_close_positions` resolves the pending order first.
* Process lifecycle: `run_app` calls `shutdown()` before the adapters and the
  database go away; on cancellation the position stays tracked; `build_context`
  calls `restore_from_db()` so a crash/restart (restart loop, stop.bat) never
  forgets a position.

### execution/pnl.py
```python
def finalize(trade: TradeRecord, fees_quote: float | None = None) -> TradeRecord   # fills actual_* fields, realized_pnl_quote, actual_net_pct, actual_slippage_pct, execution_time_s, diff_reason ("slippage +0.3%, fees 0.11%, latency 820 ms")
```

### notify/telegram.py — `TelegramNotifier`
```python
class TelegramNotifier:
    def __init__(self, settings, secrets, bus, opps: OpportunityManager, execution: ExecutionEngine | None,
                 risk: RiskManager | None, clock: Clock, session: "aiohttp.ClientSession | None" = None): ...
    async def start(self) -> None     # no-op if no token; starts getUpdates long-poll task + subscribes OPP_*
    async def stop(self) -> None
    def should_alert(self, opp: Opportunity) -> bool   # net ≥ telegram_profit_threshold_pct and (bot or human light != RED when telegram_require_executable) and not already notified and active
    def format_alert(self, opp: Opportunity, now_mono: float) -> str
    async def send(self, text: str, buttons: list[list[dict]] | None = None) -> int | None   # message id
```
Alert format (English keys, as specified):
```
🔥 REAL OPPORTUNITY
BTC/USDT  LONG
TARGET: Bybit
NET: +4.82%
BUY: $102,140
SELL: $107,065
CAPITAL: $100
EXPECTED PROFIT: +$4.82
WINDOW: ~11 sec
HUMAN: 🟡 MEDIUM   BOT: 🟢 HIGH
```
Inline button `OPEN OPPORTUNITY` → `{settings.public_url or http://host:port}/#opp/{id}`.
Dedupe by `opp.id`; on OPP_UPDATE edit the message at most every 3 s; on
EXPIRED/CLOSED edit once: `… EXPIRED after 9.4 s` / `CLOSED: realized +3.9%`.
Commands (reply in Russian): `/status`, `/stop` (emergency stop, asks to
confirm with inline button), `/resume`, `/human <sec>`, `/threshold <pct>`,
`/mode`, `/help`.  Only accept commands from `secrets.telegram_chat_id` if set;
otherwise from the first chat that writes (persist in db kv "telegram_chat_id").
Store "telegram_offset" in kv.  Never log the token (URL contains it: log only
the method name).

### api/server.py — `create_app(ctx) -> FastAPI`
Routes (JSON):
* `GET /` → `ui/index.html`; static `/static/*` from `latbot/ui/`.
* `GET /api/state` → `{mode, emergency, settings: settings.public(), health: [ExchangeHealth…], dashboard, best: opp|null, version}`
* `GET /api/opportunities?state=active|history|all&asset=&exchange=&min_net=&human=&bot=` → list of `opp.to_dict()`
* `GET /api/opportunities/{id}`
* `GET /api/opportunities/{id}/chart?seconds=15..60` → real mid-price history (`MarketState.mid_series`) of the movers and
  the target, each line in % from its first point inside the window, x = seconds before now (or before expiry for a
  finished opportunity); a stream with < 2 real points is not drawn (`has_data=false` when nothing is left)
* `POST /api/opportunities/{id}/manual` → `execution.select_manual`
* `GET  /api/opportunities/{id}/manual/support` → `execution.manual_support`
* `POST /api/opportunities/{id}/manual/entered {price, quote_amount}`; `POST …/manual/exited {price}`
* `POST /api/opportunities/{id}/auto` → `select_auto`; `POST …/auto/confirm`; `POST …/auto/deselect`
  (back out of the confirmation: AUTO_SELECTED → VALID, nothing cancelled); `POST …/cancel`.
  Both `/auto` routes pass `wait=False`: an auto-confirmed trade starts in the background and the
  response is `{ok, started, state: "EXECUTING"}` - the HTTP request never waits for the trade.
* `POST /api/emergency/stop {reason?}`, `POST /api/emergency/close`, `POST /api/emergency/resume`
* `GET /api/settings`; `PUT /api/settings {patch}` → `settings.update` + `save_settings` + publish SETTINGS (reject keys not in Settings; never secrets)
* `GET /api/trades?limit=`, `GET /api/stats`, `GET /api/balances`
* `WS /ws` → server pushes `{"topic": "...", "data": ...}` for OPP_NEW/OPP_UPDATE/OPP_STATE/TRADE_UPDATE/HEALTH/DASHBOARD/EMERGENCY/SETTINGS/BALANCE; opp payloads are `to_dict()`; coalesce OPP_UPDATE to ≤ 4/s per opp.
`ctx` is `latbot.app.AppContext` (attributes: settings, secrets, bus, clock, db,
state, stats, opps, pipeline, adapters, exec_adapters, risk, execution,
notifier).  Use duck typing — do not import app.py.

### ui/ (static: index.html, app.js, style.css — no build step, no CDN)
Dark trading terminal.  Top bar: title, **PAPER/LIVE** badge (big, coloured),
🛑 EMERGENCY STOP (confirm dialog), settings drawer (HUMAN_EXECUTION_TIME,
TELEGRAM threshold, capital, auto confirm, paper/live toggle…).  Dashboard
strip (counters from §Dashboard).  `🔥 BEST CURRENT OPPORTUNITY` panel.
Cards grid exactly like the spec card (asset, LONG/SHORT tag, TARGET badge,
NET PROFIT big, BUY/SELL, CAPITAL, EXPECTED PROFIT, OPPORTUNITY WINDOW with a
countdown bar from `remaining_s`, HUMAN 🟡 line, BOT 🟢 line, WHY? expander,
[MANUAL] [AUTO] buttons).  Filters: ALL / MANUAL / AUTO / ≥3 % / PAPER / LIVE /
EXPIRED + exchange, asset, min profit, min duration, human light, bot light.
MANUAL modal: BUY block (exchange, pair, price, amount), countdown
`OPPORTUNITY: 8.2 s remaining / HUMAN EXECUTION: ~6 s / STATUS 🟢`, auto flips to
`🔴 TOO FAST FOR HUMAN` when remaining < human time, `⚠️ OPPORTUNITY EXPIRED`
blocks the form, "I bought at …" → shows SELL block; polls
`/manual/support` every 300 ms.  AUTO modal: confirmation (`CURRENT NET`,
`EXPECTED PROFIT`, `RISK`, `BALANCE`, [EXECUTE] [CANCEL]) unless auto_confirm.
Trades/history tab with expected vs actual.  Deep link `#opp/<id>` opens the
card.  Live updates via WebSocket with reconnect.

### backtest/recorder.py & backtest/engine.py
* `MarketRecorder(settings, bus, clock, out_dir="data/market")`: when
  `settings.record_market_data`, append one JSON line per BOOK
  (`{"t": recv_ts, "ex", "asset", "symbol", "ets": exchange_ts, "b": [[p,s]…10], "a": […]}`)
  to `YYYY-MM-DD.jsonl` (gzip rotate daily).
* `run_backtest(paths: list[str], settings: Settings) -> dict`: replay through
  `SimClock`, `FakeAdapter`s, `MarketState`, `MoveDetector`, `Pipeline`,
  `PaperAdapter`, `ExecutionEngine` with `auto_confirm=True`, executing
  opportunities whose bot light is GREEN; fills are simulated **against the
  book at `t + latency`** (not the detection book).  Report: opportunities
  found, executed, expected vs realized net (mean/median), by asset/exchange,
  human-executable count, false signals.  CLI `python -m latbot.backtest <files>`.

### app.py (integration)
`AppContext`, `build_context(settings, secrets) -> AppContext`,
`async run_app(ctx, seconds: float | None = None)`, `main(argv) -> int`:
single-instance lock (`logs/bot.lock`, msvcrt/fcntl), `logs/bot.pid`,
`logs/stop.flag` polling, restart loop with backoff on crash, Windows
`WindowsSelectorEventLoopPolicy`, UTF-8 stdout; `python -m latbot` and `run.py`.

Integration decisions (implemented):
* `AppContext` fields: `settings, secrets, bus, clock, db, state, stats, detector,
  duration, opps, adapters` (market adapters, name → adapter), `pipeline,
  exec_adapters` (`{Mode: {name: adapter}}`), `risk, execution, notifier, recorder,
  app` (FastAPI), `config_path, stop_flag, stop_flag_poll_s, watchlist, poll_assets,
  poller, server, server_port, stop_reason, request_stop`.
* `build_context(settings, secrets, *, bus=None, clock=None, db=None,
  adapter_factory=None, config_path="config.json", stop_flag=None)` wires
  Database → StatsStore → MarketState → MoveDetector → DurationEstimator →
  OpportunityManager → market adapters (registry, `adapter_factory(cfg, creds)`
  lets tests inject `FakeAdapter`s) → Pipeline → execution adapters → RiskManager
  → ExecutionEngine → TelegramNotifier → MarketRecorder → TickerPoller →
  `api.create_app`.  No network is touched; optional components that fail to
  build are logged and set to `None` (the app still runs).
* `run_app(ctx, seconds=None, *, serve=True) -> int`: `resolve_watchlist` (20 s
  bound, falls back to `settings.watchlist`, never empty) → every market adapter
  is started in a **background task with retry** (5 → 60 s backoff; a dead
  exchange never blocks the UI); real execution adapters (LIVE, and the Bybit
  demo adapter used as the paper venue) are started the same way but
  **execution-only** (`start(assets, ws_assets=0)`: markets loaded, symbols
  resolved, demo host asserted, no streams and no health loop of their own -
  `_adapters_to_start(ctx)` yields `(adapter, ws_assets)` pairs) → `pipeline.start()`,
  recorder, Telegram, ticker poller (watchlist first, the common universe is
  added in the background once `resolve_universe` succeeds) → uvicorn
  `Server.serve()` **inside the same event loop** (signals are owned by the app,
  not by uvicorn; a bind failure is logged and the bot keeps running without the
  UI; `ctx.server_port` holds the bound port, `port=0` → OS-assigned).  Stop
  conditions: `seconds`, `stop.flag` (polled every `ctx.stop_flag_poll_s`, removed
  when seen), SIGINT/SIGTERM, or `ctx.request_stop(reason)`.  Shutdown order:
  helper tasks → server (10 s, then forced) → execution
  (`ExecutionEngine.shutdown`: abort running trades → market exit, bounded
  wait) → Telegram → poller → recorder → pipeline → adapters →
  `stats.save()` → `db.close()`.  Returns 0.
* `main(argv)`: `[SECONDS] [--seconds N] [--config PATH] [--env PATH]
  [--no-server]`.  Order: platform setup → secrets → settings → logging
  (`settings.log_dir/log_level`) → `config.json` written when missing (all
  parameters visible) → `logs/bot.lock` (exit **3** when another instance holds
  it) → `logs/bot.pid` → a stale `stop.flag` is removed (the lock proves the old
  process is gone) → restart loop: clean return → exit code; crash → `10 s ×
  failures` (max 120 s) then rebuild with freshly loaded `.env`/`config.json`;
  with `SECONDS` a crash exits **1** after one try; `stop.flag` after a crash →
  exit 0.  `KeyboardInterrupt` is a clean stop (0).
* `start.bat` loops `python -u run.py` with a 5 s pause and ends the loop on exit
  codes 0 and 3; `stop.bat` writes `logs\stop.flag` and waits (≤ 30 s) for
  `logs\bot.pid` to disappear; `restart.bat` = stop + start.  All `.bat` files
  are ASCII with CRLF line endings (guarded by `tests/test_app.py`).

## Dashboard counters (`OpportunityManager.counters` + `Pipeline.dashboard`)
`active`, `today`, `green`, `yellow`, `red` (by best of human/bot), `human_executable`
(human != RED), `bot_executable`, `telegram_alerts`, `paper_pnl`, `live_pnl`,
`realized_pnl`, `failed_trades`, `expired`, `avg_latency_ms` (mean of exec
adapters' order latency), `avg_execution_time_s`, `success_rate` =
wins / (closed + failed) of today's trades in the current mode (`None` when
nothing finished; EXECUTING / CANCELLED rows do not count).

## Tests
`tests/` with pytest; `pytest.ini`: `asyncio_mode = auto`.  Everything offline:
use `FakeAdapter`, `SimClock`, `Database(":memory:")`.  Each module gets its own
`tests/test_<module>.py`.  Run: `python -m pytest -q` and `python -m pyflakes latbot tests`.

---

# Phase 2 — manual-first additions (owner trades by hand; no API keys yet)

Findings from the live run of the earlier prototype: (a) 327 of 341 "lag"
events were single-exchange spikes in an empty book that reverted within ~30 s
while no other venue followed — a spike on ONE exchange is not "the market
moved"; (b) 15 assets × 6 full order-book streams overloaded the event loop
and Coinbase drops connections above ~4 book subscriptions; (c) the owner's
venues are EU-licensed (Kraken, Coinbase, Bitvavo, Bitstamp, Crypto.com, OKX
EEA, Bybit EU, Gate EU) with EUR/USDC/USD markets, ~900 assets in common.

Settings/model fields for this phase already exist (see `config.py`:
`ws_assets`, `ticker_poll_*`, `poll_assets_max`, `confirm_with_rest_book`,
`price_sample_min_interval_ms`, `min_movers`, `min_mover_volume_quote`,
`min_target_volume_quote`, `alert_human_only`; `ExchangeConfig.stream`,
`max_ws_subscriptions`, `poll_tickers`, `pair_url`, `label`;
`LagSignal.source/target_quote/mover_volumes_quote`;
`Opportunity.quote/pair_url/caught_up/human_would_make_it/max_net_pct`;
`ReactionStats.human_window_*`, `caught_up_*`; `ExecAssessment.stats_rate/stats_n`;
`latbot/links.py`).

## 2.1 Moves are compared in percent, quotes may differ
Each exchange resolves an asset to ITS market (EUR on Kraken, USDC on Coinbase,
USDT on Binance).  `MoveDetector` compares `move_pct` per exchange, so no FX is
needed.  Economics run inside the target's own book and quote; the card shows
prices/capital with the market's quote (€ / $).  `LagSignal.target_quote` and
`Opportunity.quote` carry it; `Opportunity.pair_url = links.pair_url(target, target_symbol, cfg.pair_url)`.

## 2.2 Multi-venue confirmation (`min_movers`)
A `LagSignal` is emitted only when `movers_count >= settings.min_movers`
(default 2) and every counted mover has 24h quote volume
`>= min_mover_volume_quote` (volume from `MarketState.volume24h(exchange, asset)`,
fed by tickers; unknown volume → the mover does not count).  The target market
needs `volume24h >= min_target_volume_quote` when known.  Single-venue spikes
are still recorded as `MoveEvent`s (for learning) but never become
opportunities.

## 2.3 Coverage: WebSocket core + REST polling breadth
* `CcxtAdapter.start(assets, ws_assets)`: order-book (`stream="book"`) or
  best-bid/ask (`stream="bbo"`: `watch_bids_asks` if `has['watchBidsAsks']`
  else `watch_ticker`, publishing `Ticker`) streams for at most
  `min(ws_assets, cfg.max_ws_subscriptions or ∞)` assets (watchlist order).
  All other common assets are covered by polling.
* `CcxtAdapter.fetch_tickers() -> dict[str, Ticker]` (asset → Ticker for every
  resolved market; one REST call) and `resolve_assets(assets)` for the whole
  universe.
* `latbot/market/poller.py — TickerPoller(settings, adapters, state, detector, pipeline, clock)`:
  every `ticker_poll_interval_s`, for each adapter with `cfg.poll_tickers`,
  `fetch_tickers()` → `state.on_ticker()` (mid from bid/ask, else last; stores
  volume24h) → `detector.on_ticker(ticker)` → `LagSignal(source="poll")` →
  `pipeline.evaluate(signal)`.  Per-exchange backoff on errors / rate limits.
  Implemented details: `start(assets=None)` / `stop()` / `set_assets(assets)` /
  `poll_once() -> int`; **every** polled ticker feeds `state.on_ticker` (WS
  book/bbo streams carry no 24h volume, so without polling the
  `min_mover_volume_quote` gate can never pass) — with `append_mid=False` for
  assets the same exchange streams over WebSocket (volume / ticker / liveness
  only: a REST price is one interval old and must never become the newest
  point of the live series).  `detector.on_ticker` runs once per asset per
  round (detection is asset-wide) and only for assets some polled exchange does
  not stream (streamed assets are detected on the live stream).  A round yields
  to the event loop every ~200 tickers and never waits for REST confirmations.
  The app polls the watchlist from the first second and widens to watchlist +
  common universe once `resolve_universe` succeeds.
* `Pipeline.evaluate` when the target book is missing or older than
  `max_data_age_ms` (any source; a younger book is reused as is) and
  `confirm_with_rest_book`: `adapters[target].fetch_order_book(symbol)` in a
  bounded background task (single-flight, ≤ 2 per exchange, `rest_book_timeout_s`)
  → `state.on_book(book)` → the re-measured signal is evaluated on that fresh
  book.  No book → no opportunity.
* `MarketState.on_ticker(ticker, *, append_mid=True)` appends to the same mid
  series as books; appends are throttled per stream to one per
  `price_sample_min_interval_ms`.
* Universe: `registry.resolve_universe(settings, adapters) -> (ws_assets: list[str], poll_assets: list[str])`:
  ws = settings.watchlist (or auto top-N by volume on watchlist_exchange);
  poll = assets listed on ≥ 2 enabled exchanges with ≥ 1 EXECUTION venue,
  sorted by total volume, capped at `poll_assets_max`.

## 2.4 Learned "would a human have made it"
When an opportunity leaves the active set (EXPIRED or CLOSED):
`caught_up` = the target crossed in the signal's direction during the
opportunity's life (`MoveDetector` reports via `opps.mark_caught_up(asset, target)`
or the pipeline passes the latest target move; either way it ends up on the
Opportunity), `human_would_make_it = actual_duration_s >= settings.human_execution_time_s and caught_up`.
`StatsStore.record_human_outcome(asset, target, window_ok: bool, caught_up: bool)`
updates `human_window_total/ok` and `caught_up_total/ok`.
`assess_human` receives `stats: ReactionStats | None` (new optional kwarg) and
fills `stats_rate = stats.human_success_rate`, `stats_n = human_window_total`;
probability blends the margin-based estimate with `stats_rate` when
`stats_n >= 5`; the card shows "by stats: 7/10 made it".

## 2.5 Telegram & UI for manual trading
* `TelegramNotifier.should_alert`: additionally `human.light != RED` when
  `settings.alert_human_only` (default True).  Buttons: `OPEN ON <Exchange>`
  (`opp.pair_url`) and `OPEN OPPORTUNITY` (terminal deep link).  Prices and
  capital formatted with the market quote (€ for EUR, $ otherwise).
* UI card and MANUAL modal: "Open on Kraken ↗" link, quote-aware formatting,
  "HUMAN 🟢 7 s needed / ~12 s window · by stats 7/10" line.

---

# Phase 3 — selective, context-aware signals (narrow pool · quality score · context · reactions · leverage)

Owner's principle: **fewer trades, better analysis**.  The latency engine (Phases 1–2) still finds
the lag; Phase 3 decides *which assets are worth scanning* and *how good a found signal is*, using
as many objective factors as are available.  Rules that do not change: no fake data (a factor
without data is `available=False`, never support), NET only, strategy weights are fixed and
documented (measurements are learned, parameters are never auto-fitted).

```
universe/pool.py        AssetPool       which assets are scanned at all (objective, external data + measured jumpiness)
context/base.py         contracts       HttpGetter, PollingCollector, SourceStatus, Factor, MacroEvent, NewsItem
context/calendar.py     MacroCalendar   US macro calendar (CPI, PPI, NFP, FOMC, Fed speakers...) + BlsActuals
context/markets.py      TradFiMarkets   DXY, US yields, S&P/Nasdaq futures, VIX, gold (Yahoo chart API)
context/derivs.py       Derivatives     funding, open interest, liquidations, long/short (OKX public; Binance optional) + FearGreed
context/news.py         NewsFeed        RSS + public Telegram channels (t.me/s/<channel>) + Whale Alert parsing
context/classify.py     classifiers     RuleClassifier (always) + ClaudeClassifier (ANTHROPIC_API_KEY, budgeted)
context/bars.py         MinuteBars      per-asset minute closes from our own data (+OKX 1m bootstrap); regime, vol, corr
context/reactions.py    EventReactionStore  how BTC/ETH/asset moved +10 s / +60 s / +5 min / +30 min after each event type
context/hub.py          ContextHub      runs collectors, housekeeping, factors(asset, direction) -> list[Factor], snapshot()
engine/quality.py       assess_quality  transparent 0..100 score + grade + vetoes from components and context factors
engine/leverage.py      plan_leverage   position size / margin / fees / funding / liquidation / R:R; quality-gated leverage cap
```

Every new module reads settings with `getattr(settings, name, default)` using the defaults listed
here (the integration adds the same fields to `Settings`), takes `clock` (Clock/SimClock) and an
`HttpGetter` (tests inject a fake; production `AiohttpGetter`), never raises out of `PollingCollector.step()`,
and is fully testable offline with recorded fixtures.

## P3.1 `universe/pool.py` — `AssetPool(PollingCollector)` (name `"asset_pool"`)

```python
@dataclass(slots=True)
class PoolAsset:
    asset: str; rank: int | None; market_cap_usd: float | None; volume_24h_usd: float | None
    change_24h_pct: float | None; change_7d_pct: float | None; categories: list[str]
    venues_signal: list[str]; venues_target: list[str]
    jump: dict            # jumpiness() result (minutes, spikes, max_1m_pct, tail_ratio) or {}
    included: bool; reasons: list[str]   # Russian: why excluded / "в пуле: ..."
    source: str           # "coingecko" | "cache" | "fallback"

class AssetPool(PollingCollector):
    def __init__(self, settings, clock, http, *, cache_path: Path | None = None, api_key: str = "",
                 listings_fn: Callable[[], dict[str, dict[str, float | None]]] | None = None,
                 bars_fn: Callable[[str], list[float]] | None = None) -> None
    async def poll_once(self) -> int        # CoinGecko markets (top 250 by cap, 7d change) + category lists -> cache -> reevaluate()
    def reevaluate(self) -> None            # re-apply criteria with the latest listings_fn()/bars_fn() (no network)
    def contains(self, asset: str) -> bool  # O(1); True for every asset when pool_enabled is False
    def assets(self) -> list[str]           # included, by market-cap rank (pool_include first)
    def get(self, asset: str) -> PoolAsset | None
    def snapshot(self) -> dict              # enabled, updated_ts, source, criteria, included[], excluded[] (asset+reasons), status
def jumpiness(closes: list[float], spike_pct: float) -> dict  # {"minutes", "spikes", "max_1m_pct", "tail_ratio"}
```
`listings_fn()` → `asset -> {exchange: 24h quote volume | None}` for markets that exist on ENABLED
exchanges (built by the integration from adapters / MarketState).  Criteria (settings, defaults):
`pool_enabled=True`, `pool_max_assets=25`, `pool_min_market_cap_usd=1e9`, `pool_min_volume_24h_usd=2e7`
(global, CoinGecko), `pool_max_abs_change_24h_pct=15.0`, `pool_max_abs_change_7d_pct=40.0` (hype
pumps/dumps), `pool_exclude_categories=[meme / stablecoin / wrapped / liquid-staking / bridged /
tokenized-gold / fan-token category ids]`, `pool_include=["BTC","ETH"]`, `pool_exclude=[]`,
`pool_min_signal_venues=2`, `pool_min_target_venues=1`, `pool_spike_pct=2.0`, `pool_max_spikes=3`,
`pool_jump_min_minutes=120` (too few minutes → not judged, noted), `pool_refresh_h=12`.
Offline / 429: last cache file, else bundled `universe/data/fallback_pool.json`
(`{"pool": [...], "meme_blocklist": [...], "stable_blocklist": [...]}`).  Target-venue turnover is
still enforced per signal by `min_target_volume_quote` (default raised to **500 000**).

## P3.2 `context/calendar.py`

```python
class MacroCalendar(PollingCollector):     # name "macro_calendar", interval macro_refresh_s=3600
    def __init__(self, settings, clock, http, *, cache_path: Path | None = None) -> None
    async def poll_once(self) -> int        # Forex Factory weekly JSON (this week + next week when present); cache file survives 429 / restarts
    def events(self) -> list[MacroEvent]    # filtered: macro_countries=["USD"], macro_impacts=["high","medium"]; sorted by ts
    def upcoming(self, now_wall: float, within_s: float, min_impact: str = "high") -> list[MacroEvent]
    def recent(self, now_wall: float, within_s: float, min_impact: str = "high") -> list[MacroEvent]
    def next_event(self, now_wall: float, min_impact: str = "high") -> MacroEvent | None
    def set_actual(self, event_id: str, actual: str) -> None   # computes surprise vs forecast
def classify_title(title: str) -> str      # -> MACRO_EVENT_TYPES
def parse_number(text: str) -> float | None   # "0.3%" -> 0.3, "175K" -> 175000.0, "-1.2B" -> -1.2e9
def surprise_of(actual: str, forecast: str, tolerance: float = 0.0) -> int | None
class BlsActuals(PollingCollector):         # name "bls_actuals": fills actual values of released NFP/CPI/CORE_CPI/UNEMPLOYMENT/PPI/EARNINGS
    def __init__(self, settings, clock, http, calendar: MacroCalendar, *, api_key: str = "") -> None
```

## P3.3 `context/markets.py` — `TradFiMarkets(PollingCollector)` (name `"tradfi"`, `markets_poll_s=60`)
`markets_symbols={"DXY":"DX-Y.NYB","US10Y":"^TNX","US5Y":"^FVX","US3M":"^IRX","SPX_FUT":"ES=F","NDX_FUT":"NQ=F","VIX":"^VIX","GOLD":"GC=F"}`.
`quote(key) -> {"key","symbol","last","prev_close","chg_15m_pct","chg_1h_pct","chg_1d_pct","ts","market_open"} | None`;
`returns_1m(key, minutes) -> list[(minute_epoch, pct)]`; `snapshot()`.  Yields are in percent; their
change is reported in basis points (`chg_15m_bp` …) as well.

## P3.4 `context/derivs.py`
```python
class Derivatives(PollingCollector):       # name "derivatives", derivs_poll_s=60, OKX public v5 (Binance optional fallback)
    def __init__(self, settings, clock, http, *, assets_fn: Callable[[], list[str]]) -> None
    def get(self, asset: str) -> dict | None   # funding_rate_8h (fraction), next_funding_ts, oi_usd, oi_chg_15m_pct, oi_chg_1h_pct,
                                               # liq_long_usd_5m, liq_short_usd_5m, liq_long_usd_15m, liq_short_usd_15m, ls_ratio, source, ts
    def snapshot(self) -> dict
class FearGreed(PollingCollector):         # name "fear_greed", hourly: value 0..100, classification, ts
```

## P3.5 `context/news.py` + `context/classify.py`
```python
class NewsFeed(PollingCollector):          # name "news", news_poll_s=60
    def __init__(self, settings, clock, http, *, classifier: "HeadlineClassifier", db=None,
                 assets_fn: Callable[[], list[str]] = lambda: []) -> None
    sources: dict[str, SourceStatus]        # one per RSS feed / Telegram channel
    def items(self, since_wall: float | None = None, limit: int = 100) -> list[NewsItem]   # newest first
    def relevant(self, asset: str, now_wall: float, within_s: float, min_importance: float = 0.5) -> list[NewsItem]
    def snapshot(self) -> dict
def parse_telegram_page(html: str, channel: str) -> list[dict]   # id, ts, text, url
def parse_rss(xml: str, feed: str) -> list[dict]                 # id, ts, title, text, url
def parse_whale_alert(text: str) -> dict | None   # amount, asset, usd, from, to, flow: to_exchange|from_exchange|exchange_to_exchange|other
# classify.py
class RuleClassifier:   def classify(self, title: str, text: str, assets: list[str]) -> dict
class ClaudeClassifier: async def classify(self, title: str, text: str, assets: list[str]) -> dict | None  # JSON-only, budgeted, None on any failure
class HeadlineClassifier: async def classify(self, item: NewsItem, assets: list[str]) -> NewsItem   # rules always; Claude refines when enabled + key
```
Settings: `news_enabled=True`, `news_rss` (verified feeds), `news_telegram_channels` (verified public
channels), `news_keep=500`, `news_dedup_window_s=21600`, `news_claude_enabled=True` (only with
`ANTHROPIC_API_KEY`), `news_claude_max_per_hour=30`.  A classified item is a *factor input*, never a
BUY/SELL by itself.  Persistence: duck-typed `db.save_news(dict)` / `db.list_news(since_ts, limit)`.

## P3.6 `context/bars.py`
```python
class MinuteBars:
    def __init__(self, settings, clock, price_fn: Callable[[str], float | None], http=None, *, keep_minutes: int = 1440) -> None
    def track(self, assets: Iterable[str]) -> None
    def sample(self, now_wall: float | None = None) -> None      # every ~5 s: price_fn(asset) -> current minute close
    async def bootstrap(self, assets: Iterable[str], *, limit: int = 300) -> int   # OKX spot 1m candles {A}-USDT, paced
    def closes(self, asset: str, minutes: int | None = None) -> list[float]       # completed minutes, oldest -> newest
    def returns(self, asset: str, minutes: int) -> list[tuple[int, float]]        # (minute_epoch, pct)
    def stats(self, asset: str) -> dict   # minutes, ret_5m_pct, ret_15m_pct, ret_1h_pct, vol_1m_pct, vol_pctile, tail_1m_pct, shock
def correlation(a: list[tuple[int, float]], b: list[tuple[int, float]], min_n: int = 30) -> float | None
def beta(asset_r: list[tuple[int, float]], btc_r: list[tuple[int, float]], min_n: int = 30) -> float | None
```
`price_fn(asset)` = median of FRESH mids across signal venues (integration; MarketState); no fresh
price → the minute is not written (no gap filling).

## P3.7 `context/reactions.py` — `EventReactionStore`
```python
class EventReactionStore:
    def __init__(self, settings, clock, price_fn: Callable[[str], float | None], db=None) -> None
    def watch(self, event_key: str, event_type: str, event_ts: float, assets: list[str], surprise: int | None = None) -> bool
    def set_surprise(self, event_key: str, surprise: int | None) -> None
    def tick(self, now_wall: float | None = None) -> int          # capture due horizons (reaction_horizons_s=[10, 60, 300, 1800])
    def profile(self, event_type: str, asset: str, surprise: int | None = None) -> dict[int, dict]
        # horizon -> {"n", "mean_pct", "median_pct", "up_share", "mean_abs_pct"}
    def snapshot(self) -> dict
```
Base price = price at `event_ts` (captured by `tick` within `reaction_base_tolerance_s=5`; a missed
base or horizon is skipped, never interpolated).  Event types: macro `MACRO_EVENT_TYPES`; news
`"news:<category>"`.  Persistence: duck-typed `db.save_event_reaction(dict)` / `db.load_event_reactions(limit)`.

## P3.8 `context/hub.py` — `ContextHub`
```python
class ContextHub:
    def __init__(self, settings, clock, *, pool, calendar, news, markets, derivs, fear_greed, bars, reactions,
                 actuals=None, stats=None) -> None
    async def run(self, stop: asyncio.Event) -> None   # every collector's loop + housekeeping (5 s): bars.sample(),
        # reactions.tick(), watch() macro releases (BTC, ETH + top pool assets) and important news (importance >= 0.7),
        # pool.reevaluate() every 5 min, bars.bootstrap() for new pool assets
    def factors(self, asset: str, direction: int, now_wall: float | None = None) -> list[Factor]   # NO I/O, fast
    def asset_stats(self, asset: str) -> dict          # bars.stats(asset)
    def funding(self, asset: str) -> float | None
    def status(self) -> list[SourceStatus]
    def snapshot(self) -> dict                         # /api/context payload
```
Factors (key, default weight `context_weights[key]`, semantics; score is relative to direction):

| key | w | rule (all numbers in `detail`) |
|---|---|---|
| `macro_event` | 1.0 | high-impact release ahead within `macro_block_before_s=300` → **veto**; after release within `macro_block_after_s=120`: reaction profile (h=60 s, same surprise) with n ≥ `reaction_min_n=5` and up_share ≥ 0.65 / ≤ 0.35 → ±2·|up_share−0.5| toward/against direction, otherwise **veto** ("реакция непредсказуема"); medium impact in window → −0.3; calendar down → unavailable |
| `news` | 1.0 | Σ importance·confidence·direction·dir over relevant items in `news_window_s=1800`, age half-life 15 min, clipped; source down → unavailable; no relevant items → available, 0 |
| `whale` | 0.4 | Whale Alert net flow (to exchanges − from exchanges, USD) for the asset/BTC in 60 min: tanh(net/5e7) against longs |
| `funding` | 0.6 | f·dir > 0.0005 (crowded) → −min(1, f·dir/0.001); f·dir < −0.0005 → +0.5 (squeeze); else 0 |
| `open_interest` | 0.5 | OI 15 m change with price direction: in-direction & OI ↑ → +0.5; in-direction & OI ↓ → +0.2; against & OI ↑ → −0.5 |
| `liquidations` | 0.5 | 5 m liquidations of the opposite side vs same side: tanh(diff/scale), scale 1e6 BTC/ETH else 2e5 |
| `btc_regime` | 0.8 | alts: BTC 15 m return in direction → +min(1,|r|/0.5); against → −; BTC shock against → −1; vol percentile > 0.9 → −0.3. BTC itself: ETH as confirmation |
| `tradfi` | 0.5 | S&P futures 15 m move × corr(BTC, ES) and DXY move × −corr(BTC, DXY), only when the market is open and |corr| ≥ 0.2 |
| `sentiment` | 0.2 | Fear & Greed ≥ 80 → −0.3 for longs (+0.2 shorts); ≤ 20 mirrored |
| `event_reaction` | 0.8 | recent important news with a recorded reaction profile for `news:<category>` and this asset (n ≥ reaction_min_n) |

## P3.9 `engine/quality.py`
```python
@dataclass(slots=True)
class QualityComponent: key: str; label: str; value: float; weight: float; detail: str; available: bool
@dataclass(slots=True)
class QualityReport:
    score: int; grade: str; passed: bool; vetoes: list[str]; components: list[QualityComponent]
    factors: list[dict]; coverage: float; summary: str
    def to_dict(self) -> dict
def assess_quality(signal, econ, human, bot, stats, factors: list[Factor], settings, *, remaining_s: float) -> QualityReport
```
Components (values 0..1, weights `quality_weights` = edge 0.20, breadth 0.10, depth 0.10,
history 0.20, timing 0.10, context 0.30): **edge** NET vs total cost; **breadth** movers count and
their volume; **depth** executable vs capital and book depth; **history** catch-up rate of (asset,
target) shrunk to 0.5 with a Beta prior of strength `quality_history_k=10`; **timing** stability of
reaction delays and window vs human time; **context** `0.5 + 0.5·agg·coverage` where agg = Σw·s/Σw over
available factors and coverage = Σw(available)/Σw(all).  Score = 100·Σw·v/Σw.  Any veto → `passed=False`
and score capped below `min_quality_score`.  Grades A ≥ 80, B ≥ 65, C ≥ 50, D < 50.
`min_quality_score=60`: below it an opportunity is still created and shadow-traded, but hidden by
default in the UI, not sent to Telegram and not executable in AUTO.

## P3.10 `engine/leverage.py`
```python
@dataclass(slots=True)
class LeveragePlan:
    venue_kind: str; venue_max: float; quality_cap: float; max_by_signal: float; recommended: float
    binding: str; reasons: list[str]; margin_quote: float; notional_quote: float; fees_quote: float
    slippage_quote: float; funding_quote: float; borrow_quote: float; expected_profit_quote: float
    expected_roi_pct: float; stop_pct: float; stop_price: float; loss_at_stop_quote: float
    risk_pct_of_capital: float; liq_price: float | None; liq_distance_pct: float | None
    liq_buffer: float | None; rr: float | None; sigma_hold_pct: float | None; note: str
    def to_dict(self) -> dict
def quality_cap(score: float, settings) -> float
def plan_leverage(econ, quality_score: float, settings, venue, *, sigma_1m_pct: float | None, tail_1m_pct: float | None,
                  hold_s: float, funding_rate_8h: float | None, margin_quote: float | None = None) -> LeveragePlan
```
Caps (the smallest wins, `binding` names it): quality tiers `leverage_tiers=[[70,2],[80,3],[90,5]]`
(below 70 → 1×), global `max_leverage=5`; venue `ExchangeConfig.max_leverage` (1 = spot only,
`margin_kind` spot|margin|perp); liquidation buffer: isolated liquidation distance `1/L − mmr` ≥
`leverage_liq_buffer=3.0` × stop distance; risk: loss at stop ≤ `max_risk_per_trade_pct` of
`total_capital_quote`; depth: notional ≤ book depth within `max_slippage_pct`; move: L > 1 only when
NET ≥ `leverage_min_net_pct=0.4` and R:R ≥ `leverage_min_rr=1.5`.  Stop = max(`leverage_min_stop_pct=0.3`,
`leverage_stop_sigma=2.5`·σ_1m·√(hold/60), tail_1m).  Unknown volatility → 1× ("нет данных о
волатильности").  `max_by_signal` = all caps except the venue (what the signal would justify on a
margin venue); `recommended` includes the venue.  Funding (perp) = notional·rate·hold/8h; borrow
(margin) = borrowed·`borrow_rate_daily_pct`·hold/1d + `margin_open_fee_pct`.  Execution stays spot
(no leveraged orders are sent); the plan is shown to the manual trader and used for virtual P&L.

## P3.11 Integration (config, db, pipeline, app, api, ui, telegram)
* `Settings`: every field above; `ExchangeConfig`: `max_leverage=1.0`, `margin_kind="spot"`,
  `maintenance_margin_rate=0.005`, `borrow_rate_daily_pct=0.0`, `margin_open_fee_pct=0.0`;
  `Secrets`: optional `BLS_API_KEY`.  `min_target_volume_quote` default 500 000; `min_net_pct_show` stays 0.1.
* Universe: when `pool_enabled`, the WS watchlist = configured watchlist ∩ pool, free `ws_assets` slots
  filled with the next pool assets (subscriptions fixed until restart); the REST poll set = watchlist ∪ pool
  and follows the pool every 30 s (`app._follow_pool`, which also refreshes listings hourly); the mover
  ignores non-pool assets.  Stability not tried yet (`bars.stability_pending`) keeps an asset that is already IN
  the pool until measured (noted); a new candidate enters only measured; a failed try is "нет данных" → out; the hub retries failed/new candidates every 10 min.
* `Opportunity` gets `quality: dict` (QualityReport.to_dict) and `leverage: dict` (LeveragePlan.to_dict),
  computed in the pipeline enricher on every upsert/refresh (no I/O: hub.factors is in-memory).
* Shadow trading = training: every opportunity with NET ≥ `shadow_min_net_pct=0.1` is traded virtually
  **regardless of quality**; the shadow row stores `quality_score`, `grade` and the leveraged virtual
  P&L at `max_by_signal`.  STATS shows win rate / avg NET by grade (does the score predict outcomes?).
* Telegram alerts and AUTO require `quality.passed`; the alert card shows "Качество 78/100 (B) · плечо до 3×".
* API: `GET /api/context`, `GET /api/pool`; UI: new **Market** tab (macro calendar with countdowns and
  BTC reaction profiles, news feed with impact, TradFi, BTC/ETH regime, derivatives, sources status,
  pool with exclusion reasons); cards show the quality badge + factor list (WHY) + leverage plan;
  filter "show low quality"; settings for pool / quality / leverage / per-exchange leverage caps;
  DIAG counts `quality` rejections and pool size.

## P3.12 Research refinements (2026-10-08; reports in the session scratchpad `research/REPORT_*.md`) — these OVERRIDE P3.1–P3.11 where they differ

* **Pool** (`universe/pool.py`) runs four steps; missing input → "нет данных" and the asset stays OUT
  (except `pool_include`): (1) category exclusion by **CoinGecko id** (frozen offline id list in
  `universe/data/fallback_pool.json` + live refresh; never block ambiguous tickers like AI, B, M, LUNA by
  symbol); (2) venue presence: ≥ `pool_min_signal_venues=3` signal venues and ≥ `pool_min_target_venues=1`
  target; (3) target turnover stays per signal (`min_target_volume_quote=500000`, venue's own ticker);
  (4) **stability** from OKX `{A}-USDT` 1H candles over 30 days (`stability_fn(asset)` supplied by
  `context/bars.py`): hourly correlation with BTC ≥ `pool_min_btc_corr=0.55`, hourly residual jumps
  |r − β·r_BTC| ≥ `pool_jump_hour_pct=3.0` at most `pool_max_hour_jumps_30d=20`, max 7-day move ≤
  `pool_max_abs_change_7d_pct=100`, at least `pool_min_history_days=20` of candles.  Also excluded:
  funding interval < 8 h on the main perp venue (`funding_interval_fn`, exchanges move erratic coins to
  4 h/1 h).  Fallback pool tier A: BTC ETH XRP SOL BNB LINK ADA LTC AVAX SUI HBAR AAVE DOT BCH ETC RENDER;
  tier B: XLM UNI ALGO ATOM POL APT CRV TAO ONDO INJ.  Kraken symbol mapping XBT↔BTC, XDG↔DOGE.
* **Live erratic gate** (`context/bars.py` `erratic(asset)`): in the last 60 min ≥ 2 one-minute bars with
  |r| ≥ max(`erratic_min_1m_pct=1.0`, 8·median σ1m) **or** max |r5m| ≥ `erratic_max_5m_pct=3.0` →
  `{"suspended": True, "reason": ...}`; the hub emits factor `erratic` with **veto**.
* **Calendar**: prefer `ff_calendar_thisweek.xml` (UTC times, stable `ebaseId` in `<url>`, cp1252), JSON
  as fallback (explicit offsets).  Poll ≥ 30 min apart, keep the last good copy (`fetched_ts`, age shown),
  back off ≥ 300 s on 429 / HTML "Request Denied".  Map type by ebaseId first (CPI m/m 78, Core CPI 79,
  CPI y/y 884, PPI 86, Core PPI 87, NFP 66, Unemployment 56, AHE 159, Fed Funds Rate 1, FOMC Statement 4,
  Press Conference 512, Minutes 304, Fed Chair speaks 129) then exact title.  'All Day'/'Tentative' →
  time unknown (never midnight).  No zoneinfo dependency.  **Actual values**: no keyless seconds-level
  source exists; the surprise sign comes from squawk posts (`news.macro_surprise()` on FinancialJuice /
  WalterBloomberg text: "ACTUAL/FORECAST", "EST.", "SURVEY", "ожид") or later from BLS (`BlsActuals`,
  optional, BLS Atom feeds/API).  Until then `surprise=None`.  Event windows (−2…+60 min around high US
  events) raise the required quality and lower leverage; opportunities are tagged with the event.
* **TradFi**: Yahoo v8 is delayed (~10 min futures/DXY, ~15 min VIX/TNX): regime only, never a trigger;
  `data_age_s` per quote, UI label "задержка", `context_weights["tradfi"]=0.3`.
* **Derivatives**: OKX primary (one `funding-rate?instId=ANY`, one `open-interest?instType=SWAP` call
  covers all swaps); funding normalised per 8 h (`rate·8/interval_h`, interval from nextFundingTime −
  fundingTime) and `funding_interval_h` exposed; extremes judged as **percentiles of the asset's own
  history** (samples persisted: duck-typed `db.save_deriv_sample(dict)` / `db.load_deriv_samples(asset,
  since_ts)`); fewer than `derivs_min_samples=288` → funding factor unavailable ("история копится N/288").
  OI/price quadrant over 15 min (price ±0.5 %, OI ±2 %): NEW_LONGS / NEW_SHORTS / SHORT_COVERING /
  LONG_LIQUIDATION / QUIET.  Liquidations: OKX REST liquidation-orders best-effort (USD = sz·ctVal·bkPx;
  side=sell & posSide=long ⇒ long liquidated).  `EtfFlows` (daily, SoSoValue keyless, fragile → "нет данных"
  on failure) lives in `derivs.py` too.  Contrarian funding/LS rules are context, never entry triggers.
* **News**: default channels (verified): WatcherGuru, whale_alert_io, wublockchainenglish, cointelegraph,
  WalterBloomberg, FinancialJuice, markettwits, unfolded, BWEnews, binance_announcements,
  okxannouncements, Bybit_Announcements, kucoin_news.  Poll `t.me/s/<ch>?after=<max_id>`; fast squawks
  (FinancialJuice, WalterBloomberg, markettwits, WatcherGuru, whale_alert_io, wublockchainenglish) every
  20 s, others 60 s.  RSS defaults: cointelegraph, coindesk (no trailing slash), decrypt, theblock,
  bitcoinmagazine, Fed press_all + speeches, SEC press releases.  Dedup: sha1(normalised headline) or token
  Jaccard ≥ 0.5 within 6 h.  **Fake-news guard**: ETF approval/denial, exchange hack/insolvency and Fed
  decision headlines get importance > 0.85 only when corroborated (≥ 2 independent channels, or the
  official SEC/Fed feed / the exchange's own announcement channel).  Only pool assets or market-wide
  (`["*"]`) items feed the factor.  Claude only for items with rule importance ≥ 0.4, within the hourly
  budget, through the existing `anthropic` SDK (no new dependency).
* **Leverage**: `L_liq = 1/(k·stop + mmr + taker_fee)` with k = `leverage_liq_buffer=3`; **risk-first
  sizing** notional = equity·`leverage_risk_frac_pct=0.5`% / (stop + fees + slippage) — leverage raises
  margin efficiency only, never the per-trade risk budget; optional fractional-Kelly cap
  (`leverage_kelly_fraction=0.25`) from the meta-label lower bound p10 when n ≥ `metalabel_min_n=20`;
  funding only when the hold window crosses a funding timestamp; margin venues: opening fee + rollover
  per 4 h.  Venue caps are owner settings (`ExchangeConfig.max_leverage`, default 1 = spot).
* **Meta-label table** (new `engine/metalabel.py` — `MetaLabelStore`): measurement only.  Key =
  (asset, target, first_mover, lag bucket `<0.5|0.5-1|1-2|>2 %`, regime bucket from BTC vol percentile
  `low|mid|high`); every closed shadow trade records a triple-barrier outcome (target → win, stop → loss,
  time limit → win iff NET > 0).  Posterior Beta with κ = `metalabel_kappa=10` pseudo-trades around the
  global base rate; reports mean, 10th-percentile lower bound (stdlib regularized incomplete beta +
  bisection) and n, with back-off to coarser keys (asset+target → asset → global) when n is small.
  `engine/quality.py` uses p10 for the **history** component when n ≥ `metalabel_min_n`, otherwise the
  catch-up statistics.  Persistence: duck-typed `db.save_metalabel_outcome(dict)` /
  `db.load_metalabel_outcomes(limit)`.  Shadow trades get a stop barrier (`shadow_stop_pct=1.0` or the
  leverage plan's stop) so outcomes are three-way.
* **Quality**: context aggregate uses `S_eff = S·C` (missing data can never raise a score); every
  unavailable factor is listed in WHY; the minimum score rises inside macro event windows
  (`quality_event_window_bonus=10`) and with leverage (tiers).
