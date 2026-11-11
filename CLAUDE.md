# latbot — rules for anyone (human or agent) touching this repo

Read `ARCHITECTURE.md` first: it is the contract between modules.

## What this is
Real-time cross-exchange **latency** terminal.  Other exchanges show that a move
started; the **target exchange** lags; BOTH legs (entry and exit) execute on the
target exchange.  No transfer arbitrage, no blockchain hops.  Human and bot
executability are evaluated separately (🔴🟡🟢); the user chooses MANUAL or AUTO.

## Hard rules (never weaken)
1. **No fake signals.**  No data → no opportunity.  Never fill gaps with guesses,
   never widen staleness/anomaly thresholds "to get more signals".  Every
   opportunity has a measurable WHY.
2. **NET only.**  Shown profit is after fees, spread, slippage, execution loss,
   on the size the book really supports.  Gross is never the headline.
3. **3 % is the Telegram threshold, not the opportunity threshold.**  The UI
   shows every real executable opportunity (0.3 %, 1 %, 7 %…).
4. **Never execute a stale snapshot.**  Revalidate on the latest book before
   the entry order and before the exit order.  Expected NET 4.8 %, current 1.2 %
   → cancel.
5. **Order integrity.**  Exactly one place sends orders per adapter
   (`place_order`).  Every order carries an idempotent `client_id`; a timeout is
   status `unknown`, not a retry — reconcile with `fetch_order`.  A trade is
   closed only when the exchange says the position is gone.
6. **Risk limits are hard.**  `RiskManager.check` runs before every order and
   cannot be bypassed by any flag.  Emergency stop blocks new AUTO trades,
   cancels pending orders, keeps history.
7. **Secrets.**  Keys/tokens live only in `.env`.  Never print, log, commit,
   return via API, or send to Telegram.  Logging passes through
   `logging_setup.RedactFilter`.  Demo-configured adapters refuse non-demo hosts.
   Never request withdrawal permissions.
8. **PAPER vs LIVE** are separate adapters and balances; the UI always shows
   which one is active.  Bybit Demo Trading (api-demo.bybit.com) is the paper
   venue carried over from the previous bot.
9. **Telegram is for important events only** (alerts ≥ threshold and
   executable, trade open/close, emergency, link loss/restore).  One alert per
   opportunity id; edit, don't repeat.  Only the owner chat is served.
10. **Learn, don't tune by hand.**  Reaction delays, catch-up ratios, durations,
    success rates are recorded per (asset, exchange) and fed back.  Strategy
    parameters are never auto-fitted to past trades.

## Engineering conventions
* Python ≥ 3.11, asyncio, type hints, `logging.getLogger(__name__)`.
* Percent values are percents (1.5 == 1.5 %); fee rates are fractions.
  `*_mono` for durations, `*_ts` for wall time.  `asset` = "BTC" is the
  cross-exchange key; `symbol` is the exchange market symbol.
* Tests: `python -m pytest -q` (offline: `FakeAdapter`, `SimClock`,
  `Database(":memory:")`), `python -m pyflakes latbot tests`.  TDD when fixing
  bugs: failing test first.  Never skip/disable a test to get green.
* Windows is the production host: paths via `pathlib`, UTF-8 everywhere,
  `.bat` files ASCII + CRLF, `WindowsSelectorEventLoopPolicy` in `main`.
* User-facing Russian (README, Telegram replies); code/comments English or
  Russian; Telegram alert card and UI labels follow the English spec format.
* Do not add dependencies beyond `requirements.txt` without a reason.
