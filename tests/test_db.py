"""Offline tests for latbot.storage.db.Database (sqlite ':memory:')."""
from __future__ import annotations

import sqlite3
import threading

import pytest

from latbot.clock import SimClock
from latbot.models import (
    ExecAssessment,
    ExecutionEconomics,
    Fill,
    LagSignal,
    Light,
    Mode,
    Opportunity,
    OppState,
    Order,
    OrderStatus,
    OrderType,
    ReactionStats,
    Side,
    TradeRecord,
)
from latbot.storage.db import Database

DAY = 86400.0
# 1_700_000_000 = 2023-11-14T22:13:20Z
T0 = 1_700_000_000.0
DAY0 = "2023-11-14"
DAY1 = "2023-11-15"


def make_opp(opp_id: str = "opp_1", state: OppState = OppState.VALID, asset: str = "BTC",
             exchange: str = "bybit", net: float = 2.5, detected_ts: float = T0) -> Opportunity:
    sig = LagSignal(asset=asset, direction=1, first_mover="binance", first_move_pct=2.0, first_move_start_mono=1000.0,
                    reacted={"binance": 2.0}, target=exchange, target_symbol=f"{asset}/USDT", target_move_pct=0.0,
                    lag_pct=2.0, expected_catchup_pct=1.2, catchup_ratio=0.6, detected_mono=1000.0,
                    detected_ts=detected_ts, movers_count=2, target_quote="USDT")
    econ = ExecutionEconomics(direction=1, capital_quote=100.0, executable_quote=100.0, base_amount=1.0,
                              entry_best=100.0, entry_vwap=100.0, exit_best=99.9, exit_vwap_now=99.9,
                              exit_target=103.0, spread_pct=0.1, entry_fee_pct=0.1, exit_fee_pct=0.1,
                              entry_slippage_pct=0.0, exit_slippage_pct=0.0, execution_loss_pct=0.05,
                              gross_pct=net + 0.25, total_cost_pct=0.25, net_pct=net, net_quote=net, liquidity_ok=True)
    assess = ExecAssessment(light=Light.GREEN, label="HIGH", est_execution_s=1.0, margin_s=5.0, probability=0.8, reasons=["ok"])
    return Opportunity(id=opp_id, asset=asset, target_exchange=exchange, target_symbol=f"{asset}/USDT",
                       first_mover="binance", direction=1, mode=Mode.PAPER, state=state, signal=sig, econ=econ,
                       human=assess, bot=assess, why="why", detected_ts=detected_ts, first_seen_mono=1000.0,
                       last_valid_mono=1000.0, last_update_mono=1000.0, estimated_duration_s=8.0,
                       state_history=[(detected_ts, "DETECTED", "")], net_history=[(1000.0, net)])


def make_order(order_id: str = "o1", exchange: str = "bybit", status: OrderStatus = OrderStatus.FILLED) -> Order:
    return Order(id=order_id, client_id=f"c-{order_id}", exchange=exchange, symbol="BTC/USDT", side=Side.BUY,
                 type=OrderType.LIMIT, amount=1.0, price=100.0, status=status, filled=1.0, avg_price=100.0,
                 fills=[Fill(order_id=order_id, price=100.0, amount=1.0, fee_cost=0.1, fee_asset="USDT", ts=T0)],
                 fee_cost_quote=0.1, sent_ts=T0, sent_mono=1000.0, ack_mono=1000.1, raw={"exchange": "stuff"})


def make_trade(trade_id: str = "tr_1", mode: Mode = Mode.PAPER, state: OppState = OppState.CLOSED,
               pnl: float | None = 1.5, started_ts: float = T0, finished_ts: float | None = T0 + 10,
               with_orders: bool = True) -> TradeRecord:
    t = TradeRecord(id=trade_id, opportunity_id="opp_1", mode=mode, exchange="bybit", asset="BTC", symbol="BTC/USDT",
                    direction=1, capital_quote=100.0, expected_net_pct=2.5, expected_profit_quote=2.5,
                    expected_entry_price=100.0, expected_exit_price=103.0, state=state, started_ts=started_ts,
                    finished_ts=finished_ts, realized_pnl_quote=pnl, actual_net_pct=None if pnl is None else pnl,
                    execution_time_s=3.0, entry_latency_ms=120.0, events=[(started_ts, "start")])
    if with_orders:
        t.entry_order = make_order("e-" + trade_id)
        t.exit_order = make_order("x-" + trade_id)
        t.exit_order.side = Side.SELL
    return t


@pytest.fixture
def db() -> Database:
    d = Database(":memory:", clock=SimClock(wall=T0, mono=1000.0))
    yield d
    d.close()


# --------------------------------------------------------------------------- #
# schema / lifecycle
# --------------------------------------------------------------------------- #
def test_schema_created_in_memory(db: Database) -> None:
    names = {r["name"] for r in db._conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"opportunities", "trades", "orders", "stats", "kv"} <= names
    assert db.journal_mode.lower() in ("memory", "wal")
    assert db.closed is False


def test_file_database_creates_parent_and_uses_wal(tmp_path) -> None:
    path = tmp_path / "sub" / "dir" / "latbot.sqlite3"
    d = Database(str(path))
    try:
        assert path.exists()
        assert d.journal_mode.lower() == "wal"
        d.kv_set("a", 1)
        assert d.kv_get("a") == 1
    finally:
        d.close()
    # reopen: data persisted
    d2 = Database(str(path))
    try:
        assert d2.kv_get("a") == 1
    finally:
        d2.close()


def test_close_and_context_manager() -> None:
    with Database(":memory:") as d:
        d.kv_set("k", "v")
        assert d.kv_get("k") == "v"
    assert d.closed is True
    with pytest.raises(sqlite3.ProgrammingError):
        d.kv_get("k")
    d.close()  # idempotent


# --------------------------------------------------------------------------- #
# opportunities
# --------------------------------------------------------------------------- #
def test_opportunity_round_trip_and_upsert(db: Database) -> None:
    opp = make_opp()
    db.save_opportunity(opp)
    rows = db.list_opportunities()
    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == "opp_1"
    assert row["state"] == "VALID"  # enum -> value
    assert row["mode"] == "paper"
    assert row["human"]["light"] == "green"
    assert row["econ"]["net_pct"] == 2.5
    assert row["side"] == "LONG"
    assert row["state_history"] == [[T0, "DETECTED", ""]]
    opp.state = OppState.EXPIRED
    opp.expire_reason = "window closed"
    db.save_opportunity(opp)
    rows = db.list_opportunities()
    assert len(rows) == 1 and rows[0]["state"] == "EXPIRED" and rows[0]["expire_reason"] == "window closed"
    assert db.list_opportunities(state="EXPIRED")[0]["id"] == "opp_1"
    assert db.list_opportunities(state=OppState.VALID) == []


def test_list_opportunities_filters_and_order(db: Database) -> None:
    db.save_opportunity(make_opp("a", asset="BTC", exchange="bybit", detected_ts=T0))
    db.save_opportunity(make_opp("b", asset="ETH", exchange="kraken", detected_ts=T0 + 5, state=OppState.EXPIRED))
    db.save_opportunity(make_opp("c", asset="BTC", exchange="kraken", detected_ts=T0 + 10))
    assert [r["id"] for r in db.list_opportunities()] == ["c", "b", "a"]  # newest first
    assert [r["id"] for r in db.list_opportunities(asset="BTC")] == ["c", "a"]
    assert [r["id"] for r in db.list_opportunities(exchange="kraken")] == ["c", "b"]
    assert [r["id"] for r in db.list_opportunities(since_ts=T0 + 5)] == ["c", "b"]
    assert [r["id"] for r in db.list_opportunities(limit=1)] == ["c"]
    assert [r["id"] for r in db.list_opportunities(state="EXPIRED", asset="ETH")] == ["b"]


# --------------------------------------------------------------------------- #
# trades / orders
# --------------------------------------------------------------------------- #
def test_trade_round_trip(db: Database) -> None:
    t = make_trade()
    db.save_trade(t)
    rows = db.list_trades()
    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == "tr_1"
    assert row["mode"] == "paper" and row["state"] == "CLOSED" and row["side"] == "LONG"
    assert row["realized_pnl_quote"] == 1.5
    assert row["entry_order"]["status"] == "FILLED" and row["entry_order"]["side"] == "buy"
    assert "raw" not in row["entry_order"]
    assert row["exit_order"]["side"] == "sell"
    assert row["events"] == [[T0, "start"]]
    # upsert
    t.state = OppState.FAILED
    t.realized_pnl_quote = -0.3
    db.save_trade(t)
    rows = db.list_trades()
    assert len(rows) == 1 and rows[0]["state"] == "FAILED" and rows[0]["realized_pnl_quote"] == -0.3
    assert db.get_trade("tr_1")["state"] == "FAILED"
    assert db.get_trade("nope") is None


def test_list_trades_filters(db: Database) -> None:
    db.save_trade(make_trade("p1", mode=Mode.PAPER, started_ts=T0))
    db.save_trade(make_trade("l1", mode=Mode.LIVE, started_ts=T0 + 1))
    db.save_trade(make_trade("p2", mode=Mode.PAPER, started_ts=T0 + 2))
    assert [r["id"] for r in db.list_trades()] == ["p2", "l1", "p1"]
    assert [r["id"] for r in db.list_trades(mode="paper")] == ["p2", "p1"]
    assert [r["id"] for r in db.list_trades(mode=Mode.LIVE)] == ["l1"]
    assert [r["id"] for r in db.list_trades(since_ts=T0 + 1)] == ["p2", "l1"]
    assert [r["id"] for r in db.list_trades(limit=1)] == ["p2"]


def test_order_round_trip(db: Database) -> None:
    o = make_order("o1")
    db.save_order(o, "tr_1")
    db.save_order(make_order("o2", status=OrderStatus.CANCELLED), "tr_1")
    db.save_order(make_order("o3"), "tr_2")
    rows = db.list_orders("tr_1")
    assert {r["id"] for r in rows} == {"o1", "o2"}
    assert all("raw" not in r for r in rows)
    r1 = next(r for r in rows if r["id"] == "o1")
    assert r1["status"] == "FILLED" and r1["side"] == "buy" and r1["type"] == "limit"
    assert r1["fills"][0]["fee_asset"] == "USDT"
    o.status = OrderStatus.CANCELLED
    db.save_order(o, None)  # trade_id kept on upsert
    rows = db.list_orders("tr_1")
    assert len(rows) == 2 and next(r for r in rows if r["id"] == "o1")["status"] == "CANCELLED"
    assert len(db.list_orders()) == 3


# --------------------------------------------------------------------------- #
# stats / kv
# --------------------------------------------------------------------------- #
def test_stats_round_trip(db: Database) -> None:
    rs = ReactionStats(asset="BTC", exchange="bybit", reaction_delays_s=[1.0, 2.5], first_mover_count=3,
                       catchup_ratios=[0.5, 0.6, 0.7], success_count=2, fail_count=1, human_window_total=4, human_window_ok=3)
    db.save_stats(rs)
    db.save_stats(ReactionStats(asset="ETH", exchange="kraken"))
    rs.success_count = 5
    db.save_stats(rs)  # upsert
    loaded = db.load_stats()
    assert [(s.asset, s.exchange) for s in loaded] == [("BTC", "bybit"), ("ETH", "kraken")]
    assert loaded[0] == rs
    assert loaded[0].success_count == 5 and loaded[0].human_success_rate == 0.75


def test_kv(db: Database) -> None:
    assert db.kv_get("missing") is None
    assert db.kv_get("missing", 7) == 7
    db.kv_set("telegram_offset", 42)
    db.kv_set("cfg", {"a": [1, 2], "b": "x", "mode": Mode.LIVE})
    assert db.kv_get("telegram_offset") == 42
    assert db.kv_get("cfg") == {"a": [1, 2], "b": "x", "mode": "live"}
    db.kv_set("telegram_offset", 43)
    assert db.kv_get("telegram_offset") == 43
    db.kv_delete("telegram_offset")
    assert db.kv_get("telegram_offset") is None


# --------------------------------------------------------------------------- #
# aggregates
# --------------------------------------------------------------------------- #
def test_daily_pnl_by_utc_day_and_mode(db: Database) -> None:
    db.save_trade(make_trade("a", pnl=1.5, finished_ts=T0 + 10))  # DAY0 (22:13 UTC)
    db.save_trade(make_trade("b", pnl=-4.0, finished_ts=T0 + 20))  # DAY0
    db.save_trade(make_trade("c", pnl=10.0, finished_ts=T0 + 3 * 3600))  # DAY1 (01:13 UTC)
    db.save_trade(make_trade("d", mode=Mode.LIVE, pnl=100.0, finished_ts=T0 + 30))  # live
    db.save_trade(make_trade("e", state=OppState.EXECUTING, pnl=None, finished_ts=None))  # open
    db.save_trade(make_trade("f", state=OppState.CANCELLED, pnl=0.0, finished_ts=T0 + 40))
    assert db.daily_pnl(DAY0, "paper") == pytest.approx(-2.5)
    assert db.daily_pnl(DAY1, Mode.PAPER) == pytest.approx(10.0)
    assert db.daily_pnl(DAY0, "live") == pytest.approx(100.0)
    assert db.daily_pnl("2020-01-01", "paper") == 0.0


def test_counters_today(db: Database) -> None:
    db.save_opportunity(make_opp("o1", detected_ts=T0))
    db.save_opportunity(make_opp("o2", detected_ts=T0 + 1, state=OppState.EXPIRED))
    db.save_opportunity(make_opp("o3", detected_ts=T0 - 2 * DAY))  # two days ago
    db.save_trade(make_trade("t1", pnl=2.0, started_ts=T0, finished_ts=T0 + 5))
    db.save_trade(make_trade("t2", state=OppState.FAILED, pnl=-1.0, started_ts=T0 + 1, finished_ts=T0 + 6))
    db.save_trade(make_trade("t3", pnl=5.0, started_ts=T0 + 3 * 3600, finished_ts=T0 + 3 * 3600 + 5))  # tomorrow
    db.save_trade(make_trade("t4", mode=Mode.LIVE, pnl=50.0, started_ts=T0 + 2, finished_ts=T0 + 7))
    c = db.counters_today("paper")  # clock wall = T0 -> DAY0
    assert c["day"] == DAY0 and c["mode"] == "paper"
    assert c["opportunities_today"] == 2
    assert c["expired"] == 1
    assert c["trades"] == 2
    assert c["failed"] == 1
    assert c["closed"] == 1
    assert c["wins"] == 1
    assert c["realized_pnl"] == pytest.approx(1.0)
    assert c["avg_execution_time_s"] == pytest.approx(3.0)
    assert c["avg_latency_ms"] == pytest.approx(120.0)
    tomorrow = db.counters_today(Mode.PAPER, now_ts=T0 + 3 * 3600)
    assert tomorrow["day"] == DAY1 and tomorrow["trades"] == 1 and tomorrow["realized_pnl"] == pytest.approx(5.0)
    assert tomorrow["opportunities_today"] == 0
    live = db.counters_today("live")
    assert live["trades"] == 1 and live["realized_pnl"] == pytest.approx(50.0)
    empty = db.counters_today("paper", now_ts=T0 - 10 * DAY)
    assert empty["trades"] == 0 and empty["avg_execution_time_s"] is None


def test_thread_safety_smoke(db: Database) -> None:
    errors: list[BaseException] = []

    def worker(n: int) -> None:
        try:
            for i in range(50):
                db.kv_set(f"k{n}", i)
                db.save_trade(make_trade(f"tr_{n}_{i % 5}", with_orders=False))
                db.kv_get(f"k{n}")
                db.list_trades(limit=5)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert all(db.kv_get(f"k{n}") == 49 for n in range(8))
    assert len(db.list_trades(limit=1000)) == 40


def test_order_without_exchange_id_does_not_collide(db: Database) -> None:
    """REJECTED / timed-out orders have no exchange id yet; they are keyed by
    client id so the audit trail of each failed or unknown order survives."""
    rejected = Order(id="", client_id="c-1", exchange="bybit", symbol="BTC/USDT", side=Side.BUY, type=OrderType.LIMIT,
                     amount=1.0, price=100.0, status=OrderStatus.REJECTED, error="InvalidOrder: below min notional")
    unknown = Order(id="", client_id="c-2", exchange="bybit", symbol="BTC/USDT", side=Side.BUY, type=OrderType.LIMIT,
                    amount=1.0, price=100.0, status=OrderStatus.SENT,
                    error="timeout: status unknown, reconcile with fetch_order(client_id=c-2)")
    db.save_order(rejected, "tr_a")
    db.save_order(unknown, "tr_b")
    assert len(db.list_orders()) == 2
    rows_a = db.list_orders("tr_a")
    assert len(rows_a) == 1 and rows_a[0]["client_id"] == "c-1" and rows_a[0]["status"] == "REJECTED"
    assert "below min notional" in rows_a[0]["error"]
    # the unknown order is reconciled later: same client id, now with an exchange id -> same row, updated
    unknown.id = "ex-77"
    unknown.status = OrderStatus.FILLED
    unknown.error = None
    db.save_order(unknown, "tr_b")
    assert len(db.list_orders()) == 2
    rows_b = db.list_orders("tr_b")
    assert len(rows_b) == 1 and rows_b[0]["id"] == "ex-77" and rows_b[0]["status"] == "FILLED"
    assert db.list_orders("tr_a")[0]["status"] == "REJECTED"
