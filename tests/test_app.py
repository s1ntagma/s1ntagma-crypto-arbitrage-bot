"""End-to-end tests for latbot.app: the REAL wiring (build_context) driven by
FakeAdapters - no network, no exchange keys.

Scenario used everywhere: binance + kucoin (SIGNAL) jump +2 %, kraken
(EXECUTION, BTC/EUR) lags -> one LONG opportunity on kraken with net ~0.93 %
(lag 2 % x catch-up 0.6 = 1.2 % gross, minus 0.1 % + 0.1 % taker fees and
0.05 % execution loss), executable 100 EUR, human GREEN (30 s window), bot
GREEN.
"""
from __future__ import annotations

import asyncio
import json
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from latbot import events
from latbot.api.server import build_state
from latbot.app import AppContext, InstanceLock, build_context, main, run_app
from latbot.clock import SimClock
from latbot.config import ExchangeConfig, Secrets, Settings
from latbot.events import EventBus
from latbot.exchanges.fake import FakeAdapter
from latbot.exchanges.paper import PaperAdapter
from latbot.models import ExchangeRole, Light, Mode, Opportunity, OppState, OrderStatus
from latbot.storage.db import Database

REPO = Path(__file__).resolve().parent.parent
SYMBOLS = {"binance": "BTC/USDT", "kucoin": "BTC/USDT", "kraken": "BTC/EUR"}
SECRET_KEY_RE = re.compile(r"(?i)(token|secret|api_key|apikey|passphrase)")


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def make_settings(tmp_path: Path, **over: Any) -> Settings:
    base: dict[str, Any] = dict(
        db_path=":memory:",
        watchlist=["BTC"],
        ws_assets=1,
        exchanges=[
            ExchangeConfig(name="binance", role=ExchangeRole.SIGNAL, stream="bbo", label="Binance"),
            ExchangeConfig(name="kucoin", role=ExchangeRole.SIGNAL, stream="bbo", label="KuCoin"),
            ExchangeConfig(name="kraken", role=ExchangeRole.EXECUTION, taker_fee=0.001, maker_fee=0.001, label="Kraken"),
        ],
        min_mover_volume_quote=0.0,
        min_target_volume_quote=0.0,
        min_movers=2,
        ticker_poll_enabled=False,
        record_market_data=False,
        auto_confirm=False,
        default_opp_duration_s=30.0,
        max_risk_per_trade_pct=10.0,
        total_capital_quote=1000.0,
        default_api_latency_ms=20.0,  # paper latency is slept in real time
        buy_fill_timeout_s=2.0,
        sell_timeout_s=5.0,
        max_hold_s=10.0,
        port=0,
        host="127.0.0.1",
        log_dir=str(tmp_path / "logs"),
        ui_push_interval_ms=50,
    )
    base.update(over)
    return Settings(**base)


class Built:
    def __init__(self, ctx: AppContext, fakes: dict[str, FakeAdapter], clock: SimClock) -> None:
        self.ctx = ctx
        self.fakes = fakes
        self.clock = clock
        self.topics: list[tuple[str, Any]] = []
        ctx.bus.subscribe_all(lambda t, p: self.topics.append((t, p)))

    def published(self, topic: str) -> list[Any]:
        return [p for t, p in self.topics if t == topic]


def build(tmp_path: Path, **over: Any) -> Built:
    bus, clock = EventBus(), SimClock()
    fakes: dict[str, FakeAdapter] = {}

    def factory(cfg: ExchangeConfig, creds: Any) -> FakeAdapter:
        adapter = FakeAdapter(cfg, bus, clock, symbols={"BTC": SYMBOLS[cfg.name]})
        fakes[cfg.name] = adapter
        return adapter

    ctx = build_context(
        make_settings(tmp_path, **over),
        Secrets(),
        bus=bus,
        clock=clock,
        adapter_factory=factory,
        config_path=tmp_path / "config.json",
        stop_flag=tmp_path / "stop.flag",
    )
    ctx.stop_flag_poll_s = 0.05
    return Built(ctx, fakes, clock)


def levels(mid: float, size: float = 50.0) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    bids = [(mid * (1 - 0.0001 * k), size) for k in (1, 2, 3)]
    asks = [(mid * (1 + 0.0001 * k), size) for k in (1, 2, 3)]
    return bids, asks


async def push_all(fakes: dict[str, FakeAdapter], prices: dict[str, float]) -> None:
    for name, mid in prices.items():
        bids, asks = levels(mid)
        await fakes[name].push_book("BTC", bids, asks)


async def detect(b: Built) -> Opportunity:
    """Warm up every stream, then binance + kucoin jump +2 % while kraken lags."""
    await b.ctx.pipeline.start()
    await push_all(b.fakes, {"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
    b.clock.advance(0.5)
    await push_all(b.fakes, {"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
    b.clock.advance(1.0)
    await push_all(b.fakes, {"binance": 102.0, "kucoin": 102.0, "kraken": 100.0})
    active = b.ctx.opps.active()
    assert len(active) == 1, [o.to_dict() for o in active]
    return active[0]


async def wait_for(predicate, timeout: float = 5.0, step: float = 0.01) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(step)


@pytest.fixture
def keep_root_logging():
    """main() reconfigures the root logger; restore pytest's handlers afterwards."""
    import logging

    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield
    for h in list(root.handlers):
        if h not in handlers:
            root.removeHandler(h)
            try:
                h.close()
            except Exception:  # noqa: BLE001
                pass
    for h in handlers:
        if h not in root.handlers:
            root.addHandler(h)
    root.setLevel(level)


# --------------------------------------------------------------------------- #
# 1. wiring
# --------------------------------------------------------------------------- #
def test_build_context_wiring(tmp_path: Path) -> None:
    b = build(tmp_path)
    ctx = b.ctx
    assert ctx.settings.mode() is Mode.PAPER
    assert set(ctx.adapters) == {"binance", "kucoin", "kraken"}
    assert all(ctx.adapters[n] is b.fakes[n] for n in SYMBOLS)
    paper = ctx.exec_adapters[Mode.PAPER]
    assert set(paper) == {"kraken"}
    assert isinstance(paper["kraken"], PaperAdapter)
    assert paper["kraken"].name == "kraken" and paper["kraken"].inner is b.fakes["kraken"]
    assert ctx.exec_adapters[Mode.LIVE] == {}  # no credentials -> no LIVE adapters
    assert ctx.app is not None and hasattr(ctx.app, "router")
    assert ctx.notifier is not None and ctx.notifier.enabled is False  # inert without a token
    assert ctx.recorder is not None
    assert ctx.db.kv_get("x", 1) == 1
    assert ctx.config_path == tmp_path / "config.json"
    assert ctx.stop_flag == tmp_path / "stop.flag"
    assert ctx.execution.adapter_for is not None
    ctx.db.close()


# --------------------------------------------------------------------------- #
# 2. detection through the real wiring
# --------------------------------------------------------------------------- #
async def test_pipeline_detects_opportunity_through_real_wiring(tmp_path: Path) -> None:
    b = build(tmp_path)
    ctx = b.ctx
    try:
        opp = await detect(b)
        assert opp.target_exchange == "kraken" and opp.target_symbol == "BTC/EUR"
        assert opp.first_mover in {"binance", "kucoin"}
        assert opp.signal.movers_count == 2
        assert opp.direction == 1
        assert opp.state in (OppState.VALID, OppState.NOTIFIED)
        assert 0.5 < opp.econ.net_pct < 2.0
        assert opp.econ.liquidity_ok and opp.econ.executable_quote == pytest.approx(100.0)
        assert opp.econ.entry_vwap == pytest.approx(100.01, rel=1e-6)
        assert opp.econ.exit_target == pytest.approx(99.99 * 1.012, rel=1e-4)
        assert opp.bot.light is Light.GREEN, opp.bot.reasons
        assert opp.human.light is Light.GREEN, opp.human.reasons
        assert opp.quote == "EUR"
        assert "kraken" in opp.pair_url and "btc-eur" in opp.pair_url
        assert opp.why and len(opp.why.splitlines()) <= 6
        new = b.published(events.OPP_NEW)
        assert len(new) == 1 and new[0].id == opp.id
        assert b.published(events.SIGNAL)  # LagSignal published too
        state = build_state(ctx)
        assert state["mode"] == "paper"
        assert state["best"] is not None and state["best"]["id"] == opp.id
        assert {h["exchange"] for h in state["health"]} == set(SYMBOLS)
        # same books again -> update, not a second opportunity
        before = opp.update_count
        await push_all(b.fakes, {"binance": 102.0, "kucoin": 102.0, "kraken": 100.0})
        assert len(b.published(events.OPP_NEW)) == 1
        assert ctx.opps.get(opp.id).update_count > before
        assert len(ctx.opps.active()) == 1
    finally:
        await ctx.pipeline.stop()
        ctx.db.close()


# --------------------------------------------------------------------------- #
# 3. HTTP API on the built app (TestClient)
# --------------------------------------------------------------------------- #
def test_api_state_and_opportunities_with_testclient(tmp_path: Path) -> None:
    holder: dict[str, Any] = {}

    async def prepare() -> None:
        b = build(tmp_path)
        opp = await detect(b)
        await b.ctx.pipeline.stop()
        holder["b"], holder["opp"] = b, opp

    asyncio.run(prepare())
    b: Built = holder["b"]
    opp: Opportunity = holder["opp"]
    ctx = b.ctx
    try:
        with TestClient(ctx.app) as c:
            r = c.get("/api/state")
            assert r.status_code == 200
            state = r.json()
            assert state["mode"] == "paper" and state["emergency"] is False
            assert len(state["health"]) == 3
            assert state["best"]["id"] == opp.id
            assert not [k for k in state["settings"] if SECRET_KEY_RE.search(k)]
            assert "paper_mode" in state["settings"] and "human_execution_time_s" in state["settings"]

            r = c.get("/api/opportunities")
            assert r.status_code == 200
            items = r.json()
            assert len(items) == 1
            assert items[0]["id"] == opp.id and items[0]["target_exchange"] == "kraken" and items[0]["side"] == "LONG"
            assert items[0]["quote"] == "EUR" and items[0]["net_history"]

            assert c.get(f"/api/opportunities/{opp.id}").status_code == 200
            assert c.get("/api/opportunities/opp_nope").status_code == 404

            r = c.post(f"/api/opportunities/{opp.id}/manual")
            assert r.status_code == 200
            support = r.json()
            assert support["valid"] is True and support["status"] in ("ok", "too_fast")
            assert ctx.opps.get(opp.id).state is OppState.MANUAL_SELECTED

            r = c.get(f"/api/opportunities/{opp.id}/manual/support")
            assert r.status_code == 200
            payload = r.json()
            for key in ("opp_id", "valid", "status", "side", "exchange", "symbol", "entry_price_now", "exit_target_now",
                        "net_now_pct", "expected_slippage_pct", "remaining_s", "human", "capital", "base_amount",
                        "expired", "message"):
                assert key in payload, key
            assert payload["exchange"] == "kraken" and payload["symbol"] == "BTC/EUR"

            assert c.get("/api/trades").json() == []
            assert isinstance(c.get("/api/stats").json(), list)
            bal = c.get("/api/balances").json()
            assert bal["mode"] == "paper" and "kraken" in bal["balances"] and "EUR" in bal["balances"]["kraken"]

            r = c.get("/")
            assert r.status_code == 200 and "latbot" in r.text.lower()
            assert c.get("/static/app.js").status_code == 200

            with c.websocket_connect("/ws") as ws:
                msg = ws.receive_json()
                assert msg["topic"] == "snapshot"
                assert msg["data"]["state"]["mode"] == "paper"
                assert [o["id"] for o in msg["data"]["opportunities"]] == [opp.id]

            r = c.post(f"/api/opportunities/{opp.id}/cancel", json={"reason": "test"})
            assert r.status_code == 200 and r.json()["ok"] is True
            assert ctx.opps.get(opp.id).state is OppState.CANCELLED
            assert c.get("/api/opportunities").json() == []
            assert [o["id"] for o in c.get("/api/opportunities", params={"state": "history"}).json()] == [opp.id]
    finally:
        ctx.db.close()


# --------------------------------------------------------------------------- #
# 4. MANUAL flow -> CLOSED paper trade
# --------------------------------------------------------------------------- #
async def test_manual_flow_to_closed_paper_trade(tmp_path: Path) -> None:
    b = build(tmp_path)
    ctx = b.ctx
    try:
        opp = await detect(b)
        ex = ctx.execution
        support = await ex.select_manual(opp.id)
        assert support["valid"] is True and support["opp_id"] == opp.id
        assert ctx.opps.get(opp.id).state is OppState.MANUAL_SELECTED
        live = await ex.manual_support(opp.id)
        assert live["valid"] is True and live["status"] == "ok" and live["human"]["light"] == "green"
        assert live["entry_price_now"] == pytest.approx(100.01, rel=1e-6)

        entered = await ex.manual_entered(opp.id, price=100.01, quote_amount=100.0)
        assert entered["ok"] is True
        assert entered["exit_side"] == "SELL"
        assert entered["exit_target"] == pytest.approx(99.99 * 1.012, rel=1e-4)
        assert entered["base_amount"] == pytest.approx(100.0 / 100.01)
        assert ctx.opps.get(opp.id).state is OppState.EXECUTING
        assert (await ex.manual_support(opp.id))["status"] == "in_position"
        assert ctx.risk.open_trades() == 1

        trade = await ex.manual_exited(opp.id, price=101.5)
        assert trade.state is OppState.CLOSED and trade.mode is Mode.PAPER
        assert trade.actual_entry_price == pytest.approx(100.01) and trade.actual_exit_price == pytest.approx(101.5)
        assert trade.realized_pnl_quote > 1.0
        assert trade.actual_net_pct > 1.0
        assert trade.diff_reason
        assert ctx.opps.get(opp.id).state is OppState.CLOSED
        assert ex.trades()[0].id == trade.id
        rows = ctx.db.list_trades(mode="paper")
        assert len(rows) == 1 and rows[0]["id"] == trade.id and rows[0]["state"] == "CLOSED"
        assert ctx.risk.open_trades() == 0
        assert ctx.stats.get("BTC", "kraken").success_count == 1
        updates = b.published(events.TRADE_UPDATE)
        assert updates and updates[-1].state is OppState.CLOSED
    finally:
        await ctx.pipeline.stop()
        ctx.db.close()


# --------------------------------------------------------------------------- #
# 5. AUTO flow -> CLOSED paper trade (entry IOC, resting exit filled by catch-up)
# --------------------------------------------------------------------------- #
async def test_auto_flow_to_closed_paper_trade(tmp_path: Path) -> None:
    b = build(tmp_path)
    ctx = b.ctx
    try:
        opp = await detect(b)
        ex = ctx.execution
        ex.poll_interval_s = 0.01
        paper = ctx.exec_adapters[Mode.PAPER]["kraken"]
        eur_before = (await paper.fetch_balances())["EUR"].free

        sel = await ex.select_auto(opp.id)
        assert sel["ok"] is True and sel["confirmation_required"] is True
        snap = sel["snapshot"]
        assert snap["valid"] is True, snap["reason"]
        assert 0.5 < snap["net_now_pct"] < 2.0
        assert snap["risk"]["ok"] is True and snap["risk"]["allowed"] == pytest.approx(100.0)
        assert snap["balance"]["quote"] == "EUR" and snap["balance"]["free"] == pytest.approx(eur_before)
        assert snap["mode"] == "paper" and snap["quote"] == "EUR"
        assert ctx.opps.get(opp.id).state is OppState.AUTO_SELECTED

        task = asyncio.create_task(ex.confirm_auto(opp.id, wait=True))

        def entry_filled() -> bool:
            trades = ex.trades()
            return bool(trades and trades[0].entry_order is not None and trades[0].entry_order.filled > 0)

        await wait_for(entry_filled, timeout=5.0)
        # kraken catches up: bid 101.99 >= resting exit target ~101.19 -> GTC exit fills on the next poll
        await push_all(b.fakes, {"kraken": 102.0})
        res = await asyncio.wait_for(task, 10.0)
        assert res["ok"] is True, res
        assert res["state"] == "CLOSED"

        trade = ex.trades()[0]
        assert trade.id == res["trade_id"]
        assert trade.state is OppState.CLOSED and trade.mode is Mode.PAPER
        assert trade.entry_order is not None and trade.entry_order.status is OrderStatus.FILLED
        assert trade.exit_order is not None and trade.exit_order.filled == pytest.approx(trade.entry_order.filled)
        assert trade.entry_order.client_id != trade.exit_order.client_id
        assert trade.actual_entry_price == pytest.approx(100.01, rel=1e-6)
        assert trade.actual_exit_price == pytest.approx(101.99, rel=1e-4)
        assert trade.realized_pnl_quote > 1.0
        assert trade.actual_net_pct > 1.0
        assert trade.diff_reason.startswith("expected +")
        assert trade.execution_time_s is not None
        assert ctx.opps.get(opp.id).state is OppState.CLOSED
        assert ctx.opps.get(opp.id).execution_id == trade.id
        eur_after = (await paper.fetch_balances())["EUR"].free
        assert eur_after == pytest.approx(eur_before + trade.realized_pnl_quote, abs=1e-6)
        assert ctx.risk.open_trades() == 0 and ctx.risk.exposure() == 0.0
        assert ctx.stats.get("BTC", "kraken").success_count == 1
        rows = ctx.db.list_trades()
        assert len(rows) == 1 and rows[0]["realized_pnl_quote"] == pytest.approx(trade.realized_pnl_quote)
        assert ctx.db.list_orders(trade.id)
        states = [p["to"] for p in b.published(events.OPP_STATE) if p["opportunity"].id == opp.id]
        assert states[-1] in ("CLOSED", OppState.CLOSED)
    finally:
        await ctx.pipeline.stop()
        ctx.db.close()


# --------------------------------------------------------------------------- #
# 6. hard rule 4: revalidation cancels when the edge is gone
# --------------------------------------------------------------------------- #
async def test_revalidate_cancels_when_edge_is_gone(tmp_path: Path) -> None:
    b = build(tmp_path)
    ctx = b.ctx
    try:
        opp = await detect(b)
        ex = ctx.execution
        sel = await ex.select_auto(opp.id)
        assert sel["snapshot"]["valid"] is True
        # kraken catches up before the user confirms -> net collapses
        await push_all(b.fakes, {"kraken": 101.9})
        res = await ex.confirm_auto(opp.id, wait=True)
        assert res["ok"] is False
        assert "net" in res["reason"].lower()
        assert res["state"] == "CANCELLED"
        assert ctx.opps.get(opp.id).state is OppState.CANCELLED
        trades = ex.trades()
        assert len(trades) == 1 and trades[0].state is OppState.CANCELLED and trades[0].entry_order is None
        assert not ctx.exec_adapters[Mode.PAPER]["kraken"]._broker.orders  # nothing was sent
        assert ctx.risk.open_trades() == 0
    finally:
        await ctx.pipeline.stop()
        ctx.db.close()


# --------------------------------------------------------------------------- #
# 7. run_app: stop.flag + graceful shutdown (no server)
# --------------------------------------------------------------------------- #
async def test_run_app_stop_flag_and_shutdown(tmp_path: Path) -> None:
    b = build(tmp_path)
    ctx = b.ctx
    flag = Path(ctx.stop_flag)

    async def raise_flag() -> None:
        await asyncio.sleep(0.3)
        flag.write_text("stop", encoding="utf-8")

    asyncio.create_task(raise_flag())
    rc = await asyncio.wait_for(run_app(ctx, seconds=None, serve=False), 10.0)
    assert rc == 0
    assert ctx.stop_reason == "stop.flag"
    assert not flag.exists()
    assert ctx.watchlist == ["BTC"]
    assert b.fakes["kraken"].supported_symbols() == {"BTC/EUR"}  # start() got the watchlist
    assert b.fakes["binance"].supported_symbols() == {"BTC/USDT"}
    assert ctx.db.closed is True
    assert ctx.request_stop is None and ctx.server is None
    assert b.published(events.DASHBOARD)


async def test_run_app_seconds_keeps_running_without_a_dead_exchange(tmp_path: Path) -> None:
    """An adapter whose start() fails is retried in the background; the app runs on."""
    b = build(tmp_path)
    ctx = b.ctx
    calls: list[str] = []

    async def failing_start(symbols: Any) -> None:
        calls.append("x")
        raise ConnectionError("exchange unreachable")

    b.fakes["binance"].start = failing_start  # type: ignore[method-assign]
    rc = await asyncio.wait_for(run_app(ctx, seconds=0.4, serve=False), 10.0)
    assert rc == 0 and ctx.stop_reason == "seconds"
    assert calls == ["x"]  # first retry is 5 s away
    assert b.fakes["binance"].health.error_count == 1
    assert b.fakes["kraken"].supported_symbols() == {"BTC/EUR"}


# --------------------------------------------------------------------------- #
# 8. run_app serves the UI (uvicorn in the same loop, port 0)
# --------------------------------------------------------------------------- #
async def test_run_app_seconds_serves_ui(tmp_path: Path) -> None:
    b = build(tmp_path)
    ctx = b.ctx
    task = asyncio.create_task(run_app(ctx, seconds=2.5, serve=True))
    try:
        await wait_for(lambda: ctx.server_port is not None or task.done(), timeout=8.0, step=0.05)
        assert not task.done(), "run_app ended before the server came up"
        assert ctx.server_port and ctx.server_port > 0
        base = f"http://127.0.0.1:{ctx.server_port}"
        async with httpx.AsyncClient(base_url=base, timeout=5.0) as client:
            r = await client.get("/api/state")
            assert r.status_code == 200 and r.json()["mode"] == "paper"
            r = await client.get("/")
            assert r.status_code == 200 and "text/html" in r.headers["content-type"]
            r = await client.get("/api/health")
            assert r.status_code == 200 and r.json()["ok"] is True
        rc = await asyncio.wait_for(task, 15.0)
        assert rc == 0 and ctx.stop_reason == "seconds"
        assert ctx.server is None
        async with httpx.AsyncClient(timeout=1.0) as client:
            with pytest.raises(httpx.HTTPError):
                await client.get(f"{base}/api/state")  # server is down after shutdown
    finally:
        if not task.done():
            ctx.request_stop("test teardown") if ctx.request_stop else task.cancel()
            await asyncio.gather(task, return_exceptions=True)


# --------------------------------------------------------------------------- #
# 9. main(): lock, pid, config creation, exit codes
# --------------------------------------------------------------------------- #
def test_main_lock_pid_and_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, keep_root_logging: None) -> None:
    monkeypatch.chdir(tmp_path)
    for var in ("TELEGRAM_BOT_TOKEN", "MODE", "PORT", "LOG_LEVEL"):
        monkeypatch.delenv(var, raising=False)
    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps({"exchanges": [], "db_path": ":memory:", "port": 0, "log_dir": "logs"}), encoding="utf-8")

    rc = main(["0.3", "--no-server", "--env", str(tmp_path / "missing.env")])
    assert rc == 0
    assert (tmp_path / "logs" / "console.log").exists()
    assert not (tmp_path / "logs" / "bot.pid").exists()
    assert not (tmp_path / "logs" / "bot.lock").exists()
    assert json.loads(cfg.read_text(encoding="utf-8"))["port"] == 0  # untouched

    # second instance while the lock is held -> exit code 3, pid untouched
    held = InstanceLock(tmp_path / "logs" / "bot.lock")
    assert held.acquire() is True
    try:
        rc = main(["0.3", "--no-server", "--env", str(tmp_path / "missing.env")])
        assert rc == 3
        assert not (tmp_path / "logs" / "bot.pid").exists()
    finally:
        held.release()

    # missing config -> created with every Settings field; stale stop.flag removed
    cfg2 = tmp_path / "fresh" / "config.json"
    (tmp_path / "logs").mkdir(exist_ok=True)
    (tmp_path / "logs" / "stop.flag").write_text("stale", encoding="utf-8")
    monkeypatch.setenv("PORT", "0")
    rc = main(["0.2", "--no-server", "--config", str(cfg2), "--env", str(tmp_path / "missing.env")])
    assert rc == 0
    data = json.loads(cfg2.read_text(encoding="utf-8"))
    assert set(data) == {f for f in Settings.__dataclass_fields__}
    assert data["port"] == 0 and data["paper_mode"] is True
    assert not (tmp_path / "logs" / "stop.flag").exists()


def test_instance_lock_is_exclusive_across_handles(tmp_path: Path) -> None:
    a = InstanceLock(tmp_path / "bot.lock")
    b = InstanceLock(tmp_path / "bot.lock")
    assert a.acquire() is True
    assert b.acquire() is False
    a.release()
    assert b.acquire() is True
    b.release()
    assert not (tmp_path / "bot.lock").exists()


def test_main_stop_flag_stops_a_running_bot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, keep_root_logging: None) -> None:
    monkeypatch.chdir(tmp_path)
    for var in ("TELEGRAM_BOT_TOKEN", "MODE", "PORT", "LOG_LEVEL"):
        monkeypatch.delenv(var, raising=False)
    (tmp_path / "config.json").write_text(
        json.dumps({"exchanges": [], "db_path": ":memory:", "port": 0, "log_dir": "logs"}), encoding="utf-8"
    )
    flag = tmp_path / "logs" / "stop.flag"

    def raise_flag() -> None:
        deadline = time.monotonic() + 5.0
        while not (tmp_path / "logs" / "bot.pid").exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        time.sleep(0.3)
        flag.write_text("stop", encoding="utf-8")

    t = threading.Thread(target=raise_flag, daemon=True)
    t.start()
    rc = main(["--no-server", "--env", str(tmp_path / "missing.env")])
    t.join(5.0)
    assert rc == 0
    assert not flag.exists() and not (tmp_path / "logs" / "bot.pid").exists()


# --------------------------------------------------------------------------- #
# 10. batch files and entry points
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("name", ["start.bat", "stop.bat", "restart.bat"])
def test_bat_files_are_ascii_crlf(name: str) -> None:
    data = (REPO / name).read_bytes()
    data.decode("ascii")  # raises on non-ASCII
    assert data.startswith(b"@echo off\r\n")
    assert b"\r\n" in data
    assert data.count(b"\n") == data.count(b"\r\n"), "every LF must be part of a CRLF"
    assert data.endswith(b"\r\n")


def test_start_bat_loops_and_stop_bat_raises_the_flag() -> None:
    start = (REPO / "start.bat").read_text(encoding="ascii")
    assert "set PY=" in start and "%PY% -u run.py" in start  # a detected Python 3.11+, unbuffered
    assert ":loop" in start and "timeout /t 5" in start and "goto loop" in start
    assert 'if "%RC%"=="0" goto done' in start
    rc3 = re.search(r'if "%RC%"=="3" \((.*?)\)', start, re.S)
    assert rc3 is not None and "goto done" in rc3.group(1)  # another instance running: never loop
    stop = (REPO / "stop.bat").read_text(encoding="ascii")
    assert "logs\\stop.flag" in stop and "logs\\bot.pid" in stop
    restart = (REPO / "restart.bat").read_text(encoding="ascii")
    assert "call stop.bat" in restart and "call start.bat" in restart


def test_entry_points_exist() -> None:
    assert (REPO / "run.py").exists() and "from latbot.app import main" in (REPO / "run.py").read_text(encoding="utf-8")
    assert (REPO / "latbot" / "__main__.py").exists()
    assert sys.version_info >= (3, 11)


# --------------------------------------------------------------------------- #
# 11. allow_downsize produces a trade the free balance can actually pay for
# --------------------------------------------------------------------------- #
async def test_downsize_to_free_balance_produces_trade(tmp_path: Path) -> None:
    b = build(tmp_path, capital_quote=100.0, paper_balance_quote=95.0)
    ctx = b.ctx
    try:
        opp = await detect(b)
        ex = ctx.execution
        ex.poll_interval_s = 0.01
        sel = await ex.select_auto(opp.id)
        snap = sel["snapshot"]
        assert snap["valid"] is True, snap["reason"]
        assert "downsized" in snap["reason"]
        # the entry locks notional + taker fee (0.1 %) + limit markup (0.2 %): the size is carved out of the free 95 EUR
        assert snap["capital"] == pytest.approx(95.0 / 1.003, rel=1e-3) and snap["capital"] < 95.0
        task = asyncio.create_task(ex.confirm_auto(opp.id, wait=True))

        def entry_filled() -> bool:
            trades = ex.trades()
            return bool(trades and trades[0].entry_order is not None and trades[0].entry_order.filled > 0)

        await wait_for(entry_filled, timeout=5.0)
        await push_all(b.fakes, {"kraken": 102.0})
        res = await asyncio.wait_for(task, 10.0)
        assert res["ok"] is True, res
        trade = ex.trades()[0]
        assert trade.state is OppState.CLOSED and trade.entry_order.status is OrderStatus.FILLED
        assert trade.capital_quote == pytest.approx(95.0 / 1.003, rel=1e-3)
    finally:
        await ctx.pipeline.stop()
        ctx.db.close()


# --------------------------------------------------------------------------- #
# 12. stop.flag while an AUTO trade waits for its exit: the position is closed,
#     not abandoned (never left EXECUTING in the database)
# --------------------------------------------------------------------------- #
async def test_run_app_shutdown_closes_a_trade_in_flight(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "latbot.sqlite3"
    b = build(tmp_path, db_path=str(db_path))
    ctx = b.ctx
    opp = await detect(b)
    ex = ctx.execution
    ex.poll_interval_s = 0.01
    run = asyncio.create_task(run_app(ctx, seconds=None, serve=False))
    try:
        await wait_for(lambda: ctx.request_stop is not None, timeout=5.0)
        sel = await ex.select_auto(opp.id)
        assert sel["snapshot"]["valid"] is True, sel
        start = await ex.confirm_auto(opp.id, wait=False)
        assert start["ok"] is True, start
        await wait_for(lambda: opp.id in ex._pending_exit, timeout=5.0)
        Path(ctx.stop_flag).write_text("stop", encoding="utf-8")
        rc = await asyncio.wait_for(run, 20.0)
    finally:
        if not run.done():
            run.cancel()
            await asyncio.gather(run, return_exceptions=True)
    assert rc == 0 and ctx.stop_reason == "stop.flag"
    trade = ex.trades()[0]
    assert trade.state is OppState.CLOSED
    assert trade.exit_order is not None and trade.exit_order.filled == pytest.approx(trade.entry_order.filled)
    assert ctx.risk.open_trades() == 0 and ex.open_positions() == []
    paper = ctx.exec_adapters[Mode.PAPER]["kraken"]
    assert await paper.open_orders() == []
    db = Database(str(db_path))
    try:
        row = db.get_trade(trade.id)
        assert row is not None and row["state"] == "CLOSED"
    finally:
        db.close()


# --------------------------------------------------------------------------- #
# 10. execution-only adapters (LIVE / Bybit demo) do not open market streams of their own
# --------------------------------------------------------------------------- #
class StartRecorder(FakeAdapter):
    """FakeAdapter that accepts ``ws_assets`` like CcxtAdapter and records it."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.start_calls: list[tuple[list[str], int | None]] = []
        self.market_source: Any = None

    async def start(self, symbols: Any, ws_assets: int | None = None) -> None:
        self.start_calls.append((list(symbols), ws_assets))
        if ws_assets == 0:
            return  # execution-only: markets resolved, nothing streamed
        await super().start(symbols)


def build_with_creds(tmp_path: Path, secrets: Secrets, **over: Any) -> tuple[AppContext, dict[str, list[StartRecorder]]]:
    bus, clock = EventBus(), SimClock()
    made: dict[str, list[StartRecorder]] = {}

    def factory(cfg: ExchangeConfig, creds: Any) -> StartRecorder:
        adapter = StartRecorder(cfg, bus, clock, symbols={"BTC": SYMBOLS.get(cfg.name, "BTC/USDT")})
        made.setdefault(cfg.name, []).append(adapter)
        return adapter

    ctx = build_context(make_settings(tmp_path, **over), secrets, bus=bus, clock=clock, adapter_factory=factory,
                        config_path=tmp_path / "config.json", stop_flag=tmp_path / "stop.flag")
    ctx.stop_flag_poll_s = 0.05
    return ctx, made


def exchange_creds(name: str) -> Any:
    from latbot.config import ExchangeCredentials

    return ExchangeCredentials(name=name, api_key="key-" + "k" * 12, api_secret="sec-" + "s" * 12)


async def test_live_adapter_starts_execution_only_and_shares_the_market_adapter(tmp_path: Path) -> None:
    from latbot.app import _adapters_to_start

    ctx, made = build_with_creds(tmp_path, Secrets(exchanges={"kraken": exchange_creds("kraken")}))
    market, live = made["kraken"]
    assert ctx.adapters["kraken"] is market and ctx.exec_adapters[Mode.LIVE]["kraken"] is live
    assert live.market_source is market
    plan = _adapters_to_start(ctx)
    assert plan == [(ctx.adapters["binance"], None), (ctx.adapters["kucoin"], None), (market, None), (live, 0)]
    rc = await asyncio.wait_for(run_app(ctx, seconds=0.3, serve=False), 10.0)
    assert rc == 0
    assert market.start_calls == [(["BTC"], None)] and market.supported_symbols() == {"BTC/EUR"}
    assert live.start_calls == [(["BTC"], 0)] and live.supported_symbols() == set()


async def test_bybit_demo_paper_venue_starts_execution_only(tmp_path: Path) -> None:
    from latbot.app import _adapters_to_start

    exchanges = [
        ExchangeConfig(name="binance", role=ExchangeRole.SIGNAL, stream="bbo"),
        ExchangeConfig(name="kraken", role=ExchangeRole.EXECUTION),
        ExchangeConfig(name="bybit", role=ExchangeRole.EXECUTION, demo=True),
    ]
    ctx, made = build_with_creds(tmp_path, Secrets(exchanges={"bybit": exchange_creds("bybit")}),
                                 exchanges=exchanges, paper_engine="bybit_demo")
    market, demo = made["bybit"]
    assert ctx.exec_adapters[Mode.PAPER]["bybit"] is demo and not isinstance(demo, PaperAdapter)
    assert "bybit" not in ctx.exec_adapters[Mode.LIVE]
    assert demo.market_source is market
    assert (demo, 0) in _adapters_to_start(ctx) and (market, None) in _adapters_to_start(ctx)
    assert isinstance(ctx.exec_adapters[Mode.PAPER]["kraken"], PaperAdapter)
    assert all(not isinstance(a, PaperAdapter) for a, _ in _adapters_to_start(ctx))
    rc = await asyncio.wait_for(run_app(ctx, seconds=0.3, serve=False), 10.0)
    assert rc == 0
    assert demo.start_calls == [(["BTC"], 0)] and market.start_calls == [(["BTC"], None)]


async def test_plain_adapters_without_ws_assets_support_still_start(tmp_path: Path) -> None:
    """An adapter whose start() takes no ``ws_assets`` (FakeAdapter) is started the old way."""
    b = build(tmp_path)
    ctx = b.ctx
    rc = await asyncio.wait_for(run_app(ctx, seconds=0.2, serve=False), 10.0)
    assert rc == 0
    assert b.fakes["kraken"].supported_symbols() == {"BTC/EUR"}


# --------------------------------------------------------------------------- #
# Restart: a MANUAL position survives (restore_from_db) and its exit is still reportable
# --------------------------------------------------------------------------- #
def test_restart_restores_manual_position_and_its_exit_is_reportable(tmp_path: Path) -> None:
    """The owner bought by hand, the process died before the exit was reported.
    The next process restores the position (risk exposure included) although
    the opportunity itself is gone, and the manual API routes still let the
    owner report the exit through the real wiring - otherwise the position
    would sit in EXECUTING forever, holding a simultaneous-trade slot."""
    holder: dict[str, Any] = {}

    async def prepare() -> None:
        b = build(tmp_path)
        opp = await detect(b)
        await b.ctx.execution.select_manual(opp.id)
        entered = await b.ctx.execution.manual_entered(opp.id, price=100.01, quote_amount=100.0)
        assert entered["ok"] is True and b.ctx.risk.open_trades() == 1
        await b.ctx.pipeline.stop()  # "crash": no manual_exited, the database is reopened by the next process
        holder["db"], holder["opp"], holder["trade_id"] = b.ctx.db, opp, entered["trade_id"]

    asyncio.run(prepare())
    db: Database = holder["db"]
    opp: Opportunity = holder["opp"]
    trade_id: str = holder["trade_id"]
    try:
        bus, clock = EventBus(), SimClock()

        def factory(cfg: ExchangeConfig, creds: Any) -> FakeAdapter:
            return FakeAdapter(cfg, bus, clock, symbols={"BTC": SYMBOLS[cfg.name]})

        ctx2 = build_context(make_settings(tmp_path), Secrets(), bus=bus, clock=clock, db=db, adapter_factory=factory,
                             config_path=tmp_path / "config.json", stop_flag=tmp_path / "stop.flag")
        assert ctx2.opps.get(opp.id) is None  # the card is gone ...
        assert ctx2.risk.open_trades() == 1 and ctx2.risk.exposure(exchange="kraken") == pytest.approx(100.0)  # ... the position is not
        restored = ctx2.execution.active_trade(opp.id)
        assert restored is not None and restored.id == trade_id and restored.state is OppState.EXECUTING and restored.entry_order is None
        with TestClient(ctx2.app) as c:
            assert c.get(f"/api/opportunities/{opp.id}").status_code == 404
            trades = c.get("/api/trades").json()
            assert [t["id"] for t in trades] == [trade_id] and trades[0]["state"] == "EXECUTING" and trades[0]["entry_order"] is None
            r = c.post(f"/api/opportunities/{opp.id}/manual")
            assert r.status_code == 200, r.text
            d = r.json()
            assert d["status"] == "in_position" and d["valid"] is True and d["expired"] is False and d["trade_id"] == trade_id
            assert d["entry_price"] == pytest.approx(100.01) and d["quote_amount"] == 100.0 and d["exit_side"] == "SELL"
            assert d["exchange"] == "kraken" and d["symbol"] == "BTC/EUR" and d["quote"] == "EUR"
            assert d["entry_price_now"] is None and d["net_now_pct"] is None  # nothing live is invented
            assert c.get(f"/api/opportunities/{opp.id}/manual/support").json()["status"] == "in_position"
            assert c.post(f"/api/opportunities/{opp.id}/manual/entered", json={"price": 100.0, "quote_amount": 10.0}).status_code == 404
            r = c.post(f"/api/opportunities/{opp.id}/manual/exited", json={"price": 101.5})
            assert r.status_code == 200, r.text
            assert r.json()["id"] == trade_id and r.json()["state"] == "CLOSED" and r.json()["realized_pnl_quote"] > 1.0
            assert c.post(f"/api/opportunities/{opp.id}/manual").status_code == 404
        assert ctx2.risk.open_trades() == 0 and ctx2.execution.active_trade(opp.id) is None
        assert db.get_trade(trade_id)["state"] == "CLOSED"
    finally:
        db.close()
