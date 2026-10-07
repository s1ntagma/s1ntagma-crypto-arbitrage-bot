"""Offline tests for latbot.api.server (FastAPI routes + WebSocket hub).

Everything is stubbed locally: no engine modules are imported, no network.
"""
from __future__ import annotations

import asyncio
import json
import math
import re
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient

from latbot import VERSION
from latbot.api import build_snapshot, build_state, create_app
from latbot.api.server import WsHub, _jsonable, serialise_event
from latbot.clock import SimClock
from latbot.config import ExchangeCredentials, Secrets, Settings
from latbot.events import (
    BALANCE,
    DASHBOARD,
    EMERGENCY,
    HEALTH,
    OPP_NEW,
    OPP_STATE,
    OPP_UPDATE,
    SETTINGS,
    TRADE_UPDATE,
    EventBus,
)
from latbot.logging_setup import register_secret
from latbot.models import (
    Balance,
    ExchangeHealth,
    ExecAssessment,
    ExecutionEconomics,
    LagSignal,
    Light,
    Mode,
    Opportunity,
    OppState,
    TradeRecord,
)

TOKEN = "123456:SECRETTOKENxxxxxxxxxxxxxxxxxxxxxxxx"
API_KEY = "AKIASUPERSECRETKEY1234567890"
API_SECRET = "supersecretvalue7890abcdefXYZ"
SECRET_RE = re.compile("|".join(map(re.escape, (TOKEN, API_KEY, API_SECRET))))
# load_secrets() registers every secret value with the redactor in production;
# mimic that so redact() behaves the same way here.
for _value in (TOKEN, API_KEY, API_SECRET):
    register_secret(_value)

NOW = 1000.0


# --------------------------------------------------------------------------- #
# Stubs
# --------------------------------------------------------------------------- #
def make_opp(
    opp_id: str = "opp_1",
    asset: str = "BTC",
    target: str = "bybit",
    net: float = 4.82,
    human: Light = Light.YELLOW,
    bot: Light = Light.GREEN,
    state: OppState = OppState.VALID,
    direction: int = 1,
    mode: Mode = Mode.PAPER,
    est: float = 12.0,
    first_seen: float = NOW,
    nan_econ: bool = False,
) -> Opportunity:
    signal = LagSignal(
        asset=asset,
        direction=direction,
        first_mover="binance",
        first_move_pct=4.1,
        first_move_start_mono=first_seen - 1.8,
        reacted={"binance": 4.1, target: 1.2},
        target=target,
        target_symbol=f"{asset}/USDT",
        target_move_pct=1.2,
        lag_pct=2.9,
        expected_catchup_pct=1.74,
        catchup_ratio=0.6,
        detected_mono=first_seen,
        detected_ts=1_700_000_000.0,
        movers_count=2,
        median_reaction_delay_s=3.4,
        target_quote="USDT",
    )
    econ = ExecutionEconomics(
        direction=direction,
        capital_quote=100.0,
        executable_quote=100.0,
        base_amount=0.00098,
        entry_best=102_100.0,
        entry_vwap=102_140.0,
        exit_best=102_050.0,
        exit_vwap_now=102_000.0,
        exit_target=107_065.0,
        spread_pct=0.05,
        entry_fee_pct=0.1,
        exit_fee_pct=0.1,
        entry_slippage_pct=0.04,
        exit_slippage_pct=0.05,
        execution_loss_pct=0.05,
        gross_pct=net + 0.25,
        total_cost_pct=0.25,
        net_pct=net,
        net_quote=net,
        liquidity_ok=True,
        book_age_ms=math.nan if nan_econ else 120.0,
        bid_depth_quote=5000.0,
        ask_depth_quote=4000.0,
    )
    human_a = ExecAssessment(light=human, label="MEDIUM" if human is Light.YELLOW else "HIGH" if human is Light.GREEN else "NOT EXECUTABLE",
                             est_execution_s=7.0, margin_s=4.4, probability=0.5, reasons=["7 s needed"], stats_rate=0.7, stats_n=10)
    bot_a = ExecAssessment(light=bot, label="HIGH" if bot is Light.GREEN else "LOW" if bot is Light.YELLOW else "NOT EXECUTABLE",
                           est_execution_s=0.7, margin_s=10.7, probability=0.8, reasons=["est 0.7 s"])
    return Opportunity(
        id=opp_id,
        asset=asset,
        target_exchange=target,
        target_symbol=f"{asset}/USDT",
        first_mover="binance",
        direction=direction,
        mode=mode,
        state=state,
        signal=signal,
        econ=econ,
        human=human_a,
        bot=bot_a,
        why="Binance moved +4.1% 1.8 s ago. Bybit moved only +1.2% (lag +2.9%).",
        detected_ts=1_700_000_000.0,
        first_seen_mono=first_seen,
        last_valid_mono=first_seen,
        last_update_mono=first_seen,
        estimated_duration_s=est,
        quote="USDT",
        pair_url=f"https://www.bybit.com/trade/spot/{asset}/USDT",
        net_history=[(first_seen, net)],
        state_history=[(1_700_000_000.0, state.value, "test")],
    )


def make_trade(trade_id: str = "t_1", opp_id: str = "opp_1", state: OppState = OppState.CLOSED) -> TradeRecord:
    return TradeRecord(
        id=trade_id,
        opportunity_id=opp_id,
        mode=Mode.PAPER,
        exchange="bybit",
        asset="BTC",
        symbol="BTC/USDT",
        direction=1,
        capital_quote=100.0,
        expected_net_pct=4.82,
        expected_profit_quote=4.82,
        expected_entry_price=102_140.0,
        expected_exit_price=107_065.0,
        state=state,
        actual_entry_price=102_150.0,
        actual_exit_price=106_000.0,
        actual_net_pct=3.57,
        realized_pnl_quote=3.57,
        execution_time_s=8.2,
        diff_reason="slippage +0.3%, fees 0.2%",
        finished_ts=1_700_000_010.0,
    )


class StubOpps:
    def __init__(self, opps: list[Opportunity]) -> None:
        self._opps = {o.id: o for o in opps}

    def get(self, opp_id: str) -> Opportunity | None:
        return self._opps.get(opp_id)

    def active(self) -> list[Opportunity]:
        return sorted((o for o in self._opps.values() if o.is_active), key=lambda o: o.econ.net_pct, reverse=True)

    def executing(self) -> list[Opportunity]:
        return [o for o in self._opps.values() if o.state in (OppState.EXECUTING, OppState.PARTIALLY_FILLED, OppState.FILLED)]

    def history(self, limit: int = 200) -> list[Opportunity]:
        return [o for o in self._opps.values() if o.is_terminal][:limit]

    def best(self, now_mono: float) -> Opportunity | None:
        best = None
        for o in self.active():
            if o.human.light is Light.RED and o.bot.light is Light.RED:
                continue
            if best is None or o.econ.net_pct > best.econ.net_pct:
                best = o
        return best

    def counters(self, now_mono: float) -> dict[str, Any]:
        return {"active": len(self.active()), "today": len(self._opps), "green": 1, "yellow": 1, "red": 0}


class StubRisk:
    def __init__(self, bus: EventBus) -> None:
        self.bus = bus
        self.emergency = False
        self.emergency_reason = ""

    async def set_emergency(self, active: bool, reason: str = "") -> None:
        self.emergency = active
        self.emergency_reason = reason if active else ""
        await self.bus.publish(EMERGENCY, {"active": active, "reason": reason})


class StubExecution:
    def __init__(self, opps: StubOpps, risk: StubRisk, settings: Settings) -> None:
        self.opps = opps
        self.risk = risk
        self.settings = settings
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self._trades: list[TradeRecord] = [make_trade()]
        self.manual: dict[str, str] = {}  # opp_id -> trade_id of an open manual position

    def _opp(self, opp_id: str) -> Opportunity:
        opp = self.opps.get(opp_id)
        if opp is None:
            raise KeyError(opp_id)
        return opp

    async def select_manual(self, opp_id: str) -> dict[str, Any]:
        self.calls.append(("select_manual", (opp_id,), {}))
        opp = self._opp(opp_id)
        if opp_id not in self.manual:  # like the engine: an open position is reported, not re-selected
            opp.state = OppState.MANUAL_SELECTED
        return await self.manual_support(opp_id)

    async def manual_support(self, opp_id: str) -> dict[str, Any]:
        self.calls.append(("manual_support", (opp_id,), {}))
        opp = self._opp(opp_id)
        if opp_id in self.manual:
            return {
                "opp_id": opp_id,
                "valid": True,
                "status": "in_position",
                "side": "LONG",
                "exit_side": "SELL",
                "exchange": opp.target_exchange,
                "symbol": opp.target_symbol,
                "entry_price_now": 102_140.0,
                "exit_target_now": 107_065.0,
                "net_now_pct": 4.7,
                "expected_slippage_pct": 0.04,
                "remaining_s": 2.0,
                "human": {"light": "yellow", "label": "MEDIUM", "est_execution_s": 7.0},
                "capital": 100.0,
                "base_amount": 100.0 / 102_150.0,
                "expired": False,
                "message": "🟢 IN POSITION - report your exit price",
                "trade_id": self.manual[opp_id],
                "entry_price": 102_150.0,
                "quote_amount": 100.0,
            }
        return {
            "opp_id": opp_id,
            "valid": opp.is_active,
            "status": "ok",
            "side": "LONG",
            "exchange": opp.target_exchange,
            "symbol": opp.target_symbol,
            "entry_price_now": 102_140.0,
            "exit_target_now": 107_065.0,
            "net_now_pct": 4.7,
            "expected_slippage_pct": 0.04,
            "remaining_s": 8.2,
            "human": {"light": "yellow", "label": "MEDIUM", "est_execution_s": 7.0},
            "capital": 100.0,
            "base_amount": 0.00098,
            "expired": not opp.is_active,
            "message": "" if opp.is_active else "⚠️ OPPORTUNITY EXPIRED",
        }

    async def manual_entered(self, opp_id: str, price: float, quote_amount: float) -> dict[str, Any]:
        self.calls.append(("manual_entered", (opp_id, price, quote_amount), {}))
        if price <= 0:
            raise ValueError("price must be > 0")
        opp = self._opp(opp_id)
        opp.state = OppState.EXECUTING
        self.manual[opp_id] = "t_manual"
        return {"ok": True, "opp_id": opp_id, "trade_id": "t_manual", "exit_target": 107_065.0, "exit_side": "sell", "base_amount": quote_amount / price, "estimated_fees_quote": 0.2}

    async def manual_exited(self, opp_id: str, price: float) -> TradeRecord:
        self.calls.append(("manual_exited", (opp_id, price), {}))
        opp = self._opp(opp_id)
        self.manual.pop(opp_id, None)
        opp.state = OppState.CLOSED
        trade = make_trade("t_manual", opp_id)
        trade.actual_exit_price = price
        self._trades.insert(0, trade)
        return trade

    async def select_auto(self, opp_id: str, wait: bool = True) -> dict[str, Any]:
        self.calls.append(("select_auto", (opp_id,), {"wait": wait}))
        opp = self._opp(opp_id)
        opp.state = OppState.AUTO_SELECTED
        if self.settings.auto_confirm:
            return await self.confirm_auto(opp_id, wait=wait)
        return {
            "confirmation_required": True,
            "snapshot": {
                "net_now_pct": 4.7,
                "expected_net_pct": 4.82,
                "expected_profit_quote": 4.82,
                "capital": 100.0,
                "risk": {"ok": True, "reason": "", "allowed": 100.0},
                "balance": {"quote": "USDT", "free": 10_000.0},
                "valid": True,
                "reason": "",
                "side": "LONG",
                "exchange": opp.target_exchange,
                "symbol": opp.target_symbol,
                "entry_price": 102_140.0,
                "exit_target": 107_065.0,
                "remaining_s": 9.1,
            },
        }

    async def confirm_auto(self, opp_id: str, wait: bool = True) -> dict[str, Any]:
        self.calls.append(("confirm_auto", (opp_id,), {"wait": wait}))
        opp = self._opp(opp_id)
        opp.state = OppState.EXECUTING
        trade = make_trade("t_auto", opp_id, OppState.EXECUTING)
        return {"ok": True, "started": not wait, "state": "EXECUTING", "reason": "", "trade": trade}

    async def deselect_auto(self, opp_id: str, reason: str = "user backed out of AUTO confirmation") -> dict[str, Any]:
        self.calls.append(("deselect_auto", (opp_id,), {}))
        opp = self._opp(opp_id)
        if opp.state is not OppState.AUTO_SELECTED:
            return {"ok": False, "opp_id": opp_id, "state": opp.state.value, "reason": f"nothing to deselect in state {opp.state.value}"}
        opp.state = OppState.VALID
        return {"ok": True, "opp_id": opp_id, "state": "VALID", "reason": reason}

    async def cancel(self, opp_id: str, reason: str = "user") -> dict[str, Any]:
        self.calls.append(("cancel", (opp_id,), {"reason": reason}))
        opp = self._opp(opp_id)
        opp.state = OppState.CANCELLED
        return {"ok": True, "state": "CANCELLED", "reason": reason}

    async def emergency_stop(self, reason: str = "user") -> dict[str, Any]:
        self.calls.append(("emergency_stop", (reason,), {}))
        await self.risk.set_emergency(True, reason)
        return {"ok": True, "emergency": True, "reason": reason, "cancelled_orders": 0, "aborted": [], "open_positions": []}

    async def emergency_close_positions(self) -> list[TradeRecord]:
        self.calls.append(("emergency_close_positions", (), {}))
        return [make_trade("t_closed", "opp_x")]

    async def resume(self) -> None:
        self.calls.append(("resume", (), {}))
        await self.risk.set_emergency(False, "")

    def trades(self, limit: int = 200) -> list[TradeRecord]:
        return self._trades[:limit]


class StubPipeline:
    def __init__(self, opps: StubOpps, settings: Settings) -> None:
        self.opps = opps
        self.settings = settings

    def dashboard(self) -> dict[str, Any]:
        d = self.opps.counters(NOW)
        d["mode"] = self.settings.mode().value
        d["avg_latency_ms"] = 180.0
        d["success_rate"] = 0.75
        return d


class StubStats:
    def summary(self) -> list[dict[str, Any]]:
        return [
            {"asset": "BTC", "exchange": "bybit", "first_mover_frequency": 0.2, "median_reaction_delay_s": 3.4, "catchup_ratio": 0.6, "trades": 4, "success_rate": 0.75},
            {"asset": "ETH", "exchange": "kraken", "first_mover_frequency": None, "median_reaction_delay_s": None, "catchup_ratio": None, "trades": 0, "success_rate": None},
        ]


class StubAdapter:
    def __init__(self, name: str, connected: bool = True, fail: bool = False) -> None:
        self.name = name
        self.health = ExchangeHealth(exchange=name, ws_connected=connected, last_msg_mono=NOW, ws_latency_ms=120.0, order_latency_ms=250.0)
        self.fail = fail

    async def fetch_balances(self) -> dict[str, Balance]:
        if self.fail:
            raise RuntimeError(f"auth failed for key {API_KEY}")
        return {"USDT": Balance(asset="USDT", free=9_500.0, total=10_000.0), "BTC": Balance(asset="BTC", free=0.01, total=0.01)}


class StubDb:
    def list_trades(self, limit: int = 200, mode: str | None = None, since_ts: float | None = None) -> list[dict[str, Any]]:
        return [make_trade("t_db", "opp_old").to_dict(), make_trade("t_1", "opp_1").to_dict()][:limit]


def make_ctx(tmp_path: Any, *, with_execution: bool = True, opps: list[Opportunity] | None = None) -> SimpleNamespace:
    settings = Settings()
    bus = EventBus()
    clock = SimClock(mono=NOW)
    if opps is None:
        opps = [
            make_opp("opp_1", "BTC", "bybit", 4.82, Light.YELLOW, Light.GREEN),
            make_opp("opp_2", "ETH", "kraken", 1.2, Light.RED, Light.GREEN, direction=1),
            make_opp("opp_3", "SOL", "bybit", 0.8, Light.GREEN, Light.RED, state=OppState.EXECUTING),
            make_opp("opp_4", "BTC", "kraken", 2.1, Light.GREEN, Light.GREEN, state=OppState.EXPIRED),
        ]
    stub_opps = StubOpps(opps)
    risk = StubRisk(bus)
    execution = StubExecution(stub_opps, risk, settings) if with_execution else None
    adapters = {"bybit": StubAdapter("bybit"), "kraken": StubAdapter("kraken", connected=False), "binance": StubAdapter("binance")}
    exec_adapters = {Mode.PAPER: {"bybit": StubAdapter("bybit"), "kraken": StubAdapter("kraken", fail=True)}, Mode.LIVE: {}}
    secrets = Secrets(telegram_bot_token=TOKEN, exchanges={"bybit": ExchangeCredentials(name="bybit", api_key=API_KEY, api_secret=API_SECRET)})
    return SimpleNamespace(
        settings=settings,
        secrets=secrets,
        bus=bus,
        clock=clock,
        db=StubDb(),
        state=None,
        stats=StubStats(),
        opps=stub_opps,
        pipeline=StubPipeline(stub_opps, settings),
        adapters=adapters,
        exec_adapters=exec_adapters,
        risk=risk,
        execution=execution,
        notifier=None,
        config_path=tmp_path / "config.json",
    )


@pytest.fixture
def ctx(tmp_path):
    return make_ctx(tmp_path)


@pytest.fixture
def client(ctx):
    with TestClient(create_app(ctx)) as c:
        yield c


# --------------------------------------------------------------------------- #
# Static UI
# --------------------------------------------------------------------------- #
def test_index_and_static_files(client):
    r = client.get("/")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    assert "latbot" in r.text
    assert "/static/app.js" in r.text and "/static/style.css" in r.text
    js = client.get("/static/app.js")
    assert js.status_code == 200 and "WebSocket" in js.text and "manual/support" in js.text
    css = client.get("/static/style.css")
    assert css.status_code == 200 and "--bg" in css.text
    assert client.get("/favicon.ico").status_code == 204


def test_ui_contains_spec_elements(client):
    html = client.get("/").text
    for needle in ("EMERGENCY STOP", "BEST CURRENT OPPORTUNITY", "PAPER", "settings-drawer", "modal-manual", "modal-auto",
                   'data-quick="MANUAL"', 'data-quick="AUTO"', 'data-quick="EXPIRED"', "f-exchange", "f-human", "f-bot"):
        assert needle in html, needle
    js = client.get("/static/app.js").text
    for needle in ("TOO FAST FOR HUMAN", "OPPORTUNITY EXPIRED", "CURRENT NET", "EXPECTED PROFIT", "#opp\\/", "hashchange", "300", "textContent",
                   # SELL step of an open manual position is never "too fast"; it can be resumed after a reload
                   "IN POSITION", "WINDOW CLOSED", "in_position", "REPORT EXIT",
                   # backing out of the AUTO confirmation is not a cancel
                   "auto/deselect",
                   # unchecked risk / balance is grey, never green
                   "risk.ok === true", "not checked"):
        assert needle in js, needle
    assert "risk.ok === false ? '🔴 ' + (risk.reason || 'blocked') : '🟢 OK'" not in js  # anything-not-false-is-green
    assert ".innerHTML" not in js and "insertAdjacentHTML" not in js  # all data reaches the DOM via textContent


# --------------------------------------------------------------------------- #
# State / health
# --------------------------------------------------------------------------- #
def test_state(client):
    r = client.get("/api/state")
    assert r.status_code == 200
    d = r.json()
    assert set(d) >= {"mode", "emergency", "settings", "health", "dashboard", "best", "version"}
    assert d["mode"] == "paper"
    assert d["emergency"] is False
    assert d["version"] == VERSION
    assert d["best"]["id"] == "opp_1"
    assert d["best"]["side"] == "LONG"
    assert d["best"]["remaining_s"] == 12.0
    assert d["dashboard"]["active"] == 2
    names = {hh["exchange"] for hh in d["health"]}
    assert names == {"bybit", "kraken", "binance"}
    by = {hh["exchange"]: hh for hh in d["health"]}
    assert by["bybit"]["usable"] is True and by["kraken"]["usable"] is False
    assert by["bybit"]["label"] == "Bybit EU" and by["bybit"]["role"] == "execution"
    assert d["settings"]["mode"] == "paper"


def test_health(client):
    d = client.get("/api/health").json()
    assert d["ok"] is True and d["mode"] == "paper" and d["ws_clients"] == 0
    assert len(d["exchanges"]) == 3


def test_build_state_without_optional_members(tmp_path):
    ctx = SimpleNamespace(settings=Settings(), bus=EventBus())
    d = build_state(ctx)
    assert d["mode"] == "paper" and d["best"] is None and d["dashboard"] == {}
    assert {hh["exchange"] for hh in d["health"]} == set(Settings().enabled_exchanges())
    snap = build_snapshot(ctx)
    assert snap["opportunities"] == []


# --------------------------------------------------------------------------- #
# Opportunities
# --------------------------------------------------------------------------- #
def test_opportunities_default_active(client):
    d = client.get("/api/opportunities").json()
    assert [o["id"] for o in d] == ["opp_1", "opp_2"]
    assert d[0]["buy_price"] == 102_140.0 and d[0]["sell_price"] == 107_065.0
    assert d[0]["human"]["light"] == "yellow" and d[0]["bot"]["light"] == "green"


def test_opportunities_state_filters(client):
    assert [o["id"] for o in client.get("/api/opportunities?state=executing").json()] == ["opp_3"]
    assert [o["id"] for o in client.get("/api/opportunities?state=history").json()] == ["opp_4"]
    ids = {o["id"] for o in client.get("/api/opportunities?state=all").json()}
    assert ids == {"opp_1", "opp_2", "opp_3", "opp_4"}
    r = client.get("/api/opportunities?state=bogus")
    assert r.status_code == 400 and "state" in r.json()["error"]


def test_opportunities_field_filters(client):
    assert {o["id"] for o in client.get("/api/opportunities?asset=btc").json()} == {"opp_1"}
    assert {o["id"] for o in client.get("/api/opportunities?exchange=Kraken").json()} == {"opp_2"}
    assert {o["id"] for o in client.get("/api/opportunities?min_net=3").json()} == {"opp_1"}
    assert {o["id"] for o in client.get("/api/opportunities?human=red").json()} == {"opp_2"}
    assert {o["id"] for o in client.get("/api/opportunities?bot=green").json()} == {"opp_1", "opp_2"}
    assert {o["id"] for o in client.get("/api/opportunities?min_window=20").json()} == set()
    assert len(client.get("/api/opportunities?limit=1").json()) == 1
    r = client.get("/api/opportunities?min_net=abc")
    assert r.status_code == 400 and "error" in r.json()
    r = client.get("/api/opportunities?human=blue")
    assert r.status_code == 400 and "human" in r.json()["error"]


def test_opportunity_by_id_and_404(client):
    r = client.get("/api/opportunities/opp_1")
    assert r.status_code == 200 and r.json()["id"] == "opp_1"
    r = client.get("/api/opportunities/nope")
    assert r.status_code == 404 and r.json() == {"error": "opportunity not found"}
    for path in ("/api/opportunities/nope/manual", "/api/opportunities/nope/auto", "/api/opportunities/nope/cancel"):
        r = client.post(path)
        assert r.status_code == 404 and r.json()["error"] == "opportunity not found", path
    r = client.get("/api/opportunities/nope/manual/support")
    assert r.status_code == 404


def test_unknown_route_is_json(client):
    r = client.get("/api/nope")
    assert r.status_code == 404 and "error" in r.json()


# --------------------------------------------------------------------------- #
# Manual flow
# --------------------------------------------------------------------------- #
def test_manual_flow(client, ctx):
    r = client.post("/api/opportunities/opp_1/manual")
    assert r.status_code == 200
    d = r.json()
    assert d["valid"] is True and d["status"] == "ok" and d["remaining_s"] == 8.2
    assert ctx.opps.get("opp_1").state is OppState.MANUAL_SELECTED
    r = client.get("/api/opportunities/opp_1/manual/support")
    assert r.status_code == 200 and r.json()["human"]["light"] == "yellow"

    r = client.post("/api/opportunities/opp_1/manual/entered", json={"price": 102_150, "quote_amount": 100})
    assert r.status_code == 200
    assert r.json()["exit_target"] == 107_065.0
    assert ("manual_entered", ("opp_1", 102_150.0, 100.0), {}) in ctx.execution.calls

    r = client.post("/api/opportunities/opp_1/manual/exited", json={"price": 106_000})
    assert r.status_code == 200
    t = r.json()
    assert t["id"] == "t_manual" and t["state"] == "CLOSED" and t["mode"] == "paper"
    assert t["realized_pnl_quote"] == 3.57 and t["actual_exit_price"] == 106_000.0
    assert t["side"] == "LONG"


def test_manual_reselect_returns_open_position(client, ctx):
    """A page reload / second tab re-posts ``/manual`` for an EXECUTING manual
    opportunity and must get the open position back (SELL step), not a fresh
    BUY step and not EXPIRED; the exit can then be reported and closes it."""
    assert client.post("/api/opportunities/opp_1/manual").status_code == 200
    r = client.post("/api/opportunities/opp_1/manual/entered", json={"price": 102_150, "quote_amount": 100})
    assert r.status_code == 200 and r.json()["ok"] is True and r.json()["trade_id"] == "t_manual"
    assert ctx.opps.get("opp_1").state is OppState.EXECUTING

    r = client.post("/api/opportunities/opp_1/manual")
    assert r.status_code == 200
    d = r.json()
    assert d["valid"] is True and d["expired"] is False and d["status"] == "in_position"
    assert d["trade_id"] == "t_manual" and d["entry_price"] == 102_150.0 and d["quote_amount"] == 100.0 and d["exit_side"] == "SELL"
    assert ctx.opps.get("opp_1").state is OppState.EXECUTING  # re-selecting never disturbs the open position
    assert client.get("/api/opportunities/opp_1/manual/support").json()["status"] == "in_position"

    r = client.post("/api/opportunities/opp_1/manual/exited", json={"price": 106_000})
    assert r.status_code == 200 and r.json()["id"] == "t_manual" and r.json()["state"] == "CLOSED"
    assert ctx.opps.get("opp_1").state is OppState.CLOSED and ctx.execution.manual == {}
    assert client.get("/api/opportunities/opp_1/manual/support").json()["status"] != "in_position"


def test_manual_validation_errors(client):
    r = client.post("/api/opportunities/opp_1/manual/entered", json={"quote_amount": 100})
    assert r.status_code == 400 and "price" in r.json()["error"]
    r = client.post("/api/opportunities/opp_1/manual/entered", json={"price": -1, "quote_amount": 100})
    assert r.status_code == 400 and "error" in r.json()
    r = client.post("/api/opportunities/opp_1/manual/exited", json={"price": 0})
    assert r.status_code == 400
    r = client.post("/api/opportunities/opp_1/manual/exited", content=b"not json", headers={"Content-Type": "application/json"})
    assert r.status_code == 400 and "error" in r.json()


def test_manual_without_execution_engine(tmp_path):
    ctx = make_ctx(tmp_path, with_execution=False)
    with TestClient(create_app(ctx)) as c:
        r = c.post("/api/opportunities/opp_1/manual")
        assert r.status_code == 503 and "execution engine" in r.json()["error"]
        # emergency falls back to risk.set_emergency
        r = c.post("/api/emergency/stop", json={"reason": "no engine"})
        assert r.status_code == 200 and r.json()["emergency"] is True
        assert ctx.risk.emergency is True and ctx.risk.emergency_reason == "no engine"
        r = c.post("/api/emergency/resume")
        assert r.status_code == 200 and ctx.risk.emergency is False
        r = c.post("/api/emergency/close")
        assert r.status_code == 503
        assert c.get("/api/trades").json()[0]["id"] == "t_db"


# --------------------------------------------------------------------------- #
# Auto flow
# --------------------------------------------------------------------------- #
def test_auto_flow(client, ctx):
    r = client.post("/api/opportunities/opp_1/auto")
    assert r.status_code == 200
    d = r.json()
    assert d["confirmation_required"] is True
    snap = d["snapshot"]
    assert snap["net_now_pct"] == 4.7 and snap["risk"]["ok"] is True and snap["balance"]["free"] == 10_000.0
    r = client.post("/api/opportunities/opp_1/auto/confirm")
    assert r.status_code == 200
    d = r.json()
    assert d["ok"] is True and d["state"] == "EXECUTING"
    assert d["trade"]["id"] == "t_auto" and d["trade"]["state"] == "EXECUTING" and d["trade"]["mode"] == "paper"
    # non-blocking confirm when the engine supports ``wait``
    assert ("confirm_auto", ("opp_1",), {"wait": False}) in ctx.execution.calls
    r = client.post("/api/opportunities/opp_2/cancel", json={"reason": "changed my mind"})
    assert r.status_code == 200 and r.json()["reason"] == "changed my mind"
    r = client.post("/api/opportunities/opp_2/cancel")
    assert r.status_code == 200 and r.json()["reason"] == "user"


def test_auto_with_auto_confirm_executes_immediately(client, ctx):
    ctx.settings.auto_confirm = True
    r = client.post("/api/opportunities/opp_1/auto")
    assert r.status_code == 200
    d = r.json()
    assert "confirmation_required" not in d and d["state"] == "EXECUTING"
    # the route never long-polls the whole trade: it starts it in the background like /auto/confirm
    assert d["ok"] is True and d["started"] is True
    assert ("select_auto", ("opp_1",), {"wait": False}) in ctx.execution.calls
    assert ("confirm_auto", ("opp_1",), {"wait": False}) in ctx.execution.calls


def test_auto_deselect_backs_out_without_cancelling(client, ctx):
    """[BACK] / Escape on the AUTO confirmation returns the opportunity to VALID
    (still on the board, MANUAL possible) - it must never go through ``cancel``."""
    d = client.post("/api/opportunities/opp_1/auto").json()
    assert d["confirmation_required"] is True and ctx.opps.get("opp_1").state is OppState.AUTO_SELECTED
    r = client.post("/api/opportunities/opp_1/auto/deselect")
    assert r.status_code == 200
    assert r.json()["ok"] is True and r.json()["state"] == "VALID" and r.json()["opp_id"] == "opp_1"
    assert ctx.opps.get("opp_1").state is OppState.VALID
    assert ("deselect_auto", ("opp_1",), {}) in ctx.execution.calls
    assert not any(c[0] == "cancel" for c in ctx.execution.calls)
    # nothing selected: business refusal passes through as 200 + ok False, state untouched
    r = client.post("/api/opportunities/opp_1/auto/deselect")
    assert r.status_code == 200 and r.json()["ok"] is False and ctx.opps.get("opp_1").state is OppState.VALID
    assert client.post("/api/opportunities/nope/auto/deselect").status_code == 404
    # an engine without the method answers 501, never a cancel
    ctx.execution.deselect_auto = None
    r = client.post("/api/opportunities/opp_1/auto/deselect")
    assert r.status_code == 501 and "deselect" in r.json()["error"]
    assert not any(c[0] == "cancel" for c in ctx.execution.calls)


def test_engine_business_refusal_passes_through_as_200(client, ctx):
    """The real engine answers risk/state refusals with ``{"ok": False, "reason"}``
    (not an exception); the API must forward that verbatim so the UI can show the
    reason instead of treating it as a transport error."""

    async def refuse_auto(opp_id: str) -> dict[str, Any]:
        return {"ok": False, "reason": "risk: daily loss limit reached", "opp_id": opp_id, "state": "VALID"}

    async def refuse_entry(opp_id: str, price: float, quote_amount: float) -> dict[str, Any]:
        return {"ok": False, "reason": "AUTO execution in progress for this opportunity"}

    ctx.execution.select_auto = refuse_auto
    ctx.execution.manual_entered = refuse_entry
    r = client.post("/api/opportunities/opp_1/auto")
    assert r.status_code == 200
    assert r.json() == {"ok": False, "reason": "risk: daily loss limit reached", "opp_id": "opp_1", "state": "VALID"}
    r = client.post("/api/opportunities/opp_1/manual/entered", json={"price": 100.0, "quote_amount": 50.0})
    assert r.status_code == 200 and r.json()["ok"] is False and "AUTO execution" in r.json()["reason"]


# --------------------------------------------------------------------------- #
# Emergency
# --------------------------------------------------------------------------- #
def test_emergency_stop_close_resume(client, ctx):
    r = client.post("/api/emergency/stop", json={"reason": "test"})
    assert r.status_code == 200
    assert r.json()["emergency"] is True and r.json()["reason"] == "test"
    assert ctx.risk.emergency is True and ctx.risk.emergency_reason == "test"
    assert client.get("/api/state").json()["emergency"] is True
    assert client.get("/api/state").json()["emergency_reason"] == "test"

    r = client.post("/api/emergency/close")
    assert r.status_code == 200
    closed = r.json()["closed"]
    assert len(closed) == 1 and closed[0]["id"] == "t_closed" and closed[0]["state"] == "CLOSED"

    r = client.post("/api/emergency/resume")
    assert r.status_code == 200 and r.json()["emergency"] is False
    assert ctx.risk.emergency is False

    # body-less stop defaults the reason
    r = client.post("/api/emergency/stop")
    assert r.status_code == 200 and r.json()["reason"] == "user"


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #
def test_settings_get_has_no_secrets(client):
    r = client.get("/api/settings")
    assert r.status_code == 200
    d = r.json()
    assert d["human_execution_time_s"] == 7.0 and d["mode"] == "paper"
    assert not any(re.search(r"(?i)token|secret|api_key|passphrase", k) for k in d)
    assert not SECRET_RE.search(r.text)


def test_settings_put(client, ctx, tmp_path):
    seen: list[tuple[str, Any]] = []
    ctx.bus.subscribe_all(lambda topic, payload: seen.append((topic, payload)))
    r = client.put("/api/settings", json={"human_execution_time_s": 9, "auto_confirm": "true"})
    assert r.status_code == 200
    d = r.json()
    assert set(d["changed"]) == {"human_execution_time_s", "auto_confirm"}
    assert d["saved"] is True
    assert d["settings"]["human_execution_time_s"] == 9.0 and d["settings"]["auto_confirm"] is True
    assert ctx.settings.human_execution_time_s == 9.0
    cfg = json.loads((tmp_path / "config.json").read_text(encoding="utf-8"))
    assert cfg["human_execution_time_s"] == 9.0
    assert [t for t, _ in seen] == [SETTINGS]
    assert seen[0][1]["human_execution_time_s"] == 9.0
    assert not SECRET_RE.search(r.text)
    assert not SECRET_RE.search(json.dumps(cfg))


def test_settings_put_mode_switch(client, ctx):
    r = client.put("/api/settings", json={"paper_mode": False, "live_mode": True})
    assert r.status_code == 200 and r.json()["settings"]["mode"] == "live"
    assert ctx.settings.mode() is Mode.LIVE
    assert client.get("/api/state").json()["mode"] == "live"


def test_settings_put_rejections(client, ctx):
    r = client.put("/api/settings", json={"nope": 1})
    assert r.status_code == 400 and "unknown settings keys: nope" in r.json()["error"]
    r = client.put("/api/settings", json={"api_key": "x"})
    assert r.status_code == 400 and "secrets" in r.json()["error"]
    r = client.put("/api/settings", json={"telegram_bot_token": TOKEN})
    assert r.status_code == 400 and TOKEN not in r.text
    r = client.put("/api/settings", json={})
    assert r.status_code == 400 and "empty" in r.json()["error"]
    r = client.put("/api/settings", json=[1, 2])
    assert r.status_code == 400 and "error" in r.json()
    r = client.put("/api/settings", json={"human_execution_time_s": 0})
    assert r.status_code == 400 and "human_execution_time_s" in r.json()["error"]
    assert ctx.settings.human_execution_time_s == 7.0  # restored after the failed validation
    r = client.put("/api/settings", json={"capital_quote": "abc"})
    assert r.status_code == 400
    assert ctx.settings.capital_quote == 100.0


# --------------------------------------------------------------------------- #
# Trades / stats / balances
# --------------------------------------------------------------------------- #
def test_trades(client):
    d = client.get("/api/trades").json()
    ids = [t["id"] for t in d]
    assert ids[0] == "t_1"  # in-memory first
    assert "t_db" in ids and ids.count("t_1") == 1  # db rows merged, deduplicated
    assert d[0]["expected_net_pct"] == 4.82 and d[0]["actual_net_pct"] == 3.57 and d[0]["diff_reason"]
    assert len(client.get("/api/trades?limit=1").json()) == 1
    assert client.get("/api/trades?limit=0").status_code == 400


def test_stats(client):
    d = client.get("/api/stats").json()
    assert isinstance(d, list) and d[0]["asset"] == "BTC" and d[1]["success_rate"] is None


def test_balances(client):
    d = client.get("/api/balances").json()
    assert d["mode"] == "paper"
    assert d["balances"]["bybit"]["USDT"] == {"asset": "USDT", "free": 9_500.0, "total": 10_000.0}
    assert "kraken" in d["errors"] and "kraken" not in d["balances"]
    assert API_KEY not in d["errors"]["kraken"]  # redacted
    assert "***" in d["errors"]["kraken"]


# --------------------------------------------------------------------------- #
# WebSocket
# --------------------------------------------------------------------------- #
def test_websocket_snapshot_and_events(client, ctx):
    with client.websocket_connect("/ws") as ws:
        msg = ws.receive_json()
        assert msg["topic"] == "snapshot"
        assert msg["data"]["state"]["mode"] == "paper"
        ids = {o["id"] for o in msg["data"]["opportunities"]}
        assert ids == {"opp_1", "opp_2", "opp_3"}  # active + executing, no history
        assert client.get("/api/health").json()["ws_clients"] == 1

        client.put("/api/settings", json={"human_execution_time_s": 8})
        msg = ws.receive_json()
        assert msg["topic"] == "settings" and msg["data"]["human_execution_time_s"] == 8.0
        assert not SECRET_RE.search(json.dumps(msg))

        client.post("/api/emergency/stop", json={"reason": "ws-test"})
        msg = ws.receive_json()
        assert msg["topic"] == "emergency" and msg["data"] == {"active": True, "reason": "ws-test"}

        ws.send_text("ping")
        assert ws.receive_json()["topic"] == "pong"
    assert client.get("/api/health").json()["ws_clients"] == 0


def test_websocket_receives_opportunity_events(client, ctx):
    opp = make_opp("opp_ws", "DOGE", "bybit", 1.5)

    async def publish_all() -> None:
        await ctx.bus.publish(OPP_NEW, opp)
        await ctx.bus.publish(OPP_STATE, {"opportunity": opp, "from": "VALID", "to": "NOTIFIED", "reason": "telegram"})
        await ctx.bus.publish(TRADE_UPDATE, make_trade("t_ws", "opp_ws"))
        await ctx.bus.publish(HEALTH, ctx.adapters["bybit"].health)
        await ctx.bus.publish(DASHBOARD, {"active": 3})
        await ctx.bus.publish(BALANCE, {"exchange": "bybit", "mode": Mode.PAPER, "balances": {"USDT": Balance("USDT", 1.0, 2.0)}})

    with client.websocket_connect("/ws") as ws:
        assert ws.receive_json()["topic"] == "snapshot"
        client.portal.call(publish_all)
        topics = []
        for _ in range(6):
            msg = ws.receive_json()
            topics.append(msg["topic"])
            if msg["topic"] == OPP_STATE:
                assert msg["data"]["opportunity"]["id"] == "opp_ws" and msg["data"]["to"] == "NOTIFIED"
            if msg["topic"] == BALANCE:
                assert msg["data"]["mode"] == "paper" and msg["data"]["balances"]["USDT"]["free"] == 1.0
            if msg["topic"] == HEALTH:
                assert msg["data"]["exchange"] == "bybit" and msg["data"]["usable"] is True
        assert topics == [OPP_NEW, OPP_STATE, TRADE_UPDATE, HEALTH, DASHBOARD, BALANCE]


# --------------------------------------------------------------------------- #
# Hub unit tests
# --------------------------------------------------------------------------- #
class FakeWs:
    def __init__(self, fail: bool = False) -> None:
        self.sent: list[str] = []
        self.fail = fail
        self.closed = False

    async def send_text(self, text: str) -> None:
        if self.fail:
            raise RuntimeError("socket closed")
        self.sent.append(text)

    async def close(self) -> None:
        self.closed = True


def test_serialise_event_every_topic():
    opp = make_opp()
    trade = make_trade()
    assert serialise_event(OPP_NEW, opp, NOW)["data"]["id"] == "opp_1"
    assert serialise_event(OPP_UPDATE, opp, NOW + 2)["data"]["remaining_s"] == 10.0
    st = serialise_event(OPP_STATE, {"opportunity": opp, "from": OppState.VALID, "to": OppState.EXPIRED, "reason": "window closed"}, NOW)
    assert st["data"]["from"] == "VALID" and st["data"]["to"] == "EXPIRED" and st["data"]["opportunity"]["id"] == "opp_1"
    bare = serialise_event(OPP_STATE, opp, NOW)
    assert bare["data"]["to"] == "VALID" and bare["data"]["from"] is None
    assert serialise_event(TRADE_UPDATE, trade, NOW)["data"]["state"] == "CLOSED"
    health = serialise_event(HEALTH, ExchangeHealth(exchange="bybit", ws_connected=True), NOW)
    assert health["data"]["exchange"] == "bybit" and health["data"]["usable"] is True
    assert serialise_event(DASHBOARD, {"active": 1}, NOW)["data"] == {"active": 1}
    assert serialise_event(EMERGENCY, {"active": True, "reason": "x"}, NOW)["data"]["active"] is True
    settings = Settings()
    s = serialise_event(SETTINGS, settings, NOW)
    assert s["data"]["mode"] == "paper" and "telegram_bot_token" not in s["data"]
    assert serialise_event(SETTINGS, {"capital_quote": 5}, NOW)["data"] == {"capital_quote": 5}
    bal = serialise_event(BALANCE, {"exchange": "bybit", "mode": Mode.LIVE, "balances": {"BTC": Balance("BTC", 0.5, 0.5)}}, NOW)
    assert bal["data"] == {"exchange": "bybit", "mode": "live", "balances": {"BTC": {"asset": "BTC", "free": 0.5, "total": 0.5}}}
    assert serialise_event("book", object(), NOW) is None
    assert serialise_event(OPP_NEW, None, NOW) is None
    assert serialise_event(BALANCE, "not a dict", NOW) is None


def test_serialise_event_never_leaks_secrets():
    secrets = Secrets(telegram_bot_token=TOKEN, exchanges={"bybit": ExchangeCredentials("bybit", API_KEY, API_SECRET)})
    assert serialise_event(SETTINGS, secrets, NOW) is None
    assert serialise_event(SETTINGS, {"telegram_bot_token": TOKEN}, NOW) is None
    assert _jsonable(secrets) is None
    assert _jsonable({"creds": ExchangeCredentials("bybit", API_KEY, API_SECRET)}) == {"creds": None}


def test_jsonable_handles_nan_enums_and_tuples():
    opp = make_opp(nan_econ=True)
    d = serialise_event(OPP_NEW, opp, NOW)["data"]
    assert d["econ"]["book_age_ms"] is None
    text = json.dumps(d, allow_nan=False)  # must not raise
    assert "NaN" not in text
    assert _jsonable({Mode.PAPER: (1, 2), "x": {Light.RED}, "inf": math.inf}) == {"paper": [1, 2], "x": ["red"], "inf": None}


async def test_hub_coalesces_opp_updates(ctx):
    hub = WsHub(ctx, coalesce_interval_s=0.25)
    fake = FakeWs()
    hub.clients.add(fake)
    opp = make_opp()
    for i in range(10):
        opp.update_count = i
        hub.on_event(OPP_UPDATE, opp)
    other = replace(opp, id="opp_other")
    hub.on_event(OPP_UPDATE, other)
    assert fake.sent == []  # nothing until flush
    await hub.flush()
    assert len(fake.sent) == 2
    msgs = {json.loads(t)["data"]["id"]: json.loads(t) for t in fake.sent}
    assert msgs["opp_1"]["data"]["update_count"] == 9
    assert msgs["opp_other"]["topic"] == OPP_UPDATE
    await hub.flush()
    assert len(fake.sent) == 2  # nothing pending any more


async def test_hub_state_change_drops_pending_update(ctx):
    hub = WsHub(ctx)
    fake = FakeWs()
    hub.clients.add(fake)
    opp = make_opp()
    hub.on_event(OPP_UPDATE, opp)
    opp.state = OppState.EXPIRED
    hub.on_event(OPP_STATE, {"opportunity": opp, "from": "VALID", "to": "EXPIRED", "reason": "sweep"})
    await hub.flush()
    assert fake.sent == []  # pending update dropped; the state message sits in the queue
    msg = hub._queue.get_nowait()
    assert msg["topic"] == OPP_STATE and msg["data"]["opportunity"]["state"] == "EXPIRED"


async def test_hub_broadcast_drops_dead_clients(ctx):
    hub = WsHub(ctx)
    good, dead = FakeWs(), FakeWs(fail=True)
    hub.clients.update({good, dead})
    await hub.broadcast({"topic": "dashboard", "data": {"active": 1, "nan": math.nan}})
    assert hub.clients == {good}
    assert json.loads(good.sent[0]) == {"topic": "dashboard", "data": {"active": 1, "nan": None}}
    assert hub.sent_count == 1


async def test_hub_start_pumps_bus_events_and_stops(ctx):
    hub = WsHub(ctx, coalesce_interval_s=0.02)
    fake = FakeWs()
    hub.start()
    hub.start()  # idempotent
    hub.clients.add(fake)
    try:
        await ctx.bus.publish(DASHBOARD, {"active": 7})
        await ctx.bus.publish(OPP_UPDATE, make_opp())
        for _ in range(50):
            if len(fake.sent) >= 2:
                break
            await asyncio.sleep(0.01)
        topics = [json.loads(t)["topic"] for t in fake.sent]
        assert topics == [DASHBOARD, OPP_UPDATE]
    finally:
        await hub.stop()
    assert fake.closed is True and hub.clients == set()
    # after stop the bus no longer reaches the hub
    await ctx.bus.publish(DASHBOARD, {"active": 8})
    assert hub._queue.empty()


# --------------------------------------------------------------------------- #
# Secret leak scan
# --------------------------------------------------------------------------- #
def test_no_secret_in_any_response(client, ctx):
    paths = [
        "/", "/static/app.js", "/api/state", "/api/health", "/api/opportunities?state=all", "/api/opportunities/opp_1",
        "/api/settings", "/api/trades", "/api/stats", "/api/balances", "/api/opportunities/opp_1/manual/support",
    ]
    for path in paths:
        r = client.get(path)
        assert r.status_code == 200, path
        assert not SECRET_RE.search(r.text), path
    snapshot = json.dumps(build_snapshot(ctx))
    assert not SECRET_RE.search(snapshot)
    # error paths are redacted too (engine exceptions may quote exchange responses)
    r = client.put("/api/settings", json={"telegram_bot_token": TOKEN})
    assert r.status_code == 400 and not SECRET_RE.search(r.text)
