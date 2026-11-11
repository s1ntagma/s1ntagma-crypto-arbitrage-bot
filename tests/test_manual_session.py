"""MANUAL TRADE SESSION (owner's flow): Я ЗАШЁЛ -> amount -> exact plan for that
amount -> Я КУПИЛ -> escort (sell now / wait / sell at market) -> Я УШЁЛ СО СДЕЛКИ
-> actual result saved next to the expected one -> human-success learning."""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from latbot.api.server import create_app
from latbot.engine.executability import assess_human
from latbot.models import ReactionStats
from test_app import build, detect, push_all


async def _session(tmp_path: Path, capital: float = 50.0):
    b = build(tmp_path, db_path=str(tmp_path / "data" / "latbot.sqlite3"))
    opp = await detect(b)
    sup = await b.ctx.execution.select_manual(opp.id, capital=capital)
    return b, opp, sup


async def test_amount_first_then_exact_plan_for_that_amount(tmp_path):
    b, opp, sup = await _session(tmp_path, capital=50.0)
    assert sup["valid"] is True and sup["requested_capital"] == pytest.approx(50.0)
    assert sup["capital"] == pytest.approx(50.0, rel=1e-6)
    assert sup["buy_amount"] == pytest.approx(50.0 / sup["entry_price_now"], rel=1e-3)
    assert sup["sell_amount"] == pytest.approx(sup["buy_amount"])
    assert sup["net_quote"] == pytest.approx(50.0 * sup["net_now_pct"] / 100.0, rel=1e-6)
    assert sup["entry_cap_price"] >= sup["entry_price_now"] and sup["exit_target_now"] > sup["entry_price_now"]
    assert sup["exit_price_now"] > 0 and sup["pair_url"]
    # the poll keeps the chosen amount without repeating it
    again = await b.ctx.execution.manual_support(opp.id)
    assert again["capital"] == pytest.approx(50.0, rel=1e-6)
    # the owner may change the amount before buying
    bigger = await b.ctx.execution.manual_support(opp.id, capital=80.0)
    assert bigger["capital"] == pytest.approx(80.0, rel=1e-6)


async def test_escort_wait_then_sell_now_then_result_saved(tmp_path):
    b, opp, sup = await _session(tmp_path, capital=50.0)
    ex = b.ctx.execution
    entered = await ex.manual_entered(opp.id, price=sup["entry_price_now"], quote_amount=50.0)
    assert entered["ok"] is True
    s = await ex.manual_support(opp.id)
    assert s["status"] == "in_position" and s["sell_signal"] == "wait"
    assert "ЖДИ" in s["sell_text"] and s["exit_target_now"] == pytest.approx(entered["exit_target"])
    assert s["sell_amount"] == pytest.approx(entered["base_amount"]) and s["pnl_now_pct"] < 0  # spread + fees, nothing caught up yet
    # Kraken catches up past the target -> SELL NOW, with the profit if sold now
    b.clock.advance(0.3)
    await push_all(b.fakes, {"kraken": 101.6})
    s = await ex.manual_support(opp.id)
    assert s["sell_signal"] == "sell_now" and "ПРОДАВАЙ СЕЙЧАС" in s["sell_text"]
    assert s["exit_price_now"] >= s["exit_target_now"] and s["pnl_now_pct"] > 0 and s["pnl_now_quote"] > 0
    # Я УШЁЛ СО СДЕЛКИ: the owner's actual numbers are stored next to the expected ones
    trade = await ex.manual_exited(opp.id, s["exit_price_now"], reported_pnl_quote=0.42, note="вышел по цели")
    assert trade.state.value == "CLOSED" and trade.manual is True
    assert trade.user_reported_pnl_quote == pytest.approx(0.42) and trade.user_note == "вышел по цели"
    assert trade.entered_in_window is True and trade.time_to_entry_s is not None and trade.time_to_entry_s >= 0
    row = b.ctx.db.get_trade(trade.id)
    assert row["user_reported_pnl_quote"] == pytest.approx(0.42) and row["expected_net_pct"] == pytest.approx(trade.expected_net_pct)
    assert row["actual_net_pct"] == pytest.approx(trade.actual_net_pct) and row["entered_in_window"] is True
    rs = b.ctx.stats.get(opp.asset, opp.target_exchange)
    assert rs.manual_total == 1 and rs.manual_in_window == 1 and rs.manual_wins == 1
    # back to monitoring: nothing of the session is left behind
    assert ex.active_trade(opp.id) is None
    assert opp.id not in ex._manual_capital and opp.id not in ex._manual_selected_mono


async def test_lag_gone_in_position_means_sell_at_market(tmp_path):
    b, opp, sup = await _session(tmp_path)
    ex = b.ctx.execution
    await ex.manual_entered(opp.id, price=sup["entry_price_now"], quote_amount=50.0)
    s = await ex.manual_support(opp.id)
    assert s["sell_signal"] == "wait"  # the lag is still there: waiting is right
    b.clock.advance(0.3)
    await push_all(b.fakes, {"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})  # the movers fell back: no catch-up coming
    s = await ex.manual_support(opp.id)
    assert s["sell_signal"] == "sell_market" and "по рынку" in s["sell_text"]


def test_assess_human_learns_from_real_manual_trades():
    from latbot.config import Settings
    s = Settings()
    rs = ReactionStats(asset="BTC", exchange="kraken")
    base = assess_human(60.0, 1.0, s, stats=rs)
    rs.manual_total, rs.manual_in_window, rs.manual_wins = 4, 1, 0  # the owner missed 4 of 4 in reality
    worse = assess_human(60.0, 1.0, s, stats=rs)
    assert worse.probability < base.probability
    assert any("по твоим сделкам" in r for r in worse.reasons)


async def test_session_api_end_to_end(tmp_path):
    b = build(tmp_path, db_path=str(tmp_path / "data" / "latbot.sqlite3"))
    opp = await detect(b)
    with TestClient(create_app(b.ctx)) as c:
        sel = c.post(f"/api/opportunities/{opp.id}/manual", json={"capital": 40}).json()
        assert sel["capital"] == pytest.approx(40.0, rel=1e-6)
        sup = c.get(f"/api/opportunities/{opp.id}/manual/support", params={"capital": 30}).json()
        assert sup["capital"] == pytest.approx(30.0, rel=1e-6)
        ent = c.post(f"/api/opportunities/{opp.id}/manual/entered", json={"price": sup["entry_price_now"], "quote_amount": 30}).json()
        assert ent["ok"] is True
        tr = c.post(f"/api/opportunities/{opp.id}/manual/exited",
                    json={"price": sup["exit_price_now"], "reported_pnl_quote": -0.1, "note": "рано"}).json()
        assert tr["state"] == "CLOSED" and tr["user_reported_pnl_quote"] == pytest.approx(-0.1) and tr["user_note"] == "рано"
        # old clients (no capital, no actual result) keep working
        assert c.post(f"/api/opportunities/{opp.id}/manual/exited", json={"price": 1.0}).status_code in (400, 404, 409)
