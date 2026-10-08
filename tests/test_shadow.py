"""Shadow trading: every approved opportunity is traded virtually on the real
book (no orders) and closed by the real market - target reached or window over."""
from __future__ import annotations

import pytest

from latbot.engine.shadow import ShadowTrader, summarize
from latbot.models import OppState
from latbot.storage.db import Database
from test_pipeline import Env, detect


def _env(db=None) -> tuple[Env, ShadowTrader]:
    env = Env(db=db)
    return env, ShadowTrader(env.settings, env.bus, env.clock, db=db)


async def test_shadow_opens_on_real_book_and_closes_when_target_reached():
    db = Database(":memory:")
    env, sh = _env(db)
    opp = await detect(env)
    rows = sh.trades()
    assert len(rows) == 1 and rows[0]["open"] is True and rows[0]["id"] == opp.id
    t = rows[0]
    assert t["entry_price"] == pytest.approx(opp.econ.entry_vwap) and t["expected_net_pct"] == pytest.approx(opp.econ.net_pct)
    assert t["capital_quote"] == pytest.approx(opp.econ.executable_quote)
    # kraken catches up past the exit target -> refresh -> shadow sells at the real exit VWAP
    env.clock.advance(0.3)
    await env.push("kraken", 101.5)
    rows = sh.trades()
    closed = rows[0]
    assert closed["open"] is False and closed["exit_reason"] == "target reached"
    expected = (closed["exit_price"] / closed["entry_price"] - 1) * 100 - t["entry_fee_pct"] - t["exit_fee_pct"] - t["execution_loss_pct"]
    assert closed["actual_net_pct"] == pytest.approx(expected) and closed["actual_net_pct"] > 0
    assert closed["pnl_quote"] == pytest.approx(closed["capital_quote"] * closed["actual_net_pct"] / 100)
    assert db.list_shadow()[0]["exit_reason"] == "target reached"  # persisted
    s = sh.summary()
    assert s["closed"] == 1 and s["wins"] == 1 and s["win_rate"] == 1.0 and s["target_reached"] == 1
    assert s["by_target"]["kraken"]["closed"] == 1 and s["by_asset"]["BTC"]["pnl_quote"] > 0


async def test_shadow_closes_at_last_market_price_when_window_ends():
    env, sh = _env()
    opp = await detect(env)
    await env.opps.expire(opp.id, "window over")
    row = sh.trades()[0]
    assert row["open"] is False and row["exit_reason"].startswith("opportunity expired")
    # nothing moved: sold at the bid it was bought against -> fees + spread = a loss, honestly
    assert row["actual_net_pct"] < 0
    assert sh.summary()["wins"] == 0


async def test_shadow_off_and_no_book_means_no_trade():
    env, sh = _env()
    env.settings.shadow_trading = False
    await detect(env)
    assert sh.trades() == []


def test_summarize_empty():
    s = summarize([])
    assert s["closed"] == 0 and s["win_rate"] is None and s["open"] == 0 and s["by_target"] == {}


def test_state_enum_has_expired():
    assert OppState("EXPIRED") is OppState.EXPIRED


async def test_shadow_and_trade_summary_api_through_real_app(tmp_path):
    from fastapi.testclient import TestClient
    from latbot.api.server import create_app
    from test_app import build, detect as app_detect

    b = build(tmp_path)
    assert b.ctx.shadow is not None
    opp = await app_detect(b)
    await b.ctx.execution.select_manual(opp.id)
    await b.ctx.execution.manual_entered(opp.id, price=100.01, quote_amount=100.0)
    with TestClient(create_app(b.ctx)) as c:
        d = c.get("/api/shadow").json()
        assert d["enabled"] is True and d["trades"][0]["id"] == opp.id and d["summary"]["open"] == 1
        f = c.get("/api/diagnostics").json()["funnel"]
        assert f["shadow_opened"] == 1 and f["shadow_closed"] == 0 and f["shadow_skipped"] == 0
        ts = c.get("/api/trades/summary").json()
        assert ts["trades"] == 1 and ts["open"] == 1 and ts["finished"] == 0 and ts["win_rate"] is None
        assert ts["by_exchange"]["kraken"]["trades"] == 1 and ts["by_mode"]["paper"]["trades"] == 1


async def test_shadow_trades_thin_book_at_executable_size():
    """A book thinner than the requested capital (liquidity flag) is still a real,
    executable trade at the size the book supports - shadow must trade it."""
    env, sh = _env()
    await env.warm_up()
    await env.push("binance", 102.0)
    env.adapters["kraken"].mark_msg()
    # kraken book: only ~30 quote per side within slippage (< 50 % of the 100 requested)
    await env.pipeline.on_book(env.book("kraken", 100.0, size=0.1))
    opps = await env.push("kucoin", 102.0)
    assert len(opps) == 1 and opps[0].econ.liquidity_ok is False and opps[0].econ.executable_quote >= env.settings.min_trade_quote
    row = sh.trades()[0]
    assert row["open"] is True and row["capital_quote"] == pytest.approx(opps[0].econ.executable_quote)
    assert sh.skipped == {}


async def test_stats_period_hides_older_trades_but_keeps_history():
    db = Database(":memory:")
    env = Env(db=db)
    db.save_shadow({"id": "old1", "asset": "OGN", "target": "gate", "opened_ts": env.clock.wall() - 86400,
                    "closed_ts": env.clock.wall() - 86000, "actual_net_pct": -1.0, "expected_net_pct": 1.0, "pnl_quote": -1.0})
    sh = ShadowTrader(env.settings, env.bus, env.clock, db=db)  # first start of this build: period begins now
    s = sh.summary()
    assert s["closed"] == 0 and s["all_time_closed"] == 1 and s["period_start_ts"] == env.clock.wall()
    assert len(sh.trades()) == 1  # history stays visible in the trades table and in the DB
    opp = await detect(env)
    await env.opps.expire(opp.id, "window over")
    assert sh.summary()["closed"] == 1
    env.clock.advance(60.0)
    start = sh.new_period()
    assert start == env.clock.wall() and sh.summary()["closed"] == 0 and sh.summary()["all_time_closed"] == 2
    # the period survives a restart
    assert ShadowTrader(env.settings, env.bus, env.clock, db=db).summary()["period_start_ts"] == start


async def test_virtual_balance_grows_with_closed_trades_and_limits_new_ones():
    db = Database(":memory:")
    env = Env(db=db)
    env.settings.shadow_balance_quote = 1000.0
    sh = ShadowTrader(env.settings, env.bus, env.clock, db=db)
    opp = await detect(env)
    env.clock.advance(0.3)
    await env.push("kraken", 101.5)  # target reached: a virtual win
    s = sh.summary()
    pnl = s["pnl_quote"]
    assert pnl > 0
    assert s["balance_start"] == 1000.0 and s["balance_now"] == pytest.approx(1000.0 + pnl)
    assert s["balance_pct"] == pytest.approx(pnl / 1000.0 * 100.0)
    assert s["today_closed"] == 1 and s["today_pnl_quote"] == pytest.approx(pnl)
    assert s["max_drawdown_pct"] == pytest.approx(0.0)
    assert opp.id == sh.trades()[0]["id"]


async def test_no_free_virtual_balance_skips_the_trade():
    env = Env()
    env.settings.shadow_balance_quote = 10.0  # less than one trade's stake
    env.settings.shadow_unlimited = False  # a limited virtual account (the owner's training default is unlimited)
    sh = ShadowTrader(env.settings, env.bus, env.clock)
    await detect(env)
    assert sh.trades() == [] and sh.skipped.get("no free virtual balance") == 1
