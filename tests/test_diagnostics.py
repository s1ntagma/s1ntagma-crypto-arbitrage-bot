"""The signal funnel: every silent drop is counted and explained (engine.diagnostics)."""
from __future__ import annotations

from types import SimpleNamespace

from fastapi.testclient import TestClient

from latbot.api.server import create_app
from latbot.config import Settings
from latbot.engine.diagnostics import REASON_KEYS, Diagnostics
from latbot.notify.telegram import TelegramNotifier
from test_pipeline import Env, detect
from test_telegram import Env as TgEnv


def _funnel(env: Env) -> dict:
    return env.pipeline.diagnostics()["funnel"]


async def test_quiet_market_counts_ticks_and_explains_no_moves():
    env = Env()
    d = env.pipeline.diagnostics()
    assert d["funnel"]["ticks_received"] == 0 and d["funnel"]["exchanges_connected"] == 3  # stubs report ws_connected
    assert "ни одного тика ещё не пришло" in d["why"][0]
    unplugged = Diagnostics(env.settings, env.clock).snapshot(env.state, {}, {})
    assert "Ни одна биржа не подключена" in unplugged["why"][0]
    await env.warm_up()
    d = env.pipeline.diagnostics()
    f = d["funnel"]
    assert f["ticks_received"] == 6 and f["assets_scanned"] == 1 and f["exchanges_connected"] == 3
    assert d["exchanges"]["kraken"]["books"] == 2 and d["exchanges"]["kraken"]["role"] == "execution"
    assert f["episodes_detected"] == 0 and f["signals"] == 0
    assert d["why"] == [f"Рынок спокоен: ни одна биржа не двигалась на >= {env.settings.move_threshold_pct:g}% "
                        f"за {env.settings.move_window_s:g} с (move_threshold_pct / move_window_s) по 1 монетам."]


async def test_single_mover_is_rejected_by_min_movers_and_named():
    env = Env()
    await env.warm_up()
    await env.push("binance", 102.0)
    f = _funnel(env)
    assert f["episodes_detected"] == 1 and f["rejected"]["min_movers"] == 1 and f["signals"] == 0
    env.clock.advance(0.1)
    await env.push("binance", 102.1)  # the same episode re-checked: not a second rejection
    assert _funnel(env)["rejected"]["min_movers"] == 1
    why = env.pipeline.diagnostics()["why"]
    assert any("двинулась только одна биржа" in line and "BTC" in line for line in why)


async def test_signal_then_opportunity_counts_through_the_funnel():
    env = Env()
    await detect(env)
    f = _funnel(env)
    assert f["signals"] == 1 and f["opportunities"] == 1 and f["rejected_total"] == 1  # the lone binance move
    assert env.pipeline.diagnostics()["why"][0].startswith("Сигналы были: возможностей 1")
    assert env.pipeline.diagnostics()["first_movers"] == {"binance": 1}


async def test_net_below_show_threshold_is_a_counted_rejection():
    env = Env()
    env.adapters["kraken"].taker = 0.01
    await env.warm_up()
    await env.push("binance", 102.0)
    await env.push("kucoin", 102.0)
    f = _funnel(env)
    assert f["signals"] == 1 and f["opportunities"] == 0 and f["rejected"]["below_min_net"] == 1
    why = env.pipeline.diagnostics()["why"]
    assert any("NET ниже min_net_pct_show" in line and "BTC@kraken" in line for line in why)
    assert any("ни один не стал возможностью" in line for line in why)


async def test_stale_target_is_target_no_data_then_no_fresh_book():
    env = Env()
    await env.push_all({"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
    env.clock.advance(0.5)
    await env.push_all({"binance": 100.0, "kucoin": 100.0})
    env.clock.advance(1.5)
    await env.push("binance", 102.0)
    await env.push("kucoin", 102.0)
    f = _funnel(env)
    assert f["rejected"]["target_no_data"] == 1 and f["signals"] == 0
    assert any("нет свежих данных" in line for line in env.pipeline.diagnostics()["why"])


def test_reject_dedupes_and_ignores_unknown_reasons():
    d = Diagnostics(Settings())
    for _ in range(5):
        d.reject("min_lag", "ETH", "kraken", 1)
    d.reject("min_lag", "ETH", "bybit", 1)
    d.reject("nonsense", "ETH")
    assert d.rejected["min_lag"] == 2 and list(d.recent["min_lag"]) == ["ETH@kraken", "ETH@bybit"]
    assert set(d.rejected) == set(REASON_KEYS)


def test_api_diagnostics_endpoint(tmp_path):
    env = Env()
    ctx = SimpleNamespace(settings=env.settings, secrets=None, bus=env.bus, clock=env.clock, db=None, state=env.state,
                          stats=env.stats, opps=env.opps, pipeline=env.pipeline, adapters=env.adapters,
                          exec_adapters={}, risk=None, execution=None, notifier=None, config_path=tmp_path / "c.json")
    with TestClient(create_app(ctx)) as c:
        d = c.get("/api/diagnostics").json()
    assert set(d) >= {"funnel", "exchanges", "why", "thresholds", "uptime_s"}
    assert d["thresholds"]["min_movers"] == env.settings.min_movers
    stub = SimpleNamespace(**{**ctx.__dict__, "pipeline": SimpleNamespace(dashboard=lambda: {})})
    with TestClient(create_app(stub)) as c:
        d = c.get("/api/diagnostics").json()
    assert d["error"] == "diagnostics unavailable" and d["why"]


async def test_telegram_diag_command_reports_the_funnel():
    tg = await TgEnv().start()
    pipe = Env()
    await pipe.warm_up()
    tg.notifier._pipeline = pipe.pipeline
    await tg.notifier.handle_update(tg.message("/diag"))
    text = tg.session.texts()[-1]
    assert "Диагностика" in text and "Бирж подключено: 3/3" in text and "Рынок спокоен" in text
    bare = TelegramNotifier(tg.settings, tg.secrets, tg.bus, tg.opps, tg.execution, tg.risk, tg.clock, session=tg.session)
    assert "недоступна" in bare._diag_text()


async def test_alert_counts_in_the_funnel():
    tg = await TgEnv().start()
    pipe = Env()
    tg.notifier._pipeline = pipe.pipeline
    from test_telegram import make_opp
    opp = make_opp(tg.clock)
    await tg.notifier._handle_opp(opp)
    assert pipe.pipeline.diagnostics()["funnel"]["alerts"] == 1
