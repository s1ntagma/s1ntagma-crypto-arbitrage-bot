"""Owner: trades and counters must survive switching the bot off and on, and installing a new build
into a new folder (each ZIP unpacks into its own folder: the history must follow)."""
from __future__ import annotations

import sqlite3

from latbot.engine.shadow import ShadowTrader
from latbot.storage import home
from latbot.storage.db import Database
from test_simple_mode import simple_env


async def test_training_counters_survive_a_restart(tmp_path):
    env = simple_env()
    env.adapters["kraken"].taker = 0.004  # costs eat the lag: a training trade, no offer
    db = Database(str(tmp_path / "latbot.sqlite3"), clock=env.clock)
    sh = ShadowTrader(env.settings, env.bus, env.clock, db=db, market=env.pipeline.market_view, stats=env.stats)
    env.pipeline.sweep_listeners.append(sh.tick)
    await env.warm_up()
    await env.push("binance", 101.0)
    assert sh.training_opened == 1
    sh.stop()
    again = ShadowTrader(env.settings, env.bus, env.clock, db=db, market=env.pipeline.market_view, stats=env.stats)
    assert again.training_opened == 1 and again.opened == 0
    assert any(r.get("training") for r in again.trades())  # the trade itself is still there too


def test_data_home_is_outside_the_program_folder(monkeypatch, tmp_path):
    monkeypatch.setenv("LATBOT_HOME", str(tmp_path / "h"))
    assert home.data_home() == tmp_path / "h"
    monkeypatch.delenv("LATBOT_HOME")
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "appdata"))
    monkeypatch.setattr(home, "_windows", lambda: True)
    assert home.data_home() == tmp_path / "appdata" / "latbot"


def _install(root, name, *, db_rows=0, config=None, env=None, mtime=None):
    d = root / "w" / name  # one level down: the scan of the grandparent stays inside tmp_path
    (d / "latbot").mkdir(parents=True)
    if db_rows:
        (d / "data").mkdir()
        con = sqlite3.connect(d / "data" / "latbot.sqlite3")
        con.execute("CREATE TABLE t (x INTEGER)")
        con.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(db_rows)])
        con.commit()
        con.close()
        (d / "data" / "pool.json").write_text("{}", encoding="utf-8")
    if config is not None:
        (d / "config.json").write_text(config, encoding="utf-8")
    if env is not None:
        (d / ".env").write_text(env, encoding="utf-8")
    if mtime is not None:
        import os
        for p in d.rglob("*"):
            os.utime(p, (mtime, mtime))
    return d


def test_a_new_build_takes_the_history_of_the_newest_previous_install(tmp_path):
    _install(tmp_path, "latbot-old", db_rows=3, mtime=1_000_000)
    _install(tmp_path, "latbot-prev", db_rows=7, mtime=2_000_000)
    new = _install(tmp_path, "latbot-new")
    store = tmp_path / "home"
    src = home.adopt_previous_data(store, new)
    assert src is not None and src.parent.parent.name == "latbot-prev"
    con = sqlite3.connect(store / "latbot.sqlite3")
    assert con.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 7
    con.close()
    assert (store / "pool.json").exists()
    # the history is now in the store: a later start never overwrites it
    _install(tmp_path, "latbot-newer", db_rows=1, mtime=3_000_000)
    assert home.adopt_previous_data(store, new) is None


def test_a_new_build_takes_config_and_env_of_the_previous_install_when_it_has_none(tmp_path):
    _install(tmp_path, "latbot-prev", config='{"a": 1}', env="TELEGRAM_BOT_TOKEN=x\n", mtime=2_000_000)
    new = _install(tmp_path, "latbot-new")
    assert sorted(home.adopt_previous_install(new)) == [".env", "config.json"]
    assert (new / "config.json").read_text(encoding="utf-8") == '{"a": 1}'
    (new / "config.json").write_text('{"b": 2}', encoding="utf-8")
    assert home.adopt_previous_install(new) == []  # files of the new install are never overwritten
    assert (new / "config.json").read_text(encoding="utf-8") == '{"b": 2}'


def test_an_untouched_env_template_is_replaced_by_the_previous_env(tmp_path):
    _install(tmp_path, "latbot-prev", env="TELEGRAM_BOT_TOKEN=x\n", mtime=2_000_000)
    new = _install(tmp_path, "latbot-new")
    (new / ".env.example").write_text("TELEGRAM_BOT_TOKEN=\n", encoding="utf-8")
    (new / ".env").write_text("TELEGRAM_BOT_TOKEN=\n", encoding="utf-8")
    assert home.adopt_previous_install(new) == [".env"]


def test_the_default_database_moves_to_the_data_folder_and_a_custom_path_stays(monkeypatch, tmp_path):
    from latbot import app as app_mod
    from latbot.config import Settings

    monkeypatch.setenv("LATBOT_HOME", str(tmp_path / "store"))
    s = Settings()
    assert s.db_path == "data/latbot.sqlite3"
    app_mod._persistent_store(s)
    assert s.db_path == str(tmp_path / "store" / "latbot.sqlite3")
    custom = Settings(db_path=str(tmp_path / "mine.sqlite3"))
    app_mod._persistent_store(custom)
    assert custom.db_path == str(tmp_path / "mine.sqlite3")


def test_the_latency_strategys_virtual_trades_are_switched_off_for_the_owner_config():
    from latbot.config import CONFIG_VERSION, Settings

    old = Settings.from_dict({"config_version": 10, "shadow_trading": True, "shadow_train_all_signals": True})
    assert old.shadow_trading is False and old.shadow_train_all_signals is False
    kept = Settings.from_dict({"config_version": CONFIG_VERSION, "shadow_trading": True})
    assert kept.shadow_trading is True  # a later choice of the owner stays


async def test_the_old_result_is_archived_once_and_nothing_opens_while_switched_off(tmp_path):
    env = simple_env()
    env.adapters["kraken"].taker = 0.004
    db = Database(str(tmp_path / "latbot.sqlite3"), clock=env.clock)
    sh = ShadowTrader(env.settings, env.bus, env.clock, db=db, market=env.pipeline.market_view, stats=env.stats)
    env.pipeline.sweep_listeners.append(sh.tick)
    await env.warm_up()
    await env.push("binance", 101.0)  # one training trade of the old period
    assert sh.training_opened == 1
    sh.stop()
    env.settings.shadow_trading = False
    env.settings.shadow_train_all_signals = False
    env.clock.advance(10.0)
    off = ShadowTrader(env.settings, env.bus, env.clock, db=db, market=env.pipeline.market_view, stats=env.stats)
    a = off.archive()
    assert a["training"]["trades"] == 1 and a["archived_ts"] and off.period_start_ts == a["archived_ts"]
    assert off.summary()["training"]["closed"] == 0  # Stats starts from zero
    assert db.count_shadow(training=True) == 1  # the old trade is still stored
    await env.push("binance", 102.5)
    assert off.training_opened == 1 and off.opened == 0  # nothing new while switched off
    again = ShadowTrader(env.settings, env.bus, env.clock, db=db, market=env.pipeline.market_view, stats=env.stats)
    assert again.archive()["archived_ts"] == a["archived_ts"]  # once
