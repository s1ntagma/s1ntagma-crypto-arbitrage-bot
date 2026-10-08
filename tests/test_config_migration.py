"""config.json written by older builds is upgraded once (owner's Phase 3 decisions)."""
from __future__ import annotations

import json

from latbot.config import CONFIG_VERSION, Settings, load_settings


def test_old_defaults_are_upgraded(tmp_path):
    p = tmp_path / "config.json"
    p.write_text(json.dumps({"min_target_volume_quote": 100000.0, "min_net_pct_show": 0.3}), encoding="utf-8")
    s = load_settings(p)
    assert s.min_target_volume_quote == 500_000.0  # owner: turnover on the target venue >= ~500k
    assert s.min_net_pct_show == 0.1  # owner: training enters every trade with NET >= 0.1 %
    assert s.config_version == CONFIG_VERSION


def test_custom_turnover_is_kept(tmp_path):
    p = tmp_path / "config.json"
    p.write_text(json.dumps({"min_target_volume_quote": 750000.0}), encoding="utf-8")
    assert load_settings(p).min_target_volume_quote == 750_000.0


def test_current_version_is_not_migrated_again():
    s = Settings.from_dict({"config_version": CONFIG_VERSION, "min_target_volume_quote": 100000.0, "min_net_pct_show": 0.3})
    assert s.min_target_volume_quote == 100_000.0 and s.min_net_pct_show == 0.3


def test_defaults_match_owner_decisions():
    s = Settings()
    assert s.min_target_volume_quote == 500_000.0 and s.min_net_pct_show == 0.1
