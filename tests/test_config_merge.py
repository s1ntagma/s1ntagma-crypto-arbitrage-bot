"""config.json written by an older build must still get venues added later."""
from __future__ import annotations

import json
from pathlib import Path

from latbot.config import DEFAULT_EXCHANGES, Settings, load_settings


def test_saved_exchange_list_gains_new_default_venues(tmp_path: Path) -> None:
    old = Settings()
    old.exchanges = [e for e in old.exchanges if e.name != "revolutx"]
    old.exchanges[0].enabled = False
    p = tmp_path / "config.json"
    p.write_text(old.to_json(), encoding="utf-8")
    s = load_settings(p)
    names = [e.name for e in s.exchanges]
    assert "revolutx" in names and names.index("revolutx") == len(names) - 1
    assert s.exchange(old.exchanges[0].name).enabled is False  # user's choice kept
    assert len(names) == len(DEFAULT_EXCHANGES)


def test_update_from_ui_also_keeps_new_venues() -> None:
    s = Settings()
    partial = [json.loads(json.dumps({"name": "kraken", "role": "execution"}))]
    s.update({"exchanges": partial})
    assert {e.name for e in s.exchanges} == {e.name for e in DEFAULT_EXCHANGES}
