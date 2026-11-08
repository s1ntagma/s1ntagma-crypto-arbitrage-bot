"""Every test keeps the bot's data folder (``latbot.storage.home.data_home``) inside a temp dir."""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _isolated_data_home(tmp_path_factory, monkeypatch):
    monkeypatch.setenv("LATBOT_HOME", str(tmp_path_factory.mktemp("latbot_home")))
