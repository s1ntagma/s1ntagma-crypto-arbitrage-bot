"""Exchange keys from the environment: the slot form (EXCHANGE_n_*) and the named form (BYBIT_API_KEY).

Owner's bug (2026-11-11): the template has ``EXCHANGE_1_NAME=bybit`` with empty key lines; a key written as
``BYBIT_API_KEY`` next to it was silently ignored."""
from __future__ import annotations

import pytest

from latbot.config import load_secrets, secrets_report

KEY, SEC = "kkkkDEMOkey1234", "ssssDEMOsecret5678"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    import os

    for k in list(os.environ):
        if k.startswith("EXCHANGE_") or k.startswith("BYBIT_"):
            monkeypatch.delenv(k)


def test_the_named_form_fills_an_empty_slot(monkeypatch):
    monkeypatch.setenv("EXCHANGE_1_NAME", "bybit")
    monkeypatch.setenv("EXCHANGE_1_API_KEY", "")
    monkeypatch.setenv("EXCHANGE_1_API_SECRET", "")
    monkeypatch.setenv("BYBIT_API_KEY", KEY)
    monkeypatch.setenv("BYBIT_API_SECRET", SEC)
    c = load_secrets(None).creds("bybit")
    assert c.present and c.api_key == KEY and c.api_secret == SEC
    r = secrets_report("bybit")
    assert r["present"] and r["source"] == "BYBIT_API_KEY / BYBIT_API_SECRET"


def test_a_complete_slot_wins_over_the_named_form(monkeypatch):
    monkeypatch.setenv("EXCHANGE_3_NAME", "Bybit")
    monkeypatch.setenv("EXCHANGE_3_API_KEY", "slotkey")
    monkeypatch.setenv("EXCHANGE_3_API_SECRET", "slotsecret")
    monkeypatch.setenv("BYBIT_API_KEY", KEY)
    monkeypatch.setenv("BYBIT_API_SECRET", SEC)
    c = load_secrets(None).creds("bybit")
    assert c.api_key == "slotkey" and secrets_report("bybit")["source"] == "EXCHANGE_3_API_KEY / EXCHANGE_3_API_SECRET"


def test_a_half_written_key_is_reported_by_name_never_by_value(monkeypatch):
    monkeypatch.setenv("EXCHANGE_1_NAME", "bybit")
    monkeypatch.setenv("EXCHANGE_1_API_KEY", KEY)
    r = secrets_report("bybit")
    assert not load_secrets(None).creds("bybit").present and not r["present"]
    assert "EXCHANGE_1_API_SECRET is empty" in r["text"] and KEY not in repr(r)
    monkeypatch.delenv("EXCHANGE_1_NAME")
    monkeypatch.delenv("EXCHANGE_1_API_KEY")
    r = secrets_report("bybit")
    assert not r["present"] and "EXCHANGE_1_NAME=bybit" in r["text"]  # tells what to write


def test_quotes_and_spaces_around_a_value_are_cleaned(monkeypatch):
    monkeypatch.setenv("BYBIT_API_KEY", f'  "{KEY}" ')
    monkeypatch.setenv("BYBIT_API_SECRET", f"'{SEC}'")
    c = load_secrets(None).creds("bybit")
    assert c.api_key == KEY and c.api_secret == SEC
