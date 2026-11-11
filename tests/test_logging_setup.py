"""Offline tests for latbot.logging_setup (hard rule 7: secrets never reach a log).

Every handler on the root logger shares one ``RedactFilter``.  ``Handler.handle``
runs the filter BEFORE ``Formatter.format`` renders the traceback, so the filter
must pre-render ``exc_info`` itself - otherwise the first handler (the console)
prints the raw traceback and only the second one (the file) sees a cached,
redacted ``exc_text``.
"""
from __future__ import annotations

import io
import logging

import pytest

from latbot.logging_setup import RedactFilter, redact, register_secret

TOKEN = "123456789:" + "A" * 35
KEY = "SUPERSECRETKEY123"


@pytest.fixture
def two_handlers() -> tuple[logging.Logger, list[io.StringIO]]:
    """A private logger with two stream handlers that share one RedactFilter
    (the same layout as ``setup_logging``: console first, file second)."""
    log = logging.getLogger("latbot.test.redact")
    log.propagate = False
    log.setLevel(logging.DEBUG)
    for h in list(log.handlers):
        log.removeHandler(h)
    flt = RedactFilter()
    buffers: list[io.StringIO] = []
    for _ in range(2):
        buf = io.StringIO()
        h = logging.StreamHandler(buf)
        h.setFormatter(logging.Formatter("%(message)s"))
        h.addFilter(flt)
        log.addHandler(h)
        buffers.append(buf)
    try:
        yield log, buffers
    finally:
        for h in list(log.handlers):
            log.removeHandler(h)


def test_exception_traceback_is_redacted_on_every_handler(two_handlers) -> None:
    log, buffers = two_handlers
    register_secret(TOKEN)
    try:
        raise RuntimeError(f"url=https://api.telegram.org/bot{TOKEN}/sendMessage")
    except RuntimeError:
        log.exception("boom")
    outputs = [b.getvalue() for b in buffers]
    assert all("RuntimeError" in out for out in outputs)  # the traceback itself is still there
    assert all(TOKEN not in out for out in outputs), outputs
    assert all("***" in out for out in outputs)


def test_exc_info_true_and_stack_info_are_redacted(two_handlers) -> None:
    log, buffers = two_handlers
    register_secret(KEY)
    try:
        raise ValueError(f"apiKey={KEY} rejected")
    except ValueError:
        log.error("order failed", exc_info=True)
    log.warning("context %s", KEY, stack_info=True)
    for buf in buffers:
        out = buf.getvalue()
        assert "ValueError" in out and "Stack (most recent call last)" in out
        assert KEY not in out, out
        assert "order failed" in out and "context ***" in out


def test_message_args_are_redacted(two_handlers) -> None:
    log, buffers = two_handlers
    register_secret(KEY)
    log.info("key %s used", KEY)
    for buf in buffers:
        assert buf.getvalue() == "key *** used\n"


def test_filter_is_idempotent_and_keeps_exc_info() -> None:
    register_secret(KEY)
    flt = RedactFilter()
    try:
        raise RuntimeError(KEY)
    except RuntimeError:
        record = logging.LogRecord("x", logging.ERROR, __file__, 1, "msg", (), __import__("sys").exc_info())
    assert flt.filter(record) is True
    first = record.exc_text
    assert first and KEY not in first and "***" in first
    assert record.exc_info is not None  # consumers like pytest's caplog still see the exception
    assert flt.filter(record) is True
    assert record.exc_text == first


def test_telegram_token_pattern_matches_the_bot_url_form() -> None:
    unregistered = "7654321:" + "B" * 35
    assert unregistered not in redact(f"https://api.telegram.org/bot{unregistered}/sendMessage")
    assert unregistered not in redact(f"token {unregistered}")
    assert "***" in redact(f"token {unregistered}")
    assert redact("plain text 12:34") == "plain text 12:34"
