"""Logging with secret redaction.

Never log API keys / secrets / Telegram tokens.  A RedactFilter scrubs any
configured secret value and common key/token patterns from every record.
"""
from __future__ import annotations

import logging
import logging.handlers
import os
import re
from pathlib import Path

_SECRET_VALUES: set[str] = set()
_PATTERNS = [
    re.compile(r"(?i)(api[_-]?key|api[_-]?secret|secret|passphrase|token)(\s*[=:]\s*)([^\s,;'\"]{6,})"),
    # telegram bot token - also inside the API URL (".../bot<id>:<hash>/sendMessage"):
    # there is no word boundary between "bot" and the digits, so none is required.
    re.compile(r"\d{6,}:[A-Za-z0-9_-]{30,}"),
]
_EXC_FORMATTER = logging.Formatter()  # stateless; renders exc_info the way every handler would


def register_secret(value: str | None) -> None:
    if value and len(value) >= 6:
        _SECRET_VALUES.add(value)


def redact(text: str) -> str:
    for v in _SECRET_VALUES:
        if v in text:
            text = text.replace(v, "***")
    for p in _PATTERNS:
        text = p.sub(lambda m: f"{m.group(1)}{m.group(2)}***" if m.lastindex and m.lastindex >= 2 else "***", text)
    return text


class RedactFilter(logging.Filter):
    """Scrub secrets from the message, the traceback and the stack of a record.

    ``Handler.handle`` runs filters BEFORE ``Formatter.format``, and the
    formatter renders ``exc_info`` into ``record.exc_text`` only then - so the
    traceback is rendered (and redacted) here, up front.  ``Formatter.format``
    reuses a pre-set ``exc_text``, hence every handler - the console first,
    the file second - prints the redacted text and the output format is
    unchanged.  ``exc_info`` is left in place for consumers that inspect it.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:  # noqa: BLE001
            return True
        red = redact(msg)
        if red != msg:
            record.msg = red
            record.args = ()
        if record.exc_info and not record.exc_text:
            try:
                record.exc_text = _EXC_FORMATTER.formatException(record.exc_info)
            except Exception:  # noqa: BLE001
                record.exc_text = None
        if record.exc_text:
            record.exc_text = redact(record.exc_text)
        if record.stack_info:
            record.stack_info = redact(record.stack_info)
        return True


def setup_logging(log_dir: str | os.PathLike[str] = "logs", level: str = "INFO") -> None:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    for h in list(root.handlers):
        root.removeHandler(h)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    flt = RedactFilter()

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    console.addFilter(flt)
    root.addHandler(console)

    fh = logging.handlers.RotatingFileHandler(
        Path(log_dir) / "console.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    fh.addFilter(flt)
    root.addHandler(fh)

    for noisy in ("ccxt", "websockets", "uvicorn.access", "aiohttp"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
