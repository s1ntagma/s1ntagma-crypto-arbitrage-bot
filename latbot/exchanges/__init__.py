"""Exchange adapters.

``base`` is imported eagerly (ABC + typed errors).  The concrete adapters and
the registry are exported lazily (PEP 562) because ``ccxt.pro`` takes about a
second to import and most modules only need the base contract.
"""
from __future__ import annotations

import importlib
from typing import Any

from .base import ExchangeAdapter, ExchangeError, InsufficientBalance, OrderRejected, RateLimited, StaleData

_LAZY: dict[str, str] = {
    "CcxtAdapter": ".ccxt_adapter",
    "PaperAdapter": ".paper",
    "FakeAdapter": ".fake",
    "build_market_adapters": ".registry",
    "build_execution_adapters": ".registry",
    "resolve_watchlist": ".registry",
    "resolve_universe": ".registry",
}

__all__ = [
    "ExchangeAdapter",
    "ExchangeError",
    "InsufficientBalance",
    "OrderRejected",
    "RateLimited",
    "StaleData",
]
__all__ += sorted(_LAZY)  # lazily provided by __getattr__ below


def __getattr__(name: str) -> Any:
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getattr(importlib.import_module(module, __name__), name)
