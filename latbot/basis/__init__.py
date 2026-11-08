"""Spot-Futures Arbitrage Scanner: a research module, virtual only - it never sends an order.

* ``scanner.BasisScanner`` - pure logic: books in, basis / conditions / simulated pairs / statistics out.
* ``runner.BasisRunner``   - live data: spot and perpetual order books per venue (``CcxtAdapter`` on a
  private event bus, so the latency bot never sees them), funding rates, the simulator clock.
"""
from .scanner import BasisScanner

__all__ = ["BasisScanner"]
