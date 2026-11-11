"""latbot - real-time cross-exchange latency trading terminal.

Core principle: BUY and SELL happen on ONE target exchange while a price lag
(detected via other exchanges) still exists.  No transfer arbitrage.
"""

VERSION = "1.0.0"
