"""Market data layer: latest books / tickers / trades per exchange and asset,
mid-price series and the pure feature functions used by the detector."""
from .features import move_over_window, velocity_pct_s, volume_acceleration
from .poller import TickerPoller
from .state import MarketState

__all__ = ["MarketState", "TickerPoller", "move_over_window", "velocity_pct_s", "volume_acceleration"]
