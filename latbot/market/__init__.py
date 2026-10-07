"""Market data layer: latest books / tickers / trades per exchange and asset,
mid-price series and the pure feature functions used by the detector."""
from .features import move_over_window, velocity_pct_s, volume_acceleration
from .state import MarketState

__all__ = ["MarketState", "move_over_window", "velocity_pct_s", "volume_acceleration"]
