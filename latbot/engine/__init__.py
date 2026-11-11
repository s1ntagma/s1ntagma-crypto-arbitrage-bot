"""Detection and decision engine: first mover / lagging exchange detection,
real-book economics, duration, executability lights, WHY text, the
opportunity state machine and the pipeline that wires them together."""
from .duration import DurationEstimator
from .economics import compute_economics, depth_within, walk_book
from .executability import assess_bot, assess_human
from .explain import build_why
from .mover import MoveDetector
from .opportunity import OpportunityManager
from .pipeline import Pipeline
from .stats import StatsStore

__all__ = [
    "DurationEstimator",
    "MoveDetector",
    "OpportunityManager",
    "Pipeline",
    "StatsStore",
    "assess_bot",
    "assess_human",
    "build_why",
    "compute_economics",
    "depth_within",
    "walk_book",
]
