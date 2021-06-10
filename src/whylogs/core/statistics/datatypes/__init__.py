"""
Define classes for tracking statistics for various data types
"""
from .floattracker import FloatTracker
from .integertracker import IntTracker
from .variancetracker import VarianceTracker

__ALL__ = [
    FloatTracker,
    IntTracker,
    VarianceTracker,
]
