"""
Risk Model Package for Unified Northlight
Contains churn probability calculations, FLARE scoring, and waterfall presentations
"""

from .constants import *
from .utils import *
from .churn import calculate_churn_for_campaign
from .waterfall import build_churn_waterfall

__version__ = "1.0.0"