"""
Risk Model Package

A modular risk assessment system for campaign performance analysis.
"""

# Core constants and utilities - always available
from .constants import *
from .utils import *

# Core logic
from .core.churn import (
    calculate_churn_probability,
    _is_actually_performing,
    _hr_from_cpl_ratio,
    _tenure_baseline_p,
    _collect_odds_factors_for_row,
    _shap_pp_from_factors,
    _is_sliding_to_zero,
    _tenure_bucket_from_row
)

from .core.flare import (
    attach_priority_and_flare,
    compute_priority_v2,
    _percentile_score
)

from .core.rules import (
    assess_goal_quality,
    calculate_expected_leads,
    preprocess_campaign_data,
    categorize_issues,
    process_campaign_goals
)

# Presentation layer
from .presentation.waterfall import build_churn_waterfall
from .presentation.diagnostics import (
    generate_headline_diagnosis,
    generate_diagnosis_pills,
    _goal_advice_for_row
)

# Version info
__version__ = "1.0.0"
__status__ = "Development"

# Public API
__all__ = [
    # Constants
    'AVG_CYCLE', 'GLOBAL_CR_PRIOR', 'SAFE_MIN_LEADS', 'CATEGORY_LTV_MAP',

    # Utilities
    'nz_num', 'safe_div', 'coalesce', '_ensure_columns',

    # Core churn functions
    'calculate_churn_probability', '_is_actually_performing',
    '_hr_from_cpl_ratio', '_tenure_baseline_p',

    # Core FLARE functions
    'attach_priority_and_flare', 'compute_priority_v2',

    # Core rules functions
    'assess_goal_quality', 'calculate_expected_leads', 'preprocess_campaign_data',
    'categorize_issues', 'process_campaign_goals',

    # Presentation
    'build_churn_waterfall', 'generate_headline_diagnosis',
    'generate_diagnosis_pills', '_goal_advice_for_row',
]