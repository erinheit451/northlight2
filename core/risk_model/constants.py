"""
Constants for the risk assessment model
"""
import numpy as np

# ===== CONSTANTS & CONFIGURATION =====
# Global constants (single source of truth)
AVG_CYCLE = 30.4          # single source of truth for "a month"
GLOBAL_CR_PRIOR = 0.06    # 6% prior when CPC/CPL are missing/broken

# LTV mapping
CATEGORY_LTV_MAP = {
    'Attorneys & Legal Services': 149000, 'Physicians & Surgeons': 99000,
    'Automotive -- For Sale': 98000, 'Industrial & Commercial': 92000,
    'Home & Home Improvement': 88000, 'Health & Fitness': 84000,
    'Career & Employment': 81000, 'Finance & Insurance': 79000,
    'Business Services': 65000, 'Real Estate': 62000,
    'Education & Instruction': 55000, 'Sports & Recreation': 49000,
    'Automotive -- Repair, Service & Parts': 45000, 'Travel': 39000,
    'Personal Services (Weddings, Cleaners, etc.)': 31000,
    'Computers, Telephony & Internet': 29000, 'Farming & Agriculture': 25000,
    'Restaurants & Food': 12000, 'Beauty & Personal Care': 11000,
    'Community/Garage Sales': 11000, 'Animals & Pets': 10000,
    'Apparel / Fashion & Jewelry': 10000, 'Arts & Entertainment': 9000,
    'Religion & Spirituality': 8000, 'Government & Politics': 8000,
    'Toys & Hobbies': 8000, 'z - Other (Specify Keywords Below)': 40000
}
AVERAGE_LTV = float(np.mean(list(CATEGORY_LTV_MAP.values())))

# SAFE tolerances (explicit)
SAFE_CPL_TOLERANCE = 0.20      # within +20% of goal (<= 1.20x)
SAFE_PACING_MIN = 0.75          # utilization lower bound
SAFE_PACING_MAX = 1.25          # utilization upper bound
SAFE_LEAD_RATIO_MIN = 0.80      # >= 80% of expected leads-to-date
SAFE_MIN_LEADS = 3              # absolute floor when expected >= 1
SAFE_MIN_LEADS_TINY_EXP = 1     # absolute floor when expected < 1

# Dummy-proof SAFE policy toggles
SAFE_NEW_ACCOUNT_MONTHS        = 1           # <=1 IO month counts as "new"
SAFE_NEW_ACCOUNT_CPL_TOL       = 0.10        # new acct safe if CPL ≤ 1.10× goal ...
SAFE_NEW_ACCOUNT_MIN_LEADS     = 1           # ... OR has at least 1 lead
SAFE_NEW_ACCOUNT_IGNORE_PACING = True        # pacing/spend progress never vetoes SAFE for new accts
SAFE_DOWNWEIGHT_IN_UPI         = 0.05        # 5% weight for SAFE rows in UPI (strong suppression)
SAFE_MAX_FLARE_SCORE           = 15          # visual/raw clamp; SAFE can never exceed this

# Feature flags / temporary killswitches
UNDERFUNDED_FEATURE_ENABLED = False  # hard off until redesign
REQUIRE_ROLLING_30D_LEADS = True     # feature flag while we lack rolling-30d conversion history

# Guardrail for zero-leads & "meaningful spend"
MIN_SPEND_FOR_ZERO_LEAD = 250.0     # Use ONE constant everywhere we mean "we've actually spent enough to judge"

# Zero-lead gating (strong)
MIN_DAYS_FOR_ALERTS = 5             # keep this as the single floor for all zero-lead callouts
ZERO_LEAD_MIN_DAYS_EMERGING = MIN_DAYS_FOR_ALERTS  # align to global floor
ZERO_LEAD_MIN_EXPECTED_TD       = 1.0   # to-date plan must be >= 1 lead
ZERO_LEAD_MIN_SPEND_PROGRESS    = 0.50  # must be >=50% of ideal spend
ZERO_LEAD_LAST_MO_MIN_SPENDPROG = 0.70  # 30d zero requires ~70% progress

# SEM viability gates for zero-lead logic
SEM_VIABILITY_MIN_SEM = 2500.0                 # default min monthly for SEM viability
SEM_VIABILITY_MIN_DAILY_CLICKS = 3.0           # need ~3 clicks/day
SEM_VIABILITY_MIN_MONTHLY_LEADS = SAFE_MIN_LEADS  # need capacity for >=3 leads/mo
ZERO_LEAD_HR_ATTENUATION_LOW_BUDGET = 0.60     # if we ever apply zero-lead on non-viable budget, downweight

# CPL gradient tiers (from line ~430)
_CAL_HR = [0.042, 0.063, 0.092, 0.125, 0.164, 0.212, 0.273, 0.351, 0.453, 0.592, 0.789]
_CAL_CPL_TIERS = [0.5, 0.8, 1.0, 1.25, 1.5, 2.0, 3.0, 4.0, 6.0, 10.0, np.inf]

# Churn calibration constants
P0_BASELINE = 0.11  # Baseline churn probability

FALLBACK_HR = {
    # Tenure HRs intentionally neutral; baseline now handled by _tenure_baseline_p(...)
    "is_tenure_lte_1m": 1.00,
    "is_tenure_lte_3m": 1.00,
    "is_single_product": 1.60,
    "zero_lead_last_mo": 3.20,
}

# CPL gradient tiers (upper bound inclusive -> HR).
FALLBACK_CPL_TIERS = [
    (1.2, 1.05),  # mild lift for 1.0–1.2× to match driver labels
    (1.5, 1.20),
    (3.0, 1.60),
    (999, 2.40),  # ≥3x
]

# Optional: enable ONLY if your Budget Gradient Audit shows stable uplift
ENABLE_BUDGET_HR = False

# New CPL curve parameters (replaces FALLBACK_CPL_TIERS) - OUR FIXES
USE_SMOOTH_CPL_CURVE = True
CPL_HR_ALPHA = 0.55
CPL_HR_CAP = 1.8

# Updated single product hazard ratio - OUR FIXES
SINGLE_PRODUCT_HR = 1.35