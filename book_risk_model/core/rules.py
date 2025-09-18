"""
Core business rules and logic
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List

from ..constants import *
from ..utils import nz_num, safe_div


def assess_goal_quality(df):
    """Assess if CPL goals are realistic based on vertical benchmarks"""
    vertical_medians = df['bsc_cpl_avg']
    conditions = [
        df['cpl_goal'].isnull() | (df['cpl_goal'] == 0),
        df['cpl_goal'] < (vertical_medians * 0.5),
        df['cpl_goal'] > (vertical_medians * 1.5),
    ]
    return np.select(conditions, ['missing', 'too_low', 'too_high'], default='reasonable')


def calculate_expected_leads(df):
    """Calculate robust expected leads with sane fallbacks."""
    budget = pd.to_numeric(df['campaign_budget'], errors='coerce').fillna(0.0)
    days   = pd.to_numeric(df['days_elapsed'], errors='coerce').fillna(0.0)
    spent  = pd.to_numeric(df['amount_spent'], errors='coerce').fillna(0.0)

    # Use the risk_cpl_goal for stable expected leads calculation (prevents churn inflation)
    target_cpl = pd.to_numeric(df['risk_cpl_goal'], errors='coerce').fillna(150.0)
    bench = pd.to_numeric(df['bsc_cpl_avg'], errors='coerce').fillna(150.0)

    # Benchmark CR with guardrails; if CPC missing, we'll still have CPL-only fallback
    bsc_cpc_safe = pd.to_numeric(df['bsc_cpc_average'], errors='coerce')
    benchmark_cr = (bsc_cpc_safe / bench)
    benchmark_cr = benchmark_cr.where(np.isfinite(benchmark_cr) & (benchmark_cr > 0), GLOBAL_CR_PRIOR).clip(0.01, 0.25)

    # Primary path: clicks via CPC, else fallback via CPL
    expected_clicks = budget / bsc_cpc_safe
    expected_clicks = expected_clicks.where(np.isfinite(expected_clicks), np.nan)

    # Leads per month: clicks * CR, fallback to budget/target_cpl if CPC is junk
    target_cpl_safe = pd.Series(target_cpl, index=df.index).replace(0, np.nan)
    expected_leads_monthly = (expected_clicks * benchmark_cr).where(expected_clicks.notna(),
                               budget / target_cpl_safe)
    pacing = np.clip(days / AVG_CYCLE, 0.0, 2.0)

    df['expected_leads_to_date'] = (expected_leads_monthly * pacing).fillna(0.0)
    df['expected_leads_to_date_spend'] = np.where(target_cpl > 0, spent / target_cpl, 0.0)

    return pd.Series(np.clip(expected_leads_monthly.fillna(0.0), 0.0, 1e6), index=df.index)


def preprocess_campaign_data(campaign_df: pd.DataFrame) -> pd.DataFrame:
    """
    Data validation and preprocessing for campaign risk analysis.
    Ensures required columns exist, performs data type coercion, and sanitizes utilization.
    """
    df = campaign_df.copy()

    # Ensure required columns exist
    required_cols = [
        "am", "optimizer", "gm", "partner_name", "advertiser_name", "campaign_name", "bid_name",
        "io_cycle", "campaign_budget", "running_cid_leads", "utilization", "cpl_goal",
        "bsc_cpl_avg", "running_cid_cpl", "amount_spent", "days_elapsed", "bsc_cpc_average",
        "advertiser_product_count"
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = np.nan

    # Add is_cpl_goal_missing flag
    df['is_cpl_goal_missing'] = df['cpl_goal'].isnull() | (df['cpl_goal'] == 0)

    # Data Coercion
    for col in ['io_cycle','campaign_budget','running_cid_leads','cpl_mcid','utilization',
                'bsc_cpl_avg','running_cid_cpl','amount_spent','days_elapsed','bsc_cpc_average']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Sanitize utilization
    sanitized_util = df['utilization'].apply(lambda x: x / 100 if pd.notna(x) and x >= 3 else x)
    total_days_in_cycle = (df['io_cycle'] * AVG_CYCLE).replace(0, np.nan).fillna(AVG_CYCLE)
    ideal_spend_to_date = (df['campaign_budget'] / total_days_in_cycle) * df['days_elapsed'].fillna(0)
    fallback_util = (df['amount_spent'] / ideal_spend_to_date.replace(0, np.nan)).clip(lower=0.0, upper=2.0)
    df['utilization'] = pd.Series(
        np.where((sanitized_util > 0) & (sanitized_util <= 2.0), sanitized_util, fallback_util),
        index=df.index
    ).fillna(0)

    # IO-based risk removed; maturity amplifier neutralized
    df['age_risk'] = 0
    df['maturity_amplifier'] = 1.0

    return df


def categorize_issues(df):
    """Categorize the primary issue for each account"""
    categories = []
    for _, row in df.iterrows():
        if row['running_cid_leads'] == 0 and row['amount_spent'] > 100:
            categories.append('CONVERSION_FAILURE')
        elif pd.notna(row.get('cpl_variance_pct')) and row['cpl_variance_pct'] > 200:
            categories.append('EFFICIENCY_CRISIS')
        elif row.get('unified_performance_score', 0) >= 6:
            categories.append('PERFORMANCE_ISSUE')
        elif row['maturity_amplifier'] >= 1.8:
            categories.append('NEW_ACCOUNT')
        elif row['utilization'] < 0.5:
            categories.append('UNDERPACING')
        elif pd.notna(row.get('cpl_variance_pct')) and row['cpl_variance_pct'] < -20:
            categories.append('PERFORMING')
        else:
            categories.append('MONITORING')
    return categories


def process_campaign_goals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Goal processing and substitution with three-goal system.
    Implements display, operating, and risk assessment goals for different purposes.
    """
    df = df.copy()

    df['issue_category'] = categorize_issues(df)
    df['goal_quality'] = assess_goal_quality(df)

    # System goal for absurdly low targets
    median_cpl = pd.to_numeric(df['bsc_cpl_avg'], errors='coerce')
    raw_goal   = pd.to_numeric(df['cpl_goal'], errors='coerce')
    gq         = df['goal_quality'].astype(str)

    # Three-goal system:
    # 1. Display goal (cpl_goal) - always original, shown in UI
    # 2. Operating goal (effective_cpl_goal) - for business logic, only substitute too_low/missing
    # 3. Risk assessment goal (risk_cpl_goal) - for churn/FLARE, substitute all unrealistic goals

    # Operating goal: only substitute missing/too_low (for Goal Realism UI)
    substitute_low_goal = gq.isin(['missing','too_low'])
    operating_cpl_goal = np.where(
        substitute_low_goal,
        median_cpl,                                # use p50 for missing/too_low
        raw_goal                                   # keep original for reasonable/too_high
    )
    df['effective_cpl_goal'] = pd.to_numeric(operating_cpl_goal, errors='coerce')

    # Risk assessment goal: substitute ALL unrealistic goals (for stable churn calculations)
    substitute_risk_goal = gq.isin(['missing','too_low','too_high'])
    risk_assessment_goal = np.where(
        substitute_risk_goal,
        median_cpl,                                # use p50 for all bad goals
        np.clip(raw_goal, 0.8 * median_cpl, 1.2 * median_cpl)  # clamp reasonable goals
    )
    df['risk_cpl_goal'] = pd.to_numeric(risk_assessment_goal, errors='coerce')

    # Track substitutions for UI transparency
    df['goal_was_substituted'] = substitute_low_goal

    # Recompute deltas
    df['cpl_delta'] = df['running_cid_cpl'] - df['effective_cpl_goal']
    df['cpl_variance_pct'] = np.where(
        df['effective_cpl_goal'] > 0,
        ((df['running_cid_cpl'] / df['effective_cpl_goal']) - 1) * 100,
        0
    )

    return df