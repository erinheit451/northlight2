"""
Churn probability calculation logic
"""
import pandas as pd
import numpy as np
import json
import os
from typing import Dict, Any, List, Optional

from ..constants import *
from ..utils import nz_num, safe_div, _ensure_columns

MODEL_VERSION = "risk-2025-09-18-SCOPE-FIXED"
EXPORTED_CONSTANTS = {
    "TENURE_BASELINES": {
        'LTE_90D': 0.11,  # CORRECTED
        'M3_6': 0.08,     # CORRECTED
        'GT_6M': 0.05
    },
    "SINGLE_PRODUCT_HR": SINGLE_PRODUCT_HR,  # Use updated constant (1.35)
    "USE_SMOOTH_CPL_CURVE": USE_SMOOTH_CPL_CURVE,
    "CPL_HR_ALPHA": CPL_HR_ALPHA,
    "CPL_HR_CAP": CPL_HR_CAP,
}


def _hr_from_cpl_ratio(r: float) -> float:
    """Get hazard ratio from CPL ratio using smooth curve or fallback tiers"""
    from ..constants import USE_SMOOTH_CPL_CURVE, CPL_HR_ALPHA, CPL_HR_CAP, FALLBACK_CPL_TIERS

    if USE_SMOOTH_CPL_CURVE:
        # Smooth exponential curve: HR = 1 + alpha * (cpl_ratio - 1)^2, capped at CPL_HR_CAP
        if r <= 1.0:
            return 1.0
        excess = r - 1.0
        hr = 1.0 + CPL_HR_ALPHA * (excess ** 2)
        return min(hr, CPL_HR_CAP)
    else:
        # Use the old FALLBACK_CPL_TIERS
        for ub, hr in FALLBACK_CPL_TIERS:
            if r <= ub:
                try:
                    return max(0.5, float(hr))
                except Exception:
                    return 1.0
        return 1.0


def _driver_label_for_cpl(r: float) -> str | None:
    """Get descriptive label for CPL ratio level"""
    if r >= 3.0: return "High CPL (≥3× goal)"
    if r >= 1.5: return "Elevated CPL (1.5–3×)"
    if r >= 1.2: return "CPL above goal (1.2–1.5×)"
    return None


def _tenure_baseline_p(tenure_bucket: str) -> float:
    """Get baseline churn probability by tenure bucket"""
    if tenure_bucket == 'LTE_90D':
        return 0.11  # 89% retention (first 90 days) - CORRECTED
    if tenure_bucket == 'M3_6':
        return 0.08  # 92% retention (3-6 months) - CORRECTED
    return 0.05      # >6m → 95% retention


def _collect_odds_factors_for_row(row) -> list[dict]:
    """Return multiplicative odds factors with labels, using cycle-based gates."""
    factors = []


    # Extract key metrics
    cpl_ratio = float(row.get('cpl_ratio', 1.0))

    # If cpl_ratio is default (1.0), recalculate from raw data
    if cpl_ratio == 1.0:
        cpl = float(row.get('running_cid_cpl', 0))
        goal = float(row.get('effective_cpl_goal', 0))
        if goal > 0 and cpl > 0:
            cpl_ratio = cpl / goal

    leads_raw = row.get('running_cid_leads', 0)
    leads = int(leads_raw) if pd.notna(leads_raw) else 0
    days = float(row.get('days_elapsed', 0))
    spend_prog = float(row.get('spend_progress', 0))
    sem_viable = bool(row.get('_sem_viable', False))

    # Get corrected expected leads and spend progress from DataFrame calculations
    budget = float(row.get('campaign_budget', 0))
    spend = float(row.get('amount_spent', 0))
    avg_cycle = float(row.get('avg_cycle_length', AVG_CYCLE)) or AVG_CYCLE

    # Recalculate expected leads using same logic as main function
    risk_cpl_goal = float(row.get('effective_cpl_goal', 0))
    if risk_cpl_goal <= 0:
        risk_cpl_goal = float(row.get('bsc_cpl_avg', 150))
    exp_month = max(budget / max(risk_cpl_goal, 1.0), 0)

    cycle_progress = min(days / avg_cycle, 1.0)
    exp_td_plan = max(exp_month * cycle_progress, 0.1)  # Floor at 0.1

    # Recalculate spend progress
    ideal_spend_td = max(budget, 10.0) * cycle_progress
    spend_prog = min(spend / max(ideal_spend_td, 1.0), 2.0)


    # CPL factor
    if cpl_ratio >= 1.2:
        hr = _hr_from_cpl_ratio(cpl_ratio)
        label = _driver_label_for_cpl(cpl_ratio)


        if label and hr > 1.0:
            factors.append({
                "name": label,
                "factor": hr,
                "controllable": True
            })


    # Lead deficit factors (using corrected calculations)
    if exp_td_plan >= 1:
        lead_ratio = min(leads / max(exp_td_plan, 0.1), 10.0)

        if lead_ratio <= 0.25 and spend_prog >= 0.5 and days >= 7 and sem_viable:
            factors.append({
                "name": "Severe lead deficit (≤25% of plan)",
                "factor": 2.8,
                "controllable": True
            })
        elif lead_ratio <= 0.50 and spend_prog >= 0.4 and days >= 5 and sem_viable:
            factors.append({
                "name": "Moderate lead deficit (≤50% of plan)",
                "factor": 1.6,
                "controllable": True
            })

    # Zero-lead factors
    zero_emerging = bool(row.get('zero_lead_emerging', False))
    zero_last_mo = bool(row.get('zero_lead_last_mo', False))

    if zero_emerging:
        factors.append({
            "name": "Zero leads (emerging)",
            "factor": 1.80,
            "controllable": True
        })

    if zero_last_mo:
        factors.append({
            "name": "Zero leads (30+ days)",
            "factor": 2.5,  # Using _CAL_HR value
            "controllable": True
        })

    # Structural factors
    single_product = bool(row.get('advertiser_product_count', 1) == 1)
    if single_product:
        factors.append({
            "name": "Single Product",
            "factor": SINGLE_PRODUCT_HR,  # Use consistent constant (1.35)
            "controllable": False
        })

    return factors


def _shap_pp_from_factors(base_p: float, factors: list[dict]) -> list[dict]:
    """Convert multiplicative factors to SHAP-style percentage points"""
    if not factors:
        return []

    # Calculate cumulative odds
    base_odds = base_p / (1 - base_p) if base_p < 1 else base_p
    cumulative_odds = base_odds

    result = []
    for factor in factors:
        old_p = cumulative_odds / (1 + cumulative_odds)
        cumulative_odds *= factor["factor"]
        new_p = cumulative_odds / (1 + cumulative_odds)

        pp_impact = (new_p - old_p) * 100

        result.append({
            "name": factor["name"],
            "points": round(pp_impact, 1),
            "is_controllable": factor.get("controllable", True),
            "explanation": f"Factor multiplier: {factor['factor']:.2f}x",
            "lift_x": float(factor["factor"])
        })

    return result


def _shap_pp_from_factors_with_total(base_p: float, factors: list[dict], total_pct: float) -> list[dict]:
    """Convert multiplicative factors to SHAP-style percentage points, ensuring they sum to total_pct"""
    if not factors:
        return []

    # First, calculate unclamped drivers
    drivers = _shap_pp_from_factors(base_p, factors)

    # Calculate the theoretical total from drivers
    baseline_pp = base_p * 100
    driver_sum = sum(d["points"] for d in drivers)
    theoretical_total = baseline_pp + driver_sum

    # Target total from input
    target_total = total_pct * 100

    # If there's a mismatch, proportionally adjust drivers to reconcile
    if abs(theoretical_total - target_total) > 0.1 and driver_sum != 0:  # 0.1pp tolerance
        adjustment_factor = (target_total - baseline_pp) / driver_sum
        for driver in drivers:
            driver["points"] = round(driver["points"] * adjustment_factor, 1)

    return drivers


def _is_sliding_to_zero(cplr, leads, days_elapsed, spend_prog):
    """Detect if account is sliding toward zero performance"""
    # Handle both scalar and Series inputs
    import pandas as pd
    import numpy as np

    if isinstance(leads, pd.Series):
        # Vectorized for Series
        condition1 = (leads == 0) & (days_elapsed >= 14) & (spend_prog >= 0.5)
        condition2 = (cplr >= 3.0) & (leads <= 2) & (days_elapsed >= 10)
        return condition1 | condition2
    else:
        # Scalar logic (original)
        if leads == 0 and days_elapsed >= 14 and spend_prog >= 0.5:
            return True
        if cplr >= 3.0 and leads <= 2 and days_elapsed >= 10:
            return True
        return False


def _tenure_bucket_from_row(row):
    """Get tenure bucket for baseline calculation - use pre-calculated if available"""
    # Use pre-calculated tenure bucket from DataFrame if available
    if '_tenure_bucket' in row and pd.notna(row['_tenure_bucket']):
        return row['_tenure_bucket']

    # Fallback to calculation for backward compatibility
    io_cycle = float(row.get('io_cycle', 1))
    days_elapsed = float(row.get('days_elapsed', 0))
    avg_cycle = float(row.get('avg_cycle_length', AVG_CYCLE)) or AVG_CYCLE

    total_days = ((io_cycle - 1) * avg_cycle + days_elapsed) if io_cycle > 1 else days_elapsed
    total_months = total_days / 30.0

    if total_months <= 3.0:
        return 'LTE_90D'
    elif total_months <= 6.0:
        return 'M3_6'
    else:
        return 'GT_6'


def _is_actually_performing(df: pd.DataFrame) -> pd.Series:
    """
    GOLDEN RULE: Identifies campaigns that are clearly performing well.
    This is the ONLY function you need to replace.
    """
    result = pd.Series(False, index=df.index)

    # Basic safety checks
    if 'running_cid_leads' not in df.columns:
        return result

    # Get core metrics (vectorized)
    leads = nz_num(df.get('running_cid_leads'))
    actual_cpl = nz_num(df.get('running_cid_cpl'), 999)
    spent = nz_num(df.get('amount_spent'))
    days_active = nz_num(df.get('days_elapsed'))

    # Get benchmark and goals (vectorized)
    benchmark = nz_num(df.get('bsc_cpl_avg'), 150)
    advertiser_goal = nz_num(df.get('cpl_goal'))

    # Check for zero lead issues
    zero_issues = (
        df.get('zero_lead_last_mo', pd.Series(False, index=df.index)).fillna(False) |
        df.get('zero_lead_emerging', pd.Series(False, index=df.index)).fillna(False)
    )

    # SIMPLE RULES FOR SAFE:
    # 1. Early winner: < 7 days but good performance
    early_winner = (
        (days_active <= 7) &
        (days_active >= 2) &
        (spent >= 500) &
        (leads >= 3) &
        (actual_cpl <= benchmark * 2.0) &
        ~zero_issues
    )

    # 2. Remove absurd_goal_but_performing as SAFE path - surface as Goal Misaligned instead
    # absurd_goal_but_performing = False  # Disabled - will be handled elsewhere

    # 3. Standard good performance - now requires meeting goal AND decent volume
    effective_goal = df.get('effective_cpl_goal', advertiser_goal).fillna(advertiser_goal)
    lead_ratio = df.get('_lead_ratio', 0.0).fillna(0.0)

    standard_good = (
        (actual_cpl <= effective_goal * 1.1) &  # Within 10% of goal, not just benchmark
        (lead_ratio >= 0.8) &  # At least 80% of expected leads
        (days_active >= 10) &  # Enough data for confidence
        (leads >= 3) &  # Minimum volume threshold
        ~zero_issues
    )

    # 4. Obviously excellent (regardless of other factors)
    obviously_excellent = (
        (actual_cpl <= benchmark * 0.5) &  # Half the benchmark cost
        (leads >= 10) &  # Good volume
        ~zero_issues
    )

    # 5. New and thriving: require better lead performance to avoid zero-lead SAFE
    SAFE_MIN_LEADS_TINY_EXP = 1  # Minimum leads for very new accounts
    new_and_thriving = (
        (days_active < 30) &  # New campaigns only
        (days_active >= 5) &  # Need at least 5 days of data
        ((lead_ratio >= 0.6) | (leads >= SAFE_MIN_LEADS_TINY_EXP)) &  # Better volume requirement
        (actual_cpl <= benchmark * 0.8) &  # Better than 80% of benchmark (stricter than "good")
        (spent >= 300) &  # Meaningful spend requirement
        ~zero_issues
    )

    # 6. New with excellent efficiency: captures very efficient new campaigns regardless of volume
    new_excellent_efficiency = (
        (days_active < 30) &  # New campaigns only
        (days_active >= 3) &  # Minimum data requirement
        (leads >= 1) &  # At least some conversion
        (actual_cpl <= benchmark * 0.7) &  # Exceptional efficiency (like -83%, -95% examples)
        (spent >= 100) &  # Minimal spend threshold to show real activity
        ~zero_issues
    )

    # 7. Goal performance: meeting or beating goal significantly (the key fix!)
    goal_performance = (
        (advertiser_goal.notna()) &  # Must have a goal set
        (advertiser_goal > 0) &  # Goal must be positive
        (actual_cpl <= advertiser_goal * 0.8) &  # Beating goal by 20%+ (like -83%, -95% examples)
        (leads >= 1) &  # At least some conversion
        ~zero_issues
    )

    # Mark as SAFE if ANY condition is met (removed absurd_goal_but_performing)
    result = early_winner | standard_good | obviously_excellent | new_and_thriving | new_excellent_efficiency | goal_performance

    return result


def calculate_churn_probability(df: pd.DataFrame) -> pd.DataFrame:
    """
    90d churn via odds stacking + pragmatic SAFE override that matches 'performing'.
    Key feature: SAFE accounts get churn clamped to baseline to prevent false alarms.
    """
    df = df.copy()

    # Ensure columns exist (vectorized)
    required_cols = ['io_cycle','advertiser_product_count','running_cid_leads','days_elapsed',
                     'running_cid_cpl','effective_cpl_goal','campaign_budget','amount_spent',
                     'expected_leads_monthly','expected_leads_to_date','expected_leads_to_date_spend',
                     'utilization','cpl_goal','bsc_cpc_average']
    df = _ensure_columns(df, required_cols)

    # Keep true runtime for display only (safer approach)
    rt_days_col = df.get('true_days_running')
    if rt_days_col is None or (hasattr(rt_days_col, 'isnull') and rt_days_col.isnull().all()):
        io_f  = nz_num(df.get('io_cycle'))
        avg_f = nz_num(df.get('avg_cycle_length'), AVG_CYCLE)
        days_f= nz_num(df.get('days_elapsed'))
        rt_days = ((io_f - 1).clip(lower=0) * avg_f + days_f).clip(lower=0.0)
    else:
        rt_days = nz_num(rt_days_col)

    # Always work off cycle-to-date for short-horizon risk (vectorized)
    days    = nz_num(df.get('days_elapsed')).astype(float)
    leads   = nz_num(df.get('running_cid_leads')).astype(float)
    spend   = nz_num(df.get('amount_spent')).astype(float)
    budget  = nz_num(df.get('campaign_budget'))
    avg_len_raw = nz_num(df.get('avg_cycle_length'), AVG_CYCLE)
    if isinstance(avg_len_raw, pd.Series):
        avg_len = avg_len_raw.replace(0, AVG_CYCLE)
    else:
        avg_len = avg_len_raw if avg_len_raw > 0 else AVG_CYCLE

    # Calculate expected leads properly (vectorized)
    # Monthly expectation: budget / CPL goal (with fallback to benchmark)
    risk_cpl_goal = nz_num(df.get('effective_cpl_goal')).replace(0, np.nan)
    risk_cpl_goal = risk_cpl_goal.fillna(nz_num(df.get('bsc_cpl_avg'), 150))  # Fallback to benchmark
    exp_month = (budget / np.maximum(risk_cpl_goal, 1.0)).clip(0, 999)  # Guard denominator

    # Expected to-date: proportional to days elapsed in cycle
    cycle_progress = np.minimum(days / avg_len, 1.0)  # Cap at 100% of cycle
    exp_td_plan = (exp_month * cycle_progress).clip(0.1, 999)  # Floor at 0.1 to avoid zero denominators

    # CPL ratio (neutral fallback = 1.0, not 0.0) - vectorized
    eff_goal = nz_num(df.get('effective_cpl_goal')).replace(0, np.nan)
    cpl      = nz_num(df.get('running_cid_cpl'))
    df['cpl_ratio'] = safe_div(cpl, eff_goal, fill=1.0)  # neutral, avoids accidental "good CPL" when goal is missing

    # SEM viability (vectorized)
    cpc_safe     = nz_num(df.get('bsc_cpc_average'), 3.0)
    daily_budget = budget / avg_len
    daily_clicks = safe_div(daily_budget, cpc_safe, fill=0.0)

    budget_ok = (budget >= SEM_VIABILITY_MIN_SEM)
    clicks_ok = (daily_clicks >= SEM_VIABILITY_MIN_DAILY_CLICKS)
    volume_ok = (exp_month >= SEM_VIABILITY_MIN_MONTHLY_LEADS)

    df['_viab_budget_ok'] = budget_ok
    df['_viab_clicks_ok'] = clicks_ok
    df['_viab_volume_ok'] = volume_ok

    sem_viable = budget_ok | clicks_ok | volume_ok
    df['_sem_viable'] = sem_viable

    # Calculate tenure buckets for baseline calculation (used in _row_bundle)
    ten_b = pd.cut(
        (((pd.to_numeric(df.get('io_cycle'), errors='coerce').fillna(0.0)-1).clip(lower=0)*avg_len + days)/30.0).round(1).fillna(0.0),
        bins=[-0.001,3.0,6.0,9999],
        labels=['LTE_90D','M3_6','GT_6']
    ).astype('string').fillna('GT_6')
    df['_tenure_bucket'] = ten_b  # Store for _row_bundle to use

    # Store vectorized calculations for _row_bundle to use (MOVED UP)
    lead_ratio = (leads / np.maximum(exp_td_plan, 0.1)).clip(0, 10.0)  # Calculate lead_ratio here
    # Recalculate spend progress here (was calculated earlier but out of scope)
    ideal_spend_td = np.maximum(budget, 10.0) * cycle_progress
    spend_prog = (spend / np.maximum(ideal_spend_td, 1.0)).clip(0, 2.0)

    df['_lead_ratio'] = lead_ratio
    df['_exp_td_plan'] = exp_td_plan  # Store corrected expected leads
    df['_spend_prog'] = spend_prog    # Store corrected spend progress
    df['spend_progress'] = df['_spend_prog']  # KEY FIX: ensure row-level collector sees the right key

    # Calculate zero-lead flags AFTER spend_prog is defined
    rt = pd.to_numeric(rt_days, errors='coerce').fillna(days)

    # Check for rolling 30-day leads data
    roll30_data = pd.to_numeric(df.get('leads_rolling_30d'), errors='coerce')
    if roll30_data is None:
        roll30_data = pd.Series([np.nan] * len(df), index=df.index)
    elif not isinstance(roll30_data, pd.Series):
        roll30_data = pd.Series([roll30_data] * len(df), index=df.index)

    has_roll30 = roll30_data.notna()
    roll30_zero = roll30_data.fillna(999) == 0

    df['zero_lead_last_mo'] = (
        (leads == 0) &                    # keep current-cycle zero as a sanity check
        (days >= 30) &                    # require >=30d in the SAME cycle window (no rt fallback)
        (spend >= MIN_SPEND_FOR_ZERO_LEAD) &
        (spend_prog >= ZERO_LEAD_LAST_MO_MIN_SPENDPROG) &  # Now spend_prog is in scope
        sem_viable &
        (
            (~REQUIRE_ROLLING_30D_LEADS)  # or
            | (has_roll30 & roll30_zero)  # use it when present
        )
    )

    # Emerging zero-lead (cycle 5–29d), gated by plan+spend+viability
    df['zero_lead_emerging'] = (
        (leads == 0) &
        (days >= MIN_DAYS_FOR_ALERTS) & (days < 30) &
        (spend >= MIN_SPEND_FOR_ZERO_LEAD) &
        (exp_td_plan >= ZERO_LEAD_MIN_EXPECTED_TD) &
        (spend_prog >= ZERO_LEAD_MIN_SPEND_PROGRESS) &  # Now spend_prog is in scope
        sem_viable
    )

    # Idle (paused/near-zero spend) variant: same day floor, not a performance crisis
    df['zero_lead_idle'] = (
        (leads == 0) &
        (days >= MIN_DAYS_FOR_ALERTS) &                 # aligned (was 7)
        (spend < MIN_SPEND_FOR_ZERO_LEAD)
    )

    # NOTE: Removed old vectorized odds calculation - now using unified _row_bundle approach

    df['is_safe'] = _is_actually_performing(df)

    # UNIFIED: Single-pass probability and driver calculation
    def _row_bundle(row):
        # 1) Baseline → odds
        tb = _tenure_bucket_from_row(row)
        base_p = float(np.clip(_tenure_baseline_p(tb), 0.01, 0.95))
        odds = base_p / (1 - base_p)

        # 2) Collect *exactly the same* factors that power the drivers
        factors = _collect_odds_factors_for_row(row)

        # 3) Multiply odds by all factor multipliers
        for f in factors:
            odds *= float(f.get("factor", 1.0))

        # 4) Probabilities
        p_unclamped = float(odds / (1.0 + odds))
        is_safe = bool(row.get("is_safe", False))
        p_clamped = min(p_unclamped, base_p) if is_safe else p_unclamped

        # 5) Drivers from the *same* factors; also add lift_x for UI
        drivers = _shap_pp_from_factors(base_p, factors)
        for d, f in zip(drivers, factors):
            d["lift_x"] = float(f.get("factor", 1.0))

        # 6) Assertion to catch driver/probability mismatches
        base_pp = int(round(base_p * 100))
        driver_sum = sum(round(d["points"]) for d in drivers)
        theoretical_total = base_pp + driver_sum
        target_total = int(round(p_unclamped * 100))

        if abs(theoretical_total - target_total) > 1:
            cid = row.get('campaign_id', 'unknown')
            raise ValueError(f"ASSERTION FAILED for CID {cid}: Driver sum {theoretical_total}% differs from unclamped target {target_total}% by more than ±1pp")

        return {
            "base_p": base_p,
            "p_unclamped": p_unclamped,
            "p_clamped": p_clamped,
            "drivers": drivers,
            "is_safe": is_safe,
        }

    # Apply unified calculation
    bundle = df.apply(_row_bundle, axis=1)
    df["churn_prob_90d_unclamped"] = bundle.map(lambda b: b["p_unclamped"])
    df["churn_prob_90d"] = bundle.map(lambda b: b["p_clamped"])

    df['revenue_at_risk'] = (budget * df['churn_prob_90d']).fillna(0.0)

    # Updated churn bands
    df['churn_risk_band'] = pd.cut(
        df['churn_prob_90d'],
        bins=[0, 0.15, 0.30, 0.45, 1.01],
        labels=['LOW','MEDIUM','HIGH','CRITICAL'],
        right=True
    ).astype(str).fillna('LOW')

    # Build risk_drivers_json from the same bundle
    df['risk_drivers_json'] = bundle.map(lambda b: {
        "baseline": int(round(b["base_p"] * 100)),
        "drivers": b["drivers"],
        "p_unclamped_pct": int(round(b["p_unclamped"] * 100)),
        "p_clamped_pct": int(round(b["p_clamped"] * 100)),
        "is_safe": b["is_safe"],
        "safe_clamped": b["is_safe"] and (b["p_unclamped"] > b["p_clamped"] + 0.01),
        "model_version": MODEL_VERSION,
        "constants_used": {
            "single_product_hr": SINGLE_PRODUCT_HR,
            "cpl_hr_alpha": CPL_HR_ALPHA,
            "cpl_hr_cap": CPL_HR_CAP,
            "use_smooth_curve": USE_SMOOTH_CPL_CURVE
        }
    })

    # Add model version for debugging
    df['risk_model_version'] = MODEL_VERSION

    # Add expected leads fields for frontend pacing calculations
    df['expected_leads_monthly'] = exp_month
    df['expected_leads_to_date'] = exp_td_plan

    # REMOVED: Post-processing repair was a band-aid for the spend_progress key mismatch
    # Now that gating bugs are fixed, drivers should calculate correctly the first time
    # Keeping the assertion below to catch any remaining issues

    # df['risk_drivers_json'] = df.apply(fix_broken_drivers, axis=1)  # DISABLED - no longer needed

    # Backend assertion to prevent regression
    def _assert_driver_sum_matches(df: pd.DataFrame):
        # Allow ±1pp rounding slack
        base = df["risk_drivers_json"].apply(lambda r: r["baseline"])
        drv_sum = df["risk_drivers_json"].apply(lambda r: sum(int(round(d["points"])) for d in r["drivers"]))
        puncl = df["risk_drivers_json"].apply(lambda r: r["p_unclamped_pct"])
        delta = (base + drv_sum) - puncl
        bad = delta.abs() > 1
        if bad.any():
            rows = df[bad][["campaign_id","risk_drivers_json"]].head(5)
            raise AssertionError(f"Driver sum mismatch vs p_unclamped_pct: {rows.to_dict(orient='records')}")

    _assert_driver_sum_matches(df)
    return df