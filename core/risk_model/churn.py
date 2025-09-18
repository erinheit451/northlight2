"""
Churn probability calculation logic - with unified-northlight fixes
"""
import pandas as pd
import numpy as np
import json
from typing import Dict, Any, List, Optional

from .constants import *
from .utils import nz_num, safe_div, _ensure_columns

MODEL_VERSION = "risk-2025-09-17d-UNIFIED"


def _hr_from_cpl_ratio(r: float) -> float:
    """Get hazard ratio from CPL ratio using smooth curve or fallback tiers"""
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
        return 0.09  # 91% retention (first 90 days)
    if tenure_bucket == 'M3_6':
        return 0.06  # 94% retention (3-6 months)
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

    # Lead deficit factors
    exp_td_plan = float(row.get('expected_leads_to_date', 0))
    if exp_td_plan >= 1:
        lead_ratio = leads / exp_td_plan if exp_td_plan > 0 else 1.0

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
            "factor": 2.5,
            "controllable": True
        })

    # Structural factors - USE NEW CONSTANT
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


def _tenure_bucket_from_row(row):
    """Get tenure bucket from row data"""
    days = float(row.get('days_elapsed', 0))
    io_cycle = float(row.get('io_cycle', 1))

    # Estimate total tenure in days
    total_days = (io_cycle - 1) * AVG_CYCLE + days
    tenure_months = total_days / 30.0

    if tenure_months <= 3.0:
        return 'LTE_90D'
    elif tenure_months <= 6.0:
        return 'M3_6'
    else:
        return 'GT_6'


async def calculate_churn_for_campaign(campaign_data: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate churn probability for a single campaign (async version)"""

    # Convert dict to pandas Series for compatibility
    row = pd.Series(campaign_data)

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
        "churn_prob_90d": p_clamped,
        "churn_prob_90d_unclamped": p_unclamped,
        "risk_drivers_json": {
            "baseline": int(round(base_p * 100)),
            "drivers": drivers,
            "p_unclamped_pct": int(round(p_unclamped * 100)),
            "p_clamped_pct": int(round(p_clamped * 100)),
            "is_safe": is_safe,
            "safe_clamped": is_safe and (p_unclamped > p_clamped + 0.01)
        },
        "churn_risk_band": _get_churn_band(p_clamped),
        "revenue_at_risk": float(campaign_data.get('campaign_budget', 0)) * p_clamped
    }


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


def _get_churn_band(churn_prob: float) -> str:
    """Convert churn probability to risk band"""
    if churn_prob <= 0.15:
        return 'LOW'
    elif churn_prob <= 0.30:
        return 'MEDIUM'
    elif churn_prob <= 0.45:
        return 'HIGH'
    else:
        return 'CRITICAL'