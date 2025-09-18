"""
FLARE scoring and priority calculation logic
"""
import pandas as pd
import numpy as np
import json
from typing import Dict, Any, List

from .constants import *
from .utils import nz_num, safe_div


def _percentile_score(s: pd.Series) -> pd.Series:
    """0..100 percentile (robust to ties); returns 0 if all zeros."""
    x = pd.to_numeric(s, errors="coerce").fillna(0).values
    if np.all(x == 0):
        return pd.Series(np.zeros_like(x, dtype=float), index=s.index)
    ranks = pd.Series(x).rank(method="average", pct=True).values
    return pd.Series((ranks * 100).clip(0, 100), index=s.index)


def _load_flare_calibration():
    """Optional override via /mnt/data/flare_calibration.json; safe defaults otherwise."""
    cfg = {
        "eloss_cap_usd": 25000.0,
        "band_ranges": {
            "SAFE":       [0, 24],
            "LOW":        [25, 44],
            "MEDIUM":     [45, 64],
            "HIGH":       [65, 84],
            "CRITICAL":   [85, 100],
        }
    }
    try:
        with open("/mnt/data/flare_calibration.json", "r") as f:
            loaded = json.load(f)
            for k in ("eloss_cap_usd","band_ranges"):
                if k in loaded: cfg[k] = loaded[k]
    except Exception:
        pass
    return cfg


# Load FLARE configuration
_FLARE_CFG = _load_flare_calibration()


def attach_priority_and_flare(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute UPI and FLARE scores with aggressive SAFE suppression.
    SAFE accounts get:
    - Priority index multiplied by 0.05
    - FLARE score capped at 15
    - Forced to "low" band
    """
    if df is None or df.empty:
        out = df.copy() if df is not None else pd.DataFrame()
        for c in ("priority_index","flare_score","flare_score_raw","flare_band","flare_breakdown_json"):
            out[c] = np.nan
        return out

    out  = df.copy()
    rar  = pd.to_numeric(out.get("revenue_at_risk"), errors="coerce").fillna(0.0)
    churn= pd.to_numeric(out.get("churn_prob_90d"), errors="coerce").fillna(0.0).clip(0,1)
    safe = out.get("is_safe", pd.Series(False, index=out.index)).fillna(False)

    # Log-damped UPI (no hard cap per 14B)
    ALPHA, BETA = 0.6, 0.4
    eps = 1e-12
    upi = np.exp(ALPHA * np.log(rar + eps) + BETA * np.log(churn + eps))

    # ===== SAFE: heavy down-weight in UPI =====
    upi = np.where(safe, upi * float(SAFE_DOWNWEIGHT_IN_UPI), upi)
    out["priority_index"] = upi

    # Percentile → FLARE
    ranks = pd.Series(upi, index=out.index).rank(method="average", pct=True)
    flare_raw = (100.0 * ranks)
    flare_int = flare_raw.round().astype(int)

    bands = pd.cut(ranks, bins=[0, 0.45, 0.65, 0.85, 1.01],
                   labels=["low","moderate","high","critical"], right=True).astype("string").fillna("low")

    # ===== SAFE: clamp both displayed and raw FLARE =====
    SAFE_RAW_CAP = float(SAFE_MAX_FLARE_SCORE)
    if safe.any():
        flare_int.loc[safe] = np.minimum(flare_int.loc[safe].fillna(0), SAFE_RAW_CAP).astype(int)
        flare_raw.loc[safe] = np.minimum(flare_raw.loc[safe].fillna(0.0), SAFE_RAW_CAP)
        bands.loc[safe] = "low"

    out["flare_score_raw"] = flare_raw
    out["flare_score"]     = flare_int
    out["flare_band"]      = bands

    breakdown = []
    for idx in out.index:
        try:
            r = float(rar.iloc[idx] if hasattr(rar, 'iloc') else rar)
            c = float(churn.iloc[idx] if hasattr(churn, 'iloc') else churn)
        except:
            r, c = 0.0, 0.0
        breakdown.append({"components": {"rar": float(r), "churn": float(c)}})
    out["flare_breakdown_json"] = breakdown

    return out


def compute_priority_v2(df: pd.DataFrame) -> pd.Series:
    """
    Unified Priority aligned with SAFE + crisis detectors + churn + FLARE.
    Returns: 'P1 - URGENT', 'P2 - HIGH', 'P3 - MONITOR', 'P0 - SAFE'
    """
    from .churn import _is_sliding_to_zero  # Import specific function to avoid circular dependency

    s = pd.Series('P3 - MONITOR', index=df.index, dtype='object')

    # Helper function to safely get DataFrame columns
    def safe_get_column(col_name, default_val, numeric=False):
        if col_name in df.columns:
            series = df[col_name]
            if numeric:
                series = pd.to_numeric(series, errors='coerce')
            return series.fillna(default_val)
        else:
            return pd.Series([default_val] * len(df), index=df.index)

    is_safe      = safe_get_column('is_safe', False)
    cpl_ratio    = safe_get_column('cpl_ratio', 0.0, numeric=True)
    churn        = safe_get_column('churn_prob_90d', 0.0, numeric=True)
    flare_band   = safe_get_column('flare_band', 'low').astype('string')
    amount_spent = safe_get_column('amount_spent', 0.0, numeric=True)

    zero30       = safe_get_column('zero_lead_last_mo', False)
    zero_early   = safe_get_column('zero_lead_emerging', False)

    # Runtime & pacing
    rt_days_raw = df.get('true_days_running')
    if rt_days_raw is None:
        rt_days = None
    else:
        rt_days = pd.to_numeric(rt_days_raw, errors='coerce')

    if rt_days is None or rt_days.isnull().all():
        io_col = df.get('io_cycle')
        io_f = pd.to_numeric(io_col, errors='coerce') if io_col is not None else pd.Series([0.0] * len(df), index=df.index)
        if not isinstance(io_f, pd.Series):
            io_f = pd.Series([io_f] * len(df), index=df.index)
        io_f = io_f.fillna(0.0)

        avg_col = df.get('avg_cycle_length')
        avg_f = pd.to_numeric(avg_col, errors='coerce') if avg_col is not None else pd.Series([AVG_CYCLE] * len(df), index=df.index)
        if not isinstance(avg_f, pd.Series):
            avg_f = pd.Series([avg_f] * len(df), index=df.index)
        avg_f = avg_f.fillna(AVG_CYCLE)

        days_col = df.get('days_elapsed')
        days_f = pd.to_numeric(days_col, errors='coerce') if days_col is not None else pd.Series([0.0] * len(df), index=df.index)
        if not isinstance(days_f, pd.Series):
            days_f = pd.Series([days_f] * len(df), index=df.index)
        days_f = days_f.fillna(0.0)

        rt_days = ((io_f - 1).clip(lower=0) * avg_f + days_f).clip(lower=0.0)

    # Safe column processing helper
    def safe_numeric_column(col_name, default_val=0.0):
        col_data = df.get(col_name)
        if col_data is None:
            return pd.Series([default_val] * len(df), index=df.index)
        result = pd.to_numeric(col_data, errors='coerce')
        if not isinstance(result, pd.Series):
            result = pd.Series([result] * len(df), index=df.index)
        return result.fillna(default_val)

    exp_td_plan = safe_numeric_column('expected_leads_to_date', 0.0)
    leads       = safe_numeric_column('running_cid_leads', 0.0)
    days        = safe_numeric_column('days_elapsed', 0.0)
    budget      = safe_numeric_column('campaign_budget', 0.0)
    spent       = amount_spent
    avg_len     = safe_numeric_column('avg_cycle_length', AVG_CYCLE).replace(0, AVG_CYCLE)
    ideal_spend = (budget / avg_len) * days
    spend_prog  = (spent / ideal_spend.replace(0, np.nan)).fillna(0.0)
    lead_ratio  = np.where(exp_td_plan > 0, leads / exp_td_plan, 1.0)

    sev_deficit = (exp_td_plan >= 1) & (lead_ratio <= 0.25) & (spend_prog >= 0.5) & (days >= 7)
    mod_deficit = (exp_td_plan >= 1) & (lead_ratio <= 0.50) & (spend_prog >= 0.4) & (days >= 5)

    # P0 SAFE - Always first priority
    s[is_safe] = 'P0 - SAFE'

    # Extreme CPL (≥4x) is always P1; ≥3x is P1 if not safe (kept)
    extreme_cpl = (cpl_ratio >= 4.0)

    sliding_zero = _is_sliding_to_zero(cpl_ratio, leads, days, spend_prog)

    sem_viable = safe_get_column('_sem_viable', False)

    # IMPORTANT: zero_early / zero30 are pre-gated upstream (spend/progress/viability/day floor).
    # Do NOT add redundant gates here; treat these flags as authoritative.

    # P1 URGENT: acute conditions (not safe)
    p1 = (~is_safe) & (
        zero30 |
        zero_early |                     # ← the gated flag is enough; the extra spend/viability checks are now redundant
        extreme_cpl |
        (cpl_ratio >= 3.0) |
        sev_deficit |
        sliding_zero |
        ((flare_band == 'critical') & (churn >= 0.40))
    )
    s[p1] = 'P1 - URGENT'

    # P2 HIGH: elevated conditions (not safe, not P1)
    p2 = (~is_safe) & (~p1) & (
        ((cpl_ratio >= 1.5) & (cpl_ratio < 3.0)) |
        mod_deficit |
        (flare_band == 'high') |
        (churn >= 0.25)
    )
    s[p2] = 'P2 - HIGH'

    return s


