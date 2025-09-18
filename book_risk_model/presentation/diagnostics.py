"""
Diagnostic message generation and UI presentation logic
"""
import pandas as pd
import numpy as np
import math
from typing import Dict, Any, List, Tuple

from ..constants import *
from ..utils import nz_num, safe_div


def _goal_advice_for_row(row: pd.Series) -> dict:
    """
    Return a compact, UI-ready advisory about CPL goal realism.
    Uses benchmark percentiles if present; else falls back to median.
    """
    # Inputs
    med  = pd.to_numeric(pd.Series([row.get('bsc_cpl_avg')])).fillna(np.nan).iloc[0]
    goal = pd.to_numeric(pd.Series([row.get('cpl_goal')])).fillna(np.nan).iloc[0]  # Original goal
    effective_goal = pd.to_numeric(pd.Series([row.get('effective_cpl_goal')])).fillna(np.nan).iloc[0]
    was_substituted = bool(row.get('goal_was_substituted', False))
    act  = pd.to_numeric(pd.Series([row.get('running_cid_cpl')])).fillna(np.nan).iloc[0]
    io_m = pd.to_numeric(pd.Series([row.get('io_cycle')])).fillna(0).iloc[0]
    days = pd.to_numeric(pd.Series([row.get('days_elapsed')])).fillna(0).iloc[0]

    # Enhanced percentiles using actual data fields
    p25 = pd.to_numeric(pd.Series([row.get('bsc_cpl_top_25pct')])).fillna(np.nan).iloc[0]
    p50 = med if pd.isna(row.get('bsc_cpl_avg')) else pd.to_numeric(pd.Series([row.get('bsc_cpl_avg')])).fillna(med).iloc[0]
    p75 = pd.to_numeric(pd.Series([row.get('bsc_cpl_bottom_25pct')])).fillna(np.nan).iloc[0]

    # Fallback window if percentiles not present
    if not np.isfinite(p50) or p50 <= 0:
        p50 = med if (np.isfinite(med) and med > 0) else 150.0
    if not np.isfinite(p25) or p25 <= 0:
        p25 = 0.8 * p50
    if not np.isfinite(p75) or p75 <= 0:
        p75 = 1.2 * p50

    # Gate for very early data (avoid scolding day-1 launches)
    show_gate = (days >= 7) or (io_m >= 1)

    # Classify goal realism vs benchmark median
    status = 'reasonable'
    ratio  = None
    if not np.isfinite(goal) or goal <= 0:
        status = 'missing'
    else:
        ratio = goal / p50
        if ratio < 0.5:           status = 'too_low'
        elif ratio < 0.7:         status = 'ambitious'   # aggressive but maybe attainable
        elif ratio <= 1.5:        status = 'reasonable'
        elif ratio <= 2.5:        status = 'too_high'
        else:                     status = 'wildly_high'

    # Recommended range and point target (tight, defensible)
    # If you have real percentiles, use ~P40–P60; else clamp to 0.8–1.2× median
    rec_min = max(0.8 * p50, p25)
    rec_max = min(1.2 * p50, p75)
    rec_pt  = float(np.clip(p50, rec_min, rec_max))

    # Performance bands (we'll show "vs rec goal" primarily)
    def band(r):
        if not np.isfinite(r) or r <= 0: return '—'
        if r >= 3.0:   return 'CRISIS (≥3×)'
        if r >= 2.0:   return 'Major gap (2–3×)'
        if r >= 1.5:   return 'Gap (1.5–2×)'
        if r >  1.1:   return 'Slightly high (1.1–1.5×)'
        if r >= 0.9:   return 'On target (±10%)'
        return 'Under target (<0.9×)'

    perf_vs_goal = band(act / goal) if (np.isfinite(goal) and goal > 0) else '—'
    perf_vs_rec  = band(act / rec_pt)

    rationale = f"Vertical median (p50) ≈ ${int(round(p50))}. Recommended window ${int(round(rec_min))}–${int(round(rec_max))}."

    return {
        "show": bool(show_gate and status in {"missing","too_low"}),  # Only show for missing and too_low
        "status": status,
        "goal_advertiser": float(goal) if np.isfinite(goal) and goal > 0 else None,
        "goal_effective": float(effective_goal) if np.isfinite(effective_goal) and effective_goal > 0 else None,
        "goal_was_substituted": was_substituted,
        "benchmark": {"p25": float(p25), "p50": float(p50), "p75": float(p75)},
        "recommended": {
            "point": float(rec_pt),
            "range": [float(rec_min), float(rec_max)]
        },
        "performance_band": {
            "vs_goal": perf_vs_goal,
            "vs_recommended": perf_vs_rec
        },
        "rationale": rationale
    }


def generate_headline_diagnosis(df) -> Tuple[List[str], List[str]]:
    """Generate more specific primary issue headlines"""
    headlines = []
    severities = []

    is_safe_col = df.get('is_safe')

    for idx, row in df.iterrows():
        # SAFE override
        if bool(is_safe_col.iloc[idx] if is_safe_col is not None else False):
            headlines.append('PERFORMING — ON TRACK')
            severities.append('healthy')
            continue

        d  = float(row.get('days_elapsed') or 0)
        sp = float(row.get('amount_spent') or 0)
        sem_viable = bool(row.get('_sem_viable', False))
        zero30 = bool(row.get('zero_lead_last_mo', False))
        zeroe  = bool(row.get('zero_lead_emerging', False))

        cpl_pct = (row.get('cpl_variance_pct') or 0)
        leads_val = pd.to_numeric(row.get('running_cid_leads'), errors='coerce')
        leads = int(leads_val if pd.notna(leads_val) else 0)
        io      = float(row.get('io_cycle') or 0)
        exp_td_spend = float(row.get('expected_leads_to_date_spend') or 0)

        # No-spend takes precedence for early days
        if (d >= MIN_DAYS_FOR_ALERTS) and (sp < MIN_SPEND_FOR_ZERO_LEAD):
            headlines.append('NOT SPENDING — CHECK LIVE STATE')
            severities.append('warning')
            continue

        # Idle zero-lead (paused/near-zero spend): call it out explicitly, not as a crisis
        if bool(row.get('zero_lead_idle', False)):
            headlines.append('NOT SPENDING — ZERO LEADS')
            severities.append('warning')
            continue

        # Legit zero-lead callouts only if the flags/gates are true
        if zero30 or zeroe:
            headlines.append('ZERO LEADS — NO CONVERSIONS')
            severities.append('critical')
            continue

        # Calculate SEM viability inline since fields don't exist yet - used by multiple checks below
        budget = float(row.get('campaign_budget') or 0)
        cpc_safe = float(row.get('bsc_cpc_average') or 3.0)
        avg_len = float(row.get('avg_cycle_length') or 30.4) or 30.4
        exp_month = float(row.get('expected_leads_monthly') or 0)

        daily_budget = budget / avg_len
        daily_clicks = daily_budget / cpc_safe if cpc_safe > 0 else 0

        budget_ok = budget >= SEM_VIABILITY_MIN_SEM
        clicks_ok = daily_clicks >= SEM_VIABILITY_MIN_DAILY_CLICKS
        volume_ok = exp_month >= SEM_VIABILITY_MIN_MONTHLY_LEADS
        sem_viable = budget_ok or clicks_ok or volume_ok

        # PRIORITY: Zero-lead checks come BEFORE "new account" to avoid masking performance issues
        # Acute cycle-based zero-lead (5–29d) when plan+spend indicate we should have ≥1 lead
        exp_td_plan_val = float(row.get('expected_leads_to_date') or 0)
        exp_td_spend_val = float(row.get('expected_leads_to_date_spend') or 0)
        if (leads == 0) and (d >= MIN_DAYS_FOR_ALERTS) and (d < 30) and sem_viable \
           and (exp_td_plan_val >= ZERO_LEAD_MIN_EXPECTED_TD) \
           and (sp >= MIN_SPEND_FOR_ZERO_LEAD):
            headlines.append('ZERO LEADS — NO CONVERSIONS')
            severities.append('critical')
            continue

        # Underfunded: capacity, not performance. Require BOTH capacity gates to fail AND not viable overall
        if False and UNDERFUNDED_FEATURE_ENABLED:
            if (d >= MIN_DAYS_FOR_ALERTS) and (leads <= 0) and (not budget_ok) and (not clicks_ok) and (not sem_viable):
                headlines.append('UNDERFUNDED — Increase budget to reach viability')
                severities.append('neutral')
                continue

        # Critical conditions
        if (cpl_pct > 300) and (io <= 3) and (leads <= 5):
            headlines.append('CPL CRISIS — NEW ACCOUNT — LOW LEADS')
            severities.append('critical')
            continue
        if cpl_pct > 100:
            headlines.append(f"HIGH CPL — ${int(row.get('running_cid_cpl') or 0)} vs ${int(row.get('effective_cpl_goal') or row.get('cpl_goal') or 0)} GOAL")
            severities.append('warning' if cpl_pct <= 200 else 'critical')
            continue
        if io <= 3:
            headlines.append('NEW ACCOUNT AT RISK')
            severities.append('warning')
            continue
        util = float(row.get('utilization') or 0)
        if util and util < 0.5:
            pct = int((1 - util) * 100)
            headlines.append(f"UNDERPACING — {pct}% BEHIND")
            severities.append('warning')
            continue
        if cpl_pct < -20 or (exp_td_spend and leads >= exp_td_spend):
            headlines.append('PERFORMING — ON/UNDER GOAL')
            severities.append('healthy')
            continue

        # Goal alignment check
        median_cpl_row = float(row.get('bsc_cpl_avg') or 0)
        raw_goal_row   = float(row.get('cpl_goal') or 0)
        goal_quality   = str(row.get('goal_quality') or '')
        if median_cpl_row > 0 and raw_goal_row > 0:
            absurd_goal = (goal_quality == 'too_low') and (raw_goal_row < 0.5 * median_cpl_row)
        else:
            absurd_goal = False
        if absurd_goal:
            headlines.append('GOAL MISALIGNED — Reset Required')
            severities.append('warning')
            continue

        headlines.append('MONITORING FOR CHANGES')
        severities.append('neutral')

    return headlines, severities


def generate_diagnosis_pills(row) -> List[Dict[str, str]]:
    """Generate refined diagnosis pills for each account"""
    pills = []
    sem_viable = bool(row.get('_sem_viable', False))
    budget_ok  = bool(row.get('_viab_budget_ok', False))
    clicks_ok  = bool(row.get('_viab_clicks_ok', False))
    d   = float(row.get('days_elapsed') or 0)
    sp  = float(row.get('amount_spent') or 0)
    leads = int(pd.to_numeric(pd.Series([row.get('running_cid_leads')])).fillna(0).iloc[0])

    # Healthy shortcut
    if bool(row.get('is_safe', False)):
        return [{'text': 'Performing', 'type': 'success'}]

    # Zero-lead callouts (only when gated true)
    if bool(row.get('zero_lead_last_mo', False)) or bool(row.get('zero_lead_emerging', False)):
        pills.append({'text': 'Zero Leads', 'type': 'critical'})
    elif bool(row.get('zero_lead_idle', False)):
        pills.append({'text': 'Zero Leads (Idle)', 'type': 'warning'})
    else:
        # Loosen to global alert floor (5d)
        if (d >= MIN_DAYS_FOR_ALERTS) and (leads == 0) and sem_viable:
            pills.append({'text': 'No Leads Yet', 'type': 'warning'})

    # CPL variance
    if pd.notna(row.get('cpl_variance_pct')) and abs(row['cpl_variance_pct']) > 20:
        pct = int(row['cpl_variance_pct'])
        pills.append({'text': f'CPL {("+" if pct>0 else "")}{pct}%', 'type': 'critical' if pct > 200 else 'warning'})

    # Early tenure
    tm = float(row.get('true_months_running') or 0)
    if tm <= 3.0:
        pills.append({'text': 'Early Account', 'type': 'warning'})

    # Single product
    if row.get('single_product_flag') or row.get('true_product_count') == 1:
        pills.append({'text': 'Single Product', 'type': 'neutral'})

    # Pacing
    util = row.get('utilization')
    if pd.notna(util):
        if util < 0.5:
            pills.append({'text': f'Pacing -{int((1-util)*100)}%', 'type': 'warning'})
        elif util > 1.25:
            pills.append({'text': f'Pacing +{int((util-1)*100)}%', 'type': 'warning'})

    # Goal quality
    q = row.get('goal_quality')
    if pd.notna(q):
        if q == 'missing':
            pills.append({'text': 'No Goal', 'type': 'warning'})
        elif q == 'too_low':
            pills.append({'text': 'Goal Too Low', 'type': 'warning'})

    # Underfunded pill ONLY when both hard capacity gates fail  (TEMP DISABLED)
    if False and UNDERFUNDED_FEATURE_ENABLED:
        if (not budget_ok) and (not clicks_ok):
            pills.append({'text': 'Underfunded', 'type': 'neutral'})

    # $ risk
    rar = float(row.get('revenue_at_risk') or 0)
    if rar >= 5000:
        pills.append({'text': 'High $ Risk', 'type': 'critical'})
    elif rar >= 2000:
        pills.append({'text': '$ Risk', 'type': 'warning'})

    return pills