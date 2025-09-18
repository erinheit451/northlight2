"""
Risk waterfall visualization logic
"""
from typing import Dict, Any, List, Optional


def build_churn_waterfall(risk: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Risk is your current model output for a single account.
    Required inputs (directly or derivable):
      - risk["total_pct"]   -> final churn % shown in the header (clamped)
      - risk["baseline_pp"] -> cohort baseline in percentage points (int)
      - risk["drivers"]     -> list of {name, points, is_controllable, explanation, lift_x? or rel_pct?}
    Returns an object that the frontend renders with waterfall summing to unclamped total.
    """
    if not risk:
        return None

    total_clamped = int(round(float(risk.get("total_pct", 0))))
    baseline = int(round(float(risk.get("baseline_pp", risk.get("baseline", 0)))))

    # Get unclamped total for waterfall math
    total_unclamped = risk.get("total_pct_unclamped")
    if total_unclamped is None:
        rid = risk.get("risk_drivers_json") or risk
        total_unclamped = rid.get("p_unclamped_pct", total_clamped)
    total_unclamped = int(round(float(total_unclamped)))

    drivers_in: List[Dict[str, Any]] = risk.get("drivers") or []

    # Handle both old and new driver formats
    raw_drivers: List[Dict[str, Any]] = []

    # Case A: you already have a list of drivers
    if isinstance(drivers_in, list) and drivers_in:
        raw_drivers = drivers_in
    else:
        # Case B: flat fields (keep the ones that exist) - fallback for old format
        for k, label, typ, why in [
            ("risk_cpl_pp",            "High CPL (≥3× goal)",         "controllable",
             "3× goal historically elevates churn vs cohort."),
            ("risk_new_account_pp",    "Early Account (≤90d)",        "structural",
             "First 90 days show elevated hazard vs matured accounts."),
            ("risk_single_product_pp", "Single Product",               "structural",
             "Fewer anchors → higher volatility."),
            ("risk_pacing_pp",         "Off-pacing",                   "controllable",
             "Under/over-spend drives instability and lead gaps."),
            ("risk_low_leads_pp",      "Below expected leads",         "controllable",
             "Lead scarcity increases cancel probability."),
        ]:
            if k in risk and isinstance(risk[k], (int, float)):
                raw_drivers.append({
                    "name": label,
                    "points": float(risk[k]),
                    "is_controllable": (typ == "controllable"),
                    "explanation": why,
                })

    drivers_norm: List[Dict[str, Any]] = []
    for d in raw_drivers:
        pp = int(round(float(d.get("points", d.get("impact", 0)))))
        if pp == 0:
            continue
        dtype = "controllable" if d.get("is_controllable") else "structural"
        if pp < 0:
            dtype = "protective"
        drivers_norm.append({
            "label": d.get("label") or d.get("name") or "Driver",
            "pp": pp,
            "type": dtype,
            "why": d.get("explanation") or d.get("why") or "",
            # optional: either "lift_x" (e.g., 1.7) or "rel_pct" (e.g., +40)
            "lift_x": d.get("lift_x"),
            "rel_pct": d.get("rel_pct"),
        })

    # Sum to UNCLAMPED for additive readability
    sum_pp = baseline + sum(d["pp"] for d in drivers_norm)
    residual = total_unclamped - sum_pp

    # Silent rounding reconciliation only (no fake bars)
    if abs(residual) >= 1 and drivers_norm:
        drivers_norm[-1]["pp"] += residual

    # Add clamp note if clamped < unclamped
    clamp_note = None
    if total_clamped < total_unclamped:
        clamp_note = f"SAFE clamp active: displayed churn {total_clamped}% < model {total_unclamped}%."

    if total_clamped == 0 and baseline == 0 and not drivers_norm:
        return None

    return {
        "total_pct": total_clamped,                # header (clamped)
        "math_total_unclamped": total_unclamped,   # annotation only
        "baseline": max(0, min(100, baseline)),    # frontend expects baseline
        "baseline_pp": max(0, min(100, baseline)),
        "drivers": drivers_norm,
        "note": clamp_note,
        "cap_to": 100,
        "show_ranges": False
    }