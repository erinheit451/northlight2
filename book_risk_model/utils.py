"""
Utility functions for risk model calculations
"""
import pandas as pd
import numpy as np

# ===== PANDAS HELPERS =====
# Vectorized helpers to reduce repetition and improve performance
def nz_num(s, fill=0.0):
    """Convert to numeric with fillna, handling both Series and scalar inputs"""
    if s is None:
        return fill
    result = pd.to_numeric(s, errors="coerce")
    if hasattr(result, 'fillna'):
        return result.fillna(fill)
    else:
        # Handle scalar case
        return fill if pd.isna(result) else result

def safe_div(a, b, fill=0.0):
    """Elementwise division with inf/NaN handling"""
    a_num = pd.to_numeric(a, errors='coerce')
    b_num = pd.to_numeric(b, errors='coerce')
    result = a_num / b_num

    if hasattr(result, 'replace'):
        return result.replace([np.inf, -np.inf], fill).fillna(fill)
    else:
        # Handle scalar case
        if np.isinf(result) or pd.isna(result):
            return fill
        return result

def coalesce(*series):
    """Return first non-null value across series"""
    result = series[0].copy() if len(series) > 0 else pd.Series(dtype=object)
    for s in series[1:]:
        mask = result.isnull()
        if mask.any():
            result[mask] = s[mask]
    return result

def _ensure_columns(df: pd.DataFrame, required_cols: list) -> pd.DataFrame:
    """Ensure required columns exist, adding NaN defaults if missing"""
    for col in required_cols:
        if col not in df.columns:
            df[col] = np.nan
    return df