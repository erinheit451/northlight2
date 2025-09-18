"""
Utility functions for risk model calculations
"""
import pandas as pd
import numpy as np


def nz_num(series_or_val, default=0.0):
    """Convert to numeric, handling NaN/None gracefully"""
    if pd.isna(series_or_val).any() if hasattr(series_or_val, 'any') else pd.isna(series_or_val):
        return default
    try:
        return pd.to_numeric(series_or_val, errors='coerce').fillna(default)
    except:
        return default


def safe_div(num, denom, fill=np.nan):
    """Safe division with custom fill value for zero/invalid denominators"""
    if hasattr(num, 'index'):  # Series
        return np.where(
            pd.to_numeric(denom, errors='coerce').fillna(0) == 0,
            fill,
            pd.to_numeric(num, errors='coerce').fillna(0) / pd.to_numeric(denom, errors='coerce').fillna(1)
        )
    else:  # Scalar
        try:
            denom_val = float(denom) if not pd.isna(denom) else 0
            num_val = float(num) if not pd.isna(num) else 0
            return fill if denom_val == 0 else num_val / denom_val
        except:
            return fill


def _ensure_columns(df: pd.DataFrame, required_cols: list) -> pd.DataFrame:
    """Ensure all required columns exist with default values"""
    for col in required_cols:
        if col not in df.columns:
            if col in ['campaign_id', 'maid', 'advertiser_name', 'partner_name']:
                df[col] = 'unknown'
            elif 'count' in col or 'leads' in col or 'days' in col:
                df[col] = 0
            elif 'ratio' in col or 'pct' in col or 'score' in col:
                df[col] = 0.0
            elif 'bool' in col or 'is_' in col:
                df[col] = False
            else:
                df[col] = np.nan
    return df