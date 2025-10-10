"""
Advertiser Lifetime Calculation Module
Calculates TRUE advertiser lifetime using MAID historical data
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any


async def calculate_advertiser_lifetime(df: pd.DataFrame, conn) -> pd.DataFrame:
    """
    Calculate TRUE advertiser lifetime using MAID-level historical data.

    This fixes the disconnect where io_cycle represents the current campaign cycle,
    but we need to show the full advertiser history for the "Early Account" pill.

    Args:
        df: DataFrame with campaign data including MAID
        conn: asyncpg connection to query historical data

    Returns:
        DataFrame with added advertiser lifetime fields:
        - advertiser_lifetime_days: Total days from first to latest data
        - advertiser_lifetime_months: Lifetime in months
        - advertiser_first_seen: Earliest report_month date
        - advertiser_latest_data: Latest report_month date
    """
    if df.empty:
        return df

    print("Calculating advertiser lifetime from MAID historical data...")

    # Initialize columns with defaults
    df['advertiser_lifetime_days'] = None
    df['advertiser_lifetime_months'] = None
    df['advertiser_first_seen'] = None
    df['advertiser_latest_data'] = None

    # Get unique MAIDs to query
    maids = df['maid'].dropna().unique()
    print(f"  Found {len(maids)} unique MAIDs to process")

    # Query historical data for all MAIDs in bulk
    maid_lifetime_data = {}

    for maid in maids:
        try:
            maid_int = int(maid)
        except (ValueError, TypeError):
            continue

        # Query historical performance data for this MAID
        history_query = """
            SELECT
                MIN(report_month) as earliest,
                MAX(report_month) as latest,
                COUNT(DISTINCT report_month) as month_count,
                COUNT(DISTINCT campaign_id) as campaign_count
            FROM book.raw_heartbeat_spend_revenue_performance_current
            WHERE maid = $1
        """

        try:
            result = await conn.fetchrow(history_query, maid_int)

            if result and result['earliest']:
                earliest = result['earliest']
                latest = result['latest']

                # Calculate lifetime in days
                if isinstance(earliest, str):
                    earliest = datetime.strptime(earliest, '%Y-%m-%d').date()
                if isinstance(latest, str):
                    latest = datetime.strptime(latest, '%Y-%m-%d').date()

                lifetime_days = (latest - earliest).days
                lifetime_months = lifetime_days / 30.4  # Use AVG_CYCLE constant

                maid_lifetime_data[str(maid)] = {
                    'lifetime_days': lifetime_days,
                    'lifetime_months': lifetime_months,
                    'first_seen': earliest,
                    'latest_data': latest,
                    'month_count': result['month_count'],
                    'campaign_count': result['campaign_count']
                }

        except Exception as e:
            print(f"  Warning: Could not get lifetime for MAID {maid}: {e}")
            continue

    print(f"  Successfully calculated lifetime for {len(maid_lifetime_data)} MAIDs")

    # Apply lifetime data to campaigns
    campaigns_with_lifetime = 0
    campaigns_without_lifetime = 0

    for idx, row in df.iterrows():
        maid = str(row['maid']) if pd.notna(row['maid']) else None

        if maid and maid in maid_lifetime_data:
            lifetime_info = maid_lifetime_data[maid]

            df.at[idx, 'advertiser_lifetime_days'] = lifetime_info['lifetime_days']
            df.at[idx, 'advertiser_lifetime_months'] = lifetime_info['lifetime_months']
            df.at[idx, 'advertiser_first_seen'] = lifetime_info['first_seen']
            df.at[idx, 'advertiser_latest_data'] = lifetime_info['latest_data']

            campaigns_with_lifetime += 1

            # Debug output for validation
            if idx < 5:  # Show first 5 for debugging
                print(f"  Campaign {row['campaign_id']} (MAID {maid}):")
                print(f"    Advertiser lifetime: {lifetime_info['lifetime_days']} days ({lifetime_info['lifetime_months']:.1f} months)")
                print(f"    Historical data: {lifetime_info['month_count']} months across {lifetime_info['campaign_count']} campaigns")
                print(f"    Date range: {lifetime_info['first_seen']} to {lifetime_info['latest_data']}")

                # Compare to current campaign age
                io_cycle = float(row.get('io_cycle', 1) or 1)
                days_elapsed = float(row.get('days_elapsed', 0) or 0)
                campaign_age = ((io_cycle - 1) * 30.4) + days_elapsed
                print(f"    Current campaign age (io_cycle calc): {campaign_age:.1f} days")
                if campaign_age > 0:
                    print(f"    Ratio: {lifetime_info['lifetime_days'] / campaign_age:.1f}x more history at MAID level")
        else:
            campaigns_without_lifetime += 1
            # Keep None for campaigns without historical data (truly new)

    print(f"\n  Summary:")
    print(f"    Campaigns with lifetime data: {campaigns_with_lifetime}")
    print(f"    Campaigns without lifetime data: {campaigns_without_lifetime} (truly new)")

    return df
