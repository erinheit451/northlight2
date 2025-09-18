"""
PostgreSQL Data Ingest for Book Risk Model
Replaces CSV loading with database loading for the unified platform
"""
import pandas as pd
import asyncpg
from typing import Optional, Dict, Any
import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

DATABASE_URL = "postgresql://northlight_user:northlight_secure_2024@localhost:5432/unified_northlight"

async def load_health_data() -> pd.DataFrame:
    """
    Load campaign health/performance data from PostgreSQL.
    Mimics the CSV loading but from our migrated data.
    """
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Query the main performance table with campaign-level aggregation
        # Group by campaign_id only to avoid duplicates
        query = """
        SELECT
            campaign_id::text as campaign_id,
            MIN(maid::text) as maid,
            MIN(business_name) as advertiser_name,
            MIN(business_name) as business_name,
            MIN(business_category) as business_category,
            MIN(business_sub_category) as business_sub_category,
            MIN(campaign_name) as campaign_name,
            STRING_AGG(DISTINCT channel, ', ') as channel,
            SUM(spend) as amount_spent,
            SUM(leads) as running_cid_leads,
            AVG(cost_per_lead) as running_cid_cpl,
            SUM(spend) * 1.2 as campaign_budget,  -- Estimate budget as 120% of spend
            COUNT(*) as months_of_data,
            -- Calculate utilization as rough estimate
            0.8 as utilization,
            -- Assign placeholder values for required fields
            'Partner' as partner_name,
            'Optimizer TBD' as optimizer,
            'AM TBD' as am,
            'GM TBD' as gm,
            -- Calculate days elapsed (rough estimate)
            COUNT(*) * 30 as days_elapsed,
            -- Set some defaults
            1 as io_cycle,
            30.0 as avg_cycle_length,
            'P3 - MEDIUM' as current_priority
        FROM book.raw_heartbeat_spend_revenue_performance_current
        WHERE campaign_id IS NOT NULL
        GROUP BY campaign_id
        HAVING SUM(leads) > 0 OR SUM(spend) > 0
        ORDER BY campaign_id
        """

        rows = await conn.fetch(query)

        # Convert to DataFrame
        if rows:
            df = pd.DataFrame([dict(row) for row in rows])

            # Ensure required columns exist with sensible defaults
            required_cols = {
                'campaign_id': '',
                'maid': '',
                'advertiser_name': '',
                'business_name': '',
                'campaign_name': '',
                'partner_name': 'Partner',
                'optimizer': 'Optimizer TBD',
                'am': 'AM TBD',
                'gm': 'GM TBD',
                'business_category': 'Unknown',
                'amount_spent': 0.0,
                'running_cid_leads': 0,
                'running_cid_cpl': 0.0,
                'campaign_budget': 0.0,
                'utilization': 0.0,
                'days_elapsed': 30,
                'io_cycle': 1,
                'avg_cycle_length': 30.0,
                'channel': 'Partner'
            }

            for col, default_val in required_cols.items():
                if col not in df.columns:
                    df[col] = default_val
                else:
                    df[col] = df[col].fillna(default_val)

            # Convert types
            df['campaign_id'] = df['campaign_id'].astype(str)
            df['maid'] = df['maid'].astype(str)
            df['amount_spent'] = pd.to_numeric(df['amount_spent'], errors='coerce').fillna(0)
            df['running_cid_leads'] = pd.to_numeric(df['running_cid_leads'], errors='coerce').fillna(0)
            df['running_cid_cpl'] = pd.to_numeric(df['running_cid_cpl'], errors='coerce').fillna(0)
            df['campaign_budget'] = pd.to_numeric(df['campaign_budget'], errors='coerce').fillna(0)
            df['utilization'] = pd.to_numeric(df['utilization'], errors='coerce').fillna(0)

            print(f"Loaded {len(df)} campaigns from PostgreSQL performance data")
            return df
        else:
            print("No campaign data found in PostgreSQL")
            return pd.DataFrame()

    finally:
        await conn.close()


async def load_breakout_data() -> pd.DataFrame:
    """
    Load breakout/roster data for enrichment.
    This would be additional campaign metadata if available.
    """
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Try to get additional campaign details from other tables
        query = """
        SELECT DISTINCT
            campaign_id::text as campaign_id,
            maid::text as maid,
            business_name as advertiser_name,
            -- Try to get more detailed team info if available
            'Partner' as partner_name,
            1 as true_product_count,
            business_category
        FROM book.raw_heartbeat_spend_revenue_performance_current
        WHERE campaign_id IS NOT NULL
        """

        rows = await conn.fetch(query)

        if rows:
            df = pd.DataFrame([dict(row) for row in rows])
            print(f"Loaded {len(df)} breakout records from PostgreSQL")
            return df
        else:
            print("No breakout data found in PostgreSQL")
            return pd.DataFrame()

    finally:
        await conn.close()


def load_health_data_sync() -> pd.DataFrame:
    """Synchronous wrapper for async load_health_data"""
    return asyncio.run(load_health_data())


def load_breakout_data_sync() -> pd.DataFrame:
    """Synchronous wrapper for async load_breakout_data"""
    return asyncio.run(load_breakout_data())


# For backward compatibility with the risk model
def latest_snapshot_path():
    """Mock function to maintain compatibility"""
    return "postgresql://database"