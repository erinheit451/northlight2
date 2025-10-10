#!/usr/bin/env python3
"""Check database status - verify what data was actually loaded"""
import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.database import init_database, get_db_session
from sqlalchemy import text

async def check_database():
    """Check all ETL tables and row counts"""
    await init_database()

    tables_to_check = [
        ("heartbeat_core", "ultimate_dms_campaigns"),
        ("heartbeat_core", "budget_waterfall_client"),
        ("heartbeat_core", "budget_waterfall_channel"),
        ("heartbeat_core", "bsc_standards"),
        ("heartbeat_core", "dfp_rij_alerts"),
        ("heartbeat_performance", "spend_revenue_performance"),
        ("heartbeat_performance", "agreed_cpl_performance"),
        ("heartbeat_salesforce", "sf_partner_pipeline"),
        ("heartbeat_salesforce", "sf_tim_king_partner_pipeline"),
        ("heartbeat_salesforce", "sf_partner_calls"),
        ("heartbeat_salesforce", "sf_grader_opportunities"),
    ]

    print("="*80)
    print("DATABASE STATUS CHECK")
    print("="*80)
    print(f"\n{'Table':<50} {'Row Count':<15} {'Status'}")
    print("-"*80)

    total_rows = 0
    loaded_count = 0

    async with get_db_session() as session:
        for schema, table in tables_to_check:
            try:
                result = await session.execute(
                    text(f"SELECT COUNT(*) FROM {schema}.{table}")
                )
                count = result.scalar()
                total_rows += count

                if count > 0:
                    loaded_count += 1
                    status = "[OK] LOADED"
                else:
                    status = "[X] EMPTY"

                full_name = f"{schema}.{table}"
                print(f"{full_name:<50} {count:<15} {status}")

            except Exception as e:
                full_name = f"{schema}.{table}"
                print(f"{full_name:<50} {'ERROR':<15} [X] {str(e)[:30]}")

    print("-"*80)
    print(f"\nSummary:")
    print(f"  Tables with data: {loaded_count}/11")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Success rate: {loaded_count/11*100:.0f}%")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(check_database())
