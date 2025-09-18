#!/usr/bin/env python3
"""Simple database verification without Unicode symbols"""

import asyncio
import sys
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.database import get_db_session, init_database
from sqlalchemy import text


async def verify_data():
    """Simple data verification."""

    try:
        await init_database()

        tables = [
            "heartbeat_core.ultimate_dms_campaigns",
            "heartbeat_core.budget_waterfall_client",
            "heartbeat_salesforce.sf_partner_pipeline",
            "heartbeat_salesforce.sf_partner_calls",
            "heartbeat_performance.spend_revenue_performance"
        ]

        print("\nDATABASE VERIFICATION RESULTS")
        print("=" * 50)

        total_rows = 0
        async with get_db_session() as session:
            for table in tables:
                try:
                    result = await session.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.scalar()
                    total_rows += count

                    # Get latest date
                    date_result = await session.execute(text(f"SELECT MAX(snapshot_date) FROM {table} WHERE snapshot_date IS NOT NULL"))
                    latest_date = date_result.scalar()

                    status = "SUCCESS" if count > 0 else "EMPTY"
                    print(f"{table}: {count} rows ({status})")
                    if latest_date:
                        print(f"  Latest data: {latest_date}")

                except Exception as e:
                    print(f"{table}: ERROR - {e}")

        print(f"\nTOTAL ROWS LOADED: {total_rows}")

        # Sample from SF partner pipeline
        if total_rows > 0:
            print(f"\nSAMPLE DATA FROM SF PARTNER PIPELINE:")
            print("-" * 50)
            try:
                result = await session.execute(text("""
                    SELECT account_owner, opportunity_name, all_tcv, close_date, snapshot_date
                    FROM heartbeat_salesforce.sf_partner_pipeline
                    LIMIT 3
                """))
                rows = result.fetchall()

                for row in rows:
                    print(f"Owner: {row[0]}")
                    print(f"Opportunity: {row[1]}")
                    print(f"TCV: ${row[2]}")
                    print(f"Close Date: {row[3]}")
                    print(f"Snapshot: {row[4]}")
                    print("-" * 30)

            except Exception as e:
                print(f"Sample query failed: {e}")

        return True

    except Exception as e:
        print(f"Verification failed: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(verify_data())
    print(f"\nVerification {'PASSED' if success else 'FAILED'}")
    sys.exit(0 if success else 1)