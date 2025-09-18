#!/usr/bin/env python3
"""
Database Data Verification Script
Verify that all ETL data has been loaded correctly into PostgreSQL
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.config import settings
from core.database import get_db_session, init_database
from core.shared import setup_logging, get_logger
from sqlalchemy import text


async def verify_database_data():
    """Verify data has been loaded into all tables."""
    logger = get_logger("data_verification")

    try:
        # Initialize database
        await init_database()

        # Define tables to check
        tables_to_check = [
            ("heartbeat_core.ultimate_dms_campaigns", "Ultimate DMS Campaigns"),
            ("heartbeat_core.budget_waterfall_client", "Budget Waterfall Client"),
            ("heartbeat_core.budget_waterfall_channel", "Budget Waterfall Channel"),
            ("heartbeat_salesforce.sf_partner_pipeline", "SF Partner Pipeline"),
            ("heartbeat_salesforce.sf_partner_calls", "SF Partner Calls"),
            ("heartbeat_salesforce.sf_tim_king_partner_pipeline", "SF Tim King Pipeline"),
            ("heartbeat_salesforce.sf_grader_opportunities", "SF Grader Opportunities"),
            ("heartbeat_performance.agreed_cpl_performance", "Agreed CPL Performance"),
            ("heartbeat_performance.spend_revenue_performance", "Spend Revenue Performance"),
            ("heartbeat_core.dfp_rij_alerts", "DFP RIJ Alerts"),
        ]

        print("\n" + "="*80)
        print("DATABASE DATA VERIFICATION")
        print("="*80)

        async with get_db_session() as session:
            total_rows = 0
            tables_with_data = 0

            for table_name, display_name in tables_to_check:
                try:
                    # Get row count
                    result = await session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    count = result.scalar()

                    # Get latest extraction date if data exists
                    latest_date = None
                    if count > 0:
                        date_result = await session.execute(text(f"SELECT MAX(snapshot_date) FROM {table_name}"))
                        latest_date = date_result.scalar()
                        tables_with_data += 1

                    total_rows += count

                    # Format output
                    status = "✓" if count > 0 else "✗"
                    date_str = f" (Latest: {latest_date})" if latest_date else ""
                    print(f"{status} {display_name:.<40} {count:>8,} rows{date_str}")

                except Exception as e:
                    print(f"✗ {display_name:.<40} ERROR: {str(e)}")

            print("-" * 80)
            print(f"Total Tables with Data: {tables_with_data}/{len(tables_to_check)}")
            print(f"Total Rows Loaded: {total_rows:,}")

            # Sample some data from the largest table
            if total_rows > 0:
                print("\n" + "="*80)
                print("SAMPLE DATA VERIFICATION")
                print("="*80)

                # Find the table with most data
                largest_table = None
                largest_count = 0

                for table_name, display_name in tables_to_check:
                    try:
                        result = await session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                        count = result.scalar()
                        if count > largest_count:
                            largest_count = count
                            largest_table = (table_name, display_name)
                    except:
                        continue

                if largest_table:
                    table_name, display_name = largest_table
                    print(f"Sample from {display_name} ({largest_count:,} rows):")
                    print("-" * 80)

                    # Get sample records
                    sample_result = await session.execute(text(f"""
                        SELECT * FROM {table_name}
                        ORDER BY extracted_at DESC
                        LIMIT 3
                    """))

                    rows = sample_result.fetchall()
                    if rows:
                        # Get column names
                        columns = list(rows[0]._mapping.keys())

                        # Print header
                        header = " | ".join([col[:15] for col in columns[:5]])  # First 5 columns
                        print(header)
                        print("-" * len(header))

                        # Print sample rows
                        for row in rows:
                            values = []
                            for i, col in enumerate(columns[:5]):  # First 5 columns
                                val = row._mapping[col]
                                if val is None:
                                    val_str = "NULL"
                                elif isinstance(val, str):
                                    val_str = val[:15]
                                else:
                                    val_str = str(val)[:15]
                                values.append(val_str)
                            print(" | ".join(values))

                # Check analytics views
                print("\n" + "="*80)
                print("ANALYTICS VIEWS VERIFICATION")
                print("="*80)

                analytics_views = [
                    ("unified_analytics.campaign_performance", "Campaign Performance View"),
                    ("unified_analytics.partner_pipeline_health", "Partner Pipeline Health View"),
                    ("unified_analytics.executive_dashboard", "Executive Dashboard View"),
                ]

                for view_name, display_name in analytics_views:
                    try:
                        result = await session.execute(text(f"SELECT COUNT(*) FROM {view_name}"))
                        count = result.scalar()
                        status = "✓" if count > 0 else "✗"
                        print(f"{status} {display_name:.<40} {count:>8,} rows")
                    except Exception as e:
                        print(f"✗ {display_name:.<40} ERROR: {str(e)}")

            print("="*80)
            return True

    except Exception as e:
        logger.error(f"Database verification failed: {str(e)}")
        print(f"ERROR: Database verification failed: {str(e)}")
        return False


async def main():
    """Main verification function."""
    setup_logging()

    success = await verify_database_data()
    return 0 if success else 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except Exception as e:
        print(f"Verification script failed: {str(e)}")
        sys.exit(1)