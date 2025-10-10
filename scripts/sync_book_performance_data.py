"""
Sync Performance Data to Book Schema
Copies latest data from heartbeat_performance to book schema tables.
Run this after ETL completes to keep book tables current.
"""

import asyncio
import logging
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.database import get_db_session, init_database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def sync_spend_revenue_performance():
    """
    Sync spend_revenue_performance data from heartbeat_performance to book schema.

    This ensures the book.raw_heartbeat_spend_revenue_performance_current table
    stays in sync with the source data extracted by ETL.
    """
    await init_database()

    async with get_db_session() as session:
        try:
            logger.info("Starting sync of spend_revenue_performance to book schema...")

            # Step 1: Check data freshness in source table
            check_query = text("""
                SELECT
                    COUNT(*) as total_rows,
                    MAX(report_month) as latest_month,
                    COUNT(DISTINCT campaign_id) as campaign_count,
                    COUNT(DISTINCT maid) as advertiser_count
                FROM heartbeat_performance.spend_revenue_performance
            """)

            result = await session.execute(check_query)
            source_stats = result.fetchone()

            logger.info(f"Source table stats: {source_stats.total_rows} rows, "
                       f"{source_stats.campaign_count} campaigns, "
                       f"{source_stats.advertiser_count} advertisers, "
                       f"latest data: {source_stats.latest_month}")

            if source_stats.total_rows == 0:
                logger.warning("Source table is empty! Skipping sync.")
                return False

            # Step 2: Create or recreate the book table with correct schema
            # This ensures column names and types match the source
            logger.info("Recreating book table with correct schema...")

            # Drop table
            await session.execute(text("""
                DROP TABLE IF EXISTS book.raw_heartbeat_spend_revenue_performance_current CASCADE
            """))
            await session.commit()

            # Create table structure
            await session.execute(text("""
                CREATE TABLE book.raw_heartbeat_spend_revenue_performance_current AS
                SELECT
                    campaign_id,
                    maid,
                    report_month,
                    cpl as cost_per_lead,
                    leads_generated as leads,
                    amount_spent as spend,
                    revenue_generated as revenue,
                    advertiser_name as business_name,
                    cobrand,
                    campaign_name,
                    snapshot_date,
                    extracted_at as created_at,
                    extracted_at as updated_at
                FROM heartbeat_performance.spend_revenue_performance
                WHERE 1=0
            """))
            await session.commit()

            # Add indexes
            await session.execute(text("""
                CREATE INDEX idx_book_spend_rev_campaign_id
                    ON book.raw_heartbeat_spend_revenue_performance_current(campaign_id)
            """))
            await session.execute(text("""
                CREATE INDEX idx_book_spend_rev_maid
                    ON book.raw_heartbeat_spend_revenue_performance_current(maid)
            """))
            await session.execute(text("""
                CREATE INDEX idx_book_spend_rev_report_month
                    ON book.raw_heartbeat_spend_revenue_performance_current(report_month)
            """))
            await session.execute(text("""
                CREATE INDEX idx_book_spend_rev_business_name
                    ON book.raw_heartbeat_spend_revenue_performance_current(business_name)
            """))
            await session.commit()

            # Step 3: Copy data from source to book table
            sync_query = text("""
                INSERT INTO book.raw_heartbeat_spend_revenue_performance_current
                SELECT
                    campaign_id,
                    maid,
                    report_month,
                    cpl as cost_per_lead,
                    leads_generated as leads,
                    amount_spent as spend,
                    revenue_generated as revenue,
                    advertiser_name as business_name,
                    cobrand,
                    campaign_name,
                    snapshot_date,
                    extracted_at as created_at,
                    extracted_at as updated_at
                FROM heartbeat_performance.spend_revenue_performance
            """)

            logger.info("Copying data to book schema...")
            result = await session.execute(sync_query)
            await session.commit()

            # Step 4: Verify sync succeeded
            verify_query = text("""
                SELECT
                    COUNT(*) as total_rows,
                    MAX(report_month) as latest_month,
                    COUNT(DISTINCT campaign_id) as campaign_count,
                    COUNT(DISTINCT maid) as advertiser_count
                FROM book.raw_heartbeat_spend_revenue_performance_current
            """)

            result = await session.execute(verify_query)
            book_stats = result.fetchone()

            logger.info(f"Book table stats: {book_stats.total_rows} rows, "
                       f"{book_stats.campaign_count} campaigns, "
                       f"{book_stats.advertiser_count} advertisers, "
                       f"latest data: {book_stats.latest_month}")

            # Step 5: Validate sync
            if book_stats.total_rows != source_stats.total_rows:
                logger.error(f"Sync validation FAILED! Row count mismatch: "
                           f"source={source_stats.total_rows}, book={book_stats.total_rows}")
                return False

            if book_stats.latest_month != source_stats.latest_month:
                logger.warning(f"Latest month mismatch: "
                             f"source={source_stats.latest_month}, book={book_stats.latest_month}")

            logger.info("[SUCCESS] Sync completed successfully!")
            return True

        except Exception as e:
            logger.error(f"Error during sync: {e}")
            await session.rollback()
            raise


async def sync_all_book_tables():
    """
    Sync all performance tables from heartbeat schemas to book schema.
    Add additional table syncs here as needed.
    """
    start_time = datetime.now()
    logger.info(f"=== Starting Book Schema Sync at {start_time} ===")

    success = await sync_spend_revenue_performance()

    # TODO: Add other table syncs here
    # await sync_budget_waterfall()
    # await sync_ultimate_dms()

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logger.info(f"=== Book Schema Sync completed in {duration:.2f}s ===")

    if not success:
        raise Exception("Sync failed validation")


if __name__ == "__main__":
    asyncio.run(sync_all_book_tables())
