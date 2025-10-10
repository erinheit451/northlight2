"""
Monitor data freshness in book schema tables.
Alerts if book tables are stale compared to source.
"""

import asyncio
import logging
import sys
from pathlib import Path
from sqlalchemy import text

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.database import get_db_session, init_database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def check_data_freshness():
    """Check if book tables are in sync with source tables."""

    await init_database()

    async with get_db_session() as session:
        query = text("""
            WITH source_stats AS (
                SELECT
                    MAX(report_month) as latest_month,
                    MAX(extracted_at) as last_update,
                    COUNT(*) as row_count
                FROM heartbeat_performance.spend_revenue_performance
            ),
            book_stats AS (
                SELECT
                    MAX(report_month) as latest_month,
                    MAX(updated_at) as last_update,
                    COUNT(*) as row_count
                FROM book.raw_heartbeat_spend_revenue_performance_current
            )
            SELECT
                source_stats.latest_month as source_latest,
                book_stats.latest_month as book_latest,
                source_stats.last_update as source_update,
                book_stats.last_update as book_update,
                source_stats.row_count as source_rows,
                book_stats.row_count as book_rows,
                EXTRACT(EPOCH FROM (source_stats.last_update - COALESCE(book_stats.last_update, '2000-01-01'::timestamp)))/3600 as hours_behind
            FROM source_stats
            LEFT JOIN book_stats ON true
        """)

        result = await session.execute(query)
        stats = result.fetchone()

        issues = []

        # Check if book table is behind
        if stats.hours_behind and stats.hours_behind > 24:
            issues.append(f"Book table is {stats.hours_behind:.1f} hours behind source")

        # Check row count mismatch
        if stats.book_rows is None:
            issues.append("Book table does not exist or is empty")
        else:
            row_diff = abs(stats.source_rows - stats.book_rows)
            if row_diff > 100:
                issues.append(f"Row count mismatch: source={stats.source_rows}, book={stats.book_rows}")

        # Check if latest data month differs
        if stats.source_latest != stats.book_latest:
            issues.append(f"Latest month mismatch: source={stats.source_latest}, book={stats.book_latest}")

        if issues:
            logger.warning("[WARNING] Book schema data freshness issues detected:")
            for issue in issues:
                logger.warning(f"  - {issue}")
            print("\n[WARNING] ISSUES DETECTED:")
            for issue in issues:
                print(f"  - {issue}")
            print("\nRun 'python scripts/run_book_sync.py' to sync data.")
            return False
        else:
            logger.info("[SUCCESS] Book schema is in sync with source data")
            print("[SUCCESS] Book schema is in sync with source data")
            print(f"  - Source rows: {stats.source_rows}")
            print(f"  - Book rows: {stats.book_rows}")
            print(f"  - Latest month: {stats.source_latest}")
            return True


if __name__ == "__main__":
    try:
        result = asyncio.run(check_data_freshness())
        sys.exit(0 if result else 1)
    except Exception as e:
        logger.error(f"Error checking data freshness: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
