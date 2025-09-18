"""
Budget Waterfall Client Data Loader
PostgreSQL loader for budget waterfall and client metrics data
"""

import sys
from pathlib import Path
from typing import List
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from .base import BasePostgresLoader
from core.shared import log_step


class BudgetWaterfallLoader(BasePostgresLoader):
    """Loader for budget waterfall client data."""

    def __init__(self):
        super().__init__("budget_waterfall", "heartbeat_core")

    def get_staging_table_name(self) -> str:
        return "budget_waterfall_client_staging"

    def get_historical_table_name(self) -> str:
        return "budget_waterfall_client"

    def get_deduplication_columns(self) -> List[str]:
        return ["business_id", "advertiser_name", "snapshot_date"]

    async def create_table_schema(self, session: AsyncSession) -> None:
        """Create Budget Waterfall table schema if it doesn't exist."""

        # Create staging table
        staging_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_prefix}{self.get_staging_table_name()} (
            id UUID DEFAULT gen_random_uuid(),
            business_id VARCHAR(255),
            advertiser_name TEXT,
            channel VARCHAR(100),
            office VARCHAR(100),
            area VARCHAR(100),
            budgets DECIMAL(15,2),
            som_budgets DECIMAL(15,2),
            net_new_budgets DECIMAL(15,2),
            net_change_pct DECIMAL(8,4),
            starting_clients INTEGER,
            ending_clients INTEGER,
            new_clients INTEGER,
            churned_clients INTEGER,

            -- Metadata columns
            snapshot_date DATE NOT NULL,
            extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        """

        # Create historical table (same structure with primary key)
        historical_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_prefix}{self.get_historical_table_name()} (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            business_id VARCHAR(255),
            advertiser_name TEXT,
            channel VARCHAR(100),
            office VARCHAR(100),
            area VARCHAR(100),
            budgets DECIMAL(15,2),
            som_budgets DECIMAL(15,2),
            net_new_budgets DECIMAL(15,2),
            net_change_pct DECIMAL(8,4),
            starting_clients INTEGER,
            ending_clients INTEGER,
            new_clients INTEGER,
            churned_clients INTEGER,

            -- Metadata columns
            snapshot_date DATE NOT NULL,
            extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        """

        try:
            # Create staging table
            await session.execute(text(staging_sql))
            log_step(
                self.loader_name,
                f"Created/verified staging table: {self.get_staging_table_name()}",
                logger_name=self.logger.name
            )

            # Create historical table
            await session.execute(text(historical_sql))
            log_step(
                self.loader_name,
                f"Created/verified historical table: {self.get_historical_table_name()}",
                logger_name=self.logger.name
            )

            # Create indexes for performance
            await self._create_indexes(session)

        except Exception as e:
            self.logger.error(f"Failed to create table schema: {e}")
            raise

    async def _create_indexes(self, session: AsyncSession) -> None:
        """Create performance indexes."""
        historical_table = f"{self.table_prefix}{self.get_historical_table_name()}"

        indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_business_id ON {historical_table}(business_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_advertiser ON {historical_table}(advertiser_name)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_snapshot_date ON {historical_table}(snapshot_date)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_channel ON {historical_table}(channel)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_office ON {historical_table}(office)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_budgets ON {historical_table}(budgets) WHERE budgets IS NOT NULL",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_clients ON {historical_table}(ending_clients) WHERE ending_clients IS NOT NULL"
        ]

        for index_sql in indexes:
            try:
                await session.execute(text(index_sql))
            except Exception as e:
                self.logger.warning(f"Failed to create index: {e}")

        log_step(
            self.loader_name,
            f"Created performance indexes for {historical_table}",
            logger_name=self.logger.name
        )


# Convenience function for external usage
async def load_budget_waterfall_data(parquet_file: Path, extract_date: str = None):
    """
    Load Budget Waterfall data from Parquet file to PostgreSQL.

    Args:
        parquet_file: Path to the Parquet file
        extract_date: Optional extraction date

    Returns:
        Load statistics dictionary
    """
    loader = BudgetWaterfallLoader()
    return await loader.load_from_parquet(parquet_file, extract_date)