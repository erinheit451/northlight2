"""
Ultimate DMS Campaign Data Loader
PostgreSQL loader for Ultimate DMS campaign performance data
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


class UltimateDMSLoader(BasePostgresLoader):
    """Loader for Ultimate DMS campaign data."""

    def __init__(self):
        super().__init__("ultimate_dms", "heartbeat_core")

    def get_staging_table_name(self) -> str:
        return "ultimate_dms_campaigns_staging"

    def get_historical_table_name(self) -> str:
        return "ultimate_dms_campaigns"

    def get_deduplication_columns(self) -> List[str]:
        return ["campaign_name", "advertiser_name", "extract_date"]

    async def create_table_schema(self, session: AsyncSession) -> None:
        """Create Ultimate DMS table schema if it doesn't exist."""

        # Create staging table
        staging_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_prefix}{self.get_staging_table_name()} (
            id UUID DEFAULT gen_random_uuid(),
            last_active DATE,
            channel VARCHAR(100),
            business_name TEXT,
            business_id VARCHAR(255),
            advertiser_name TEXT,
            primary_user_name VARCHAR(255),
            am VARCHAR(255),
            am_manager VARCHAR(255),
            optimizer1_manager VARCHAR(255),
            optimizer1 VARCHAR(255),
            optimizer2_manager VARCHAR(255),
            optimizer2 VARCHAR(255),
            maid VARCHAR(255),
            mcid_clicks INTEGER,
            mcid_leads INTEGER,
            mcid VARCHAR(255),
            campaign_name TEXT,
            campaign_id VARCHAR(255),
            product VARCHAR(255),
            offer_name TEXT,
            finance_product VARCHAR(255),
            tracking_method VARCHAR(255),
            all_reviews_p30 INTEGER,
            io_cycle INTEGER,
            avg_cycle_length DECIMAL(8,2),
            running_cid_leads INTEGER,
            amount_spent DECIMAL(15,2),
            days_elapsed INTEGER,
            utilization DECIMAL(8,4),
            campaign_performance_rating VARCHAR(50),
            bc DECIMAL(8,2),
            bsc DECIMAL(8,2),
            campaign_budget DECIMAL(15,2),
            budget_10 DECIMAL(15,2),
            budget_25 DECIMAL(15,2),
            budget_average DECIMAL(15,2),
            budget_75 DECIMAL(15,2),
            budget_90 DECIMAL(15,2),
            cpl_agreed DECIMAL(10,2),
            cid_cpl DECIMAL(10,2),
            cpl_mcid DECIMAL(10,2),
            cpl_last15_days DECIMAL(10,2),
            cpl_15_to_30days DECIMAL(10,2),
            cpl_30_to_60days DECIMAL(10,2),
            cplead_10 DECIMAL(10,2),
            cplead_25 DECIMAL(10,2),
            cplead_average DECIMAL(10,2),
            cplead_75 DECIMAL(10,2),
            cplead_90 DECIMAL(10,2),
            mcid_avg_cpc DECIMAL(10,2),
            cpclick_10 DECIMAL(10,2),
            cpclick_25 DECIMAL(10,2),
            cpclick_average DECIMAL(10,2),
            cpclick_75 DECIMAL(10,2),
            cpclick_90 DECIMAL(10,2),

            -- Metadata columns
            extract_date DATE NOT NULL,
            extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        """

        # Create historical table (same structure)
        historical_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_prefix}{self.get_historical_table_name()} (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            last_active DATE,
            channel VARCHAR(100),
            business_name TEXT,
            business_id VARCHAR(255),
            advertiser_name TEXT,
            primary_user_name VARCHAR(255),
            am VARCHAR(255),
            am_manager VARCHAR(255),
            optimizer1_manager VARCHAR(255),
            optimizer1 VARCHAR(255),
            optimizer2_manager VARCHAR(255),
            optimizer2 VARCHAR(255),
            maid VARCHAR(255),
            mcid_clicks INTEGER,
            mcid_leads INTEGER,
            mcid VARCHAR(255),
            campaign_name TEXT,
            campaign_id VARCHAR(255),
            product VARCHAR(255),
            offer_name TEXT,
            finance_product VARCHAR(255),
            tracking_method VARCHAR(255),
            all_reviews_p30 INTEGER,
            io_cycle INTEGER,
            avg_cycle_length DECIMAL(8,2),
            running_cid_leads INTEGER,
            amount_spent DECIMAL(15,2),
            days_elapsed INTEGER,
            utilization DECIMAL(8,4),
            campaign_performance_rating VARCHAR(50),
            bc DECIMAL(8,2),
            bsc DECIMAL(8,2),
            campaign_budget DECIMAL(15,2),
            budget_10 DECIMAL(15,2),
            budget_25 DECIMAL(15,2),
            budget_average DECIMAL(15,2),
            budget_75 DECIMAL(15,2),
            budget_90 DECIMAL(15,2),
            cpl_agreed DECIMAL(10,2),
            cid_cpl DECIMAL(10,2),
            cpl_mcid DECIMAL(10,2),
            cpl_last15_days DECIMAL(10,2),
            cpl_15_to_30days DECIMAL(10,2),
            cpl_30_to_60days DECIMAL(10,2),
            cplead_10 DECIMAL(10,2),
            cplead_25 DECIMAL(10,2),
            cplead_average DECIMAL(10,2),
            cplead_75 DECIMAL(10,2),
            cplead_90 DECIMAL(10,2),
            mcid_avg_cpc DECIMAL(10,2),
            cpclick_10 DECIMAL(10,2),
            cpclick_25 DECIMAL(10,2),
            cpclick_average DECIMAL(10,2),
            cpclick_75 DECIMAL(10,2),
            cpclick_90 DECIMAL(10,2),

            -- Metadata columns
            extract_date DATE NOT NULL,
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
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_campaign_advertiser ON {historical_table}(campaign_name, advertiser_name)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_extract_date ON {historical_table}(extract_date)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_extracted_at ON {historical_table}(extracted_at)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_channel ON {historical_table}(channel)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_product ON {historical_table}(product)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_amount_spent ON {historical_table}(amount_spent) WHERE amount_spent IS NOT NULL",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_leads ON {historical_table}(running_cid_leads) WHERE running_cid_leads > 0"
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
async def load_ultimate_dms_data(parquet_file: Path, extract_date: str = None):
    """
    Load Ultimate DMS data from Parquet file to PostgreSQL.

    Args:
        parquet_file: Path to the Parquet file
        extract_date: Optional extraction date

    Returns:
        Load statistics dictionary
    """
    loader = UltimateDMSLoader()
    return await loader.load_from_parquet(parquet_file, extract_date)