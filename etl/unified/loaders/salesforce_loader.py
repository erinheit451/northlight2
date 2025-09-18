"""
Salesforce Data Loader
PostgreSQL loader for Salesforce partner pipeline and opportunity data
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


class SalesforcePartnerPipelineLoader(BasePostgresLoader):
    """Loader for Salesforce partner pipeline data."""

    def __init__(self):
        super().__init__("sf_partner_pipeline", "heartbeat_salesforce")

    def get_staging_table_name(self) -> str:
        return "sf_partner_pipeline_staging"

    def get_historical_table_name(self) -> str:
        return "sf_partner_pipeline"

    def get_deduplication_columns(self) -> List[str]:
        return ["opportunity_id", "partner_name", "last_modified_date"]

    async def create_table_schema(self, session: AsyncSession) -> None:
        """Create Salesforce Partner Pipeline table schema."""

        # Create staging table
        staging_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_prefix}{self.get_staging_table_name()} (
            id UUID DEFAULT gen_random_uuid(),
            opportunity_id VARCHAR(255),
            opportunity_name TEXT,
            partner_name TEXT,
            account_name TEXT,
            stage VARCHAR(100),
            amount DECIMAL(15,2),
            close_date DATE,
            probability DECIMAL(5,2),
            lead_source VARCHAR(255),
            campaign_source VARCHAR(255),
            type VARCHAR(100),
            next_step TEXT,
            description TEXT,
            owner_name VARCHAR(255),
            created_date DATE,
            last_modified_date TIMESTAMP WITH TIME ZONE,
            fiscal_quarter VARCHAR(10),
            fiscal_year INTEGER,

            -- Additional fields from Heartbeat
            modification_amount DECIMAL(15,2),
            partner_tier VARCHAR(50),
            region VARCHAR(100),
            territory VARCHAR(100),

            -- Metadata columns
            extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
            extract_date DATE NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        """

        # Create historical table (same structure with primary key)
        historical_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_prefix}{self.get_historical_table_name()} (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            opportunity_id VARCHAR(255),
            opportunity_name TEXT,
            partner_name TEXT,
            account_name TEXT,
            stage VARCHAR(100),
            amount DECIMAL(15,2),
            close_date DATE,
            probability DECIMAL(5,2),
            lead_source VARCHAR(255),
            campaign_source VARCHAR(255),
            type VARCHAR(100),
            next_step TEXT,
            description TEXT,
            owner_name VARCHAR(255),
            created_date DATE,
            last_modified_date TIMESTAMP WITH TIME ZONE,
            fiscal_quarter VARCHAR(10),
            fiscal_year INTEGER,

            -- Additional fields from Heartbeat
            modification_amount DECIMAL(15,2),
            partner_tier VARCHAR(50),
            region VARCHAR(100),
            territory VARCHAR(100),

            -- Metadata columns
            extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
            extract_date DATE NOT NULL,
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
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_opportunity_id ON {historical_table}(opportunity_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_partner_name ON {historical_table}(partner_name)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_stage ON {historical_table}(stage)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_close_date ON {historical_table}(close_date)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_amount ON {historical_table}(amount) WHERE amount IS NOT NULL",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_last_modified ON {historical_table}(last_modified_date)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_fiscal ON {historical_table}(fiscal_year, fiscal_quarter)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_extract_date ON {historical_table}(extract_date)"
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


class SalesforcePartnerCallsLoader(BasePostgresLoader):
    """Loader for Salesforce partner calls data."""

    def __init__(self):
        super().__init__("sf_partner_calls", "heartbeat_salesforce")

    def get_staging_table_name(self) -> str:
        return "sf_partner_calls_staging"

    def get_historical_table_name(self) -> str:
        return "sf_partner_calls"

    def get_deduplication_columns(self) -> List[str]:
        return ["call_id", "partner_name", "call_date"]

    async def create_table_schema(self, session: AsyncSession) -> None:
        """Create Salesforce Partner Calls table schema."""

        # Create staging table
        staging_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_prefix}{self.get_staging_table_name()} (
            id UUID DEFAULT gen_random_uuid(),
            call_id VARCHAR(255),
            partner_name TEXT,
            account_name TEXT,
            call_date DATE,
            call_type VARCHAR(100),
            call_duration_minutes INTEGER,
            call_subject TEXT,
            call_description TEXT,
            call_outcome VARCHAR(255),
            follow_up_required BOOLEAN,
            follow_up_date DATE,
            owner_name VARCHAR(255),
            created_date DATE,
            last_modified_date TIMESTAMP WITH TIME ZONE,

            -- Additional context fields
            opportunity_id VARCHAR(255),
            campaign_context VARCHAR(255),
            call_priority VARCHAR(50),
            call_status VARCHAR(50),

            -- Metadata columns
            extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
            extract_date DATE NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        """

        # Create historical table (same structure with primary key)
        historical_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_prefix}{self.get_historical_table_name()} (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            call_id VARCHAR(255),
            partner_name TEXT,
            account_name TEXT,
            call_date DATE,
            call_type VARCHAR(100),
            call_duration_minutes INTEGER,
            call_subject TEXT,
            call_description TEXT,
            call_outcome VARCHAR(255),
            follow_up_required BOOLEAN,
            follow_up_date DATE,
            owner_name VARCHAR(255),
            created_date DATE,
            last_modified_date TIMESTAMP WITH TIME ZONE,

            -- Additional context fields
            opportunity_id VARCHAR(255),
            campaign_context VARCHAR(255),
            call_priority VARCHAR(50),
            call_status VARCHAR(50),

            -- Metadata columns
            extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
            extract_date DATE NOT NULL,
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
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_call_id ON {historical_table}(call_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_partner_name ON {historical_table}(partner_name)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_call_date ON {historical_table}(call_date)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_call_type ON {historical_table}(call_type)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_outcome ON {historical_table}(call_outcome)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_follow_up ON {historical_table}(follow_up_required, follow_up_date)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.get_historical_table_name()}_extract_date ON {historical_table}(extract_date)"
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


# Convenience functions for external usage
async def load_sf_partner_pipeline_data(parquet_file: Path, extract_date: str = None):
    """Load Salesforce partner pipeline data from Parquet file to PostgreSQL."""
    loader = SalesforcePartnerPipelineLoader()
    return await loader.load_from_parquet(parquet_file, extract_date)


async def load_sf_partner_calls_data(parquet_file: Path, extract_date: str = None):
    """Load Salesforce partner calls data from Parquet file to PostgreSQL."""
    loader = SalesforcePartnerCallsLoader()
    return await loader.load_from_parquet(parquet_file, extract_date)