"""
Base PostgreSQL Loader for Unified ETL Pipeline
Provides common functionality for all PostgreSQL-based data loaders
"""

import asyncio
import logging
import sys
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from contextlib import asynccontextmanager

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import text, MetaData, Table
from sqlalchemy.ext.asyncio import AsyncSession

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from core.database import get_db_session
from core.shared import get_logger, log_step, PerformanceTimer
from core.config import settings


class BasePostgresLoader(ABC):
    """
    Base class for PostgreSQL data loaders.

    Provides common functionality for:
    - Database connections and transactions
    - Error handling and retries
    - Data validation and quality checks
    - Staging and historical table management
    - Performance monitoring and logging
    """

    def __init__(self, loader_name: str, schema_name: str):
        self.loader_name = loader_name
        self.schema_name = schema_name
        self.logger = get_logger(f"etl.{loader_name}")
        self.table_prefix = f"{schema_name}."

    @abstractmethod
    def get_staging_table_name(self) -> str:
        """Return the staging table name."""
        pass

    @abstractmethod
    def get_historical_table_name(self) -> str:
        """Return the historical table name."""
        pass

    @abstractmethod
    def get_deduplication_columns(self) -> List[str]:
        """Return columns used for deduplication."""
        pass

    @abstractmethod
    async def create_table_schema(self, session: AsyncSession) -> None:
        """Create table schema if it doesn't exist."""
        pass

    async def load_from_parquet(self, parquet_file: Path, extract_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Load data from Parquet file into PostgreSQL.

        Args:
            parquet_file: Path to the Parquet file
            extract_date: Optional extraction date for tracking

        Returns:
            Dictionary with load statistics
        """
        if extract_date is None:
            extract_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        with PerformanceTimer(f"{self.loader_name} load from {parquet_file.name}", self.logger.name):
            async with get_db_session() as session:
                try:
                    # Ensure schema and tables exist
                    await self.create_table_schema(session)

                    # Load and validate data
                    df = await self._load_parquet_data(parquet_file)
                    if df.empty:
                        self.logger.warning(f"No data found in {parquet_file}")
                        return {"status": "no_data", "rows_processed": 0}

                    # Add extraction metadata
                    df = self._add_extraction_metadata(df, extract_date)

                    # Load to staging table
                    staging_stats = await self._load_to_staging(session, df)

                    # Move to historical table with deduplication
                    historical_stats = await self._load_to_historical(session)

                    # Run data quality checks
                    quality_stats = await self._run_quality_checks(session)

                    await session.commit()

                    stats = {
                        "status": "success",
                        "extract_date": extract_date,
                        "staging": staging_stats,
                        "historical": historical_stats,
                        "quality": quality_stats,
                        "completed_at": datetime.now(timezone.utc).isoformat()
                    }

                    log_step(
                        self.loader_name,
                        f"Successfully loaded {staging_stats['rows_loaded']} rows to staging, "
                        f"{historical_stats['rows_inserted']} new rows to historical",
                        logger_name=self.logger.name
                    )

                    return stats

                except Exception as e:
                    await session.rollback()
                    log_step(
                        self.loader_name,
                        f"Load failed: {str(e)}",
                        is_error=True,
                        logger_name=self.logger.name
                    )
                    raise

    async def _load_parquet_data(self, parquet_file: Path) -> pd.DataFrame:
        """Load and validate Parquet data."""
        try:
            # Read Parquet file
            table = pq.read_table(parquet_file)
            df = table.to_pandas()

            log_step(
                self.loader_name,
                f"Loaded {len(df)} rows from {parquet_file.name}",
                logger_name=self.logger.name
            )

            return df

        except Exception as e:
            self.logger.error(f"Failed to read Parquet file {parquet_file}: {e}")
            raise

    def _add_extraction_metadata(self, df: pd.DataFrame, extract_date: str) -> pd.DataFrame:
        """Add extraction metadata to DataFrame."""
        df = df.copy()
        df['extracted_at'] = datetime.now(timezone.utc)
        df['extract_date'] = pd.to_datetime(extract_date).date()
        return df

    async def _load_to_staging(self, session: AsyncSession, df: pd.DataFrame) -> Dict[str, Any]:
        """Load data to staging table."""
        staging_table = self.get_staging_table_name()
        full_table_name = f"{self.table_prefix}{staging_table}"

        try:
            # Clear staging table
            await session.execute(text(f"TRUNCATE TABLE {full_table_name}"))

            # Insert data in batches
            batch_size = 1000
            total_rows = len(df)
            rows_inserted = 0

            for i in range(0, total_rows, batch_size):
                batch_df = df.iloc[i:i + batch_size]

                # Convert DataFrame to SQL INSERT
                await self._insert_dataframe_batch(session, batch_df, full_table_name)
                rows_inserted += len(batch_df)

                if rows_inserted % 5000 == 0:
                    log_step(
                        self.loader_name,
                        f"Inserted {rows_inserted}/{total_rows} rows to staging",
                        logger_name=self.logger.name
                    )

            return {
                "rows_loaded": rows_inserted,
                "table_name": full_table_name
            }

        except Exception as e:
            self.logger.error(f"Failed to load to staging table {full_table_name}: {e}")
            raise

    async def _insert_dataframe_batch(self, session: AsyncSession, df: pd.DataFrame, table_name: str):
        """Insert DataFrame batch using efficient SQL."""
        if df.empty:
            return

        # Get column names and prepare values
        columns = list(df.columns)
        column_list = ", ".join(columns)

        # Prepare parameterized values
        values_list = []
        params = {}

        for idx, row in df.iterrows():
            row_values = []
            for col in columns:
                param_name = f"p_{idx}_{col}"
                params[param_name] = row[col]
                row_values.append(f":{param_name}")
            values_list.append(f"({', '.join(row_values)})")

        values_str = ", ".join(values_list)

        insert_sql = f"""
        INSERT INTO {table_name} ({column_list})
        VALUES {values_str}
        """

        await session.execute(text(insert_sql), params)

    async def _load_to_historical(self, session: AsyncSession) -> Dict[str, Any]:
        """Move data from staging to historical with deduplication."""
        staging_table = f"{self.table_prefix}{self.get_staging_table_name()}"
        historical_table = f"{self.table_prefix}{self.get_historical_table_name()}"
        dedup_columns = self.get_deduplication_columns()

        try:
            # Build deduplication WHERE clause
            dedup_conditions = []
            for col in dedup_columns:
                dedup_conditions.append(
                    f"COALESCE(s.{col}, '') = COALESCE(h.{col}, '')"
                )

            dedup_where = " AND ".join(dedup_conditions)

            # Insert new rows that don't exist in historical
            insert_sql = f"""
            INSERT INTO {historical_table}
            SELECT s.*
            FROM {staging_table} s
            LEFT JOIN {historical_table} h ON ({dedup_where})
            WHERE h.{dedup_columns[0]} IS NULL
            """

            result = await session.execute(text(insert_sql))
            rows_inserted = result.rowcount

            # Get total historical count
            count_result = await session.execute(
                text(f"SELECT COUNT(*) FROM {historical_table}")
            )
            total_rows = count_result.scalar()

            return {
                "rows_inserted": rows_inserted,
                "total_rows": total_rows,
                "table_name": historical_table
            }

        except Exception as e:
            self.logger.error(f"Failed to load to historical table {historical_table}: {e}")
            raise

    async def _run_quality_checks(self, session: AsyncSession) -> Dict[str, Any]:
        """Run data quality checks on loaded data."""
        historical_table = f"{self.table_prefix}{self.get_historical_table_name()}"

        try:
            # Basic quality checks
            checks = {}

            # Row count check
            count_result = await session.execute(
                text(f"SELECT COUNT(*) FROM {historical_table}")
            )
            checks["total_rows"] = count_result.scalar()

            # Null value checks for key columns
            null_checks = {}
            for col in self.get_deduplication_columns():
                null_result = await session.execute(
                    text(f"SELECT COUNT(*) FROM {historical_table} WHERE {col} IS NULL")
                )
                null_checks[f"{col}_nulls"] = null_result.scalar()

            checks["null_values"] = null_checks

            # Date range check
            date_result = await session.execute(text(f"""
                SELECT
                    MIN(extracted_at) as min_date,
                    MAX(extracted_at) as max_date
                FROM {historical_table}
            """))
            date_range = date_result.fetchone()
            if date_range:
                checks["date_range"] = {
                    "min_date": date_range[0].isoformat() if date_range[0] else None,
                    "max_date": date_range[1].isoformat() if date_range[1] else None
                }

            return checks

        except Exception as e:
            self.logger.error(f"Quality checks failed for {historical_table}: {e}")
            return {"error": str(e)}

    async def get_load_status(self) -> Dict[str, Any]:
        """Get current load status and statistics."""
        async with get_db_session() as session:
            historical_table = f"{self.table_prefix}{self.get_historical_table_name()}"

            try:
                # Get latest load information
                result = await session.execute(text(f"""
                    SELECT
                        COUNT(*) as total_rows,
                        MAX(extracted_at) as last_load,
                        COUNT(DISTINCT extract_date) as unique_extract_dates
                    FROM {historical_table}
                """))

                stats = result.fetchone()

                return {
                    "loader_name": self.loader_name,
                    "table_name": historical_table,
                    "total_rows": stats[0] if stats else 0,
                    "last_load": stats[1].isoformat() if stats and stats[1] else None,
                    "unique_extract_dates": stats[2] if stats else 0,
                    "status": "healthy" if stats and stats[0] > 0 else "no_data"
                }

            except Exception as e:
                return {
                    "loader_name": self.loader_name,
                    "table_name": historical_table,
                    "status": "error",
                    "error": str(e)
                }