#!/usr/bin/env python3
"""
Historical Data Migration Script
Migrates existing data from DuckDB to PostgreSQL
"""

import asyncio
import sys
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.config import settings
from core.database import get_db_session, init_database
from core.shared import setup_logging, get_logger, PerformanceTimer
from etl.unified.loaders.ultimate_dms_loader import UltimateDMSLoader
from etl.unified.loaders.budget_waterfall_loader import BudgetWaterfallLoader
from etl.unified.loaders.salesforce_loader import SalesforcePartnerPipelineLoader


class DataMigrator:
    """Migrates historical data from DuckDB to PostgreSQL."""

    def __init__(self):
        self.logger = get_logger("data_migration")
        self.migration_stats = {
            "tables_migrated": 0,
            "total_rows_migrated": 0,
            "migration_errors": [],
            "start_time": None,
            "end_time": None
        }

    async def migrate_all_data(self) -> Dict[str, Any]:
        """Migrate all historical data from DuckDB to PostgreSQL."""
        self.logger.info("Starting complete data migration from DuckDB to PostgreSQL")
        self.migration_stats["start_time"] = datetime.now(timezone.utc)

        try:
            # Initialize PostgreSQL database
            await init_database()

            # Migration tasks
            migration_tasks = [
                ("ultimate_dms_campaigns", self._migrate_ultimate_dms),
                ("budget_waterfall_client", self._migrate_budget_waterfall),
                ("sf_partner_pipeline", self._migrate_sf_partner_pipeline),
            ]

            migration_results = {}

            for table_name, migration_func in migration_tasks:
                try:
                    self.logger.info(f"Migrating {table_name}...")
                    result = await migration_func()
                    migration_results[table_name] = result

                    if result["status"] == "success":
                        self.migration_stats["tables_migrated"] += 1
                        self.migration_stats["total_rows_migrated"] += result.get("rows_migrated", 0)
                    else:
                        self.migration_stats["migration_errors"].append({
                            "table": table_name,
                            "error": result.get("error", "Unknown error")
                        })

                except Exception as e:
                    error_msg = f"Failed to migrate {table_name}: {str(e)}"
                    self.logger.error(error_msg)
                    migration_results[table_name] = {
                        "status": "failed",
                        "error": str(e)
                    }
                    self.migration_stats["migration_errors"].append({
                        "table": table_name,
                        "error": str(e)
                    })

            self.migration_stats["end_time"] = datetime.now(timezone.utc)

            # Summary
            total_duration = (self.migration_stats["end_time"] - self.migration_stats["start_time"]).total_seconds()
            self.logger.info(f"Migration completed in {total_duration:.2f} seconds")
            self.logger.info(f"Tables migrated: {self.migration_stats['tables_migrated']}")
            self.logger.info(f"Total rows migrated: {self.migration_stats['total_rows_migrated']}")

            if self.migration_stats["migration_errors"]:
                self.logger.warning(f"Migration errors: {len(self.migration_stats['migration_errors'])}")

            return {
                "status": "completed",
                "migration_stats": self.migration_stats,
                "table_results": migration_results
            }

        except Exception as e:
            self.migration_stats["end_time"] = datetime.now(timezone.utc)
            self.logger.error(f"Migration failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e),
                "migration_stats": self.migration_stats
            }

    async def _migrate_ultimate_dms(self) -> Dict[str, Any]:
        """Migrate Ultimate DMS campaign data."""
        return await self._migrate_table_from_duckdb(
            source_duckdb=settings.LEGACY_HEARTBEAT_DUCKDB,
            source_table="ultimate_dms_historical",
            loader_class=UltimateDMSLoader,
            table_name="ultimate_dms_campaigns"
        )

    async def _migrate_budget_waterfall(self) -> Dict[str, Any]:
        """Migrate Budget Waterfall client data."""
        return await self._migrate_table_from_duckdb(
            source_duckdb=settings.LEGACY_HEARTBEAT_DUCKDB,
            source_table="budget_waterfall_client_historical",
            loader_class=BudgetWaterfallLoader,
            table_name="budget_waterfall_client"
        )

    async def _migrate_sf_partner_pipeline(self) -> Dict[str, Any]:
        """Migrate Salesforce partner pipeline data."""
        return await self._migrate_table_from_duckdb(
            source_duckdb=settings.LEGACY_HEARTBEAT_DUCKDB,
            source_table="sf_partner_pipeline_historical_current",
            loader_class=SalesforcePartnerPipelineLoader,
            table_name="sf_partner_pipeline"
        )

    async def _migrate_table_from_duckdb(self, source_duckdb: str, source_table: str,
                                       loader_class: type, table_name: str) -> Dict[str, Any]:
        """
        Migrate a specific table from DuckDB to PostgreSQL.

        Args:
            source_duckdb: Path to source DuckDB file
            source_table: Source table name in DuckDB
            loader_class: PostgreSQL loader class
            table_name: Target table name for logging
        """
        try:
            with PerformanceTimer(f"Migration of {table_name}", self.logger.name):
                # Check if source DuckDB exists
                if not Path(source_duckdb).exists():
                    return {
                        "status": "skipped",
                        "reason": f"Source DuckDB not found: {source_duckdb}"
                    }

                # Connect to DuckDB and export data
                self.logger.info(f"Connecting to source DuckDB: {source_duckdb}")
                duckdb_conn = duckdb.connect(str(source_duckdb))

                # Check if source table exists
                try:
                    table_check = duckdb_conn.execute(
                        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{source_table}'"
                    ).fetchone()

                    if not table_check or table_check[0] == 0:
                        return {
                            "status": "skipped",
                            "reason": f"Source table {source_table} not found in DuckDB"
                        }
                except Exception as e:
                    return {
                        "status": "skipped",
                        "reason": f"Could not check source table: {str(e)}"
                    }

                # Get row count
                row_count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]

                if row_count == 0:
                    return {
                        "status": "skipped",
                        "reason": f"Source table {source_table} is empty"
                    }

                self.logger.info(f"Found {row_count} rows in {source_table}")

                # Export to temporary Parquet file
                temp_export_path = Path(settings.DATA_ROOT) / "migration" / f"{table_name}_export.parquet"
                temp_export_path.parent.mkdir(parents=True, exist_ok=True)

                self.logger.info(f"Exporting to temporary Parquet file: {temp_export_path}")

                # Export with metadata
                export_sql = f"""
                COPY (
                    SELECT *,
                           CAST(CURRENT_DATE AS DATE) as extract_date,
                           CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE) as extracted_at
                    FROM {source_table}
                ) TO '{temp_export_path}' (FORMAT PARQUET)
                """

                duckdb_conn.execute(export_sql)
                duckdb_conn.close()

                # Load into PostgreSQL using unified loader
                self.logger.info(f"Loading into PostgreSQL using {loader_class.__name__}")
                loader = loader_class()

                # Load the exported data
                load_result = await loader.load_from_parquet(
                    temp_export_path,
                    extract_date=datetime.now(timezone.utc).strftime("%Y-%m-%d")
                )

                # Clean up temporary file
                if temp_export_path.exists():
                    temp_export_path.unlink()

                if load_result["status"] == "success":
                    rows_migrated = load_result.get("historical", {}).get("rows_inserted", 0)
                    self.logger.info(f"Successfully migrated {rows_migrated} rows for {table_name}")

                    return {
                        "status": "success",
                        "rows_migrated": rows_migrated,
                        "source_rows": row_count,
                        "load_result": load_result
                    }
                else:
                    return {
                        "status": "failed",
                        "error": f"PostgreSQL load failed: {load_result}"
                    }

        except Exception as e:
            self.logger.error(f"Migration failed for {table_name}: {str(e)}")
            return {
                "status": "failed",
                "error": str(e)
            }

    async def verify_migration(self) -> Dict[str, Any]:
        """Verify the migration by comparing row counts."""
        self.logger.info("Verifying migration...")

        verification_results = {}

        try:
            # Tables to verify
            tables_to_verify = [
                ("ultimate_dms_historical", "heartbeat_core.ultimate_dms_campaigns"),
                ("budget_waterfall_client_historical", "heartbeat_core.budget_waterfall_client"),
                ("sf_partner_pipeline_historical_current", "heartbeat_salesforce.sf_partner_pipeline"),
            ]

            async with get_db_session() as session:
                for source_table, target_table in tables_to_verify:
                    try:
                        # Get PostgreSQL count
                        pg_result = await session.execute(f"SELECT COUNT(*) FROM {target_table}")
                        pg_count = pg_result.scalar()

                        # Get DuckDB count (if available)
                        duckdb_count = 0
                        if Path(settings.LEGACY_HEARTBEAT_DUCKDB).exists():
                            duckdb_conn = duckdb.connect(str(settings.LEGACY_HEARTBEAT_DUCKDB))
                            try:
                                duckdb_result = duckdb_conn.execute(f"SELECT COUNT(*) FROM {source_table}")
                                duckdb_count = duckdb_result.fetchone()[0]
                            except:
                                duckdb_count = 0
                            duckdb_conn.close()

                        verification_results[target_table] = {
                            "postgresql_count": pg_count,
                            "duckdb_count": duckdb_count,
                            "migration_complete": pg_count > 0,
                            "counts_match": pg_count == duckdb_count if duckdb_count > 0 else True
                        }

                        self.logger.info(f"{target_table}: PostgreSQL={pg_count}, DuckDB={duckdb_count}")

                    except Exception as e:
                        verification_results[target_table] = {
                            "error": str(e),
                            "migration_complete": False
                        }

            return {
                "status": "completed",
                "verification_results": verification_results
            }

        except Exception as e:
            self.logger.error(f"Migration verification failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e)
            }


async def main():
    """Main migration function."""
    # Setup logging
    setup_logging()
    logger = get_logger("data_migration")

    logger.info("Starting historical data migration...")

    try:
        migrator = DataMigrator()

        # Run migration
        migration_result = await migrator.migrate_all_data()

        if migration_result["status"] == "completed":
            logger.info("Migration completed successfully")

            # Run verification
            verification_result = await migrator.verify_migration()

            if verification_result["status"] == "completed":
                logger.info("Migration verification completed")

                # Print summary
                print("\n" + "="*60)
                print("MIGRATION SUMMARY")
                print("="*60)
                print(f"Tables migrated: {migration_result['migration_stats']['tables_migrated']}")
                print(f"Total rows migrated: {migration_result['migration_stats']['total_rows_migrated']}")
                print(f"Migration errors: {len(migration_result['migration_stats']['migration_errors'])}")

                if migration_result['migration_stats']['migration_errors']:
                    print("\nErrors:")
                    for error in migration_result['migration_stats']['migration_errors']:
                        print(f"  - {error['table']}: {error['error']}")

                print("\nVerification Results:")
                for table, result in verification_result['verification_results'].items():
                    if 'error' in result:
                        print(f"  - {table}: ERROR - {result['error']}")
                    else:
                        status = "✓" if result['migration_complete'] else "✗"
                        print(f"  - {table}: {status} PostgreSQL={result['postgresql_count']}, DuckDB={result['duckdb_count']}")

                print("="*60)
                return 0
            else:
                logger.error("Migration verification failed")
                return 1
        else:
            logger.error("Migration failed")
            return 1

    except Exception as e:
        logger.error(f"Migration script failed: {str(e)}")
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nMigration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Migration script failed: {str(e)}")
        sys.exit(1)