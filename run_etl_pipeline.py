#!/usr/bin/env python3
"""
Complete ETL Pipeline Runner
Orchestrates data extraction and loading for all sources with database integration
"""

import asyncio
import sys
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.config import settings
from core.database import init_database
from core.shared import setup_logging, get_logger, PerformanceTimer
from etl.unified.data_loader import UnifiedDataLoader
from run_all_extractors import (
    run_corp_portal_extractors, run_salesforce_extractors
)


class ETLPipelineOrchestrator:
    """Complete ETL pipeline orchestrator with extraction and loading."""

    def __init__(self):
        self.logger = get_logger("etl_pipeline")
        self.pipeline_stats = {
            "extraction_results": {},
            "loading_results": {},
            "total_duration": 0,
            "success_count": 0,
            "failure_count": 0,
            "start_time": None,
            "end_time": None
        }

        # Extractor function mapping - simplified to use existing orchestration
        self.extractors = {
            "corp_portal": run_corp_portal_extractors,
            "salesforce": run_salesforce_extractors
        }

    def run_extraction_phase(self, sources_to_extract: Optional[List[str]] = None) -> Dict[str, Any]:
        """Run data extraction for specified sources or all sources."""
        if not sources_to_extract:
            sources_to_extract = list(self.extractors.keys())

        self.logger.info(f"Starting extraction phase for {len(sources_to_extract)} sources")
        extraction_results = {}

        for source in sources_to_extract:
            if source not in self.extractors:
                self.logger.warning(f"Unknown extractor: {source}")
                extraction_results[source] = {
                    "status": "failed",
                    "error": f"Unknown extractor: {source}"
                }
                continue

            try:
                self.logger.info(f"Running extractor: {source}")
                start_time = time.time()

                # Run the extractor
                extractor_func = self.extractors[source]
                success = extractor_func()

                end_time = time.time()
                duration = end_time - start_time

                extraction_results[source] = {
                    "status": "success" if success else "failed",
                    "duration": duration,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }

                if success:
                    self.pipeline_stats["success_count"] += 1
                    self.logger.info(f"✓ {source} extraction completed in {duration:.2f}s")
                else:
                    self.pipeline_stats["failure_count"] += 1
                    self.logger.error(f"✗ {source} extraction failed")

            except Exception as e:
                self.pipeline_stats["failure_count"] += 1
                error_msg = f"Extractor {source} failed: {str(e)}"
                self.logger.error(error_msg)

                extraction_results[source] = {
                    "status": "failed",
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }

        return extraction_results

    async def run_loading_phase(self, extract_date: str = None) -> Dict[str, Any]:
        """Run data loading phase to PostgreSQL."""
        if not extract_date:
            extract_date = datetime.now().strftime('%Y-%m-%d')

        self.logger.info(f"Starting loading phase for extract date: {extract_date}")

        try:
            # Initialize database
            await init_database()

            # Create data loader
            loader = UnifiedDataLoader()

            # Load all data
            loading_result = await loader.load_all_data(extract_date)

            if loading_result["status"] == "completed":
                self.logger.info(f"✓ Loading phase completed successfully")
                self.logger.info(f"Files processed: {loading_result['load_stats']['files_processed']}")
                self.logger.info(f"Total rows loaded: {loading_result['load_stats']['total_rows_loaded']}")
            else:
                self.logger.error(f"✗ Loading phase failed: {loading_result.get('error')}")

            return loading_result

        except Exception as e:
            error_msg = f"Loading phase failed: {str(e)}"
            self.logger.error(error_msg)
            return {
                "status": "failed",
                "error": str(e)
            }

    async def run_database_setup(self) -> bool:
        """Ensure database schema is up to date."""
        try:
            self.logger.info("Setting up database schema...")

            # Initialize database connection
            await init_database()

            # Run schema initialization scripts
            from core.database import get_db_session
            from sqlalchemy import text
            async with get_db_session() as session:
                # Read and execute the new ETL tables script
                etl_tables_script = Path("database/init/06_etl_tables.sql")
                if etl_tables_script.exists():
                    self.logger.info("Creating ETL tables...")
                    with open(etl_tables_script, 'r', encoding='utf-8') as f:
                        schema_sql = f.read()

                    # Clean SQL and split properly
                    # Remove comments and empty lines
                    lines = []
                    for line in schema_sql.split('\n'):
                        line = line.strip()
                        if line and not line.startswith('--'):
                            lines.append(line)

                    clean_sql = '\n'.join(lines)

                    # Split into statements
                    statements = [stmt.strip() for stmt in clean_sql.split(';') if stmt.strip()]

                    # Execute table creation statements first, then indexes
                    table_statements = []
                    other_statements = []

                    for stmt in statements:
                        if 'CREATE TABLE' in stmt.upper():
                            table_statements.append(stmt)
                        else:
                            other_statements.append(stmt)

                    # Execute in proper order
                    all_statements = table_statements + other_statements

                    for stmt in all_statements:
                        try:
                            await session.execute(text(stmt))
                        except Exception as e:
                            self.logger.warning(f"SQL statement failed (continuing): {e}")
                            continue

                    await session.commit()
                    self.logger.info("✓ ETL tables created successfully")
                else:
                    self.logger.warning("ETL tables script not found, skipping schema setup")

            return True

        except Exception as e:
            self.logger.error(f"Database setup failed: {str(e)}")
            return False

    async def run_complete_pipeline(self,
                                  sources_to_extract: Optional[List[str]] = None,
                                  extract_date: str = None,
                                  skip_extraction: bool = False,
                                  skip_loading: bool = False) -> Dict[str, Any]:
        """Run the complete ETL pipeline with extraction and loading."""

        self.pipeline_stats["start_time"] = datetime.now(timezone.utc)

        try:
            with PerformanceTimer("Complete ETL Pipeline", self.logger.name):

                # Phase 1: Database Setup
                self.logger.info("=== PHASE 1: Database Setup ===")
                schema_setup_success = await self.run_database_setup()
                if not schema_setup_success:
                    self.logger.error("Database setup failed, aborting pipeline")
                    return {
                        "status": "failed",
                        "error": "Database setup failed",
                        "pipeline_stats": self.pipeline_stats
                    }

                # Phase 2: Data Extraction
                if not skip_extraction:
                    self.logger.info("=== PHASE 2: Data Extraction ===")
                    extraction_results = self.run_extraction_phase(sources_to_extract)
                    self.pipeline_stats["extraction_results"] = extraction_results
                else:
                    self.logger.info("=== PHASE 2: Skipping Data Extraction ===")
                    self.pipeline_stats["extraction_results"] = {"skipped": True}

                # Phase 3: Data Loading
                if not skip_loading:
                    self.logger.info("=== PHASE 3: Data Loading ===")
                    loading_results = await self.run_loading_phase(extract_date)
                    self.pipeline_stats["loading_results"] = loading_results
                else:
                    self.logger.info("=== PHASE 3: Skipping Data Loading ===")
                    self.pipeline_stats["loading_results"] = {"skipped": True}

                # Phase 4: Analytics Refresh
                self.logger.info("=== PHASE 4: Analytics Refresh ===")
                analytics_result = await self.refresh_analytics_views()
                self.pipeline_stats["analytics_refresh"] = analytics_result

                self.pipeline_stats["end_time"] = datetime.now(timezone.utc)
                self.pipeline_stats["total_duration"] = (
                    self.pipeline_stats["end_time"] - self.pipeline_stats["start_time"]
                ).total_seconds()

                # Determine overall success
                extraction_success = (
                    skip_extraction or
                    self.pipeline_stats["extraction_results"].get("skipped") or
                    self.pipeline_stats["success_count"] > 0
                )

                loading_success = (
                    skip_loading or
                    self.pipeline_stats["loading_results"].get("skipped") or
                    self.pipeline_stats["loading_results"].get("status") == "completed"
                )

                overall_status = "completed" if (extraction_success and loading_success) else "partial"

                return {
                    "status": overall_status,
                    "pipeline_stats": self.pipeline_stats,
                    "summary": {
                        "total_duration": self.pipeline_stats["total_duration"],
                        "extraction_success": extraction_success,
                        "loading_success": loading_success,
                        "analytics_refreshed": analytics_result.get("status") == "success"
                    }
                }

        except Exception as e:
            self.pipeline_stats["end_time"] = datetime.now(timezone.utc)
            self.logger.error(f"Pipeline failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e),
                "pipeline_stats": self.pipeline_stats
            }

    async def refresh_analytics_views(self) -> Dict[str, Any]:
        """Refresh materialized views and analytics tables."""
        try:
            from core.database import get_db_session
            from sqlalchemy import text

            async with get_db_session() as session:
                # Refresh the executive dashboard materialized view
                await session.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY unified_analytics.executive_dashboard"))
                await session.commit()

                self.logger.info("Analytics views refreshed successfully")
                return {"status": "success"}

        except Exception as e:
            self.logger.error(f"Analytics refresh failed: {str(e)}")
            return {"status": "failed", "error": str(e)}

    def print_pipeline_summary(self, result: Dict[str, Any]):
        """Print a comprehensive pipeline summary."""
        print("\n" + "="*80)
        print("ETL PIPELINE EXECUTION SUMMARY")
        print("="*80)

        # Overall status
        status_emoji = "✓" if result["status"] in ["completed", "partial"] else "✗"
        print(f"Overall Status: {status_emoji} {result['status'].upper()}")

        if "summary" in result:
            summary = result["summary"]
            print(f"Total Duration: {summary['total_duration']:.2f} seconds")
            print(f"Extraction: {'✓' if summary['extraction_success'] else '✗'}")
            print(f"Loading: {'✓' if summary['loading_success'] else '✗'}")
            print(f"Analytics: {'✓' if summary['analytics_refreshed'] else '✗'}")

        # Extraction results
        if "extraction_results" in result["pipeline_stats"]:
            extraction = result["pipeline_stats"]["extraction_results"]
            if not extraction.get("skipped"):
                print(f"\nExtraction Results:")
                for source, ext_result in extraction.items():
                    status_char = "✓" if ext_result.get("status") == "success" else "✗"
                    duration = ext_result.get("duration", 0)
                    print(f"  {status_char} {source}: {duration:.2f}s")

        # Loading results
        if "loading_results" in result["pipeline_stats"]:
            loading = result["pipeline_stats"]["loading_results"]
            if not loading.get("skipped") and loading.get("status") == "completed":
                stats = loading.get("load_stats", {})
                print(f"\nLoading Results:")
                print(f"  Files Processed: {stats.get('files_processed', 0)}")
                print(f"  Total Rows Loaded: {stats.get('total_rows_loaded', 0)}")
                print(f"  Loading Errors: {len(stats.get('loading_errors', []))}")

                if "file_results" in loading:
                    print("  File Details:")
                    for source, file_result in loading["file_results"].items():
                        status_char = "✓" if file_result.get("status") == "success" else "✗"
                        rows = file_result.get("rows_processed", 0)
                        print(f"    {status_char} {source}: {rows} rows")

        # Errors
        if result["status"] == "failed":
            print(f"\nError: {result.get('error', 'Unknown error')}")

        print("="*80)


async def main():
    """Main function for ETL pipeline."""
    # Setup logging
    setup_logging()
    logger = get_logger("etl_pipeline")

    import argparse
    parser = argparse.ArgumentParser(description="Run complete ETL pipeline")
    parser.add_argument("--date", help="Extract date (YYYY-MM-DD)", default=None)
    parser.add_argument("--sources", nargs='+', help="Specific sources to extract", default=None)
    parser.add_argument("--skip-extraction", action="store_true", help="Skip extraction phase")
    parser.add_argument("--skip-loading", action="store_true", help="Skip loading phase")
    parser.add_argument("--setup-only", action="store_true", help="Only run database setup")

    args = parser.parse_args()

    try:
        orchestrator = ETLPipelineOrchestrator()

        if args.setup_only:
            # Just run database setup
            logger.info("Running database setup only...")
            success = await orchestrator.run_database_setup()
            return 0 if success else 1

        # Run complete pipeline
        result = await orchestrator.run_complete_pipeline(
            sources_to_extract=args.sources,
            extract_date=args.date,
            skip_extraction=args.skip_extraction,
            skip_loading=args.skip_loading
        )

        # Print summary
        orchestrator.print_pipeline_summary(result)

        # Return appropriate exit code
        if result["status"] in ["completed", "partial"]:
            logger.info("ETL pipeline completed successfully")
            return 0
        else:
            logger.error("ETL pipeline failed")
            return 1

    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except Exception as e:
        print(f"ETL Pipeline failed: {str(e)}")
        sys.exit(1)