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

        # Define all expected data sources for comprehensive tracking
        self.expected_data_sources = [
            "ultimate_dms",
            "budget_waterfall_client",
            "budget_waterfall_channel",
            "spend_revenue_performance",
            "dfp_rij",
            "agreed_cpl_performance",
            "bsc_standards",
            "sf_partner_pipeline",
            "sf_tim_king_partner_pipeline",
            "sf_partner_calls",
            "sf_grader_opportunities"
        ]

        self.pipeline_stats = {
            "extraction_results": {},
            "loading_results": {},
            "total_duration": 0,
            "success_count": 0,
            "failure_count": 0,
            "start_time": None,
            "end_time": None,
            "expected_sources": self.expected_data_sources,
            "extraction_failures": [],
            "loading_failures": [],
            "missing_files": []
        }

        # Extractor function mapping - simplified to use existing orchestration
        self.extractors = {
            "corp_portal": run_corp_portal_extractors,
            "salesforce": run_salesforce_extractors
        }

    def run_extraction_phase(self, sources_to_extract: Optional[List[str]] = None) -> Dict[str, Any]:
        """Run data extraction for specified sources or all sources.

        Note: Sync extractors (like Salesforce) are called directly without await.
        This method itself is synchronous to maintain compatibility with sync Playwright API.
        """
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

                # Run the extractor (sync call - these are sync functions)
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
                    self.logger.info(f"[SUCCESS] {source} extraction completed in {duration:.2f}s")
                else:
                    self.pipeline_stats["failure_count"] += 1
                    self.logger.error(f"[FAILED] {source} extraction failed")
                    self.pipeline_stats["extraction_failures"].append({
                        "source": source,
                        "error": "Extractor returned False",
                        "duration": duration
                    })

            except Exception as e:
                self.pipeline_stats["failure_count"] += 1
                error_msg = f"Extractor {source} failed: {str(e)}"
                self.logger.error(error_msg)

                extraction_results[source] = {
                    "status": "failed",
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                self.pipeline_stats["extraction_failures"].append({
                    "source": source,
                    "error": str(e)
                })

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
                self.logger.info(f"[SUCCESS] Loading phase completed successfully")
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
                    self.logger.info("[SUCCESS] ETL tables created successfully")
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
                    # Run sync extraction in thread pool to avoid blocking async event loop
                    # This is critical for Salesforce extractors that use sync Playwright API
                    extraction_results = await asyncio.to_thread(
                        self.run_extraction_phase, sources_to_extract
                    )
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

                # Phase 4.5: Book Data Transformation (for /book dashboard)
                self.logger.info("=== PHASE 4.5: Book Data Transformation ===")
                book_result = await self.run_book_transformation()
                self.pipeline_stats["book_transformation"] = book_result

                # Phase 4.6: Book Schema Sync (sync performance data to book tables)
                self.logger.info("=== PHASE 4.6: Book Schema Sync ===")
                sync_result = await self.run_book_schema_sync()
                self.pipeline_stats["book_schema_sync"] = sync_result

                self.pipeline_stats["end_time"] = datetime.now(timezone.utc)
                self.pipeline_stats["total_duration"] = (
                    self.pipeline_stats["end_time"] - self.pipeline_stats["start_time"]
                ).total_seconds()

                # Determine overall success with stricter criteria
                total_expected = len(self.expected_data_sources)
                extraction_failures = len(self.pipeline_stats.get("extraction_failures", []))

                extraction_success = (
                    skip_extraction or
                    self.pipeline_stats["extraction_results"].get("skipped") or
                    (self.pipeline_stats["success_count"] > 0 and extraction_failures == 0)
                )

                # Consider partial success if most sources succeeded
                extraction_partial = (
                    self.pipeline_stats["success_count"] >= total_expected * 0.8  # 80% threshold
                )

                loading_success = (
                    skip_loading or
                    self.pipeline_stats["loading_results"].get("skipped") or
                    (self.pipeline_stats["loading_results"].get("status") == "completed" and
                     len(self.pipeline_stats["loading_results"].get("load_stats", {}).get("loading_errors", [])) == 0)
                )

                # Set overall status
                if extraction_success and loading_success:
                    overall_status = "completed"
                elif extraction_partial or loading_success:
                    overall_status = "partial"
                else:
                    overall_status = "failed"

                book_success = (
                    self.pipeline_stats.get("book_transformation", {}).get("status") == "success"
                )

                sync_success = (
                    self.pipeline_stats.get("book_schema_sync", {}).get("status") == "success"
                )

                return {
                    "status": overall_status,
                    "pipeline_stats": self.pipeline_stats,
                    "summary": {
                        "total_duration": self.pipeline_stats["total_duration"],
                        "extraction_success": extraction_success,
                        "loading_success": loading_success,
                        "analytics_refreshed": analytics_result.get("status") == "success",
                        "book_transformed": book_success,
                        "book_schema_synced": sync_success
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

    async def run_book_transformation(self) -> Dict[str, Any]:
        """Transform Ultimate DMS data into book.campaigns for /book dashboard."""
        try:
            self.logger.info("Starting book data transformation...")

            # Import and run the transformation
            import sys
            from pathlib import Path
            scripts_path = Path(__file__).parent / "scripts"
            sys.path.insert(0, str(scripts_path))

            from scripts.transform_book_data import BookDataTransformer

            transformer = BookDataTransformer()

            # Load and process data
            processed_df = await transformer.load_and_process_data()
            self.logger.info(f"Processed {len(processed_df)} campaigns with risk calculations")

            # Save to database
            await transformer.save_to_database(processed_df)
            self.logger.info(f"Book transformation completed - {len(processed_df)} campaigns loaded")

            return {
                "status": "success",
                "campaigns_processed": len(processed_df)
            }

        except Exception as e:
            error_msg = f"Book transformation failed: {str(e)}"
            self.logger.error(error_msg)
            return {
                "status": "failed",
                "error": str(e)
            }

    async def run_book_schema_sync(self) -> Dict[str, Any]:
        """Sync performance data from heartbeat_performance to book schema."""
        try:
            self.logger.info("Starting book schema sync...")

            # Import and run the sync
            from scripts.sync_book_performance_data import sync_all_book_tables

            success = await sync_all_book_tables()

            self.logger.info("Book schema sync completed successfully")

            return {
                "status": "success"
            }

        except Exception as e:
            error_msg = f"Book schema sync failed: {str(e)}"
            self.logger.error(error_msg)
            # Don't fail the entire ETL if sync fails
            # But do log it prominently so it gets fixed
            return {
                "status": "failed",
                "error": str(e)
            }

    def print_pipeline_summary(self, result: Dict[str, Any]):
        """Print a comprehensive pipeline summary with detailed error reporting."""
        print("\n" + "="*80)
        print("ETL PIPELINE EXECUTION SUMMARY")
        print("="*80)

        # Overall status
        status_emoji = "[OK]" if result["status"] in ["completed", "partial"] else "[FAIL]"
        print(f"Overall Status: {status_emoji} {result['status'].upper()}")

        if "summary" in result:
            summary = result["summary"]
            print(f"Total Duration: {summary['total_duration']:.2f} seconds")
            print(f"Extraction: {'[SUCCESS]' if summary['extraction_success'] else '[FAILED]'}")
            print(f"Loading: {'[SUCCESS]' if summary['loading_success'] else '[FAILED]'}")
            print(f"Analytics: {'[SUCCESS]' if summary['analytics_refreshed'] else '[FAILED]'}")
            print(f"Book Transform: {'[SUCCESS]' if summary.get('book_transformed') else '[FAILED]'}")
            print(f"Book Schema Sync: {'[SUCCESS]' if summary.get('book_schema_synced') else '[FAILED]'}")

        # Expected vs Actual Tracking
        expected_sources = result["pipeline_stats"].get("expected_sources", [])
        if expected_sources:
            print(f"\nExpected Data Sources: {len(expected_sources)}")
            print(f"Sources: {', '.join(expected_sources)}")

        # Extraction results with detailed breakdown
        if "extraction_results" in result["pipeline_stats"]:
            extraction = result["pipeline_stats"]["extraction_results"]
            if not extraction.get("skipped"):
                print(f"\nExtraction Phase:")
                success_count = sum(1 for r in extraction.values() if isinstance(r, dict) and r.get("status") == "success")
                total_count = len([r for r in extraction.values() if isinstance(r, dict)])
                success_rate = (success_count / total_count * 100) if total_count > 0 else 0
                print(f"  Success Rate: {success_count}/{total_count} ({success_rate:.0f}%)")

                for source, ext_result in extraction.items():
                    if not isinstance(ext_result, dict):
                        continue
                    status_char = "[OK]" if ext_result.get("status") == "success" else "[FAIL]"
                    duration = ext_result.get("duration", 0)
                    error_msg = f" - {ext_result['error']}" if ext_result.get("error") else ""
                    print(f"  {status_char} {source}: {duration:.2f}s{error_msg}")

        # Extraction failures
        extraction_failures = result["pipeline_stats"].get("extraction_failures", [])
        if extraction_failures:
            print(f"\nExtraction Failures ({len(extraction_failures)}):")
            for failure in extraction_failures:
                print(f"  [X] {failure['source']}: {failure['error']}")

        # Loading results with detailed breakdown
        if "loading_results" in result["pipeline_stats"]:
            loading = result["pipeline_stats"]["loading_results"]
            if not loading.get("skipped") and loading.get("status") == "completed":
                stats = loading.get("load_stats", {})
                print(f"\nLoading Phase:")
                print(f"  Files Processed: {stats.get('files_processed', 0)}")
                print(f"  Total Rows Loaded: {stats.get('total_rows_loaded', 0)}")
                print(f"  Loading Errors: {len(stats.get('loading_errors', []))}")

                if "file_results" in loading:
                    print("  File Details:")
                    for source, file_result in loading["file_results"].items():
                        status_char = "[OK]" if file_result.get("status") == "success" else "[FAIL]"
                        rows = file_result.get("rows_processed", 0)
                        print(f"    {status_char} {source}: {rows} rows")

                # Show which expected sources were NOT loaded
                if expected_sources and "file_results" in loading:
                    loaded_sources = set(loading["file_results"].keys())
                    missing_sources = set(expected_sources) - loaded_sources
                    if missing_sources:
                        print(f"\n  Missing from Load ({len(missing_sources)}):")
                        for source in sorted(missing_sources):
                            print(f"    [X] {source}")

        # Loading errors detail
        if "loading_results" in result["pipeline_stats"]:
            loading = result["pipeline_stats"]["loading_results"]
            loading_errors = loading.get("load_stats", {}).get("loading_errors", [])
            if loading_errors:
                print(f"\nLoading Errors ({len(loading_errors)}):")
                for error in loading_errors[:5]:  # Show first 5
                    print(f"  [X] {error.get('source', 'unknown')}: {error.get('error', 'Unknown error')}")
                if len(loading_errors) > 5:
                    print(f"  ... and {len(loading_errors) - 5} more errors")

        # Overall errors
        if result["status"] == "failed":
            print(f"\nPipeline Error: {result.get('error', 'Unknown error')}")

        print("="*80)


async def main():
    """Main entry point for the ETL pipeline."""
    import argparse

    parser = argparse.ArgumentParser(description="Run the complete ETL pipeline")
    parser.add_argument("--skip-extraction", action="store_true", help="Skip data extraction phase")
    parser.add_argument("--skip-loading", action="store_true", help="Skip data loading phase")
    parser.add_argument("--date", type=str, help="Extract date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--sources", nargs="+", help="Specific sources to extract (e.g., corp_portal salesforce)")

    args = parser.parse_args()

    # Setup logging
    setup_logging()

    print("[INFO] Starting Complete ETL Pipeline...")

    # Create orchestrator
    orchestrator = ETLPipelineOrchestrator()

    # Run pipeline
    result = await orchestrator.run_complete_pipeline(
        sources_to_extract=args.sources,
        extract_date=args.date,
        skip_extraction=args.skip_extraction,
        skip_loading=args.skip_loading
    )

    # Print summary
    orchestrator.print_pipeline_summary(result)

    # Return appropriate exit code
    if result["status"] == "completed":
        print("\n[INFO] ETL pipeline completed successfully")
        return 0
    elif result["status"] == "partial":
        print("\n[WARN] ETL pipeline completed with some failures")
        return 1
    else:
        print("\n[ERROR] ETL pipeline failed")
        return 2


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
