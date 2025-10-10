#!/usr/bin/env python3
"""
Complete ETL Pipeline Runner - Sync/Async Hybrid
Runs extraction synchronously (for Playwright compatibility) then async loading/transformation
"""

import asyncio
import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.shared import setup_logging, get_logger
from run_all_extractors import run_corp_portal_extractors, run_salesforce_extractors
from run_etl_pipeline import ETLPipelineOrchestrator


def run_extraction_sync(sources_to_extract=None):
    """Run extraction phase synchronously to avoid Playwright async conflicts."""
    logger = get_logger("etl_sync_wrapper")

    if not sources_to_extract:
        sources_to_extract = ["corp_portal", "salesforce"]

    logger.info("=== PHASE 1: Data Extraction (Synchronous) ===")
    extraction_results = {}

    # Run Corp Portal extractors
    if "corp_portal" in sources_to_extract:
        logger.info("Running Corporate Portal extractors...")
        corp_results = run_corp_portal_extractors()
        extraction_results["corp_portal"] = corp_results

    # Run Salesforce extractors
    if "salesforce" in sources_to_extract:
        logger.info("Running Salesforce extractors...")
        sf_results = run_salesforce_extractors()
        extraction_results["salesforce"] = sf_results

    return extraction_results


async def run_loading_and_analytics(extract_date=None, skip_loading=False):
    """Run async loading and analytics phases."""
    logger = get_logger("etl_sync_wrapper")

    orchestrator = ETLPipelineOrchestrator()

    # Phase 2: Database Setup
    logger.info("=== PHASE 2: Database Setup ===")
    schema_setup_success = await orchestrator.run_database_setup()
    if not schema_setup_success:
        logger.error("Database setup failed")
        return {"status": "failed", "error": "Database setup failed"}

    # Phase 3: Data Loading
    if not skip_loading:
        logger.info("=== PHASE 3: Data Loading ===")
        loading_results = await orchestrator.run_loading_phase(extract_date)
    else:
        logger.info("=== PHASE 3: Skipping Data Loading ===")
        loading_results = {"skipped": True}

    # Phase 4: Analytics Refresh
    logger.info("=== PHASE 4: Analytics Refresh ===")
    analytics_result = await orchestrator.refresh_analytics_views()

    # Phase 5: Book Data Transformation
    logger.info("=== PHASE 5: Book Data Transformation ===")
    book_result = await orchestrator.run_book_transformation()

    return {
        "loading_results": loading_results,
        "analytics_result": analytics_result,
        "book_result": book_result
    }


def main():
    """Main function - runs sync extraction then async loading."""
    setup_logging()
    logger = get_logger("etl_sync_wrapper")

    parser = argparse.ArgumentParser(description="Run complete ETL pipeline (sync+async)")
    parser.add_argument("--date", help="Extract date (YYYY-MM-DD)", default=None)
    parser.add_argument("--sources", nargs='+', help="Specific sources to extract", default=None)
    parser.add_argument("--skip-extraction", action="store_true", help="Skip extraction phase")
    parser.add_argument("--skip-loading", action="store_true", help="Skip loading phase")

    args = parser.parse_args()

    try:
        start_time = datetime.now()
        logger.info("="*80)
        logger.info("STARTING COMPLETE ETL PIPELINE (SYNC/ASYNC HYBRID)")
        logger.info("="*80)

        # Phase 1: Extraction (Synchronous - for Playwright compatibility)
        if not args.skip_extraction:
            extraction_results = run_extraction_sync(args.sources)
            logger.info(f"Extraction phase completed")
        else:
            logger.info("Skipping extraction phase")
            extraction_results = {"skipped": True}

        # Phase 2-5: Loading and Analytics (Asynchronous)
        # Check if event loop is already running (from SF extractors)
        try:
            loop = asyncio.get_running_loop()
            # Event loop is running, we need to await
            logger.warning("Event loop already running - SF extractors may have started one")
            # Create new event loop for async operations
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            try:
                async_results = new_loop.run_until_complete(
                    run_loading_and_analytics(args.date, args.skip_loading)
                )
            finally:
                new_loop.close()
        except RuntimeError:
            # No event loop running, safe to use asyncio.run()
            async_results = asyncio.run(
                run_loading_and_analytics(args.date, args.skip_loading)
            )

        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("="*80)
        logger.info("ETL PIPELINE SUMMARY")
        logger.info("="*80)
        logger.info(f"Total Duration: {duration:.1f}s")

        if not args.skip_extraction:
            # Count extraction successes
            total_extractors = 0
            successful_extractors = 0

            if "corp_portal" in extraction_results:
                corp_results = extraction_results["corp_portal"]
                for name, result in corp_results.items():
                    total_extractors += 1
                    if result.get("success"):
                        successful_extractors += 1
                        logger.info(f"✓ {name}")
                    else:
                        logger.error(f"✗ {name}: {result.get('info', {}).get('error', 'Unknown error')}")

            if "salesforce" in extraction_results:
                sf_results = extraction_results["salesforce"]
                for name, result in sf_results.items():
                    total_extractors += 1
                    if result.get("success"):
                        successful_extractors += 1
                        logger.info(f"✓ {name}")
                    else:
                        logger.error(f"✗ {name}: {result.get('info', {}).get('error', 'Unknown error')}")

            logger.info(f"Extraction: {successful_extractors}/{total_extractors} successful")

        # Loading results
        loading_results = async_results.get("loading_results", {})
        if not loading_results.get("skipped"):
            if loading_results.get("status") == "completed":
                stats = loading_results.get("load_stats", {})
                logger.info(f"✓ Loading: {stats.get('files_processed', 0)} files, {stats.get('total_rows_loaded', 0)} rows")
            else:
                logger.error(f"✗ Loading failed: {loading_results.get('error', 'Unknown error')}")

        # Analytics
        analytics_result = async_results.get("analytics_result", {})
        if analytics_result.get("status") == "success":
            logger.info("✓ Analytics refreshed")
        else:
            logger.error(f"✗ Analytics failed: {analytics_result.get('error', 'Unknown error')}")

        # Book transformation
        book_result = async_results.get("book_result", {})
        if book_result.get("status") == "success":
            logger.info(f"✓ Book transformation: {book_result.get('campaigns_processed', 0)} campaigns")
        else:
            logger.error(f"✗ Book transformation failed: {book_result.get('error', 'Unknown error')}")

        logger.info("="*80)

        # Determine exit code
        all_success = (
            (args.skip_extraction or successful_extractors > 0) and
            (args.skip_loading or loading_results.get("status") == "completed") and
            analytics_result.get("status") == "success" and
            book_result.get("status") == "success"
        )

        return 0 if all_success else 1

    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except Exception as e:
        print(f"ETL Pipeline failed: {str(e)}")
        sys.exit(1)
