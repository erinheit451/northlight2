#!/usr/bin/env python3
"""
Unified ETL Pipeline Runner
Main script to execute the complete ETL pipeline
"""

import asyncio
import sys
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.config import settings
from core.database import init_database
from core.shared import setup_logging, get_logger
from etl.unified.orchestration.scheduler import ETLScheduler
from etl.unified.orchestration.monitor import ETLMonitor
from etl.unified.extractors.heartbeat_wrapper import extract_all_data


class UnifiedETLRunner:
    """Main ETL pipeline runner combining extraction and loading."""

    def __init__(self):
        self.logger = get_logger("unified_etl")
        self.scheduler = ETLScheduler()
        self.monitor = ETLMonitor()

    async def run_full_pipeline(self, extract_first: bool = True,
                              job_ids: Optional[List[str]] = None) -> dict:
        """
        Run the complete ETL pipeline.

        Args:
            extract_first: Whether to run extraction before loading
            job_ids: Specific job IDs to run (None for all)

        Returns:
            Pipeline execution results
        """
        pipeline_start = datetime.now(timezone.utc)

        self.logger.info("Starting unified ETL pipeline")
        self.logger.info(f"Extract first: {extract_first}")
        self.logger.info(f"Job IDs: {job_ids or 'all'}")

        results = {
            "pipeline_start": pipeline_start.isoformat(),
            "extraction_results": None,
            "loading_results": None,
            "status": "running"
        }

        try:
            # Initialize database
            await init_database()

            # Step 1: Data Extraction (if requested)
            if extract_first:
                self.logger.info("Starting data extraction phase...")
                extraction_results = await extract_all_data()
                results["extraction_results"] = extraction_results

                # Check if extraction was successful
                successful_extractions = sum(
                    1 for r in extraction_results.get("results", {}).values()
                    if r.get("status") == "success"
                )

                if successful_extractions == 0:
                    self.logger.error("All extractions failed, skipping loading phase")
                    results["status"] = "failed"
                    results["error"] = "All data extractions failed"
                    return results

                self.logger.info(f"Extraction completed: {successful_extractions} successful")

            # Step 2: Data Loading
            self.logger.info("Starting data loading phase...")
            loading_results = await self.scheduler.execute_pipeline(job_ids)
            results["loading_results"] = loading_results

            # Determine overall status
            successful_loads = sum(
                1 for execution in loading_results.values()
                if execution.status.value == "completed"
            )

            total_loads = len(loading_results)

            if successful_loads == total_loads:
                results["status"] = "success"
                self.logger.info(f"Pipeline completed successfully: {successful_loads}/{total_loads} jobs")
            elif successful_loads > 0:
                results["status"] = "partial_success"
                self.logger.warning(f"Pipeline partially successful: {successful_loads}/{total_loads} jobs")
            else:
                results["status"] = "failed"
                self.logger.error("Pipeline failed: no jobs completed successfully")

        except Exception as e:
            self.logger.error(f"Pipeline failed with exception: {str(e)}")
            results["status"] = "failed"
            results["error"] = str(e)

        finally:
            results["pipeline_end"] = datetime.now(timezone.utc).isoformat()
            duration = (
                datetime.fromisoformat(results["pipeline_end"].replace('Z', '+00:00')) -
                pipeline_start
            ).total_seconds()
            results["duration_seconds"] = duration

            self.logger.info(f"Pipeline completed in {duration:.2f} seconds with status: {results['status']}")

        return results

    async def run_extraction_only(self) -> dict:
        """Run only the data extraction phase."""
        self.logger.info("Running extraction-only pipeline")

        try:
            results = await extract_all_data()
            self.logger.info("Extraction-only pipeline completed")
            return results
        except Exception as e:
            self.logger.error(f"Extraction-only pipeline failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e),
                "failed_at": datetime.now(timezone.utc).isoformat()
            }

    async def run_loading_only(self, job_ids: Optional[List[str]] = None) -> dict:
        """Run only the data loading phase."""
        self.logger.info("Running loading-only pipeline")

        try:
            await init_database()
            results = await self.scheduler.execute_pipeline(job_ids)
            self.logger.info("Loading-only pipeline completed")
            return {"loading_results": results}
        except Exception as e:
            self.logger.error(f"Loading-only pipeline failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e),
                "failed_at": datetime.now(timezone.utc).isoformat()
            }

    def get_pipeline_status(self) -> dict:
        """Get current pipeline status."""
        return self.scheduler.get_pipeline_status()

    async def start_monitoring(self):
        """Start the ETL monitoring system."""
        await self.monitor.start_monitoring()

    async def stop_monitoring(self):
        """Stop the ETL monitoring system."""
        await self.monitor.stop_monitoring()


async def main():
    """Main function with command-line interface."""
    parser = argparse.ArgumentParser(description="Unified ETL Pipeline Runner")

    parser.add_argument(
        "--mode",
        choices=["full", "extract", "load"],
        default="full",
        help="Pipeline mode: full (extract+load), extract only, or load only"
    )

    parser.add_argument(
        "--no-extract",
        action="store_true",
        help="Skip extraction phase (only for full mode)"
    )

    parser.add_argument(
        "--jobs",
        nargs="*",
        help="Specific job IDs to run (default: all jobs)"
    )

    parser.add_argument(
        "--monitor",
        action="store_true",
        help="Start monitoring system after pipeline execution"
    )

    parser.add_argument(
        "--status",
        action="store_true",
        help="Show pipeline status and exit"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging()
    logger = get_logger("unified_etl")

    try:
        runner = UnifiedETLRunner()

        # Show status and exit
        if args.status:
            status = runner.get_pipeline_status()
            print("\n" + "="*60)
            print("PIPELINE STATUS")
            print("="*60)
            print(f"Total jobs: {status['total_jobs']}")
            print(f"Enabled jobs: {status['enabled_jobs']}")
            print(f"Running jobs: {status['running_jobs']}")

            if status['last_pipeline_run']:
                print(f"Last pipeline run: {status['last_pipeline_run']}")

            print("\nJob Status:")
            for job_id, job_status in status['jobs'].items():
                status_icon = "[OK]" if job_status['latest_execution'] and job_status['latest_execution']['status'] == 'completed' else "[--]"
                running_icon = "[RUNNING]" if job_status['is_running'] else ""
                print(f"  {status_icon} {running_icon} {job_status['job_name']} ({job_id})")

            print("="*60)
            return 0

        # Run pipeline based on mode
        if args.mode == "full":
            results = await runner.run_full_pipeline(
                extract_first=not args.no_extract,
                job_ids=args.jobs
            )
        elif args.mode == "extract":
            results = await runner.run_extraction_only()
        elif args.mode == "load":
            results = await runner.run_loading_only(job_ids=args.jobs)

        # Print results summary
        print("\n" + "="*60)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*60)
        print(f"Status: {results.get('status', 'unknown')}")

        if 'duration_seconds' in results:
            print(f"Duration: {results['duration_seconds']:.2f} seconds")

        if 'error' in results:
            print(f"Error: {results['error']}")

        # Extraction summary
        if 'extraction_results' in results and results['extraction_results']:
            ext_results = results['extraction_results'].get('results', {})
            successful_ext = sum(1 for r in ext_results.values() if r.get('status') == 'success')
            print(f"Extractions: {successful_ext}/{len(ext_results)} successful")

        # Loading summary
        if 'loading_results' in results and results['loading_results']:
            load_results = results['loading_results']
            successful_loads = sum(1 for ex in load_results.values() if ex.status.value == 'completed')
            print(f"Data loads: {successful_loads}/{len(load_results)} successful")

        print("="*60)

        # Start monitoring if requested
        if args.monitor:
            logger.info("Starting monitoring system...")
            await runner.start_monitoring()

        # Return appropriate exit code
        if results.get('status') == 'success':
            return 0
        elif results.get('status') == 'partial_success':
            return 1
        else:
            return 2

    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Pipeline runner failed: {str(e)}")
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except Exception as e:
        print(f"Failed to run pipeline: {str(e)}")
        sys.exit(1)