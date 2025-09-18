"""
Heartbeat Extractor Wrapper
Preserves original Heartbeat extractors while adapting output for PostgreSQL loaders
"""

import sys
import os
import asyncio
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, timezone

# Add project root and heartbeat paths
project_root = Path(__file__).parent.parent.parent.parent
heartbeat_path = project_root / "etl" / "heartbeat"
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(heartbeat_path))

from core.shared import get_logger, log_step, PerformanceTimer
from core.config import settings

# Import original Heartbeat extractors (preserved unchanged)
try:
    from etl.heartbeat.extractor.corp_portal.ultimate_dms import run as extract_udms_original
    from etl.heartbeat.extractor.corp_portal.budget_waterfall_client import run as extract_bwc_original
    from etl.heartbeat.extractor.salesforce.partner_pipeline import run as extract_sf_pipeline_original
    from etl.heartbeat.extractor.salesforce.partner_calls import run as extract_sf_calls_original
except ImportError as e:
    # Handle missing extractors gracefully
    print(f"Warning: Could not import Heartbeat extractors: {e}")
    extract_udms_original = None
    extract_bwc_original = None
    extract_sf_pipeline_original = None
    extract_sf_calls_original = None


class HeartbeatExtractorWrapper:
    """
    Wrapper for original Heartbeat extractors.

    Preserves all original functionality while:
    - Adapting output paths for unified structure
    - Adding error handling and logging
    - Maintaining compatibility with existing authentication
    - Providing async interfaces
    """

    def __init__(self, extractor_name: str):
        self.extractor_name = extractor_name
        self.logger = get_logger(f"etl.extractor.{extractor_name}")

    async def extract_with_original(self, original_extractor_func, output_subdir: str, **kwargs) -> Dict[str, Any]:
        """
        Execute original Heartbeat extractor with unified output handling.

        Args:
            original_extractor_func: Original Heartbeat extractor function
            output_subdir: Subdirectory for output files
            **kwargs: Additional arguments for the extractor

        Returns:
            Extraction results and metadata
        """
        if original_extractor_func is None:
            raise ValueError(f"Original extractor function not available for {self.extractor_name}")

        # Set up unified output paths
        raw_data_path = Path(settings.RAW_DATA_PATH) / output_subdir
        raw_data_path.mkdir(parents=True, exist_ok=True)

        extract_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        output_file = raw_data_path / f"{extract_date}_{self.extractor_name}.parquet"

        log_step(
            self.extractor_name,
            f"Starting extraction to {output_file}",
            logger_name=self.logger.name
        )

        with PerformanceTimer(f"{self.extractor_name} extraction", self.logger.name):
            try:
                # Prepare environment for original extractor
                original_env = self._prepare_heartbeat_environment(raw_data_path)

                # Run original extractor in thread pool to avoid blocking
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None,
                    self._run_original_extractor,
                    original_extractor_func,
                    original_env,
                    kwargs
                )

                # Process and validate results
                extraction_stats = self._process_extraction_results(result, output_file)

                log_step(
                    self.extractor_name,
                    f"Extraction completed successfully. Output: {output_file}",
                    logger_name=self.logger.name
                )

                return {
                    "status": "success",
                    "extractor": self.extractor_name,
                    "output_file": str(output_file),
                    "extract_date": extract_date,
                    "extraction_stats": extraction_stats,
                    "completed_at": datetime.now(timezone.utc).isoformat()
                }

            except Exception as e:
                log_step(
                    self.extractor_name,
                    f"Extraction failed: {str(e)}",
                    is_error=True,
                    logger_name=self.logger.name
                )

                return {
                    "status": "failed",
                    "extractor": self.extractor_name,
                    "error_message": str(e),
                    "extract_date": extract_date,
                    "failed_at": datetime.now(timezone.utc).isoformat()
                }

    def _prepare_heartbeat_environment(self, output_path: Path) -> Dict[str, Any]:
        """Prepare environment variables and paths for original Heartbeat extractors."""

        # Set up paths that original extractors expect
        heartbeat_env = {
            "RAW_DATA_PATH": str(output_path),
            "WAREHOUSE_PATH": str(Path(settings.WAREHOUSE_PATH)),
            "DATABASE_URL": str(Path(settings.WAREHOUSE_PATH) / "northlight.duckdb"),  # For compatibility

            # Authentication (preserved from original .env)
            "CORP_PORTAL_USERNAME": settings.CORP_PORTAL_USERNAME,
            "CORP_PORTAL_PASSWORD": settings.CORP_PORTAL_PASSWORD,
            "CORP_PORTAL_URL": settings.CORP_PORTAL_URL,
            "SF_USERNAME": settings.SF_USERNAME,
            "SF_PASSWORD": settings.SF_PASSWORD,

            # Logging
            "LOG_LEVEL": settings.LOG_LEVEL
        }

        # Temporarily set environment variables
        for key, value in heartbeat_env.items():
            os.environ[key] = str(value)

        return heartbeat_env

    def _run_original_extractor(self, extractor_func, env_vars: Dict[str, Any], kwargs: Dict[str, Any]):
        """Run the original extractor function with proper environment."""
        try:
            # Original extractors typically don't return values, they write files
            # Call with any additional arguments
            if kwargs:
                return extractor_func(**kwargs)
            else:
                return extractor_func()
        except Exception as e:
            self.logger.error(f"Original extractor {self.extractor_name} failed: {e}")
            raise

    def _process_extraction_results(self, result: Any, output_file: Path) -> Dict[str, Any]:
        """Process and validate extraction results."""
        stats = {
            "files_created": [],
            "total_size_bytes": 0,
            "rows_extracted": 0
        }

        # Check if expected output file was created
        if output_file.exists():
            stats["files_created"].append(str(output_file))
            stats["total_size_bytes"] = output_file.stat().st_size

            # Try to count rows if it's a Parquet file
            try:
                import pyarrow.parquet as pq
                table = pq.read_table(output_file)
                stats["rows_extracted"] = len(table)
            except Exception:
                # If we can't read the file, that's okay
                pass

        # Look for other files in the output directory
        output_dir = output_file.parent
        for file_path in output_dir.glob("*"):
            if file_path.is_file() and str(file_path) not in stats["files_created"]:
                stats["files_created"].append(str(file_path))
                stats["total_size_bytes"] += file_path.stat().st_size

        return stats


# Specific extractor wrapper functions
async def extract_ultimate_dms_data(**kwargs) -> Dict[str, Any]:
    """Extract Ultimate DMS campaign data using original Heartbeat extractor."""
    wrapper = HeartbeatExtractorWrapper("ultimate_dms")
    return await wrapper.extract_with_original(
        extract_udms_original,
        "ultimate_dms",
        **kwargs
    )


async def extract_budget_waterfall_data(**kwargs) -> Dict[str, Any]:
    """Extract Budget Waterfall client data using original Heartbeat extractor."""
    wrapper = HeartbeatExtractorWrapper("budget_waterfall")
    return await wrapper.extract_with_original(
        extract_bwc_original,
        "budget_waterfall",
        **kwargs
    )


async def extract_salesforce_data(data_type: str = "partner_pipeline", **kwargs) -> Dict[str, Any]:
    """
    Extract Salesforce data using original Heartbeat extractors.

    Args:
        data_type: Type of Salesforce data ("partner_pipeline" or "partner_calls")
        **kwargs: Additional arguments for the extractor
    """
    if data_type == "partner_pipeline":
        wrapper = HeartbeatExtractorWrapper("sf_partner_pipeline")
        return await wrapper.extract_with_original(
            extract_sf_pipeline_original,
            "salesforce/partner_pipeline",
            **kwargs
        )
    elif data_type == "partner_calls":
        wrapper = HeartbeatExtractorWrapper("sf_partner_calls")
        return await wrapper.extract_with_original(
            extract_sf_calls_original,
            "salesforce/partner_calls",
            **kwargs
        )
    else:
        raise ValueError(f"Unknown Salesforce data type: {data_type}")


# Unified extraction function for full pipeline
async def extract_all_data() -> Dict[str, Any]:
    """
    Extract all data types in the correct order.

    Returns:
        Dictionary with extraction results for each data source
    """
    logger = get_logger("etl.extractor.unified")

    log_step(
        "Unified Extractor",
        "Starting full data extraction pipeline",
        logger_name=logger.name
    )

    results = {}

    # Extract in dependency order
    extraction_sequence = [
        ("ultimate_dms", extract_ultimate_dms_data),
        ("budget_waterfall", extract_budget_waterfall_data),
        ("sf_partner_pipeline", lambda: extract_salesforce_data("partner_pipeline")),
        ("sf_partner_calls", lambda: extract_salesforce_data("partner_calls"))
    ]

    for extractor_name, extractor_func in extraction_sequence:
        try:
            log_step(
                "Unified Extractor",
                f"Extracting {extractor_name}",
                logger_name=logger.name
            )

            result = await extractor_func()
            results[extractor_name] = result

            if result["status"] == "success":
                log_step(
                    "Unified Extractor",
                    f"Successfully extracted {extractor_name}",
                    logger_name=logger.name
                )
            else:
                log_step(
                    "Unified Extractor",
                    f"Failed to extract {extractor_name}: {result.get('error_message', 'Unknown error')}",
                    is_error=True,
                    logger_name=logger.name
                )

        except Exception as e:
            log_step(
                "Unified Extractor",
                f"Exception during {extractor_name} extraction: {str(e)}",
                is_error=True,
                logger_name=logger.name
            )

            results[extractor_name] = {
                "status": "failed",
                "error_message": str(e),
                "failed_at": datetime.now(timezone.utc).isoformat()
            }

    # Summary
    successful = sum(1 for r in results.values() if r.get("status") == "success")
    failed = len(results) - successful

    log_step(
        "Unified Extractor",
        f"Extraction pipeline completed: {successful} successful, {failed} failed",
        is_error=(failed > 0),
        logger_name=logger.name
    )

    return {
        "pipeline_status": "completed",
        "successful_extractions": successful,
        "failed_extractions": failed,
        "results": results,
        "completed_at": datetime.now(timezone.utc).isoformat()
    }