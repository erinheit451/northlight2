#!/usr/bin/env python3
"""
Benchmark Data Migration Script
Migrates Northlight's JSON benchmark data to PostgreSQL
"""

import asyncio
import sys
import json
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime, timezone

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.config import settings
from core.database import get_db_session, init_database
from core.shared import setup_logging, get_logger
from sqlalchemy import text


class BenchmarkDataMigrator:
    """Migrates benchmark data from JSON files to PostgreSQL."""

    def __init__(self):
        self.logger = get_logger("benchmark_migration")

    async def migrate_northlight_benchmarks(self) -> Dict[str, Any]:
        """Migrate Northlight's JSON benchmark data to PostgreSQL."""
        self.logger.info("Starting benchmark data migration from JSON to PostgreSQL")

        try:
            # Initialize database
            await init_database()

            # Find benchmark JSON files
            benchmark_files = self._find_benchmark_files()
            if not benchmark_files:
                return {
                    "status": "skipped",
                    "reason": "No benchmark JSON files found"
                }

            # Use the most recent benchmark file
            latest_file = max(benchmark_files, key=lambda p: p.stat().st_mtime)
            self.logger.info(f"Using benchmark file: {latest_file}")

            # Load and parse JSON data
            benchmark_data = self._load_benchmark_json(latest_file)

            # Migrate to PostgreSQL
            migration_result = await self._migrate_to_postgres(benchmark_data, latest_file)

            return {
                "status": "completed",
                "source_file": str(latest_file),
                "migration_result": migration_result
            }

        except Exception as e:
            self.logger.error(f"Benchmark migration failed: {e}")
            return {
                "status": "failed",
                "error": str(e)
            }

    def _find_benchmark_files(self) -> List[Path]:
        """Find all benchmark JSON files."""
        # Look in Northlight's data directory
        northlight_data_dir = Path("../northlight/backend/data")

        benchmark_files = []

        # Check various locations
        search_patterns = [
            "*benchmarks*.json",
            "*benchmark*.json",
            "20*-*-*-benchmarks.json"
        ]

        for pattern in search_patterns:
            if northlight_data_dir.exists():
                benchmark_files.extend(northlight_data_dir.glob(pattern))
                benchmark_files.extend(northlight_data_dir.glob(f"**/{pattern}"))

        # Also check current data directory
        current_data_dir = Path(settings.DATA_ROOT)
        for pattern in search_patterns:
            benchmark_files.extend(current_data_dir.glob(pattern))
            benchmark_files.extend(current_data_dir.glob(f"**/{pattern}"))

        # Remove duplicates
        unique_files = []
        seen_names = set()
        for file_path in benchmark_files:
            if file_path.name not in seen_names:
                unique_files.append(file_path)
                seen_names.add(file_path.name)

        self.logger.info(f"Found {len(unique_files)} benchmark files")
        return unique_files

    def _load_benchmark_json(self, file_path: Path) -> Dict[str, Any]:
        """Load and validate benchmark JSON data."""
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                data = json.load(f)

            if "records" not in data:
                raise ValueError("Benchmark file missing 'records' section")

            records = data["records"]
            if not isinstance(records, dict):
                raise ValueError("Benchmark 'records' must be a dictionary")

            self.logger.info(f"Loaded {len(records)} benchmark records")
            return data

        except Exception as e:
            self.logger.error(f"Failed to load benchmark JSON: {e}")
            raise

    async def _migrate_to_postgres(self, benchmark_data: Dict[str, Any], source_file: Path) -> Dict[str, Any]:
        """Migrate benchmark data to PostgreSQL tables."""
        async with get_db_session() as session:
            try:
                # Create benchmark snapshot record
                snapshot_id = await self._create_benchmark_snapshot(session, benchmark_data, source_file)

                # Migrate categories and data
                categories_migrated = 0
                records_migrated = 0

                records = benchmark_data.get("records", {})

                for key, record_data in records.items():
                    if key == "_version":
                        continue

                    if not isinstance(record_data, dict):
                        self.logger.warning(f"Skipping invalid record: {key}")
                        continue

                    # Extract category information
                    meta = record_data.get("meta", {})
                    category = meta.get("category", "unknown")
                    subcategory = meta.get("subcategory", "unknown")

                    # Create/get category
                    category_id = await self._create_or_get_category(session, category, subcategory)
                    if category_id:
                        categories_migrated += 1

                    # Create benchmark data record
                    if await self._create_benchmark_data(session, snapshot_id, category_id, key, record_data):
                        records_migrated += 1

                await session.commit()

                self.logger.info(f"Migration completed: {categories_migrated} categories, {records_migrated} records")

                return {
                    "snapshot_id": snapshot_id,
                    "categories_migrated": categories_migrated,
                    "records_migrated": records_migrated
                }

            except Exception as e:
                await session.rollback()
                self.logger.error(f"Migration transaction failed: {e}")
                raise

    async def _create_benchmark_snapshot(self, session, benchmark_data: Dict[str, Any], source_file: Path) -> str:
        """Create a benchmark snapshot record."""
        version = benchmark_data.get("version") or benchmark_data.get("date") or source_file.stem
        snapshot_date = datetime.now(timezone.utc).date()

        # Check for existing version data in the filename
        if "20" in source_file.stem and "-" in source_file.stem:
            try:
                # Try to parse date from filename
                date_parts = source_file.stem.split("-")
                if len(date_parts) >= 3:
                    year, month, day = date_parts[0], date_parts[1], date_parts[2]
                    if year.startswith("20") and len(year) == 4:
                        snapshot_date = datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d").date()
            except:
                pass  # Use current date as fallback

        # Count records
        records_count = len([k for k in benchmark_data.get("records", {}).keys() if k != "_version"])

        insert_query = """
        INSERT INTO northlight_benchmarks.benchmark_snapshots
        (version, snapshot_date, description, records_count, file_checksum)
        VALUES (:version, :snapshot_date, :description, :records_count, :file_checksum)
        RETURNING id
        """

        # Simple checksum of the file
        file_checksum = str(hash(json.dumps(benchmark_data, sort_keys=True)))

        result = await session.execute(text(insert_query), {
            "version": version,
            "snapshot_date": snapshot_date,
            "description": f"Migrated from {source_file.name}",
            "records_count": records_count,
            "file_checksum": file_checksum
        })

        snapshot_id = result.scalar()
        self.logger.info(f"Created benchmark snapshot: {snapshot_id} (version: {version})")
        return snapshot_id

    async def _create_or_get_category(self, session, category: str, subcategory: str) -> str:
        """Create or get benchmark category ID."""
        # Check if category exists
        select_query = """
        SELECT id FROM northlight_benchmarks.benchmark_categories
        WHERE category = :category AND subcategory = :subcategory
        """

        result = await session.execute(text(select_query), {
            "category": category,
            "subcategory": subcategory
        })

        existing = result.scalar()
        if existing:
            return existing

        # Create new category
        insert_query = """
        INSERT INTO northlight_benchmarks.benchmark_categories
        (category, subcategory, display_name, active)
        VALUES (:category, :subcategory, :display_name, true)
        RETURNING id
        """

        display_name = f"{category} - {subcategory}" if subcategory != category else category

        result = await session.execute(text(insert_query), {
            "category": category,
            "subcategory": subcategory,
            "display_name": display_name
        })

        category_id = result.scalar()
        self.logger.debug(f"Created category: {category}/{subcategory} (ID: {category_id})")
        return category_id

    async def _create_benchmark_data(self, session, snapshot_id: str, category_id: str,
                                   key: str, record_data: Dict[str, Any]) -> bool:
        """Create benchmark data record."""
        try:
            # Extract metrics from the record data
            cpl_data = record_data.get("cpl", {})
            cpc_data = record_data.get("cpc", {})
            ctr_data = record_data.get("ctr", {})
            budget_data = record_data.get("budget", {})

            # Prepare insert data
            insert_data = {
                "snapshot_id": snapshot_id,
                "category_id": category_id,
                "key": key,

                # CPL metrics
                "cpl_median": self._extract_metric(cpl_data, "median"),
                "cpl_top10": self._extract_metric(cpl_data.get("dms", {}), "top10"),
                "cpl_top25": self._extract_metric(cpl_data.get("dms", {}), "top25"),
                "cpl_avg": self._extract_metric(cpl_data.get("dms", {}), "avg"),
                "cpl_bot25": self._extract_metric(cpl_data.get("dms", {}), "bot25"),
                "cpl_bot10": self._extract_metric(cpl_data.get("dms", {}), "bot10"),

                # CPC metrics
                "cpc_median": self._extract_metric(cpc_data, "median"),
                "cpc_top10": self._extract_metric(cpc_data.get("dms", {}), "top10"),
                "cpc_top25": self._extract_metric(cpc_data.get("dms", {}), "top25"),
                "cpc_avg": self._extract_metric(cpc_data.get("dms", {}), "avg"),
                "cpc_bot25": self._extract_metric(cpc_data.get("dms", {}), "bot25"),
                "cpc_bot10": self._extract_metric(cpc_data.get("dms", {}), "bot10"),

                # CTR metrics
                "ctr_median": self._extract_metric(ctr_data, "median"),
                "ctr_top10": self._extract_metric(ctr_data.get("dms", {}), "top10"),
                "ctr_top25": self._extract_metric(ctr_data.get("dms", {}), "top25"),
                "ctr_avg": self._extract_metric(ctr_data.get("dms", {}), "avg"),
                "ctr_bot25": self._extract_metric(ctr_data.get("dms", {}), "bot25"),
                "ctr_bot10": self._extract_metric(ctr_data.get("dms", {}), "bot10"),

                # Budget metrics
                "budget_median": self._extract_metric(budget_data, "median"),
                "budget_p10_bottom": self._extract_metric(budget_data.get("dms", {}), "p10_bottom"),
                "budget_p25_bottom": self._extract_metric(budget_data.get("dms", {}), "p25_bottom"),
                "budget_avg": self._extract_metric(budget_data.get("dms", {}), "avg"),
                "budget_p25_top": self._extract_metric(budget_data.get("dms", {}), "p25_top"),
                "budget_p10_top": self._extract_metric(budget_data.get("dms", {}), "p10_top"),

                # Quality metrics
                "sample_size": record_data.get("sample_size"),
                "confidence_level": record_data.get("confidence_level"),
                "data_quality_score": record_data.get("data_quality_score")
            }

            insert_query = """
            INSERT INTO northlight_benchmarks.benchmark_data (
                snapshot_id, category_id, key,
                cpl_median, cpl_top10, cpl_top25, cpl_avg, cpl_bot25, cpl_bot10,
                cpc_median, cpc_top10, cpc_top25, cpc_avg, cpc_bot25, cpc_bot10,
                ctr_median, ctr_top10, ctr_top25, ctr_avg, ctr_bot25, ctr_bot10,
                budget_median, budget_p10_bottom, budget_p25_bottom, budget_avg, budget_p25_top, budget_p10_top,
                sample_size, confidence_level, data_quality_score
            ) VALUES (
                :snapshot_id, :category_id, :key,
                :cpl_median, :cpl_top10, :cpl_top25, :cpl_avg, :cpl_bot25, :cpl_bot10,
                :cpc_median, :cpc_top10, :cpc_top25, :cpc_avg, :cpc_bot25, :cpc_bot10,
                :ctr_median, :ctr_top10, :ctr_top25, :ctr_avg, :ctr_bot25, :ctr_bot10,
                :budget_median, :budget_p10_bottom, :budget_p25_bottom, :budget_avg, :budget_p25_top, :budget_p10_top,
                :sample_size, :confidence_level, :data_quality_score
            )
            """

            await session.execute(text(insert_query), insert_data)
            return True

        except Exception as e:
            self.logger.error(f"Failed to create benchmark data for key {key}: {e}")
            return False

    def _extract_metric(self, data: Dict[str, Any], key: str) -> float:
        """Extract numeric metric from data, handling various formats."""
        if not isinstance(data, dict):
            return None

        value = data.get(key)
        if value is None:
            return None

        try:
            return float(value)
        except (ValueError, TypeError):
            return None


async def main():
    """Main migration function."""
    setup_logging()
    logger = get_logger("benchmark_migration")

    logger.info("Starting benchmark data migration...")

    try:
        migrator = BenchmarkDataMigrator()
        result = await migrator.migrate_northlight_benchmarks()

        if result["status"] == "completed":
            logger.info("Benchmark migration completed successfully")
            print("\n" + "="*60)
            print("BENCHMARK MIGRATION SUMMARY")
            print("="*60)
            print(f"Status: {result['status']}")
            print(f"Source file: {result['source_file']}")

            migration_stats = result['migration_result']
            print(f"Categories migrated: {migration_stats['categories_migrated']}")
            print(f"Records migrated: {migration_stats['records_migrated']}")
            print("="*60)
            return 0
        else:
            logger.error(f"Benchmark migration failed: {result.get('reason', 'Unknown error')}")
            return 1

    except Exception as e:
        logger.error(f"Benchmark migration script failed: {e}")
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nMigration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Migration script failed: {e}")
        sys.exit(1)