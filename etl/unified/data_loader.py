#!/usr/bin/env python3
"""
Unified Data Loader
Handles loading all 10 extracted data files into PostgreSQL with proper encoding and validation
"""

import asyncio
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timezone, date
import logging
import sys
import re
from io import StringIO

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.config import settings
from core.database import get_db_session, init_database
from core.shared import setup_logging, get_logger, PerformanceTimer


class UnifiedDataLoader:
    """Universal data loader for all ETL data sources."""

    def __init__(self):
        self.logger = get_logger("unified_data_loader")
        self.load_stats = {
            "files_processed": 0,
            "total_rows_loaded": 0,
            "loading_errors": [],
            "start_time": None,
            "end_time": None
        }

        # File mapping configuration
        self.file_configs = {
            "ultimate_dms": {
                "file_pattern": "ultimate_dms_*.csv",
                "table": "heartbeat_core.ultimate_dms_campaigns",
                "unique_key": ["campaign_id", "snapshot_date"],
                "encoding_issues": True
            },
            "budget_waterfall_client": {
                "file_pattern": "budget_waterfall_client_*.csv",
                "table": "heartbeat_core.budget_waterfall_client",
                "unique_key": ["advertiser_name", "from_sod", "to_eod"],
                "encoding_issues": True
            },
            "budget_waterfall_channel": {
                "file_pattern": "budget_waterfall_channel_*.csv",
                "table": "heartbeat_core.budget_waterfall_channel",
                "unique_key": ["channel", "advertiser_name", "from_sod", "to_eod"],
                "encoding_issues": True
            },
            "sf_partner_pipeline": {
                "file_pattern": "sf_partner_pipeline_*.csv",
                "table": "heartbeat_salesforce.sf_partner_pipeline",
                "unique_key": ["opportunity_name", "account_name", "created_date"],
                "encoding_issues": False
            },
            "sf_partner_calls": {
                "file_pattern": "sf_partner_calls_*.csv",
                "table": "heartbeat_salesforce.sf_partner_calls",
                "unique_key": ["call_id", "partner_name", "call_date"],
                "encoding_issues": False
            },
            "sf_tim_king_partner_pipeline": {
                "file_pattern": "sf_tim_king_partner_pipeline_*.csv",
                "table": "heartbeat_salesforce.sf_tim_king_partner_pipeline",
                "unique_key": ["opportunity_name", "account_name", "created_date"],
                "encoding_issues": False
            },
            "sf_grader_opportunities": {
                "file_pattern": "sf_grader_opportunities_*.csv",
                "table": "heartbeat_salesforce.sf_grader_opportunities",
                "unique_key": ["opportunity_id", "graded_date"],
                "encoding_issues": False
            },
            "dfp_rij": {
                "file_pattern": "dfp_rij_*.csv",
                "table": "heartbeat_core.dfp_rij_alerts",
                "unique_key": ["business_id", "alert_type", "created_date"],
                "encoding_issues": True
            },
            "agreed_cpl_performance": {
                "file_pattern": "agreed_cpl_performance_*.csv",
                "table": "heartbeat_performance.agreed_cpl_performance",
                "unique_key": ["advertiser_id", "mcid", "measurement_period_start", "measurement_period_end"],
                "encoding_issues": True
            },
            "spend_revenue_performance": {
                "file_pattern": "spend_revenue_*.csv",
                "table": "heartbeat_performance.spend_revenue_performance",
                "unique_key": ["maid", "campaign_id", "performance_period_start", "performance_period_end"],
                "encoding_issues": True
            }
        }

    def clean_unicode_text(self, text: str) -> str:
        """Clean Unicode text with extra spaces and special characters."""
        if pd.isna(text) or text is None:
            return ""

        # Convert to string if not already
        text = str(text)

        # Remove Unicode BOM and control characters
        text = text.replace('\ufeff', '')  # BOM
        text = text.replace('\u0000', '')  # Null characters

        # Clean up extra spaces between characters (common in our files)
        text = re.sub(r'(\w)\s+(\w)', r'\1\2', text)
        text = re.sub(r'\s+', ' ', text)  # Multiple spaces to single
        text = text.strip()

        return text

    def read_csv_with_encoding_handling(self, file_path: Path, has_encoding_issues: bool = False) -> pd.DataFrame:
        """Read CSV with proper encoding handling."""
        try:
            # First try standard UTF-8
            if not has_encoding_issues:
                return pd.read_csv(file_path, encoding='utf-8')

            # For files with Unicode issues, try multiple approaches
            encodings_to_try = ['utf-16', 'utf-8-sig', 'utf-8', 'cp1252', 'latin1']

            for encoding in encodings_to_try:
                try:
                    self.logger.info(f"Trying encoding {encoding} for {file_path.name}")
                    df = pd.read_csv(file_path, encoding=encoding, sep='\t' if encoding.startswith('utf-16') else ',')

                    # Clean column names
                    df.columns = [self.clean_unicode_text(col) for col in df.columns]

                    # Clean all text data
                    for col in df.select_dtypes(include=['object']).columns:
                        df[col] = df[col].apply(self.clean_unicode_text)

                    self.logger.info(f"Successfully read {file_path.name} with encoding {encoding}")
                    return df

                except Exception as e:
                    self.logger.debug(f"Failed to read with {encoding}: {e}")
                    continue

            # If all encodings fail, raise the last exception
            raise Exception(f"Could not read {file_path.name} with any encoding")

        except Exception as e:
            self.logger.error(f"Failed to read {file_path}: {e}")
            raise

    def standardize_column_names(self, df: pd.DataFrame, data_source: str) -> pd.DataFrame:
        """Standardize column names to match database schema."""
        # Create a copy to avoid modifying original
        df = df.copy()

        # Handle duplicate column names first
        cols = []
        seen = {}
        for col in df.columns:
            if col in seen:
                seen[col] += 1
                cols.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                cols.append(col)
        df.columns = cols

        # General column name standardization
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.replace('-', '_')
        df.columns = df.columns.str.replace('.', '_')
        df.columns = df.columns.str.replace('(', '')
        df.columns = df.columns.str.replace(')', '')
        df.columns = df.columns.str.replace('%', '_pct')

        # Source-specific column mappings
        column_mappings = {
            "ultimate_dms": {
                "last_active": "last_active",
                "channel": "channel",
                "bid_name": "bid_name",
                "bid": "bid",
                "advertiser_name": "advertiser_name",
                "primary_user_name": "primary_user_name",
                "am": "am",
                "am_manager": "am_manager",
                "optimizer_1_manager": "optimizer_1_manager",
                "optimizer_1": "optimizer_1",
                "optimizer_2_manager": "optimizer_2_manager",
                "optimizer_2": "optimizer_2",
                "maid": "maid",
                "mcid_clicks": "mcid_clicks",
                "mcid_leads": "mcid_leads",
                "mcid": "mcid",
                "campaign_name": "campaign_name",
                "campaign_id": "campaign_id",
                "product": "product",
                "offer_name": "offer_name",
                "finance_product": "finance_product",
                "tracking_method_name": "tracking_method_name",
                "sem_reviews_p30": "sem_reviews_p30",
                "io_cycle": "io_cycle",
                "avg_cycle_length": "avg_cycle_length",
                "running_cid_leads": "running_cid_leads",
                "amount_spent": "amount_spent",
                "days_elapsed": "days_elapsed",
                "utilization": "utilization",
                "campaign_performance_rating": "campaign_performance_rating",
                "bc": "bc",
                "bsc": "bsc",
                "campaign_budget": "campaign_budget"
            },
            "sf_partner_pipeline": {
                "account_owner": "account_owner",
                "bid": "bid",
                "account_name": "account_name",
                "opportunity_name": "opportunity_name",
                "type": "type_1",
                "type_1": "type_2",
                "created_date": "created_date",
                "last_stage_change_date": "last_stage_change_date",
                "all_tcv": "all_tcv",
                "mo__offer_amt": "mo_offer_amt",
                "net_new_tcv": "net_new_tcv",
                "modification_amount": "modification_amount",
                "close_date": "close_date",
                "solution": "solution",
                "stage_duration": "stage_duration",
                "cycles": "cycles",
                "processed_date": "processed_date"
            }
        }

        # Apply source-specific mappings if available
        if data_source in column_mappings:
            df = df.rename(columns=column_mappings[data_source])

        return df

    def add_metadata_columns(self, df: pd.DataFrame, source_file: str, snapshot_date: str) -> pd.DataFrame:
        """Add metadata columns required by all tables."""
        df = df.copy()

        # Add snapshot date
        df['snapshot_date'] = pd.to_datetime(snapshot_date).date()

        # Add extraction timestamp
        df['extracted_at'] = datetime.now(timezone.utc)

        # Add source file name
        df['source_file'] = source_file

        return df

    def clean_and_validate_data(self, df: pd.DataFrame, data_source: str) -> pd.DataFrame:
        """Clean and validate data before loading."""
        df = df.copy()

        # Handle missing values
        df = df.replace('', np.nan)
        df = df.replace('NULL', np.nan)

        # Convert date columns
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                    # Replace NaT values with None for database compatibility
                    df[col] = df[col].where(pd.notna(df[col]), None)
                except:
                    self.logger.warning(f"Could not convert {col} to date in {data_source}")

        # Convert numeric columns
        numeric_patterns = ['amount', 'budget', 'tcv', 'cpl', 'cpc', 'leads', 'clicks', 'duration', 'cycles', 'clients']
        for col in df.columns:
            if any(pattern in col.lower() for pattern in numeric_patterns):
                try:
                    # Remove currency symbols and commas
                    if df[col].dtype == 'object':
                        df[col] = df[col].astype(str).str.replace(r'[^\d.-]', '', regex=True)
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    # Replace NaN values with None for database compatibility
                    df[col] = df[col].where(pd.notna(df[col]), None)
                except:
                    self.logger.warning(f"Could not convert {col} to numeric in {data_source}")

        # Replace any remaining NaN/NaT values with None - more aggressive approach
        for col in df.columns:
            try:
                # Handle datetime columns specifically
                if df[col].dtype.name.startswith('datetime') or 'date' in str(df[col].dtype).lower():
                    df[col] = df[col].where(pd.notna(df[col]), None)
                # Handle all other columns
                else:
                    df[col] = df[col].where(pd.notna(df[col]), None)
            except:
                # Fallback: convert series to object and replace problematic values
                df[col] = df[col].astype('object')
                mask = (df[col].isna()) | (df[col] == 'NaT') | (df[col] == 'nan')
                df[col] = df[col].where(~mask, None)

        # Remove completely empty rows
        df = df.dropna(how='all')

        return df

    async def load_data_to_table(self, df: pd.DataFrame, table_name: str, unique_key: List[str]) -> Dict[str, Any]:
        """Load dataframe to PostgreSQL table with upsert logic."""
        try:
            from sqlalchemy import text
            async with get_db_session() as session:
                # Convert DataFrame to list of dictionaries
                records = df.to_dict('records')

                # Final cleaning of records - replace any NaT/NaN values in the dict records
                for record in records:
                    for key, value in record.items():
                        if pd.isna(value) or (hasattr(value, '__class__') and 'NaT' in str(value.__class__)):
                            record[key] = None

                if not records:
                    return {"status": "success", "rows_inserted": 0, "rows_updated": 0}

                # Build upsert query
                columns = list(records[0].keys())

                # Create INSERT statement with proper parameter binding
                # Use :param syntax for SQLAlchemy text()
                insert_sql = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({', '.join([f':{col}' for col in columns])})
                """

                # Add ON CONFLICT clause for upsert
                if unique_key:
                    conflict_columns = ', '.join(unique_key)
                    update_columns = [col for col in columns if col not in unique_key + ['id', 'extracted_at']]
                    update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])

                    insert_sql += f"""
                    ON CONFLICT ({conflict_columns})
                    DO UPDATE SET {update_set}, extracted_at = EXCLUDED.extracted_at
                    """
                else:
                    # If no unique key defined, just ignore conflicts
                    insert_sql += " ON CONFLICT DO NOTHING"

                # Execute batch insert
                rows_affected = 0
                batch_size = 1000

                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]

                    # Execute the batch
                    result = await session.execute(text(insert_sql), batch)
                    rows_affected += len(batch)

                await session.commit()

                self.logger.info(f"Loaded {rows_affected} rows to {table_name}")

                return {
                    "status": "success",
                    "rows_processed": rows_affected,
                    "table": table_name
                }

        except Exception as e:
            self.logger.error(f"Failed to load data to {table_name}: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "table": table_name
            }

    async def process_data_file(self, data_source: str, file_path: Path) -> Dict[str, Any]:
        """Process a single data file and load to database."""
        try:
            with PerformanceTimer(f"Processing {data_source}", self.logger.name):
                # Initialize database if needed
                await init_database()

                config = self.file_configs[data_source]

                self.logger.info(f"Processing {data_source} from {file_path}")

                # Read the CSV file
                df = self.read_csv_with_encoding_handling(file_path, config.get("encoding_issues", False))

                self.logger.info(f"Read {len(df)} rows from {file_path.name}")

                # Standardize column names
                df = self.standardize_column_names(df, data_source)

                # Clean and validate data
                df = self.clean_and_validate_data(df, data_source)

                # Extract snapshot date from filename
                snapshot_date = re.search(r'(\d{4}-\d{2}-\d{2})', file_path.name)
                snapshot_date = snapshot_date.group(1) if snapshot_date else datetime.now().strftime('%Y-%m-%d')

                # Add metadata columns
                df = self.add_metadata_columns(df, file_path.name, snapshot_date)

                # Load to database
                load_result = await self.load_data_to_table(
                    df,
                    config["table"],
                    config["unique_key"]
                )

                if load_result["status"] == "success":
                    self.load_stats["files_processed"] += 1
                    self.load_stats["total_rows_loaded"] += load_result.get("rows_processed", 0)
                else:
                    self.load_stats["loading_errors"].append({
                        "file": data_source,
                        "error": load_result.get("error", "Unknown error")
                    })

                return load_result

        except Exception as e:
            error_msg = f"Failed to process {data_source}: {str(e)}"
            self.logger.error(error_msg)
            self.load_stats["loading_errors"].append({
                "file": data_source,
                "error": str(e)
            })
            return {
                "status": "failed",
                "error": str(e),
                "file": data_source
            }

    async def load_all_data(self, extract_date: str = None) -> Dict[str, Any]:
        """Load all available data files for a given date."""
        if not extract_date:
            extract_date = datetime.now().strftime('%Y-%m-%d')

        self.logger.info(f"Starting bulk data load for {extract_date}")
        self.load_stats["start_time"] = datetime.now(timezone.utc)

        try:
            # Initialize database if needed
            await init_database()

            # Find and process all data files
            raw_data_path = Path(settings.RAW_DATA_PATH)
            loading_results = {}

            for data_source, config in self.file_configs.items():
                try:
                    # Find the file for this data source
                    source_dir = raw_data_path / data_source
                    if not source_dir.exists():
                        self.logger.warning(f"Source directory not found: {source_dir}")
                        continue

                    # Look for files matching the pattern and date
                    pattern = config["file_pattern"].replace("*", extract_date)
                    matching_files = list(source_dir.glob(pattern))

                    if not matching_files:
                        self.logger.warning(f"No files found for {data_source} on {extract_date}")
                        continue

                    # Process the most recent file if multiple found
                    file_path = max(matching_files, key=lambda x: x.stat().st_mtime)

                    # Load the data
                    result = await self.process_data_file(data_source, file_path)
                    loading_results[data_source] = result

                except Exception as e:
                    error_msg = f"Error processing {data_source}: {str(e)}"
                    self.logger.error(error_msg)
                    loading_results[data_source] = {
                        "status": "failed",
                        "error": str(e)
                    }

            self.load_stats["end_time"] = datetime.now(timezone.utc)

            # Calculate summary
            total_duration = (self.load_stats["end_time"] - self.load_stats["start_time"]).total_seconds()

            self.logger.info(f"Bulk load completed in {total_duration:.2f} seconds")
            self.logger.info(f"Files processed: {self.load_stats['files_processed']}")
            self.logger.info(f"Total rows loaded: {self.load_stats['total_rows_loaded']}")

            if self.load_stats["loading_errors"]:
                self.logger.warning(f"Loading errors: {len(self.load_stats['loading_errors'])}")

            return {
                "status": "completed",
                "load_stats": self.load_stats,
                "file_results": loading_results,
                "extract_date": extract_date
            }

        except Exception as e:
            self.load_stats["end_time"] = datetime.now(timezone.utc)
            self.logger.error(f"Bulk load failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e),
                "load_stats": self.load_stats
            }


async def main():
    """Main function for running data loader."""
    # Setup logging
    setup_logging()
    logger = get_logger("unified_data_loader")

    import argparse
    parser = argparse.ArgumentParser(description="Load extracted data into PostgreSQL")
    parser.add_argument("--date", help="Extract date (YYYY-MM-DD)", default=None)
    parser.add_argument("--source", help="Specific data source to load", default=None)

    args = parser.parse_args()

    try:
        loader = UnifiedDataLoader()

        if args.source:
            # Load specific source
            raw_data_path = Path(settings.RAW_DATA_PATH)
            source_dir = raw_data_path / args.source

            if not source_dir.exists():
                logger.error(f"Source directory not found: {source_dir}")
                return 1

            # Find the file
            extract_date = args.date or datetime.now().strftime('%Y-%m-%d')
            config = loader.file_configs.get(args.source)

            if not config:
                logger.error(f"Unknown data source: {args.source}")
                return 1

            pattern = config["file_pattern"].replace("*", extract_date)
            matching_files = list(source_dir.glob(pattern))

            if not matching_files:
                logger.error(f"No files found for {args.source} on {extract_date}")
                return 1

            file_path = max(matching_files, key=lambda x: x.stat().st_mtime)
            result = await loader.process_data_file(args.source, file_path)

            if result["status"] == "success":
                logger.info(f"Successfully loaded {args.source}")
                return 0
            else:
                logger.error(f"Failed to load {args.source}: {result.get('error')}")
                return 1
        else:
            # Load all data
            result = await loader.load_all_data(args.date)

            if result["status"] == "completed":
                # Print summary
                print("\n" + "="*60)
                print("DATA LOADING SUMMARY")
                print("="*60)
                print(f"Extract Date: {result['extract_date']}")
                print(f"Files Processed: {result['load_stats']['files_processed']}")
                print(f"Total Rows Loaded: {result['load_stats']['total_rows_loaded']}")
                print(f"Loading Errors: {len(result['load_stats']['loading_errors'])}")

                if result['load_stats']['loading_errors']:
                    print("\nErrors:")
                    for error in result['load_stats']['loading_errors']:
                        print(f"  - {error['file']}: {error['error']}")

                print("\nFile Results:")
                for source, file_result in result['file_results'].items():
                    status = "✓" if file_result['status'] == 'success' else "✗"
                    rows = file_result.get('rows_processed', 0)
                    print(f"  - {source}: {status} ({rows} rows)")

                print("="*60)
                return 0
            else:
                logger.error(f"Bulk load failed: {result.get('error')}")
                return 1

    except Exception as e:
        logger.error(f"Data loader failed: {str(e)}")
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nData loading interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Data loader failed: {str(e)}")
        sys.exit(1)