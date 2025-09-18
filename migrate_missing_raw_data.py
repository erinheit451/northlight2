#!/usr/bin/env python3
"""
Migrate Missing Raw Data Sources from Heartbeat
Processes all the CSV files that weren't migrated in the initial migration
"""

import pandas as pd
import os
import glob
from sqlalchemy import create_engine, text
import traceback
from datetime import datetime

# Database configuration
DATABASE_URL = "postgresql+psycopg2://northlight_user:northlight_secure_2024@localhost:5432/unified_northlight"
RAW_DATA_PATH = "C:/Users/Roci/heartbeat/data/raw"

def clean_table_name(name):
    """Clean table name for PostgreSQL"""
    return name.lower().replace('-', '_').replace(' ', '_')

def clean_column_name(name):
    """Clean column name for PostgreSQL"""
    return name.lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '').replace('%', 'pct').replace('.', '_')

def create_table_from_csv(csv_file, table_name, engine):
    """Create PostgreSQL table based on CSV structure"""
    try:
        # Try different encodings to handle the file
        encodings = ['utf-8', 'utf-16', 'latin-1', 'cp1252']
        df = None

        for encoding in encodings:
            try:
                df = pd.read_csv(csv_file, encoding=encoding, nrows=5)
                print(f"  Successfully read with {encoding} encoding")
                break
            except:
                continue

        if df is None:
            print(f"  ERROR: Could not read {csv_file} with any encoding")
            return False

        # Clean column names
        df.columns = [clean_column_name(col) for col in df.columns]

        # Create table schema
        columns_sql = []
        for col in df.columns:
            columns_sql.append(f'"{col}" TEXT')

        columns_sql_str = ',\n    '.join(columns_sql)

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS analytics.{table_name} (
            id SERIAL PRIMARY KEY,
            {columns_sql_str},
            source_file TEXT,
            load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS analytics.{table_name}"))
            conn.execute(text(create_sql))
            conn.commit()

        print(f"  Created table analytics.{table_name} with {len(df.columns)} columns")
        return True

    except Exception as e:
        print(f"  ERROR creating table for {csv_file}: {str(e)}")
        return False

def migrate_csv_data(csv_file, table_name, engine, encoding='utf-8'):
    """Migrate CSV data to PostgreSQL table"""
    try:
        # Read the full CSV file
        encodings = ['utf-8', 'utf-16', 'latin-1', 'cp1252']
        df = None

        for enc in encodings:
            try:
                df = pd.read_csv(csv_file, encoding=enc)
                break
            except:
                continue

        if df is None:
            return 0

        # Clean column names to match table
        df.columns = [clean_column_name(col) for col in df.columns]

        # Add metadata
        df['source_file'] = os.path.basename(csv_file)

        # Clean data - convert all to string and handle nulls
        for col in df.columns:
            if col not in ['source_file']:
                df[col] = df[col].astype(str)
                df[col] = df[col].replace('nan', None)
                df[col] = df[col].replace('None', None)

        # Insert data in batches
        batch_size = 500
        total_rows = len(df)
        rows_inserted = 0

        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            batch_df.to_sql(
                table_name,
                engine,
                schema='analytics',
                if_exists='append',
                index=False,
                method='multi'
            )
            rows_inserted += len(batch_df)
            print(f"    Inserted {rows_inserted}/{total_rows} rows")

        return rows_inserted

    except Exception as e:
        print(f"    ERROR inserting data from {csv_file}: {str(e)}")
        return 0

def migrate_data_source(source_name, engine):
    """Migrate all CSV files for a data source"""
    print(f"\n{'='*60}")
    print(f"MIGRATING: {source_name}")
    print(f"{'='*60}")

    source_path = os.path.join(RAW_DATA_PATH, source_name)
    table_name = clean_table_name(source_name)

    # Find all CSV files
    csv_files = glob.glob(os.path.join(source_path, "*.csv"))

    if not csv_files:
        print(f"  No CSV files found in {source_path}")
        return 0

    print(f"  Found {len(csv_files)} CSV files")

    # Create table based on first file
    first_file = csv_files[0]
    if not create_table_from_csv(first_file, table_name, engine):
        return 0

    # Migrate all files
    total_rows = 0
    for csv_file in csv_files:
        print(f"  Processing: {os.path.basename(csv_file)}")
        rows = migrate_csv_data(csv_file, table_name, engine)
        total_rows += rows

    print(f"  TOTAL ROWS MIGRATED: {total_rows:,}")
    return total_rows

def main():
    print("="*80)
    print("MISSING RAW DATA MIGRATION")
    print("="*80)

    # Raw data sources that need migration
    missing_sources = [
        'agreed_cpl_performance',
        'bsc_standards',
        'budget_waterfall_channel',
        'budget_waterfall_client',
        'ultimate_dms',
        'sf_partner_pipeline',
        'dfp_rij',
        'report_6615',
        'sf_grader_opportunities',
        'sf_tim_king_partner_pipeline'
    ]

    try:
        # Connect to PostgreSQL
        engine = create_engine(DATABASE_URL)
        print("Connected to PostgreSQL")

        # Ensure analytics schema exists
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
            conn.commit()

        # Migrate each source
        migration_summary = []
        total_migrated = 0

        for source in missing_sources:
            rows = migrate_data_source(source, engine)
            migration_summary.append((source, rows))
            total_migrated += rows

        # Print final summary
        print(f"\n{'='*80}")
        print("MIGRATION SUMMARY")
        print(f"{'='*80}")

        for source, rows in migration_summary:
            status = "SUCCESS" if rows > 0 else "FAILED"
            print(f"{status:<8} {source:<35} -> {rows:>8,} rows")

        print(f"\nGRAND TOTAL ROWS MIGRATED: {total_migrated:,}")
        print(f"SOURCES PROCESSED: {len([x for x in migration_summary if x[1] > 0])}/{len(missing_sources)}")
        print("="*80)

        engine.dispose()
        print("Migration completed!")

    except Exception as e:
        print(f"Migration failed: {str(e)}")
        traceback.print_exc()
        return 1

    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)