#!/usr/bin/env python3
"""
Analyze Heartbeat DuckDB data to understand the structure
"""

import duckdb
import pandas as pd
from pathlib import Path

def analyze_duckdb(db_path):
    """Analyze a DuckDB database."""
    print(f"\n=== Analyzing {db_path.name} ===")

    try:
        conn = duckdb.connect(str(db_path))

        # Get all tables
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"Found {len(tables)} tables:")

        for table in tables:
            table_name = table[0]
            print(f"\n--- Table: {table_name} ---")

            # Get table info
            try:
                info = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
                print(f"Columns ({len(info)}):")
                for col in info[:10]:  # Show first 10 columns
                    print(f"  - {col[1]} ({col[2]})")
                if len(info) > 10:
                    print(f"  ... and {len(info) - 10} more columns")

                # Get row count
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"Rows: {count:,}")

                # Show sample data for key tables
                if 'campaign' in table_name.lower() or 'performance' in table_name.lower():
                    print("Sample data:")
                    sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
                    if sample:
                        for i, row in enumerate(sample):
                            print(f"  Row {i+1}: {str(row)[:200]}...")

            except Exception as e:
                print(f"  Error analyzing table: {e}")

        conn.close()

    except Exception as e:
        print(f"Error connecting to {db_path}: {e}")

def main():
    """Main analysis function."""
    print("HEARTBEAT DATA ANALYSIS")
    print("="*50)

    # Analyze both DuckDB files
    heartbeat_db = Path("C:/Users/Roci/heartbeat/data/warehouse/heartbeat.duckdb")
    northlight_db = Path("C:/Users/Roci/heartbeat/data/warehouse/northlight.duckdb")

    if heartbeat_db.exists():
        analyze_duckdb(heartbeat_db)
    else:
        print(f"Heartbeat DB not found: {heartbeat_db}")

    if northlight_db.exists():
        analyze_duckdb(northlight_db)
    else:
        print(f"Northlight DB not found: {northlight_db}")

    # Look for the most recent parquet files
    warehouse_dir = Path("C:/Users/Roci/heartbeat/data/warehouse")
    print(f"\n=== Recent Parquet Files ===")

    parquet_files = list(warehouse_dir.glob("*.parquet"))
    recent_files = sorted([f for f in parquet_files if "2025-09-17" in f.name])

    print(f"Found {len(recent_files)} files from today:")
    for f in recent_files:
        print(f"  - {f.name} ({f.stat().st_size / 1024:.1f} KB)")

if __name__ == "__main__":
    main()