#!/usr/bin/env python3
"""
Check actual column names in heartbeat tables
"""

import duckdb
from pathlib import Path

def check_table_columns(db_path, table_name):
    """Check columns in a specific table."""
    conn = duckdb.connect(str(db_path))
    try:
        info = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        print(f"\n{table_name} columns:")
        for col in info:
            print(f"  - {col[1]} ({col[2]})")
        return [col[1] for col in info]
    except Exception as e:
        print(f"Error checking {table_name}: {e}")
        return []
    finally:
        conn.close()

def main():
    heartbeat_db = Path("C:/Users/Roci/heartbeat/data/warehouse/heartbeat.duckdb")

    # Check key tables
    check_table_columns(heartbeat_db, "master_campaigns")
    check_table_columns(heartbeat_db, "spend_revenue_performance_current")

if __name__ == "__main__":
    main()