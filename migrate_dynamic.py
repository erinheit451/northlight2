#!/usr/bin/env python3
"""
Dynamic Heartbeat Data Migration - Inspects source schemas and creates matching tables
"""

import duckdb
import pandas as pd
from sqlalchemy import create_engine, text
import traceback

# Database configuration
DATABASE_URL = "postgresql+psycopg2://northlight_user:northlight_secure_2024@localhost:5432/unified_northlight"
HEARTBEAT_DB = "../heartbeat/data/warehouse/heartbeat.duckdb"

def create_table_from_dataframe(df, table_name, engine):
    """Create PostgreSQL table based on DataFrame structure"""

    # Map pandas dtypes to PostgreSQL types
    def get_pg_type(dtype):
        if dtype == 'object':
            return 'TEXT'
        elif 'int' in str(dtype):
            return 'INTEGER'
        elif 'float' in str(dtype):
            return 'NUMERIC(12,4)'
        elif 'bool' in str(dtype):
            return 'BOOLEAN'
        elif 'datetime' in str(dtype):
            return 'TIMESTAMP'
        else:
            return 'TEXT'

    # Clean column names for PostgreSQL
    clean_columns = []
    for col in df.columns:
        clean_col = col.lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '').replace('%', 'pct')
        clean_columns.append(f'"{clean_col}" {get_pg_type(df[col].dtype)}')

    columns_sql = ',\n    '.join(clean_columns)

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS analytics.{table_name} (
        id SERIAL PRIMARY KEY,
        {columns_sql},
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS analytics.{table_name}"))
        conn.execute(text(create_sql))
        conn.commit()

    print(f"  Created table analytics.{table_name} with {len(df.columns)} columns")

def migrate_table_dynamic(duckdb_conn, pg_engine, source_table):
    """Dynamically migrate table based on actual source structure"""
    try:
        print(f"Migrating {source_table}...")

        # Get data from DuckDB
        df = duckdb_conn.execute(f'SELECT * FROM "{source_table}"').df()

        if df.empty:
            print(f"  No data found in {source_table}")
            return 0

        # Clean column names for PostgreSQL
        df.columns = [col.lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '').replace('%', 'pct') for col in df.columns]

        # Convert data types for PostgreSQL compatibility
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)
                df[col] = df[col].replace('nan', None)
                df[col] = df[col].replace('None', None)
            elif df[col].dtype in ['float64', 'float32']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif df[col].dtype in ['int64', 'int32']:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        # Create table based on DataFrame structure
        table_name = source_table.lower().replace(' ', '_')
        create_table_from_dataframe(df, table_name, pg_engine)

        # Insert data
        rows_inserted = 0
        batch_size = 500
        total_rows = len(df)

        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            batch_df.to_sql(
                table_name,
                pg_engine,
                schema='analytics',
                if_exists='append',
                index=False,
                method='multi'
            )
            rows_inserted += len(batch_df)
            print(f"  Inserted {rows_inserted}/{total_rows} rows")

        print(f"  SUCCESS: migrated {rows_inserted} rows")
        return rows_inserted

    except Exception as e:
        print(f"  ERROR migrating {source_table}: {str(e)}")
        return 0

def main():
    print("=" * 60)
    print("DYNAMIC HEARTBEAT DATA MIGRATION")
    print("=" * 60)

    try:
        # Connect to databases
        duckdb_conn = duckdb.connect(HEARTBEAT_DB)
        pg_engine = create_engine(DATABASE_URL)
        print("Connected to both databases")

        # Create analytics schema
        with pg_engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
            conn.commit()
        print("Analytics schema ready")

        # Get list of working tables from our investigation
        working_tables = [
            'advertiser_product_saturation',
            'master_account_managers',
            'master_advertisers',
            'master_campaigns',
            'master_growth_managers',
            'master_partners',
            'master_partners_v2',
            'master_product_catalog',
            'master_salesforce_integration',
            'partner_product_saturation_dashboard',
            'product_saturation_kpis',
            'salesforce_to_corporate_mapping',
            'scorecard_metrics',
            'scorecard_summary',
            'sf_partner_calls_historical_staging',
            'spend_revenue_am_performance',
            'spend_revenue_business_trends',
            'spend_revenue_data_quality',
            'spend_revenue_performance_current',
            'spend_revenue_performance_historical',
            'spend_revenue_seasonal_analysis',
            'spend_revenue_time_series',
            'spend_revenue_yoy_comparison'
        ]

        # Migrate each table
        total_migrated = 0
        migration_summary = []

        for table in working_tables:
            rows = migrate_table_dynamic(duckdb_conn, pg_engine, table)
            migration_summary.append((table, rows))
            total_migrated += rows

        # Print summary
        print("\n" + "=" * 60)
        print("MIGRATION SUMMARY")
        print("=" * 60)

        for table, rows in migration_summary:
            status = "SUCCESS" if rows > 0 else "FAILED"
            print(f"{status:<8} {table:<45} -> {rows:>8,} rows")

        print(f"\nTOTAL ROWS MIGRATED: {total_migrated:,}")
        print("=" * 60)

        # Close connections
        duckdb_conn.close()
        pg_engine.dispose()

        print("Migration completed!")

    except Exception as e:
        print(f"Migration failed: {str(e)}")
        traceback.print_exc()
        return 1

    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)