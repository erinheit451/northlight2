#!/usr/bin/env python3
"""
Complete Heartbeat Data Migration to PostgreSQL - Fixed Unicode
Migrates all remaining analytics and business data from heartbeat.duckdb
"""

import asyncio
import sys
from pathlib import Path
import duckdb
import pandas as pd
from datetime import datetime, date
from decimal import Decimal
import json
import traceback
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer, Numeric, DateTime, Boolean, Text
from sqlalchemy.dialects.postgresql import UUID
import uuid

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Database configuration
DATABASE_URL = "postgresql+psycopg2://northlight_user:northlight_secure_2024@localhost:5432/unified_northlight"
HEARTBEAT_DB = "../heartbeat/data/warehouse/heartbeat.duckdb"

def create_analytics_schema(engine):
    """Create analytics schema and tables for heartbeat data"""
    print("Creating analytics schema...")

    with engine.connect() as conn:
        # Create analytics schema
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
        conn.commit()

        # Create tables for each data type
        tables_sql = """
        -- Spend Revenue Performance Tables
        CREATE TABLE IF NOT EXISTS analytics.spend_revenue_performance_current (
            id SERIAL PRIMARY KEY,
            maid VARCHAR(50),
            advertiser_name TEXT,
            partner_name TEXT,
            am TEXT,
            optimizer TEXT,
            gm TEXT,
            business_category TEXT,
            campaign_budget NUMERIC(12,2),
            amount_spent NUMERIC(12,2),
            days_elapsed INTEGER,
            utilization NUMERIC(5,2),
            running_cid_leads INTEGER,
            cpl_agreed NUMERIC(8,2),
            cid_cpl NUMERIC(8,2),
            cpl_mcid NUMERIC(8,2),
            campaign_performance_rating TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS analytics.spend_revenue_performance_historical (
            id SERIAL PRIMARY KEY,
            maid VARCHAR(50),
            advertiser_name TEXT,
            partner_name TEXT,
            am TEXT,
            optimizer TEXT,
            gm TEXT,
            business_category TEXT,
            campaign_budget NUMERIC(12,2),
            amount_spent NUMERIC(12,2),
            days_elapsed INTEGER,
            utilization NUMERIC(5,2),
            running_cid_leads INTEGER,
            cpl_agreed NUMERIC(8,2),
            cid_cpl NUMERIC(8,2),
            cpl_mcid NUMERIC(8,2),
            campaign_performance_rating TEXT,
            record_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Master Data Tables
        CREATE TABLE IF NOT EXISTS analytics.master_advertisers (
            id SERIAL PRIMARY KEY,
            maid VARCHAR(50) UNIQUE,
            advertiser_name TEXT,
            partner_name TEXT,
            am TEXT,
            optimizer TEXT,
            gm TEXT,
            business_category TEXT,
            total_budget NUMERIC(12,2),
            total_spent NUMERIC(12,2),
            campaign_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS analytics.master_campaigns (
            id SERIAL PRIMARY KEY,
            campaign_id VARCHAR(50),
            maid VARCHAR(50),
            advertiser_name TEXT,
            partner_name TEXT,
            campaign_name TEXT,
            am TEXT,
            optimizer TEXT,
            gm TEXT,
            business_category TEXT,
            campaign_budget NUMERIC(12,2),
            amount_spent NUMERIC(12,2),
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS analytics.master_partners (
            id SERIAL PRIMARY KEY,
            partner_name TEXT UNIQUE,
            total_advertisers INTEGER,
            total_campaigns INTEGER,
            total_budget NUMERIC(12,2),
            total_spent NUMERIC(12,2),
            avg_performance_rating TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Analytics and Scorecard Tables
        CREATE TABLE IF NOT EXISTS analytics.scorecard_metrics (
            id SERIAL PRIMARY KEY,
            metric_name TEXT,
            metric_value NUMERIC(12,4),
            metric_type TEXT,
            calculation_date DATE,
            partner_name TEXT,
            business_category TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS analytics.scorecard_summary (
            id SERIAL PRIMARY KEY,
            partner_name TEXT,
            total_score NUMERIC(5,2),
            performance_grade TEXT,
            campaigns_count INTEGER,
            avg_utilization NUMERIC(5,2),
            avg_cpl NUMERIC(8,2),
            summary_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Product Saturation Tables
        CREATE TABLE IF NOT EXISTS analytics.advertiser_product_saturation (
            id SERIAL PRIMARY KEY,
            maid VARCHAR(50),
            advertiser_name TEXT,
            partner_name TEXT,
            product_count INTEGER,
            saturation_level TEXT,
            expansion_opportunity TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Business Performance Tables
        CREATE TABLE IF NOT EXISTS analytics.spend_revenue_am_performance (
            id SERIAL PRIMARY KEY,
            am_name TEXT,
            total_advertisers INTEGER,
            total_budget NUMERIC(12,2),
            total_spent NUMERIC(12,2),
            avg_utilization NUMERIC(5,2),
            avg_cpl NUMERIC(8,2),
            performance_rating TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS analytics.spend_revenue_business_trends (
            id SERIAL PRIMARY KEY,
            business_category TEXT,
            trend_date DATE,
            total_budget NUMERIC(12,2),
            total_spent NUMERIC(12,2),
            avg_cpl NUMERIC(8,2),
            lead_count INTEGER,
            campaign_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS analytics.spend_revenue_time_series (
            id SERIAL PRIMARY KEY,
            date_period DATE,
            total_budget NUMERIC(12,2),
            total_spent NUMERIC(12,2),
            total_leads INTEGER,
            avg_cpl NUMERIC(8,2),
            campaign_count INTEGER,
            active_advertisers INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Salesforce Integration Tables
        CREATE TABLE IF NOT EXISTS analytics.salesforce_to_corporate_mapping (
            id SERIAL PRIMARY KEY,
            sf_account_id TEXT,
            corporate_maid VARCHAR(50),
            advertiser_name TEXT,
            mapping_confidence TEXT,
            last_verified DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS analytics.sf_partner_calls_historical (
            id SERIAL PRIMARY KEY,
            call_id TEXT,
            partner_name TEXT,
            call_date DATE,
            call_type TEXT,
            duration_minutes INTEGER,
            outcome TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        # Execute table creation
        for sql_statement in tables_sql.split(';'):
            if sql_statement.strip():
                conn.execute(text(sql_statement))

        conn.commit()
        print("Analytics schema and tables created successfully")

def migrate_table_data(duckdb_conn, pg_engine, source_table, target_table, batch_size=1000):
    """Migrate data from DuckDB table to PostgreSQL table"""
    try:
        print(f"Migrating {source_table} -> {target_table}...")

        # Get data from DuckDB
        df = duckdb_conn.execute(f'SELECT * FROM "{source_table}"').df()

        if df.empty:
            print(f"  No data found in {source_table}")
            return 0

        # Clean column names
        df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]

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

        # Insert data in batches
        total_rows = len(df)
        rows_inserted = 0

        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            batch_df.to_sql(
                target_table.split('.')[-1],
                pg_engine,
                schema=target_table.split('.')[0] if '.' in target_table else 'public',
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
    print("COMPREHENSIVE HEARTBEAT DATA MIGRATION")
    print("=" * 60)

    # Connect to databases
    print("Connecting to databases...")

    try:
        # Connect to DuckDB
        duckdb_conn = duckdb.connect(HEARTBEAT_DB)
        print("Connected to heartbeat DuckDB")

        # Connect to PostgreSQL
        pg_engine = create_engine(DATABASE_URL)
        print("Connected to PostgreSQL")

        # Create analytics schema
        create_analytics_schema(pg_engine)

        # Get list of tables to migrate (only working tables)
        working_tables = [
            ('advertiser_product_saturation', 'analytics.advertiser_product_saturation'),
            ('master_advertisers', 'analytics.master_advertisers'),
            ('master_campaigns', 'analytics.master_campaigns'),
            ('master_partners', 'analytics.master_partners'),
            ('scorecard_metrics', 'analytics.scorecard_metrics'),
            ('scorecard_summary', 'analytics.scorecard_summary'),
            ('spend_revenue_am_performance', 'analytics.spend_revenue_am_performance'),
            ('spend_revenue_business_trends', 'analytics.spend_revenue_business_trends'),
            ('spend_revenue_performance_current', 'analytics.spend_revenue_performance_current'),
            ('spend_revenue_performance_historical', 'analytics.spend_revenue_performance_historical'),
            ('spend_revenue_time_series', 'analytics.spend_revenue_time_series'),
            ('salesforce_to_corporate_mapping', 'analytics.salesforce_to_corporate_mapping'),
            ('sf_partner_calls_historical_staging', 'analytics.sf_partner_calls_historical'),
        ]

        # Migrate each table
        total_migrated = 0
        migration_summary = []

        for source_table, target_table in working_tables:
            try:
                rows = migrate_table_data(duckdb_conn, pg_engine, source_table, target_table)
                migration_summary.append((source_table, target_table, rows))
                total_migrated += rows
            except Exception as e:
                print(f"FAILED to migrate {source_table}: {str(e)}")
                migration_summary.append((source_table, target_table, 0))

        # Print summary
        print("\n" + "=" * 60)
        print("MIGRATION SUMMARY")
        print("=" * 60)

        for source, target, rows in migration_summary:
            status = "SUCCESS" if rows > 0 else "FAILED"
            print(f"{status:<8} {source:<40} -> {rows:>8,} rows")

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
    sys.exit(exit_code)