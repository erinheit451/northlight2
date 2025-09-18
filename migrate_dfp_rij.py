#!/usr/bin/env python3

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import datetime
import sys

def connect_to_db():
    """Connect to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="unified_northlight",
            user="northlight_user",
            password="northlight_secure_2024"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def create_dfp_rij_table(conn):
    """Create the DFP RIJ table if it doesn't exist"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS heartbeat_etl.dfp_rij_alerts (
        id SERIAL PRIMARY KEY,
        office TEXT,
        service_assignment TEXT,
        agent TEXT,
        business_id TEXT,
        business_name TEXT,
        advertiser_name TEXT,
        campaign_id TEXT,
        product TEXT,
        campaign_name TEXT,
        campaign_budget NUMERIC(12,2),
        percent_spent NUMERIC(10,6),
        alert_type TEXT,
        expected_end_date DATE,
        cycle TEXT,
        days_without_revenue INTEGER,
        evergreen INTEGER,
        churn_or_down TEXT,
        details TEXT,
        notes_about_campaign TEXT,
        new_since_yesterday TEXT,
        auto_renew_type TEXT,
        high_alert TEXT,
        extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_file TEXT
    );

    -- Create index on campaign_id for joining
    CREATE INDEX IF NOT EXISTS idx_dfp_rij_campaign_id ON heartbeat_etl.dfp_rij_alerts(campaign_id);
    CREATE INDEX IF NOT EXISTS idx_dfp_rij_business_id ON heartbeat_etl.dfp_rij_alerts(business_id);
    CREATE INDEX IF NOT EXISTS idx_dfp_rij_office ON heartbeat_etl.dfp_rij_alerts(office);
    """

    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        print("DFP RIJ table created successfully")
        cursor.close()
        return True
    except Exception as e:
        print(f"Error creating table: {e}")
        return False

def clean_numeric_value(value):
    """Clean and convert string values to numeric"""
    if pd.isna(value) or value == '' or value == '""':
        return None
    try:
        # Remove quotes and convert
        cleaned = str(value).strip('"').strip()
        if cleaned == '':
            return None
        return float(cleaned)
    except:
        return None

def clean_integer_value(value):
    """Clean and convert string values to integer"""
    if pd.isna(value) or value == '' or value == '""':
        return None
    try:
        # Remove quotes and convert
        cleaned = str(value).strip('"').strip()
        if cleaned == '':
            return None
        return int(cleaned)
    except:
        return None

def clean_date_value(value):
    """Clean and convert string values to date"""
    if pd.isna(value) or value == '' or value == '""':
        return None
    try:
        # Remove quotes and convert
        cleaned = str(value).strip('"').strip()
        if cleaned == '':
            return None
        return pd.to_datetime(cleaned).date()
    except:
        return None

def read_dfp_rij_file(file_path):
    """Read DFP RIJ file with proper encoding handling"""
    try:
        # Read with UTF-16 encoding and tab separator
        df = pd.read_csv(file_path, encoding='utf-16', sep='\t', quoting=3)

        # Clean column names (remove extra spaces and quotes)
        df.columns = [col.strip().strip('"') for col in df.columns]

        print(f"Read {len(df)} rows from {file_path}")
        print(f"Columns: {list(df.columns)}")

        return df
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None

def migrate_dfp_rij_data(conn, df, source_file):
    """Migrate DFP RIJ data to database"""
    try:
        cursor = conn.cursor()

        # Clear existing data for this date (in case of re-runs)
        cursor.execute("DELETE FROM heartbeat_etl.dfp_rij_alerts WHERE source_file = %s", (source_file,))

        # Prepare data for insertion
        insert_data = []

        for _, row in df.iterrows():
            data_row = (
                row.get('Office', '').strip('"').strip() if pd.notna(row.get('Office')) else None,
                row.get('Service Assignment', '').strip('"').strip() if pd.notna(row.get('Service Assignment')) else None,
                row.get('Agent', '').strip('"').strip() if pd.notna(row.get('Agent')) else None,
                row.get('Business ID', '').strip('"').strip() if pd.notna(row.get('Business ID')) else None,
                row.get('Business Name', '').strip('"').strip() if pd.notna(row.get('Business Name')) else None,
                row.get('Advertiser Name', '').strip('"').strip() if pd.notna(row.get('Advertiser Name')) else None,
                row.get('Campaign ID', '').strip('"').strip() if pd.notna(row.get('Campaign ID')) else None,
                row.get('Product', '').strip('"').strip() if pd.notna(row.get('Product')) else None,
                row.get('Campaign Name', '').strip('"').strip() if pd.notna(row.get('Campaign Name')) else None,
                clean_numeric_value(row.get('Campaign Budget')),
                clean_numeric_value(row.get('Percent Spent')),
                row.get('Alert Type', '').strip('"').strip() if pd.notna(row.get('Alert Type')) else None,
                clean_date_value(row.get('Expected End Date')),
                row.get('Cycle', '').strip('"').strip() if pd.notna(row.get('Cycle')) else None,
                clean_integer_value(row.get('Days Without Revenue')),
                clean_integer_value(row.get('Evergreen')),
                row.get('Churn Or Down', '').strip('"').strip() if pd.notna(row.get('Churn Or Down')) else None,
                row.get('Details', '').strip('"').strip() if pd.notna(row.get('Details')) else None,
                row.get('Notes About Campaign', '').strip('"').strip() if pd.notna(row.get('Notes About Campaign')) else None,
                row.get('New Since Yesterday', '').strip('"').strip() if pd.notna(row.get('New Since Yesterday')) else None,
                row.get('Auto Renew Type', '').strip('"').strip() if pd.notna(row.get('Auto Renew Type')) else None,
                row.get('High Alert', '').strip('"').strip() if pd.notna(row.get('High Alert')) else None,
                datetime.now(),
                source_file
            )
            insert_data.append(data_row)

        # Insert data
        insert_sql = """
        INSERT INTO heartbeat_etl.dfp_rij_alerts (
            office, service_assignment, agent, business_id, business_name, advertiser_name,
            campaign_id, product, campaign_name, campaign_budget, percent_spent, alert_type,
            expected_end_date, cycle, days_without_revenue, evergreen, churn_or_down, details,
            notes_about_campaign, new_since_yesterday, auto_renew_type, high_alert,
            extracted_at, source_file
        ) VALUES %s
        """

        execute_values(cursor, insert_sql, insert_data)
        conn.commit()

        print(f"Successfully migrated {len(insert_data)} DFP RIJ records")
        cursor.close()
        return True

    except Exception as e:
        print(f"Error migrating data: {e}")
        conn.rollback()
        return False

def main():
    # File path for most recent DFP RIJ data
    dfp_rij_file = r"C:\Users\Roci\Heartbeat\data\raw\dfp_rij\dfp_rij_2025-09-17.csv"

    if not os.path.exists(dfp_rij_file):
        print(f"File not found: {dfp_rij_file}")
        return

    print(f"Starting DFP RIJ migration for {dfp_rij_file}")

    # Connect to database
    conn = connect_to_db()
    if not conn:
        return

    try:
        # Create table
        if not create_dfp_rij_table(conn):
            return

        # Read CSV file
        df = read_dfp_rij_file(dfp_rij_file)
        if df is None:
            return

        # Migrate data
        if migrate_dfp_rij_data(conn, df, os.path.basename(dfp_rij_file)):
            print("DFP RIJ migration completed successfully!")
        else:
            print("DFP RIJ migration failed")

    finally:
        conn.close()

if __name__ == "__main__":
    main()