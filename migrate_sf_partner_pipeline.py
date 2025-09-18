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

def create_sf_partner_pipeline_tables(conn):
    """Create the SF partner pipeline tables if they don't exist"""
    create_current_table_sql = """
    CREATE TABLE IF NOT EXISTS heartbeat_etl.sf_partner_pipeline_current (
        id SERIAL PRIMARY KEY,
        account_owner TEXT,
        bid TEXT,
        account_name TEXT,
        opportunity_name TEXT,
        type_1 TEXT,
        type_2 TEXT,
        created_date DATE,
        last_stage_change_date DATE,
        all_tcv NUMERIC(15,2),
        mo_offer_amt NUMERIC(12,2),
        net_new_tcv NUMERIC(15,2),
        modification_amount NUMERIC(15,2),
        close_date DATE,
        solution TEXT,
        stage_duration NUMERIC(10,2),
        cycles INTEGER,
        processed_date DATE,
        extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_file TEXT
    );

    CREATE TABLE IF NOT EXISTS heartbeat_etl.sf_partner_pipeline_historical (
        id SERIAL PRIMARY KEY,
        account_owner TEXT,
        bid TEXT,
        account_name TEXT,
        opportunity_name TEXT,
        type_1 TEXT,
        type_2 TEXT,
        created_date DATE,
        last_stage_change_date DATE,
        all_tcv_currency TEXT,
        all_tcv NUMERIC(15,2),
        mo_offer_amt_currency TEXT,
        mo_offer_amt NUMERIC(12,2),
        net_new_tcv_currency TEXT,
        net_new_tcv NUMERIC(15,2),
        modification_amount_currency TEXT,
        modification_amount NUMERIC(15,2),
        close_date DATE,
        solution TEXT,
        stage_duration NUMERIC(10,2),
        cycles INTEGER,
        processed_date DATE,
        opportunity_owner TEXT,
        stage TEXT,
        extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_file TEXT
    );

    -- Create indexes for joining
    CREATE INDEX IF NOT EXISTS idx_sf_pipeline_current_bid ON heartbeat_etl.sf_partner_pipeline_current(bid);
    CREATE INDEX IF NOT EXISTS idx_sf_pipeline_current_account_owner ON heartbeat_etl.sf_partner_pipeline_current(account_owner);
    CREATE INDEX IF NOT EXISTS idx_sf_pipeline_current_close_date ON heartbeat_etl.sf_partner_pipeline_current(close_date);

    CREATE INDEX IF NOT EXISTS idx_sf_pipeline_hist_bid ON heartbeat_etl.sf_partner_pipeline_historical(bid);
    CREATE INDEX IF NOT EXISTS idx_sf_pipeline_hist_account_owner ON heartbeat_etl.sf_partner_pipeline_historical(account_owner);
    CREATE INDEX IF NOT EXISTS idx_sf_pipeline_hist_close_date ON heartbeat_etl.sf_partner_pipeline_historical(close_date);
    CREATE INDEX IF NOT EXISTS idx_sf_pipeline_hist_stage ON heartbeat_etl.sf_partner_pipeline_historical(stage);
    """

    try:
        cursor = conn.cursor()
        cursor.execute(create_current_table_sql)
        conn.commit()
        print("SF partner pipeline tables created successfully")
        cursor.close()
        return True
    except Exception as e:
        print(f"Error creating tables: {e}")
        return False

def clean_numeric_value(value):
    """Clean and convert string values to numeric"""
    if pd.isna(value) or value == '' or value in ['', 'None', None]:
        return None
    try:
        if isinstance(value, str):
            cleaned = value.strip().replace('$', '').replace(',', '')
            if cleaned == '':
                return None
        else:
            cleaned = value
        return float(cleaned)
    except:
        return None

def clean_integer_value(value):
    """Clean and convert string values to integer"""
    if pd.isna(value) or value == '' or value in ['', 'None', None]:
        return None
    try:
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned == '':
                return None
        else:
            cleaned = value
        return int(float(cleaned))
    except:
        return None

def clean_date_value(value):
    """Clean and convert string values to date"""
    if pd.isna(value) or value == '' or value in ['', 'None', None]:
        return None
    try:
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned == '':
                return None
        else:
            cleaned = value
        return pd.to_datetime(cleaned).date()
    except:
        return None

def clean_text_value(value):
    """Clean text values"""
    if pd.isna(value) or value in ['', 'None', None]:
        return None
    return str(value).strip() if str(value).strip() != '' else None

def migrate_current_pipeline(conn, file_path):
    """Migrate current SF partner pipeline data"""
    try:
        print(f"Reading current pipeline file: {file_path}")
        df = pd.read_csv(file_path)

        print(f"Read {len(df)} rows from current pipeline")
        print(f"Columns: {list(df.columns)}")

        cursor = conn.cursor()

        # Clear existing data for this file
        source_file = os.path.basename(file_path)
        cursor.execute("DELETE FROM heartbeat_etl.sf_partner_pipeline_current WHERE source_file = %s", (source_file,))

        # Prepare data for insertion
        insert_data = []

        for _, row in df.iterrows():
            data_row = (
                clean_text_value(row.get('Account Owner')),
                clean_text_value(row.get('BID')),
                clean_text_value(row.get('Account Name')),
                clean_text_value(row.get('Opportunity Name')),
                clean_text_value(row.get('Type')),
                clean_text_value(row.get('Type.1', row.get('Type'))),  # Handle both column naming variations
                clean_date_value(row.get('Created Date')),
                clean_date_value(row.get('Last Stage Change Date')),
                clean_numeric_value(row.get('All TCV')),
                clean_numeric_value(row.get('Mo. Offer Amt')),
                clean_numeric_value(row.get('Net New TCV')),
                clean_numeric_value(row.get('Modification Amount')),
                clean_date_value(row.get('Close Date')),
                clean_text_value(row.get('Solution')),
                clean_numeric_value(row.get('Stage Duration')),
                clean_integer_value(row.get('Cycles')),
                clean_date_value(row.get('Processed Date')),
                datetime.now(),
                source_file
            )
            insert_data.append(data_row)

        # Insert data
        insert_sql = """
        INSERT INTO heartbeat_etl.sf_partner_pipeline_current (
            account_owner, bid, account_name, opportunity_name, type_1, type_2,
            created_date, last_stage_change_date, all_tcv, mo_offer_amt, net_new_tcv,
            modification_amount, close_date, solution, stage_duration, cycles,
            processed_date, extracted_at, source_file
        ) VALUES %s
        """

        execute_values(cursor, insert_sql, insert_data)
        conn.commit()

        print(f"Successfully migrated {len(insert_data)} current pipeline records")
        cursor.close()
        return True

    except Exception as e:
        print(f"Error migrating current pipeline data: {e}")
        conn.rollback()
        return False

def migrate_historical_pipeline(conn, file_path):
    """Migrate historical SF partner pipeline data"""
    try:
        print(f"Reading historical pipeline file: {file_path}")
        df = pd.read_csv(file_path)

        print(f"Read {len(df)} rows from historical pipeline")
        print(f"Columns: {list(df.columns)}")

        cursor = conn.cursor()

        # Clear existing data for this file
        source_file = os.path.basename(file_path)
        cursor.execute("DELETE FROM heartbeat_etl.sf_partner_pipeline_historical WHERE source_file = %s", (source_file,))

        # Prepare data for insertion
        insert_data = []

        for _, row in df.iterrows():
            data_row = (
                clean_text_value(row.get('Account Owner')),
                clean_text_value(row.get('BID')),
                clean_text_value(row.get('Account Name')),
                clean_text_value(row.get('Opportunity Name')),
                clean_text_value(row.get('Type')),
                clean_text_value(row.get('Type.1')),
                clean_date_value(row.get('Created Date')),
                clean_date_value(row.get('Last Stage Change Date')),
                clean_text_value(row.get('All TCV Currency')),
                clean_numeric_value(row.get('All TCV')),
                clean_text_value(row.get('Mo. Offer Amt Currency')),
                clean_numeric_value(row.get('Mo. Offer Amt')),
                clean_text_value(row.get('Net New TCV Currency')),
                clean_numeric_value(row.get('Net New TCV')),
                clean_text_value(row.get('Modification Amount Currency')),
                clean_numeric_value(row.get('Modification Amount')),
                clean_date_value(row.get('Close Date')),
                clean_text_value(row.get('Solution')),
                clean_numeric_value(row.get('Stage Duration')),
                clean_integer_value(row.get('Cycles')),
                clean_date_value(row.get('Processed Date')),
                clean_text_value(row.get('Opportunity Owner')),
                clean_text_value(row.get('Stage')),
                datetime.now(),
                source_file
            )
            insert_data.append(data_row)

        # Insert data
        insert_sql = """
        INSERT INTO heartbeat_etl.sf_partner_pipeline_historical (
            account_owner, bid, account_name, opportunity_name, type_1, type_2,
            created_date, last_stage_change_date, all_tcv_currency, all_tcv,
            mo_offer_amt_currency, mo_offer_amt, net_new_tcv_currency, net_new_tcv,
            modification_amount_currency, modification_amount, close_date, solution,
            stage_duration, cycles, processed_date, opportunity_owner, stage,
            extracted_at, source_file
        ) VALUES %s
        """

        execute_values(cursor, insert_sql, insert_data)
        conn.commit()

        print(f"Successfully migrated {len(insert_data)} historical pipeline records")
        cursor.close()
        return True

    except Exception as e:
        print(f"Error migrating historical pipeline data: {e}")
        conn.rollback()
        return False

def main():
    # File paths
    current_file = r"C:\Users\Roci\Heartbeat\data\raw\sf_partner_pipeline\sf_partner_pipeline_2025-09-13.csv"
    historical_file = r"C:\Users\Roci\Heartbeat\data\raw\sf_partner_pipeline\report_backfill_converted.csv"

    print("Starting SF partner pipeline migration")

    # Connect to database
    conn = connect_to_db()
    if not conn:
        return

    try:
        # Create tables
        if not create_sf_partner_pipeline_tables(conn):
            return

        # Migrate current pipeline data
        if os.path.exists(current_file):
            if not migrate_current_pipeline(conn, current_file):
                print("Failed to migrate current pipeline data")
                return
        else:
            print(f"Current file not found: {current_file}")

        # Migrate historical pipeline data
        if os.path.exists(historical_file):
            if not migrate_historical_pipeline(conn, historical_file):
                print("Failed to migrate historical pipeline data")
                return
        else:
            print(f"Historical file not found: {historical_file}")

        print("SF partner pipeline migration completed successfully!")

    finally:
        conn.close()

if __name__ == "__main__":
    main()