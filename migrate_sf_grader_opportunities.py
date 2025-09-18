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

def clean_numeric_value(value):
    """Clean and convert string values to numeric"""
    if pd.isna(value) or value == '' or value in ['', 'None', None, '-']:
        return None
    try:
        if isinstance(value, str):
            # Remove currency symbols, commas, and quotes
            cleaned = value.strip().replace('USD', '').replace('$', '').replace(',', '').replace('"', '').strip()
            if cleaned == '' or cleaned == '-':
                return None
        else:
            cleaned = value
        return float(cleaned)
    except:
        return None

def clean_integer_value(value):
    """Clean and convert string values to integer"""
    if pd.isna(value) or value == '' or value in ['', 'None', None, '-']:
        return None
    try:
        if isinstance(value, str):
            cleaned = value.strip().replace(',', '').replace('"', '').strip()
            if cleaned == '' or cleaned == '-':
                return None
        else:
            cleaned = value
        return int(float(cleaned))
    except:
        return None

def clean_date_value(value):
    """Clean and convert string values to date"""
    if pd.isna(value) or value == '' or value in ['', 'None', None, '-']:
        return None
    try:
        if isinstance(value, str):
            cleaned = value.strip().replace('"', '').strip()
            if cleaned == '' or cleaned == '-':
                return None
        else:
            cleaned = value
        return pd.to_datetime(cleaned)
    except:
        return None

def clean_text_value(value):
    """Clean text values"""
    if pd.isna(value) or value in ['', 'None', None, '-']:
        return None
    cleaned = str(value).strip().replace('"', '').strip()
    return cleaned if cleaned != '' and cleaned != '-' else None

def migrate_grader_opportunities(conn, file_path):
    """Migrate SF grader opportunities data"""
    try:
        print(f"Reading grader opportunities file: {file_path}")
        df = pd.read_csv(file_path)

        print(f"Read {len(df)} rows from grader opportunities")
        print(f"Columns: {list(df.columns)}")

        cursor = conn.cursor()

        # Clear existing data in the table
        cursor.execute("DELETE FROM heartbeat_etl.sf_grader_opportunities_2025_09_13")

        # Prepare data for insertion
        insert_data = []

        for _, row in df.iterrows():
            data_row = (
                clean_text_value(row.get('ACCOUNT_NAME')),
                clean_text_value(row.get('ACCOUNT_OWNER')),
                clean_integer_value(row.get('AGE')),
                clean_text_value(row.get('AMOUNT')),  # Keep as text since it includes currency
                clean_text_value(row.get('Account.Business__c')),
                clean_date_value(row.get('CLOSE_DATE')),
                clean_date_value(row.get('CREATED_DATE')),
                clean_text_value(row.get('FISCAL_QUARTER')),
                clean_text_value(row.get('FULL_NAME')),
                clean_text_value(row.get('OPPORTUNITY_NAME')),
                clean_text_value(row.get('Opportunity.Net_New_Amount__c')),  # Keep as text
                clean_integer_value(row.get('Opportunity.Opportunity_Count__c')),
                clean_text_value(row.get('PROBABILITY')),  # Keep as text since it includes %
                clean_text_value(row.get('ROLLUP_DESCRIPTION')),
                clean_text_value(row.get('STAGE_NAME')),
                datetime.now().isoformat()
            )
            insert_data.append(data_row)

        # Insert data using the existing table structure
        insert_sql = """
        INSERT INTO heartbeat_etl.sf_grader_opportunities_2025_09_13 (
            account_name, owner, age, amount, account_business_c,
            close_date, created_date, fiscal_quarter, full_name, opportunity_name,
            opportunity_net_new_amount_c, opportunity_opportunity_count_c, probability,
            rollup_description, stage, etl_processed_at
        ) VALUES %s
        """

        execute_values(cursor, insert_sql, insert_data)
        conn.commit()

        print(f"Successfully migrated {len(insert_data)} grader opportunities records")
        cursor.close()
        return True

    except Exception as e:
        print(f"Error migrating grader opportunities data: {e}")
        conn.rollback()
        return False

def main():
    # File path
    grader_file = r"C:\Users\Roci\Heartbeat\data\raw\sf_grader_opportunities\sf_grader_opportunities_2025-09-13.csv"

    if not os.path.exists(grader_file):
        print(f"File not found: {grader_file}")
        return

    print("Starting SF grader opportunities migration")

    # Connect to database
    conn = connect_to_db()
    if not conn:
        return

    try:
        # Migrate grader opportunities data
        if migrate_grader_opportunities(conn, grader_file):
            print("SF grader opportunities migration completed successfully!")
        else:
            print("SF grader opportunities migration failed")

    finally:
        conn.close()

if __name__ == "__main__":
    main()