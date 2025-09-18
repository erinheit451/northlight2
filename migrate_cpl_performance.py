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

def create_cpl_performance_table(conn):
    """Create the CPL performance table if it doesn't exist"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS heartbeat_etl.agreed_cpl_performance (
        id SERIAL PRIMARY KEY,
        advertiser_id TEXT,
        advertiser_name TEXT,
        first_aid TEXT,
        mcid TEXT,
        campaign_id TEXT,
        campaign_name TEXT,
        insertion_order_id TEXT,
        business_id TEXT,
        business_name TEXT,
        product TEXT,
        offer_id TEXT,
        campaign_budget NUMERIC(12,2),
        current_cycle_status TEXT,
        cycle_start_date DATE,
        cycle_end_date DATE,
        auto_renew_type TEXT,
        prior_cycle_status TEXT,
        prior_cycle_paid TEXT,
        prior_cycle_started TEXT,
        prior_cycle_ended TEXT,
        days_elapsed INTEGER,
        cpl_change_rank TEXT,
        cpl_change TEXT,
        trending_4 TEXT,
        utilization NUMERIC(10,4),
        utilization_status_7 TEXT,
        campaign_spend_rate NUMERIC(10,4),
        cpl_mcid NUMERIC(10,2),
        cpl_bsc_median NUMERIC(10,2),
        cpl_agreed NUMERIC(10,2),
        cpl_vs_median TEXT,
        cpl_vs_agreed TEXT,
        cid_spend_to_date NUMERIC(12,2),
        pct_spent NUMERIC(10,6),
        mcid_position_prior_quarter TEXT,
        google_spend_pct_current_month NUMERIC(5,2),
        yahoo_bing_spend_pct_current_month NUMERIC(5,2),
        yelp_spend_pct_current_month NUMERIC(5,2),
        other_spend_pct_current_month NUMERIC(5,2),
        google_spend_pct_previous_month NUMERIC(5,2),
        yahoo_bing_spend_pct_previous_month NUMERIC(5,2),
        yelp_spend_pct_previous_month NUMERIC(5,2),
        other_spend_pct_previous_month NUMERIC(5,2),
        bsc TEXT,
        bc TEXT,
        vertical TEXT,
        account_owner_payroll_id TEXT,
        account_owner_name TEXT,
        csm TEXT,
        optimizer_1 TEXT,
        optimizer_2 TEXT,
        seo_analyst TEXT,
        cs_analyst TEXT,
        media_analyst TEXT,
        office TEXT,
        area TEXT,
        channel TEXT,
        tier TEXT,
        add_comments TEXT,
        campaign_performance_rating_2 TEXT,
        tickets TEXT,
        mcid_cycles TEXT,
        io_length TEXT,
        io_cycle TEXT,
        method_of_payment TEXT,
        predictive_churn_score TEXT,
        review_completed TEXT,
        child_status TEXT,
        child_detail TEXT,
        sfu_account_link TEXT,
        cpl_change_alert TEXT,
        extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_file TEXT
    );

    -- Create indexes for key linking fields
    CREATE INDEX IF NOT EXISTS idx_cpl_performance_campaign_id ON heartbeat_etl.agreed_cpl_performance(campaign_id);
    CREATE INDEX IF NOT EXISTS idx_cpl_performance_business_id ON heartbeat_etl.agreed_cpl_performance(business_id);
    CREATE INDEX IF NOT EXISTS idx_cpl_performance_business_name ON heartbeat_etl.agreed_cpl_performance(business_name);
    CREATE INDEX IF NOT EXISTS idx_cpl_performance_advertiser_name ON heartbeat_etl.agreed_cpl_performance(advertiser_name);
    CREATE INDEX IF NOT EXISTS idx_cpl_performance_office ON heartbeat_etl.agreed_cpl_performance(office);
    CREATE INDEX IF NOT EXISTS idx_cpl_performance_account_owner ON heartbeat_etl.agreed_cpl_performance(account_owner_name);
    """

    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        print("CPL performance table created successfully")
        cursor.close()
        return True
    except Exception as e:
        print(f"Error creating table: {e}")
        return False

def clean_numeric_value(value):
    """Clean and convert string values to numeric"""
    if pd.isna(value) or value == '' or value in ['', 'None', None, 'N/A', 'Not Entered']:
        return None
    try:
        if isinstance(value, str):
            cleaned = value.strip().replace('$', '').replace(',', '').replace('"', '').strip()
            if cleaned == '' or cleaned in ['N/A', 'Not Entered']:
                return None
        else:
            cleaned = value
        return float(cleaned)
    except:
        return None

def clean_integer_value(value):
    """Clean and convert string values to integer"""
    if pd.isna(value) or value == '' or value in ['', 'None', None, 'N/A', 'Not Entered']:
        return None
    try:
        if isinstance(value, str):
            cleaned = value.strip().replace(',', '').replace('"', '').strip()
            if cleaned == '' or cleaned in ['N/A', 'Not Entered']:
                return None
        else:
            cleaned = value
        return int(float(cleaned))
    except:
        return None

def clean_date_value(value):
    """Clean and convert string values to date"""
    if pd.isna(value) or value == '' or value in ['', 'None', None, 'N/A', 'Not Entered']:
        return None
    try:
        if isinstance(value, str):
            cleaned = value.strip().replace('"', '').strip()
            if cleaned == '' or cleaned in ['N/A', 'Not Entered']:
                return None
        else:
            cleaned = value
        return pd.to_datetime(cleaned).date()
    except:
        return None

def clean_text_value(value):
    """Clean text values"""
    if pd.isna(value) or value in ['', 'None', None, 'N/A', 'Not Entered']:
        return None
    cleaned = str(value).strip().replace('"', '').strip()
    return cleaned if cleaned != '' and cleaned not in ['N/A', 'Not Entered'] else None

def read_cpl_performance_file(file_path):
    """Read CPL performance file with proper encoding handling"""
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

def migrate_cpl_performance_data(conn, df, source_file):
    """Migrate CPL performance data to database"""
    try:
        cursor = conn.cursor()

        # Clear existing data for this file
        cursor.execute("DELETE FROM heartbeat_etl.agreed_cpl_performance WHERE source_file = %s", (source_file,))

        # Prepare data for insertion
        insert_data = []

        for _, row in df.iterrows():
            data_row = (
                clean_text_value(row.get('Advertiser ID')),
                clean_text_value(row.get('Advertiser Name')),
                clean_text_value(row.get('First AID')),
                clean_text_value(row.get('MCID')),
                clean_text_value(row.get('Campaign ID')),
                clean_text_value(row.get('Campaign Name')),
                clean_text_value(row.get('InsertionOrder ID')),
                clean_text_value(row.get('Business ID')),
                clean_text_value(row.get('Business Name')),
                clean_text_value(row.get('Product')),
                clean_text_value(row.get('Offer ID')),
                clean_numeric_value(row.get('Campaign Budget')),
                clean_text_value(row.get('Current Cycle Status')),
                clean_date_value(row.get('Cycle Start Date')),
                clean_date_value(row.get('Cycle End Date')),
                clean_text_value(row.get('Auto Renew Type')),
                clean_text_value(row.get('Prior Cycle Status')),
                clean_text_value(row.get('Prior Cycle Paid')),
                clean_text_value(row.get('Prior Cycle Started')),
                clean_text_value(row.get('Prior Cycle Ended')),
                clean_integer_value(row.get('Days Elapsed')),
                clean_text_value(row.get('CPL Change Rank')),
                clean_text_value(row.get('CPL Change')),
                clean_text_value(row.get('Trending (4)')),
                clean_numeric_value(row.get('Utilization')),
                clean_text_value(row.get('Utilization Status (7)')),
                clean_numeric_value(row.get('Campaign Spend Rate')),
                clean_numeric_value(row.get('CPL MCID')),
                clean_numeric_value(row.get('CPL BSC Median')),
                clean_numeric_value(row.get('CPL Agreed')),
                clean_text_value(row.get('CPL Vs Median')),
                clean_text_value(row.get('CPL Vs Agreed')),
                clean_numeric_value(row.get('CID Spend To Date')),
                clean_numeric_value(row.get('Pct Spent')),
                clean_text_value(row.get('MCID Position Prior Quarter')),
                clean_numeric_value(row.get('Google Spend % (Current Month)')),
                clean_numeric_value(row.get('Yahoo Bing Spend % (Current Month)')),
                clean_numeric_value(row.get('Yelp Spend % (Current Month)')),
                clean_numeric_value(row.get('Other Spend % (Current Month)')),
                clean_numeric_value(row.get('Google Spend % (Previous Month)')),
                clean_numeric_value(row.get('Yahoo Bing Spend % (Previous Month)')),
                clean_numeric_value(row.get('Yelp Spend % (Previous Month)')),
                clean_numeric_value(row.get('Other Spend % (Previous Month)')),
                clean_text_value(row.get('BSC')),
                clean_text_value(row.get('BC')),
                clean_text_value(row.get('Vertical')),
                clean_text_value(row.get('Account Owner Payroll ID')),
                clean_text_value(row.get('Account Owner Name')),
                clean_text_value(row.get('CSM')),
                clean_text_value(row.get('Optimizer 1')),
                clean_text_value(row.get('Optimizer 2')),
                clean_text_value(row.get('SEO Analyst')),
                clean_text_value(row.get('CS Analyst')),
                clean_text_value(row.get('Media Analyst')),
                clean_text_value(row.get('Office')),
                clean_text_value(row.get('Area')),
                clean_text_value(row.get('Channel')),
                clean_text_value(row.get('Tier')),
                clean_text_value(row.get('Add Comments')),
                clean_text_value(row.get('Campaign Performance Rating (2)')),
                clean_text_value(row.get('Tickets')),
                clean_text_value(row.get('MCID Cycles')),
                clean_text_value(row.get('IO Length')),
                clean_text_value(row.get('IO Cycle')),
                clean_text_value(row.get('Method Of Payment')),
                clean_text_value(row.get('Predictive Churn Score')),
                clean_text_value(row.get('Review Completed')),
                clean_text_value(row.get('Child Status')),
                clean_text_value(row.get('Child Detail')),
                clean_text_value(row.get('SFU Account Link')),
                clean_text_value(row.get('Cpl Change Alert')),
                datetime.now(),
                source_file
            )
            insert_data.append(data_row)

        # Insert data
        insert_sql = """
        INSERT INTO heartbeat_etl.agreed_cpl_performance (
            advertiser_id, advertiser_name, first_aid, mcid, campaign_id, campaign_name,
            insertion_order_id, business_id, business_name, product, offer_id, campaign_budget,
            current_cycle_status, cycle_start_date, cycle_end_date, auto_renew_type,
            prior_cycle_status, prior_cycle_paid, prior_cycle_started, prior_cycle_ended,
            days_elapsed, cpl_change_rank, cpl_change, trending_4, utilization,
            utilization_status_7, campaign_spend_rate, cpl_mcid, cpl_bsc_median, cpl_agreed,
            cpl_vs_median, cpl_vs_agreed, cid_spend_to_date, pct_spent, mcid_position_prior_quarter,
            google_spend_pct_current_month, yahoo_bing_spend_pct_current_month,
            yelp_spend_pct_current_month, other_spend_pct_current_month,
            google_spend_pct_previous_month, yahoo_bing_spend_pct_previous_month,
            yelp_spend_pct_previous_month, other_spend_pct_previous_month,
            bsc, bc, vertical, account_owner_payroll_id, account_owner_name, csm,
            optimizer_1, optimizer_2, seo_analyst, cs_analyst, media_analyst,
            office, area, channel, tier, add_comments, campaign_performance_rating_2,
            tickets, mcid_cycles, io_length, io_cycle, method_of_payment,
            predictive_churn_score, review_completed, child_status, child_detail,
            sfu_account_link, cpl_change_alert, extracted_at, source_file
        ) VALUES %s
        """

        execute_values(cursor, insert_sql, insert_data)
        conn.commit()

        print(f"Successfully migrated {len(insert_data)} CPL performance records")
        cursor.close()
        return True

    except Exception as e:
        print(f"Error migrating data: {e}")
        conn.rollback()
        return False

def main():
    # File path for most recent CPL performance data
    cpl_file = r"C:\Users\Roci\Heartbeat\data\raw\agreed_cpl_performance\agreed_cpl_performance_2025-09-17.csv"

    if not os.path.exists(cpl_file):
        print(f"File not found: {cpl_file}")
        return

    print(f"Starting CPL performance migration for {cpl_file}")

    # Connect to database
    conn = connect_to_db()
    if not conn:
        return

    try:
        # Create table
        if not create_cpl_performance_table(conn):
            return

        # Read CSV file
        df = read_cpl_performance_file(cpl_file)
        if df is None:
            return

        # Migrate data
        if migrate_cpl_performance_data(conn, df, os.path.basename(cpl_file)):
            print("CPL performance migration completed successfully!")
        else:
            print("CPL performance migration failed")

    finally:
        conn.close()

if __name__ == "__main__":
    main()