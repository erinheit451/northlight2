#!/usr/bin/env python3
"""
Ultimate DMS Data Migration Script
Migrates Ultimate DMS CSV data to PostgreSQL book.campaigns table
"""

import asyncio
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import uuid
import json

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Database imports
from core.database import get_db_session, init_database
from core.models.book import Campaign, DataSnapshot

# Import the full Northlight risk model
from book_risk_model.core.churn import calculate_churn_probability
from book_risk_model.core.flare import attach_priority_and_flare
from book_risk_model.core.rules import preprocess_campaign_data, process_campaign_goals
from book_risk_model.presentation.diagnostics import (
    generate_headline_diagnosis,
    generate_diagnosis_pills
)
from book_risk_model.presentation.waterfall import build_churn_waterfall


class UltimateDMSMigrator:
    """Migrate Ultimate DMS CSV data to book.campaigns table"""

    def __init__(self):
        self.stats = {
            "campaigns_processed": 0,
            "campaigns_created": 0,
            "errors": []
        }

    def load_ultimate_dms_data(self) -> pd.DataFrame:
        """Load the latest Ultimate DMS CSV file"""
        data_dir = Path("C:/Users/Roci/Heartbeat/data/raw/ultimate_dms")

        # Find the latest file
        csv_files = list(data_dir.glob("ultimate_dms_*.csv"))
        if not csv_files:
            raise FileNotFoundError("No Ultimate DMS CSV files found")

        latest_file = max(csv_files, key=lambda x: x.stem.split('_')[-1])
        print(f"Loading data from: {latest_file}")

        # Read with UTF-16 encoding
        df = pd.read_csv(latest_file, sep='\t', encoding='utf-16')
        print(f"Loaded {len(df)} rows from Ultimate DMS")

        return df

    def transform_ultimate_dms_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Ultimate DMS data to book.campaigns format"""
        print("Transforming Ultimate DMS data...")

        # Create the mapping from Ultimate DMS columns to book.campaigns
        transformed_df = pd.DataFrame()

        # Direct mappings
        transformed_df['campaign_id'] = df['Campaign ID'].astype(str)
        transformed_df['maid'] = df['MAID'].astype(str)
        transformed_df['advertiser_name'] = df['Advertiser Name']
        transformed_df['partner_name'] = df['BID Name']  # Using BID Name as partner
        transformed_df['bid_name'] = df['BID Name']
        transformed_df['campaign_name'] = df['Campaign Name']
        transformed_df['am'] = df['AM']
        transformed_df['optimizer'] = df['Optimizer 1']
        transformed_df['gm'] = df.get('AM Manager', '')  # Use AM Manager as GM if available
        transformed_df['business_category'] = df['BC']

        # Financial data
        transformed_df['campaign_budget'] = pd.to_numeric(df['Campaign Budget'], errors='coerce')
        transformed_df['amount_spent'] = pd.to_numeric(df['Amount Spent'], errors='coerce')

        # Operational metrics
        transformed_df['io_cycle'] = pd.to_numeric(df['IO Cycle'], errors='coerce').fillna(1)
        transformed_df['avg_cycle_length'] = pd.to_numeric(df['Avg Cycle Length'], errors='coerce')
        transformed_df['days_elapsed'] = pd.to_numeric(df['Days Elapsed'], errors='coerce')
        transformed_df['days_active'] = transformed_df['days_elapsed']  # Use same as days_elapsed for Ultimate DMS
        transformed_df['utilization'] = pd.to_numeric(df['Utilization'], errors='coerce')

        # Fix utilization format and unrealistic values
        # Ultimate DMS stores utilization as percentage (99.71), but frontend expects decimal (0.9971)
        def fix_utilization(row):
            spent = row.get('amount_spent')
            budget = row.get('campaign_budget')
            days_elapsed = row.get('days_elapsed')
            avg_cycle = row.get('avg_cycle_length')
            original_util = row.get('utilization')

            # Skip if missing data
            if pd.isna(spent) or pd.isna(budget) or pd.isna(avg_cycle) or avg_cycle == 0 or budget == 0:
                # Convert percentage to decimal for frontend
                return original_util / 100 if original_util else None

            # Expected daily spend rate
            expected_daily = budget / avg_cycle

            # If utilization > 300%, infer actual days from spend level (before converting to decimal)
            if original_util and original_util > 300:
                # Infer days from spend (min 0.5 days, max cycle length)
                inferred_days = min(max(spent / expected_daily, 0.5), avg_cycle)
                corrected_util_pct = (spent / (expected_daily * inferred_days)) * 100
                # Convert to decimal for frontend
                return corrected_util_pct / 100
            else:
                # Convert percentage to decimal for frontend (99.71% -> 0.9971)
                return original_util / 100 if original_util else None

        # Apply the fix
        transformed_df['utilization'] = transformed_df.apply(fix_utilization, axis=1)

        # Lead metrics
        transformed_df['running_cid_leads'] = pd.to_numeric(df['Running CID Leads'], errors='coerce')
        transformed_df['running_cid_cpl'] = pd.to_numeric(df['Running CID CPL'], errors='coerce')
        transformed_df['cpl_goal'] = pd.to_numeric(df['CPL Goal'], errors='coerce')
        transformed_df['bsc_cpl_avg'] = pd.to_numeric(df['BSC CPL Avg'], errors='coerce')

        # Set effective CPL goal same as CPL goal for now
        transformed_df['effective_cpl_goal'] = transformed_df['cpl_goal']

        # Add missing required columns with proper defaults
        transformed_df['advertiser_product_count'] = 1  # Default to single product
        transformed_df['bsc_cpc_average'] = 3.0  # Default CPC value

        # Apply the full Northlight risk model
        print("Applying Northlight risk model...")

        # Step 1: Preprocess the data for the risk model
        risk_df = preprocess_campaign_data(transformed_df.copy())

        # Step 2: Process goals
        risk_df = process_campaign_goals(risk_df)

        # Step 3: Calculate churn probabilities using the full model
        risk_df = calculate_churn_probability(risk_df)

        # Step 4: Calculate FLARE scores and priority
        risk_df = attach_priority_and_flare(risk_df)

        # Step 5: Apply advanced diagnostics to generate headlines and pills
        risk_df = self.apply_advanced_diagnostics(risk_df)

        # Copy ALL calculated data back to the main dataframe (don't override)
        risk_columns = [
            'churn_prob_90d', 'flare_score', 'priority_index', 'priority_tier',
            'primary_issue', 'headline_diagnosis', 'risk_drivers_json', 'diagnosis_pills',
            'is_safe',  # Include is_safe from risk model
            'expected_leads_monthly', 'expected_leads_to_date'  # Include expected leads for pacing calculations
        ]

        for col in risk_columns:
            if col in risk_df.columns:
                transformed_df[col] = risk_df[col]
            else:
                # Fallback to basic values if calculation failed
                if col == 'churn_prob_90d':
                    transformed_df[col] = 0.15
                elif col == 'flare_score':
                    transformed_df[col] = 50.0
                elif col == 'priority_tier':
                    transformed_df[col] = 'P3 - MEDIUM'
                elif col == 'is_safe':
                    transformed_df[col] = False
                elif col in ['risk_drivers_json']:
                    transformed_df[col] = [{} for _ in range(len(transformed_df))]
                elif col in ['diagnosis_pills']:
                    transformed_df[col] = [[] for _ in range(len(transformed_df))]
                else:
                    transformed_df[col] = ''

        # Status
        transformed_df['status'] = 'active'
        # Don't override is_safe - use the one from risk model

        print(f"Transformed {len(transformed_df)} campaigns with full risk model")
        return transformed_df

    def apply_advanced_diagnostics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply advanced diagnostics using Northlight presentation layer"""
        print("Generating advanced diagnostics...")

        # Generate headline diagnosis for entire DataFrame
        try:
            headlines, severities = generate_headline_diagnosis(df)
            df['headline_diagnosis'] = headlines
            df['primary_issue'] = severities
        except Exception as e:
            print(f"Warning: Could not generate headlines: {e}")
            df['headline_diagnosis'] = 'Campaign under review'
            df['primary_issue'] = 'Monitoring'

        # Generate diagnosis pills for each row
        try:
            df['diagnosis_pills'] = df.apply(
                lambda row: generate_diagnosis_pills(row.to_dict()),
                axis=1
            )
        except Exception as e:
            print(f"Warning: Could not generate pills: {e}")
            df['diagnosis_pills'] = [[] for _ in range(len(df))]

        # Note: risk_drivers_json is already generated by the risk model
        # No need to regenerate waterfall data here

        return df

    def safe_generate_diagnosis(self, row_dict: dict) -> str:
        """Safely generate headline diagnosis"""
        try:
            return generate_headline_diagnosis(row_dict)
        except Exception as e:
            print(f"Warning: Could not generate diagnosis for campaign {row_dict.get('campaign_id', 'unknown')}: {e}")
            return "Campaign under review"

    def safe_generate_pills(self, row_dict: dict) -> list:
        """Safely generate diagnosis pills"""
        try:
            return generate_diagnosis_pills(row_dict)
        except Exception as e:
            print(f"Warning: Could not generate pills for campaign {row_dict.get('campaign_id', 'unknown')}: {e}")
            return []

    # Removed: safe_build_waterfall - risk_drivers_json is generated by risk model


    async def save_to_database(self, df: pd.DataFrame):
        """Save transformed campaigns to book.campaigns table"""
        print(f"Saving {len(df)} campaigns to database...")

        async with get_db_session() as session:
            # Clear existing data
            from sqlalchemy import text
            await session.execute(text("DELETE FROM book.campaigns"))

            for idx, row in df.iterrows():
                try:
                    # Create Campaign object
                    campaign = Campaign(
                        id=uuid.uuid4(),
                        campaign_id=str(row.get('campaign_id', '')),
                        maid=str(row.get('maid', '')),
                        advertiser_name=str(row.get('advertiser_name', '')),
                        partner_name=str(row.get('partner_name', '')),
                        bid_name=str(row.get('bid_name', '')),
                        campaign_name=str(row.get('campaign_name', '')),
                        am=str(row.get('am', '')),
                        optimizer=str(row.get('optimizer', '')),
                        gm=str(row.get('gm', '')),
                        business_category=str(row.get('business_category', '')),

                        # Financial data
                        campaign_budget=self.safe_decimal(row.get('campaign_budget')),
                        amount_spent=self.safe_decimal(row.get('amount_spent')),

                        # Operational metrics
                        io_cycle=self.safe_int(row.get('io_cycle')),
                        avg_cycle_length=self.safe_decimal(row.get('avg_cycle_length')),
                        days_elapsed=self.safe_int(row.get('days_elapsed')),
                        days_active=self.safe_int(row.get('days_active')),
                        utilization=self.safe_decimal(row.get('utilization')),

                        # Lead metrics
                        running_cid_leads=self.safe_int(row.get('running_cid_leads')),
                        running_cid_cpl=self.safe_decimal(row.get('running_cid_cpl')),
                        cpl_goal=self.safe_decimal(row.get('cpl_goal')),
                        bsc_cpl_avg=self.safe_decimal(row.get('bsc_cpl_avg')),
                        effective_cpl_goal=self.safe_decimal(row.get('effective_cpl_goal')),

                        # Expected leads for pacing calculations
                        expected_leads_monthly=self.safe_decimal(row.get('expected_leads_monthly')),
                        expected_leads_to_date=self.safe_decimal(row.get('expected_leads_to_date')),

                        # Risk scores
                        churn_prob_90d=self.safe_decimal(row.get('churn_prob_90d')),
                        flare_score=self.safe_decimal(row.get('flare_score')),
                        priority_index=self.safe_decimal(row.get('priority_index')),
                        priority_tier=str(row.get('priority_tier', 'P3 - MEDIUM')),

                        # Diagnostics
                        primary_issue=str(row.get('primary_issue', '')),
                        headline_diagnosis=str(row.get('headline_diagnosis', '')),
                        risk_drivers_json=row.get('risk_drivers_json', {}),
                        diagnosis_pills=row.get('diagnosis_pills', []),

                        # Status
                        status='active',
                        is_safe=bool(row.get('is_safe', False))
                    )

                    session.add(campaign)
                    self.stats["campaigns_created"] += 1

                except Exception as e:
                    error_msg = f"Error saving campaign {row.get('campaign_id', 'unknown')}: {e}"
                    print(f"ERROR: {error_msg}")
                    self.stats["errors"].append(error_msg)

                self.stats["campaigns_processed"] += 1

            await session.commit()
            print(f"Saved {self.stats['campaigns_created']} campaigns to database")

    def safe_decimal(self, value):
        """Safely convert value to decimal"""
        if pd.isna(value) or value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def safe_int(self, value):
        """Safely convert value to integer"""
        if pd.isna(value) or value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    async def run_migration(self):
        """Run the complete Ultimate DMS migration"""
        print("Starting Ultimate DMS Migration")
        print("=" * 60)

        start_time = datetime.now()

        try:
            # Initialize database
            await init_database()

            # Load Ultimate DMS data
            raw_df = self.load_ultimate_dms_data()

            # Transform to book format
            transformed_df = self.transform_ultimate_dms_data(raw_df)

            # Save to database
            await self.save_to_database(transformed_df)

            # Print summary
            duration = (datetime.now() - start_time).total_seconds()

            print("\n" + "=" * 60)
            print("MIGRATION SUMMARY")
            print("=" * 60)
            print(f"Campaigns Processed: {self.stats['campaigns_processed']}")
            print(f"Campaigns Created: {self.stats['campaigns_created']}")
            print(f"Errors: {len(self.stats['errors'])}")
            print(f"Duration: {duration:.1f} seconds")

            if self.stats["errors"]:
                print(f"\nFirst 3 errors:")
                for error in self.stats["errors"][:3]:
                    print(f"  - {error}")

            if self.stats["campaigns_created"] > 0:
                print("\nUltimate DMS migration completed successfully!")
                print(f"Your book app at http://localhost:8000/book should now show {self.stats['campaigns_created']} REAL campaigns!")
                return True
            else:
                print("\nNo campaigns were created")
                return False

        except Exception as e:
            print(f"\nMigration failed: {e}")
            import traceback
            traceback.print_exc()
            return False


async def main():
    """Main function"""
    migrator = UltimateDMSMigrator()
    success = await migrator.run_migration()
    return 0 if success else 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nMigration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Migration failed: {e}")
        sys.exit(1)