#!/usr/bin/env python3
"""
Book Data Transformation Script
Uses the Northlight risk model to transform raw data into structured book.campaigns
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

# Import our book risk model
from book_risk_model.constants import *
from book_risk_model.utils import *

# Import database
from core.database import get_db_session, init_database
from core.models.book import Campaign, DataSnapshot
from book_risk_model.pg_ingest import load_health_data_sync, load_breakout_data_sync


class BookDataTransformer:
    """Transform raw data into book.campaigns using basic processing"""

    def __init__(self):
        self.stats = {
            "campaigns_processed": 0,
            "campaigns_created": 0,
            "errors": []
        }

    async def load_and_process_data(self) -> pd.DataFrame:
        """Load data from PostgreSQL and run basic processing"""
        print("Loading campaign data from PostgreSQL...")

        # Load health data (main campaign performance)
        from book_risk_model.pg_ingest import load_health_data
        health_data = await load_health_data()
        if health_data.empty:
            raise ValueError("No health data found in PostgreSQL")

        print(f"Loaded {len(health_data)} campaigns")

        # Basic processing - add required fields
        df = health_data.copy()

        # Add default risk scores for now
        df['churn_prob_90d'] = 0.15  # Default 15% churn probability
        df['flare_score'] = 50.0     # Default moderate score
        df['priority_index'] = 50.0  # Default priority
        df['priority_tier'] = 'P3 - MEDIUM'  # Default tier
        df['primary_issue'] = 'Monitoring'
        df['headline_diagnosis'] = 'Campaign under review'
        df['risk_drivers_json'] = [{} for _ in range(len(df))]  # Create list of empty dicts
        df['diagnosis_pills'] = [[] for _ in range(len(df))]    # Create list of empty lists
        df['is_safe'] = False

        print(f"Processed {len(df)} campaigns with basic risk scores")
        return df

    async def save_to_database(self, df: pd.DataFrame):
        """Save processed campaigns to book.campaigns table"""
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
                        partner_name=str(row.get('partner_name', 'Partner')),
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

            # Create data snapshot record - skip for now
            # snapshot = DataSnapshot(...)
            # session.add(snapshot)

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

    async def run_transformation(self):
        """Run the complete data transformation"""
        print("Starting Book Data Transformation")
        print("=" * 60)

        start_time = datetime.now()

        try:
            # Initialize database
            await init_database()

            # Load and process data
            processed_df = await self.load_and_process_data()

            # Save to database
            await self.save_to_database(processed_df)

            # Print summary
            duration = (datetime.now() - start_time).total_seconds()

            print("\n" + "=" * 60)
            print("TRANSFORMATION SUMMARY")
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
                print("\nTransformation completed successfully!")
                print(f"Your book app at http://localhost:8000/book should now show {self.stats['campaigns_created']} campaigns!")
                return True
            else:
                print("\nNo campaigns were created")
                return False

        except Exception as e:
            print(f"\nTransformation failed: {e}")
            import traceback
            traceback.print_exc()
            return False


async def main():
    """Main function"""
    transformer = BookDataTransformer()
    success = await transformer.run_transformation()
    return 0 if success else 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nTransformation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Transformation failed: {e}")
        sys.exit(1)