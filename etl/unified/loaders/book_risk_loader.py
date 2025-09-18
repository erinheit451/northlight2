"""
Book Risk Assessment Loader
Calculates churn probabilities and risk data for campaigns
"""

import sys
import json
from pathlib import Path
from typing import List, Dict, Any
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd
import logging

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from .base import BasePostgresLoader
from core.shared import log_step
from core.risk_model.churn import calculate_churn_for_campaign
from core.risk_model.flare import attach_priority_and_flare, compute_priority_v2
from core.models.book import Campaign

logger = logging.getLogger(__name__)


class BookRiskLoader(BasePostgresLoader):
    """Loader for book risk assessment data."""

    def __init__(self):
        super().__init__("book_risk", "book")

    def get_staging_table_name(self) -> str:
        return "campaigns_staging"

    def get_historical_table_name(self) -> str:
        return "campaigns"

    def get_deduplication_columns(self) -> List[str]:
        return ["campaign_id"]

    async def create_table_schema(self, session: AsyncSession) -> None:
        """Create book risk tables if they don't exist."""

        # Create book schema
        await session.execute(text("CREATE SCHEMA IF NOT EXISTS book"))

        # The campaigns table should already exist from the models
        # This is just a safety check
        create_campaigns_sql = """
        CREATE TABLE IF NOT EXISTS book.campaigns (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            campaign_id VARCHAR(255) UNIQUE NOT NULL,
            maid VARCHAR(255),
            advertiser_name VARCHAR(255),
            partner_name VARCHAR(255),
            bid_name VARCHAR(255),
            campaign_name VARCHAR(255),
            am VARCHAR(255),
            optimizer VARCHAR(255),
            gm VARCHAR(255),
            business_category VARCHAR(255),
            campaign_budget DECIMAL(12,2),
            amount_spent DECIMAL(12,2),
            io_cycle INTEGER,
            avg_cycle_length DECIMAL(8,2),
            days_elapsed INTEGER,
            days_active INTEGER,
            utilization DECIMAL(5,2),
            running_cid_leads INTEGER,
            running_cid_cpl DECIMAL(10,2),
            cpl_goal DECIMAL(10,2),
            bsc_cpl_avg DECIMAL(10,2),
            effective_cpl_goal DECIMAL(10,2),
            expected_leads_monthly INTEGER,
            expected_leads_to_date INTEGER,
            expected_leads_to_date_spend DECIMAL(12,2),
            true_days_running INTEGER,
            true_months_running DECIMAL(4,2),
            cycle_label VARCHAR(100),
            age_risk DECIMAL(5,2),
            lead_risk DECIMAL(5,2),
            cpl_risk DECIMAL(5,2),
            util_risk DECIMAL(5,2),
            structure_risk DECIMAL(5,2),
            total_risk_score DECIMAL(5,2),
            value_score DECIMAL(5,2),
            final_priority_score DECIMAL(5,2),
            priority_index DECIMAL(8,4),
            priority_tier VARCHAR(50),
            primary_issue VARCHAR(255),
            churn_prob_90d DECIMAL(5,4),
            churn_risk_band VARCHAR(50),
            revenue_at_risk DECIMAL(12,2),
            risk_drivers_json JSONB,
            flare_score DECIMAL(8,4),
            flare_band VARCHAR(50),
            flare_breakdown_json JSONB,
            flare_score_raw DECIMAL(8,4),
            headline_diagnosis TEXT,
            headline_severity VARCHAR(50),
            diagnosis_pills JSONB,
            campaign_count INTEGER,
            true_product_count INTEGER,
            is_safe BOOLEAN,
            goal_advice_json JSONB,
            status VARCHAR(50) DEFAULT 'new',
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
        """

        await session.execute(text(create_campaigns_sql))
        await session.commit()

    async def load_and_calculate_risk(self, session: AsyncSession, source_data: List[Dict[str, Any]]) -> None:
        """Load campaign data and calculate risk assessments."""

        log_step("Starting book risk calculation", "info")

        processed_campaigns = []
        errors = []

        for campaign_data in source_data:
            try:
                # Calculate risk data using our fixed risk model
                risk_results = await calculate_churn_for_campaign(campaign_data)

                # Merge with original campaign data
                enriched_campaign = {**campaign_data, **risk_results}
                processed_campaigns.append(enriched_campaign)

            except Exception as e:
                logger.error(f"Error calculating risk for campaign {campaign_data.get('campaign_id', 'unknown')}: {e}")
                errors.append(f"Campaign {campaign_data.get('campaign_id', 'unknown')}: {str(e)}")

        if errors:
            logger.warning(f"Encountered {len(errors)} errors during risk calculation")
            for error in errors[:5]:  # Log first 5 errors
                logger.warning(f"  - {error}")

        log_step(f"Successfully calculated risk for {len(processed_campaigns)} campaigns", "info")

        # Calculate FLARE scores (percentile ranking across all campaigns)
        if processed_campaigns:
            log_step("Calculating FLARE scores and priority rankings", "info")

            # Convert to DataFrame for FLARE calculation
            df = pd.DataFrame(processed_campaigns)

            # Calculate FLARE scores and priority rankings
            df_with_flare = attach_priority_and_flare(df)
            df_with_flare['priority_tier'] = compute_priority_v2(df_with_flare)

            # Convert back to list of dicts
            processed_campaigns = df_with_flare.to_dict('records')

            log_step(f"FLARE calculation complete for {len(processed_campaigns)} campaigns", "info")

        # Insert/update campaigns in the database
        if processed_campaigns:
            await self._upsert_campaigns(session, processed_campaigns)

    async def _upsert_campaigns(self, session: AsyncSession, campaigns: List[Dict[str, Any]]) -> None:
        """Insert or update campaigns in the database."""

        for campaign in campaigns:
            # Convert to format suitable for database
            db_data = self._prepare_campaign_for_db(campaign)

            # Upsert query
            upsert_sql = """
            INSERT INTO book.campaigns (
                campaign_id, maid, advertiser_name, partner_name, bid_name, campaign_name,
                am, optimizer, gm, business_category, campaign_budget, amount_spent,
                io_cycle, avg_cycle_length, days_elapsed, days_active, utilization,
                running_cid_leads, running_cid_cpl, cpl_goal, bsc_cpl_avg, effective_cpl_goal,
                expected_leads_monthly, expected_leads_to_date, expected_leads_to_date_spend,
                churn_prob_90d, churn_risk_band, revenue_at_risk, risk_drivers_json,
                flare_score, flare_band, flare_breakdown_json, flare_score_raw,
                priority_index, priority_tier, final_priority_score,
                is_safe, updated_at
            ) VALUES (
                :campaign_id, :maid, :advertiser_name, :partner_name, :bid_name, :campaign_name,
                :am, :optimizer, :gm, :business_category, :campaign_budget, :amount_spent,
                :io_cycle, :avg_cycle_length, :days_elapsed, :days_active, :utilization,
                :running_cid_leads, :running_cid_cpl, :cpl_goal, :bsc_cpl_avg, :effective_cpl_goal,
                :expected_leads_monthly, :expected_leads_to_date, :expected_leads_to_date_spend,
                :churn_prob_90d, :churn_risk_band, :revenue_at_risk, :risk_drivers_json,
                :flare_score, :flare_band, :flare_breakdown_json, :flare_score_raw,
                :priority_index, :priority_tier, :final_priority_score,
                :is_safe, NOW()
            )
            ON CONFLICT (campaign_id) DO UPDATE SET
                maid = EXCLUDED.maid,
                advertiser_name = EXCLUDED.advertiser_name,
                partner_name = EXCLUDED.partner_name,
                bid_name = EXCLUDED.bid_name,
                campaign_name = EXCLUDED.campaign_name,
                am = EXCLUDED.am,
                optimizer = EXCLUDED.optimizer,
                gm = EXCLUDED.gm,
                business_category = EXCLUDED.business_category,
                campaign_budget = EXCLUDED.campaign_budget,
                amount_spent = EXCLUDED.amount_spent,
                io_cycle = EXCLUDED.io_cycle,
                avg_cycle_length = EXCLUDED.avg_cycle_length,
                days_elapsed = EXCLUDED.days_elapsed,
                days_active = EXCLUDED.days_active,
                utilization = EXCLUDED.utilization,
                running_cid_leads = EXCLUDED.running_cid_leads,
                running_cid_cpl = EXCLUDED.running_cid_cpl,
                cpl_goal = EXCLUDED.cpl_goal,
                bsc_cpl_avg = EXCLUDED.bsc_cpl_avg,
                effective_cpl_goal = EXCLUDED.effective_cpl_goal,
                expected_leads_monthly = EXCLUDED.expected_leads_monthly,
                expected_leads_to_date = EXCLUDED.expected_leads_to_date,
                expected_leads_to_date_spend = EXCLUDED.expected_leads_to_date_spend,
                churn_prob_90d = EXCLUDED.churn_prob_90d,
                churn_risk_band = EXCLUDED.churn_risk_band,
                revenue_at_risk = EXCLUDED.revenue_at_risk,
                risk_drivers_json = EXCLUDED.risk_drivers_json,
                flare_score = EXCLUDED.flare_score,
                flare_band = EXCLUDED.flare_band,
                flare_breakdown_json = EXCLUDED.flare_breakdown_json,
                flare_score_raw = EXCLUDED.flare_score_raw,
                priority_index = EXCLUDED.priority_index,
                priority_tier = EXCLUDED.priority_tier,
                final_priority_score = EXCLUDED.final_priority_score,
                is_safe = EXCLUDED.is_safe,
                updated_at = NOW()
            """

            await session.execute(text(upsert_sql), db_data)

        await session.commit()
        log_step(f"Upserted {len(campaigns)} campaigns to database", "info")

    def _prepare_campaign_for_db(self, campaign: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare campaign data for database insertion."""

        # Extract only the fields we need for the database
        db_data = {}

        # Required fields
        db_data['campaign_id'] = str(campaign.get('campaign_id', ''))
        db_data['maid'] = campaign.get('maid')
        db_data['advertiser_name'] = campaign.get('advertiser_name')
        db_data['partner_name'] = campaign.get('partner_name')
        db_data['bid_name'] = campaign.get('bid_name')
        db_data['campaign_name'] = campaign.get('campaign_name')

        # Account management
        db_data['am'] = campaign.get('am')
        db_data['optimizer'] = campaign.get('optimizer')
        db_data['gm'] = campaign.get('gm')
        db_data['business_category'] = campaign.get('business_category')

        # Financial data
        db_data['campaign_budget'] = campaign.get('campaign_budget')
        db_data['amount_spent'] = campaign.get('amount_spent')

        # Operational metrics
        db_data['io_cycle'] = campaign.get('io_cycle')
        db_data['avg_cycle_length'] = campaign.get('avg_cycle_length')
        db_data['days_elapsed'] = campaign.get('days_elapsed')
        db_data['days_active'] = campaign.get('days_active')
        db_data['utilization'] = campaign.get('utilization')

        # Lead metrics
        db_data['running_cid_leads'] = campaign.get('running_cid_leads')
        db_data['running_cid_cpl'] = campaign.get('running_cid_cpl')
        db_data['cpl_goal'] = campaign.get('cpl_goal')
        db_data['bsc_cpl_avg'] = campaign.get('bsc_cpl_avg')
        db_data['effective_cpl_goal'] = campaign.get('effective_cpl_goal')
        db_data['expected_leads_monthly'] = campaign.get('expected_leads_monthly')
        db_data['expected_leads_to_date'] = campaign.get('expected_leads_to_date')
        db_data['expected_leads_to_date_spend'] = campaign.get('expected_leads_to_date_spend')

        # Risk data (our calculated values)
        db_data['churn_prob_90d'] = campaign.get('churn_prob_90d')
        db_data['churn_risk_band'] = campaign.get('churn_risk_band')
        db_data['revenue_at_risk'] = campaign.get('revenue_at_risk')

        # Serialize JSON data
        risk_drivers = campaign.get('risk_drivers_json')
        if risk_drivers and isinstance(risk_drivers, dict):
            db_data['risk_drivers_json'] = json.dumps(risk_drivers)
        else:
            db_data['risk_drivers_json'] = risk_drivers

        # FLARE and priority data
        db_data['flare_score'] = campaign.get('flare_score')
        db_data['flare_band'] = campaign.get('flare_band')
        db_data['flare_score_raw'] = campaign.get('flare_score_raw')
        db_data['priority_index'] = campaign.get('priority_index')
        db_data['priority_tier'] = campaign.get('priority_tier')
        db_data['final_priority_score'] = campaign.get('final_priority_score')

        # Serialize FLARE breakdown JSON
        flare_breakdown = campaign.get('flare_breakdown_json')
        if flare_breakdown and isinstance(flare_breakdown, dict):
            db_data['flare_breakdown_json'] = json.dumps(flare_breakdown)
        else:
            db_data['flare_breakdown_json'] = flare_breakdown

        db_data['is_safe'] = campaign.get('is_safe', False)

        return db_data