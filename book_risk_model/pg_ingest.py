"""
PostgreSQL Data Ingest for Book Risk Model
Replaces CSV loading with database loading for the unified platform
"""
import pandas as pd
import asyncpg
from typing import Optional, Dict, Any
import asyncio
import sys
from pathlib import Path
import numpy as np

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import risk calculation modules
from book_risk_model.core.churn import calculate_churn_probability
from book_risk_model.core.rules import process_campaign_goals
from book_risk_model.partner_gm_mapping import correct_gm_by_partner
from book_risk_model.core.flare import attach_priority_and_flare, compute_priority_v2
from book_risk_model.advertiser_lifetime import calculate_advertiser_lifetime
from book_risk_model.constants import *

DATABASE_URL = "postgresql://northlight_user:northlight_secure_2024@localhost:5432/unified_northlight"

def assign_gm_by_advertiser(advertiser_name: str, am_name: str) -> str:
    """
    Assign GM based on advertiser name patterns and business rules.
    This is a temporary mapping until we establish proper AID->BID->GM linkage.
    """
    if not advertiser_name:
        return "Unassigned"

    advertiser = advertiser_name.lower()

    # SPECIFIC OVERRIDES - These take precedence over keyword matching
    specific_overrides = {
        # U of I cases - should be Trent Hebert
        'u of i college of aces': 'Trent Hebert',
        'u of i gies college of business': 'Trent Hebert',
        'outdoor spaces': 'Trent Hebert',  # Based on investigation results

        # Sano Heating - should be Trent Hebert with Salem partner
        'sano heating & air conditioning': 'Trent Hebert',
        'sano heating and air conditioning': 'Trent Hebert',
        'sano heating': 'Trent Hebert',

        # Blinds companies - should be Stephen Jones (not Ashley Reamer)
        'all about blinds': 'Stephen Jones',
        'integrity blinds & shutters': 'Stephen Jones',
        'southern accent shutters & blinds': 'Stephen Jones',
        'window magic blinds & drapery inc.': 'Stephen Jones',
        'beginning to end interiors & blinds': 'Stephen Jones',
        'alton bay premium blinds': 'Stephen Jones',
        'wessco blinds': 'Stephen Jones',
        'sun catchers blinds shades shutters': 'Stephen Jones',
        'classic blinds and shutters': 'Stephen Jones',
        'georgia blinds & interiors': 'Stephen Jones',
        "blinds 'n' shades express": 'Stephen Jones',
        'blinds and us inc, seneca': 'Stephen Jones',
        "little's custom blinds & shutters": 'Stephen Jones',
        "ivan's blinds and more": 'Stephen Jones',
        'blinds plus': 'Stephen Jones',
        'all about blinds etc inc': 'Stephen Jones',
        'nh blinds': 'Stephen Jones',
        'viking blinds': 'Stephen Jones',
        'blinds & shutters of all kinds': 'Stephen Jones',
        'elden draperies, blinds, & shades': 'Stephen Jones',
        'blinds plus designs': 'Stephen Jones',
        'blinds and us of greenville': 'Stephen Jones',
        'advantage blinds': 'Stephen Jones',
        'tri city blinds': 'Stephen Jones',
        'time for blinds': 'Stephen Jones',
        'custom wood shutters & blinds': 'Stephen Jones',
        'florida custom blinds, shades & shutters': 'Stephen Jones',
        'blindsmith, inc': 'Stephen Jones',
        'blinds and designs llc': 'Stephen Jones',
        "kerry's blinds": 'Stephen Jones',
        'discount best blinds & shutters': 'Stephen Jones',
        'betz blinds': 'Stephen Jones'
    }

    # Check specific overrides first
    for company, gm in specific_overrides.items():
        if company in advertiser:
            return gm

    # Business rule mapping based on advertiser patterns/geography/verticals
    # These are secondary to specific overrides

    # Healthcare/Medical advertisers -> Trent Hebert
    healthcare_keywords = [
        'coloplast', 'medical', 'health', 'doctor', 'dr.', 'clinic', 'wound care',
        'orthodontics', 'podiatry', 'rehab', 'therapy', 'dental', 'urology'
    ]

    # Education advertisers -> Trent Hebert (fixed - was Stephen Jones)
    # Most education clients actually go to Trent based on investigation
    education_keywords = [
        'university', 'college', 'school', 'campbellsville',
        'saint louis school', 'education'
    ]

    # Home services/Construction -> Ashley Reamer (but NOT blinds - those are Stephen)
    home_services_keywords = [
        'construction', 'roofing', 'garage door', 'home',
        'restoration', 'waterproofing', 'renovation', 'flooring', 'hvac'
    ]

    # Check for healthcare
    if any(keyword in advertiser for keyword in healthcare_keywords):
        return "Trent Hebert"

    # Check for education
    if any(keyword in advertiser for keyword in education_keywords):
        return "Trent Hebert"

    # Check for home services (excluding blinds which are handled by overrides)
    if any(keyword in advertiser for keyword in home_services_keywords):
        return "Ashley Reamer"

    # Fallback: assign based on AM if we recognize the AM->GM relationship
    am_to_gm = {
        'beau waskom': 'Trent Hebert',
        'katharine pierce': 'Ashley Reamer',
        'robert reynolds iii': 'Stephen Jones',
        'christopher waters': 'Ashley Reamer',
        'jeannette king': 'Stephen Jones',
        'eisha potter': 'Ashley Reamer'
    }

    am_lower = am_name.lower() if am_name else ""
    if am_lower in am_to_gm:
        return am_to_gm[am_lower]

    # Final fallback - distribute evenly
    first_letter = advertiser[0] if advertiser else 'z'
    if first_letter <= 'h':
        return "Ashley Reamer"
    elif first_letter <= 'p':
        return "Stephen Jones"
    else:
        return "Trent Hebert"


def assign_partner_from_performance_data(maid: str, performance_data: dict) -> str:
    """
    Assign marketing partner/agency from performance data business_name field.
    This is the proper way to get real partner information.
    """
    # Check if we have performance data for this MAID
    if maid and str(maid) in performance_data:
        perf_data = performance_data[str(maid)]
        business_name = perf_data.get('business_name', '').strip()

        if business_name and business_name != 'None':
            # Clean up business name and extract partner
            # Handle common variations
            partner_name_mappings = {
                'BFM Group Inc.': 'BFM Group',
                'BFM Group Inc. Invoice': 'BFM Group',
                'SalemSurround': 'Salem',
                'enCOMPASS Agency': 'enCOMPASS Agency',
                # Add more mappings as needed
            }

            return partner_name_mappings.get(business_name, business_name)

    # Fallback to generic partner
    return 'Partner'


def assign_partners_from_performance_data(df: pd.DataFrame, perf_data: dict) -> pd.DataFrame:
    """
    Assign partners using business_name from performance data for each campaign.
    This is the proper root-cause solution for partner mapping.
    """
    print("Assigning partners from performance data business_name...")

    for idx, row in df.iterrows():
        campaign_id = str(row['campaign_id'])

        # Check if we have performance data for this campaign
        if campaign_id in perf_data:
            perf = perf_data[campaign_id]
            business_name = (perf.get('business_name', '') or '').strip()

            if business_name and business_name != 'None':
                # Apply partner name mappings to clean up the data
                partner_mappings = {
                    'BFM Group Inc.': 'BFM Group',
                    'BFM Group Inc. Invoice': 'BFM Group',
                    'SalemSurround': 'Salem',
                    'enCOMPASS Agency': 'enCOMPASS Agency',
                    # Add more as discovered
                }

                clean_partner = partner_mappings.get(business_name, business_name)
                df.at[idx, 'partner_name'] = clean_partner
                print(f"   Campaign {campaign_id}: {business_name} -> {clean_partner}")
            else:
                # No business name in performance data
                df.at[idx, 'partner_name'] = 'Unknown'
        else:
            # No performance data available
            df.at[idx, 'partner_name'] = 'Unknown'

    return df


def correct_gm_assignments_from_performance_data(df: pd.DataFrame, perf_data: dict) -> pd.DataFrame:
    """
    Correct GM assignments using the Area field from performance data.
    The Area field contains the authoritative GM assignments.
    Also handles orphaned GM assignments (like Ashley who is no longer employed).
    """
    print("Correcting GM assignments from performance data Area field...")
    corrected_count = 0
    ashley_reassigned = 0

    # Ashley is no longer employed - active GMs only
    active_gms = ['Trent Hebert', 'Stephen Jones']

    for idx, row in df.iterrows():
        campaign_id = str(row['campaign_id'])
        original_gm = row.get('gm', '')

        # Check if we have performance data for this campaign
        if campaign_id in perf_data:
            perf = perf_data[campaign_id]
            area_gm = perf.get('area', '').strip()

            # If area contains an active GM name, use it
            if area_gm and area_gm in active_gms:
                if original_gm != area_gm:
                    df.at[idx, 'gm'] = area_gm
                    corrected_count += 1
                    if original_gm == 'Ashley Reamer':
                        ashley_reassigned += 1
                    print(f"   Campaign {campaign_id}: GM corrected from '{original_gm}' to '{area_gm}'")

            # Handle Ashley assignments when Area field doesn't have active GM
            elif original_gm == 'Ashley Reamer':
                # For Ashley's orphaned campaigns, default to Trent Hebert
                # (The Area field is authoritative - don't make partner assumptions)
                df.at[idx, 'gm'] = 'Trent Hebert'
                corrected_count += 1
                ashley_reassigned += 1
                print(f"   Campaign {campaign_id}: Ashley -> Trent Hebert (orphaned, default)")

        # Handle Ashley assignments without performance data
        elif original_gm == 'Ashley Reamer':
            # Default all Ashley campaigns without performance data to Trent Hebert
            df.at[idx, 'gm'] = 'Trent Hebert'
            corrected_count += 1
            ashley_reassigned += 1
            print(f"   Campaign {campaign_id}: Ashley -> Trent Hebert (no perf data, default)")

    print(f"Corrected GM assignments for {corrected_count} campaigns using Area field")
    if ashley_reassigned > 0:
        print(f"Reassigned {ashley_reassigned} orphaned Ashley campaigns to active GMs")
    return df


def assign_partner_by_gm_and_data_raw(gm: str, advertiser_name: str, area: str) -> str:
    """
    Assign partner based on GM assignment and available data.
    This is a fallback function when performance data isn't available.
    """
    # If area contains actual partner data, use it
    if area and area.strip() and area.strip() != 'None':
        area_clean = area.strip()

        # Check if area contains a GM name vs partner name
        gm_names = ['Trent Hebert', 'Stephen Jones', 'Ashley Reamer', 'Other']
        if area_clean not in gm_names:
            return area_clean

    # Fall back to GM-based partner assignment
    gm_partner_mapping = {
        'Trent Hebert': 'Salem',
        'Stephen Jones': 'BFM Group',
        'Ashley Reamer': 'enCOMPASS Agency',
    }

    return gm_partner_mapping.get(gm, 'Partner')


def assign_partner_by_gm_and_data(row, performance_data: dict) -> str:
    """
    Wrapper function for row-based partner assignment.
    """
    return assign_partner_by_gm_and_data_raw(
        row.get('gm', ''),
        row.get('advertiser_name', ''),
        row.get('area', '')
    )


def apply_risk_calculations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply real risk calculations to replace default values.
    Uses the same business logic as the original risk model.
    """
    if df.empty:
        return df

    print("Applying risk calculations...")

    # Prepare required columns for risk calculations
    df = prepare_columns_for_risk_calc(df)

    # Process campaign goals (calculates cpl_variance_pct, effective_cpl_goal, risk_cpl_goal)
    print("  Processing campaign goals...")
    df = process_campaign_goals(df)

    # Calculate churn probability using business logic
    print("  Calculating churn probabilities...")
    df = calculate_churn_probability(df)

    # Calculate FLARE scores and priority index
    print("  Calculating FLARE scores...")
    df = attach_priority_and_flare(df)

    # Calculate priority tiers
    print("  Calculating priority tiers...")
    df['priority_tier'] = compute_priority_v2(df)

    # Generate diagnosis and pills
    print("  Generating diagnosis...")
    df = generate_diagnosis_and_pills(df)

    return df


def generate_diagnosis_and_pills(df: pd.DataFrame) -> pd.DataFrame:
    """Generate headline diagnosis and diagnosis pills using the presentation layer."""
    print("  Generating headline diagnosis and pills...")

    try:
        # Import the diagnosis functions
        from book_risk_model.presentation.diagnostics import (
            generate_headline_diagnosis,
            generate_diagnosis_pills
        )

        # Generate headline diagnosis for entire DataFrame
        headlines, severities = generate_headline_diagnosis(df)
        df['headline_diagnosis'] = headlines
        df['primary_issue'] = severities

        # Generate diagnosis pills for each row
        df['diagnosis_pills'] = df.apply(
            lambda row: generate_diagnosis_pills(row.to_dict()),
            axis=1
        )

        print(f"  Generated diagnosis for {len(df)} campaigns")

    except Exception as e:
        print(f"  Warning: Could not generate diagnosis: {e}")
        # Fall back to default values
        df['headline_diagnosis'] = 'Campaign under review'
        df['primary_issue'] = 'Monitoring'
        df['diagnosis_pills'] = [[] for _ in range(len(df))]

    return df


def calculate_expected_leads(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate expected leads based on budget, CPL goals, and campaign progress.
    Uses multiple approaches to provide the most accurate expected leads.
    """
    print("Calculating expected leads...")

    # Initialize expected leads columns
    df['expected_leads_monthly'] = 0.0
    df['expected_leads_to_date'] = 0.0
    df['expected_leads_to_date_spend'] = 0.0

    for idx, row in df.iterrows():
        campaign_budget = row.get('campaign_budget', 0) or 0
        cpl_goal = row.get('cpl_goal', 0) or 0
        amount_spent = row.get('amount_spent', 0) or 0
        days_elapsed = row.get('days_elapsed', 30) or 30
        io_cycle = row.get('io_cycle', 1) or 1
        avg_cycle_length = row.get('avg_cycle_length', 30) or 30

        # Method 1: Budget-based calculation (most accurate for full campaign)
        if campaign_budget > 0 and cpl_goal > 0:
            total_expected_leads = float(campaign_budget) / float(cpl_goal)
            df.at[idx, 'expected_leads_monthly'] = total_expected_leads

            # Calculate expected leads to date based on time progress
            cycle_length = max(float(avg_cycle_length), 30)  # At least 30 days
            time_progress = min(float(days_elapsed) / cycle_length, 1.0)  # Cap at 100%
            expected_to_date = total_expected_leads * time_progress
            df.at[idx, 'expected_leads_to_date'] = expected_to_date

        # Method 2: Spend-based calculation (most accurate for current spend)
        if amount_spent > 0 and cpl_goal > 0:
            spend_based_expected = float(amount_spent) / float(cpl_goal)
            df.at[idx, 'expected_leads_to_date_spend'] = spend_based_expected

            # If we don't have budget-based, use spend-based as fallback
            if df.at[idx, 'expected_leads_to_date'] == 0:
                df.at[idx, 'expected_leads_to_date'] = spend_based_expected

        # Method 3: For campaigns with no budget info, use spend extrapolation
        if (df.at[idx, 'expected_leads_to_date'] == 0 and
            amount_spent > 0 and cpl_goal > 0 and days_elapsed > 0):

            # Extrapolate monthly expected from current spend pace
            daily_spend = float(amount_spent) / float(days_elapsed)
            monthly_spend = daily_spend * 30
            monthly_expected = monthly_spend / float(cpl_goal)

            df.at[idx, 'expected_leads_monthly'] = monthly_expected

            # Current expected based on time elapsed
            time_progress = min(float(days_elapsed) / 30, 1.0)
            df.at[idx, 'expected_leads_to_date'] = monthly_expected * time_progress

    # Round to reasonable precision
    df['expected_leads_monthly'] = df['expected_leads_monthly'].round(1)
    df['expected_leads_to_date'] = df['expected_leads_to_date'].round(1)
    df['expected_leads_to_date_spend'] = df['expected_leads_to_date_spend'].round(1)

    # Log summary
    calculated_campaigns = (df['expected_leads_to_date'] > 0).sum()
    print(f"  Calculated expected leads for {calculated_campaigns}/{len(df)} campaigns")

    return df


def calculate_utilization(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate utilization/pacing based on spend vs budget over time."""
    print("Calculating utilization/pacing...")

    df['utilization'] = 0.0

    for idx, row in df.iterrows():
        try:
            amount_spent = row.get('amount_spent', 0)
            campaign_budget = row.get('campaign_budget', 0)
            days_elapsed = row.get('days_elapsed', 30)
            avg_cycle_length = row.get('avg_cycle_length', 30)

            # Convert to float for calculations
            if pd.notna(amount_spent) and pd.notna(campaign_budget) and pd.notna(days_elapsed) and pd.notna(avg_cycle_length):
                amount_spent = float(amount_spent)
                campaign_budget = float(campaign_budget)
                days_elapsed = float(days_elapsed)
                avg_cycle_length = float(avg_cycle_length)

                # Calculate utilization using multiple methods
                if campaign_budget > 0 and days_elapsed > 0 and avg_cycle_length > 0:
                    # Method 1: Spend-based pacing
                    spend_pacing = amount_spent / campaign_budget

                    # Method 2: Time-adjusted pacing
                    time_progress = min(days_elapsed / avg_cycle_length, 1.0)
                    if time_progress > 0:
                        time_adjusted_pacing = spend_pacing / time_progress
                    else:
                        time_adjusted_pacing = spend_pacing

                    # Use time-adjusted pacing as utilization
                    df.at[idx, 'utilization'] = time_adjusted_pacing

                elif amount_spent > 0:
                    # Fallback: Just use spend ratio if we have spending but no budget
                    df.at[idx, 'utilization'] = 1.0  # Assume fully utilized if spending without budget data

        except (ValueError, TypeError, ZeroDivisionError):
            df.at[idx, 'utilization'] = 0.0

    calculated_campaigns = (df['utilization'] > 0).sum()
    print(f"  Calculated utilization for {calculated_campaigns}/{len(df)} campaigns")
    return df


def prepare_columns_for_risk_calc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare and add missing columns needed for risk calculations.
    """
    # Calculate expected leads first (this replaces the default 0.0 values)
    df = calculate_expected_leads(df)

    # Calculate utilization/pacing
    df = calculate_utilization(df)

    # Add other missing columns with default values for risk calculation
    required_columns = {
        'effective_cpl_goal': df.get('cpl_goal', 0).fillna(df.get('bsc_cpl_avg', 150)),
        'advertiser_product_count': 1,  # Default to single product
        'bsc_cpc_average': 3.0,  # Default CPC
        'leads_rolling_30d': None,  # Will be handled by zero-lead logic
        'true_days_running': None,  # Will use calculated value
        'maturity_amplifier': 1.0,  # Default maturity amplifier for categorize_issues()
        'unified_performance_score': 0,  # Default performance score for categorize_issues()
    }

    for col, default_val in required_columns.items():
        if col not in df.columns:
            df[col] = default_val
        else:
            df[col] = df[col].fillna(default_val)

    return df


async def enrich_with_performance_metrics(df: pd.DataFrame, conn) -> pd.DataFrame:
    """
    Enrich campaign data with actual performance metrics from the raw performance table.
    This replaces zero values with real CPL, leads, and other metrics.
    """
    if df.empty:
        return df

    print("Enriching campaigns with performance metrics...")

    # Get all campaign IDs to lookup
    campaign_ids = df['campaign_id'].astype(str).tolist()

    # Query performance data for all campaigns including business_name and area for proper mapping
    perf_query = """
        SELECT DISTINCT ON (campaign_id)
            campaign_id::text,
            cost_per_lead,
            leads,
            spend,
            report_month,
            cobrand as business_name,
            area
        FROM book.raw_heartbeat_spend_revenue_performance_current
        WHERE campaign_id::text = ANY($1)
            AND cost_per_lead IS NOT NULL
        ORDER BY campaign_id, report_month DESC
    """

    try:
        perf_rows = await conn.fetch(perf_query, campaign_ids)

        if perf_rows:
            # Convert to dict for easy lookup
            perf_data = {}
            for row in perf_rows:
                perf_data[str(row['campaign_id'])] = {
                    'cost_per_lead': float(row['cost_per_lead']) if row['cost_per_lead'] else None,
                    'leads': int(row['leads']) if row['leads'] else 0,
                    'spend': float(row['spend']) if row['spend'] else 0.0,
                    'business_name': row['business_name'] if row['business_name'] else None,
                    'area': row['area'] if row['area'] else None,
                }

            print(f"Found performance data for {len(perf_data)} campaigns")

            # Update campaigns with performance data
            for idx, row in df.iterrows():
                campaign_id = str(row['campaign_id'])
                if campaign_id in perf_data:
                    perf = perf_data[campaign_id]

                    # Update CPL if we have better data
                    if perf['cost_per_lead'] and perf['cost_per_lead'] > 0:
                        df.at[idx, 'running_cid_cpl'] = perf['cost_per_lead']

                    # Update leads if we have better data
                    if perf['leads'] > 0:
                        df.at[idx, 'running_cid_leads'] = perf['leads']

                    # Update spend if we have better data
                    if perf['spend'] > df.at[idx, 'amount_spent']:
                        df.at[idx, 'amount_spent'] = perf['spend']

            # Calculate Days Active based on campaign age and activity
            df['days_active'] = df.apply(lambda row:
                min(row['days_elapsed'] or 30, 30) if row['running_cid_leads'] > 0 or row['amount_spent'] > 0
                else None, axis=1)

            # CPL goals should come from DMS Ultimate data - don't fabricate defaults
            # Keep original cpl_goal values from the database as-is

            print(f"Updated {len([c for c in campaign_ids if c in perf_data])} campaigns with performance metrics")
        else:
            print("No performance data found")
            # Still calculate days_active and cpl_goal for all campaigns
            df['days_active'] = df.apply(lambda row:
                min(row['days_elapsed'] or 30, 30) if row['running_cid_leads'] > 0 or row['amount_spent'] > 0
                else None, axis=1)
            # Keep original cpl_goal values from database - don't override with defaults

    except Exception as e:
        print(f"Error enriching performance metrics: {e}")
        # Set defaults if enrichment fails
        df['days_active'] = df.apply(lambda row:
            min(row['days_elapsed'] or 30, 30) if row['running_cid_leads'] > 0 or row['amount_spent'] > 0
            else None, axis=1)
        # Keep original cpl_goal values from database - don't override with defaults

    # Apply GM and partner assignments using performance data
    print("Correcting GM assignments using performance data Area field...")
    if 'perf_data' in locals() and perf_data:
        df = assign_partners_from_performance_data(df, perf_data)
        df = correct_gm_assignments_from_performance_data(df, perf_data)

        # Apply authoritative Partner->GM mapping to override any wrong Area assignments
        df = correct_gm_by_partner(df)

    else:
        # If no performance data, partner is unknown - don't fabricate
        df['partner_name'] = 'Unknown'
        print("Warning: No performance data available - partners set to 'Unknown'")

    return df


async def load_health_data() -> pd.DataFrame:
    """
    Load campaign data from DMS Ultimate table in PostgreSQL.
    This is the proper source for book UI data with real team assignments.
    """
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Query the DMS Ultimate campaigns table - the real source of truth
        query = """
        SELECT
            campaign_id::text as campaign_id,
            maid::text as maid,
            advertiser_name,
            advertiser_name as business_name,  -- Map for compatibility
            bc as business_category,
            campaign_name,
            channel,

            -- Financial data
            amount_spent,
            running_cid_leads,
            running_cid_cpl,
            campaign_budget,
            cpl_goal,
            bsc_cpl_avg,
            utilization,

            -- Team assignments - map to expected field names
            am,
            '' as gm,  -- GM will be calculated using business rules
            COALESCE(optimizer_1, optimizer_2, 'Unassigned') as optimizer,

            -- Campaign details
            bid_name,
            product,
            offer_name,
            finance_product,

            -- Operational data
            io_cycle,
            avg_cycle_length,
            days_elapsed,

            -- Set default values for missing risk fields (will be calculated later)
            0.15 as churn_prob_90d,  -- Default 15% churn probability
            50.0 as flare_score,     -- Default moderate score
            50.0 as priority_index,  -- Default priority
            'P3 - MEDIUM' as priority_tier,  -- Default tier
            'Monitoring' as primary_issue,
            'Campaign under review' as headline_diagnosis,

            -- Metadata
            extracted_at,
            snapshot_date

        FROM heartbeat_core.ultimate_dms_campaigns
        WHERE campaign_id IS NOT NULL
        ORDER BY campaign_id
        """

        rows = await conn.fetch(query)

        # Convert to DataFrame
        if rows:
            df = pd.DataFrame([dict(row) for row in rows])

            # Ensure required columns exist with sensible defaults
            required_cols = {
                'campaign_id': '',
                'maid': '',
                'advertiser_name': '',
                'business_name': '',
                'campaign_name': '',
                'channel': 'Partner',
                'optimizer': 'Unassigned',
                'am': 'Unassigned',
                'gm': 'Unassigned',
                'business_category': '',
                'amount_spent': 0.0,
                'running_cid_leads': 0,
                'running_cid_cpl': 0.0,
                'campaign_budget': 0.0,
                'cpl_goal': 0.0,
                'bsc_cpl_avg': 0.0,
                'utilization': 0.0,
                'days_elapsed': 30,
                'io_cycle': 1,
                'avg_cycle_length': 30.0,
                # Risk fields
                'churn_prob_90d': 0.15,
                'flare_score': 50.0,
                'priority_index': 50.0,
                'priority_tier': 'P3 - MEDIUM',
                'primary_issue': 'Monitoring',
                'headline_diagnosis': 'Campaign under review',
                # Additional fields
                'bid_name': '',
                'product': '',
                'offer_name': '',
                'finance_product': ''
            }

            for col, default_val in required_cols.items():
                if col not in df.columns:
                    df[col] = default_val
                else:
                    df[col] = df[col].fillna(default_val)

            # Convert types
            df['campaign_id'] = df['campaign_id'].astype(str)
            df['maid'] = df['maid'].astype(str)
            df['amount_spent'] = pd.to_numeric(df['amount_spent'], errors='coerce').fillna(0)
            df['running_cid_leads'] = pd.to_numeric(df['running_cid_leads'], errors='coerce').fillna(0)
            df['running_cid_cpl'] = pd.to_numeric(df['running_cid_cpl'], errors='coerce').fillna(0)
            df['campaign_budget'] = pd.to_numeric(df['campaign_budget'], errors='coerce').fillna(0)
            df['utilization'] = pd.to_numeric(df['utilization'], errors='coerce').fillna(0)
            df['cpl_goal'] = pd.to_numeric(df['cpl_goal'], errors='coerce').fillna(0)
            df['bsc_cpl_avg'] = pd.to_numeric(df['bsc_cpl_avg'], errors='coerce').fillna(0)

            # Calculate GM assignments using business rules
            df['gm'] = df.apply(lambda row: assign_gm_by_advertiser(
                row['advertiser_name'],
                row['am']
            ), axis=1)

            # Partner assignments will be handled by enrich_with_performance_metrics()
            # Do NOT fabricate partner names here - wait for performance data
            df['partner_name'] = 'TBD'  # Will be filled by performance data

            print(f"Loaded {len(df)} campaigns from DMS Ultimate database table")
            print(f"GM assignments: {df['gm'].value_counts().to_dict()}")

            # Enrich with performance metrics before risk calculations
            df = await enrich_with_performance_metrics(df, conn)

            # Calculate advertiser lifetime (MAID-level historical data)
            df = await calculate_advertiser_lifetime(df, conn)

            # Apply real risk calculations instead of defaults
            df = apply_risk_calculations(df)

            print(f"Applied risk calculations to {len(df)} campaigns")
            if not df.empty:
                print(f"FLARE score range: {df['flare_score'].min()}-{df['flare_score'].max()}")
                print(f"Churn probability range: {df['churn_prob_90d'].min():.3f}-{df['churn_prob_90d'].max():.3f}")
                print(f"Priority tiers: {df['priority_tier'].value_counts().to_dict()}")

            return df
        else:
            print("No campaign data found in DMS Ultimate table")
            return pd.DataFrame()

    finally:
        await conn.close()


async def load_breakout_data() -> pd.DataFrame:
    """
    Load breakout/roster data for enrichment.
    This would be additional campaign metadata if available.
    """
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Try to get additional campaign details from other tables
        query = """
        SELECT DISTINCT
            campaign_id::text as campaign_id,
            maid::text as maid,
            business_name as advertiser_name,
            -- Try to get more detailed team info if available
            'Partner' as partner_name,
            1 as true_product_count,
            business_category
        FROM book.raw_heartbeat_spend_revenue_performance_current
        WHERE campaign_id IS NOT NULL
        """

        rows = await conn.fetch(query)

        if rows:
            df = pd.DataFrame([dict(row) for row in rows])
            print(f"Loaded {len(df)} breakout records from PostgreSQL")
            return df
        else:
            print("No breakout data found in PostgreSQL")
            return pd.DataFrame()

    finally:
        await conn.close()


def load_health_data_sync() -> pd.DataFrame:
    """Synchronous wrapper for async load_health_data"""
    return asyncio.run(load_health_data())


def load_breakout_data_sync() -> pd.DataFrame:
    """Synchronous wrapper for async load_breakout_data"""
    return asyncio.run(load_breakout_data())


# For backward compatibility with the risk model
def latest_snapshot_path():
    """Mock function to maintain compatibility"""
    return "postgresql://database"