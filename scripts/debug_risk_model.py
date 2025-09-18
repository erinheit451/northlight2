#!/usr/bin/env python3
"""
Debug script to test risk model on a single campaign
"""

import pandas as pd
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import the risk model
from book_risk_model.core.churn import calculate_churn_probability, _collect_odds_factors_for_row
from book_risk_model.core.rules import preprocess_campaign_data, process_campaign_goals

def test_single_campaign():
    """Test risk calculation on campaign 4986576"""

    # Load Ultimate DMS data
    df = pd.read_csv('C:/Users/Roci/Heartbeat/data/raw/ultimate_dms/ultimate_dms_2025-09-17.csv', sep='\t', encoding='utf-16')

    # Get the problem campaign
    problem_row = df[df['Campaign ID'] == 4986576].iloc[0]

    print("=== PROBLEM CAMPAIGN DATA ===")
    print(f"Campaign ID: {problem_row['Campaign ID']}")
    print(f"Advertiser: {problem_row['Advertiser Name']}")
    print(f"CPL Goal: {problem_row['CPL Goal']}")
    print(f"Running CPL: {problem_row['Running CID CPL']}")
    print(f"CPL Ratio: {float(problem_row['Running CID CPL']) / float(problem_row['CPL Goal']):.3f}")
    print(f"Leads: {problem_row['Running CID Leads']}")
    print(f"Days Elapsed: {problem_row['Days Elapsed']}")

    # Transform to our format
    test_df = pd.DataFrame([{
        'campaign_id': str(problem_row['Campaign ID']),
        'maid': str(problem_row['MAID']),
        'advertiser_name': problem_row['Advertiser Name'],
        'partner_name': problem_row['BID Name'],
        'bid_name': problem_row['BID Name'],
        'campaign_name': problem_row['Campaign Name'],
        'am': problem_row['AM'],
        'optimizer': problem_row['Optimizer 1'],
        'gm': problem_row.get('AM Manager', ''),
        'business_category': problem_row['BC'],
        'campaign_budget': float(problem_row['Campaign Budget']),
        'amount_spent': float(problem_row['Amount Spent']),
        'io_cycle': int(problem_row['IO Cycle']),
        'avg_cycle_length': float(problem_row['Avg Cycle Length']),
        'days_elapsed': int(problem_row['Days Elapsed']),
        'utilization': float(problem_row['Utilization']),
        'running_cid_leads': int(problem_row['Running CID Leads']),
        'running_cid_cpl': float(problem_row['Running CID CPL']),
        'cpl_goal': float(problem_row['CPL Goal']),
        'bsc_cpl_avg': float(problem_row['BSC CPL Avg']),
        'effective_cpl_goal': float(problem_row['CPL Goal']),
        'advertiser_product_count': 1,  # Default
        'bsc_cpc_average': 3.0,  # Default
    }])

    print("\n=== STEP 1: PREPROCESS ===")
    try:
        processed_df = preprocess_campaign_data(test_df)
        print("Preprocessing successful")
        print(f"Columns after preprocessing: {list(processed_df.columns)}")
    except Exception as e:
        print(f"Preprocessing failed: {e}")
        return

    print("\n=== STEP 2: PROCESS GOALS ===")
    try:
        goal_df = process_campaign_goals(processed_df)
        print("Goal processing successful")
    except Exception as e:
        print(f"Goal processing failed: {e}")
        return

    print("\n=== STEP 3: CALCULATE CHURN ===")
    try:
        final_df = calculate_churn_probability(goal_df)
        print("Churn calculation successful")

        row = final_df.iloc[0]
        print(f"Churn probability: {row.get('churn_prob_90d', 'MISSING')}")
        print(f"Risk drivers JSON: {row.get('risk_drivers_json', 'MISSING')}")
        print(f"Is safe: {row.get('is_safe', 'MISSING')}")

    except Exception as e:
        print(f"Churn calculation failed: {e}")
        import traceback
        traceback.print_exc()
        return

    print("\n=== STEP 4: TEST FACTORS ===")
    try:
        # Test the odds factor calculation directly
        row_dict = final_df.iloc[0].to_dict()
        factors = _collect_odds_factors_for_row(row_dict)
        print(f"Odds factors: {factors}")
    except Exception as e:
        print(f"Factor calculation failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_single_campaign()