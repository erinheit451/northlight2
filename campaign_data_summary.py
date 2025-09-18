#!/usr/bin/env python3
"""
Comprehensive Campaign Database Analysis
"""
import asyncpg
import asyncio

async def comprehensive_data_summary():
    conn = await asyncpg.connect('postgresql://northlight_user:northlight_secure_2024@localhost:5432/unified_northlight')

    try:
        print('=' * 80)
        print('COMPREHENSIVE CAMPAIGN DATABASE ANALYSIS')
        print('=' * 80)

        # Current book campaigns overview
        current_stats = await conn.fetchrow('''
            SELECT
                COUNT(*) as total_campaigns,
                COUNT(DISTINCT partner_name) as unique_partners,
                COUNT(DISTINCT advertiser_name) as unique_advertisers,
                AVG(campaign_budget) as avg_budget,
                SUM(amount_spent) as total_spent,
                SUM(running_cid_leads) as total_leads
            FROM book.campaigns
        ''')

        print(f'\nCURRENT CAMPAIGNS OVERVIEW:')
        print(f'  Total active campaigns: {current_stats["total_campaigns"]:,}')
        print(f'  Unique partners: {current_stats["unique_partners"]:,}')
        print(f'  Unique advertisers: {current_stats["unique_advertisers"]:,}')
        print(f'  Average budget: ${current_stats["avg_budget"]:,.0f}')
        print(f'  Total spent: ${current_stats["total_spent"]:,.0f}')
        print(f'  Total leads: {current_stats["total_leads"]:,}')

        # Historical data depth
        print(f'\nHISTORICAL DATA DEPTH:')

        hist_depth = await conn.fetchrow('''
            SELECT
                MIN(report_month) as earliest,
                MAX(report_month) as latest,
                COUNT(DISTINCT campaign_id) as unique_campaigns,
                COUNT(*) as total_records,
                COUNT(DISTINCT business_name) as unique_businesses
            FROM book.raw_heartbeat_spend_revenue_performance_historical
        ''')

        print(f'  Timeframe: {hist_depth["earliest"]} to {hist_depth["latest"]}')
        print(f'  Historical campaigns: {hist_depth["unique_campaigns"]:,}')
        print(f'  Total monthly records: {hist_depth["total_records"]:,}')
        print(f'  Unique businesses: {hist_depth["unique_businesses"]:,}')

        # Current vs Historical linkage
        print(f'\nCURRENT vs HISTORICAL LINKAGE:')

        linkage = await conn.fetchval('''
            SELECT COUNT(DISTINCT c.campaign_id)
            FROM book.campaigns c
            WHERE EXISTS (
                SELECT 1 FROM book.raw_heartbeat_spend_revenue_performance_historical h
                WHERE h.campaign_id::text = c.campaign_id
            )
        ''')

        total_current = current_stats['total_campaigns']
        print(f'  Current campaigns with historical data: {linkage}/{total_current} ({linkage/total_current*100:.1f}%)')

        # Available metrics summary
        print(f'\nAVAILABLE METRICS PER CAMPAIGN:')
        print(f'  CURRENT STATE (book.campaigns):')
        print(f'    - Campaign budget, spend, leads, CPL')
        print(f'    - Days elapsed, utilization, cycle info')
        print(f'    - Team (AM, Optimizer, GM)')
        print(f'    - Risk scores (churn probability, FLARE)')
        print(f'    - Diagnosis (headlines, pills, waterfall)')
        print(f'    - Partner and advertiser details')

        print(f'  HISTORICAL PERFORMANCE:')
        print(f'    - Monthly spend, leads, CPL trends')
        print(f'    - Clicks, impressions, CTR trends')
        print(f'    - Channel and geographic breakdown')
        print(f'    - Up to 33 months of history (2023-2025)')

        print(f'  OPERATIONAL DATA:')
        print(f'    - Agreed CPL performance tracking')
        print(f'    - Cycle status and renewal info')
        print(f'    - Team assignments and office details')
        print(f'    - Business categorization and verticals')

    except Exception as e:
        print(f'Error: {e}')
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(comprehensive_data_summary())