#!/usr/bin/env python3
"""
Simple sample data loader without Unicode characters
"""

import asyncio
import sys
import random
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.database import init_database, get_db_session
from core.models.book import Campaign, Partner, PartnerOpportunity, DataSnapshot


async def create_sample_campaigns():
    """Create sample campaign data."""
    campaigns = []

    # Sample data
    partners = ["Acme Marketing", "Digital Solutions Inc", "WebGrow Partners", "Local Lead Gen", "Marketing Plus"]
    advertisers = ["Joe's Pizza", "Smith Law Firm", "ABC Plumbing", "Best Dentist", "Auto Repair Co"]
    ams = ["Sarah Johnson", "Mike Chen", "Lisa Rodriguez", "Tom Wilson", "Amy Davis"]
    optimizers = ["Alex Smith", "Jordan Lee", "Casey Brown", "Taylor Garcia", "Morgan Clark"]

    for i in range(50):  # Create 50 sample campaigns
        campaign_id = f"CID{10000 + i}"
        partner = random.choice(partners)
        advertiser = random.choice(advertisers)

        budget = random.randint(1000, 10000)
        churn_prob = random.uniform(0.05, 0.40)
        risk_score = random.uniform(20, 95)

        # Risk drivers
        risk_drivers = {
            "baseline": random.randint(8, 15),
            "drivers": [
                {"name": "High CPL (≥3× goal)", "impact": random.randint(5, 20)},
                {"name": "Single Product", "impact": random.randint(8, 15)}
            ]
        }

        campaign = Campaign(
            campaign_id=campaign_id,
            maid=f"MA{random.randint(100000, 999999)}",
            advertiser_name=f"{advertiser} #{i}",
            partner_name=partner,
            bid_name=f"BID-{random.randint(1000, 9999)}",
            campaign_name=f"{advertiser} - Search Campaign",
            am=random.choice(ams),
            optimizer=random.choice(optimizers),
            gm=random.choice(ams),
            business_category=random.choice(["Restaurant", "Legal", "Home Services", "Healthcare"]),
            campaign_budget=Decimal(str(budget)),
            amount_spent=Decimal(str(budget * random.uniform(0.7, 1.0))),
            running_cid_leads=random.randint(10, 100),
            running_cid_cpl=Decimal(str(random.uniform(50, 200))),
            cpl_goal=Decimal(str(random.uniform(40, 150))),
            days_elapsed=random.randint(30, 365),
            days_active=random.randint(30, 365),
            utilization=Decimal(str(random.uniform(50, 120))),
            priority_tier=random.choice(["P1 - CRITICAL", "P2 - HIGH", "P3 - MEDIUM", "P4 - LOW"]),
            churn_prob_90d=Decimal(str(churn_prob)),
            churn_risk_band="High" if churn_prob > 0.3 else "Medium" if churn_prob > 0.15 else "Low",
            revenue_at_risk=Decimal(str(budget * churn_prob)),
            risk_drivers_json=risk_drivers,
            flare_score=Decimal(str(risk_score)),
            priority_index=Decimal(str(random.uniform(0, 1000))),
            headline_diagnosis=f"Campaign requires attention",
            true_product_count=random.randint(1, 3),
            is_safe=random.choice([True, False]),
            status="new"
        )

        campaigns.append(campaign)

    return campaigns


async def create_sample_partners(campaigns):
    """Create partner summary data from campaigns."""
    partners = {}

    # Group campaigns by partner
    for campaign in campaigns:
        partner_name = campaign.partner_name
        if partner_name not in partners:
            partners[partner_name] = {
                "total_budget": 0,
                "single_product_count": 0,
                "two_product_count": 0,
                "three_plus_product_count": 0,
                "cross_sell_ready_count": 0,
                "upsell_ready_count": 0
            }

        data = partners[partner_name]
        data["total_budget"] += float(campaign.campaign_budget or 0)

        # Count by product count
        product_count = campaign.true_product_count or 1
        if product_count == 1:
            data["single_product_count"] += 1
            data["cross_sell_ready_count"] += 1
        elif product_count == 2:
            data["two_product_count"] += 1
        else:
            data["three_plus_product_count"] += 1

        # Upsell logic
        if campaign.priority_tier in ["P3 - MEDIUM", "P4 - LOW"]:
            data["upsell_ready_count"] += 1

    # Create Partner objects
    partner_objects = []
    for partner_name, data in partners.items():
        partner = Partner(
            partner_name=partner_name,
            playbook="seo_dash",
            total_budget=Decimal(str(data["total_budget"])),
            single_product_count=data["single_product_count"],
            two_product_count=data["two_product_count"],
            three_plus_product_count=data["three_plus_product_count"],
            cross_sell_ready_count=data["cross_sell_ready_count"],
            upsell_ready_count=data["upsell_ready_count"]
        )
        partner_objects.append(partner)

    return partner_objects


async def main():
    """Main function."""
    print("Starting sample data loading...")

    try:
        await init_database()
        print("Database connected")

        async with get_db_session() as session:
            # Create sample data
            print("Creating sample campaigns...")
            campaigns = await create_sample_campaigns()

            print("Creating partner data...")
            partners = await create_sample_partners(campaigns)

            # Add to database
            print("Inserting data into database...")

            # Add campaigns
            for campaign in campaigns:
                session.add(campaign)

            # Add partners
            for partner in partners:
                session.add(partner)

            # Commit transaction
            await session.commit()

            print(f"SUCCESS: Loaded {len(campaigns)} campaigns and {len(partners)} partners")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())