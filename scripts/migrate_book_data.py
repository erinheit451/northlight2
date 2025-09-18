#!/usr/bin/env python3
"""
Book Data Migration Script
Loads sample data into the book system database tables
"""

import asyncio
import sys
import os
from pathlib import Path
from datetime import datetime, date
import json
import random
from decimal import Decimal

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.database import init_database, get_db_session
from core.models.book import Campaign, Partner, PartnerOpportunity, DataSnapshot


async def create_sample_campaigns():
    """Create sample campaign data."""
    campaigns = []

    # Sample data templates
    partners = ["Acme Marketing", "Digital Solutions Inc", "WebGrow Partners", "Local Lead Gen", "Marketing Plus"]
    advertisers = ["Joe's Pizza", "Smith Law Firm", "ABC Plumbing", "Best Dentist", "Auto Repair Co", "Flower Shop"]
    ams = ["Sarah Johnson", "Mike Chen", "Lisa Rodriguez", "Tom Wilson", "Amy Davis"]
    optimizers = ["Alex Smith", "Jordan Lee", "Casey Brown", "Taylor Garcia", "Morgan Clark"]
    business_categories = ["Restaurant", "Legal", "Home Services", "Healthcare", "Automotive", "Retail"]

    # Priority tiers for realistic distribution
    priority_tiers = [
        ("P1 - CRITICAL", 0.15),
        ("P2 - HIGH", 0.25),
        ("P3 - MEDIUM", 0.35),
        ("P4 - LOW", 0.25)
    ]

    for i in range(100):  # Create 100 sample campaigns
        campaign_id = f"CID{10000 + i}"
        partner = random.choice(partners)
        advertiser = random.choice(advertisers)

        # Realistic budget distribution
        budget = random.choice([
            random.randint(500, 1500),    # Small campaigns
            random.randint(1500, 5000),   # Medium campaigns
            random.randint(5000, 15000),  # Large campaigns
            random.randint(15000, 50000)  # Enterprise campaigns
        ])

        # Calculate derived metrics
        days_running = random.randint(30, 365)
        utilization = random.uniform(0.3, 1.2) * 100
        leads = max(1, int(budget / random.uniform(50, 200)))  # CPL between $50-200
        cpl = budget / leads if leads > 0 else budget

        # Risk scores
        churn_prob = random.uniform(0.05, 0.45)  # 5% to 45% churn probability
        risk_score = random.uniform(20, 95)

        # Priority tier based on weighted random selection
        tier_value = random.random()
        cumulative = 0
        priority_tier = "P4 - LOW"
        for tier, weight in priority_tiers:
            cumulative += weight
            if tier_value <= cumulative:
                priority_tier = tier
                break

        # Risk drivers JSON
        possible_drivers = [
            {"name": "High CPL (≥3× goal)", "impact": random.randint(5, 25)},
            {"name": "Single Product", "impact": random.randint(8, 20)},
            {"name": "Zero Leads (30d)", "impact": random.randint(10, 30)},
            {"name": "Early Account (≤90d)", "impact": random.randint(5, 15)},
            {"name": "Lead deficit (≤50% plan)", "impact": random.randint(3, 12)},
            {"name": "Good volume / CPL (protective)", "impact": random.randint(-5, -15)}
        ]

        # Select 2-4 random drivers
        selected_drivers = random.sample(possible_drivers, random.randint(2, 4))
        risk_drivers = {
            "baseline": random.randint(8, 15),
            "drivers": selected_drivers
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
            gm=random.choice(ams),  # GMs can be from AM list
            business_category=random.choice(business_categories),
            campaign_budget=Decimal(str(budget)),
            amount_spent=Decimal(str(budget * random.uniform(0.6, 1.1))),
            io_cycle=random.choice([30, 60, 90]),
            avg_cycle_length=Decimal(str(random.uniform(25, 35))),
            days_elapsed=days_running,
            days_active=days_running,
            utilization=Decimal(str(utilization)),
            running_cid_leads=leads,
            running_cid_cpl=Decimal(str(cpl)),
            cpl_goal=Decimal(str(cpl * random.uniform(0.8, 1.2))),
            bsc_cpl_avg=Decimal(str(cpl * random.uniform(0.9, 1.1))),
            effective_cpl_goal=Decimal(str(cpl * random.uniform(0.85, 1.15))),
            expected_leads_monthly=int(leads * 30 / days_running) if days_running > 0 else leads,
            expected_leads_to_date=int(leads * random.uniform(0.8, 1.2)),
            true_days_running=days_running,
            true_months_running=Decimal(str(days_running / 30)),
            cycle_label=f"Cycle {random.randint(1, 12)}",
            age_risk=Decimal(str(random.uniform(0, 100))),
            lead_risk=Decimal(str(random.uniform(0, 100))),
            cpl_risk=Decimal(str(random.uniform(0, 100))),
            util_risk=Decimal(str(random.uniform(0, 100))),
            structure_risk=Decimal(str(random.uniform(0, 100))),
            total_risk_score=Decimal(str(risk_score)),
            value_score=Decimal(str(random.uniform(10, 90))),
            final_priority_score=Decimal(str(random.uniform(0, 100))),
            priority_index=Decimal(str(random.uniform(0, 1000))),
            priority_tier=priority_tier,
            primary_issue="Lead Volume" if leads < 10 else "CPL Performance" if cpl > 150 else "Optimization",
            churn_prob_90d=Decimal(str(churn_prob)),
            churn_risk_band="High" if churn_prob > 0.3 else "Medium" if churn_prob > 0.15 else "Low",
            revenue_at_risk=Decimal(str(budget * churn_prob)),
            risk_drivers_json=risk_drivers,
            flare_score=Decimal(str(random.uniform(0, 100))),
            flare_band="Red" if risk_score > 70 else "Yellow" if risk_score > 40 else "Green",
            flare_breakdown_json={"factors": ["performance", "structure", "tenure"]},
            headline_diagnosis=f"Campaign requires attention on {random.choice(['lead volume', 'CPL optimization', 'budget utilization'])}",
            headline_severity="High" if priority_tier in ["P1 - CRITICAL", "P2 - HIGH"] else "Medium",
            diagnosis_pills=["lead_volume", "cpl_high"] if cpl > 150 else ["optimization"],
            campaign_count=1,
            true_product_count=random.randint(1, 4),
            is_safe=priority_tier in ["P4 - LOW"],
            goal_advice_json={"recommendation": "Optimize for lead volume", "confidence": random.uniform(0.7, 0.95)},
            status=random.choice(["new", "in_review", "action_taken"])
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


async def create_data_snapshot():
    """Create a data snapshot record."""
    return DataSnapshot(
        snapshot_date=date.today(),
        file_name="sample_migration.py",
        file_size_bytes=1024000,  # 1MB
        record_count=100,
        last_modified=datetime.now(),
        is_current=True
    )


async def main():
    """Main migration function."""
    print("Starting Book Data Migration...")

    try:
        # Initialize database
        await init_database()
        print("✅ Database connection established")

        async with get_db_session() as session:
            # Create sample data
            print("Creating sample campaigns...")
            campaigns = await create_sample_campaigns()

            print("Creating partner data...")
            partners = await create_sample_partners(campaigns)

            print("Creating data snapshot...")
            snapshot = await create_data_snapshot()

            # Add to database
            print("Inserting data into database...")

            # Add campaigns
            for campaign in campaigns:
                session.add(campaign)

            # Add partners
            for partner in partners:
                session.add(partner)

            # Add snapshot
            session.add(snapshot)

            # Commit transaction
            await session.commit()

            print(f"✅ Migration completed successfully!")
            print(f"   - Created {len(campaigns)} campaigns")
            print(f"   - Created {len(partners)} partners")
            print(f"   - Created 1 data snapshot")

    except Exception as e:
        print(f"❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())