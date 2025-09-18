"""
Book System API Router
Handles campaign health, risk assessment, and partner management endpoints
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc, and_, or_
from sqlalchemy.orm import selectinload

from core.database import get_db
from core.models.book import Campaign, Partner, PartnerOpportunity, DataSnapshot

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/book", tags=["book"])


@router.get("/summary")
async def get_summary(
    view: str = Query("optimizer"),
    partner: Optional[str] = Query(None),
    advertiser: Optional[str] = Query(None),
    am: Optional[str] = Query(None),
    optimizer: Optional[str] = Query(None),
    gm: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Get summary statistics for the book dashboard."""
    try:
        # Build base query
        query = select(Campaign)

        # Apply filters
        filters = []
        if partner:
            filters.append(Campaign.partner_name == partner)
        if advertiser:
            filters.append(Campaign.advertiser_name == advertiser)
        if am:
            filters.append(Campaign.am == am)
        if optimizer:
            filters.append(Campaign.optimizer == optimizer)
        if gm:
            filters.append(Campaign.gm == gm)

        if filters:
            query = query.where(and_(*filters))

        # Execute query
        result = await db.execute(query)
        campaigns = result.scalars().all()

        # Calculate summary statistics
        total_accounts = len(campaigns)

        # Priority counts
        p1_critical = sum(1 for c in campaigns if c.priority_tier == "P1 - CRITICAL")
        p2_high = sum(1 for c in campaigns if c.priority_tier == "P2 - HIGH")

        # Budget at risk calculation
        budget_at_risk = sum(
            float(c.revenue_at_risk or 0) for c in campaigns
            if c.revenue_at_risk
        )

        # If no revenue_at_risk data, fall back to campaign budgets for critical/high priority
        if budget_at_risk == 0:
            budget_at_risk = sum(
                float(c.campaign_budget or 0) for c in campaigns
                if c.priority_tier in ["P1 - CRITICAL", "P2 - HIGH"] and c.campaign_budget
            )

        # Get facet data for filters
        partners = sorted(set(c.partner_name for c in campaigns if c.partner_name))
        ams = sorted(set(c.am for c in campaigns if c.am))
        optimizers = sorted(set(c.optimizer for c in campaigns if c.optimizer))
        gms = sorted(set(c.gm for c in campaigns if c.gm))

        return {
            "counts": {
                "total_accounts": total_accounts,
                "p1_critical": p1_critical,
                "p2_high": p2_high,
            },
            "budget_at_risk": budget_at_risk,
            "facets": {
                "partners": partners,
                "ams": ams,
                "optimizers": optimizers,
                "gms": gms,
            },
        }

    except Exception as e:
        logger.error(f"Error in get_summary: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get summary: {str(e)}")


@router.get("/all")
async def get_all_accounts(
    view: str = Query("optimizer"),
    partner: Optional[str] = Query(None),
    advertiser: Optional[str] = Query(None),
    am: Optional[str] = Query(None),
    optimizer: Optional[str] = Query(None),
    gm: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
) -> List[Dict[str, Any]]:
    """Get all campaigns/accounts with optional filtering."""
    try:
        # Build base query
        query = select(Campaign)

        # Apply filters
        filters = []
        if partner:
            filters.append(Campaign.partner_name == partner)
        if advertiser:
            filters.append(Campaign.advertiser_name == advertiser)
        if am:
            filters.append(Campaign.am == am)
        if optimizer:
            filters.append(Campaign.optimizer == optimizer)
        if gm:
            filters.append(Campaign.gm == gm)

        if filters:
            query = query.where(and_(*filters))

        # Sort by priority
        query = query.order_by(
            desc(Campaign.priority_index),
            desc(Campaign.revenue_at_risk),
            desc(Campaign.churn_prob_90d),
            Campaign.campaign_id
        )

        # Execute query
        result = await db.execute(query)
        campaigns = result.scalars().all()

        # Convert to dictionaries
        return [campaign.to_dict() for campaign in campaigns]

    except Exception as e:
        logger.error(f"Error in get_all_accounts: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get accounts: {str(e)}")


@router.get("/accounts")
async def get_accounts(
    view: str = Query("optimizer"),
    partner: Optional[str] = Query(None),
    advertiser: Optional[str] = Query(None),
    am: Optional[str] = Query(None),
    optimizer: Optional[str] = Query(None),
    gm: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Get accounts in the format expected by the frontend (matches /api/book/accounts)."""
    try:
        # Reuse the existing get_all_accounts logic
        accounts = await get_all_accounts(view, partner, advertiser, am, optimizer, gm, db)

        # Return in the format the frontend expects
        return {
            "accounts": accounts,
            "total": len(accounts),
            "view": view,
            "filters_applied": {
                "partner": partner,
                "advertiser": advertiser,
                "am": am,
                "optimizer": optimizer,
                "gm": gm
            }
        }

    except Exception as e:
        logger.error(f"Error in get_accounts: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get accounts: {str(e)}")


@router.get("/actions")
async def get_actions(
    campaign_id: str,
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Get recommended actions for a specific campaign."""
    try:
        # Get campaign
        query = select(Campaign).where(Campaign.campaign_id == campaign_id)
        result = await db.execute(query)
        campaign = result.scalar_one_or_none()

        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")

        actions = []

        # Extract drivers from risk_drivers_json
        if campaign.risk_drivers_json:
            drivers = campaign.risk_drivers_json.get("drivers", [])
            impacts = {d.get("name", ""): int(d.get("points", 0)) for d in drivers}  # FIXED: was "impact", should be "points"

            # Generate actions based on risk drivers
            if impacts.get("Single Product", 0) > 0:
                actions.append({
                    "title": "Add a second product (e.g., SEO or Website)",
                    "impact": impacts["Single Product"],
                    "cta": "Start Proposal →"
                })

            if impacts.get("Zero Leads (30d)", 0) > 0:
                actions.append({
                    "title": "Fix tracking & lead quality audit",
                    "impact": impacts["Zero Leads (30d)"],
                    "cta": "Open Checklist →"
                })

            # CPL-related actions
            for key, value in impacts.items():
                if value > 0 and any(cpl_term in key for cpl_term in ["High CPL", "Elevated CPL", "CPL above goal"]):
                    actions.append({
                        "title": "Budget/keyword optimization for lead volume",
                        "impact": value,
                        "cta": "Open Planner →"
                    })
                    break

            # Tenure-related actions
            tenure_impact = max(
                impacts.get("Early Account (≤90d)", 0),
                impacts.get("Early Tenure (≤3m)", 0)
            )
            if tenure_impact > 0:
                actions.append({
                    "title": "Set expectations / launch plan call",
                    "impact": tenure_impact,
                    "cta": "Schedule Call →"
                })

            # Sort by impact and limit to top 3
            actions = sorted(actions, key=lambda x: x["impact"], reverse=True)[:3]

        return {
            "campaign_id": campaign_id,
            "actions": actions
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_actions: {e}")
        # Return empty actions rather than error to prevent UI breakage
        return {
            "campaign_id": campaign_id,
            "actions": []
        }


@router.get("/partners")
async def get_partners(
    playbook: str = Query("seo_dash"),
    db: AsyncSession = Depends(get_db)
) -> List[Dict[str, Any]]:
    """Get partner summary cards for the growth dashboard."""
    try:
        # Get partners with their metrics
        query = select(Partner).where(Partner.playbook == playbook)
        result = await db.execute(query)
        partners = result.scalars().all()

        # If no partners exist, generate them from campaign data
        if not partners:
            await _generate_partner_data(db, playbook)
            result = await db.execute(query)
            partners = result.scalars().all()

        return [partner.to_dict() for partner in partners]

    except Exception as e:
        logger.error(f"Error in get_partners: {e}")
        # Return empty list to prevent UI breakage
        return []


@router.get("/partners/{partner_name}/opportunities")
async def get_partner_opportunities(
    partner_name: str,
    playbook: str = Query("seo_dash"),
    cid: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Get detailed opportunities for a specific partner."""
    try:
        # Get partner opportunities
        query = select(PartnerOpportunity).where(
            and_(
                PartnerOpportunity.partner_name == partner_name,
                PartnerOpportunity.playbook == playbook
            )
        )
        result = await db.execute(query)
        opportunity = result.scalar_one_or_none()

        if not opportunity:
            # Generate opportunities if they don't exist
            await _generate_partner_opportunities(db, partner_name, playbook)
            result = await db.execute(query)
            opportunity = result.scalar_one_or_none()

        if not opportunity:
            raise HTTPException(status_code=404, detail=f"Partner not found: {partner_name}")

        response = opportunity.to_dict()

        # Add churn waterfall data if CID is requested
        if cid:
            campaign_query = select(Campaign).where(Campaign.campaign_id == cid)
            campaign_result = await db.execute(campaign_query)
            campaign = campaign_result.scalar_one_or_none()

            if campaign and campaign.risk_drivers_json:
                # Build waterfall data
                churn_prob = float(campaign.churn_prob_90d or 0)
                total_pct = int(round(churn_prob * 100))

                drivers_data = campaign.risk_drivers_json
                if drivers_data and isinstance(drivers_data, dict):
                    waterfall = {
                        "total_pct": total_pct,
                        "baseline_pp": drivers_data.get("baseline", 11),
                        "drivers": []
                    }

                    for driver in drivers_data.get("drivers", []):
                        waterfall["drivers"].append({
                            "name": driver.get("name", "Risk Factor"),
                            "points": int(driver.get("points", 0)),  # FIXED: was "impact", should be "points"
                            "is_controllable": driver.get("is_controllable", False),  # Use actual field
                            "explanation": driver.get("explanation", "Risk factor affecting churn probability."),
                            "lift_x": driver.get("lift_x", 1.0)
                        })

                    response["churn_waterfall"] = waterfall

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_partner_opportunities: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load partner opportunities: {str(e)}")


@router.get("/partners/{partner_name}/advertisers")
async def get_partner_advertisers(
    partner_name: str,
    db: AsyncSession = Depends(get_db)
) -> List[Dict[str, Any]]:
    """Get all advertisers for a specific partner."""
    try:
        # Get campaigns for this partner
        query = select(Campaign).where(Campaign.partner_name == partner_name)
        result = await db.execute(query)
        campaigns = result.scalars().all()

        if not campaigns:
            raise HTTPException(status_code=404, detail=f"Partner not found: {partner_name}")

        # Group by advertiser
        advertisers_dict = {}
        for campaign in campaigns:
            if campaign.advertiser_name:
                if campaign.advertiser_name not in advertisers_dict:
                    advertisers_dict[campaign.advertiser_name] = {
                        "name": campaign.advertiser_name,
                        "campaign_count": 0,
                        "total_budget": 0,
                        "products": set()
                    }

                adv = advertisers_dict[campaign.advertiser_name]
                adv["campaign_count"] += 1
                adv["total_budget"] += float(campaign.campaign_budget or 0)

                # Add product types (simplified)
                if campaign.business_category:
                    adv["products"].add(campaign.business_category)

        # Convert to list and sort by budget
        advertisers = []
        for adv_data in advertisers_dict.values():
            adv_data["products"] = list(adv_data["products"])
            advertisers.append(adv_data)

        return sorted(advertisers, key=lambda x: x["total_budget"], reverse=True)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_partner_advertisers: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load advertisers: {str(e)}")


@router.get("/metadata")
async def get_metadata(db: AsyncSession = Depends(get_db)) -> Dict[str, Any]:
    """Get metadata about the current dataset including data freshness information."""
    try:
        # Get the current snapshot
        query = select(DataSnapshot).where(DataSnapshot.is_current == True).order_by(desc(DataSnapshot.snapshot_date))
        result = await db.execute(query)
        current_snapshot = result.scalar_one_or_none()

        if current_snapshot:
            return current_snapshot.to_dict()

        # If no current snapshot, get the latest one
        query = select(DataSnapshot).order_by(desc(DataSnapshot.snapshot_date))
        result = await db.execute(query)
        latest_snapshot = result.scalar_one_or_none()

        if latest_snapshot:
            return latest_snapshot.to_dict()

        # If no snapshots exist, return default metadata
        return {
            "data_snapshot_date": datetime.now().strftime("%Y-%m-%d"),
            "last_modified": datetime.now().isoformat(),
            "last_modified_display": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "file_name": "database",
            "file_size_bytes": 0,
            "record_count": 0,
            "available_snapshots": [],
            "is_current": True
        }

    except Exception as e:
        logger.error(f"Error in get_metadata: {e}")
        return {
            "error": str(e),
            "data_snapshot_date": None,
            "last_modified": None,
            "last_modified_display": "Unknown",
            "file_name": "Unknown",
            "file_size_bytes": 0,
            "record_count": 0,
            "available_snapshots": [],
            "is_current": False
        }


async def _generate_partner_data(db: AsyncSession, playbook: str):
    """Generate partner summary data from campaign data."""
    try:
        # Get all campaigns with partner data
        query = select(Campaign).where(Campaign.partner_name.isnot(None))
        result = await db.execute(query)
        campaigns = result.scalars().all()

        # Group by partner
        partner_data = {}
        for campaign in campaigns:
            partner_name = campaign.partner_name
            if partner_name not in partner_data:
                partner_data[partner_name] = {
                    "total_budget": 0,
                    "single_product_count": 0,
                    "two_product_count": 0,
                    "three_plus_product_count": 0,
                    "cross_sell_ready_count": 0,
                    "upsell_ready_count": 0
                }

            data = partner_data[partner_name]
            data["total_budget"] += float(campaign.campaign_budget or 0)

            # Count by product count (simplified logic)
            product_count = campaign.true_product_count or 1
            if product_count == 1:
                data["single_product_count"] += 1
                data["cross_sell_ready_count"] += 1  # Assume single product ready for cross-sell
            elif product_count == 2:
                data["two_product_count"] += 1
            else:
                data["three_plus_product_count"] += 1

            # Simple upsell logic based on performance
            if campaign.priority_tier in ["P3 - MEDIUM", "P4 - LOW"]:
                data["upsell_ready_count"] += 1

        # Create partner records
        for partner_name, data in partner_data.items():
            partner = Partner(
                partner_name=partner_name,
                playbook=playbook,
                total_budget=data["total_budget"],
                single_product_count=data["single_product_count"],
                two_product_count=data["two_product_count"],
                three_plus_product_count=data["three_plus_product_count"],
                cross_sell_ready_count=data["cross_sell_ready_count"],
                upsell_ready_count=data["upsell_ready_count"]
            )
            db.add(partner)

        await db.commit()

    except Exception as e:
        logger.error(f"Error generating partner data: {e}")
        await db.rollback()


async def _generate_partner_opportunities(db: AsyncSession, partner_name: str, playbook: str):
    """Generate partner opportunity data from campaign data."""
    try:
        # Get partner record
        partner_query = select(Partner).where(Partner.partner_name == partner_name)
        partner_result = await db.execute(partner_query)
        partner = partner_result.scalar_one_or_none()

        if not partner:
            return

        # Create basic opportunity structure
        opportunity = PartnerOpportunity(
            partner_id=partner.id,
            partner_name=partner_name,
            playbook=playbook,
            single_ready=[],
            two_ready=[],
            three_plus_ready=[],
            scale_ready=[],
            too_low=[],
            playbook_config={
                "label": playbook.replace('_', ' ').title(),
                "elements": ["Search", "SEO", "DASH"],
                "min_sem": 2500
            }
        )

        db.add(opportunity)
        await db.commit()

    except Exception as e:
        logger.error(f"Error generating partner opportunities: {e}")
        await db.rollback()