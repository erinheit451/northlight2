"""
Campaign Trajectory API Router
Handles historical CPL trends and performance analysis
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
import statistics

from core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trajectory", tags=["trajectory"])


@router.get("/campaign/{campaign_id}")
async def get_campaign_trajectory(
    campaign_id: str,
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Get trajectory data for a specific campaign."""
    try:
        # Get current campaign data
        current_query = text("""
            SELECT campaign_id, running_cid_cpl, cpl_goal, days_elapsed, partner_name, campaign_name
            FROM book.campaigns
            WHERE campaign_id = :campaign_id
        """)

        current_result = await db.execute(current_query, {"campaign_id": campaign_id})
        current_data = current_result.fetchone()

        if not current_data:
            raise HTTPException(status_code=404, detail="Campaign not found")

        current_cpl = float(current_data.running_cid_cpl) if current_data.running_cid_cpl else None
        goal_cpl = float(current_data.cpl_goal) if current_data.cpl_goal else None
        campaign_age_days = current_data.days_elapsed or 0
        partner_name = current_data.partner_name
        campaign_name = current_data.campaign_name

        # Get agreed CPL data for additional context
        agreed_query = text("""
            SELECT cpl_agreed, cpl_mcid, cycle_start_date
            FROM heartbeat_etl.agreed_cpl_performance
            WHERE campaign_id = :campaign_id
        """)

        agreed_result = await db.execute(agreed_query, {"campaign_id": campaign_id})
        agreed_data = agreed_result.fetchone()

        # Primary: Try direct campaign_id match in historical data
        # Convert campaign_id to integer for the bigint column
        try:
            campaign_id_int = int(campaign_id)
        except ValueError:
            campaign_id_int = None

        historical_query = text("""
            SELECT report_month, cost_per_lead, leads, spend
            FROM heartbeat_etl.spend_revenue_performance_current
            WHERE campaign_id = :campaign_id
              AND cost_per_lead IS NOT NULL
            ORDER BY report_month DESC
            LIMIT 12
        """)

        historical_result = await db.execute(historical_query, {"campaign_id": campaign_id_int})
        historical_data = historical_result.fetchall()

        # Fallback: Try business name + campaign name fuzzy match
        if not historical_data and partner_name and campaign_name:
            # Extract key terms from campaign name
            campaign_keywords = _extract_campaign_keywords(campaign_name)

            fallback_query = text("""
                SELECT report_month, cost_per_lead, leads, spend
                FROM heartbeat_etl.spend_revenue_performance_current
                WHERE business_name = :partner_name
                  AND cost_per_lead IS NOT NULL
                  AND (:keywords = '' OR campaign_name ILIKE '%' || :keywords || '%')
                ORDER BY report_month DESC
                LIMIT 12
            """)

            historical_result = await db.execute(
                fallback_query,
                {
                    "partner_name": partner_name,
                    "keywords": campaign_keywords
                }
            )
            historical_data = historical_result.fetchall()

        # Process historical data
        historical_points = [
            {
                "month": row.report_month.strftime("%Y-%m") if row.report_month else None,
                "cpl": float(row.cost_per_lead) if row.cost_per_lead else None,
                "leads": int(row.leads) if row.leads else 0,
                "spend": float(row.spend) if row.spend else 0
            }
            for row in historical_data
            if row.cost_per_lead is not None
        ]

        # Calculate trajectory metrics
        trajectory_data = _calculate_trajectory_metrics(
            historical_points, current_cpl, goal_cpl, campaign_age_days
        )

        # Add additional context
        trajectory_data.update({
            "campaign_id": campaign_id,
            "campaign_name": campaign_name,
            "partner_name": partner_name,
            "agreed_cpl": float(agreed_data.cpl_agreed) if agreed_data and agreed_data.cpl_agreed else None,
            "mcid_cpl": float(agreed_data.cpl_mcid) if agreed_data and agreed_data.cpl_mcid else None,
            "cycle_start": agreed_data.cycle_start_date.strftime("%Y-%m-%d") if agreed_data and agreed_data.cycle_start_date else None
        })

        return trajectory_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting trajectory for campaign {campaign_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get trajectory data: {str(e)}")


def _extract_campaign_keywords(campaign_name: str) -> str:
    """Extract key search terms from campaign name."""
    if not campaign_name:
        return ""

    # Split by common delimiters and take meaningful parts
    parts = campaign_name.replace("|", " ").replace("-", " ").split()

    # Filter out common words and take the most specific terms
    stopwords = {"search", "sem", "campaign", "the", "and", "or", "for", "of", "in", "on", "at"}
    keywords = [part.strip() for part in parts if part.lower().strip() not in stopwords and len(part) > 2]

    # Return the first meaningful keyword
    return keywords[0] if keywords else ""


def _calculate_trajectory_metrics(
    historical_points: List[Dict],
    current_cpl: Optional[float],
    goal_cpl: Optional[float],
    campaign_age_days: int
) -> Dict[str, Any]:
    """Calculate trajectory metrics from historical data."""

    # Assess data quality
    data_quality = _assess_data_quality(len(historical_points), campaign_age_days)

    if data_quality == "none":
        return {
            "current_cpl": current_cpl,
            "goal_cpl": goal_cpl,
            "campaign_age_days": campaign_age_days,
            "data_quality": data_quality,
            "trend": {"direction": "insufficient_data", "percentage": None, "period_days": None},
            "sparkline": [],
            "metrics": {"last_30d_avg": None, "last_90d_avg": None, "ytd_avg": None},
            "data_points": 0,
            "earliest_data": None
        }

    # Extract CPL values for calculations
    cpl_values = [point["cpl"] for point in historical_points if point["cpl"] is not None]

    if not cpl_values:
        return {
            "current_cpl": current_cpl,
            "goal_cpl": goal_cpl,
            "campaign_age_days": campaign_age_days,
            "data_quality": data_quality,
            "trend": {"direction": "insufficient_data", "percentage": None, "period_days": None},
            "sparkline": [],
            "metrics": {"last_30d_avg": None, "last_90d_avg": None, "ytd_avg": None},
            "data_points": 0,
            "earliest_data": None
        }

    # Calculate trend
    trend = _calculate_trend_direction(cpl_values, current_cpl)

    # Generate sparkline
    sparkline = _generate_sparkline(cpl_values[-7:])  # Last 7 points

    # Calculate time-based metrics
    metrics = _calculate_time_metrics(historical_points)

    return {
        "current_cpl": current_cpl,
        "goal_cpl": goal_cpl,
        "campaign_age_days": campaign_age_days,
        "data_quality": data_quality,
        "trend": trend,
        "sparkline": sparkline,
        "metrics": metrics,
        "data_points": len(historical_points),
        "earliest_data": historical_points[-1]["month"] if historical_points else None
    }


def _assess_data_quality(data_points: int, campaign_age_days: int) -> str:
    """Assess the quality of available historical data."""
    if data_points == 0:
        return "none"
    elif data_points < 3 or campaign_age_days < 60:
        return "limited"
    elif data_points < 6:
        return "moderate"
    else:
        return "rich"


def _calculate_trend_direction(cpl_values: List[float], current_cpl: Optional[float]) -> Dict[str, Any]:
    """Calculate trend direction and percentage change."""
    if len(cpl_values) < 2:
        return {"direction": "insufficient_data", "percentage": None, "period_days": None, "confidence": "low"}

    # Compare recent vs older periods
    recent_period = cpl_values[:min(3, len(cpl_values)//2)]  # Most recent 3 or half
    older_period = cpl_values[len(recent_period):]  # Older data

    if not recent_period or not older_period:
        return {"direction": "insufficient_data", "percentage": None, "period_days": None, "confidence": "low"}

    recent_avg = statistics.mean(recent_period)
    older_avg = statistics.mean(older_period)

    # Calculate percentage change (negative = improvement for CPL)
    pct_change = ((recent_avg - older_avg) / older_avg) * 100

    # Determine direction
    if pct_change > 10:  # CPL increased significantly
        direction = "declining"
    elif pct_change < -10:  # CPL decreased significantly
        direction = "improving"
    else:
        direction = "stable"

    # Confidence based on data points
    confidence = "high" if len(cpl_values) > 6 else "medium" if len(cpl_values) > 3 else "low"

    return {
        "direction": direction,
        "percentage": abs(pct_change),
        "period_days": len(cpl_values) * 30,  # Approximate days based on monthly data
        "confidence": confidence
    }


def _generate_sparkline(cpl_values: List[float]) -> List[int]:
    """Generate normalized sparkline values (0-100)."""
    if len(cpl_values) < 2:
        return []

    min_cpl = min(cpl_values)
    max_cpl = max(cpl_values)

    if min_cpl == max_cpl:
        return [50] * len(cpl_values)  # Flat line

    # Normalize to 0-100 scale
    return [
        int(round(((cpl - min_cpl) / (max_cpl - min_cpl)) * 100))
        for cpl in cpl_values
    ]


def _calculate_time_metrics(historical_points: List[Dict]) -> Dict[str, Optional[float]]:
    """Calculate time-based average CPL metrics."""
    if not historical_points:
        return {"last_30d_avg": None, "last_90d_avg": None, "ytd_avg": None}

    now = datetime.now()

    # For monthly data, approximate time periods
    last_30d_data = []
    last_90d_data = []
    ytd_data = []

    for point in historical_points:
        if not point["month"] or not point["cpl"]:
            continue

        try:
            point_date = datetime.strptime(point["month"], "%Y-%m")
            days_ago = (now - point_date).days

            # Group by approximate time periods
            if days_ago <= 45:  # Approximate last 30 days (allowing for monthly granularity)
                last_30d_data.append(point["cpl"])
            if days_ago <= 105:  # Approximate last 90 days
                last_90d_data.append(point["cpl"])
            if point_date.year == now.year:  # Year to date
                ytd_data.append(point["cpl"])

        except ValueError:
            continue

    return {
        "last_30d_avg": statistics.mean(last_30d_data) if last_30d_data else None,
        "last_90d_avg": statistics.mean(last_90d_data) if last_90d_data else None,
        "ytd_avg": statistics.mean(ytd_data) if ytd_data else None
    }# Updated Thu, Sep 18, 2025 11:37:19 AM
# Force reload
