"""
Analytics API Endpoints
Cross-platform analytics combining Heartbeat ETL data with Northlight benchmarks
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, date, timedelta

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.database import get_db
from core.shared import get_logger, format_currency, format_percentage

router = APIRouter()
logger = get_logger("api.analytics")

# Pydantic models
class CampaignPerformanceResponse(BaseModel):
    """Campaign performance analytics response."""
    campaign_name: str
    advertiser_name: str
    channel: str
    product: str
    actual_spend: Optional[float]
    budget: Optional[float]
    leads: int
    clicks: int
    actual_cpl: Optional[float]
    actual_cpc: Optional[float]
    conversion_rate: Optional[float]
    benchmark_cpl_median: Optional[float]
    cpl_performance_tier: str
    snapshot_date: str


class PartnerPipelineResponse(BaseModel):
    """Partner pipeline analytics response."""
    partner_name: str
    opportunity_name: str
    stage: str
    amount: Optional[float]
    close_date: Optional[str]
    probability: Optional[float]
    partner_budget: Optional[float]
    pipeline_status: str


class ExecutiveDashboardResponse(BaseModel):
    """Executive dashboard metrics response."""
    month: str
    total_campaigns: int
    total_spend: Optional[float]
    total_leads: int
    total_clicks: int
    avg_cpl: Optional[float]
    avg_cpc: Optional[float]
    excellent_campaigns: int
    good_campaigns: int
    average_campaigns: int
    below_avg_campaigns: int
    overall_conversion_rate: Optional[float]
    blended_cpl: Optional[float]
    last_updated: str


class AnalyticsFilters(BaseModel):
    """Analytics filters."""
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    channels: Optional[List[str]] = None
    products: Optional[List[str]] = None
    performance_tiers: Optional[List[str]] = None
    limit: int = Field(100, ge=1, le=1000)


# Campaign Performance Analytics
@router.get("/campaigns/performance", response_model=List[CampaignPerformanceResponse])
async def get_campaign_performance(
    start_date: Optional[date] = Query(None, description="Start date filter"),
    end_date: Optional[date] = Query(None, description="End date filter"),
    channel: Optional[str] = Query(None, description="Channel filter"),
    performance_tier: Optional[str] = Query(None, description="Performance tier filter"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    db: AsyncSession = Depends(get_db)
):
    """Get campaign performance data with benchmark comparisons."""
    try:
        # Build query with filters
        where_conditions = []
        params = {"limit": limit}

        if start_date:
            where_conditions.append("snapshot_date >= :start_date")
            params["start_date"] = start_date

        if end_date:
            where_conditions.append("snapshot_date <= :end_date")
            params["end_date"] = end_date

        if channel:
            where_conditions.append("channel = :channel")
            params["channel"] = channel

        if performance_tier:
            where_conditions.append("cpl_performance_tier = :performance_tier")
            params["performance_tier"] = performance_tier

        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

        query = f"""
        SELECT
            campaign_name,
            advertiser_name,
            channel,
            product,
            actual_spend,
            budget,
            leads,
            clicks,
            actual_cpl,
            actual_cpc,
            conversion_rate,
            benchmark_cpl_median,
            cpl_performance_tier,
            snapshot_date
        FROM unified_analytics.campaign_performance
        {where_clause}
        ORDER BY snapshot_date DESC, actual_spend DESC NULLS LAST
        LIMIT :limit
        """

        result = await db.execute(text(query), params)
        rows = result.fetchall()

        campaigns = []
        for row in rows:
            campaigns.append(CampaignPerformanceResponse(
                campaign_name=row.campaign_name or "",
                advertiser_name=row.advertiser_name or "",
                channel=row.channel or "",
                product=row.product or "",
                actual_spend=float(row.actual_spend) if row.actual_spend else None,
                budget=float(row.budget) if row.budget else None,
                leads=int(row.leads) if row.leads else 0,
                clicks=int(row.clicks) if row.clicks else 0,
                actual_cpl=float(row.actual_cpl) if row.actual_cpl else None,
                actual_cpc=float(row.actual_cpc) if row.actual_cpc else None,
                conversion_rate=float(row.conversion_rate) if row.conversion_rate else None,
                benchmark_cpl_median=float(row.benchmark_cpl_median) if row.benchmark_cpl_median else None,
                cpl_performance_tier=row.cpl_performance_tier or "unknown",
                snapshot_date=row.snapshot_date.isoformat() if row.snapshot_date else ""
            ))

        return campaigns

    except Exception as e:
        logger.error(f"Failed to get campaign performance: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve campaign performance data")


@router.get("/campaigns/summary")
async def get_campaign_summary(
    start_date: Optional[date] = Query(None, description="Start date filter"),
    end_date: Optional[date] = Query(None, description="End date filter"),
    db: AsyncSession = Depends(get_db)
):
    """Get campaign performance summary statistics."""
    try:
        # Default to last 30 days if no dates provided
        if not end_date:
            end_date = date.today()
        if not start_date:
            start_date = end_date - timedelta(days=30)

        query = """
        SELECT
            COUNT(*) as total_campaigns,
            COUNT(DISTINCT advertiser_name) as unique_advertisers,
            COUNT(DISTINCT channel) as unique_channels,
            SUM(actual_spend) as total_spend,
            SUM(leads) as total_leads,
            SUM(clicks) as total_clicks,
            AVG(actual_cpl) as avg_cpl,
            AVG(actual_cpc) as avg_cpc,
            AVG(conversion_rate) as avg_conversion_rate,
            COUNT(CASE WHEN cpl_performance_tier = 'excellent' THEN 1 END) as excellent_count,
            COUNT(CASE WHEN cpl_performance_tier = 'good' THEN 1 END) as good_count,
            COUNT(CASE WHEN cpl_performance_tier = 'average' THEN 1 END) as average_count,
            COUNT(CASE WHEN cpl_performance_tier = 'below_average' THEN 1 END) as below_avg_count
        FROM unified_analytics.campaign_performance
        WHERE snapshot_date BETWEEN :start_date AND :end_date
        """

        result = await db.execute(text(query), {
            "start_date": start_date,
            "end_date": end_date
        })

        row = result.fetchone()

        return {
            "period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            },
            "summary": {
                "total_campaigns": row.total_campaigns or 0,
                "unique_advertisers": row.unique_advertisers or 0,
                "unique_channels": row.unique_channels or 0,
                "total_spend": float(row.total_spend) if row.total_spend else 0,
                "total_leads": int(row.total_leads) if row.total_leads else 0,
                "total_clicks": int(row.total_clicks) if row.total_clicks else 0,
                "avg_cpl": float(row.avg_cpl) if row.avg_cpl else None,
                "avg_cpc": float(row.avg_cpc) if row.avg_cpc else None,
                "avg_conversion_rate": float(row.avg_conversion_rate) if row.avg_conversion_rate else None
            },
            "performance_distribution": {
                "excellent": row.excellent_count or 0,
                "good": row.good_count or 0,
                "average": row.average_count or 0,
                "below_average": row.below_avg_count or 0
            }
        }

    except Exception as e:
        logger.error(f"Failed to get campaign summary: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve campaign summary")


# Partner Pipeline Analytics
@router.get("/partners/pipeline", response_model=List[PartnerPipelineResponse])
async def get_partner_pipeline(
    stage: Optional[str] = Query(None, description="Pipeline stage filter"),
    partner_name: Optional[str] = Query(None, description="Partner name filter"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    db: AsyncSession = Depends(get_db)
):
    """Get partner pipeline health data."""
    try:
        where_conditions = []
        params = {"limit": limit}

        if stage:
            where_conditions.append("stage = :stage")
            params["stage"] = stage

        if partner_name:
            where_conditions.append("partner_name ILIKE :partner_name")
            params["partner_name"] = f"%{partner_name}%"

        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

        query = f"""
        SELECT
            partner_name,
            opportunity_name,
            stage,
            amount,
            close_date,
            probability,
            partner_budget,
            pipeline_status
        FROM unified_analytics.partner_pipeline_health
        {where_clause}
        ORDER BY amount DESC NULLS LAST, close_date ASC NULLS LAST
        LIMIT :limit
        """

        result = await db.execute(text(query), params)
        rows = result.fetchall()

        pipeline = []
        for row in rows:
            pipeline.append(PartnerPipelineResponse(
                partner_name=row.partner_name or "",
                opportunity_name=row.opportunity_name or "",
                stage=row.stage or "",
                amount=float(row.amount) if row.amount else None,
                close_date=row.close_date.isoformat() if row.close_date else None,
                probability=float(row.probability) if row.probability else None,
                partner_budget=float(row.partner_budget) if row.partner_budget else None,
                pipeline_status=row.pipeline_status or "unknown"
            ))

        return pipeline

    except Exception as e:
        logger.error(f"Failed to get partner pipeline: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve partner pipeline data")


# Executive Dashboard
@router.get("/executive/dashboard", response_model=List[ExecutiveDashboardResponse])
async def get_executive_dashboard(
    months: int = Query(12, ge=1, le=24, description="Number of months to include"),
    db: AsyncSession = Depends(get_db)
):
    """Get executive dashboard metrics by month."""
    try:
        # Refresh materialized view first
        await db.execute(text("SELECT unified_analytics.refresh_dashboards()"))

        query = """
        SELECT
            month,
            total_campaigns,
            total_spend,
            total_leads,
            total_clicks,
            avg_cpl,
            avg_cpc,
            excellent_campaigns,
            good_campaigns,
            average_campaigns,
            below_avg_campaigns,
            overall_conversion_rate,
            blended_cpl,
            last_updated
        FROM unified_analytics.executive_dashboard
        ORDER BY month DESC
        LIMIT :months
        """

        result = await db.execute(text(query), {"months": months})
        rows = result.fetchall()

        dashboard_data = []
        for row in rows:
            dashboard_data.append(ExecutiveDashboardResponse(
                month=row.month.strftime("%Y-%m") if row.month else "",
                total_campaigns=row.total_campaigns or 0,
                total_spend=float(row.total_spend) if row.total_spend else None,
                total_leads=row.total_leads or 0,
                total_clicks=row.total_clicks or 0,
                avg_cpl=float(row.avg_cpl) if row.avg_cpl else None,
                avg_cpc=float(row.avg_cpc) if row.avg_cpc else None,
                excellent_campaigns=row.excellent_campaigns or 0,
                good_campaigns=row.good_campaigns or 0,
                average_campaigns=row.average_campaigns or 0,
                below_avg_campaigns=row.below_avg_campaigns or 0,
                overall_conversion_rate=float(row.overall_conversion_rate) if row.overall_conversion_rate else None,
                blended_cpl=float(row.blended_cpl) if row.blended_cpl else None,
                last_updated=row.last_updated.isoformat() if row.last_updated else ""
            ))

        return dashboard_data

    except Exception as e:
        logger.error(f"Failed to get executive dashboard: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve executive dashboard")


# Trend Analysis
@router.get("/trends/performance")
async def get_performance_trends(
    metric: str = Query("cpl", description="Metric to analyze (cpl, cpc, conversion_rate)"),
    period: str = Query("month", description="Time period (day, week, month)"),
    channel: Optional[str] = Query(None, description="Channel filter"),
    db: AsyncSession = Depends(get_db)
):
    """Get performance trends over time."""
    try:
        # Validate metric
        valid_metrics = ["cpl", "cpc", "conversion_rate", "spend"]
        if metric not in valid_metrics:
            raise HTTPException(status_code=400, detail=f"Invalid metric. Must be one of: {valid_metrics}")

        # Build query based on period
        if period == "day":
            date_trunc = "DATE_TRUNC('day', snapshot_date)"
        elif period == "week":
            date_trunc = "DATE_TRUNC('week', snapshot_date)"
        else:  # month
            date_trunc = "DATE_TRUNC('month', snapshot_date)"

        # Build metric selection
        if metric == "cpl":
            metric_select = "AVG(actual_cpl) as metric_value"
        elif metric == "cpc":
            metric_select = "AVG(actual_cpc) as metric_value"
        elif metric == "conversion_rate":
            metric_select = "AVG(conversion_rate) as metric_value"
        else:  # spend
            metric_select = "SUM(actual_spend) as metric_value"

        where_clause = ""
        params = {}
        if channel:
            where_clause = "WHERE channel = :channel"
            params["channel"] = channel

        query = f"""
        SELECT
            {date_trunc} as period,
            {metric_select},
            COUNT(*) as campaign_count,
            COUNT(DISTINCT advertiser_name) as advertiser_count
        FROM unified_analytics.campaign_performance
        {where_clause}
        GROUP BY {date_trunc}
        ORDER BY period DESC
        LIMIT 24
        """

        result = await db.execute(text(query), params)
        rows = result.fetchall()

        trends = []
        for row in rows:
            trends.append({
                "period": row.period.isoformat() if row.period else "",
                "metric_value": float(row.metric_value) if row.metric_value else None,
                "campaign_count": row.campaign_count or 0,
                "advertiser_count": row.advertiser_count or 0
            })

        return {
            "metric": metric,
            "period": period,
            "channel": channel,
            "trends": trends
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get performance trends: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve performance trends")


# Real-time Data Refresh
@router.post("/refresh/materialized-views")
async def refresh_materialized_views(
    db: AsyncSession = Depends(get_db)
):
    """Refresh materialized views for real-time analytics."""
    try:
        await db.execute(text("SELECT unified_analytics.refresh_dashboards()"))
        await db.commit()

        return {
            "message": "Materialized views refreshed successfully",
            "refreshed_at": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        logger.error(f"Failed to refresh materialized views: {e}")
        raise HTTPException(status_code=500, detail="Failed to refresh materialized views")


# Data Export
@router.get("/export/campaigns")
async def export_campaign_data(
    format: str = Query("csv", description="Export format (csv, json)"),
    start_date: Optional[date] = Query(None, description="Start date filter"),
    end_date: Optional[date] = Query(None, description="End date filter"),
    db: AsyncSession = Depends(get_db)
):
    """Export campaign performance data."""
    try:
        # This would implement data export functionality
        # For now, return a placeholder
        return {
            "message": "Data export functionality not yet implemented",
            "format": format,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None
        }

    except Exception as e:
        logger.error(f"Failed to export campaign data: {e}")
        raise HTTPException(status_code=500, detail="Failed to export campaign data")