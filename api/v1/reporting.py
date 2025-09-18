"""
Reporting API Endpoints
Automated report generation and export functionality
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, date
from io import BytesIO, StringIO
import csv
import json

from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.database import get_db
from core.shared import get_logger, format_currency, format_percentage

router = APIRouter()
logger = get_logger("api.reporting")

# Pydantic models
class ReportRequest(BaseModel):
    """Base report request model."""
    name: str = Field(..., description="Report name")
    start_date: Optional[date] = Field(None, description="Start date filter")
    end_date: Optional[date] = Field(None, description="End date filter")
    filters: Optional[Dict[str, Any]] = Field(None, description="Additional filters")
    format: str = Field("json", description="Output format (json, csv, excel)")


class ScheduledReportRequest(BaseModel):
    """Scheduled report request model."""
    name: str = Field(..., description="Report name")
    schedule: str = Field(..., description="Cron schedule expression")
    recipients: List[str] = Field(..., description="Email recipients")
    report_config: Dict[str, Any] = Field(..., description="Report configuration")
    enabled: bool = Field(True, description="Whether the report is enabled")


class ReportResponse(BaseModel):
    """Report response model."""
    report_id: str
    name: str
    generated_at: str
    format: str
    row_count: int
    data: Optional[List[Dict[str, Any]]] = None
    download_url: Optional[str] = None


# Report Templates
REPORT_TEMPLATES = {
    "campaign_performance": {
        "name": "Campaign Performance Report",
        "description": "Detailed campaign metrics with benchmark comparisons",
        "query": """
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
        WHERE snapshot_date BETWEEN :start_date AND :end_date
        ORDER BY snapshot_date DESC, actual_spend DESC NULLS LAST
        """,
        "default_filters": {
            "start_date": "30_days_ago",
            "end_date": "today"
        }
    },
    "partner_pipeline": {
        "name": "Partner Pipeline Report",
        "description": "Partner opportunities and pipeline health",
        "query": """
        SELECT
            partner_name,
            opportunity_name,
            stage,
            amount,
            close_date,
            probability,
            partner_budget,
            pipeline_status,
            created_date,
            last_modified_date
        FROM unified_analytics.partner_pipeline_health
        WHERE created_date BETWEEN :start_date AND :end_date
        ORDER BY amount DESC NULLS LAST, close_date ASC NULLS LAST
        """,
        "default_filters": {
            "start_date": "90_days_ago",
            "end_date": "today"
        }
    },
    "executive_summary": {
        "name": "Executive Summary Report",
        "description": "High-level KPIs and performance metrics",
        "query": """
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
            blended_cpl
        FROM unified_analytics.executive_dashboard
        WHERE month BETWEEN DATE_TRUNC('month', :start_date::date) AND DATE_TRUNC('month', :end_date::date)
        ORDER BY month DESC
        """,
        "default_filters": {
            "start_date": "12_months_ago",
            "end_date": "today"
        }
    },
    "data_quality": {
        "name": "Data Quality Report",
        "description": "Data freshness and quality metrics",
        "query": """
        SELECT
            source_table,
            record_count,
            earliest_date,
            latest_date,
            last_extraction
        FROM unified_analytics.data_quality_monitor
        ORDER BY source_table
        """,
        "default_filters": {}
    }
}


# Report Generation Service
class ReportService:
    """Service for generating and managing reports."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def generate_report(self, template_name: str, request: ReportRequest) -> ReportResponse:
        """Generate a report based on template and request."""
        if template_name not in REPORT_TEMPLATES:
            raise HTTPException(status_code=404, detail=f"Report template '{template_name}' not found")

        template = REPORT_TEMPLATES[template_name]

        try:
            # Prepare date filters
            start_date, end_date = self._prepare_date_filters(request, template)

            # Prepare query parameters
            params = {
                "start_date": start_date,
                "end_date": end_date
            }

            # Add custom filters
            if request.filters:
                params.update(request.filters)

            # Execute query
            result = await self.db.execute(text(template["query"]), params)
            rows = result.fetchall()

            # Convert to list of dictionaries
            columns = result.keys()
            data = []
            for row in rows:
                row_dict = {}
                for i, column in enumerate(columns):
                    value = row[i]
                    # Format values for JSON serialization
                    if isinstance(value, date):
                        row_dict[column] = value.isoformat()
                    elif isinstance(value, datetime):
                        row_dict[column] = value.isoformat()
                    elif isinstance(value, (int, float)) and value is not None:
                        row_dict[column] = float(value)
                    else:
                        row_dict[column] = value
                data.append(row_dict)

            # Generate report ID
            report_id = f"{template_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

            response = ReportResponse(
                report_id=report_id,
                name=request.name or template["name"],
                generated_at=datetime.now(timezone.utc).isoformat(),
                format=request.format,
                row_count=len(data),
                data=data if request.format == "json" else None
            )

            return response

        except Exception as e:
            logger.error(f"Failed to generate report {template_name}: {e}")
            raise HTTPException(status_code=500, detail=f"Report generation failed: {str(e)}")

    def _prepare_date_filters(self, request: ReportRequest, template: Dict[str, Any]) -> tuple:
        """Prepare start and end dates for the report."""
        end_date = request.end_date or date.today()

        if request.start_date:
            start_date = request.start_date
        else:
            # Use template defaults
            default_start = template["default_filters"].get("start_date", "30_days_ago")
            if default_start == "30_days_ago":
                start_date = end_date - datetime.timedelta(days=30)
            elif default_start == "90_days_ago":
                start_date = end_date - datetime.timedelta(days=90)
            elif default_start == "12_months_ago":
                start_date = end_date - datetime.timedelta(days=365)
            else:
                start_date = end_date - datetime.timedelta(days=30)

        return start_date, end_date

    async def export_report_as_csv(self, data: List[Dict[str, Any]]) -> BytesIO:
        """Export report data as CSV."""
        if not data:
            return BytesIO(b"No data available")

        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

        csv_content = output.getvalue()
        return BytesIO(csv_content.encode('utf-8'))

    async def export_report_as_excel(self, data: List[Dict[str, Any]], report_name: str) -> BytesIO:
        """Export report data as Excel file."""
        try:
            import pandas as pd
            from io import BytesIO

            if not data:
                # Create empty DataFrame
                df = pd.DataFrame()
            else:
                df = pd.DataFrame(data)

            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name=report_name[:31], index=False)  # Excel sheet name limit

            output.seek(0)
            return output

        except ImportError:
            raise HTTPException(status_code=501, detail="Excel export requires pandas and xlsxwriter")


# API Endpoints
@router.get("/templates")
async def list_report_templates():
    """List all available report templates."""
    templates = []
    for template_id, template in REPORT_TEMPLATES.items():
        templates.append({
            "id": template_id,
            "name": template["name"],
            "description": template["description"],
            "default_filters": template["default_filters"]
        })

    return {
        "templates": templates,
        "count": len(templates)
    }


@router.post("/generate/{template_name}")
async def generate_report(
    template_name: str,
    request: ReportRequest,
    db: AsyncSession = Depends(get_db)
):
    """Generate a report based on template."""
    service = ReportService(db)
    report = await service.generate_report(template_name, request)

    if request.format == "csv":
        if report.data:
            csv_file = await service.export_report_as_csv(report.data)
            filename = f"{report.report_id}.csv"
            return StreamingResponse(
                BytesIO(csv_file.getvalue()),
                media_type="text/csv",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'}
            )
        else:
            raise HTTPException(status_code=500, detail="No data available for CSV export")

    elif request.format == "excel":
        if report.data:
            excel_file = await service.export_report_as_excel(report.data, report.name)
            filename = f"{report.report_id}.xlsx"
            return StreamingResponse(
                excel_file,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'}
            )
        else:
            raise HTTPException(status_code=500, detail="No data available for Excel export")

    else:  # JSON format
        return report


@router.get("/campaign-performance")
async def generate_campaign_performance_report(
    start_date: Optional[date] = Query(None, description="Start date"),
    end_date: Optional[date] = Query(None, description="End date"),
    format: str = Query("json", description="Output format"),
    channel: Optional[str] = Query(None, description="Channel filter"),
    performance_tier: Optional[str] = Query(None, description="Performance tier filter"),
    db: AsyncSession = Depends(get_db)
):
    """Generate campaign performance report with optional filters."""
    filters = {}
    if channel:
        filters["channel"] = channel
    if performance_tier:
        filters["performance_tier"] = performance_tier

    request = ReportRequest(
        name="Campaign Performance Report",
        start_date=start_date,
        end_date=end_date,
        format=format,
        filters=filters
    )

    return await generate_report("campaign_performance", request, db)


@router.get("/partner-pipeline")
async def generate_partner_pipeline_report(
    start_date: Optional[date] = Query(None, description="Start date"),
    end_date: Optional[date] = Query(None, description="End date"),
    format: str = Query("json", description="Output format"),
    stage: Optional[str] = Query(None, description="Pipeline stage filter"),
    db: AsyncSession = Depends(get_db)
):
    """Generate partner pipeline report."""
    filters = {}
    if stage:
        filters["stage"] = stage

    request = ReportRequest(
        name="Partner Pipeline Report",
        start_date=start_date,
        end_date=end_date,
        format=format,
        filters=filters
    )

    return await generate_report("partner_pipeline", request, db)


@router.get("/executive-summary")
async def generate_executive_summary_report(
    start_date: Optional[date] = Query(None, description="Start date"),
    end_date: Optional[date] = Query(None, description="End date"),
    format: str = Query("json", description="Output format"),
    db: AsyncSession = Depends(get_db)
):
    """Generate executive summary report."""
    request = ReportRequest(
        name="Executive Summary Report",
        start_date=start_date,
        end_date=end_date,
        format=format
    )

    return await generate_report("executive_summary", request, db)


@router.get("/data-quality")
async def generate_data_quality_report(
    format: str = Query("json", description="Output format"),
    db: AsyncSession = Depends(get_db)
):
    """Generate data quality report."""
    request = ReportRequest(
        name="Data Quality Report",
        format=format
    )

    return await generate_report("data_quality", request, db)


# Scheduled Reports (placeholder implementation)
@router.post("/scheduled")
async def create_scheduled_report(
    request: ScheduledReportRequest,
    db: AsyncSession = Depends(get_db)
):
    """Create a scheduled report."""
    # This would implement scheduled report functionality
    # For now, return a placeholder response
    return {
        "message": "Scheduled report functionality not yet implemented",
        "report_name": request.name,
        "schedule": request.schedule,
        "recipients": request.recipients
    }


@router.get("/scheduled")
async def list_scheduled_reports():
    """List all scheduled reports."""
    return {
        "message": "Scheduled reports functionality not yet implemented",
        "scheduled_reports": []
    }


@router.delete("/scheduled/{report_id}")
async def delete_scheduled_report(report_id: str):
    """Delete a scheduled report."""
    return {
        "message": f"Scheduled report {report_id} deletion not yet implemented"
    }


# Report History
@router.get("/history")
async def get_report_history(
    limit: int = Query(50, ge=1, le=100, description="Maximum number of reports"),
    db: AsyncSession = Depends(get_db)
):
    """Get report generation history."""
    # This would query a report history table
    # For now, return a placeholder
    return {
        "message": "Report history functionality not yet implemented",
        "reports": []
    }