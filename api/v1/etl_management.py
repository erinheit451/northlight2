"""
ETL Management API Endpoints
Real-time control and monitoring of ETL pipeline operations
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.database import get_db
from core.shared import get_logger
from etl.unified.orchestration.scheduler import ETLScheduler, JobStatus
from etl.unified.orchestration.monitor import ETLMonitor

router = APIRouter()
logger = get_logger("api.etl_management")

# Global instances (in production, these would be properly managed)
etl_scheduler = ETLScheduler()
etl_monitor = ETLMonitor()

# Pydantic models
class JobExecutionRequest(BaseModel):
    """Request to execute a specific job."""
    job_id: str = Field(..., description="Job ID to execute")
    force: bool = Field(False, description="Force execution even if dependencies aren't met")


class PipelineExecutionRequest(BaseModel):
    """Request to execute pipeline."""
    job_ids: Optional[List[str]] = Field(None, description="Specific job IDs to run (None for all)")
    extract_first: bool = Field(True, description="Run extraction before loading")


class JobStatusResponse(BaseModel):
    """Job status response."""
    job_id: str
    job_name: str
    enabled: bool
    is_running: bool
    latest_execution: Optional[Dict[str, Any]]
    recent_executions: List[Dict[str, Any]]


class PipelineStatusResponse(BaseModel):
    """Pipeline status response."""
    total_jobs: int
    enabled_jobs: int
    running_jobs: int
    jobs: Dict[str, JobStatusResponse]
    last_pipeline_run: Optional[str]


class HealthMetricsResponse(BaseModel):
    """Health metrics response."""
    system_status: str
    latest_metrics: Optional[Dict[str, Any]]
    alert_summary: Dict[str, Any]
    monitoring_active: bool
    last_updated: str


# ETL Control Endpoints
@router.post("/jobs/{job_id}/execute")
async def execute_job(
    job_id: str,
    request: JobExecutionRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Execute a specific ETL job."""
    try:
        # Validate job exists
        if job_id not in etl_scheduler.jobs:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        job = etl_scheduler.jobs[job_id]

        # Check if job is already running
        if job_id in etl_scheduler.running_jobs:
            raise HTTPException(
                status_code=409,
                detail=f"Job {job_id} is already running"
            )

        # Execute job in background
        background_tasks.add_task(
            _execute_job_background,
            job_id,
            request.force
        )

        return {
            "message": f"Job {job.job_name} ({job_id}) execution started",
            "job_id": job_id,
            "status": "started"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to execute job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Job execution failed: {str(e)}")


@router.post("/pipeline/execute")
async def execute_pipeline(
    request: PipelineExecutionRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Execute the complete ETL pipeline or specific jobs."""
    try:
        job_ids = request.job_ids or list(etl_scheduler.jobs.keys())

        # Validate job IDs
        invalid_jobs = [jid for jid in job_ids if jid not in etl_scheduler.jobs]
        if invalid_jobs:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid job IDs: {invalid_jobs}"
            )

        # Execute pipeline in background
        background_tasks.add_task(
            _execute_pipeline_background,
            job_ids,
            request.extract_first
        )

        return {
            "message": f"Pipeline execution started for {len(job_ids)} jobs",
            "job_ids": job_ids,
            "extract_first": request.extract_first,
            "status": "started"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to execute pipeline: {e}")
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {str(e)}")


@router.get("/jobs/{job_id}/status", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """Get status of a specific job."""
    status = etl_scheduler.get_job_status(job_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    return JobStatusResponse(**status)


@router.get("/jobs/{job_id}/logs")
async def get_job_logs(
    job_id: str,
    lines: int = 100,
    level: str = "INFO"
):
    """Get recent logs for a specific job."""
    try:
        # This would read logs from the logging system
        # For now, return a placeholder
        return {
            "job_id": job_id,
            "lines_requested": lines,
            "level": level,
            "logs": [
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "level": "INFO",
                    "message": f"Log entry for job {job_id}"
                }
            ],
            "message": "Log retrieval not yet implemented"
        }

    except Exception as e:
        logger.error(f"Failed to get logs for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve logs")


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    """Cancel a running job."""
    try:
        if job_id not in etl_scheduler.running_jobs:
            raise HTTPException(
                status_code=400,
                detail=f"Job {job_id} is not currently running"
            )

        # Cancel the job
        task = etl_scheduler.running_jobs[job_id]
        task.cancel()

        return {
            "message": f"Job {job_id} cancellation requested",
            "job_id": job_id,
            "status": "cancelling"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Job cancellation failed: {str(e)}")


# Pipeline Status Endpoints
@router.get("/pipeline/status", response_model=PipelineStatusResponse)
async def get_pipeline_status():
    """Get overall pipeline status."""
    try:
        status = etl_scheduler.get_pipeline_status()
        return PipelineStatusResponse(**status)

    except Exception as e:
        logger.error(f"Failed to get pipeline status: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve pipeline status")


@router.get("/jobs", response_model=List[JobStatusResponse])
async def list_jobs():
    """List all available ETL jobs."""
    try:
        jobs = []
        for job_id in etl_scheduler.jobs.keys():
            status = etl_scheduler.get_job_status(job_id)
            if status:
                jobs.append(JobStatusResponse(**status))

        return jobs

    except Exception as e:
        logger.error(f"Failed to list jobs: {e}")
        raise HTTPException(status_code=500, detail="Failed to list jobs")


# Monitoring Endpoints
@router.get("/health", response_model=HealthMetricsResponse)
async def get_health_metrics():
    """Get current system health metrics."""
    try:
        dashboard_data = etl_monitor.get_health_dashboard()
        return HealthMetricsResponse(**dashboard_data)

    except Exception as e:
        logger.error(f"Failed to get health metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve health metrics")


@router.get("/alerts")
async def get_active_alerts():
    """Get all active alerts."""
    try:
        active_alerts = etl_monitor.get_active_alerts()
        return {
            "active_alerts": [alert.to_dict() for alert in active_alerts],
            "count": len(active_alerts)
        }

    except Exception as e:
        logger.error(f"Failed to get alerts: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve alerts")


@router.post("/alerts/{alert_id}/resolve")
async def resolve_alert(alert_id: str):
    """Mark an alert as resolved."""
    try:
        etl_monitor.resolve_alert(alert_id)
        return {
            "message": f"Alert {alert_id} marked as resolved",
            "alert_id": alert_id
        }

    except Exception as e:
        logger.error(f"Failed to resolve alert {alert_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to resolve alert")


# Data Quality Endpoints
@router.get("/data-quality")
async def get_data_quality_report(
    db: AsyncSession = Depends(get_db)
):
    """Get data quality report across all tables."""
    try:
        # Query the unified analytics data quality view
        query = """
        SELECT * FROM unified_analytics.data_quality_monitor
        ORDER BY source_table
        """

        result = await db.execute(query)
        rows = result.fetchall()

        quality_report = []
        for row in rows:
            quality_report.append({
                "source_table": row.source_table,
                "record_count": row.record_count,
                "null_count": getattr(row, 'null_spend_count', 0) or getattr(row, 'null_cpl_count', 0) or getattr(row, 'null_amount_count', 0),
                "earliest_date": row.earliest_date.isoformat() if row.earliest_date else None,
                "latest_date": row.latest_date.isoformat() if row.latest_date else None,
                "last_extraction": row.last_extraction.isoformat() if row.last_extraction else None
            })

        return {
            "data_quality_report": quality_report,
            "generated_at": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        logger.error(f"Failed to get data quality report: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve data quality report")


# Background task functions
async def _execute_job_background(job_id: str, force: bool = False):
    """Execute job in background."""
    try:
        execution = await etl_scheduler.execute_job(job_id, force)
        logger.info(f"Background job {job_id} completed with status: {execution.status.value}")
    except Exception as e:
        logger.error(f"Background job {job_id} failed: {e}")


async def _execute_pipeline_background(job_ids: List[str], extract_first: bool = True):
    """Execute pipeline in background."""
    try:
        if extract_first:
            # Run extraction first
            from etl.unified.extractors.heartbeat_wrapper import extract_all_data
            extraction_results = await extract_all_data()
            logger.info(f"Background extraction completed: {extraction_results.get('successful_extractions', 0)} successful")

        # Run loading pipeline
        executions = await etl_scheduler.execute_pipeline(job_ids)
        successful = sum(1 for ex in executions.values() if ex.status == JobStatus.COMPLETED)
        logger.info(f"Background pipeline completed: {successful}/{len(executions)} jobs successful")

    except Exception as e:
        logger.error(f"Background pipeline failed: {e}")


# WebSocket endpoint for real-time updates (placeholder)
@router.websocket("/ws/status")
async def websocket_status_updates(websocket):
    """WebSocket endpoint for real-time status updates."""
    # This would provide real-time updates of job status, alerts, etc.
    # Implementation would depend on WebSocket requirements
    await websocket.accept()
    await websocket.send_json({"message": "WebSocket status updates not yet implemented"})
    await websocket.close()