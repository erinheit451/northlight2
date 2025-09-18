"""
ETL Scheduler for Unified Northlight Platform
Orchestrates and schedules ETL pipeline execution
"""

import asyncio
import logging
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, asdict
from enum import Enum
import json

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from core.config import settings
from core.shared import get_logger, log_step, PerformanceTimer
from core.database import get_db_session

# Import our unified loaders
from ..loaders.ultimate_dms_loader import UltimateDMSLoader
from ..loaders.budget_waterfall_loader import BudgetWaterfallLoader
from ..loaders.salesforce_loader import SalesforcePartnerPipelineLoader, SalesforcePartnerCallsLoader


class JobStatus(Enum):
    """ETL job status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ETLJob:
    """ETL job definition."""
    job_id: str
    job_name: str
    loader_class: type
    source_path: Path
    schedule_cron: Optional[str] = None
    retry_attempts: int = 3
    timeout_seconds: int = 3600
    dependencies: List[str] = None
    enabled: bool = True

    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []


@dataclass
class JobExecution:
    """ETL job execution tracking."""
    job_id: str
    execution_id: str
    status: JobStatus
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    result: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        data['status'] = self.status.value
        data['started_at'] = self.started_at.isoformat() if self.started_at else None
        data['completed_at'] = self.completed_at.isoformat() if self.completed_at else None
        return data


class ETLScheduler:
    """
    ETL Pipeline Scheduler and Orchestrator.

    Manages:
    - Job scheduling and execution
    - Dependency management
    - Error handling and retries
    - Performance monitoring
    - Status tracking
    """

    def __init__(self):
        self.logger = get_logger("etl.scheduler")
        self.jobs: Dict[str, ETLJob] = {}
        self.executions: Dict[str, JobExecution] = {}
        self.running_jobs: Dict[str, asyncio.Task] = {}
        self._shutdown_requested = False

        # Initialize job definitions
        self._initialize_default_jobs()

    def _initialize_default_jobs(self):
        """Initialize default ETL jobs based on Heartbeat configuration."""

        # Ultimate DMS Campaign Data
        self.register_job(ETLJob(
            job_id="ultimate_dms",
            job_name="Ultimate DMS Campaign Data",
            loader_class=UltimateDMSLoader,
            source_path=Path(settings.RAW_DATA_PATH) / "ultimate_dms",
            schedule_cron="0 7 * * *",  # Daily at 7 AM
            retry_attempts=3,
            timeout_seconds=1800  # 30 minutes
        ))

        # Budget Waterfall Client Data
        self.register_job(ETLJob(
            job_id="budget_waterfall",
            job_name="Budget Waterfall Client Data",
            loader_class=BudgetWaterfallLoader,
            source_path=Path(settings.RAW_DATA_PATH) / "budget_waterfall",
            schedule_cron="0 8 * * *",  # Daily at 8 AM
            retry_attempts=3,
            timeout_seconds=1200  # 20 minutes
        ))

        # Salesforce Partner Pipeline
        self.register_job(ETLJob(
            job_id="sf_partner_pipeline",
            job_name="Salesforce Partner Pipeline",
            loader_class=SalesforcePartnerPipelineLoader,
            source_path=Path(settings.RAW_DATA_PATH) / "salesforce" / "partner_pipeline",
            schedule_cron="0 9 * * *",  # Daily at 9 AM
            retry_attempts=3,
            timeout_seconds=1800,  # 30 minutes
            dependencies=["ultimate_dms"]  # Depends on Ultimate DMS data
        ))

        # Salesforce Partner Calls
        self.register_job(ETLJob(
            job_id="sf_partner_calls",
            job_name="Salesforce Partner Calls",
            loader_class=SalesforcePartnerCallsLoader,
            source_path=Path(settings.RAW_DATA_PATH) / "salesforce" / "partner_calls",
            schedule_cron="0 9 * * *",  # Daily at 9 AM
            retry_attempts=3,
            timeout_seconds=900,  # 15 minutes
            dependencies=["sf_partner_pipeline"]
        ))

        self.logger.info(f"Initialized {len(self.jobs)} default ETL jobs")

    def register_job(self, job: ETLJob):
        """Register a new ETL job."""
        self.jobs[job.job_id] = job
        self.logger.info(f"Registered ETL job: {job.job_name} ({job.job_id})")

    def unregister_job(self, job_id: str):
        """Unregister an ETL job."""
        if job_id in self.jobs:
            del self.jobs[job_id]
            self.logger.info(f"Unregistered ETL job: {job_id}")

    async def execute_job(self, job_id: str, force: bool = False) -> JobExecution:
        """
        Execute a single ETL job.

        Args:
            job_id: ID of the job to execute
            force: Force execution even if dependencies aren't met

        Returns:
            JobExecution object with results
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job {job_id} not found")

        job = self.jobs[job_id]
        execution_id = f"{job_id}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

        execution = JobExecution(
            job_id=job_id,
            execution_id=execution_id,
            status=JobStatus.PENDING
        )

        self.executions[execution_id] = execution

        try:
            # Check dependencies
            if not force and not await self._check_dependencies(job):
                execution.status = JobStatus.FAILED
                execution.error_message = "Dependencies not satisfied"
                return execution

            # Check if job is already running
            if job_id in self.running_jobs:
                execution.status = JobStatus.FAILED
                execution.error_message = f"Job {job_id} is already running"
                return execution

            log_step(
                "ETL Scheduler",
                f"Starting job {job.job_name} ({job_id})",
                logger_name=self.logger.name
            )

            execution.status = JobStatus.RUNNING
            execution.started_at = datetime.now(timezone.utc)

            # Find the latest Parquet file in the source path
            parquet_files = list(job.source_path.glob("*.parquet"))
            if not parquet_files:
                execution.status = JobStatus.FAILED
                execution.error_message = f"No Parquet files found in {job.source_path}"
                return execution

            # Use the most recent file
            latest_file = max(parquet_files, key=lambda p: p.stat().st_mtime)

            # Execute with timeout and retries
            async with asyncio.timeout(job.timeout_seconds):
                loader = job.loader_class()
                result = await loader.load_from_parquet(latest_file)

                execution.status = JobStatus.COMPLETED
                execution.completed_at = datetime.now(timezone.utc)
                execution.result = result

                log_step(
                    "ETL Scheduler",
                    f"Completed job {job.job_name} ({job_id}) successfully",
                    logger_name=self.logger.name
                )

        except asyncio.TimeoutError:
            execution.status = JobStatus.FAILED
            execution.error_message = f"Job timed out after {job.timeout_seconds} seconds"
            self.logger.error(f"Job {job_id} timed out")

        except Exception as e:
            execution.status = JobStatus.FAILED
            execution.error_message = str(e)
            execution.completed_at = datetime.now(timezone.utc)

            log_step(
                "ETL Scheduler",
                f"Job {job.job_name} ({job_id}) failed: {str(e)}",
                is_error=True,
                logger_name=self.logger.name
            )

            # Retry logic
            if execution.retry_count < job.retry_attempts:
                execution.retry_count += 1
                self.logger.info(f"Retrying job {job_id} (attempt {execution.retry_count})")
                await asyncio.sleep(60)  # Wait 1 minute before retry
                return await self.execute_job(job_id, force)

        finally:
            # Clean up running job tracking
            if job_id in self.running_jobs:
                del self.running_jobs[job_id]

        return execution

    async def execute_pipeline(self, job_ids: Optional[List[str]] = None) -> Dict[str, JobExecution]:
        """
        Execute a full ETL pipeline.

        Args:
            job_ids: Optional list of specific job IDs to execute

        Returns:
            Dictionary of job executions
        """
        if job_ids is None:
            job_ids = list(self.jobs.keys())

        executions = {}

        log_step(
            "ETL Scheduler",
            f"Starting pipeline execution for {len(job_ids)} jobs",
            logger_name=self.logger.name
        )

        with PerformanceTimer("ETL Pipeline Execution", self.logger.name):
            # Execute jobs in dependency order
            remaining_jobs = set(job_ids)
            completed_jobs = set()

            while remaining_jobs and not self._shutdown_requested:
                # Find jobs that can be executed (dependencies satisfied)
                ready_jobs = []
                for job_id in remaining_jobs:
                    job = self.jobs[job_id]
                    if all(dep in completed_jobs for dep in job.dependencies):
                        ready_jobs.append(job_id)

                if not ready_jobs:
                    # Check for circular dependencies or missing dependencies
                    self.logger.error(f"No jobs ready to execute. Remaining: {remaining_jobs}")
                    break

                # Execute ready jobs in parallel
                if settings.ETL_PARALLEL_JOBS > 1:
                    tasks = []
                    for job_id in ready_jobs[:settings.ETL_PARALLEL_JOBS]:
                        task = asyncio.create_task(self.execute_job(job_id))
                        tasks.append((job_id, task))
                        self.running_jobs[job_id] = task

                    # Wait for all tasks to complete
                    for job_id, task in tasks:
                        execution = await task
                        executions[job_id] = execution
                        remaining_jobs.discard(job_id)

                        if execution.status == JobStatus.COMPLETED:
                            completed_jobs.add(job_id)
                else:
                    # Execute jobs sequentially
                    for job_id in ready_jobs:
                        execution = await self.execute_job(job_id)
                        executions[job_id] = execution
                        remaining_jobs.discard(job_id)

                        if execution.status == JobStatus.COMPLETED:
                            completed_jobs.add(job_id)

        # Log pipeline summary
        successful = sum(1 for ex in executions.values() if ex.status == JobStatus.COMPLETED)
        failed = sum(1 for ex in executions.values() if ex.status == JobStatus.FAILED)

        log_step(
            "ETL Scheduler",
            f"Pipeline completed: {successful} successful, {failed} failed",
            is_error=(failed > 0),
            logger_name=self.logger.name
        )

        return executions

    async def _check_dependencies(self, job: ETLJob) -> bool:
        """Check if job dependencies are satisfied."""
        for dep_job_id in job.dependencies:
            # Check if dependency job has run successfully today
            today = datetime.now(timezone.utc).date()
            recent_executions = [
                ex for ex in self.executions.values()
                if ex.job_id == dep_job_id and
                ex.started_at and
                ex.started_at.date() == today and
                ex.status == JobStatus.COMPLETED
            ]

            if not recent_executions:
                self.logger.warning(f"Dependency {dep_job_id} not satisfied for job {job.job_id}")
                return False

        return True

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get current status of a job."""
        if job_id not in self.jobs:
            return None

        job = self.jobs[job_id]
        recent_executions = [
            ex for ex in self.executions.values()
            if ex.job_id == job_id
        ]

        recent_executions.sort(key=lambda x: x.started_at or datetime.min, reverse=True)
        latest_execution = recent_executions[0] if recent_executions else None

        return {
            "job_id": job_id,
            "job_name": job.job_name,
            "enabled": job.enabled,
            "is_running": job_id in self.running_jobs,
            "latest_execution": latest_execution.to_dict() if latest_execution else None,
            "recent_executions": [ex.to_dict() for ex in recent_executions[:5]]
        }

    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get overall pipeline status."""
        jobs_status = {job_id: self.get_job_status(job_id) for job_id in self.jobs.keys()}

        running_count = sum(1 for status in jobs_status.values() if status["is_running"])
        enabled_count = sum(1 for status in jobs_status.values() if status["enabled"])

        return {
            "total_jobs": len(self.jobs),
            "enabled_jobs": enabled_count,
            "running_jobs": running_count,
            "jobs": jobs_status,
            "last_pipeline_run": max(
                (ex.started_at for ex in self.executions.values() if ex.started_at),
                default=None
            )
        }

    async def shutdown(self):
        """Gracefully shutdown the scheduler."""
        self.logger.info("Shutting down ETL scheduler...")
        self._shutdown_requested = True

        # Cancel running jobs
        for job_id, task in self.running_jobs.items():
            self.logger.info(f"Cancelling running job: {job_id}")
            task.cancel()

        # Wait for jobs to finish
        if self.running_jobs:
            await asyncio.gather(*self.running_jobs.values(), return_exceptions=True)

        self.logger.info("ETL scheduler shutdown complete")