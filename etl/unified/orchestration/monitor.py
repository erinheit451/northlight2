"""
ETL Monitoring and Alerting System
Comprehensive monitoring for the unified ETL pipeline
"""

import asyncio
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from enum import Enum

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from core.config import settings
from core.shared import get_logger, log_step
from core.database import get_db_session, DatabaseHealthChecker
from sqlalchemy import text


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertType(Enum):
    """Types of alerts."""
    JOB_FAILED = "job_failed"
    JOB_TIMEOUT = "job_timeout"
    DATA_QUALITY = "data_quality"
    DATABASE_ERROR = "database_error"
    SYSTEM_ERROR = "system_error"
    DEPENDENCY_FAILURE = "dependency_failure"


@dataclass
class Alert:
    """ETL alert definition."""
    alert_id: str
    alert_type: AlertType
    level: AlertLevel
    message: str
    context: Dict[str, Any]
    created_at: datetime
    resolved_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        data['alert_type'] = self.alert_type.value
        data['level'] = self.level.value
        data['created_at'] = self.created_at.isoformat()
        data['resolved_at'] = self.resolved_at.isoformat() if self.resolved_at else None
        return data


@dataclass
class HealthMetrics:
    """System health metrics."""
    database_healthy: bool
    active_jobs: int
    failed_jobs_24h: int
    data_freshness_hours: float
    disk_usage_percent: float
    memory_usage_percent: float
    last_successful_pipeline: Optional[datetime]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        data['last_successful_pipeline'] = (
            self.last_successful_pipeline.isoformat()
            if self.last_successful_pipeline else None
        )
        return data


class ETLMonitor:
    """
    ETL Pipeline Monitor and Alerting System.

    Provides:
    - Real-time job monitoring
    - Health metrics collection
    - Alert generation and management
    - Performance tracking
    - Data quality monitoring
    """

    def __init__(self):
        self.logger = get_logger("etl.monitor")
        self.alerts: Dict[str, Alert] = {}
        self.alert_handlers: List[Callable] = []
        self.metrics_history: List[HealthMetrics] = []
        self._monitoring_active = False

        # Initialize alert handlers
        self._setup_alert_handlers()

    def _setup_alert_handlers(self):
        """Setup alert notification handlers."""
        # Slack webhook handler
        if settings.SLACK_WEBHOOK_URL:
            self.alert_handlers.append(self._send_slack_alert)

        # Email handler
        if settings.EMAIL_ALERTS_ENABLED:
            self.alert_handlers.append(self._send_email_alert)

        # Always log alerts
        self.alert_handlers.append(self._log_alert)

    async def start_monitoring(self):
        """Start the monitoring system."""
        self.logger.info("Starting ETL monitoring system...")
        self._monitoring_active = True

        # Start monitoring tasks
        monitoring_tasks = [
            asyncio.create_task(self._health_check_loop()),
            asyncio.create_task(self._alert_processor_loop()),
        ]

        try:
            await asyncio.gather(*monitoring_tasks)
        except asyncio.CancelledError:
            self.logger.info("Monitoring tasks cancelled")
        except Exception as e:
            self.logger.error(f"Monitoring system error: {e}")
            raise

    async def stop_monitoring(self):
        """Stop the monitoring system."""
        self.logger.info("Stopping ETL monitoring system...")
        self._monitoring_active = False

    async def _health_check_loop(self):
        """Continuous health monitoring loop."""
        while self._monitoring_active:
            try:
                metrics = await self.collect_health_metrics()
                self.metrics_history.append(metrics)

                # Keep only last 24 hours of metrics
                cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
                self.metrics_history = [
                    m for m in self.metrics_history
                    if hasattr(m, 'created_at') and m.created_at > cutoff_time
                ]

                # Check for alert conditions
                await self._check_health_alerts(metrics)

                # Wait for next check
                await asyncio.sleep(settings.HEALTH_CHECK_INTERVAL)

            except Exception as e:
                self.logger.error(f"Health check error: {e}")
                await self.create_alert(
                    AlertType.SYSTEM_ERROR,
                    AlertLevel.ERROR,
                    f"Health check failed: {str(e)}",
                    {"error": str(e)}
                )
                await asyncio.sleep(60)  # Wait longer on error

    async def _alert_processor_loop(self):
        """Process and send alerts."""
        while self._monitoring_active:
            try:
                # Process unresolved alerts
                unresolved_alerts = [
                    alert for alert in self.alerts.values()
                    if alert.resolved_at is None
                ]

                for alert in unresolved_alerts:
                    await self._process_alert(alert)

                await asyncio.sleep(30)  # Check every 30 seconds

            except Exception as e:
                self.logger.error(f"Alert processor error: {e}")
                await asyncio.sleep(60)

    async def collect_health_metrics(self) -> HealthMetrics:
        """Collect current system health metrics."""
        try:
            # Database health
            db_healthy = await DatabaseHealthChecker.check_connection()

            # Get database info
            db_info = await DatabaseHealthChecker.get_database_info()

            # Get job statistics
            active_jobs = 0  # This would be populated by scheduler
            failed_jobs_24h = await self._count_failed_jobs_24h()

            # Data freshness
            data_freshness = await self._calculate_data_freshness()

            # System metrics (simplified - in production would use proper system monitoring)
            disk_usage = 50.0  # Placeholder
            memory_usage = 30.0  # Placeholder

            # Last successful pipeline
            last_pipeline = await self._get_last_successful_pipeline()

            metrics = HealthMetrics(
                database_healthy=db_healthy,
                active_jobs=active_jobs,
                failed_jobs_24h=failed_jobs_24h,
                data_freshness_hours=data_freshness,
                disk_usage_percent=disk_usage,
                memory_usage_percent=memory_usage,
                last_successful_pipeline=last_pipeline
            )

            self.logger.debug(f"Collected health metrics: {metrics}")
            return metrics

        except Exception as e:
            self.logger.error(f"Failed to collect health metrics: {e}")
            raise

    async def _count_failed_jobs_24h(self) -> int:
        """Count failed jobs in the last 24 hours."""
        try:
            # This would query the job execution history table
            # For now, return 0 as placeholder
            return 0
        except Exception as e:
            self.logger.error(f"Failed to count failed jobs: {e}")
            return -1

    async def _calculate_data_freshness(self) -> float:
        """Calculate data freshness in hours."""
        try:
            async with get_db_session() as session:
                # Check the most recent data across all tables
                freshness_queries = [
                    "SELECT MAX(extracted_at) FROM heartbeat_core.ultimate_dms_campaigns",
                    "SELECT MAX(extracted_at) FROM heartbeat_core.budget_waterfall_client",
                    "SELECT MAX(extracted_at) FROM heartbeat_salesforce.sf_partner_pipeline"
                ]

                latest_times = []
                for query in freshness_queries:
                    try:
                        result = await session.execute(text(query))
                        latest_time = result.scalar()
                        if latest_time:
                            latest_times.append(latest_time)
                    except Exception:
                        # Table might not exist yet
                        continue

                if not latest_times:
                    return float('inf')  # No data found

                # Calculate hours since most recent data
                most_recent = max(latest_times)
                now = datetime.now(timezone.utc)

                # Handle timezone-naive datetime
                if most_recent.tzinfo is None:
                    most_recent = most_recent.replace(tzinfo=timezone.utc)

                delta = now - most_recent
                return delta.total_seconds() / 3600  # Convert to hours

        except Exception as e:
            self.logger.error(f"Failed to calculate data freshness: {e}")
            return float('inf')

    async def _get_last_successful_pipeline(self) -> Optional[datetime]:
        """Get the timestamp of the last successful pipeline run."""
        # This would query the job execution history
        # For now, return None as placeholder
        return None

    async def _check_health_alerts(self, metrics: HealthMetrics):
        """Check health metrics for alert conditions."""
        # Database health alert
        if not metrics.database_healthy:
            await self.create_alert(
                AlertType.DATABASE_ERROR,
                AlertLevel.CRITICAL,
                "Database connection failed",
                {"metrics": metrics.to_dict()}
            )

        # Data freshness alert
        if metrics.data_freshness_hours > 26:  # More than 26 hours old
            await self.create_alert(
                AlertType.DATA_QUALITY,
                AlertLevel.WARNING if metrics.data_freshness_hours < 48 else AlertLevel.ERROR,
                f"Data is {metrics.data_freshness_hours:.1f} hours old",
                {"data_freshness_hours": metrics.data_freshness_hours}
            )

        # Failed jobs alert
        if metrics.failed_jobs_24h > 3:
            await self.create_alert(
                AlertType.JOB_FAILED,
                AlertLevel.WARNING,
                f"{metrics.failed_jobs_24h} jobs failed in the last 24 hours",
                {"failed_jobs_24h": metrics.failed_jobs_24h}
            )

        # Disk usage alert
        if metrics.disk_usage_percent > 85:
            await self.create_alert(
                AlertType.SYSTEM_ERROR,
                AlertLevel.WARNING if metrics.disk_usage_percent < 95 else AlertLevel.CRITICAL,
                f"Disk usage at {metrics.disk_usage_percent:.1f}%",
                {"disk_usage_percent": metrics.disk_usage_percent}
            )

    async def create_alert(self, alert_type: AlertType, level: AlertLevel,
                          message: str, context: Dict[str, Any]) -> Alert:
        """Create a new alert."""
        alert_id = f"{alert_type.value}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

        alert = Alert(
            alert_id=alert_id,
            alert_type=alert_type,
            level=level,
            message=message,
            context=context,
            created_at=datetime.now(timezone.utc)
        )

        self.alerts[alert_id] = alert

        log_step(
            "ETL Monitor",
            f"Created {level.value} alert: {message}",
            is_error=(level in [AlertLevel.ERROR, AlertLevel.CRITICAL]),
            logger_name=self.logger.name
        )

        return alert

    async def _process_alert(self, alert: Alert):
        """Process an alert by sending notifications."""
        try:
            for handler in self.alert_handlers:
                await handler(alert)
        except Exception as e:
            self.logger.error(f"Failed to process alert {alert.alert_id}: {e}")

    async def _log_alert(self, alert: Alert):
        """Log alert handler."""
        level_map = {
            AlertLevel.INFO: self.logger.info,
            AlertLevel.WARNING: self.logger.warning,
            AlertLevel.ERROR: self.logger.error,
            AlertLevel.CRITICAL: self.logger.critical
        }

        log_func = level_map.get(alert.level, self.logger.info)
        log_func(f"ALERT [{alert.alert_type.value}]: {alert.message}")

    async def _send_slack_alert(self, alert: Alert):
        """Send alert to Slack (placeholder implementation)."""
        # This would use the Slack webhook to send alerts
        # For now, just log that we would send to Slack
        self.logger.info(f"Would send Slack alert: {alert.message}")

    async def _send_email_alert(self, alert: Alert):
        """Send alert via email (placeholder implementation)."""
        # This would send email alerts
        # For now, just log that we would send email
        self.logger.info(f"Would send email alert: {alert.message}")

    def resolve_alert(self, alert_id: str):
        """Mark an alert as resolved."""
        if alert_id in self.alerts:
            self.alerts[alert_id].resolved_at = datetime.now(timezone.utc)
            self.logger.info(f"Resolved alert: {alert_id}")

    def get_active_alerts(self) -> List[Alert]:
        """Get all unresolved alerts."""
        return [
            alert for alert in self.alerts.values()
            if alert.resolved_at is None
        ]

    def get_alert_summary(self) -> Dict[str, Any]:
        """Get summary of alerts."""
        all_alerts = list(self.alerts.values())
        active_alerts = self.get_active_alerts()

        summary = {
            "total_alerts": len(all_alerts),
            "active_alerts": len(active_alerts),
            "alerts_by_level": {},
            "alerts_by_type": {},
            "recent_alerts": []
        }

        # Count by level and type
        for alert in active_alerts:
            level = alert.level.value
            alert_type = alert.alert_type.value

            summary["alerts_by_level"][level] = summary["alerts_by_level"].get(level, 0) + 1
            summary["alerts_by_type"][alert_type] = summary["alerts_by_type"].get(alert_type, 0) + 1

        # Recent alerts (last 10)
        recent_alerts = sorted(all_alerts, key=lambda a: a.created_at, reverse=True)[:10]
        summary["recent_alerts"] = [alert.to_dict() for alert in recent_alerts]

        return summary

    def get_health_dashboard(self) -> Dict[str, Any]:
        """Get complete health dashboard data."""
        latest_metrics = self.metrics_history[-1] if self.metrics_history else None

        return {
            "system_status": "healthy" if (latest_metrics and latest_metrics.database_healthy) else "unhealthy",
            "latest_metrics": latest_metrics.to_dict() if latest_metrics else None,
            "alert_summary": self.get_alert_summary(),
            "metrics_history": [m.to_dict() for m in self.metrics_history[-24:]],  # Last 24 data points
            "monitoring_active": self._monitoring_active,
            "last_updated": datetime.now(timezone.utc).isoformat()
        }