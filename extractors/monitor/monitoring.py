# extractors/monitor/monitoring.py
"""
Monitoring utilities for ETL pipeline.
Provides error handling, logging, and notifications.
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Callable, Tuple


def log_step(step_name: str, message: str):
    """Log a step in the ETL process."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"[{timestamp}] {step_name}: {message}"
    print(log_message)

    # Also log to file
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    with open(log_dir / "etl.log", "a", encoding="utf-8") as f:
        f.write(log_message + "\n")


def notify(level: str, title: str, message: str, details: Dict[str, Any] = None):
    """Send notification about ETL events."""
    timestamp = datetime.now().isoformat()

    notification = {
        "timestamp": timestamp,
        "level": level,
        "title": title,
        "message": message,
        "details": details or {}
    }

    # Log to console
    log_step("NOTIFY", f"{level}: {title} - {message}")

    # Save to alerts file
    alerts_dir = Path("alerts")
    alerts_dir.mkdir(exist_ok=True)

    with open(alerts_dir / "alerts.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(notification) + "\n")

    # TODO: Add Slack notification if SLACK_WEBHOOK_URL is configured
    slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
    if slack_webhook and level in ["CRIT", "WARN"]:
        try:
            import requests
            payload = {
                "text": f"🚨 ETL Alert: {title}",
                "attachments": [{
                    "color": "danger" if level == "CRIT" else "warning",
                    "fields": [
                        {"title": "Level", "value": level, "short": True},
                        {"title": "Message", "value": message, "short": False}
                    ]
                }]
            }
            requests.post(slack_webhook, json=payload, timeout=10)
        except Exception as e:
            log_step("NOTIFY", f"Failed to send Slack notification: {e}")


def run_guard(func: Callable, step_name: str) -> Tuple[bool, Any]:
    """
    Execute a function with comprehensive error handling and monitoring.
    Returns (success: bool, result_or_error_info: Any)
    """
    start_time = datetime.now()
    log_step(step_name, "Starting...")

    try:
        result = func()
        duration = (datetime.now() - start_time).total_seconds()
        log_step(step_name, f"Completed successfully in {duration:.2f}s")
        return True, result

    except KeyboardInterrupt:
        log_step(step_name, "Interrupted by user")
        notify("WARN", f"{step_name} Interrupted", "Operation was interrupted by user")
        return False, {"error": "User interruption", "error_type": "KeyboardInterrupt"}

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        error_info = {
            "error": str(e),
            "error_type": type(e).__name__,
            "duration": duration
        }

        log_step(step_name, f"Failed after {duration:.2f}s: {e}")
        notify("CRIT", f"{step_name} Failed", str(e), error_info)

        return False, error_info


def ensure_directories():
    """Ensure all required directories exist."""
    dirs = [
        "data/raw/ultimate_dms",
        "data/raw/budget_waterfall_client",
        "data/raw/spend_revenue_performance",
        "data/raw/dfp_rij",
        "data/raw/agreed_cpl_performance",
        "data/raw/bsc_standards",
        "data/raw/budget_waterfall_channel",
        "data/raw/sf_partner_pipeline",
        "data/raw/sf_tim_king_partner_pipeline",
        "data/raw/sf_partner_calls",
        "secrets",
        "tmp",
        "logs",
        "alerts"
    ]

    for directory in dirs:
        Path(directory).mkdir(parents=True, exist_ok=True)

    log_step("SETUP", f"Ensured {len(dirs)} directories exist")