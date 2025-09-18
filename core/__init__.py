"""
Core module for the Unified Northlight Platform
Provides database connectivity, configuration, and shared utilities
"""

from .config import settings, get_settings
from .database import (
    Base,
    init_database,
    close_database,
    get_db_session,
    get_db,
    DatabaseHealthChecker
)
from .shared import (
    setup_logging,
    get_logger,
    log_step,
    format_currency,
    format_percentage,
    format_number,
    DataValidator,
    PerformanceTimer
)

__all__ = [
    # Configuration
    "settings",
    "get_settings",

    # Database
    "Base",
    "init_database",
    "close_database",
    "get_db_session",
    "get_db",
    "DatabaseHealthChecker",

    # Shared utilities
    "setup_logging",
    "get_logger",
    "log_step",
    "format_currency",
    "format_percentage",
    "format_number",
    "DataValidator",
    "PerformanceTimer"
]