"""
Shared Utilities and Common Functions
Common functionality used across the unified platform
"""

import logging
import logging.config
import sys
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime, timezone
import json
import hashlib

from .config import settings


def setup_logging() -> None:
    """Configure application logging."""

    # Ensure logs directory exists
    log_path = Path(settings.LOG_FILE).parent
    log_path.mkdir(parents=True, exist_ok=True)

    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "format": "[{asctime}] [{levelname}] [{name}] {message}",
                "style": "{",
                "datefmt": "%Y-%m-%d %H:%M:%S"
            },
            "simple": {
                "format": "[{levelname}] {message}",
                "style": "{"
            }
        },
        "handlers": {
            "console": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "simple",
                "stream": sys.stdout
            },
            "file": {
                "level": settings.LOG_LEVEL,
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "detailed",
                "filename": settings.LOG_FILE,
                "maxBytes": 10 * 1024 * 1024,  # 10MB
                "backupCount": 5
            },
            "etl_file": {
                "level": settings.LOG_LEVEL,
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "detailed",
                "filename": settings.ETL_LOG_FILE,
                "maxBytes": 10 * 1024 * 1024,  # 10MB
                "backupCount": 5
            },
            "api_file": {
                "level": settings.LOG_LEVEL,
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "detailed",
                "filename": settings.API_LOG_FILE,
                "maxBytes": 10 * 1024 * 1024,  # 10MB
                "backupCount": 5
            }
        },
        "loggers": {
            "": {  # Root logger
                "level": settings.LOG_LEVEL,
                "handlers": ["console", "file"]
            },
            "etl": {
                "level": settings.LOG_LEVEL,
                "handlers": ["console", "etl_file"],
                "propagate": False
            },
            "api": {
                "level": settings.LOG_LEVEL,
                "handlers": ["console", "api_file"],
                "propagate": False
            },
            "uvicorn": {
                "level": "INFO",
                "handlers": ["console", "file"],
                "propagate": False
            },
            "sqlalchemy.engine": {
                "level": "WARNING",
                "handlers": ["file"],
                "propagate": False
            }
        }
    }

    logging.config.dictConfig(logging_config)


def get_logger(name: str) -> logging.Logger:
    """Get a configured logger instance."""
    return logging.getLogger(name)


def log_step(step_name: str, message: str, is_error: bool = False, logger_name: str = "etl") -> None:
    """Log ETL pipeline steps with consistent formatting."""
    logger = get_logger(logger_name)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if is_error:
        logger.error(f"{step_name}: {message}")
    else:
        logger.info(f"{step_name}: {message}")


def calculate_file_hash(file_path: Path, algorithm: str = "sha256") -> str:
    """Calculate hash of a file for integrity checking."""
    hash_algo = hashlib.new(algorithm)

    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_algo.update(chunk)
        return hash_algo.hexdigest()
    except Exception as e:
        raise ValueError(f"Failed to calculate hash for {file_path}: {e}")


def safe_json_load(file_path: Path, default: Any = None) -> Any:
    """Safely load JSON file with error handling."""
    try:
        if not file_path.exists():
            return default

        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger = get_logger(__name__)
        logger.error(f"Failed to load JSON from {file_path}: {e}")
        return default


def safe_json_dump(data: Any, file_path: Path, indent: int = 2) -> bool:
    """Safely write JSON file with error handling."""
    try:
        # Ensure parent directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, ensure_ascii=False, default=str)
        return True
    except (TypeError, IOError) as e:
        logger = get_logger(__name__)
        logger.error(f"Failed to write JSON to {file_path}: {e}")
        return False


def format_currency(amount: Optional[float], currency: str = "USD") -> str:
    """Format currency values consistently."""
    if amount is None:
        return "—"

    if currency == "USD":
        return f"${amount:,.2f}"
    else:
        return f"{amount:,.2f} {currency}"


def format_percentage(value: Optional[float], decimals: int = 2) -> str:
    """Format percentage values consistently."""
    if value is None:
        return "—"

    return f"{value * 100:.{decimals}f}%"


def format_number(value: Optional[float], decimals: int = 0) -> str:
    """Format numbers with consistent thousand separators."""
    if value is None:
        return "—"

    if decimals == 0:
        return f"{int(value):,}"
    else:
        return f"{value:,.{decimals}f}"


def sanitize_filename(filename: str) -> str:
    """Sanitize filename for safe file system usage."""
    import re

    # Remove or replace problematic characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    filename = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', filename)  # Remove control characters
    filename = filename.strip('. ')  # Remove leading/trailing dots and spaces

    # Limit length
    if len(filename) > 255:
        name, ext = filename.rsplit('.', 1) if '.' in filename else (filename, '')
        name = name[:255 - len(ext) - 1]
        filename = f"{name}.{ext}" if ext else name

    return filename


def parse_cors_origins(origins_str: str) -> list:
    """Parse CORS origins from comma-separated string."""
    if not origins_str:
        return []

    origins = [origin.strip() for origin in origins_str.split(',')]
    return [origin for origin in origins if origin]  # Filter empty strings


class DataValidator:
    """Common data validation utilities."""

    @staticmethod
    def is_valid_email(email: str) -> bool:
        """Basic email validation."""
        import re
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None

    @staticmethod
    def is_positive_number(value: Any) -> bool:
        """Check if value is a positive number."""
        try:
            return float(value) > 0
        except (ValueError, TypeError):
            return False

    @staticmethod
    def is_valid_date(date_str: str, format_str: str = "%Y-%m-%d") -> bool:
        """Check if string is a valid date."""
        try:
            datetime.strptime(date_str, format_str)
            return True
        except ValueError:
            return False


class PerformanceTimer:
    """Context manager for timing operations."""

    def __init__(self, operation_name: str, logger_name: str = __name__):
        self.operation_name = operation_name
        self.logger = get_logger(logger_name)
        self.start_time = None

    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.info(f"Starting {self.operation_name}...")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = datetime.now() - self.start_time
        if exc_type is None:
            self.logger.info(f"Completed {self.operation_name} in {duration.total_seconds():.2f} seconds")
        else:
            self.logger.error(f"Failed {self.operation_name} after {duration.total_seconds():.2f} seconds")


# Export commonly used items
__all__ = [
    "setup_logging",
    "get_logger",
    "log_step",
    "calculate_file_hash",
    "safe_json_load",
    "safe_json_dump",
    "format_currency",
    "format_percentage",
    "format_number",
    "sanitize_filename",
    "parse_cors_origins",
    "DataValidator",
    "PerformanceTimer"
]