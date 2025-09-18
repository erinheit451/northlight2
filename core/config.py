"""
Unified Configuration Management
Handles all configuration settings for the unified platform
"""

import os
from pathlib import Path
from typing import List, Optional
from pydantic import Field
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Application Configuration
    ENVIRONMENT: str = Field(default="development", env="ENVIRONMENT")
    DEBUG: bool = Field(default=True, env="DEBUG")
    SECRET_KEY: str = Field(default="change-me-in-production", env="SECRET_KEY")

    # API Configuration
    API_HOST: str = Field(default="0.0.0.0", env="API_HOST")
    API_PORT: int = Field(default=8000, env="API_PORT")
    MAX_REQUEST_SIZE: str = Field(default="50MB", env="MAX_REQUEST_SIZE")

    # Database Configuration
    DATABASE_URL: str = Field(env="DATABASE_URL")
    DATABASE_POOL_SIZE: int = Field(default=20, env="DATABASE_POOL_SIZE")
    DATABASE_MAX_OVERFLOW: int = Field(default=30, env="DATABASE_MAX_OVERFLOW")

    # Legacy Database URLs (for migration)
    LEGACY_HEARTBEAT_DUCKDB: Optional[str] = Field(env="LEGACY_HEARTBEAT_DUCKDB")
    LEGACY_NORTHLIGHT_DUCKDB: Optional[str] = Field(env="LEGACY_NORTHLIGHT_DUCKDB")

    # Corporate Portal Configuration
    CORP_PORTAL_USERNAME: str = Field(env="CORP_PORTAL_USERNAME")
    CORP_PORTAL_PASSWORD: str = Field(env="CORP_PORTAL_PASSWORD")
    CORP_PORTAL_URL: str = Field(env="CORP_PORTAL_URL")

    # Corportal Configuration
    CORPORTAL_LOGIN_URL: str = Field(env="CORPORTAL_LOGIN_URL")
    CORPORTAL_REPORT_URL: str = Field(env="CORPORTAL_REPORT_URL")

    # Salesforce Configuration
    SF_USERNAME: str = Field(env="SF_USERNAME")
    SF_PASSWORD: str = Field(env="SF_PASSWORD")
    SF_TOTP_CODE: Optional[str] = Field(default=None, env="SF_TOTP_CODE")

    # Ultimate DMS Configuration
    ULTIMATE_DMS_BASE_URL: str = Field(env="ULTIMATE_DMS_BASE_URL")

    # Data Storage Paths
    RAW_DATA_PATH: str = Field(default="data/raw", env="RAW_DATA_PATH")
    WAREHOUSE_PATH: str = Field(default="data/warehouse", env="WAREHOUSE_PATH")
    DATA_ROOT: str = Field(default="data", env="DATA_ROOT")
    BOOK_DIR: str = Field(default="data/book", env="BOOK_DIR")
    EXPORT_DIR: str = Field(default="data/exports", env="EXPORT_DIR")

    # Logging Configuration
    LOG_LEVEL: str = Field(default="INFO", env="LOG_LEVEL")
    LOG_FILE: str = Field(default="logs/unified.log", env="LOG_FILE")
    ETL_LOG_FILE: str = Field(default="logs/etl.log", env="ETL_LOG_FILE")
    API_LOG_FILE: str = Field(default="logs/api.log", env="API_LOG_FILE")

    # Cache Configuration
    REDIS_URL: str = Field(default="redis://localhost:6379/0", env="REDIS_URL")
    CACHE_TTL: int = Field(default=3600, env="CACHE_TTL")

    # Security Configuration
    CORS_ORIGINS: List[str] = Field(
        default=[
            "https://northlight.pages.dev",
            "https://develop.northlight.pages.dev",
            "http://localhost",
            "http://localhost:8000",
            "http://127.0.0.1:5500",
            "http://127.0.0.1:8000",
            "http://127.0.0.1:9000"
        ],
        env="CORS_ORIGINS"
    )

    # Timezone and Localization
    TIMEZONE: str = Field(default="America/Los_Angeles", env="TIMEZONE")
    DEFAULT_LOCALE: str = Field(default="en_US", env="DEFAULT_LOCALE")

    # Monitoring and Alerting
    SLACK_WEBHOOK_URL: Optional[str] = Field(default=None, env="SLACK_WEBHOOK_URL")
    EMAIL_ALERTS_ENABLED: bool = Field(default=False, env="EMAIL_ALERTS_ENABLED")
    HEALTH_CHECK_INTERVAL: int = Field(default=300, env="HEALTH_CHECK_INTERVAL")

    # ETL Configuration
    ETL_SCHEDULE_ENABLED: bool = Field(default=True, env="ETL_SCHEDULE_ENABLED")
    ETL_RETRY_ATTEMPTS: int = Field(default=3, env="ETL_RETRY_ATTEMPTS")
    ETL_TIMEOUT_SECONDS: int = Field(default=3600, env="ETL_TIMEOUT_SECONDS")
    ETL_PARALLEL_JOBS: int = Field(default=4, env="ETL_PARALLEL_JOBS")

    # Feature Flags
    FEATURE_ADVANCED_ANALYTICS: bool = Field(default=True, env="FEATURE_ADVANCED_ANALYTICS")
    FEATURE_REAL_TIME_UPDATES: bool = Field(default=True, env="FEATURE_REAL_TIME_UPDATES")
    FEATURE_EXPORT_POWERPOINT: bool = Field(default=True, env="FEATURE_EXPORT_POWERPOINT")
    FEATURE_AUTOMATED_REPORTS: bool = Field(default=True, env="FEATURE_AUTOMATED_REPORTS")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Ensure data directories exist
        self.create_data_directories()

    def create_data_directories(self):
        """Create required data directories if they don't exist."""
        base_path = Path.cwd()

        directories = [
            self.RAW_DATA_PATH,
            self.WAREHOUSE_PATH,
            self.DATA_ROOT,
            self.BOOK_DIR,
            self.EXPORT_DIR,
            "logs"
        ]

        for directory in directories:
            dir_path = base_path / directory
            dir_path.mkdir(parents=True, exist_ok=True)

    @property
    def database_config(self) -> dict:
        """Get database configuration for SQLAlchemy."""
        return {
            "url": self.DATABASE_URL,
            "pool_size": self.DATABASE_POOL_SIZE,
            "max_overflow": self.DATABASE_MAX_OVERFLOW,
            "echo": self.DEBUG
        }

    @property
    def corp_portal_config(self) -> dict:
        """Get corporate portal configuration."""
        return {
            "username": self.CORP_PORTAL_USERNAME,
            "password": self.CORP_PORTAL_PASSWORD,
            "url": self.CORP_PORTAL_URL,
            "login_url": self.CORPORTAL_LOGIN_URL,
            "report_url": self.CORPORTAL_REPORT_URL
        }

    @property
    def salesforce_config(self) -> dict:
        """Get Salesforce configuration."""
        return {
            "username": self.SF_USERNAME,
            "password": self.SF_PASSWORD,
            "totp_code": self.SF_TOTP_CODE
        }

    @property
    def etl_config(self) -> dict:
        """Get ETL configuration."""
        return {
            "schedule_enabled": self.ETL_SCHEDULE_ENABLED,
            "retry_attempts": self.ETL_RETRY_ATTEMPTS,
            "timeout_seconds": self.ETL_TIMEOUT_SECONDS,
            "parallel_jobs": self.ETL_PARALLEL_JOBS
        }


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Global settings instance
settings = get_settings()