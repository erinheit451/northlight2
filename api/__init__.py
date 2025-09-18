"""
Unified Northlight Platform API
Consolidated API endpoints combining Northlight benchmarking with ETL management
"""

from .v1 import api_router

__all__ = ["api_router"]