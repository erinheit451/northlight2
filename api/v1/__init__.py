"""
API Version 1 - Unified Endpoints
"""

from fastapi import APIRouter
from .auth import router as auth_router
from .benchmarking import router as benchmarking_router
from .etl_management import router as etl_router
from .reporting import router as reporting_router
from .analytics import router as analytics_router
from .book import router as book_router
from .trajectory import router as trajectory_router

# Create main API router
api_router = APIRouter(prefix="/api/v1")

# Include all sub-routers
api_router.include_router(auth_router, prefix="/auth", tags=["authentication"])
api_router.include_router(benchmarking_router, prefix="/benchmarks", tags=["benchmarks"])
api_router.include_router(etl_router, prefix="/etl", tags=["etl"])
api_router.include_router(reporting_router, prefix="/reports", tags=["reports"])
api_router.include_router(analytics_router, prefix="/analytics", tags=["analytics"])
# Include book router at the API root level (not v1) to match frontend expectations
api_router.include_router(book_router, tags=["book"])
api_router.include_router(trajectory_router, tags=["trajectory"])

__all__ = ["api_router"]