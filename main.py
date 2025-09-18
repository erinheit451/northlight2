"""
Unified Northlight Platform - Main Application Entry Point
Combines Heartbeat ETL capabilities with Northlight's benchmarking and analytics
"""

import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
import uvicorn
import logging

# Import core modules
from core.config import settings
from core.database import init_database, close_database, DatabaseHealthChecker
from core.shared import setup_logging

# Import unified API
from api.v1 import api_router

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

# Create FastAPI application
app = FastAPI(
    title="Unified Northlight Platform",
    description="Integrated data pipeline and benchmarking analytics platform",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    """Initialize the application on startup."""
    logger.info("Starting Unified Northlight Platform...")

    try:
        # Initialize database connection
        await init_database()
        logger.info("Database connection established")

        # Verify database health
        db_healthy = await DatabaseHealthChecker.check_connection()
        if not db_healthy:
            logger.warning("Database health check failed")

        # Log database info
        db_info = await DatabaseHealthChecker.get_database_info()
        logger.info(f"Database info: {db_info}")

        logger.info("Unified Northlight Platform started successfully")

    except Exception as e:
        logger.error(f"Failed to initialize application: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on application shutdown."""
    logger.info("Shutting down Unified Northlight Platform...")
    try:
        await close_database()
        logger.info("Database connections closed")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

# Root endpoint
@app.get("/")
async def root():
    """Root endpoint - redirect to dashboard."""
    return RedirectResponse(url="/dashboard")

# Simple test endpoint
@app.get("/test")
async def test_endpoint():
    """Simple test endpoint."""
    return {"message": "Unified Northlight Platform is running!", "status": "ok"}

# Health check endpoint
@app.get("/health")
async def health_check():
    """Application health check."""
    try:
        db_healthy = await DatabaseHealthChecker.check_connection()
        db_info = await DatabaseHealthChecker.get_database_info()

        return {
            "status": "healthy" if db_healthy else "unhealthy",
            "version": app.version,
            "environment": settings.ENVIRONMENT,
            "database": {
                "connected": db_healthy,
                "info": db_info
            },
            "features": {
                "etl_enabled": settings.ETL_SCHEDULE_ENABLED,
                "advanced_analytics": settings.FEATURE_ADVANCED_ANALYTICS,
                "real_time_updates": settings.FEATURE_REAL_TIME_UPDATES,
                "export_powerpoint": settings.FEATURE_EXPORT_POWERPOINT
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "version": app.version,
            "environment": settings.ENVIRONMENT
        }

# API version endpoint
@app.get("/version")
async def version():
    """Get application version information."""
    return {
        "application": app.title,
        "version": app.version,
        "environment": settings.ENVIRONMENT,
        "api_version": "v1",
        "features": {
            "benchmarking": True,
            "etl_management": True,
            "analytics": True,
            "reporting": True
        }
    }

# Include unified API router
app.include_router(api_router)

# Include book router directly at /api/book to match frontend expectations
# DISABLED: Book router is already included in api_router above
# from api.v1.book import router as book_router
# app.include_router(book_router, prefix="/api")

# Legacy endpoints for backward compatibility
@app.get("/benchmarks/meta")
async def legacy_benchmarks_meta(limit: int = 2000):
    """Legacy endpoint - redirect to new API."""
    return RedirectResponse(url=f"/api/v1/benchmarks/meta?limit={limit}")

@app.post("/diagnose")
async def legacy_diagnose():
    """Legacy endpoint - redirect to new API."""
    return RedirectResponse(url="/api/v1/benchmarks/diagnose")

# Mount static files for frontend
frontend_path = project_root / "frontend"
if frontend_path.exists():
    # Serve the main dashboard
    @app.get("/dashboard")
    async def serve_dashboard():
        """Serve the main dashboard."""
        from fastapi.responses import FileResponse
        return FileResponse(frontend_path / "index.html")

    # Serve the book page
    @app.get("/book")
    async def serve_book():
        """Serve the book dashboard."""
        from fastapi.responses import FileResponse
        return FileResponse(frontend_path / "book" / "index.html")

    # Mount static assets
    app.mount("/static", StaticFiles(directory=str(frontend_path)), name="static")

    # Mount the entire frontend directory for CSS, JS, and other assets
    app.mount("/frontend", StaticFiles(directory=str(frontend_path)), name="frontend")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower()
    )