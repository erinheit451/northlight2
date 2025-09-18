"""
Database Connection and Management
Handles PostgreSQL connections and session management
"""

import logging
from typing import AsyncGenerator, Optional
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import MetaData, text
from contextlib import asynccontextmanager

from .config import settings

logger = logging.getLogger(__name__)

# Create SQLAlchemy components
engine: Optional[create_async_engine] = None
async_session_maker: Optional[async_sessionmaker] = None

# Base class for SQLAlchemy models
Base = declarative_base()

# Naming convention for constraints
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}

Base.metadata = MetaData(naming_convention=convention)


async def init_database() -> None:
    """Initialize database connection and session factory."""
    global engine, async_session_maker

    try:
        logger.info("Initializing database connection...")

        # Create async engine
        engine = create_async_engine(
            settings.DATABASE_URL,
            pool_size=settings.DATABASE_POOL_SIZE,
            max_overflow=settings.DATABASE_MAX_OVERFLOW,
            echo=settings.DEBUG,
            future=True
        )

        # Create session factory
        async_session_maker = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False
        )

        # Test connection
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))

        logger.info("Database initialized successfully")

    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


async def close_database() -> None:
    """Close database connections."""
    global engine

    if engine:
        logger.info("Closing database connections...")
        await engine.dispose()
        logger.info("Database connections closed")


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Get an async database session.

    Usage:
        async with get_db_session() as session:
            # Use session here
            result = await session.execute(...)
    """
    if not async_session_maker:
        raise RuntimeError("Database not initialized. Call init_database() first.")

    async with async_session_maker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency for getting database sessions.

    Usage in FastAPI endpoints:
        @app.get("/example")
        async def example(db: AsyncSession = Depends(get_db)):
            # Use db session here
    """
    async with get_db_session() as session:
        yield session


class DatabaseHealthChecker:
    """Database health monitoring utilities."""

    @staticmethod
    async def check_connection() -> bool:
        """Check if database connection is healthy."""
        try:
            async with get_db_session() as session:
                await session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    @staticmethod
    async def get_database_info() -> dict:
        """Get database information for monitoring."""
        try:
            async with get_db_session() as session:
                # Get PostgreSQL version
                version_result = await session.execute(text("SELECT version()"))
                version = version_result.scalar()

                # Get database size
                size_result = await session.execute(text(
                    "SELECT pg_size_pretty(pg_database_size(current_database()))"
                ))
                database_size = size_result.scalar()

                # Get connection count
                conn_result = await session.execute(text(
                    "SELECT count(*) FROM pg_stat_activity WHERE state = 'active'"
                ))
                active_connections = conn_result.scalar()

                return {
                    "version": version,
                    "database_size": database_size,
                    "active_connections": active_connections,
                    "health": "healthy"
                }

        except Exception as e:
            logger.error(f"Failed to get database info: {e}")
            return {
                "health": "unhealthy",
                "error": str(e)
            }


# Export commonly used items
__all__ = [
    "Base",
    "init_database",
    "close_database",
    "get_db_session",
    "get_db",
    "DatabaseHealthChecker"
]