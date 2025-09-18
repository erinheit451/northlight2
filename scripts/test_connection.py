#!/usr/bin/env python3
"""
Test database connection and basic functionality
"""

import sys
import asyncio
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.config import settings
from core.database import init_database, get_db_session, DatabaseHealthChecker
from core.shared import setup_logging, get_logger
import asyncpg
import psycopg2

# Setup logging
setup_logging()
logger = get_logger(__name__)


async def test_asyncpg_connection():
    """Test direct asyncpg connection."""
    try:
        logger.info("Testing asyncpg connection...")

        # Parse connection URL
        db_url = settings.DATABASE_URL
        if db_url.startswith("postgresql://"):
            db_url = db_url.replace("postgresql://", "")

        # Extract connection components
        # Format: user:password@host:port/database
        user_pass, host_db = db_url.split("@")
        user, password = user_pass.split(":")
        host_port, database = host_db.split("/")
        host, port = host_port.split(":")

        conn = await asyncpg.connect(
            user=user,
            password=password,
            database=database,
            host=host,
            port=int(port)
        )

        # Test query
        result = await conn.fetchval("SELECT version()")
        logger.info(f"✅ AsyncPG connection successful")
        logger.info(f"PostgreSQL version: {result}")

        await conn.close()
        return True

    except Exception as e:
        logger.error(f"❌ AsyncPG connection failed: {e}")
        return False


def test_psycopg2_connection():
    """Test direct psycopg2 connection."""
    try:
        logger.info("Testing psycopg2 connection...")

        db_url = settings.DATABASE_URL.replace("postgresql://", "postgres://")
        conn = psycopg2.connect(db_url)

        with conn.cursor() as cursor:
            cursor.execute("SELECT version()")
            result = cursor.fetchone()[0]
            logger.info(f"✅ Psycopg2 connection successful")
            logger.info(f"PostgreSQL version: {result}")

        conn.close()
        return True

    except Exception as e:
        logger.error(f"❌ Psycopg2 connection failed: {e}")
        return False


async def test_sqlalchemy_connection():
    """Test SQLAlchemy async connection."""
    try:
        logger.info("Testing SQLAlchemy connection...")

        await init_database()

        async with get_db_session() as session:
            result = await session.execute("SELECT 'SQLAlchemy test successful'")
            message = result.scalar()
            logger.info(f"✅ {message}")

        return True

    except Exception as e:
        logger.error(f"❌ SQLAlchemy connection failed: {e}")
        return False


async def test_database_health():
    """Test database health checker."""
    try:
        logger.info("Testing database health checker...")

        is_healthy = await DatabaseHealthChecker.check_connection()
        if is_healthy:
            logger.info("✅ Database health check passed")

            info = await DatabaseHealthChecker.get_database_info()
            logger.info(f"Database info: {info}")
            return True
        else:
            logger.error("❌ Database health check failed")
            return False

    except Exception as e:
        logger.error(f"❌ Database health check error: {e}")
        return False


async def test_schema_existence():
    """Test if required schemas exist."""
    try:
        logger.info("Checking database schemas...")

        async with get_db_session() as session:
            result = await session.execute("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name IN (
                    'heartbeat_core',
                    'heartbeat_performance',
                    'heartbeat_salesforce',
                    'heartbeat_reporting',
                    'heartbeat_standards',
                    'northlight_benchmarks',
                    'unified_analytics'
                )
                ORDER BY schema_name
            """)

            schemas = [row[0] for row in result.fetchall()]

            expected_schemas = [
                'heartbeat_core',
                'heartbeat_performance',
                'heartbeat_salesforce',
                'heartbeat_reporting',
                'heartbeat_standards',
                'northlight_benchmarks',
                'unified_analytics'
            ]

            missing_schemas = [s for s in expected_schemas if s not in schemas]

            if missing_schemas:
                logger.warning(f"⚠️  Missing schemas: {missing_schemas}")
                logger.info("Run database initialization to create schemas")
            else:
                logger.info("✅ All required schemas exist")

            logger.info(f"Found schemas: {schemas}")
            return len(missing_schemas) == 0

    except Exception as e:
        logger.error(f"❌ Schema check failed: {e}")
        return False


async def main():
    """Run all connection tests."""
    logger.info("🚀 Starting database connection tests...")
    logger.info(f"Database URL: {settings.DATABASE_URL}")

    tests = [
        ("Direct AsyncPG", test_asyncpg_connection),
        ("Direct Psycopg2", lambda: test_psycopg2_connection()),
        ("SQLAlchemy Async", test_sqlalchemy_connection),
        ("Health Checker", test_database_health),
        ("Schema Check", test_schema_existence),
    ]

    results = []
    for test_name, test_func in tests:
        logger.info(f"\n--- {test_name} Test ---")
        try:
            if asyncio.iscoroutinefunction(test_func):
                result = await test_func()
            else:
                result = test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"Test {test_name} failed with exception: {e}")
            results.append((test_name, False))

    # Summary
    logger.info("\n" + "="*50)
    logger.info("TEST SUMMARY")
    logger.info("="*50)

    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{test_name}: {status}")
        if result:
            passed += 1

    logger.info(f"\nPassed: {passed}/{len(results)} tests")

    if passed == len(results):
        logger.info("🎉 All tests passed! Database is ready.")
        return 0
    else:
        logger.error("❌ Some tests failed. Check configuration and database status.")
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test suite failed: {e}")
        sys.exit(1)