#!/usr/bin/env python3
"""
Book System Setup Script
Sets up the complete book system with database tables and sample data
"""

import asyncio
import sys
import os
from pathlib import Path
import subprocess
import time

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.database import init_database, engine
from core.config import settings


async def run_sql_file(sql_file_path: Path):
    """Execute a SQL file against the database."""
    print(f"Executing SQL file: {sql_file_path.name}")

    try:
        # Read the SQL file
        sql_content = sql_file_path.read_text(encoding='utf-8')

        # Execute the SQL
        async with engine.begin() as conn:
            await conn.execute(text(sql_content))

        print(f"OK {sql_file_path.name} executed successfully")
        return True

    except Exception as e:
        print(f"ERROR executing {sql_file_path.name}: {e}")
        return False


async def setup_database_schema():
    """Set up the database schema by running SQL init files."""
    print("Setting up database schema...")

    # Get SQL files in order
    sql_dir = project_root / "database" / "init"
    sql_files = [
        "01_init_schema.sql",
        "03_northlight_tables.sql",
        "04_unified_analytics.sql",
        "05_book_tables.sql"
    ]

    success_count = 0
    for sql_file in sql_files:
        sql_path = sql_dir / sql_file
        if sql_path.exists():
            if await run_sql_file(sql_path):
                success_count += 1
        else:
            print(f"⚠️ SQL file not found: {sql_file}")

    return success_count == len(sql_files)


async def load_sample_data():
    """Load sample data using the migration script."""
    print("Loading sample data...")

    try:
        # Import and run the migration
        from scripts.migrate_book_data import main as migrate_main
        await migrate_main()
        return True

    except Exception as e:
        print(f"❌ Error loading sample data: {e}")
        import traceback
        traceback.print_exc()
        return False


async def verify_setup():
    """Verify that the setup was successful."""
    print("Verifying setup...")

    try:
        from core.models.book import Campaign, Partner, DataSnapshot
        from core.database import get_db_session

        async with get_db_session() as session:
            # Check campaigns
            from sqlalchemy import select, func
            campaign_count = await session.scalar(select(func.count(Campaign.id)))
            partner_count = await session.scalar(select(func.count(Partner.id)))
            snapshot_count = await session.scalar(select(func.count(DataSnapshot.id)))

            print(f"✅ Verification complete:")
            print(f"   - Campaigns: {campaign_count}")
            print(f"   - Partners: {partner_count}")
            print(f"   - Data Snapshots: {snapshot_count}")

            return campaign_count > 0 and partner_count > 0

    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False


def test_api_endpoints():
    """Test the API endpoints to ensure they're working."""
    print("Testing API endpoints...")

    try:
        import requests
        base_url = f"http://{settings.API_HOST}:{settings.API_PORT}"

        # Test health endpoint
        response = requests.get(f"{base_url}/health", timeout=5)
        if response.status_code == 200:
            print("✅ Health endpoint working")
        else:
            print(f"⚠️ Health endpoint returned {response.status_code}")

        # Test book endpoints (these will only work if server is running)
        endpoints = [
            "/api/book/summary",
            "/api/book/all",
            "/api/book/partners",
            "/api/book/metadata"
        ]

        working_endpoints = 0
        for endpoint in endpoints:
            try:
                response = requests.get(f"{base_url}{endpoint}", timeout=5)
                if response.status_code == 200:
                    print(f"✅ {endpoint} working")
                    working_endpoints += 1
                else:
                    print(f"⚠️ {endpoint} returned {response.status_code}")
            except requests.exceptions.ConnectionError:
                print(f"⚠️ {endpoint} - Connection failed (server not running)")
            except Exception as e:
                print(f"⚠️ {endpoint} - Error: {e}")

        return working_endpoints > 0

    except ImportError:
        print("⚠️ requests module not available, skipping API tests")
        return True
    except Exception as e:
        print(f"⚠️ API testing failed: {e}")
        return True  # Don't fail setup for API test issues


async def main():
    """Main setup function."""
    print("="*60)
    print("BOOK SYSTEM SETUP")
    print("="*60)

    success = True

    try:
        # Step 1: Initialize database connection
        print("\n1. Initializing database connection...")
        await init_database()
        print("✅ Database connection established")

        # Step 2: Set up database schema
        print("\n2. Setting up database schema...")
        if await setup_database_schema():
            print("✅ Database schema setup complete")
        else:
            print("❌ Database schema setup failed")
            success = False

        # Step 3: Load sample data
        if success:
            print("\n3. Loading sample data...")
            if await load_sample_data():
                print("✅ Sample data loaded successfully")
            else:
                print("❌ Sample data loading failed")
                success = False

        # Step 4: Verify setup
        if success:
            print("\n4. Verifying setup...")
            if await verify_setup():
                print("✅ Setup verification passed")
            else:
                print("❌ Setup verification failed")
                success = False

        # Step 5: Test API endpoints (if server is running)
        print("\n5. Testing API endpoints...")
        test_api_endpoints()

        # Summary
        print("\n" + "="*60)
        if success:
            print("🎉 BOOK SYSTEM SETUP COMPLETE!")
            print("\nNext steps:")
            print("1. Start the server: python main.py")
            print("2. Open your browser to: http://localhost:8000/frontend/book/index.html")
            print("3. Check the API docs: http://localhost:8000/docs")
            print("\nAPI Endpoints available:")
            print("• http://localhost:8000/api/book/summary")
            print("• http://localhost:8000/api/book/all")
            print("• http://localhost:8000/api/book/partners")
            print("• http://localhost:8000/api/book/metadata")
        else:
            print("❌ SETUP FAILED")
            print("Please check the errors above and try again.")

        return success

    except Exception as e:
        print(f"\n❌ Setup failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # Need to import these after path setup
    from sqlalchemy import text

    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nSetup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Setup failed: {e}")
        sys.exit(1)