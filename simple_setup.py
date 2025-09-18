#!/usr/bin/env python3
"""
Simple Book System Setup Script
Sets up the database and loads sample data without Unicode characters
"""

import asyncio
import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.database import init_database, get_db_session
from core.config import settings
from sqlalchemy import text


async def run_sql_file(sql_file_path: Path):
    """Execute a SQL file against the database."""
    print(f"Executing SQL file: {sql_file_path.name}")

    try:
        # Read the SQL file
        sql_content = sql_file_path.read_text(encoding='utf-8')

        # Execute the SQL
        async with get_db_session() as session:
            await session.execute(text(sql_content))
            await session.commit()

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
            print(f"WARNING SQL file not found: {sql_file}")

    return success_count >= 3  # Allow some files to be missing


async def load_sample_data():
    """Load sample data using the migration script."""
    print("Loading sample data...")

    try:
        # Import and run the migration
        from scripts.migrate_book_data import main as migrate_main
        await migrate_main()
        return True

    except Exception as e:
        print(f"ERROR loading sample data: {e}")
        import traceback
        traceback.print_exc()
        return False


async def verify_setup():
    """Verify that the setup was successful."""
    print("Verifying setup...")

    try:
        from core.models.book import Campaign, Partner, DataSnapshot
        from core.database import get_db_session
        from sqlalchemy import select, func

        async with get_db_session() as session:
            # Check campaigns
            campaign_count = await session.scalar(select(func.count(Campaign.id)))
            partner_count = await session.scalar(select(func.count(Partner.id)))
            snapshot_count = await session.scalar(select(func.count(DataSnapshot.id)))

            print(f"OK Verification complete:")
            print(f"   - Campaigns: {campaign_count}")
            print(f"   - Partners: {partner_count}")
            print(f"   - Data Snapshots: {snapshot_count}")

            return campaign_count > 0 and partner_count > 0

    except Exception as e:
        print(f"ERROR Verification failed: {e}")
        return False


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
        print("OK Database connection established")

        # Step 2: Set up database schema
        print("\n2. Setting up database schema...")
        if await setup_database_schema():
            print("OK Database schema setup complete")
        else:
            print("ERROR Database schema setup failed")
            success = False

        # Step 3: Load sample data
        if success:
            print("\n3. Loading sample data...")
            if await load_sample_data():
                print("OK Sample data loaded successfully")
            else:
                print("ERROR Sample data loading failed")
                success = False

        # Step 4: Verify setup
        if success:
            print("\n4. Verifying setup...")
            if await verify_setup():
                print("OK Setup verification passed")
            else:
                print("ERROR Setup verification failed")
                success = False

        # Summary
        print("\n" + "="*60)
        if success:
            print("SUCCESS: BOOK SYSTEM SETUP COMPLETE!")
            print("\nNext steps:")
            print("1. Start the server: python main.py")
            print("2. Open your browser to: http://localhost:8000/frontend/book/index.html")
            print("3. Check the API docs: http://localhost:8000/docs")
        else:
            print("ERROR: SETUP FAILED")
            print("Please check the errors above and try again.")

        return success

    except Exception as e:
        print(f"\nERROR Setup failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nSetup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Setup failed: {e}")
        sys.exit(1)