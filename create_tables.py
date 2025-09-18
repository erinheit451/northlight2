#!/usr/bin/env python3
"""
Simple table creation script that creates tables step by step
"""

import asyncio
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.database import init_database, get_db_session
from core.models.book import Campaign, Partner, PartnerOpportunity, DataSnapshot
from sqlalchemy import text


async def create_book_schema():
    """Create the book schema."""
    async with get_db_session() as session:
        await session.execute(text("CREATE SCHEMA IF NOT EXISTS book"))
        await session.commit()
        print("Created book schema")


async def create_tables():
    """Create all tables using SQLAlchemy."""
    from core.database import engine, Base

    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    print("Created all tables")


async def add_sample_data():
    """Add a basic data snapshot record."""
    from datetime import date, datetime

    async with get_db_session() as session:
        snapshot = DataSnapshot(
            snapshot_date=date.today(),
            file_name="manual_setup.py",
            file_size_bytes=1024,
            record_count=0,
            last_modified=datetime.now(),
            is_current=True
        )
        session.add(snapshot)
        await session.commit()
        print("Added initial data snapshot")


async def main():
    """Main function."""
    try:
        await init_database()
        print("Database connected")

        await create_book_schema()
        await create_tables()
        await add_sample_data()

        print("SUCCESS: Database setup complete!")
        print("The /book application should now work")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())