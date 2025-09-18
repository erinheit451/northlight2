#!/usr/bin/env python3
"""
Simplified Data Migration Script
Migrates data from DuckDB and JSON sources to PostgreSQL using asyncpg directly
"""

import asyncio
import sys
import json
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd
import duckdb
import asyncpg
from datetime import datetime, timezone

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

DATABASE_URL = "postgresql://northlight_user:northlight_secure_2024@localhost:5432/unified_northlight"

class SimpleDataMigrator:
    """Simple data migrator using direct asyncpg connection."""

    def __init__(self):
        self.stats = {
            "tables_created": 0,
            "rows_migrated": 0,
            "errors": [],
            "start_time": datetime.now()
        }

    async def create_schemas(self, conn: asyncpg.Connection):
        """Create database schemas if they don't exist."""
        print("🔧 Creating database schemas...")

        schemas = [
            "book",
            "heartbeat_etl",
            "northlight_benchmarks",
            "unified_analytics"
        ]

        for schema in schemas:
            try:
                await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                print(f"✅ Schema created: {schema}")
            except Exception as e:
                print(f"❌ Error creating schema {schema}: {e}")

    async def migrate_heartbeat_duckdb(self, conn: asyncpg.Connection):
        """Migrate data from Heartbeat DuckDB."""
        print("\n📊 Migrating Heartbeat DuckDB data...")

        heartbeat_db_path = "C:/Users/Roci/Heartbeat/data/warehouse/heartbeat.duckdb"
        if not Path(heartbeat_db_path).exists():
            print(f"⚠️ Heartbeat database not found at {heartbeat_db_path}")
            return

        try:
            # Connect to DuckDB
            duck_conn = duckdb.connect(heartbeat_db_path)

            # Get list of tables
            tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            tables_df = duck_conn.execute(tables_query).fetchdf()

            print(f"Found {len(tables_df)} tables in Heartbeat DuckDB")

            for _, row in tables_df.iterrows():
                table_name = row['table_name']
                await self.migrate_duckdb_table(duck_conn, conn, table_name, "heartbeat_etl")

            duck_conn.close()
            print("✅ Heartbeat data migration completed")

        except Exception as e:
            print(f"❌ Error migrating Heartbeat data: {e}")
            self.stats["errors"].append(f"Heartbeat migration: {e}")

    async def migrate_duckdb_table(self, duck_conn, pg_conn: asyncpg.Connection, table_name: str, schema: str):
        """Migrate a single table from DuckDB to PostgreSQL."""
        try:
            print(f"  📋 Migrating table: {table_name}")

            # Get data from DuckDB
            df = duck_conn.execute(f"SELECT * FROM {table_name}").fetchdf()

            if len(df) == 0:
                print(f"    ⚠️ Table {table_name} is empty, skipping")
                return

            # Create PostgreSQL table with simple schema
            await self.create_table_from_dataframe(pg_conn, df, table_name, schema)

            # Insert data in chunks
            chunk_size = 1000
            total_rows = len(df)

            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                await self.insert_dataframe_chunk(pg_conn, chunk, table_name, schema)

            print(f"    ✅ Migrated {total_rows} rows to {schema}.{table_name}")
            self.stats["rows_migrated"] += total_rows
            self.stats["tables_created"] += 1

        except Exception as e:
            print(f"    ❌ Error migrating table {table_name}: {e}")
            self.stats["errors"].append(f"Table {table_name}: {e}")

    async def create_table_from_dataframe(self, conn: asyncpg.Connection, df: pd.DataFrame, table_name: str, schema: str):
        """Create PostgreSQL table based on DataFrame structure."""
        # Map pandas dtypes to PostgreSQL types
        type_mapping = {
            'object': 'TEXT',
            'int64': 'BIGINT',
            'int32': 'INTEGER',
            'float64': 'DOUBLE PRECISION',
            'float32': 'REAL',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'datetime64[ns, UTC]': 'TIMESTAMP WITH TIME ZONE'
        }

        columns = []
        for col_name, dtype in df.dtypes.items():
            pg_type = type_mapping.get(str(dtype), 'TEXT')
            # Clean column name for PostgreSQL
            clean_name = col_name.replace(' ', '_').replace('-', '_').lower()
            columns.append(f'"{clean_name}" {pg_type}')

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            id SERIAL PRIMARY KEY,
            {', '.join(columns)},
            migrated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        """

        await conn.execute(create_sql)

    async def insert_dataframe_chunk(self, conn: asyncpg.Connection, df: pd.DataFrame, table_name: str, schema: str):
        """Insert a chunk of DataFrame data into PostgreSQL."""
        if len(df) == 0:
            return

        # Clean column names
        df.columns = [col.replace(' ', '_').replace('-', '_').lower() for col in df.columns]

        # Convert DataFrame to list of tuples
        data_tuples = []
        for _, row in df.iterrows():
            # Convert NaN to None and handle different data types
            clean_row = []
            for val in row:
                if pd.isna(val):
                    clean_row.append(None)
                elif isinstance(val, (pd.Timestamp, datetime)):
                    clean_row.append(val)
                else:
                    clean_row.append(val)
            data_tuples.append(tuple(clean_row))

        # Create insert query
        placeholders = ', '.join(['$' + str(i+1) for i in range(len(df.columns))])
        columns = ', '.join([f'"{col}"' for col in df.columns])

        insert_sql = f"INSERT INTO {schema}.{table_name} ({columns}) VALUES ({placeholders})"

        # Execute batch insert
        await conn.executemany(insert_sql, data_tuples)

    async def migrate_northlight_json(self, conn: asyncpg.Connection):
        """Migrate Northlight JSON data."""
        print("\n🔍 Migrating Northlight JSON data...")

        json_files = [
            ("C:/Users/Roci/northlight/data.json", "benchmark_data"),
            ("C:/Users/Roci/northlight/advertisers.json", "advertisers"),
            ("C:/Users/Roci/northlight/partners.json", "partners")
        ]

        for file_path, table_name in json_files:
            if not Path(file_path).exists():
                print(f"⚠️ File not found: {file_path}")
                continue

            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)

                await self.migrate_json_data(conn, data, table_name, "northlight_benchmarks")
                print(f"✅ Migrated {file_path} to northlight_benchmarks.{table_name}")

            except Exception as e:
                print(f"❌ Error migrating {file_path}: {e}")
                self.stats["errors"].append(f"JSON {file_path}: {e}")

    async def migrate_json_data(self, conn: asyncpg.Connection, data: Any, table_name: str, schema: str):
        """Migrate JSON data to PostgreSQL table."""
        # Create table for JSON data
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            id SERIAL PRIMARY KEY,
            data JSONB,
            migrated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        """
        await conn.execute(create_sql)

        # Insert JSON data
        if isinstance(data, list):
            # Insert each item as a separate row
            for item in data:
                await conn.execute(
                    f"INSERT INTO {schema}.{table_name} (data) VALUES ($1)",
                    json.dumps(item)
                )
            self.stats["rows_migrated"] += len(data)
        else:
            # Insert single JSON object
            await conn.execute(
                f"INSERT INTO {schema}.{table_name} (data) VALUES ($1)",
                json.dumps(data)
            )
            self.stats["rows_migrated"] += 1

        self.stats["tables_created"] += 1

    async def migrate_parquet_files(self, conn: asyncpg.Connection):
        """Migrate recent parquet files."""
        print("\n📁 Migrating recent Parquet files...")

        parquet_dir = Path("C:/Users/Roci/Heartbeat/data/warehouse")
        if not parquet_dir.exists():
            print("⚠️ Parquet directory not found")
            return

        parquet_files = list(parquet_dir.glob("*.parquet"))
        print(f"Found {len(parquet_files)} parquet files")

        for parquet_file in parquet_files[:10]:  # Limit to recent 10 files
            try:
                df = pd.read_parquet(parquet_file)
                table_name = parquet_file.stem.replace('-', '_')

                await self.create_table_from_dataframe(conn, df, table_name, "heartbeat_etl")

                # Insert data in chunks
                chunk_size = 1000
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i:i+chunk_size]
                    await self.insert_dataframe_chunk(conn, chunk, table_name, "heartbeat_etl")

                print(f"✅ Migrated {len(df)} rows from {parquet_file.name}")
                self.stats["rows_migrated"] += len(df)
                self.stats["tables_created"] += 1

            except Exception as e:
                print(f"❌ Error migrating {parquet_file.name}: {e}")
                self.stats["errors"].append(f"Parquet {parquet_file.name}: {e}")

    async def migrate_book_data(self, conn: asyncpg.Connection):
        """Migrate specific data needed for the book system."""
        print("\n📋 Migrating Book System data...")

        # Check for specific campaign/book data in DuckDB files
        duckdb_files = [
            ("C:/Users/Roci/Heartbeat/data/warehouse/heartbeat.duckdb", "heartbeat"),
            ("C:/Users/Roci/Heartbeat/data/warehouse/northlight.duckdb", "northlight")
        ]

        for db_path, db_name in duckdb_files:
            if not Path(db_path).exists():
                continue

            try:
                duck_conn = duckdb.connect(db_path)

                # Get all tables and look for campaign-related data
                tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                tables_df = duck_conn.execute(tables_query).fetchdf()

                print(f"  📊 Checking {db_name} database: {len(tables_df)} tables")

                for _, row in tables_df.iterrows():
                    table_name = row['table_name']

                    # Check if table has campaign-related columns
                    try:
                        sample_query = f"SELECT * FROM {table_name} LIMIT 1"
                        sample_df = duck_conn.execute(sample_query).fetchdf()

                        if len(sample_df) > 0:
                            columns = [col.lower() for col in sample_df.columns]
                            campaign_indicators = ['campaign', 'advertiser', 'partner', 'cid', 'maid', 'budget']

                            if any(indicator in ' '.join(columns) for indicator in campaign_indicators):
                                print(f"    📋 Found campaign data in {table_name}: {len(sample_df.columns)} columns")

                                # Get full data
                                full_df = duck_conn.execute(f"SELECT * FROM {table_name}").fetchdf()
                                if len(full_df) > 0:
                                    await self.migrate_book_table(conn, full_df, f"{db_name}_{table_name}")

                    except Exception as e:
                        # Skip tables that can't be read
                        continue

                duck_conn.close()

            except Exception as e:
                print(f"    ❌ Error checking {db_name}: {e}")

    async def migrate_book_table(self, conn: asyncpg.Connection, df: pd.DataFrame, source_name: str):
        """Migrate a table to the book schema with campaign data mapping."""
        try:
            print(f"    📊 Migrating {len(df)} rows from {source_name}")

            # Clean column names for PostgreSQL
            df.columns = [col.replace(' ', '_').replace('-', '_').lower() for col in df.columns]

            # Create a flexible table in book schema
            columns = []
            for col_name, dtype in df.dtypes.items():
                pg_type = 'TEXT'  # Use TEXT for flexibility during migration
                if 'int' in str(dtype):
                    pg_type = 'BIGINT'
                elif 'float' in str(dtype):
                    pg_type = 'DOUBLE PRECISION'
                elif 'bool' in str(dtype):
                    pg_type = 'BOOLEAN'

                columns.append(f'"{col_name}" {pg_type}')

            table_name = f"raw_{source_name}"
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS book.{table_name} (
                id SERIAL PRIMARY KEY,
                {', '.join(columns)},
                migrated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
            """

            await conn.execute(create_sql)

            # Insert data in chunks
            chunk_size = 500
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                await self.insert_dataframe_chunk(conn, chunk, table_name, "book")

            print(f"    ✅ Migrated {len(df)} rows to book.{table_name}")
            self.stats["rows_migrated"] += len(df)
            self.stats["tables_created"] += 1

        except Exception as e:
            print(f"    ❌ Error migrating {source_name}: {e}")
            self.stats["errors"].append(f"Book table {source_name}: {e}")

    async def run_migration(self):
        """Run the complete data migration."""
        print("🚀 Starting Simple Data Migration")
        print("=" * 60)

        try:
            # Connect to PostgreSQL
            conn = await asyncpg.connect(DATABASE_URL)
            print("✅ Connected to PostgreSQL")

            # Create schemas
            await self.create_schemas(conn)

            # Migrate all data sources
            await self.migrate_heartbeat_duckdb(conn)
            await self.migrate_northlight_json(conn)
            await self.migrate_parquet_files(conn)
            await self.migrate_book_data(conn)

            await conn.close()

        except Exception as e:
            print(f"❌ Migration failed: {e}")
            self.stats["errors"].append(f"Connection error: {e}")

        # Print final stats
        self.stats["end_time"] = datetime.now()
        duration = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()

        print("\n" + "=" * 60)
        print("MIGRATION SUMMARY")
        print("=" * 60)
        print(f"Tables Created: {self.stats['tables_created']}")
        print(f"Rows Migrated: {self.stats['rows_migrated']:,}")
        print(f"Duration: {duration:.1f} seconds")
        print(f"Errors: {len(self.stats['errors'])}")

        if self.stats["errors"]:
            print("\nErrors encountered:")
            for error in self.stats["errors"][:5]:  # Show first 5 errors
                print(f"  - {error}")

        if self.stats["tables_created"] > 0:
            print("\n🎉 Migration completed successfully!")
            return True
        else:
            print("\n⚠️ Migration completed with issues")
            return False

async def main():
    """Main migration function."""
    migrator = SimpleDataMigrator()
    success = await migrator.run_migration()
    return 0 if success else 1

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nMigration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Migration failed: {e}")
        sys.exit(1)