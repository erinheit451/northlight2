#!/usr/bin/env python3
"""Quick database connection test"""

import asyncio
import asyncpg

async def test_connection():
    try:
        print("Testing database connection...")
        conn = await asyncpg.connect('postgresql://northlight_user:northlight_secure_2024@localhost:5432/unified_northlight')
        print('✅ Database connection successful!')

        # Test basic query
        result = await conn.fetchval('SELECT version()')
        print(f'PostgreSQL version: {result[:50]}...')

        await conn.close()
        return True
    except Exception as e:
        print(f'❌ Database connection failed: {e}')
        return False

if __name__ == "__main__":
    success = asyncio.run(test_connection())
    print(f"Connection test: {'PASSED' if success else 'FAILED'}")