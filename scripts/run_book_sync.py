"""
Standalone script to sync book schema tables.
Use this to manually sync data without running full ETL.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.sync_book_performance_data import sync_all_book_tables


if __name__ == "__main__":
    print("Starting book schema sync...")
    try:
        asyncio.run(sync_all_book_tables())
        print("[SUCCESS] Sync completed successfully!")
        sys.exit(0)
    except Exception as e:
        print(f"[FAILED] Sync failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
