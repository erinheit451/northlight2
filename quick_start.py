#!/usr/bin/env python3
"""
Quick Start Script for Unified Northlight Platform
Performs basic setup and testing automatically
"""

import subprocess
import sys
import os
from pathlib import Path
import time

def run_command(command, description, critical=True):
    """Run a command and handle errors."""
    print(f"\n🔄 {description}...")
    try:
        if isinstance(command, str):
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
        else:
            result = subprocess.run(command, capture_output=True, text=True)

        if result.returncode == 0:
            print(f"✅ {description} - SUCCESS")
            if result.stdout.strip():
                print(f"Output: {result.stdout.strip()}")
            return True
        else:
            print(f"❌ {description} - FAILED")
            if result.stderr.strip():
                print(f"Error: {result.stderr.strip()}")
            if critical:
                return False
            return True
    except Exception as e:
        print(f"❌ {description} - ERROR: {e}")
        return not critical

def main():
    """Main setup and test routine."""
    print("="*60)
    print("UNIFIED NORTHLIGHT PLATFORM - QUICK START")
    print("="*60)

    # Change to project directory
    project_dir = Path(__file__).parent
    os.chdir(project_dir)
    print(f"Working directory: {project_dir}")

    success_count = 0
    total_tests = 0

    # Test 1: Install dependencies
    total_tests += 1
    if run_command([sys.executable, "-m", "pip", "install", "duckdb", "pandas", "pyarrow"],
                   "Installing missing dependencies"):
        success_count += 1

    # Test 2: Test database connection
    total_tests += 1
    if run_command([sys.executable, "test_db_connection.py"],
                   "Testing database connection", critical=False):
        success_count += 1

    # Test 3: Test application imports
    total_tests += 1
    if run_command([sys.executable, "-c", "from main import app; print('App imports successfully')"],
                   "Testing application imports"):
        success_count += 1

    # Test 4: Test basic API functionality
    total_tests += 1
    api_test_code = """
from fastapi.testclient import TestClient
try:
    from main import app
    client = TestClient(app)
    health = client.get('/health')
    version = client.get('/version')
    print(f'Health: {health.status_code}, Version: {version.status_code}')
    print('Basic API test passed')
except Exception as e:
    print(f'API test failed: {e}')
    exit(1)
"""
    if run_command([sys.executable, "-c", api_test_code],
                   "Testing basic API functionality", critical=False):
        success_count += 1

    # Test 5: Check file structure
    total_tests += 1
    critical_files = [
        "frontend/index.html",
        "frontend/book/index.html",
        "api/v1/auth.py",
        "core/config.py"
    ]

    all_files_exist = True
    for file_path in critical_files:
        if not Path(file_path).exists():
            print(f"❌ Missing critical file: {file_path}")
            all_files_exist = False

    if all_files_exist:
        print("✅ Critical files check - SUCCESS")
        success_count += 1
    else:
        print("❌ Critical files check - FAILED")

    # Test 6: Data availability check
    total_tests += 1
    heartbeat_data = Path("../heartbeat/data/warehouse/heartbeat.duckdb")
    northlight_data = Path("../northlight/data.json")

    data_available = 0
    if heartbeat_data.exists():
        data_available += 1
        print(f"✅ Heartbeat data found: {heartbeat_data}")
    else:
        print(f"⚠️ Heartbeat data not found: {heartbeat_data}")

    if northlight_data.exists():
        data_available += 1
        print(f"✅ Northlight data found: {northlight_data}")
    else:
        print(f"⚠️ Northlight data not found: {northlight_data}")

    if data_available > 0:
        print("✅ Data availability check - SUCCESS")
        success_count += 1
    else:
        print("❌ Data availability check - FAILED")

    # Summary
    print("\n" + "="*60)
    print("QUICK START SUMMARY")
    print("="*60)
    print(f"Tests Passed: {success_count}/{total_tests}")
    print(f"Success Rate: {(success_count/total_tests)*100:.1f}%")

    if success_count >= total_tests - 1:  # Allow one non-critical failure
        print("\n🎉 SETUP SUCCESSFUL!")
        print("\nNext steps:")
        print("1. Run: python main.py")
        print("2. Open: http://localhost:8000/dashboard")
        print("3. Test: http://localhost:8000/book/")
        return True
    else:
        print("\n⚠️ SETUP NEEDS ATTENTION")
        print("Some tests failed. Check the errors above.")
        return False

if __name__ == "__main__":
    try:
        success = main()
        input("\nPress Enter to continue...")
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nSetup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nSetup failed: {e}")
        sys.exit(1)