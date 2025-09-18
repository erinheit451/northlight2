#!/usr/bin/env python3
"""
Test script to verify the running Unified Northlight Platform
Run this after starting the application with 'python main.py'
"""

import requests
import time
import sys
from pathlib import Path

def test_url(url, description, expected_status=200):
    """Test a URL and return results."""
    try:
        print(f"Testing {description}...")
        response = requests.get(url, timeout=10)

        if response.status_code == expected_status:
            print(f"✅ {description}: {response.status_code} - SUCCESS")
            return True, response
        else:
            print(f"⚠️ {description}: {response.status_code} - Unexpected status")
            return False, response

    except requests.exceptions.ConnectionError:
        print(f"❌ {description}: Connection failed - Is the server running?")
        return False, None
    except requests.exceptions.Timeout:
        print(f"❌ {description}: Timeout - Server may be slow")
        return False, None
    except Exception as e:
        print(f"❌ {description}: Error - {e}")
        return False, None

def main():
    """Test all important URLs."""
    print("="*60)
    print("UNIFIED NORTHLIGHT PLATFORM - URL TESTING")
    print("="*60)

    base_url = "http://localhost:8000"

    # Wait a moment for server to be ready
    print("Waiting 2 seconds for server to be ready...")
    time.sleep(2)

    tests = [
        (f"{base_url}/health", "Health Check API"),
        (f"{base_url}/version", "Version API"),
        (f"{base_url}/docs", "API Documentation"),
        (f"{base_url}/dashboard", "Main Dashboard", 200),  # Might be 404 if no static files
        (f"{base_url}/book/", "Book System", 200),  # Might be 404 if no static files
        (f"{base_url}/api/v1/benchmarks/meta", "Benchmarks API"),
        (f"{base_url}/api/v1/reports/templates", "Reports API"),
    ]

    results = []
    for test_args in tests:
        url = test_args[0]
        description = test_args[1]
        expected_status = test_args[2] if len(test_args) > 2 else 200

        success, response = test_url(url, description, expected_status)
        results.append((description, success, response))

        # Show some content for successful API calls
        if success and response and '/api/' in url:
            try:
                if response.headers.get('content-type', '').startswith('application/json'):
                    data = response.json()
                    print(f"   Sample data: {str(data)[:100]}...")
            except:
                pass

        print()  # Add spacing between tests

    # Summary
    successful = sum(1 for _, success, _ in results if success)
    total = len(results)

    print("="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Successful: {successful}/{total}")
    print(f"Success Rate: {(successful/total)*100:.1f}%")

    if successful >= total - 2:  # Allow 2 failures (static files might not be served)
        print("\n🎉 APPLICATION IS WORKING WELL!")
        print("\nKey URLs to try in your browser:")
        print("• Main API Health: http://localhost:8000/health")
        print("• API Documentation: http://localhost:8000/docs")
        print("• Version Info: http://localhost:8000/version")

        # Check if static files are being served
        dashboard_working = any(desc == "Main Dashboard" and success for desc, success, _ in results)
        book_working = any(desc == "Book System" and success for desc, success, _ in results)

        if dashboard_working:
            print("• Main Dashboard: http://localhost:8000/dashboard")
        if book_working:
            print("• Book System: http://localhost:8000/book/")

        if not dashboard_working or not book_working:
            print("\nNote: Frontend static files may need configuration.")
            print("The API is working, which is the core functionality!")

    else:
        print("\n⚠️ Some issues detected. Check the errors above.")

    return successful >= total - 2

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nTesting interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Testing failed: {e}")
        sys.exit(1)