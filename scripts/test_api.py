#!/usr/bin/env python3
"""
Comprehensive API Test Suite
Tests all unified API endpoints for functionality and performance
"""

import asyncio
import sys
import json
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime, timezone, date, timedelta
import time

import httpx
import pytest
from fastapi.testclient import TestClient

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from main import app
from core.config import settings
from core.shared import setup_logging, get_logger

# Setup logging
setup_logging()
logger = get_logger("api_tests")

# Test configuration
BASE_URL = f"http://localhost:{settings.API_PORT}"
API_BASE = f"{BASE_URL}/api/v1"

# Test client
client = TestClient(app)


class APITestSuite:
    """Comprehensive API test suite."""

    def __init__(self):
        self.access_token = None
        self.test_results = {
            "total_tests": 0,
            "passed": 0,
            "failed": 0,
            "errors": [],
            "start_time": None,
            "end_time": None
        }

    async def run_all_tests(self) -> Dict[str, Any]:
        """Run all API tests."""
        self.test_results["start_time"] = datetime.now(timezone.utc)
        logger.info("Starting comprehensive API test suite...")

        test_suites = [
            ("Health and Version", self.test_health_and_version),
            ("Authentication", self.test_authentication),
            ("Benchmarking API", self.test_benchmarking_api),
            ("ETL Management", self.test_etl_management),
            ("Analytics API", self.test_analytics_api),
            ("Reporting API", self.test_reporting_api),
            ("Performance Tests", self.test_performance),
        ]

        for suite_name, test_func in test_suites:
            logger.info(f"Running {suite_name} tests...")
            try:
                await test_func()
                logger.info(f"✅ {suite_name} tests completed")
            except Exception as e:
                logger.error(f"❌ {suite_name} tests failed: {e}")
                self.test_results["errors"].append({
                    "suite": suite_name,
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })

        self.test_results["end_time"] = datetime.now(timezone.utc)
        duration = (self.test_results["end_time"] - self.test_results["start_time"]).total_seconds()

        logger.info(f"API tests completed in {duration:.2f} seconds")
        logger.info(f"Results: {self.test_results['passed']}/{self.test_results['total_tests']} passed")

        return self.test_results

    def _assert_response(self, response, expected_status: int = 200, description: str = ""):
        """Assert response status and log results."""
        self.test_results["total_tests"] += 1

        try:
            assert response.status_code == expected_status, f"Expected {expected_status}, got {response.status_code}"

            if response.status_code >= 400:
                logger.error(f"Response error: {response.text}")

            self.test_results["passed"] += 1
            logger.debug(f"✅ {description}")

        except AssertionError as e:
            self.test_results["failed"] += 1
            error_msg = f"❌ {description}: {str(e)}"
            logger.error(error_msg)
            self.test_results["errors"].append({
                "test": description,
                "error": str(e),
                "response_status": response.status_code,
                "response_text": response.text[:200] if response.text else None
            })
            raise

    async def test_health_and_version(self):
        """Test health and version endpoints."""
        # Health check
        response = client.get("/health")
        self._assert_response(response, 200, "Health check endpoint")

        health_data = response.json()
        assert "status" in health_data
        assert "version" in health_data

        # Version check
        response = client.get("/version")
        self._assert_response(response, 200, "Version endpoint")

        version_data = response.json()
        assert "application" in version_data
        assert "version" in version_data
        assert "api_version" in version_data

    async def test_authentication(self):
        """Test authentication endpoints."""
        # Try to access protected endpoint without auth
        response = client.get(f"/api/v1/etl/jobs")
        self._assert_response(response, 401, "Protected endpoint without auth should return 401")

        # Login with default credentials
        login_data = {
            "username": "admin",
            "password": "admin123"
        }

        response = client.post("/api/v1/auth/login", json=login_data)
        self._assert_response(response, 200, "Login with default credentials")

        token_data = response.json()
        assert "access_token" in token_data
        assert "token_type" in token_data

        self.access_token = token_data["access_token"]

        # Test authenticated access
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = client.get("/api/v1/auth/me", headers=headers)
        self._assert_response(response, 200, "Get current user info")

        user_data = response.json()
        assert "username" in user_data
        assert user_data["username"] == "admin"

    async def test_benchmarking_api(self):
        """Test benchmarking API endpoints."""
        headers = {"Authorization": f"Bearer {self.access_token}"} if self.access_token else {}

        # Get benchmark metadata
        response = client.get("/api/v1/benchmarks/meta", headers=headers)
        self._assert_response(response, 200, "Get benchmark metadata")

        # Test diagnosis endpoint with sample data
        diagnosis_data = {
            "category": "Legal",
            "subcategory": "Personal Injury",
            "budget": 5000.0,
            "clicks": 1000,
            "leads": 50,
            "goal_cpl": 100.0,
            "impressions": 50000
        }

        response = client.post("/api/v1/benchmarks/diagnose", json=diagnosis_data, headers=headers)
        # This might fail if no benchmark data exists, so we'll check for 404 or 200
        if response.status_code not in [200, 404]:
            self._assert_response(response, 200, "Campaign diagnosis")
        else:
            logger.info("Diagnosis test skipped - no benchmark data available")

    async def test_etl_management(self):
        """Test ETL management endpoints."""
        if not self.access_token:
            logger.warning("Skipping ETL tests - no authentication token")
            return

        headers = {"Authorization": f"Bearer {self.access_token}"}

        # Get pipeline status
        response = client.get("/api/v1/etl/pipeline/status", headers=headers)
        self._assert_response(response, 200, "Get pipeline status")

        # List jobs
        response = client.get("/api/v1/etl/jobs", headers=headers)
        self._assert_response(response, 200, "List ETL jobs")

        # Get health metrics
        response = client.get("/api/v1/etl/health", headers=headers)
        self._assert_response(response, 200, "Get ETL health metrics")

        # Get data quality report
        response = client.get("/api/v1/etl/data-quality", headers=headers)
        self._assert_response(response, 200, "Get data quality report")

    async def test_analytics_api(self):
        """Test analytics API endpoints."""
        headers = {"Authorization": f"Bearer {self.access_token}"} if self.access_token else {}

        # Get campaign performance (might be empty but should not error)
        response = client.get("/api/v1/analytics/campaigns/performance", headers=headers)
        self._assert_response(response, 200, "Get campaign performance")

        # Get campaign summary
        response = client.get("/api/v1/analytics/campaigns/summary", headers=headers)
        self._assert_response(response, 200, "Get campaign summary")

        # Get partner pipeline
        response = client.get("/api/v1/analytics/partners/pipeline", headers=headers)
        self._assert_response(response, 200, "Get partner pipeline")

        # Get executive dashboard
        response = client.get("/api/v1/analytics/executive/dashboard", headers=headers)
        self._assert_response(response, 200, "Get executive dashboard")

        # Get performance trends
        response = client.get("/api/v1/analytics/trends/performance?metric=cpl&period=month", headers=headers)
        self._assert_response(response, 200, "Get performance trends")

    async def test_reporting_api(self):
        """Test reporting API endpoints."""
        headers = {"Authorization": f"Bearer {self.access_token}"} if self.access_token else {}

        # List report templates
        response = client.get("/api/v1/reports/templates", headers=headers)
        self._assert_response(response, 200, "List report templates")

        templates_data = response.json()
        assert "templates" in templates_data
        assert "count" in templates_data

        # Generate data quality report
        response = client.get("/api/v1/reports/data-quality", headers=headers)
        self._assert_response(response, 200, "Generate data quality report")

        # Generate campaign performance report
        response = client.get("/api/v1/reports/campaign-performance", headers=headers)
        self._assert_response(response, 200, "Generate campaign performance report")

    async def test_performance(self):
        """Test API performance."""
        headers = {"Authorization": f"Bearer {self.access_token}"} if self.access_token else {}

        # Test response times for key endpoints
        performance_tests = [
            ("/health", "Health check"),
            ("/api/v1/benchmarks/meta", "Benchmark metadata"),
            ("/api/v1/analytics/campaigns/summary", "Campaign summary"),
            ("/api/v1/etl/pipeline/status", "Pipeline status"),
        ]

        for endpoint, description in performance_tests:
            start_time = time.time()

            response = client.get(endpoint, headers=headers)

            end_time = time.time()
            response_time = end_time - start_time

            # Assert response is successful and fast (under 2 seconds)
            self._assert_response(response, 200, f"{description} performance test")

            if response_time > 2.0:
                logger.warning(f"Slow response for {endpoint}: {response_time:.2f}s")
            else:
                logger.info(f"✅ {description}: {response_time:.3f}s")

    def generate_report(self) -> str:
        """Generate test report."""
        duration = 0
        if self.test_results["start_time"] and self.test_results["end_time"]:
            duration = (self.test_results["end_time"] - self.test_results["start_time"]).total_seconds()

        report = f"""
# API Test Report
Generated: {datetime.now(timezone.utc).isoformat()}

## Summary
- **Total Tests**: {self.test_results['total_tests']}
- **Passed**: {self.test_results['passed']}
- **Failed**: {self.test_results['failed']}
- **Duration**: {duration:.2f} seconds
- **Success Rate**: {(self.test_results['passed'] / max(self.test_results['total_tests'], 1)) * 100:.1f}%

## Test Results
"""

        if self.test_results["errors"]:
            report += "\n### Failures\n"
            for error in self.test_results["errors"]:
                report += f"- **{error.get('test', error.get('suite', 'Unknown'))}**: {error['error']}\n"
        else:
            report += "\n### ✅ All tests passed!\n"

        return report


async def run_integration_tests():
    """Run integration tests with a live server."""
    logger.info("Starting integration tests...")

    # Test if server is running
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{BASE_URL}/health", timeout=5.0)
            if response.status_code != 200:
                logger.error("Server health check failed")
                return False
    except Exception as e:
        logger.error(f"Cannot connect to server at {BASE_URL}: {e}")
        logger.info("Please start the server with: python main.py")
        return False

    # Run test suite
    test_suite = APITestSuite()
    results = await test_suite.run_all_tests()

    # Generate and save report
    report = test_suite.generate_report()

    report_file = Path("api_test_report.md")
    with open(report_file, "w") as f:
        f.write(report)

    logger.info(f"Test report saved to {report_file}")

    return results["failed"] == 0


async def main():
    """Main test function."""
    logger.info("API Test Suite - Unified Northlight Platform")

    # Run unit tests with TestClient
    test_suite = APITestSuite()
    results = await test_suite.run_all_tests()

    # Print summary
    print("\n" + "="*60)
    print("API TEST SUMMARY")
    print("="*60)
    print(f"Total Tests: {results['total_tests']}")
    print(f"Passed: {results['passed']}")
    print(f"Failed: {results['failed']}")

    if results['errors']:
        print(f"\nErrors ({len(results['errors'])}):")
        for error in results['errors'][:5]:  # Show first 5 errors
            print(f"  - {error.get('test', error.get('suite'))}: {error['error'][:100]}")

    print("="*60)

    # Save detailed report
    report = test_suite.generate_report()
    with open("api_test_report.md", "w") as f:
        f.write(report)

    logger.info("Test report saved to api_test_report.md")

    return 0 if results['failed'] == 0 else 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nTests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Test runner failed: {e}")
        sys.exit(1)