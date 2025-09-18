#!/usr/bin/env python3
"""
Frontend Integration Test Script
Tests the unified frontend with the backend APIs
"""

import asyncio
import sys
from pathlib import Path
from typing import Dict, Any
import json
import subprocess
import time
import signal
import os

import httpx
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.shared import get_logger

logger = get_logger("frontend_test")

class FrontendIntegrationTest:
    """Test frontend integration with unified APIs."""

    def __init__(self):
        self.base_url = "http://localhost:8001"
        self.frontend_url = f"{self.base_url}/dashboard"
        self.api_url = f"{self.base_url}/api/v1"
        self.driver = None
        self.server_process = None

    async def run_tests(self) -> Dict[str, Any]:
        """Run all frontend integration tests."""
        results = {
            "total_tests": 0,
            "passed": 0,
            "failed": 0,
            "errors": [],
            "details": []
        }

        logger.info("Starting frontend integration tests...")

        try:
            # Start the server
            if not await self.start_server():
                raise Exception("Failed to start server")

            # Wait for server to be ready
            await asyncio.sleep(3)

            # Verify API is accessible
            if not await self.verify_api_health():
                raise Exception("API health check failed")

            # Setup Selenium WebDriver
            self.setup_webdriver()

            # Run test suite
            test_methods = [
                self.test_page_loads,
                self.test_navigation_tabs,
                self.test_benchmark_form,
                self.test_login_functionality,
                self.test_etl_tab_auth_required,
                self.test_analytics_tab_loads,
                self.test_reports_tab_loads
            ]

            for test_method in test_methods:
                test_name = test_method.__name__
                results["total_tests"] += 1

                try:
                    logger.info(f"Running {test_name}...")
                    await test_method()
                    results["passed"] += 1
                    results["details"].append({"test": test_name, "status": "PASSED"})
                    logger.info(f"✅ {test_name} passed")

                except Exception as e:
                    results["failed"] += 1
                    error_msg = str(e)
                    results["errors"].append({"test": test_name, "error": error_msg})
                    results["details"].append({"test": test_name, "status": "FAILED", "error": error_msg})
                    logger.error(f"❌ {test_name} failed: {error_msg}")

        except Exception as e:
            logger.error(f"Test suite setup failed: {e}")
            results["errors"].append({"test": "setup", "error": str(e)})

        finally:
            # Cleanup
            await self.cleanup()

        # Summary
        logger.info(f"Frontend tests completed: {results['passed']}/{results['total_tests']} passed")
        return results

    async def start_server(self) -> bool:
        """Start the unified server."""
        try:
            # Check if server is already running
            async with httpx.AsyncClient() as client:
                try:
                    response = await client.get(f"{self.base_url}/health", timeout=2.0)
                    if response.status_code == 200:
                        logger.info("Server already running")
                        return True
                except:
                    pass

            # Start server process
            logger.info("Starting unified server...")
            self.server_process = subprocess.Popen(
                [sys.executable, "main.py"],
                cwd=project_root,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )

            # Wait for server to start
            for i in range(10):
                await asyncio.sleep(1)
                try:
                    async with httpx.AsyncClient() as client:
                        response = await client.get(f"{self.base_url}/health", timeout=2.0)
                        if response.status_code == 200:
                            logger.info("Server started successfully")
                            return True
                except:
                    continue

            return False

        except Exception as e:
            logger.error(f"Failed to start server: {e}")
            return False

    async def verify_api_health(self) -> bool:
        """Verify API is responding."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.api_url}/../health", timeout=5.0)
                return response.status_code == 200
        except Exception as e:
            logger.error(f"API health check failed: {e}")
            return False

    def setup_webdriver(self):
        """Setup Selenium WebDriver."""
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run in headless mode
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")

        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.implicitly_wait(10)
            logger.info("WebDriver initialized")
        except Exception as e:
            logger.warning(f"Chrome WebDriver not available: {e}")
            logger.info("Skipping browser-based tests")
            raise

    async def test_page_loads(self):
        """Test that the main page loads correctly."""
        self.driver.get(self.frontend_url)

        # Check title
        assert "Unified Northlight Platform" in self.driver.title

        # Check main elements are present
        brand = self.driver.find_element(By.CLASS_NAME, "company-name")
        assert "UNIFIED NORTHLIGHT" in brand.text

        # Check navigation tabs
        nav_items = self.driver.find_elements(By.CLASS_NAME, "nav-item")
        assert len(nav_items) >= 4  # Benchmarks, ETL, Analytics, Reports

    async def test_navigation_tabs(self):
        """Test navigation between tabs."""
        self.driver.get(self.frontend_url)

        # Test clicking each tab
        tabs = ["etlTab", "analyticsTab", "reportsTab", "benchmarksTab"]

        for tab_id in tabs:
            tab = self.driver.find_element(By.ID, tab_id)
            tab.click()

            # Wait for tab to become active
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, f"#{tab_id}.active"))
            )

            # Verify corresponding section is visible
            section_id = tab_id.replace("Tab", "Section")
            section = self.driver.find_element(By.ID, section_id)
            assert section.is_displayed()

    async def test_benchmark_form(self):
        """Test benchmark form functionality."""
        self.driver.get(self.frontend_url)

        # Ensure we're on benchmarks tab
        benchmarks_tab = self.driver.find_element(By.ID, "benchmarksTab")
        benchmarks_tab.click()

        # Check form elements exist
        category = self.driver.find_element(By.ID, "category")
        subcategory = self.driver.find_element(By.ID, "subcategory")
        goal_cpl = self.driver.find_element(By.ID, "goal_cpl")
        budget = self.driver.find_element(By.ID, "budget")
        clicks = self.driver.find_element(By.ID, "clicks")
        leads = self.driver.find_element(By.ID, "leads")
        run_btn = self.driver.find_element(By.ID, "runBtn")
        reset_btn = self.driver.find_element(By.ID, "resetBtn")

        # Test reset functionality
        goal_cpl.send_keys("100")
        reset_btn.click()
        assert goal_cpl.get_attribute("value") == ""

    async def test_login_functionality(self):
        """Test login modal and functionality."""
        self.driver.get(self.frontend_url)

        # Click login button
        login_btn = self.driver.find_element(By.ID, "loginBtn")
        login_btn.click()

        # Check modal appears
        modal = self.driver.find_element(By.ID, "loginModal")
        assert modal.is_displayed()

        # Check form elements
        username_field = self.driver.find_element(By.ID, "loginUsername")
        password_field = self.driver.find_element(By.ID, "loginPassword")
        cancel_btn = self.driver.find_element(By.ID, "cancelLoginBtn")

        # Test cancel functionality
        cancel_btn.click()
        assert not modal.is_displayed()

    async def test_etl_tab_auth_required(self):
        """Test ETL tab shows authentication required message."""
        self.driver.get(self.frontend_url)

        # Click ETL tab
        etl_tab = self.driver.find_element(By.ID, "etlTab")
        etl_tab.click()

        # Should show authentication required message
        pipeline_status = self.driver.find_element(By.ID, "pipelineStatus")
        assert "log in" in pipeline_status.text.lower()

    async def test_analytics_tab_loads(self):
        """Test analytics tab loads without errors."""
        self.driver.get(self.frontend_url)

        # Click analytics tab
        analytics_tab = self.driver.find_element(By.ID, "analyticsTab")
        analytics_tab.click()

        # Check analytics grid is present
        analytics_grid = self.driver.find_element(By.CLASS_NAME, "analytics-grid")
        assert analytics_grid.is_displayed()

        # Check metric containers exist
        campaign_metrics = self.driver.find_element(By.ID, "campaignMetrics")
        partner_metrics = self.driver.find_element(By.ID, "partnerMetrics")
        executive_metrics = self.driver.find_element(By.ID, "executiveMetrics")

        assert campaign_metrics.is_displayed()
        assert partner_metrics.is_displayed()
        assert executive_metrics.is_displayed()

    async def test_reports_tab_loads(self):
        """Test reports tab loads correctly."""
        self.driver.get(self.frontend_url)

        # Click reports tab
        reports_tab = self.driver.find_element(By.ID, "reportsTab")
        reports_tab.click()

        # Check report controls exist
        report_controls = self.driver.find_element(By.CLASS_NAME, "report-controls")
        assert report_controls.is_displayed()

        # Check report buttons
        data_quality_btn = self.driver.find_element(By.ID, "dataQualityReportBtn")
        campaign_btn = self.driver.find_element(By.ID, "campaignReportBtn")
        partner_btn = self.driver.find_element(By.ID, "partnerReportBtn")

        assert data_quality_btn.is_displayed()
        assert campaign_btn.is_displayed()
        assert partner_btn.is_displayed()

    async def cleanup(self):
        """Cleanup resources."""
        if self.driver:
            self.driver.quit()
            logger.info("WebDriver closed")

        if self.server_process:
            self.server_process.terminate()
            try:
                self.server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.server_process.kill()
            logger.info("Server process terminated")

    def generate_report(self, results: Dict[str, Any]) -> str:
        """Generate test report."""
        report = f"""
# Frontend Integration Test Report
Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}

## Summary
- **Total Tests**: {results['total_tests']}
- **Passed**: {results['passed']}
- **Failed**: {results['failed']}
- **Success Rate**: {(results['passed'] / max(results['total_tests'], 1)) * 100:.1f}%

## Test Results
"""

        for detail in results['details']:
            status_icon = "✅" if detail['status'] == "PASSED" else "❌"
            report += f"- {status_icon} **{detail['test']}**: {detail['status']}\n"
            if detail.get('error'):
                report += f"  - Error: {detail['error']}\n"

        if results['errors']:
            report += "\n### Errors\n"
            for error in results['errors']:
                report += f"- **{error['test']}**: {error['error']}\n"

        return report


async def main():
    """Main test function."""
    try:
        tester = FrontendIntegrationTest()
        results = await tester.run_tests()

        # Generate and save report
        report = tester.generate_report(results)

        report_file = Path("frontend_integration_test_report.md")
        with open(report_file, "w") as f:
            f.write(report)

        logger.info(f"Test report saved to {report_file}")

        # Print summary
        print("\n" + "="*60)
        print("FRONTEND INTEGRATION TEST SUMMARY")
        print("="*60)
        print(f"Total Tests: {results['total_tests']}")
        print(f"Passed: {results['passed']}")
        print(f"Failed: {results['failed']}")
        print(f"Success Rate: {(results['passed'] / max(results['total_tests'], 1)) * 100:.1f}%")
        print("="*60)

        return 0 if results['failed'] == 0 else 1

    except Exception as e:
        logger.error(f"Test runner failed: {e}")
        return 1


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