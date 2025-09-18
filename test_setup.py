#!/usr/bin/env python3
"""
Test script to verify Unified Northlight extractor setup.
Checks dependencies, credentials, and basic functionality without running full extractions.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add extractors to path
sys.path.insert(0, str(Path(__file__).parent / "extractors"))

def test_python_version():
    """Test Python version compatibility."""
    print("Testing Python version...")
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print(f"[FAIL] Python {version.major}.{version.minor} is too old. Need Python 3.8+")
        return False
    print(f"[OK] Python {version.major}.{version.minor}.{version.micro} is compatible")
    return True

def test_dependencies():
    """Test required package availability."""
    print("\nTesting dependencies...")
    required_packages = [
        ("playwright", "Playwright browser automation"),
        ("dotenv", "python-dotenv for environment variables"),
        ("pathlib", "Path handling (built-in)"),
        ("json", "JSON handling (built-in)")
    ]

    all_good = True
    for package, description in required_packages:
        try:
            if package == "dotenv":
                import dotenv
            else:
                __import__(package)
            print(f"[OK] {package} - {description}")
        except ImportError:
            print(f"[FAIL] {package} - {description} (MISSING)")
            all_good = False

    return all_good

def test_environment():
    """Test environment configuration."""
    print("\nTesting environment configuration...")

    if not Path(".env").exists():
        print("[FAIL] .env file not found")
        print("   Copy .env.template to .env and fill in your credentials")
        return False

    load_dotenv(dotenv_path=".env")

    required_vars = [
        ("CORP_PORTAL_USERNAME", "Corporate Portal username"),
        ("CORP_PORTAL_PASSWORD", "Corporate Portal password"),
        ("SF_USERNAME", "Salesforce username"),
        ("SF_PASSWORD", "Salesforce password")
    ]

    all_good = True
    for var, description in required_vars:
        value = os.getenv(var)
        if value and value != f"your_{var.lower()}":
            print(f"[OK] {var} - {description}")
        else:
            print(f"[FAIL] {var} - {description} (NOT SET)")
            all_good = False

    return all_good

def test_directories():
    """Test directory structure."""
    print("\nTesting directory structure...")

    required_dirs = [
        "extractors",
        "extractors/corp_portal",
        "extractors/salesforce",
        "extractors/monitor",
        "data/raw"
    ]

    all_good = True
    for dir_path in required_dirs:
        if Path(dir_path).exists():
            print(f"[OK] {dir_path}")
        else:
            print(f"[FAIL] {dir_path} (MISSING)")
            all_good = False

    return all_good

def test_imports():
    """Test extractor module imports."""
    print("\nTesting extractor imports...")

    try:
        from extractors.playwright_bootstrap import new_persistent_browser_context
        print("[OK] Playwright bootstrap")
    except ImportError as e:
        print(f"[FAIL] Playwright bootstrap: {e}")
        return False

    try:
        from extractors.corp_portal.auth import ensure_logged_in
        print("[OK] Corporate Portal auth")
    except ImportError as e:
        print(f"[FAIL] Corporate Portal auth: {e}")
        return False

    try:
        from extractors.salesforce.auth_enhanced import should_skip_sf
        print("[OK] Salesforce auth enhanced")
    except ImportError as e:
        print(f"[FAIL] Salesforce auth enhanced: {e}")
        return False

    try:
        from extractors.monitor.monitoring import log_step, notify
        print("[OK] Monitoring utilities")
    except ImportError as e:
        print(f"[FAIL] Monitoring utilities: {e}")
        return False

    return True

def test_browser():
    """Test browser availability."""
    print("\nTesting browser setup...")

    try:
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://example.com")
        browser.close()
        pw.stop()
        print("[OK] Browser automation working")
        return True
    except Exception as e:
        print(f"[FAIL] Browser automation failed: {e}")
        print("   Try running: playwright install chromium")
        return False

def main():
    """Run all tests."""
    print("=" * 60)
    print("UNIFIED NORTHLIGHT EXTRACTOR SETUP TEST")
    print("=" * 60)

    tests = [
        ("Python Version", test_python_version),
        ("Dependencies", test_dependencies),
        ("Environment", test_environment),
        ("Directories", test_directories),
        ("Imports", test_imports),
        ("Browser", test_browser)
    ]

    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"[ERROR] {test_name} failed with exception: {e}")
            results.append((test_name, False))

    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    passed = 0
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        icon = "[OK]" if result else "[FAIL]"
        print(f"{icon} {test_name}: {status}")
        if result:
            passed += 1

    total = len(results)
    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("\nAll tests passed! Setup is ready.")
        print("You can now run: run_extractors.bat")
        return 0
    else:
        print(f"\n{total - passed} tests failed. Please fix the issues above.")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)