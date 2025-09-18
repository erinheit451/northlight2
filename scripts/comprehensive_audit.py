#!/usr/bin/env python3
"""
Comprehensive Audit Script for Unified Northlight Platform
Tests all components that can be verified without external dependencies
"""

import sys
import json
from pathlib import Path
from typing import Dict, Any
import importlib.util

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_file_structure() -> Dict[str, Any]:
    """Test that all required files and directories exist."""
    print("=== FILE STRUCTURE AUDIT ===")

    results = {"passed": 0, "failed": 0, "details": []}

    required_files = [
        ".env",
        "main.py",
        "requirements.txt",
        "docker-compose.yml",
        "frontend/index.html",
        "frontend/unified-script.js",
        "frontend/styles.css",
        "frontend/book/index.html",
        "frontend/book/partners.html",
        "core/config.py",
        "core/database.py",
        "api/v1/__init__.py",
        "api/v1/auth.py",
        "api/v1/benchmarking.py",
        "api/v1/etl_management.py",
        "api/v1/analytics.py",
        "api/v1/reporting.py"
    ]

    required_dirs = [
        "core/",
        "api/v1/",
        "etl/unified/",
        "frontend/",
        "frontend/book/",
        "scripts/",
        "database/init/",
        "data/raw/",
        "data/warehouse/",
        "data/book/",
        "data/exports/"
    ]

    # Check files
    for file_path in required_files:
        full_path = project_root / file_path
        if full_path.exists():
            results["passed"] += 1
            results["details"].append(f"✅ {file_path}")
        else:
            results["failed"] += 1
            results["details"].append(f"❌ {file_path}")

    # Check directories
    for dir_path in required_dirs:
        full_path = project_root / dir_path
        if full_path.exists() and full_path.is_dir():
            results["passed"] += 1
            results["details"].append(f"✅ {dir_path}")
        else:
            results["failed"] += 1
            results["details"].append(f"❌ {dir_path}")

    print(f"File structure: {results['passed']}/{results['passed'] + results['failed']} items found")
    return results

def test_configuration() -> Dict[str, Any]:
    """Test configuration files and settings."""
    print("\n=== CONFIGURATION AUDIT ===")

    results = {"passed": 0, "failed": 0, "details": []}

    try:
        # Test .env file
        env_file = project_root / ".env"
        if env_file.exists():
            env_content = env_file.read_text()
            required_vars = [
                "DATABASE_URL",
                "SECRET_KEY",
                "CORP_PORTAL_USERNAME",
                "API_PORT",
                "REDIS_URL"
            ]

            for var in required_vars:
                if var in env_content:
                    results["passed"] += 1
                    results["details"].append(f"✅ {var} configured")
                else:
                    results["failed"] += 1
                    results["details"].append(f"❌ {var} missing")

        # Test config loading
        try:
            from core.config import settings
            results["passed"] += 1
            results["details"].append("✅ Configuration loads successfully")
        except Exception as e:
            results["failed"] += 1
            results["details"].append(f"❌ Configuration load failed: {e}")

    except Exception as e:
        results["failed"] += 1
        results["details"].append(f"❌ Configuration test failed: {e}")

    print(f"Configuration: {results['passed']}/{results['passed'] + results['failed']} checks passed")
    return results

def test_api_structure() -> Dict[str, Any]:
    """Test API module structure and imports."""
    print("\n=== API STRUCTURE AUDIT ===")

    results = {"passed": 0, "failed": 0, "details": []}

    api_modules = [
        "api.v1.auth",
        "api.v1.benchmarking",
        "api.v1.etl_management",
        "api.v1.analytics",
        "api.v1.reporting",
        "api.v1.caching"
    ]

    for module_name in api_modules:
        try:
            spec = importlib.util.find_spec(module_name)
            if spec is not None:
                results["passed"] += 1
                results["details"].append(f"✅ {module_name}")
            else:
                results["failed"] += 1
                results["details"].append(f"❌ {module_name} not found")
        except Exception as e:
            results["failed"] += 1
            results["details"].append(f"❌ {module_name}: {e}")

    print(f"API modules: {results['passed']}/{results['passed'] + results['failed']} modules available")
    return results

def test_etl_structure() -> Dict[str, Any]:
    """Test ETL pipeline structure."""
    print("\n=== ETL STRUCTURE AUDIT ===")

    results = {"passed": 0, "failed": 0, "details": []}

    etl_components = [
        "etl/unified/loaders/base.py",
        "etl/unified/loaders/ultimate_dms_loader.py",
        "etl/unified/loaders/budget_waterfall_loader.py",
        "etl/unified/loaders/salesforce_loader.py",
        "etl/unified/orchestration/scheduler.py",
        "etl/unified/orchestration/monitor.py",
        "etl/unified/extractors/heartbeat_wrapper.py"
    ]

    for component in etl_components:
        file_path = project_root / component
        if file_path.exists():
            results["passed"] += 1
            results["details"].append(f"✅ {component}")
        else:
            results["failed"] += 1
            results["details"].append(f"❌ {component}")

    print(f"ETL components: {results['passed']}/{results['passed'] + results['failed']} components found")
    return results

def test_frontend_assets() -> Dict[str, Any]:
    """Test frontend assets and book system."""
    print("\n=== FRONTEND ASSETS AUDIT ===")

    results = {"passed": 0, "failed": 0, "details": []}

    # Check main frontend files
    frontend_files = [
        "frontend/index.html",
        "frontend/unified-script.js",
        "frontend/styles.css"
    ]

    for file_path in frontend_files:
        full_path = project_root / file_path
        if full_path.exists():
            file_size = full_path.stat().st_size
            results["passed"] += 1
            results["details"].append(f"✅ {file_path} ({file_size:,} bytes)")
        else:
            results["failed"] += 1
            results["details"].append(f"❌ {file_path}")

    # Check book system
    book_files = [
        "frontend/book/index.html",
        "frontend/book/partners.html",
        "frontend/book/partners.js",
        "frontend/book/script.js"
    ]

    for file_path in book_files:
        full_path = project_root / file_path
        if full_path.exists():
            file_size = full_path.stat().st_size
            results["passed"] += 1
            results["details"].append(f"✅ {file_path} ({file_size:,} bytes)")
        else:
            results["failed"] += 1
            results["details"].append(f"❌ {file_path}")

    print(f"Frontend assets: {results['passed']}/{results['passed'] + results['failed']} files found")
    return results

def test_data_availability() -> Dict[str, Any]:
    """Test data availability from original systems."""
    print("\n=== DATA AVAILABILITY AUDIT ===")

    results = {"passed": 0, "failed": 0, "details": []}

    # Check Heartbeat data
    heartbeat_path = Path("../heartbeat/data/warehouse/")
    if heartbeat_path.exists():
        heartbeat_files = list(heartbeat_path.glob("*.duckdb"))
        parquet_files = list(heartbeat_path.glob("*.parquet"))

        results["passed"] += len(heartbeat_files)
        results["passed"] += min(len(parquet_files), 5)  # Count up to 5 parquet files

        results["details"].append(f"✅ Heartbeat DuckDB files: {len(heartbeat_files)}")
        results["details"].append(f"✅ Heartbeat Parquet files: {len(parquet_files)}")
    else:
        results["failed"] += 1
        results["details"].append("❌ Heartbeat data directory not found")

    # Check Northlight data
    northlight_path = Path("../northlight/")
    if northlight_path.exists():
        json_files = list(northlight_path.glob("*.json"))
        results["passed"] += len(json_files)
        results["details"].append(f"✅ Northlight JSON files: {len(json_files)}")

        for json_file in json_files:
            size = json_file.stat().st_size
            results["details"].append(f"  - {json_file.name}: {size:,} bytes")
    else:
        results["failed"] += 1
        results["details"].append("❌ Northlight data directory not found")

    print(f"Data availability: {results['passed']} datasets found, {results['failed']} missing")
    return results

def test_scripts_and_tools() -> Dict[str, Any]:
    """Test utility scripts and tools."""
    print("\n=== SCRIPTS AND TOOLS AUDIT ===")

    results = {"passed": 0, "failed": 0, "details": []}

    script_files = [
        "scripts/data_migration.py",
        "scripts/migrate_benchmark_data.py",
        "scripts/run_unified_etl.py",
        "scripts/test_api.py",
        "scripts/test_frontend_integration.py",
        "scripts/setup_unified_platform.bat"
    ]

    for script in script_files:
        file_path = project_root / script
        if file_path.exists():
            results["passed"] += 1
            results["details"].append(f"✅ {script}")
        else:
            results["failed"] += 1
            results["details"].append(f"❌ {script}")

    print(f"Scripts: {results['passed']}/{results['passed'] + results['failed']} scripts available")
    return results

def generate_audit_summary(all_results: Dict[str, Dict[str, Any]]) -> str:
    """Generate comprehensive audit summary."""
    total_passed = sum(r["passed"] for r in all_results.values())
    total_failed = sum(r["failed"] for r in all_results.values())
    total_tests = total_passed + total_failed
    success_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0

    summary = f"""
# UNIFIED NORTHLIGHT PLATFORM - AUDIT SUMMARY

**Total Tests:** {total_tests}
**Passed:** {total_passed}
**Failed:** {total_failed}
**Success Rate:** {success_rate:.1f}%

## Component Status

"""

    for component, results in all_results.items():
        component_total = results["passed"] + results["failed"]
        component_rate = (results["passed"] / component_total * 100) if component_total > 0 else 0
        status = "✅ EXCELLENT" if component_rate >= 90 else "⚠️ NEEDS ATTENTION" if component_rate >= 70 else "❌ CRITICAL"

        summary += f"### {component.replace('_', ' ').title()}\n"
        summary += f"- **Status:** {status}\n"
        summary += f"- **Score:** {results['passed']}/{component_total} ({component_rate:.1f}%)\n\n"

    summary += "\n## Overall Assessment\n\n"

    if success_rate >= 95:
        summary += "🎉 **OUTSTANDING** - Platform integration is excellent and ready for production.\n"
    elif success_rate >= 85:
        summary += "✅ **GOOD** - Platform integration is solid with minor issues to address.\n"
    elif success_rate >= 70:
        summary += "⚠️ **FAIR** - Platform integration has some issues that need attention.\n"
    else:
        summary += "❌ **POOR** - Platform integration has significant issues requiring fixes.\n"

    return summary

def main():
    """Run comprehensive audit."""
    print("UNIFIED NORTHLIGHT PLATFORM - COMPREHENSIVE AUDIT")
    print("=" * 60)

    # Run all audit tests
    audit_results = {
        "file_structure": test_file_structure(),
        "configuration": test_configuration(),
        "api_structure": test_api_structure(),
        "etl_structure": test_etl_structure(),
        "frontend_assets": test_frontend_assets(),
        "data_availability": test_data_availability(),
        "scripts_and_tools": test_scripts_and_tools()
    }

    # Generate summary
    summary = generate_audit_summary(audit_results)

    # Print summary
    print("\n" + "=" * 60)
    print(summary)

    # Save detailed results
    detailed_results = {
        "summary": summary,
        "detailed_results": audit_results,
        "timestamp": "2025-09-17"
    }

    results_file = project_root / "AUDIT_RESULTS.json"
    with open(results_file, "w") as f:
        json.dump(detailed_results, f, indent=2)

    print(f"\nDetailed results saved to: {results_file}")

    # Return success code
    total_passed = sum(r["passed"] for r in audit_results.values())
    total_failed = sum(r["failed"] for r in audit_results.values())
    success_rate = (total_passed / (total_passed + total_failed) * 100) if (total_passed + total_failed) > 0 else 0

    return 0 if success_rate >= 85 else 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except Exception as e:
        print(f"Audit failed: {e}")
        sys.exit(1)