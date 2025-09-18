#!/usr/bin/env python3
"""
Unified Northlight Multi-Report Extractor
Main orchestrator for downloading all 10 reports with sophisticated error handling.

Reports:
1. Ultimate DMS Campaign Performance
2. Budget Waterfall Client
3. Spend Revenue Performance
4. DFP-RIJ (Down For Payment & Revenue In Jeopardy)
5. Agreed CPL Performance
6. BSC Standards
7. Budget Waterfall Channel
8. Salesforce Partner Pipeline
9. Salesforce Tim King Partner Pipeline
10. Salesforce Partner Calls

Features:
- MFA lockout detection and cooldown for Salesforce
- Persistent browser sessions
- Comprehensive error handling
- Individual extractor isolation
- Environment validation
"""

import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Add extractors to path
sys.path.insert(0, str(Path(__file__).parent / "extractors"))

from extractors.monitor.monitoring import log_step, notify, run_guard, ensure_directories

load_dotenv(dotenv_path=".env")


def check_environment():
    """Check that required environment variables are set."""
    required_vars = [
        "CORP_PORTAL_USERNAME",
        "CORP_PORTAL_PASSWORD",
        "SF_USERNAME",
        "SF_PASSWORD"
    ]

    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)

    if missing_vars:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing_vars)}\n"
            f"Please check your .env file"
        )


def run_extractor_subprocess(module_path: str, timeout: int = 300) -> tuple[bool, dict]:
    """Run an extractor in isolated subprocess."""
    try:
        result = subprocess.run([
            sys.executable, "-m", module_path
        ], capture_output=True, text=True, timeout=timeout, cwd=Path.cwd())

        if result.returncode != 0:
            error_lines = result.stderr.strip().split('\n')[:5] if result.stderr else ["Unknown error"]
            return False, {"error": '\n'.join(error_lines), "full_stderr": result.stderr, "stdout": result.stdout}

        return True, {"stdout": result.stdout}
    except subprocess.TimeoutExpired:
        return False, {"error": f"Extractor timeout after {timeout}s"}
    except Exception as e:
        return False, {"error": str(e)}


def run_corp_portal_extractors():
    """Run all Corporate Portal extractors."""
    extractors = [
        ("Ultimate DMS", "extractors.corp_portal.ultimate_dms"),
        ("Budget Waterfall Client", "extractors.corp_portal.budget_waterfall_client"),
        ("Spend Revenue Performance", "extractors.corp_portal.spend_revenue_performance"),
        ("DFP-RIJ", "extractors.corp_portal.dfp_rij"),
        ("Agreed CPL Performance", "extractors.corp_portal.agreed_cpl_performance"),
        ("BSC Standards", "extractors.corp_portal.bsc_standards"),
        ("Budget Waterfall Channel", "extractors.corp_portal.budget_waterfall_channel"),
    ]

    results = {}
    for name, module in extractors:
        log_step("CORP_PORTAL", f"=== Starting {name} ===")
        success, info = run_extractor_subprocess(module, timeout=300)
        results[name] = {"success": success, "info": info}

        if success:
            log_step("CORP_PORTAL", f"{name} completed successfully")
        else:
            log_step("CORP_PORTAL", f"{name} FAILED: {info.get('error', 'Unknown error')}")
            notify("CRIT", f"{name} Failed", f"Corp Portal extractor failed", info)

    return results


def run_salesforce_extractors():
    """Run all Salesforce extractors with MFA handling (direct execution for interactive MFA)."""
    # Check if we should skip due to lockout
    from extractors.salesforce.auth_enhanced import should_skip_sf

    if should_skip_sf():
        log_step("SALESFORCE", "Skipping all Salesforce extractors due to MFA lockout cooldown")
        return {
            "SF Partner Pipeline": {"success": False, "info": {"error": "MFA lockout cooldown active"}},
            "SF Tim King Pipeline": {"success": False, "info": {"error": "MFA lockout cooldown active"}},
            "SF Partner Calls": {"success": False, "info": {"error": "MFA lockout cooldown active"}},
        }

    # Run Salesforce extractors directly (not subprocess) to allow interactive MFA
    extractors = [
        ("SF Partner Pipeline", lambda: _run_sf_partner_pipeline()),
        ("SF Tim King Pipeline", lambda: _run_sf_tim_king_pipeline()),
        ("SF Partner Calls", lambda: _run_sf_partner_calls()),
    ]

    results = {}
    for name, extractor_func in extractors:
        log_step("SALESFORCE", f"=== Starting {name} ===")
        try:
            success = extractor_func()
            results[name] = {"success": success, "info": {"message": "Completed successfully" if success else "Extraction failed"}}

            if success:
                log_step("SALESFORCE", f"{name} completed successfully")
            else:
                log_step("SALESFORCE", f"{name} FAILED")
                notify("CRIT", f"{name} Failed", f"Salesforce extractor failed", {"error": "Extraction returned False"})

        except Exception as e:
            error_msg = str(e)
            results[name] = {"success": False, "info": {"error": error_msg}}
            log_step("SALESFORCE", f"{name} FAILED: {error_msg}")
            notify("CRIT", f"{name} Failed", f"Salesforce extractor failed", {"error": error_msg})

            # Check if this was MFA-related and we should stop
            if 'mfa' in error_msg.lower() or 'lockout' in error_msg.lower() or 'verification' in error_msg.lower():
                log_step("SALESFORCE", "Stopping remaining SF extractors due to MFA issue")
                break

    return results

def _run_sf_partner_pipeline():
    """Run SF Partner Pipeline extractor directly."""
    try:
        from extractors.salesforce.partner_pipeline import export_partner_pipeline
        return export_partner_pipeline()
    except Exception as e:
        print(f"[ERROR] SF Partner Pipeline failed: {e}")
        return False

def _run_sf_tim_king_pipeline():
    """Run SF Tim King Pipeline extractor directly."""
    try:
        from extractors.salesforce.tim_king_partner_pipeline import export_tim_king_partner_pipeline
        return export_tim_king_partner_pipeline()
    except Exception as e:
        print(f"[ERROR] SF Tim King Pipeline failed: {e}")
        return False

def _run_sf_partner_calls():
    """Run SF Partner Calls extractor directly."""
    try:
        from extractors.salesforce.partner_calls import export_partner_calls
        return export_partner_calls()
    except Exception as e:
        print(f"[ERROR] SF Partner Calls failed: {e}")
        return False


def main():
    """Main orchestrator function."""
    start_time = datetime.now()

    notify("INFO", "Multi-Report Extraction Starting",
           f"Starting all 10 report extractors at {start_time}")

    try:
        # Pre-flight checks
        log_step("SETUP", "Running pre-flight checks...")
        check_environment()
        ensure_directories()

        results = {
            "start_time": start_time.isoformat(),
            "corp_portal": {},
            "salesforce": {}
        }

        # Run Corporate Portal extractors
        log_step("PIPELINE", "=== Starting Corporate Portal Extractors ===")
        corp_results = run_corp_portal_extractors()
        results["corp_portal"] = corp_results

        # Run Salesforce extractors
        log_step("PIPELINE", "=== Starting Salesforce Extractors ===")
        sf_results = run_salesforce_extractors()
        results["salesforce"] = sf_results

        # Calculate overall status
        end_time = datetime.now()
        duration = end_time - start_time
        results["end_time"] = end_time.isoformat()
        results["duration_seconds"] = duration.total_seconds()

        # Count successes
        corp_successes = sum(1 for r in corp_results.values() if r["success"])
        sf_successes = sum(1 for r in sf_results.values() if r["success"])
        total_successes = corp_successes + sf_successes
        total_extractors = len(corp_results) + len(sf_results)

        print(f"""
{'='*70}
MULTI-REPORT EXTRACTION SUMMARY
{'='*70}
Duration: {duration}

Corporate Portal Extractors: {corp_successes}/{len(corp_results)} successful
Salesforce Extractors: {sf_successes}/{len(sf_results)} successful
Total: {total_successes}/{total_extractors} successful

Success Details:""")

        for category, cat_results in [("Corp Portal", corp_results), ("Salesforce", sf_results)]:
            print(f"\n{category}:")
            for name, result in cat_results.items():
                status = "[SUCCESS]" if result["success"] else "[FAILED]"
                print(f"  {status} {name}")
                if not result["success"]:
                    error = result["info"].get("error", "Unknown error")
                    print(f"    Error: {error}")

        print(f"\nData saved to: data/raw/")
        print(f"Logs saved to: logs/etl.log")
        print(f"Alerts saved to: alerts/alerts.jsonl")
        print(f"{'='*70}")

        # Determine exit code
        if total_successes == total_extractors:
            notify("INFO", "All Extractors Succeeded",
                   f"All {total_extractors} extractors completed successfully")
            return 0
        elif total_successes > 0:
            notify("WARN", "Partial Extraction Success",
                   f"{total_successes}/{total_extractors} extractors succeeded")
            return 1
        else:
            notify("CRIT", "All Extractors Failed",
                   "All extraction attempts failed")
            return 2

    except KeyboardInterrupt:
        notify("WARN", "Extraction Interrupted", "User interrupted the extraction process")
        return 130

    except Exception as e:
        notify("CRIT", "Extraction Pipeline Failed", f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)