"""
Salesforce Partner Pipeline Report Extractor.
Navigates to SF report, exports CSV, saves to data/raw/sf_partner_pipeline/YYYY-MM-DD.csv
"""

import os
import pathlib
import sys
import datetime as dt
import csv
import json
import re
import time
import io
from typing import List, Dict, Any
from urllib.parse import urljoin, urlparse
from contextlib import suppress
from dotenv import load_dotenv
from playwright.sync_api import expect, TimeoutError as PWTimeoutError
from ..playwright_bootstrap import new_persistent_browser_context
from .auth_enhanced import login_if_needed, should_skip_sf
from . import selectors as S

load_dotenv()

RAW_DIR = pathlib.Path("data/raw/sf_partner_pipeline")
LOG_DIR = pathlib.Path("logs")
LOG_DIR.mkdir(exist_ok=True, parents=True)
RAW_DIR.mkdir(exist_ok=True, parents=True)


def _safe_save_bytes(path: str, data: bytes, attempts: int = 5, sleep_s: float = 0.5) -> str:
    """
    Write bytes to path; if PermissionError (file locked by Excel), retry with
    incremented suffix. Returns the final path written.
    """
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    suffix_idx = 0
    last_err = None
    for _ in range(attempts):
        candidate = p if suffix_idx == 0 else p.with_stem(f"{p.stem}_{suffix_idx}")
        try:
            with open(candidate, "wb") as f:
                f.write(data)
            return str(candidate)
        except PermissionError as e:
            last_err = e
            suffix_idx += 1
            time.sleep(sleep_s)
    raise PermissionError(f"Could not write file (locked/read-only?): {p} :: {last_err}")


def _sf_origin_and_sid(page):
    m = re.match(r"^https?://[^/]+", page.url)
    if not m:
        raise RuntimeError(f"Cannot derive SF origin from URL: {page.url}")
    origin = m.group(0)

    # Extract session ID from cookies or URL
    sid = None
    try:
        for cookie in page.context.cookies():
            if cookie['name'] == 'sid':
                sid = cookie['value']
                break
    except:
        pass

    return origin, sid


def _report_id_from_url(url: str) -> str:
    m = re.search(r"/Report/([a-zA-Z0-9]{15,18})/view", url)
    if not m:
        raise RuntimeError(f"Cannot parse report id from {url}")
    return m.group(1)

def _get_sid(page) -> str:
    for c in page.context.cookies():
        if c.get("name") == "sid" and ("salesforce.com" in c.get("domain","") or "force.com" in c.get("domain","")):
            return c.get("value")
    raise RuntimeError("No Salesforce session cookie (sid) found.")

def export_partner_pipeline():
    """
    Export Salesforce Partner Pipeline report to CSV.
    Handles authentication, navigation, export modal, and file download.
    """
    # Check for MFA lockout before attempting extraction
    if should_skip_sf():
        raise RuntimeError('Skipping SF jobs due to recent MFA lockout (cooldown in effect).')

    pw, ctx = new_persistent_browser_context()
    page = ctx.new_page()
    page.set_default_timeout(60_000)

    try:
        print("[INFO] Starting SF Partner Pipeline extraction...")

        # Authenticate if needed (handles daily MFA)
        login_if_needed(page, ctx)

        # Verify we're actually authenticated to Lightning
        current_url = page.url
        if not ("/lightning/" in current_url or "/one/one.app" in current_url or ".force.com" in current_url):
            print(f"[WARNING] Not on Lightning interface: {current_url}")
            print("[INFO] Attempting to navigate to report...")

        # Navigate to the report (only if not already there)
        if S.SF_REPORT_URL not in current_url:
            print(f"[INFO] Navigating to report: {S.SF_REPORT_URL}")
            page.goto(S.SF_REPORT_URL, wait_until="domcontentloaded", timeout=45000)
        else:
            print(f"[INFO] Already on report page: {current_url}")

        # Give Lightning time to render the report header
        print("[INFO] Waiting for report to load...")
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception as e:
            print(f"[DEBUG] Page load timeout (this is normal for heavy Lightning reports): {e}")
            # Continue anyway - Lightning reports often don't reach "networkidle"

        # Wait a bit more for Lightning components to fully render
        page.wait_for_timeout(3000)

        # Verify we're on the correct report page now
        final_url = page.url
        print(f"[DEBUG] Final URL after navigation: {final_url}")

        if "verification" in final_url.lower() or "totp" in final_url.lower():
            print("[DEBUG] Found MFA verification page, attempting to solve...")
            try:
                # Import the robust MFA solver
                from .auth_enhanced import solve_mfa_if_present
                if not solve_mfa_if_present(page, totp_code=os.environ.get("SF_TOTP_CODE")):
                    raise RuntimeError("Failed to solve MFA verification - session may have expired")
                print("[DEBUG] Successfully solved MFA, continuing...")
            except Exception as e:
                raise RuntimeError(f"Still on MFA verification page after authentication: {e}")

        # Ensure we're on the report view and parse the ID
        print("[DEBUG] Final URL after navigation:", page.url)
        report_id = _report_id_from_url(page.url)
        target_date = dt.date.today().isoformat()
        target = RAW_DIR / f"{target_date}.csv"

        print(f"[INFO] Attempting UI export → {target}")
        try:
            _export_via_ui(page, str(target))
            print(f"[OK] Exported via UI → {target}")
            return True
        except Exception as e_ui:
            print(f"[WARNING] UI export failed: {e_ui}")
            return False

    except Exception as e:
        print(f"[ERROR] Partner Pipeline extraction failed: {e}")
        return False

    finally:
        try:
            ctx.close()
            pw.stop()
        except:
            pass

def _export_via_ui(page, target_path):
    """
    UI-based export approach with improved selectors.
    """
    print("[INFO] Looking for export controls...")

    try:
        # Try direct export button first
        export_btn = page.locator('button[title*="Export"], button[aria-label*="Export"], .slds-button:has-text("Export")').first
        if export_btn.is_visible(timeout=10_000):
            print("[INFO] Found direct Export button")
            with page.expect_download(timeout=60_000) as download_info:
                export_btn.click()
            download = download_info.value
            download.save_as(target_path)
            return
    except Exception as e:
        print(f"[DEBUG] Direct export failed: {e}")

    try:
        print("[INFO] Trying overflow menu → Export")
        # Try overflow/kebab menu
        kebab = page.locator("lightning-button-menu button[title*='Show more actions'], button[aria-label*='Show more actions']").first
        if kebab.is_visible(timeout=15_000):
            kebab.click()

            menu_export = page.get_by_role("menuitem", name=re.compile(r"\bExport\b", re.I))
            if menu_export.is_visible(timeout=10_000):
                print("[INFO] Found Export in overflow menu")
                with page.expect_download(timeout=60_000) as download_info:
                    menu_export.click()
                download = download_info.value
                download.save_as(target_path)
                return
    except Exception as e:
        print(f"[DEBUG] Overflow menu failed: {e}")

    raise RuntimeError("Could not find export controls")


def run():
    """Main function for compatibility with subprocess calls."""
    return export_partner_pipeline()


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)