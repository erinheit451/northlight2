# extractors/salesforce/partner_calls.py
"""
Salesforce Partner Calls Report Extractor.
Navigates to SF report, exports CSV, saves to data/raw/sf_partner_calls/YYYY-MM-DD.csv
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

sys.path.append(str(pathlib.Path(__file__).parent.parent))
from ..playwright_bootstrap import new_persistent_browser_context
from .auth_enhanced import login_if_needed
from . import selectors as S

load_dotenv(dotenv_path=".env")

RAW_DIR = pathlib.Path("data/raw/sf_partner_calls")
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

    if last_err:
        raise last_err


def _report_id_from_url(url: str) -> str:
    """Extract report ID from Salesforce Lightning report URL."""
    # https://sso.lightning.force.com/lightning/r/Report/00OQp000009jCz8MAE/view
    match = re.search(r'/Report/([A-Za-z0-9]{15,18})/', url)
    if not match:
        raise ValueError(f"Could not extract report ID from URL: {url}")
    return match.group(1)


def _flatten_report_data(report_data: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Flatten Salesforce Reports API response into CSV-like rows.
    Handles grouped reports and different formats.
    """
    try:
        report_metadata = report_data.get('reportMetadata', {})
        fact_map = report_data.get('factMap', {})

        # Get column info
        detail_columns = report_metadata.get('detailColumns', [])

        # Common factMap keys:
        # - Tabular: "T!T"
        # - Summary/Matrix: e.g., "0!T", "1!T", "0_0!T" etc. (group buckets)
        candidate_keys = ["T!T"] + [k for k in fact_map.keys() if k.endswith("!T")]

        for key in candidate_keys:
            node = fact_map.get(key, {})
            if node and node.get('rows'):
                rows = node['rows']
                flattened = []

                for row in rows:
                    row_data = {}
                    data_cells = row.get('dataCells', [])

                    # Map each data cell to its column
                    for i, cell in enumerate(data_cells):
                        if i < len(detail_columns):
                            column_name = detail_columns[i]
                            # Get the display value, fallback to value, then to empty string
                            cell_value = cell.get('label', cell.get('value', ''))
                            row_data[column_name] = str(cell_value) if cell_value is not None else ''
                        else:
                            # Fallback column naming if we run out of detail columns
                            row_data[f'Column_{i}'] = str(cell.get('label', cell.get('value', '')))

                    flattened.append(row_data)

                return flattened

        # If no data found, return empty list
        return []

    except Exception as e:
        print(f"[WARNING] Error flattening report data: {e}")
        return []


def _export_via_rest_to_csv(page, report_id: str, target_path: str) -> str:
    """Export report using Salesforce Reports API and convert to CSV."""
    print(f"[DEBUG] REST API: Starting export for report {report_id}")

    # Get session info from browser cookies
    print(f"[DEBUG] REST API: Extracting session cookies...")
    cookies = page.context.cookies()
    session_cookie = None

    # Prefer the Classic origin derived from the current Lightning URL
    classic_origin = _classic_origin(page.url)  # e.g., https://sso.my.salesforce.com
    server_url = classic_origin
    print(f"[DEBUG] REST API: Classic origin: {classic_origin}")

    cookie_count = 0
    for cookie in cookies:
        cookie_count += 1
        if cookie['name'] == 'sid':
            session_cookie = cookie['value']
            print(f"[DEBUG] REST API: Found 'sid' cookie (length: {len(session_cookie)})")
            # If cookie says "*.my.salesforce.com" with a subdomain, use that.
            domain = cookie.get('domain', '').lstrip('.')
            if domain.endswith('my.salesforce.com') and domain.count('.') >= 2:
                # has a subdomain like sso.my.salesforce.com or naXX.my.salesforce.com
                server_url = f"https://{domain}"
                print(f"[DEBUG] REST API: Updated server URL from cookie domain: {server_url}")
            break

    print(f"[DEBUG] REST API: Processed {cookie_count} cookies")

    if not session_cookie:
        print(f"[ERROR] REST API: No Salesforce session cookie found in {cookie_count} cookies")
        for cookie in cookies[:5]:  # Log first 5 cookie names for debugging
            print(f"[DEBUG] REST API: Found cookie: {cookie['name']}")
        raise RuntimeError("No Salesforce session cookie found")

    print(f"[DEBUG] REST API: Using server: {server_url}")

    # Make API request using Playwright's request context
    api_url = f"{server_url}/services/data/v60.0/analytics/reports/{report_id}?includeDetails=true"
    print(f"[DEBUG] REST API: Constructed API URL: {api_url}")

    try:
        # Use page context to make authenticated request
        print(f"[DEBUG] REST API: Making GET request to API...")
        request_start = time.time()
        response = page.request.get(api_url, headers={
            'Authorization': f'Bearer {session_cookie}',
            'Accept': 'application/json'
        }, timeout=60000)
        print(f"[DEBUG] REST API: Request completed in {time.time() - request_start:.2f}s with status {response.status}")

        if response.status != 200:
            response_text = response.text()
            print(f"[ERROR] REST API: Request failed with status {response.status}")
            print(f"[DEBUG] REST API: Response text: {response_text[:500]}...")
            raise RuntimeError(f"API request failed with status {response.status}: {response_text}")

        # Parse JSON response
        print(f"[DEBUG] REST API: Parsing JSON response...")
        json_start = time.time()
        report_data = response.json()
        print(f"[DEBUG] REST API: JSON parsed in {time.time() - json_start:.2f}s")
        print(f"[DEBUG] REST API: Report data keys: {list(report_data.keys())}")

        # Flatten the data to CSV format
        print(f"[DEBUG] REST API: Flattening report data...")
        flatten_start = time.time()
        rows = _flatten_report_data(report_data)
        print(f"[DEBUG] REST API: Data flattened in {time.time() - flatten_start:.2f}s, found {len(rows)} rows")

        if not rows:
            # Some Lightning reports require an instance run to materialize detail rows
            print("[DEBUG] No rows found, attempting to run report...")
            run_url = f"{server_url}/services/data/v60.0/analytics/reports/{report_id}"
            run_resp = page.request.post(
                run_url,
                headers={
                    "Authorization": f"Bearer {session_cookie}",
                    "Accept": "application/json",
                    "Content-Type": "application/json"
                },
                data=json.dumps({"reportMetadata": report_data.get("reportMetadata", {})})
            )
            if run_resp.status == 200:
                report_data = run_resp.json()
                rows = _flatten_report_data(report_data)

        if not rows:
            raise RuntimeError("No data rows found in report response")

        print(f"[DEBUG] Flattened {len(rows)} rows from API response")

        # Convert to CSV
        if rows:
            # Get all unique column names from all rows
            all_columns = set()
            for row in rows:
                all_columns.update(row.keys())
            all_columns = sorted(list(all_columns))

            # Write CSV
            csv_content = io.StringIO()
            writer = csv.DictWriter(csv_content, fieldnames=all_columns)
            writer.writeheader()
            writer.writerows(rows)

            # Save to file
            csv_bytes = csv_content.getvalue().encode('utf-8')
            saved_path = _safe_save_bytes(target_path, csv_bytes)

            # Sanity check: count actual CSV rows
            with open(saved_path, "rb") as f:
                n_rows = sum(1 for _ in f) - 1  # minus header
            print(f"[OK] REST API export successful: {max(n_rows,0)} CSV rows")

            return saved_path
        else:
            raise RuntimeError("No data to convert to CSV")

    except Exception as e:
        print(f"[DEBUG] REST API export failed: {e}")
        raise


def _classic_origin(lightning_url: str) -> str:
    """Convert Lightning URL to Classic domain format."""
    parsed = urlparse(lightning_url)

    # Map Lightning domains to Classic domains
    if 'lightning.force.com' in parsed.netloc:
        # sso.lightning.force.com -> sso.my.salesforce.com
        classic_domain = parsed.netloc.replace('lightning.force.com', 'my.salesforce.com')
        return f"{parsed.scheme}://{classic_domain}"

    # For other domains, try removing 'lightning.' prefix
    if parsed.netloc.startswith('lightning.'):
        classic_domain = parsed.netloc.replace('lightning.', '', 1)
        return f"{parsed.scheme}://{classic_domain}"

    # Default: use as-is
    return f"{parsed.scheme}://{parsed.netloc}"


def _export_via_classic_csv(page, report_id: str, target_path: str) -> str:
    """Export report using Classic CSV servlet endpoint."""
    print(f"[DEBUG] Attempting Classic CSV export for report {report_id}")

    current_url = page.url
    origin = _classic_origin(current_url)

    # Classic CSV export URL
    csv_url = f"{origin}/{report_id}?isdtp=p1&export=1&enc=UTF-8&xf=csv"
    print(f"[DEBUG] Classic CSV URL: {csv_url}")

    try:
        # Use request fetch instead of navigation to avoid download cancellation
        resp = page.request.get(
            csv_url,
            headers={"Accept": "text/csv"},
            timeout=60000
        )
        if resp.status != 200:
            raise RuntimeError(f"CSV export request failed with status {resp.status}")

        content_bytes = resp.body()
        if not content_bytes:
            raise RuntimeError("No content received from CSV export")

        saved_path = _safe_save_bytes(target_path, content_bytes)

        # Sanity check: count actual CSV rows
        with open(saved_path, "rb") as f:
            n_rows = sum(1 for _ in f) - 1  # minus header
        print(f"[OK] Classic CSV export successful: {max(n_rows,0)} CSV rows ({len(content_bytes)} bytes)")

        return saved_path

    except Exception as e:
        print(f"[DEBUG] Classic CSV export failed: {e}")
        raise


def _export_via_ui(page, target_path: str) -> str:
    """Export report by clicking UI elements (last resort)."""
    print("[DEBUG] Attempting UI-based export")
    try:
        # Open the action bar/kebab if present
        # Try multiple known selectors; Lightning varies by org/theme
        candidates = [
            "button[title='Show more actions']",
            "button[aria-label='Show more actions']",
            "button.slds-button_icon-border-filled",  # common kebab style
            "button[title='Export']",                 # sometimes direct
        ]
        found = None
        for sel in candidates:
            try:
                el = page.locator(sel).first
                el.wait_for(state="visible", timeout=5000)
                el.click()
                found = True
                break
            except Exception:
                continue
        if not found:
            # Fallback: try the global action bar
            page.keyboard.press("Alt+E")  # sometimes opens export in report builder

        # Now click "Export" menu item
        export_item = page.get_by_role("menuitem", name=re.compile(r"^Export$", re.I))
        export_item.wait_for(state="visible", timeout=5000)
        export_item.click()

        # Choose CSV
        csv_option = page.get_by_label(re.compile(r"Comma.*csv", re.I))
        csv_option.wait_for(state="visible", timeout=5000)
        csv_option.check()

        # Confirm export and capture the download
        with page.expect_download(timeout=60000) as dl_info:
            page.get_by_role("button", name=re.compile(r"^Export$", re.I)).click()
        download = dl_info.value
        saved_path = _safe_save_bytes(target_path, download.path().read_bytes())
        print("[DEBUG] UI export successful")
        return saved_path

    except Exception as e:
        print(f"[DEBUG] UI export failed: {e}")
        raise


def _dump_error_context(page, context_name: str):
    """Save HTML and screenshot for debugging."""
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    try:
        # Save HTML
        html_path = LOG_DIR / f"sf_partner_calls_{context_name}_{timestamp}.html"
        html_path.write_text(page.content(), encoding='utf-8')

        # Save screenshot
        screenshot_path = LOG_DIR / f"sf_partner_calls_{context_name}_{timestamp}.png"
        page.screenshot(path=screenshot_path)

        print(f"[DEBUG] Saved debug context:")
        print(f"  HTML: {html_path}")
        print(f"  Screenshot: {screenshot_path}")
        print(f"  Current URL: {page.url}")
        print(f"  Page Title: {page.title()}")

    except Exception as e:
        print(f"[WARNING] Failed to save debug context: {e}")


def export_partner_calls():
    """Main entry point for Partner Calls export."""
    # Check for MFA lockout before attempting extraction
    from .auth_enhanced import should_skip_sf
    if should_skip_sf():
        raise RuntimeError('Skipping SF jobs due to recent MFA lockout (cooldown in effect).')

    report_url = "https://sso.lightning.force.com/lightning/r/Report/00OQp000009jCz8MAE/view?reportFilters=%5B%7B%22value%22%3A%22THIS%20MONTH%22%2C%22operator%22%3A%22equals%22%2C%22column%22%3A%22CREATED_DATE%22%7D%5D"

    print("[INFO] Starting SF Partner Calls extraction...")
    print(f"[DEBUG] Target report URL: {report_url}")

    print("[DEBUG] Step 1: Creating browser context...")
    start_time = time.time()
    pw, ctx = new_persistent_browser_context()
    page = ctx.new_page()
    page.set_default_timeout(60_000)
    print(f"[DEBUG] Browser context created in {time.time() - start_time:.2f}s")

    try:
        print(f"[DEBUG] Step 2: Initial page state - URL: {page.url}")
        print("[DEBUG] Step 3: Starting authentication process...")
        auth_start = time.time()

        # Enhanced login process
        login_if_needed(page, ctx)
        print(f"[DEBUG] Authentication completed in {time.time() - auth_start:.2f}s")

        # Verify we're actually authenticated to Lightning
        print("[DEBUG] Step 4: Verifying authentication state...")
        current_url = page.url
        print(f"[DEBUG] Current URL after auth: {current_url}")
        if not ("/lightning/" in current_url or "/one/one.app" in current_url or ".force.com" in current_url):
            print(f"[WARNING] Not on Lightning interface: {current_url}")
            print("[INFO] Attempting to navigate to report...")

        # Navigate to the report (only if not already there)
        print("[DEBUG] Step 5: Checking if navigation needed...")
        if report_url not in current_url:
            print(f"[DEBUG] Navigation required to: {report_url}")
            nav_start = time.time()
            # Lightning never "idles"; use domcontentloaded and a concrete selector
            page.goto(report_url, wait_until="domcontentloaded", timeout=45000)
            print(f"[DEBUG] Navigation completed in {time.time() - nav_start:.2f}s")
        else:
            print(f"[DEBUG] Already on report page: {current_url}")

        # Check if we're stuck on MFA before attempting export
        print("[DEBUG] Step 6: Checking for MFA intercepts...")
        final_url = page.url
        print(f"[DEBUG] Final URL after navigation: {final_url}")
        if re.search(r"verification|TotpVerification", final_url, flags=re.I):
            print(f"[ERROR] Still on MFA page: {final_url}")
            _dump_error_context(page, "still_on_mfa_page")
            raise RuntimeError("Still on MFA verification page after authentication - please run extraction manually")

        # Skip UI verification - go straight to API export with report ID from URL
        print("[DEBUG] Step 7: Extracting report ID from URL...")
        report_id = _report_id_from_url(final_url)
        print(f"[DEBUG] Extracted report ID: {report_id}")

        target_date = dt.date.today().isoformat()
        target = RAW_DIR / f"sf_partner_calls_{target_date}.csv"
        print(f"[DEBUG] Target file: {target}")

        # Preferred: REST -> CSV with structured logging
        print("[DEBUG] Step 8: Starting export process...")
        print(f"[INFO] Attempting Reports API export -> {target}")
        print(f"[DEBUG] Report ID: {report_id}")
        print(f"[DEBUG] API endpoint: /services/data/v60.0/analytics/reports/{report_id}")

        api_start = time.time()
        try:
            print("[DEBUG] Step 8a: Calling _export_via_rest_to_csv...")
            saved = _export_via_rest_to_csv(page, report_id, str(target))
            print(f"[OK] Exported via Reports API -> {saved} (in {time.time() - api_start:.2f}s)")
            return True
        except Exception as e_api:
            print(f"[WARNING] Reports API export failed after {time.time() - api_start:.2f}s: {e_api}")
            _dump_error_context(page, "rest_api_failed")

        # Fallback: Classic CSV servlet
        print("[DEBUG] Step 8b: Falling back to Classic CSV...")
        print(f"[INFO] Attempting Classic CSV export -> {target}")
        print(f"[DEBUG] Classic servlet URL: /{report_id}?isdtp=p1&export=1&enc=UTF-8&xf=csv")

        classic_start = time.time()
        try:
            print("[DEBUG] Step 8b: Calling _export_via_classic_csv...")
            saved = _export_via_classic_csv(page, report_id, str(target))
            print(f"[OK] Exported via Classic CSV -> {saved} (in {time.time() - classic_start:.2f}s)")
            return True
        except Exception as e_classic:
            print(f"[WARNING] Classic CSV export failed after {time.time() - classic_start:.2f}s: {e_classic}")
            _dump_error_context(page, "classic_export_failed")

        # Last resort: UI export
        print("[DEBUG] Step 8c: Falling back to UI export...")
        print(f"[INFO] Attempting UI export -> {target}")

        ui_start = time.time()
        try:
            print("[DEBUG] Step 8c: Calling _export_via_ui...")
            saved = _export_via_ui(page, str(target))
            print(f"[OK] Exported via UI -> {saved} (in {time.time() - ui_start:.2f}s)")
            return True
        except Exception as e_ui:
            print(f"[ERROR] UI export failed after {time.time() - ui_start:.2f}s: {e_ui}")
            print(f"[ERROR] Total extraction time: {time.time() - start_time:.2f}s")
            raise RuntimeError("All export methods failed")

    except Exception as e:
        print(f"[ERROR] SF Partner Calls extraction failed after {time.time() - start_time:.2f}s: {e}")
        print(f"[DEBUG] Error type: {type(e).__name__}")
        print(f"[DEBUG] Current page URL: {page.url}")
        print(f"[DEBUG] Page title: {page.title()}")
        _dump_error_context(page, "general_error")
        return False
    finally:
        try:
            page.close()
            print(f"[DEBUG] Page closed successfully")
        except Exception as close_e:
            print(f"[WARNING] Error closing page: {close_e}")


def run():
    """Main function for compatibility with subprocess calls."""
    return export_partner_calls()


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)