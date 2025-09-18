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

    sid = None
    for c in page.context.cookies():
        if c.get("name") == "sid" and ("salesforce.com" in c.get("domain","") or "force.com" in c.get("domain","")):
            sid = c.get("value"); break
    if not sid:
        raise RuntimeError("No Salesforce session cookie (sid) found. Are you fully authenticated?")

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

def _classic_origin(page) -> str:
    """
    Use *.my.salesforce.com for Classic servlet, even if current page is on *.lightning.force.com.
    Prefer a cookie domain that ends with '.my.salesforce.com'; fall back to mapping the current host.
    """
    # 1) Try cookie domains
    for c in page.context.cookies():
        dom = (c.get("domain") or "").lstrip(".")
        if dom.endswith(".my.salesforce.com"):
            return f"https://{dom}"
    # 2) Map the current URL
    host = urlparse(page.url).hostname or ""
    if host.endswith(".lightning.force.com"):
        mapped = host.replace(".lightning.force.com", ".my.salesforce.com")
        return f"https://{mapped}"
    # 3) Fallback: if current host already my.salesforce.com or my domain
    if host.endswith(".my.salesforce.com"):
        return f"https://{host}"
    raise RuntimeError(f"Cannot determine Classic origin from host '{host}'")

def _rest_fetch_report_json(page, report_id: str) -> Dict[str, Any]:
    origin, sid = _sf_origin_and_sid(page)
    api = f"{origin}/services/data/v60.0/analytics/reports/{report_id}"
    # includeDetails=true returns detail rows; CSV comes from flattening this JSON
    resp = page.request.get(api + "?includeDetails=true", headers={"Authorization": f"Bearer {sid}"}, timeout=60_000)
    if resp.status != 200:
        raise RuntimeError(f"Reports API HTTP {resp.status}: {resp.text()[:500]}")
    body = resp.body()
    if not body or body[:1] not in (b"{", b"["):
        # not JSON → probably HTML or empty
        raise RuntimeError("Reports API did not return JSON (auth redirect or error HTML).")
    return json.loads(body.decode("utf-8", errors="replace"))

def _flatten_salesforce_report(json_obj: Dict[str, Any]) -> (List[str], List[List[Any]]):
    """
    Works for tabular and matrix reports with hasDetailRows=true.
    Uses reportMetadata.detailColumns for column order.
    """
    meta = json_obj.get("reportMetadata", {})
    detail_cols = meta.get("detailColumns") or []
    if not detail_cols:
        raise RuntimeError("No detailColumns in report metadata; cannot flatten details.")
    # Build column labels from extended metadata; fall back to the API names
    ext = json_obj.get("reportExtendedMetadata", {}).get("detailColumnInfo", {})
    headers = []
    for api_name in detail_cols:
        info = ext.get(api_name, {})
        headers.append(info.get("label") or api_name)

    # detail rows live in factMap entries that contain 'rows'
    rows_out: List[List[Any]] = []
    fact_map = json_obj.get("factMap", {}) or {}
    for key, block in fact_map.items():
        for r in block.get("rows", []) or []:
            cells = r.get("dataCells", [])
            # Map the known detailColumns order to the cells. When Salesforce includes
            # grouping columns, dataCells aligns to detailColumns in order.
            if len(cells) < len(detail_cols):
                # tolerate short rows by padding
                values = [None] * len(detail_cols)
                for i, c in enumerate(cells):
                    values[i] = c.get("value")
            else:
                values = [c.get("value") for c in cells[:len(detail_cols)]]
            # Normalize complex currency objects, dates, etc.
            norm = []
            for v in values:
                if isinstance(v, dict) and "amount" in v:
                    norm.append(v.get("amount"))
                else:
                    norm.append(v)
            rows_out.append(norm)
    if not rows_out:
        # Some tabular reports use a special key like "T!T" and hide detail rows;
        # surface that clearly instead of silently producing an empty CSV.
        raise RuntimeError("Report returned no detail rows to flatten (check filters or report format).")
    return headers, rows_out

def _write_csv(target_path: str, headers: List[str], rows: List[List[Any]]):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(headers)
    for r in rows:
        w.writerow(r)
    # commit via safe bytes (handles locks)
    _safe_save_bytes(target_path, buf.getvalue().encode("utf-8"))

def _export_via_rest_to_csv(page, report_id: str, target_csv: str):
    """
    Preferred path: REST → JSON → flattened CSV.
    Also writes alongside: target_csv with '.json' for audit.
    """
    data = _rest_fetch_report_json(page, report_id)
    # keep raw JSON for audit/debug
    raw_json_path = re.sub(r"\.csv$", ".json", target_csv, flags=re.I)
    _safe_save_bytes(raw_json_path, json.dumps(data, ensure_ascii=False).encode("utf-8"))

    headers, rows = _flatten_salesforce_report(data)
    _write_csv(target_csv, headers, rows)
    return target_csv

def _ensure_bytes_are_csv_or_raise(b: bytes):
    head = (b or b"")[:256].lstrip()
    if head[:1] in (b"{", b"["):
        raise RuntimeError("Body looks like JSON, not CSV.")
    if head.startswith(b"<!DOCTYPE") or b"<html" in head.lower():
        raise RuntimeError("Body looks like HTML, not CSV.")

def _export_via_classic_csv(page, report_id: str, target_csv: str):
    """
    Fallback: Classic export servlet returns CSV inline (not attachment JSON/HTML).
    """
    origin = _classic_origin(page)
    url = f"{origin}/servlet/servlet.ReportExport?reportId={report_id}&export=1&enc=UTF-8&xf=csv"
    resp = page.request.get(url, headers={"Authorization": f"Bearer {_get_sid(page)}"}, timeout=60_000)
    body = resp.body()
    if resp.status != 200:
        raise RuntimeError(f"Classic export HTTP {resp.status}: {resp.text()[:500]}")
    # Guard: reject HTML/JSON masquerading as CSV
    _ensure_bytes_are_csv_or_raise(body)
    return _safe_save_bytes(target_csv, body)

def _dump_error_context(page, error_type):
    """
    Dump page HTML and screenshot for debugging on failure.

    Args:
        page: Playwright page object
        error_type: String describing the error context
    """
    try:
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        html_path = LOG_DIR / f"sf_pipeline_{error_type}_{stamp}.html"
        png_path = LOG_DIR / f"sf_pipeline_{error_type}_{stamp}.png"

        html_path.write_text(page.content(), encoding="utf-8", errors="ignore")
        page.screenshot(path=str(png_path), full_page=True)

        print(f"[DEBUG] Saved error context:")
        print(f"  HTML: {html_path}")
        print(f"  Screenshot: {png_path}")

        if error_type == "export_button_not_found":
            print("[DEBUG] Couldn't find Export; Lightning hasn't rendered or org UI differs.")
            print("[DEBUG] Open the HTML in a browser and locate the Export control to refine selectors.")
        elif error_type == "html_download":
            print("[DEBUG] Download returned HTML (auth/redirect).")
            print("[DEBUG] Fix: Run once interactively, complete verification code.")

    except Exception as dump_error:
        print(f"[WARNING] Failed to dump error context: {dump_error}")

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
        target = RAW_DIR / f"sf_partner_pipeline_{target_date}.csv"

        # Preferred: REST → CSV
        print(f"[INFO] Attempting Reports API export → {target}")
        try:
            saved = _export_via_rest_to_csv(page, report_id, str(target))
            print(f"[OK] Exported via Reports API → {saved}")
            return True
        except Exception as e_api:
            print(f"[WARNING] Reports API export failed: {e_api}")

        # Fallback: Classic CSV servlet
        print("[INFO] Trying Classic servlet export…")
        try:
            saved = _export_via_classic_csv(page, report_id, str(target))
            print(f"[OK] Exported via Classic → {saved}")
            return True
        except Exception as e_cls:
            print(f"[WARNING] Classic export failed: {e_cls}")

        # Last resort: UI click path
        print("[INFO] Falling back to UI-based export…")
        try:
            _export_via_ui(page, str(target))
            print(f"[OK] Exported via UI → {target}")
            return True
        except Exception as e_ui:
            print(f"[ERROR] UI export failed: {e_ui}")
            _dump_error_context(page, "all_export_methods_failed")
            raise RuntimeError("All export paths failed (REST + Classic + UI). Check logs and .env.")

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
    Fallback UI-based export approach with improved selectors.
    """
    print("[INFO] Looking for export controls...")

    try:
        # Try direct export button first
        export_btn = page.locator('button[title*="Export"], button[aria-label*="Export"], .slds-button:has-text("Export")').first
        expect(export_btn).to_be_visible(timeout=10_000)
        export_btn.click()
        print("[INFO] Clicked direct Export button")
    except PWTimeoutError:
        print("[INFO] Trying overflow menu → Export")
        # Try overflow/kebab menu
        try:
            kebab = page.locator("lightning-button-menu button[title*='Show more actions'], button[aria-label*='Show more actions']").first
            expect(kebab).to_be_visible(timeout=15_000)
            kebab.click()

            menu_export = page.get_by_role("menuitem", name=re.compile(r"\bExport\b", re.I))
            expect(menu_export).to_be_visible(timeout=10_000)
            menu_export.click()
            print("[INFO] Clicked Export from overflow menu")
        except PWTimeoutError:
            _dump_error_context(page, "export_controls_not_found")
            raise RuntimeError("Could not find Export button or overflow menu")

    # Handle export modal
    print("[INFO] Waiting for export modal...")
    try:
        dlg = page.get_by_role("dialog", name=re.compile(r"Export", re.I))
        expect(dlg).to_be_visible(timeout=20_000)
        print("[INFO] Export modal opened")

        # Select Details Only
        page.get_by_label(re.compile(r"Details\s*Only", re.I)).check()
        print("[INFO] Selected Details Only")

        # Select CSV format
        fmt_select = page.get_by_label(re.compile(r"Format", re.I))
        fmt_select.select_option(label="CSV")
        print("[INFO] Selected CSV format")

        # Set up download handler and click export
        with page.expect_download() as dl_info:
            page.get_by_role("button", name=re.compile(r"\bExport\b", re.I)).click()
            print("[INFO] Clicked Export in modal")

        download = dl_info.value
        temp_path = download.path()

        # Guard: Check if download is HTML (auth redirect)
        with open(temp_path, "rb") as fh:
            head = fh.read(256)

        if head.lstrip().startswith(b"<!DOCTYPE") or b"<html" in head.lower():
            _dump_error_context(page, "html_download")
            raise RuntimeError("Download returned HTML (auth/redirect). Session may have expired.")

        # Save the file
        download.save_as(str(target_path))
        print(f"[OK] SF Partner Pipeline exported via UI → {target_path}")

    except PWTimeoutError:
        _dump_error_context(page, "export_modal_timeout")
        raise RuntimeError("Export modal did not appear or timed out")


def run():
    """Main function for compatibility with subprocess calls."""
    return export_partner_pipeline()


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)