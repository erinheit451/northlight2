# extractors/corp_portal/ultimate_dms.py
import os
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import TimeoutError as PWTimeoutError

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
sys.path.append(str(Path(__file__).parent))
from ..playwright_bootstrap import new_persistent_browser_context
from .auth import ensure_logged_in
from .portal_selectors import ULTIMATE_DMS_URL, RUN_REPORT_SELECTORS, RESULTS_READY_MARKERS

load_dotenv(dotenv_path=".env")

def _date_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def _looks_like_login(buf: bytes) -> bool:
    head = (buf or b"")[:4096].lower()
    return b"maincontent-logon" in head or b"corporate portal" in head or b'name="ux"' in head

def _find_in_any_frame(page, css: str):
    for fr in page.frames:
        loc = fr.locator(css).first
        if loc.count():
            return loc
    return None

def _wait_results_ready(page) -> bool:
    # success if any results marker appears
    for sel in RESULTS_READY_MARKERS:
        try:
            loc = _find_in_any_frame(page, sel)
            if loc:
                loc.wait_for(state="visible", timeout=5_000)
                return True
        except Exception:
            pass
    return False

def run():
    download_dir = os.getenv("DOWNLOAD_DIR", "data/raw/ultimate_dms")
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    pw, context = new_persistent_browser_context()
    page = context.new_page()
    page.set_default_timeout(120_000)

    try:
        # 1) Fresh login first
        print("[DEBUG] Starting authentication...")
        ensure_logged_in(page)
        print("[DEBUG] Authentication completed, navigating to report...")

        # 2) Navigate to report
        page.goto(ULTIMATE_DMS_URL, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except:
            print("[DEBUG] Network idle timeout, but continuing...")
        print(f"[DEBUG] Report loaded, current URL: {page.url}")

        print(f"[DEBUG] Looking for Run button...")
        run_clicked = False

        for sel in RUN_REPORT_SELECTORS:
            try:
                loc = _find_in_any_frame(page, sel)
                if loc and loc.is_visible():
                    print(f"[DEBUG] Found run button: {sel}")
                    loc.click()
                    run_clicked = True
                    break
            except Exception as e:
                print(f"[DEBUG] Run selector '{sel}' failed: {e}")

        if not run_clicked:
            raise RuntimeError("Could not find or click Run button")

        print(f"[DEBUG] Waiting for results to load...")

        # Wait for results with extended timeout
        if not _wait_results_ready(page):
            print("[WARNING] Results ready markers not found, proceeding anyway...")

        # Wait a bit more for full load
        page.wait_for_timeout(5000)

        print(f"[DEBUG] Looking for Export link...")

        # Look for export link
        export_clicked = False
        export_selectors = [
            'a[href*="excel_version=1"]',
            'a:has-text("Export")',
            'a[href*="everything=1&excel_version=1"]'
        ]

        for sel in export_selectors:
            try:
                loc = _find_in_any_frame(page, sel)
                if loc and loc.is_visible():
                    print(f"[DEBUG] Found export link: {sel}")

                    # Set up download handling
                    with page.expect_download(timeout=60_000) as download_info:
                        loc.click()

                    download = download_info.value
                    filename = f"ultimate_dms_{_date_stamp()}.csv"
                    filepath = Path(download_dir) / filename

                    download.save_as(filepath)
                    print(f"[SUCCESS] Downloaded Ultimate DMS report to: {filepath}")

                    # Verify file
                    if filepath.exists() and filepath.stat().st_size > 0:
                        with open(filepath, 'rb') as f:
                            head = f.read(1024)
                            if _looks_like_login(head):
                                print("[ERROR] Downloaded file appears to be login page")
                                return False

                        print(f"[SUCCESS] File verified: {filepath.stat().st_size} bytes")
                        export_clicked = True
                        break
                    else:
                        print(f"[ERROR] Download failed or empty file")
                        return False

            except Exception as e:
                print(f"[DEBUG] Export selector '{sel}' failed: {e}")

        if not export_clicked:
            raise RuntimeError("Could not find or click Export link")

        return True

    except Exception as e:
        print(f"[ERROR] Ultimate DMS extraction failed: {e}")
        return False
    finally:
        context.close()
        pw.stop()

if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)