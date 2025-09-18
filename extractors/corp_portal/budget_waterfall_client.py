# extractors/corp_portal/budget_waterfall_client.py
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
from .portal_selectors import R_RUN_REPORT_SELECTORS, R_EXPORT_SELECTORS, select_partner_channel

load_dotenv(dotenv_path=".env")

# Budget Waterfall Client Report URL
BUDGET_WATERFALL_URL = "https://corp.reachlocal.com/reports/run.php?db_node=maindb&db_type=unified&link_id=8086"

def _date_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def run():
    download_dir = os.getenv("DOWNLOAD_DIR", "data/raw/budget_waterfall_client")
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    pw, context = new_persistent_browser_context()
    page = context.new_page()
    page.set_default_timeout(120_000)

    try:
        print(f"[DEBUG] Navigating to Budget Waterfall Client report...")
        page.goto(BUDGET_WATERFALL_URL, wait_until="domcontentloaded")

        # Ensure logged in
        ensure_logged_in(page)

        # Select Partner channel if available
        select_partner_channel(page)

        print(f"[DEBUG] Looking for Run button...")
        run_clicked = False

        for sel in R_RUN_REPORT_SELECTORS:
            try:
                loc = page.locator(sel).first
                if loc.is_visible(timeout=2000):
                    print(f"[DEBUG] Found run button: {sel}")
                    loc.click()
                    run_clicked = True
                    break
            except Exception as e:
                print(f"[DEBUG] Run selector '{sel}' failed: {e}")

        if not run_clicked:
            raise RuntimeError("Could not find or click Run button")

        print(f"[DEBUG] Waiting for results to load...")
        page.wait_for_timeout(10000)

        print(f"[DEBUG] Looking for Export link...")
        export_clicked = False

        for sel in R_EXPORT_SELECTORS:
            try:
                loc = page.locator(sel).first
                if loc.is_visible(timeout=5000):
                    print(f"[DEBUG] Found export link: {sel}")

                    with page.expect_download(timeout=60_000) as download_info:
                        loc.click()

                    download = download_info.value
                    filename = f"budget_waterfall_client_{_date_stamp()}.csv"
                    filepath = Path(download_dir) / filename

                    download.save_as(filepath)
                    print(f"[SUCCESS] Downloaded Budget Waterfall Client report to: {filepath}")

                    if filepath.exists() and filepath.stat().st_size > 0:
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
        print(f"[ERROR] Budget Waterfall Client extraction failed: {e}")
        return False
    finally:
        context.close()
        pw.stop()

if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)