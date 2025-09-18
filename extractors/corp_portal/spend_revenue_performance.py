# extractors/corp_portal/spend_revenue_performance.py
import os
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import TimeoutError as PWTimeoutError

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
sys.path.append(str(Path(__file__).parent))
from ..playwright_bootstrap import new_persistent_browser_context
from .auth import ensure_logged_in, force_login
from .portal_selectors import RUN_REPORT_SELECTORS, RESULTS_READY_MARKERS, select_partner_channel

load_dotenv(dotenv_path=".env")

# Spend, Revenue & Performance Over Time Report URL - Partner Custom Report
def get_spend_revenue_url():
    """Generate Spend Revenue URL with current month dates using provided configuration."""
    from datetime import datetime
    current_month = datetime.now().strftime("%Y-%m-01")

    return (
        "https://corp.reachlocal.com/reports/run.php?db_node=maindb&db_type=unified"
        f"&link_id=6615&favorite_id=18757&favorite_title=Spend%2C%20Revenue%20%26%20Performance%20Over%20Time%20-%20Custom"
        f"&reportmenu=1&Start_Month={current_month}&End_Month={current_month}"
        "&td_Report_month=on&td_country=on&td_channel=on&td_coBrand_name=on&td_area_name=on"
        "&td_office_name=on&td_account_owner=on&td_idbusiness=on&td_BusinessSubType_Name=on"
        "&td_Service_Tier=on&td_FAID=on&td_MAID=on&td_idadvertiser=on&td_MCID=on"
        "&td_finance_product=on&td_idcampaign=on&td_idOffer=on&td_Vertical=on"
        "&td_BusinessCategory=on&td_BusinessSubCategory=on&td_business_name=on"
        "&td_client_name=on&td_Campaign_name=on&td_net_cost=on&td_net_cost_adjustment=on"
        "&td_spend=on&td_total_fees=on&td_revenue=on&td_impressions=on&td_clicks=on"
        "&td_calls=on&td_emails=on&td_qualified_web_events=on&td_leads=on"
        "&td_Cost_per_call=on&td_Cost_per_Click=on&td_Cost_per_Lead=on&td_CPM=on"
        "&td_CTR=on&td_Clicks_per_Lead=on&td_currency=on"
    )

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
    # Success if any results marker appears
    for sel in RESULTS_READY_MARKERS:
        try:
            loc = _find_in_any_frame(page, sel)
            if loc:
                loc.wait_for(state="visible", timeout=5_000)
                return True
        except Exception:
            pass
    return False

def _set_current_month_dates(page):
    """Set Start_Month and End_Month to current month if selectors are found."""
    from datetime import datetime
    current_month = datetime.now().strftime("%Y-%m-01")

    try:
        # Try to set Start_Month
        start_selectors = [
            'select[name="Start_Month"]',
            'select#Start_Month',
            'select[name*="Start_Month"]'
        ]

        for selector in start_selectors:
            try:
                start_select = page.locator(selector).first
                if start_select.is_visible(timeout=2000):
                    start_select.select_option(value=current_month)
                    print(f"[DEBUG] Set Start_Month to {current_month}")
                    break
            except:
                continue

        # Try to set End_Month
        end_selectors = [
            'select[name="End_Month"]',
            'select#End_Month',
            'select[name*="End_Month"]'
        ]

        for selector in end_selectors:
            try:
                end_select = page.locator(selector).first
                if end_select.is_visible(timeout=2000):
                    end_select.select_option(value=current_month)
                    print(f"[DEBUG] Set End_Month to {current_month}")
                    break
            except:
                continue

    except Exception as e:
        print(f"[DEBUG] Date setting failed (may not be needed): {e}")

def run(period=None):
    download_dir = os.getenv("DOWNLOAD_DIR", "data/raw/spend_revenue_performance")
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

        # 2) Navigate to the Spend Revenue report
        url = get_spend_revenue_url()
        print(f"[DEBUG] Navigating to Spend Revenue report: {url}")
        page.goto(url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except:
            print("[DEBUG] Network idle timeout, but continuing...")
        print(f"[DEBUG] Report loaded, current URL: {page.url}")

        # 3) Set date parameters if needed
        _set_current_month_dates(page)

        # 4) Select Partner channel if needed
        select_partner_channel(page)

        # 5) If results are NOT present, click "Run Report"
        if not _wait_results_ready(page):
            run_btn = None
            for css in RUN_REPORT_SELECTORS:
                loc = _find_in_any_frame(page, css)
                if loc and loc.is_visible():
                    run_btn = loc
                    break

            if not run_btn:
                # Dump DOM to inspect the actual run control
                page.screenshot(path="logs/spend_revenue_no_run.png", full_page=True)
                with open("logs/spend_revenue_dom_before_run.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
                raise RuntimeError('Could not find a "Run Report" control for Spend Revenue.')

            # Click run and wait for results to materialize
            print("[DEBUG] Clicking Run Report button...")
            run_btn.click()
            try:
                page.wait_for_load_state("networkidle", timeout=30_000)
            except:
                print("[DEBUG] Network idle timeout after run click, but continuing...")

            if not _wait_results_ready(page):
                # Sometimes the page uses async JS; give it another second
                page.wait_for_timeout(1500)
                if not _wait_results_ready(page):
                    page.screenshot(path="logs/spend_revenue_after_run_no_results.png", full_page=True)
                    with open("logs/spend_revenue_dom_after_run.html", "w", encoding="utf-8") as f:
                        f.write(page.content())
                    raise RuntimeError("Ran the report but did not detect results/controls.")

        # 6) Click "Export" and capture download
        export_link = _find_in_any_frame(page, 'a[href*="excel_version=1"]') \
                      or _find_in_any_frame(page, 'a:has-text("Export")')
        if not export_link:
            page.screenshot(path="logs/spend_revenue_no_export.png", full_page=True)
            with open("logs/spend_revenue_dom_no_export.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            raise RuntimeError('Export link not found after results.')

        export_link.scroll_into_view_if_needed()
        with page.expect_download(timeout=60_000) as dl_info:
            export_link.click()
        dl = dl_info.value

        suggested = dl.suggested_filename or f"spend_revenue_{_date_stamp()}.xls"
        print(f"[DEBUG] Download suggested filename: {suggested}")

        # Get absolute paths to ensure we save to the right place
        download_dir_abs = os.path.abspath(download_dir)
        tmp_path = os.path.join(download_dir_abs, suggested)
        print(f"[DEBUG] Saving download to: {tmp_path}")

        # Save the download
        try:
            dl.save_as(tmp_path)
        except Exception as e:
            print(f"[DEBUG] save_as failed: {e}")

        # Check if file was saved where expected
        if not os.path.exists(tmp_path):
            print(f"[DEBUG] File not at expected location, checking Downloads folder...")
            downloads_folder = os.path.join(os.path.expanduser("~"), "Downloads")

            # Find the most recent Corporate_Portal_run or report file
            potential_files = []
            for filename in os.listdir(downloads_folder):
                if (filename.startswith("Corporate_Portal_run") or filename.startswith("report")) and filename.endswith((".csv", ".xls", ".xlsx")):
                    file_path = os.path.join(downloads_folder, filename)
                    file_time = os.path.getmtime(file_path)
                    potential_files.append((file_time, filename, file_path))

            if potential_files:
                # Get the most recent file (highest timestamp)
                potential_files.sort(reverse=True)
                latest_time, latest_name, latest_path = potential_files[0]
                print(f"[DEBUG] Found latest file in Downloads: {latest_name}")

                # Move it to our target location
                import shutil
                shutil.move(latest_path, tmp_path)
                suggested = latest_name
                print(f"[DEBUG] Moved {latest_name} to {tmp_path}")
            else:
                raise RuntimeError("No report files found in Downloads folder")

        ext = os.path.splitext(suggested)[1] or ".csv"
        final_path = os.path.join(download_dir_abs, f"spend_revenue_{_date_stamp()}{ext}")
        print(f"[DEBUG] Final path will be: {final_path}")

        if os.path.exists(final_path):
            os.remove(final_path)

        if os.path.exists(tmp_path):
            print(f"[DEBUG] tmp_path exists, renaming to final_path")
            os.rename(tmp_path, final_path)
            print(f"[DEBUG] Rename completed")
        else:
            print(f"[ERROR] tmp_path does not exist: {tmp_path}")
            raise RuntimeError(f"Could not locate downloaded file at {tmp_path}")

        # Verify final file exists
        if not os.path.exists(final_path):
            print(f"[ERROR] Final file does not exist: {final_path}")
            raise RuntimeError(f"Final file was not created: {final_path}")

        print(f"[DEBUG] Final file confirmed to exist: {final_path}")

        # 7) Guardrail: ensure not login HTML
        with open(final_path, "rb") as f:
            head = f.read(4096)
        if _looks_like_login(head):
            page.screenshot(path="logs/spend_revenue_login_after_download.png", full_page=True)
            with open("logs/spend_revenue_after_download.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            raise RuntimeError("Downloaded login HTML instead of data (auth bounce).")

        print(f"[OK] Spend Revenue Performance exported to {final_path}")
        return True

    except Exception as e:
        print(f"[ERROR] Spend Revenue Performance extraction failed: {e}")
        return False
    finally:
        context.close()
        pw.stop()

if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)