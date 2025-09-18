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
from .auth import ensure_logged_in, force_login
from .portal_selectors import RUN_REPORT_SELECTORS, RESULTS_READY_MARKERS, select_partner_channel

load_dotenv(dotenv_path=".env")

# Budget Waterfall by Client Report URL with Partner filter and date parameters
BUDGET_WATERFALL_URL = "https://corp.reachlocal.com/reports/run.php?db_node=maindb&db_type=unified&link_id=7504&favorite_id=18756&favorite_title=Budget%20Waterfall%20by%20Client%20-%20With%20Live%20MTD%20Estimates%20-%20Custom&reportmenu=1&Region_Channel=NA%20-%20Partner&td_From_SOD=on&td_To_EOD=on&td_Region_Channel=on&td_First_AID=on&td_Advertiser_Name=on&td_Maturity=on&td_BID=on&td_Business_Name=on&td_Office=on&td_Area=on&td_Currency=on&td_Starting_Clients=on&td_Churned_Clients=on&td_New_Clients=on&td_Winback_Clients=on&td_Ending_Clients=on&td_SOM_Budgets=on&td_Budgets=on&td_Net_New_Budgets=on&td_Net_Change_Pct=on&td_New_Client_Budgets=on&td_Winback_Client_Budgets=on&td_Flighted_Acquired_Client_Budgets=on&td_Total_CrossSales=on&td_Total_Upsells=on&td_Total_Inflows=on&td_Total_Reverse_CrossSales=on&td_Total_Downsells=on&td_Raw_Churned_Existing_Client_Total_Budgets=on&td_Flighted_Churned_Existing_Client_Total_Budgets=on&td_Total_Decrease=on&td_DrillDown3=on&td_DrillDown4=on"

def _date_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def _configure_date_range(page):
    """Configure the date range for the budget waterfall report based on current date."""
    try:
        current_date = datetime.now()

        # Set start date to first day of current month
        start_date = current_date.replace(day=1)
        start_date_str = start_date.strftime("%Y-%m-%d")

        # Set end date to current date
        end_date_str = current_date.strftime("%Y-%m-%d")

        print(f"[DEBUG] Setting date range: {start_date_str} to {end_date_str}")

        # Try to find and set the start date field (From_SOD)
        start_date_selectors = [
            'input[name="From_SOD"]',
            'input[name="from_sod"]',
            'input[id="From_SOD"]',
            'input[type="date"][name*="From"]',
            'input[type="text"][name*="From"]'
        ]

        for selector in start_date_selectors:
            try:
                start_field = page.locator(selector).first
                if start_field.is_visible(timeout=2000):
                    start_field.clear()
                    start_field.fill(start_date_str)
                    print(f"[DEBUG] Set start date using selector: {selector}")
                    break
            except Exception as e:
                print(f"[DEBUG] Start date selector {selector} failed: {e}")
                continue

        # Try to find and set the end date field (To_EOD)
        end_date_selectors = [
            'input[name="To_EOD"]',
            'input[name="to_eod"]',
            'input[id="To_EOD"]',
            'input[type="date"][name*="To"]',
            'input[type="text"][name*="To"]'
        ]

        for selector in end_date_selectors:
            try:
                end_field = page.locator(selector).first
                if end_field.is_visible(timeout=2000):
                    end_field.clear()
                    end_field.fill(end_date_str)
                    print(f"[DEBUG] Set end date using selector: {selector}")
                    break
            except Exception as e:
                print(f"[DEBUG] End date selector {selector} failed: {e}")
                continue

        return True

    except Exception as e:
        print(f"[DEBUG] Date configuration failed: {e}")
        return False

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

def run():
    download_dir = os.getenv("DOWNLOAD_DIR", "data/raw/budget_waterfall_client")
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    pw, context = new_persistent_browser_context()
    page = context.new_page()
    page.set_default_timeout(120_000)

    try:
        # 1) Fresh login and navigate to report
        print("[DEBUG] Starting authentication...")
        ensure_logged_in(page)
        print("[DEBUG] Authentication completed, navigating to Budget Waterfall report...")
        page.goto(BUDGET_WATERFALL_URL, wait_until="domcontentloaded")

        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except:
            print("[DEBUG] Network idle timeout, but continuing...")
        print(f"[DEBUG] Budget Waterfall report loaded, current URL: {page.url}")

        # 2) Select Partner channel if available
        print("[DEBUG] Attempting to select Partner channel...")
        select_partner_channel(page)

        # 3) Configure date range
        print("[DEBUG] Configuring date range...")
        _configure_date_range(page)

        # 4) Check if we got bounced to login and retry once
        if "logon.php" in (page.url or "") or _find_in_any_frame(page, 'form[name="logon"]'):
            print("[DEBUG] Detected login page, forcing fresh authentication...")
            force_login(page)
            page.goto(BUDGET_WATERFALL_URL, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=30_000)
            except:
                print("[DEBUG] Network idle timeout after retry, but continuing...")

        # 5) If results are NOT present, click "Run Report"
        if not _wait_results_ready(page):
            run_btn = None

            # Try the standard selectors plus some specific ones for Budget Waterfall
            selectors_to_try = RUN_REPORT_SELECTORS + [
                'input[name="go"]',
                'input[type="submit"][value*="Run"]',
                'button:has-text("Run")',
                'a:has-text("Run")'
            ]

            for css in selectors_to_try:
                loc = _find_in_any_frame(page, css)
                if loc:
                    run_btn = loc
                    print(f"[DEBUG] Found run button with selector: {css}")
                    break

            if not run_btn:
                # Dump DOM for debugging
                page.screenshot(path="logs/budget_waterfall_no_run.png", full_page=True)
                with open("logs/budget_waterfall_dom_before_run.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
                with open("logs/budget_waterfall_frames_before_run.txt", "w", encoding="utf-8") as f:
                    for fr in page.frames:
                        f.write(f"{fr.name} | {fr.url}\n")
                raise RuntimeError('Could not find "Run Report" control for Budget Waterfall. See logs/*.html')

            # Click run and wait for results
            print("[DEBUG] Clicking run button...")
            try:
                with page.expect_navigation(timeout=60_000):
                    run_btn.click()
            except:
                # Sometimes no navigation happens
                run_btn.click()

            try:
                page.wait_for_load_state("networkidle", timeout=30_000)
            except:
                print("[DEBUG] Network idle timeout after run click, but continuing...")

            if not _wait_results_ready(page):
                # Give it more time for async processing
                page.wait_for_timeout(2000)
                if not _wait_results_ready(page):
                    page.screenshot(path="logs/budget_waterfall_after_run_no_results.png", full_page=True)
                    with open("logs/budget_waterfall_dom_after_run.html", "w", encoding="utf-8") as f:
                        f.write(page.content())
                    raise RuntimeError("Budget Waterfall: Ran report but did not detect results/controls.")

        # 6) Click "Export" and capture download
        export_selectors = [
            'a[href*="excel_version=1"]',
            'a:has-text("Export")',
            'input[type="submit"][value*="Export"]',
            'button:has-text("Export")'
        ]

        export_link = None
        for sel in export_selectors:
            export_link = _find_in_any_frame(page, sel)
            if export_link:
                print(f"[DEBUG] Found export link with selector: {sel}")
                break

        if not export_link:
            page.screenshot(path="logs/budget_waterfall_no_export.png", full_page=True)
            with open("logs/budget_waterfall_dom_no_export.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            raise RuntimeError('Budget Waterfall: Export link not found. Check logs/budget_waterfall_dom_no_export.html')

        export_link.scroll_into_view_if_needed()
        with page.expect_download(timeout=60_000) as dl_info:
            export_link.click()
        dl = dl_info.value

        suggested = dl.suggested_filename or f"budget_waterfall_client_{_date_stamp()}.xls"
        print(f"[DEBUG] Download suggested filename: {suggested}")

        # Get absolute paths
        download_dir_abs = os.path.abspath(download_dir)
        tmp_path = os.path.join(download_dir_abs, suggested)
        print(f"[DEBUG] Saving download to: {tmp_path}")

        # Save the download
        try:
            dl.save_as(tmp_path)
        except Exception as e:
            print(f"[DEBUG] save_as failed: {e}")

        # Check if file was saved where expected, fallback to Downloads folder if needed
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
                raise RuntimeError("No Budget Waterfall report files found in Downloads folder")

        # Create final path with date stamp
        ext = os.path.splitext(suggested)[1] or ".csv"
        final_path = os.path.join(download_dir_abs, f"budget_waterfall_client_{_date_stamp()}{ext}")
        print(f"[DEBUG] Final path will be: {final_path}")

        if os.path.exists(final_path):
            os.remove(final_path)

        if os.path.exists(tmp_path):
            print(f"[DEBUG] tmp_path exists, renaming to final_path")
            os.rename(tmp_path, final_path)
            print(f"[DEBUG] Rename completed")
        else:
            print(f"[ERROR] tmp_path does not exist: {tmp_path}")
            raise RuntimeError(f"Could not locate downloaded Budget Waterfall file at {tmp_path}")

        # Verify final file exists
        if not os.path.exists(final_path):
            print(f"[ERROR] Final file does not exist: {final_path}")
            raise RuntimeError(f"Final Budget Waterfall file was not created: {final_path}")

        print(f"[DEBUG] Final file confirmed to exist: {final_path}")

        # Guardrail: ensure not login HTML
        with open(final_path, "rb") as f:
            head = f.read(4096)
        if _looks_like_login(head):
            page.screenshot(path="logs/budget_waterfall_login_after_download.png", full_page=True)
            with open("logs/budget_waterfall_after_download.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            raise RuntimeError("Budget Waterfall: Downloaded login HTML instead of data (auth bounce).")

        print(f"[OK] Budget Waterfall exported to {final_path}")
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