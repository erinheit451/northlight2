# extractors/corp_portal/agreed_cpl_performance.py
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
from .portal_selectors import AGREED_CPL_URL, R_RUN_REPORT_SELECTORS, RESULTS_READY_MARKERS, select_partner_channel

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
    download_dir = os.getenv("DOWNLOAD_DIR", "data/raw/agreed_cpl_performance")
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    pw, context = new_persistent_browser_context()
    page = context.new_page()
    page.set_default_timeout(120_000)

    try:
        # 1) Fresh login and navigate to Agreed CPL Performance report
        print("[DEBUG] Starting authentication...")
        ensure_logged_in(page)
        print("[DEBUG] Authentication completed, navigating to Agreed CPL Performance report...")
        page.goto(AGREED_CPL_URL, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except:
            print("[DEBUG] Network idle timeout, but continuing...")
        print(f"[DEBUG] Agreed CPL Performance report loaded, current URL: {page.url}")

        # Select Partner channel if available
        print("[DEBUG] Attempting to select Partner channel...")
        select_partner_channel(page)

        # 2) If results are NOT present, click "Run Report"
        if not _wait_results_ready(page):
            run_btn = None
            for css in R_RUN_REPORT_SELECTORS:
                loc = _find_in_any_frame(page, css)
                if loc:
                    run_btn = loc
                    break

            if not run_btn:
                # dump DOM to inspect the actual run control
                page.screenshot(path="logs/agreed_cpl_no_run.png", full_page=True)
                with open("logs/agreed_cpl_dom_before_run.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
                with open("logs/agreed_cpl_frames_before_run.txt", "w", encoding="utf-8") as f:
                    for fr in page.frames:
                        f.write(f"{fr.name} | {fr.url}\n")
                raise RuntimeError('Could not find a "Run Report" control. See logs/*.html to add the right selector.')

            # Click run and wait for results to materialize
            run_btn.click()
            try:
                page.wait_for_load_state("networkidle", timeout=30_000)
            except:
                print("[DEBUG] Network idle timeout after run click, but continuing...")

            if not _wait_results_ready(page):
                # Sometimes the page uses async JS; give it another second
                page.wait_for_timeout(1500)
                if not _wait_results_ready(page):
                    page.screenshot(path="logs/agreed_cpl_after_run_no_results.png", full_page=True)
                    with open("logs/agreed_cpl_dom_after_run.html", "w", encoding="utf-8") as f:
                        f.write(page.content())
                    raise RuntimeError("Ran the Agreed CPL Performance report but did not detect results/controls.")

        # 3) Click "Export" and capture download
        export_link = _find_in_any_frame(page, 'a[href*="excel_version=1"]') \
                      or _find_in_any_frame(page, 'a:has-text("Export")')
        if not export_link:
            page.screenshot(path="logs/agreed_cpl_no_export.png", full_page=True)
            with open("logs/agreed_cpl_dom_no_export.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            raise RuntimeError('Export link not found after results. Check logs/agreed_cpl_dom_no_export.html')

        export_link.scroll_into_view_if_needed()
        with page.expect_download(timeout=60_000) as dl_info:
            export_link.click()
        dl = dl_info.value

        suggested = dl.suggested_filename or f"agreed_cpl_performance_{_date_stamp()}.xls"
        print(f"[DEBUG] Download suggested filename: {suggested}")

        # Get absolute paths to ensure we save to the right place
        download_dir_abs = os.path.abspath(download_dir)
        tmp_path = os.path.join(download_dir_abs, suggested)
        print(f"[DEBUG] Saving download to: {tmp_path}")

        # Save the download (this often fails silently, so we'll check Downloads folder)
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
        final_path = os.path.join(download_dir_abs, f"agreed_cpl_performance_{_date_stamp()}{ext}")
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

        # 5) Guardrail: ensure not login HTML
        with open(final_path, "rb") as f:
            head = f.read(4096)
        if _looks_like_login(head):
            page.screenshot(path="logs/agreed_cpl_login_after_download.png", full_page=True)
            with open("logs/agreed_cpl_after_download.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            raise RuntimeError("Downloaded login HTML instead of data (auth bounce).")

        print(f"[OK] Agreed CPL Performance exported to {final_path}")
        return True

    except Exception as e:
        print(f"[ERROR] Agreed CPL Performance extraction failed: {e}")
        return False
    finally:
        context.close()
        pw.stop()

if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)