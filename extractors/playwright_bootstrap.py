import os
import json
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv(dotenv_path=".env")

def new_persistent_browser_context(pw=None, headless=True):
    """Create a persistent browser context with proper session persistence."""
    from pathlib import Path

    STORAGE = Path("secrets/sf_auth.json")
    STORAGE.parent.mkdir(parents=True, exist_ok=True)

    if pw is None:
        # Handle potential asyncio conflict more robustly
        try:
            pw = sync_playwright().start()
        except RuntimeError as e:
            if "asyncio" in str(e).lower():
                print(f"[DEBUG] Asyncio conflict detected: {e}")
                print("[DEBUG] Attempting to resolve by clearing event loop policy...")

                # Try multiple approaches to resolve asyncio conflict
                try:
                    # Method 1: Clear asyncio event loop policy
                    asyncio.set_event_loop_policy(None)
                    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
                    pw = sync_playwright().start()
                    print("[DEBUG] Successfully resolved via event loop policy reset")
                except Exception as policy_e:
                    print(f"[DEBUG] Event loop policy reset failed: {policy_e}")

                    # Method 2: Try with new thread approach
                    print("[DEBUG] Attempting to resolve by running in new thread...")
                    import threading
                    import queue

                    result_queue = queue.Queue()
                    def init_playwright():
                        try:
                            # Clear any thread-local asyncio state
                            try:
                                asyncio.set_event_loop(None)
                            except:
                                pass
                            result_queue.put(("success", sync_playwright().start()))
                        except Exception as thread_e:
                            result_queue.put(("error", thread_e))

                    thread = threading.Thread(target=init_playwright)
                    thread.start()
                    thread.join()

                    status, result = result_queue.get()
                    if status == "error":
                        raise result
                    pw = result
                    print("[DEBUG] Successfully initialized Playwright in separate thread")
            else:
                raise

    browser = pw.chromium.launch(
        headless=headless,
        args=[
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",  # Avoid detection
            "--disable-web-security",  # Better compatibility with enterprise SSO
            "--ignore-certificate-errors",  # Handle SSL issues in corporate environments
            "--disable-extensions-except=",
            "--disable-extensions"
        ]
    )

    if STORAGE.exists():
        print("[DEBUG] Loaded existing Salesforce storage_state")
        ctx = browser.new_context(
            storage_state=str(STORAGE),
            viewport={"width": 1280, "height": 720},
            ignore_https_errors=True,
            locale="en-US",
            timezone_id="America/New_York",
            accept_downloads=True
        )
    else:
        print("[DEBUG] No existing storage state found, will create new session")
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 720},
            ignore_https_errors=True,
            locale="en-US",
            timezone_id="America/New_York",
            accept_downloads=True
        )

    return pw, ctx

def save_storage_state(ctx):
    """Save current browser session state for reuse across all SF reports."""
    from pathlib import Path

    STORAGE = Path("secrets/sf_auth.json")

    try:
        # Ensure parent directory exists
        STORAGE.parent.mkdir(parents=True, exist_ok=True)

        # Save the storage state
        ctx.storage_state(path=str(STORAGE))
        print(f"[DEBUG] Saved Salesforce storage_state → {STORAGE}")
        return True
    except Exception as e:
        print(f"[WARNING] Failed to save storage state: {e}")
        return False

def clear_storage_state(storage_state_file=None):
    """Clear stored session state (forces fresh login)."""
    if storage_state_file is None:
        storage_state_file = os.getenv("STORAGE_STATE_FILE", ".pw_storage_state.json")

    try:
        storage_state_path = Path(storage_state_file)
        if storage_state_path.exists():
            storage_state_path.unlink()
            print(f"[DEBUG] Cleared storage state from {storage_state_file}")
            return True
    except Exception as e:
        print(f"[WARNING] Failed to clear storage state: {e}")

    return False