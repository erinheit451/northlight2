import os
import json
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
        pw = sync_playwright().start()

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