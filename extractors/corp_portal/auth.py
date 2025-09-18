# extractors/corp_portal/auth.py
import os
from dotenv import load_dotenv
from playwright.sync_api import Page, TimeoutError

from .portal_selectors import (
    LOGIN_URL, PORTAL_HOME,
    SEL_LOGIN_EMAIL, SEL_LOGIN_PASSWORD, SEL_LOGIN_SUBMIT
)

load_dotenv(dotenv_path=".env")

def _on_login(page: Page) -> bool:
    try:
        if "logon.php" in (page.url or ""):
            return True
        # direct selector check
        return page.locator(SEL_LOGIN_EMAIL).first.is_visible(timeout=500)
    except Exception:
        return False

def login(page: Page):
    username = os.getenv("CORP_PORTAL_USERNAME")
    password = os.getenv("CORP_PORTAL_PASSWORD")
    if not username or not password:
        raise RuntimeError("Missing CORP_PORTAL_USERNAME or CORP_PORTAL_PASSWORD in .env")

    print(f"[DEBUG] Attempting login with username: {username}")
    page.goto(LOGIN_URL, wait_until="domcontentloaded")
    print(f"[DEBUG] Login page loaded: {page.url}")

    # If already logged in, the portal may redirect away from logon.php
    if not _on_login(page):
        print("[DEBUG] Already logged in, skipping login form")
        return

    print("[DEBUG] Filling login form...")
    page.locator(SEL_LOGIN_EMAIL).fill(username)
    page.locator(SEL_LOGIN_PASSWORD).fill(password)
    print("[DEBUG] Clicking submit button...")
    page.locator(SEL_LOGIN_SUBMIT).click()
    page.wait_for_load_state("domcontentloaded")
    print(f"[DEBUG] After login click, URL: {page.url}")

    # Cement session by hitting home
    page.goto(PORTAL_HOME, wait_until="domcontentloaded")
    print(f"[DEBUG] After goto home, URL: {page.url}")
    if _on_login(page):
        raise RuntimeError("Login failed or blocked by portal. Check creds / VPN.")

def force_login(page: Page):
    """Force a fresh login by clearing any existing session."""
    print("[DEBUG] force_login: Clearing session and forcing fresh login...")
    # Clear cookies to force fresh authentication
    page.context.clear_cookies()
    login(page)
    print("[DEBUG] force_login: Fresh login completed")

def ensure_logged_in(page: Page):
    # Always force a fresh login for report access (report requires elevated session)
    print("[DEBUG] ensure_logged_in: Forcing fresh login for report access...")
    login(page)
    print("[DEBUG] ensure_logged_in: Fresh login completed")