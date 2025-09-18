"""
Enhanced Salesforce authentication with better error handling and domain support.
"""

import os
import re
import time
from playwright.sync_api import expect, TimeoutError as PWTimeoutError
from . import selectors as S


LOCKOUT_PATTERNS = r"(exceeded the maximum number of verification attempts|too many attempts|temporarily locked|try again later|verification attempts have been exceeded)"

def is_lockout(page):
    """Detect if Salesforce has locked out MFA attempts."""
    try:
        txt = page.inner_text("body", timeout=5000) if page else ""
        return re.search(LOCKOUT_PATTERNS, txt, re.I) is not None
    except Exception:
        return False

def should_skip_sf():
    """Check if we should skip SF extractors due to recent lockout."""
    import json
    import datetime as dt
    from pathlib import Path

    FLAG = Path("tmp/sf_mfa_lock.json")
    COOLDOWN_MIN = 20

    if not FLAG.exists():
        return False

    try:
        ts_str = json.loads(FLAG.read_text())["ts"]
        ts = dt.datetime.fromisoformat(ts_str)
        elapsed_min = (dt.datetime.now() - ts).total_seconds() / 60

        if elapsed_min < COOLDOWN_MIN:
            print(f"[WARNING] SF MFA lockout cooldown active - {COOLDOWN_MIN - elapsed_min:.1f}m remaining")
            return True
        else:
            # Cooldown expired, remove lock file
            FLAG.unlink()
            return False
    except Exception:
        return False

def set_sf_lock():
    """Set MFA lockout flag to skip SF extractors for cooldown period."""
    import json
    import datetime as dt
    from pathlib import Path

    FLAG = Path("tmp/sf_mfa_lock.json")
    FLAG.parent.mkdir(parents=True, exist_ok=True)
    FLAG.write_text(json.dumps({"ts": dt.datetime.now().isoformat()}))
    print(f"[WARNING] Set SF MFA lockout flag - will skip SF jobs for 20 minutes")

def solve_mfa_if_present(page, totp_code=None, timeout_ms=60000):
    """
    Robust MFA handling covering method chooser + multiple selectors + iframes.
    As per fix #2 - handles verification pages comprehensively.
    """
    # Are we on any verification page?
    if not re.search(r"verification|TotpVerification", page.url, re.I):
        return False  # no MFA

    print(f"[DEBUG] MFA verification page detected: {page.url}")

    # CRITICAL: Check for lockout first to avoid extending lockout
    if is_lockout(page):
        print("[ERROR] Salesforce MFA lockout detected: too many attempts")
        set_sf_lock()
        raise RuntimeError("Salesforce MFA lockout detected: too many attempts. Skipping SF pipelines for this run.")

    # If there's a "use different method" link, click it and choose Authenticator/Code
    try:
        link = page.get_by_role("link", name=re.compile("use a different|try another", re.I))
        if link and link.is_visible():
            print("[DEBUG] Clicking 'use different method' link")
            link.click()
            page.wait_for_load_state("networkidle", timeout=5000)
    except Exception as e:
        print(f"[DEBUG] No 'use different method' link found: {e}")

    # Pick "Authenticator App" / "Verification code" if shown
    try:
        # button or option labels vary a lot, cover common variants
        method = (
            page.get_by_role("button", name=re.compile("authenticator|verification code|time-based", re.I)).first
            or page.get_by_role("option", name=re.compile("authenticator|verification code|time-based", re.I)).first
        )
        if method and method.is_visible():
            print("[DEBUG] Selecting authenticator app method")
            method.click()
            page.wait_for_load_state("networkidle", timeout=5000)
    except Exception as e:
        print(f"[DEBUG] No method selector found: {e}")

    # Get TOTP code if not provided
    if not totp_code:
        totp_code = os.environ.get("SF_TOTP_CODE")
        if not totp_code:
            try:
                totp_code = input("Enter your Salesforce MFA verification code (from authenticator app): ").strip()
            except EOFError:
                raise RuntimeError("MFA verification required but no TOTP code available. Set SF_TOTP_CODE environment variable or run interactively.")

    if not totp_code:
        raise ValueError("TOTP verification code cannot be empty")

    # Find the code input (page or frames) - enhanced selectors
    input_selectors = (
        '[autocomplete="one-time-code"], '
        'input[id*="code" i], input[name*="code" i], '
        'input[id*="otp" i],  input[name*="otp" i], '
        'input[id*="totp" i], input[name*="totp" i], '
        'input[id*="smc" i], input[name*="smc" i], '
        'input[type="tel"], input[type="text"], input[type="number"], '
        'input[placeholder*="code" i], input[placeholder*="verification" i]'
    )

    field = None

    # Try main page first
    try:
        field = page.locator(input_selectors).first
        if field and field.is_visible(timeout=2000):
            print("[DEBUG] Found MFA input field on main page")
        else:
            field = None
    except Exception:
        field = None

    # If not found on main page, try all frames
    if not field:
        for fr in page.frames:
            try:
                cand = fr.locator(input_selectors).first
                if cand and cand.is_visible(timeout=2000):
                    field = cand
                    print(f"[DEBUG] Found MFA input field in frame: {fr.url}")
                    break
            except Exception:
                continue

    if not field or not field.is_visible():
        raise RuntimeError("MFA verification failed: Could not find MFA verification code input field")

    print(f"[DEBUG] Filling MFA code: {totp_code}")
    field.fill(totp_code)

    # Click a verify/submit/continue button
    verify_selectors = [
        "button:has-text('Verify')",
        "button:has-text('Submit')",
        "button:has-text('Continue')",
        "button:has-text('Next')",
        "input[type='submit']",
        "button[type='submit']",
        "input[value*='verify' i]",
        "input[value*='submit' i]"
    ]

    verify = None
    for selector in verify_selectors:
        try:
            button = page.locator(selector).first
            if button and button.is_visible(timeout=2000):
                verify = button
                print(f"[DEBUG] Found verify button: {selector}")
                break
        except Exception:
            continue

    if not verify:
        # Fallback to generic button search
        verify = (
            page.get_by_role("button", name=re.compile("verify|submit|continue", re.I)).first
            or page.locator("input[type='submit']").first
        )

    if not verify:
        raise RuntimeError("Could not find MFA verify/submit button")

    print("[DEBUG] Clicking verify button")
    verify.click()

    # Wait to leave verification and reach Lightning or the report
    try:
        print("[DEBUG] Waiting to leave MFA verification page...")
        page.wait_for_url(re.compile(r"lightning/|/one/one\.app", re.I), timeout=timeout_ms)
        print("[DEBUG] Successfully left MFA verification page")
        return True
    except PWTimeoutError:
        print(f"[WARNING] Still on verification page after {timeout_ms}ms: {page.url}")
        return False


def retry_on_failure(max_retries=3, delay=2):
    """Decorator to retry functions on failure with exponential backoff."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:  # Last attempt
                        raise e
                    wait_time = delay * (2 ** attempt)  # Exponential backoff
                    print(f"[WARNING] Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
            return None
        return wrapper
    return decorator


def login_if_needed(page, context=None, max_retries=3):
    """
    Enhanced Salesforce login with SSO and domain support, retry logic, and session state management.

    Args:
        page: Playwright page object
        context: Playwright browser context (for storage state management)
        max_retries: Maximum number of retry attempts for authentication steps
    """
    # Check if we should skip SF due to recent lockout
    if should_skip_sf():
        raise RuntimeError("Skipping SF authentication due to recent MFA lockout (cooldown in effect).")

    current_url = page.url
    print(f"[DEBUG] Current URL: {current_url}")

    # First, test if existing session is valid by trying to access Salesforce
    if current_url == "about:blank" or "salesforce" not in current_url.lower():
        print("[DEBUG] Testing existing session validity...")
        try:
            # Try to navigate to Salesforce homepage to test session
            test_url = "https://sso.lightning.force.com/lightning/page/home"
            page.goto(test_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=10000)

            current_url = page.url
            print(f"[DEBUG] After session test navigation: {current_url}")

        except Exception as e:
            print(f"[DEBUG] Session test navigation failed: {e}")
            # Continue with fresh auth below

    # Check if already authenticated in Lightning (but not on MFA pages)
    if (("/lightning/r/Report/" in current_url or
         "/one/one.app" in current_url or
         "/lightning/" in current_url or
         ".force.com" in current_url) and
        "verification" not in current_url.lower() and
        "totp" not in current_url.lower()):
        print(f"[INFO] Already authenticated in Lightning: {current_url}")
        _save_storage_state_if_needed(context)
        return

    print("[INFO] Logging into Salesforce...")

    # Get credentials from environment
    username = os.getenv("SF_USERNAME")
    password = os.getenv("SF_PASSWORD")
    custom_domain = os.getenv("SF_DOMAIN")  # Optional custom domain

    if not username or not password:
        raise ValueError("SF_USERNAME and SF_PASSWORD must be set in .env file")

    print(f"[DEBUG] Using username: {username}")
    if custom_domain:
        print(f"[DEBUG] Using custom domain: {custom_domain}")

    # For SSO orgs, try to navigate directly to the report first
    # If it redirects to login, we'll handle it there
    if "sso.lightning.force.com" in S.SF_REPORT_URL:
        print("[DEBUG] SSO detected - attempting direct report access...")
        try:
            page.goto(S.SF_REPORT_URL, wait_until="networkidle", timeout=30000)
            current_url = page.url
            print(f"[DEBUG] After SSO redirect: {current_url}")

            # Check if we successfully reached Lightning (but not MFA verification)
            if (("/lightning/" in current_url or
                 "/one/one.app" in current_url or
                 ".force.com" in current_url) and
                "verification" not in current_url.lower() and
                "totp" not in current_url.lower()):
                print("[INFO] SSO authentication successful - already in Lightning")
                _save_storage_state_if_needed(context)
                return

            # If we're still not in Lightning, continue with login flow
            print("[DEBUG] SSO redirect completed, now handling authentication...")

            # Check if we're on an ADFS or SSO login page
            if "adfs" in current_url.lower() or "sso" in current_url.lower():
                print("[DEBUG] ADFS/SSO login page detected")
                _handle_adfs_login(page, username, password)
                return

        except Exception as e:
            # Check if we actually have a valid session despite UI timeout
            current_url = page.url
            if (("/lightning/" in current_url or "/one/one.app" in current_url or ".force.com" in current_url) and
                "verification" not in current_url.lower() and "totp" not in current_url.lower()):
                print(f"[WARNING] Lightning UI wait timed out, but session is valid - switching to API export: {current_url}")
                _save_storage_state_if_needed(context)
                return
            else:
                print(f"[DEBUG] SSO direct access failed: {e}")

    # If we reach here, ADFS/SSO authentication failed, try standard login
    # But first check one more time if we're already authenticated
    current_url = page.url
    if (("/lightning/" in current_url or "/one/one.app" in current_url or ".force.com" in current_url) and
        "verification" not in current_url.lower() and "totp" not in current_url.lower()):
        print(f"[INFO] Already authenticated despite SSO exception: {current_url}")
        return

    # Navigate to login page - use custom domain if provided
    login_url = f"https://{custom_domain}.my.salesforce.com/" if custom_domain else "https://login.salesforce.com"
    print(f"[DEBUG] Navigating to login page: {login_url}")

    # Only navigate to login if we're not already there
    if page.url != login_url and "login" not in page.url:
        page.goto(login_url, wait_until="networkidle")

    # Wait for page to fully load
    page.wait_for_load_state("networkidle")

    # Check if we need to handle custom domain selection
    if _handle_custom_domain_if_needed(page, custom_domain):
        # Reload page after domain selection
        page.wait_for_load_state("networkidle")

    # Check if we're on username chooser page first
    if "Choose a Username" in page.title():
        print("[DEBUG] Username chooser page detected...")
        _handle_username_selection(page)
        return

    # Fill login form with enhanced error checking
    try:
        print("[DEBUG] Looking for login form...")

        # Wait for username field to be visible
        username_field = page.locator("input[name='username'], input[id='username'], input[type='email']").first
        expect(username_field).to_be_visible(timeout=10000)

        password_field = page.locator("input[name='pw'], input[name='password'], input[id='password'], input[type='password']").first
        expect(password_field).to_be_visible(timeout=10000)

        login_btn = page.locator("input[name='Login'], input[value='Log In'], button:has-text('Log In')").first
        expect(login_btn).to_be_visible(timeout=10000)

        print("[DEBUG] Filling login form...")
        username_field.clear()
        username_field.fill(username)

        password_field.clear()
        password_field.fill(password)

        print("[DEBUG] Clicking login button...")
        login_btn.click()

        # Wait for page response
        page.wait_for_load_state("networkidle", timeout=30000)

    except PWTimeoutError as e:
        _dump_debug_info(page, "login_form_not_found")
        raise RuntimeError(f"Login form not found or not accessible: {e}")

    # Check for login errors
    if _check_login_errors(page):
        _dump_debug_info(page, "login_credentials_failed")
        raise RuntimeError("Login failed - check credentials and account access")

    # Check for verification code requirement with retry logic as per fix #2
    for attempt in range(2):
        if re.search(r"verification|TotpVerification", page.url, re.I):
            print(f"[INFO] MFA verification required (attempt {attempt + 1}/2)...")
            if solve_mfa_if_present(page, totp_code=os.environ.get("SF_TOTP_CODE")):
                break  # Successfully solved MFA
        else:
            break  # Not on verification page

    # Wait for Lightning interface
    try:
        print("[DEBUG] Waiting for Lightning interface...")
        page.wait_for_function(
            """
            window.location.href.includes('/lightning/') ||
            window.location.href.includes('/one/one.app') ||
            window.location.href.includes('.force.com') ||
            document.title.includes('Lightning')
            """,
            timeout=60000
        )
        page.wait_for_load_state("networkidle", timeout=30000)
        print(f"[INFO] Successfully authenticated to Lightning: {page.url}")
        _save_storage_state_if_needed(context)

    except PWTimeoutError:
        _dump_debug_info(page, "lightning_interface_timeout")
        raise RuntimeError(f"Failed to reach Lightning interface. Current URL: {page.url}")


def _handle_custom_domain_if_needed(page, custom_domain):
    """Handle custom domain selection if the page shows domain options."""
    try:
        # Check if custom domain input is visible
        domain_input = page.locator("input[name='mydomain'], input[id='mydomain']")
        if domain_input.is_visible(timeout=3000) and custom_domain:
            print(f"[DEBUG] Setting custom domain: {custom_domain}")
            domain_input.fill(custom_domain)

            continue_btn = page.locator("button:has-text('Continue'), input[value='Continue']")
            if continue_btn.is_visible(timeout=2000):
                continue_btn.click()
                return True

    except PWTimeoutError:
        pass

    return False


def _check_login_errors(page):
    """Check for login error messages on the page."""
    try:
        # Common error selectors (using first() to avoid strict mode violations)
        error_selectors = [
            "#error",
            ".loginError",
            ".error",
            "[role='alert']",
            ".slds-notify--alert"
        ]

        for selector in error_selectors:
            error_element = page.locator(selector).first
            if error_element.is_visible(timeout=2000):
                error_text = error_element.text_content()
                if error_text and error_text.strip() and "error" in error_text.lower():
                    print(f"[ERROR] Login error detected: {error_text}")
                    return True

    except PWTimeoutError:
        pass

    # Check if still on login page (indicates failed login)
    if "login.salesforce.com" in page.url and "login" in page.url.lower():
        return True

    return False


def _handle_adfs_login(page, username, password):
    """
    Handle ADFS/SSO login flow.

    Args:
        page: Playwright page object
        username: Username for authentication
        password: Password for authentication
    """
    try:
        print("[DEBUG] Handling ADFS/SSO login...")

        # Wait for ADFS login form to load
        page.wait_for_load_state("networkidle")

        # Common ADFS selectors
        username_selectors = [
            "input[name='UserName']",
            "input[name='Username']",
            "input[name='username']",
            "input[id='userNameInput']",
            "input[type='email']",
            "input[type='text']"
        ]

        password_selectors = [
            "input[name='Password']",
            "input[name='password']",
            "input[id='passwordInput']",
            "input[type='password']"
        ]

        submit_selectors = [
            "#submitButton",  # ADFS specific
            "span[role='button']:has-text('Sign in')",
            "span.submit",
            "input[type='submit']",
            "button[type='submit']",
            "input[value*='Sign in']",
            "input[value*='Login']",
            "button:has-text('Sign in')",
            "button:has-text('Login')"
        ]

        # Find and fill username
        username_field = None
        for selector in username_selectors:
            try:
                field = page.locator(selector).first
                if field.is_visible(timeout=2000):
                    username_field = field
                    print(f"[DEBUG] Found username field: {selector}")
                    break
            except PWTimeoutError:
                continue

        if not username_field:
            raise RuntimeError("Could not find username field on ADFS page")

        # Find password field
        password_field = None
        for selector in password_selectors:
            try:
                field = page.locator(selector).first
                if field.is_visible(timeout=2000):
                    password_field = field
                    print(f"[DEBUG] Found password field: {selector}")
                    break
            except PWTimeoutError:
                continue

        if not password_field:
            raise RuntimeError("Could not find password field on ADFS page")

        # Find submit button
        submit_button = None
        for selector in submit_selectors:
            try:
                button = page.locator(selector).first
                if button.is_visible(timeout=2000):
                    submit_button = button
                    print(f"[DEBUG] Found submit button: {selector}")
                    break
            except PWTimeoutError:
                continue

        if not submit_button:
            raise RuntimeError("Could not find submit button on ADFS page")

        # Fill credentials and submit
        print("[DEBUG] Filling ADFS credentials...")
        username_field.clear()
        username_field.fill(username)

        password_field.clear()
        password_field.fill(password)

        print("[DEBUG] Submitting ADFS login...")
        submit_button.click()

        # Wait for redirect back to Salesforce
        page.wait_for_load_state("networkidle", timeout=30000)

        # Wait for Lightning interface or MFA verification
        try:
            print("[DEBUG] Waiting for Lightning interface after ADFS...")
            page.wait_for_function(
                """
                window.location.href.includes('/lightning/') ||
                window.location.href.includes('/one/one.app') ||
                window.location.href.includes('.force.com') ||
                window.location.href.includes('verification') ||
                document.title.includes('Lightning')
                """,
                timeout=60000
            )
            page.wait_for_load_state("networkidle", timeout=30000)

            current_url = page.url
            print(f"[INFO] ADFS authentication successful: {current_url}")

            # Check if we need to handle MFA verification using robust solver
            if "verification" in current_url.lower() or "totp" in current_url.lower():
                print("[DEBUG] MFA verification required after ADFS...")
                for attempt in range(2):
                    if solve_mfa_if_present(page, totp_code=os.environ.get("SF_TOTP_CODE")):
                        break  # Successfully solved MFA

                # Wait for post-MFA redirect
                page.wait_for_load_state("networkidle", timeout=30000)
                current_url = page.url

                # Check if we need to handle username selection
                if "login.salesforce.com" in current_url and "Choose a Username" in page.title():
                    print("[DEBUG] Username selection required after MFA...")
                    _handle_username_selection(page)

                    # Wait for Lightning after username selection
                    page.wait_for_load_state("networkidle", timeout=30000)
                    current_url = page.url

                # Wait for Lightning after MFA/username selection
                try:
                    page.wait_for_function(
                        """
                        window.location.href.includes('/lightning/') ||
                        window.location.href.includes('/one/one.app') ||
                        window.location.href.includes('.force.com')
                        """,
                        timeout=30000
                    )
                    page.wait_for_load_state("networkidle", timeout=30000)
                    print(f"[INFO] MFA verification completed: {page.url}")
                except PWTimeoutError:
                    # Check if we're already on Lightning (sometimes the wait fails even though we're there)
                    current_url = page.url
                    if ("/lightning/" in current_url or "/one/one.app" in current_url or ".force.com" in current_url):
                        print(f"[INFO] Already on Lightning after MFA: {current_url}")
                    else:
                        raise RuntimeError(f"Failed to reach Lightning after MFA. Current URL: {current_url}")

        except PWTimeoutError:
            # Check if we're actually on Lightning despite timeout
            current_url = page.url
            if (("/lightning/" in current_url or "/one/one.app" in current_url or ".force.com" in current_url) and
                "verification" not in current_url.lower() and "totp" not in current_url.lower()):
                print(f"[WARNING] Lightning UI timeout, but authentication succeeded - continuing with API: {current_url}")
                _save_storage_state_if_needed(context)
                return
            else:
                _dump_debug_info(page, "adfs_lightning_timeout")
                raise RuntimeError(f"Lightning UI timeout after ADFS authentication. Current URL: {current_url}")

    except Exception as e:
        # Check for session validity before failing
        current_url = page.url
        if (("/lightning/" in current_url or "/one/one.app" in current_url or ".force.com" in current_url) and
            "verification" not in current_url.lower() and "totp" not in current_url.lower()):
            print(f"[WARNING] ADFS UI flow error, but session is valid - continuing: {current_url}")
            _save_storage_state_if_needed(context)
            return
        else:
            _dump_debug_info(page, "adfs_login_failed")
            raise RuntimeError(f"ADFS authentication failed: {e}")


def _handle_username_selection(page):
    """
    Handle Salesforce username selection when multiple saved usernames are available.

    Args:
        page: Playwright page object
    """
    try:
        print("[DEBUG] Handling username selection...")

        # Wait for username chooser to load
        page.wait_for_load_state("networkidle")

        # Look for saved username links (they contain the username in an <li> with an <a> tag)
        username_links = page.locator("#idlist li a")

        if username_links.count() > 0:
            # Click the first saved username
            first_username = username_links.first
            username_text = first_username.locator("span").text_content()
            print(f"[DEBUG] Selecting saved username: {username_text}")
            first_username.click()

            # Wait for navigation to complete and Lightning to load
            page.wait_for_load_state("networkidle", timeout=30000)

            # Wait specifically for Lightning interface
            try:
                print("[DEBUG] Waiting for Lightning interface after username selection...")
                page.wait_for_function(
                    """
                    window.location.href.includes('/lightning/') ||
                    window.location.href.includes('/one/one.app') ||
                    window.location.href.includes('.force.com')
                    """,
                    timeout=60000
                )
                page.wait_for_load_state("networkidle", timeout=30000)
                print(f"[INFO] Lightning authentication completed: {page.url}")
            except PWTimeoutError:
                # Check if we're already on Lightning
                current_url = page.url
                if ("/lightning/" in current_url or "/one/one.app" in current_url or ".force.com" in current_url):
                    print(f"[INFO] Already on Lightning after username selection: {current_url}")
                else:
                    print(f"[WARNING] Not on Lightning after username selection: {current_url}")

            print("[INFO] Username selection completed")
        else:
            # Look for "Log In with a Different Username" link
            different_username_link = page.locator("#use_new_identity")
            if different_username_link.is_visible(timeout=5000):
                print("[DEBUG] Clicking 'Log In with a Different Username'")
                different_username_link.click()
                page.wait_for_load_state("networkidle", timeout=30000)
            else:
                raise RuntimeError("No username options found on username selection page")

    except Exception as e:
        _dump_debug_info(page, "username_selection_failed")
        raise RuntimeError(f"Username selection failed: {e}")


def _dump_debug_info(page, error_type):
    """
    Dump page information for debugging.
    """
    try:
        import datetime
        import pathlib

        logs_dir = pathlib.Path("logs")
        logs_dir.mkdir(exist_ok=True)

        stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        html_path = logs_dir / f"sf_auth_debug_{error_type}_{stamp}.html"
        png_path = logs_dir / f"sf_auth_debug_{error_type}_{stamp}.png"

        html_path.write_text(page.content(), encoding="utf-8", errors="ignore")
        page.screenshot(path=str(png_path), full_page=True)

        print(f"[DEBUG] Saved debug context:")
        print(f"  HTML: {html_path}")
        print(f"  Screenshot: {png_path}")
        print(f"  Current URL: {page.url}")
        print(f"  Page Title: {page.title()}")

    except Exception as dump_error:
        print(f"[WARNING] Failed to dump debug info: {dump_error}")


def _save_storage_state_if_needed(context):
    """Save storage state for session reuse across all SF reports if context is provided."""
    if context:
        try:
            # Import here to avoid circular imports
            from ..playwright_bootstrap import save_storage_state
            save_storage_state(context)
        except Exception as e:
            print(f"[WARNING] Failed to save storage state: {e}")