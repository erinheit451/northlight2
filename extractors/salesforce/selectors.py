"""
Salesforce Lightning UI selectors and constants.
Centralizes URLs and resilient locators for Lightning components.
"""

import re

SF_LOGIN_URL = "https://login.salesforce.com/"

SF_REPORT_URL = "https://sso.lightning.force.com/lightning/r/Report/00OQp00000616O5MAI/view"

# Login selectors
USERNAME_SELECTOR = "input[name='username'], input[id='username'], input[type='email']"
PASSWORD_SELECTOR = "input[name='pw'], input[name='password'], input[id='password'], input[type='password']"
LOGIN_BUTTON_SELECTOR = "input[type='submit'], button[type='submit'], input[name='Login'], button:has-text('Log In')"

def BTN_EXPORT(page):
    """Modern Lightning Export button (multiple possible selectors)"""
    # Try multiple approaches for Lightning export buttons
    selectors = [
        'button:has-text("Export")',
        'button[title*="Export"]',
        'lightning-button-menu button:has-text("Export")',
        '.slds-button:has-text("Export")',
        'a:has-text("Export")',
        '[data-target-selection-name*="export"]',
        'button[aria-label*="Export"]'
    ]

    for selector in selectors:
        try:
            element = page.locator(selector).first
            if element.is_visible(timeout=1000):
                return element
        except:
            continue

    return page.get_by_role("button", name="Export")

def BTN_EDIT(page):
    """Overflow menu is more reliable than literal 'Edit'"""
    return page.locator(
        "lightning-button-menu button[title*='Show more actions'], "
        "button[aria-label*='Show more actions']"
    ).first

def MENUITEM_EXPORT(page):
    """Export menu item under Edit dropdown"""
    return page.get_by_role("menuitem", name=re.compile(r"\bExport\b", re.I))

def DIALOG_EXPORT(page):
    """Export modal dialog"""
    return page.get_by_role("dialog", name="Export")

def RADIO_DETAILS_ONLY(page):
    """Details Only radio button in export modal"""
    return page.get_by_label("Details Only")

def SELECT_FORMAT(page):
    """Format dropdown in export modal"""
    return page.get_by_label("Format")

def BTN_MODAL_EXPORT(page):
    """Export button within the modal"""
    return page.get_by_role("button", name="Export")