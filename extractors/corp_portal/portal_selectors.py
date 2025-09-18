# extractors/corp_portal/portal_selectors.py

LOGIN_URL = "https://corp.reachlocal.com/common/logon.php"
PORTAL_HOME = "https://corp.reachlocal.com/common/index.php"

# Login form fields from provided HTML:
SEL_LOGIN_EMAIL = 'input[name="ux"]'
SEL_LOGIN_PASSWORD = 'input[name="px"]'
SEL_LOGIN_SUBMIT = 'input[name="commit"]'

# Report + Export
ULTIMATE_DMS_URL = (
    "https://corp.reachlocal.com/reports/run.php?db_node=maindb&db_type=unified"
    "&link_id=7524&favorite_id=18690&favorite_title=Ultimate%20DMS%20Campaign%20Performance%20Report%20-%20Partner"
    "&reportmenu=1&Channel=Partner&td_last_active=on&td_Channel=on&td_business_name2=on&td_idbusiness=on"
    "&td_advertiser_name=on&td_primary_user_name=on&td_AM=on&td_AM_Manager=on&td_Optimizer1_Manager=on&td_Optimizer1=on"
    "&td_Optimizer2_Manager=on&td_Optimizer2=on&td_MAID=on&td_MCID_clicks=on&td_MCID_leads=on&td_MCID=on&td_campaign_name=on"
    "&td_idcampaign=on&td_product=on&td_offer_name=on&td_finance_product=on&td_tracking_method_name=on&td_All_Reviews_P30=on"
    "&td_IO_Cycle=on&td_Avg_Cycle_Length=on&td_RunningCID_Leads=on&td_amount_Spent=on&td_days_elapsed=on&td_Utilization=on"
    "&td_Campaign_Performance_Rating=on&td_BC=on&td_BSC=on&td_campaign_budget=on&td_Budget_10=on&td_Budget_25=on"
    "&td_Budget_Average=on&td_Budget_75=on&td_Budget_90=on&td_CPL_Agreed=on&td_CID_CPL=on&td_CPL_MCID=on"
    "&td_CPL_Last15_Days=on&td_CPL_15_to_30Days=on&td_CPL_30_to_60Days=on&td_CPLead_10=on&td_CPLead_25=on"
    "&td_CPLead_Average=on&td_CPLead_75=on&td_CPLead_90=on&td_MCID_Avg_CPC=on&td_CPClick_10=on"
    "&td_CPClick_25=on&td_CPClick_Average=on&td_CPClick_75=on&td_CPClick_90=on"
)

# You said the export element looks like: <a href="run.php?...&everything=1&excel_version=1">Export</a>
# We'll locate it by href substring or link text.
SEL_EXPORT_LINK_TEXT = "text=Export"
SEL_EXPORT_HREF_SUBSTR = "everything=1&excel_version=1"

# Heuristics for the Run button on legacy PHP reports
RUN_REPORT_SELECTORS = [
    'input[type="submit"][value*="Run"]',
    'input[type="button"][value*="Run"]',
    'button:has-text("Run Report")',
    'button:has-text("Run")',
    'a:has-text("Run Report")',
    'a:has-text("Run")',
    # Add exact matches for the legacy Run control
    'input[name="go"]',
    'input.Options-SearchButton[value="Run Report"]',
    'input[type="submit"][value="Run Report"]',
    'input[type="SUBMIT"][value="Run Report"]',   # case seen in your HTML
    'input[name="go"][type="submit"]',
    'input[name="go"][type="SUBMIT"]',
]

# Presence that indicates results table/controls are rendered
RESULTS_READY_MARKERS = [
    'a[href*="excel_version=1"]',           # Export link
    'a[href*="start_row=0"][href*="everything=1"]',  # Show All
    'div.Controls-Area',                     # legacy controls bar
    'table .Reports-LineTwoText',            # row cells
]

# ==== Report 6615 (Spend, Revenue & Performance Over Time) ====
R6615_URL = "https://corp.reachlocal.com/reports/run.php?db_node=maindb&db_type=unified&link_id=6615"

# Start/End selects (portal uses YYYY-MM-01 option values)
R6615_START_MONTH_SELECTORS = [
    'select#Start_Month', 'select[name="Start_Month"]',
    'select[name*="Start_Month"]', 'select[name*="StartMonth"]',
    'select[name*="Begin_Month"]'
]
R6615_END_MONTH_SELECTORS = [
    'select#End_Month', 'select[name="End_Month"]',
    'select[name*="End_Month"]', 'select[name*="EndMonth"]'
]

# Checkbox names we need ON for this report
# 'Client Name' is often 'td_advertiser_name' in this portal; include both to be safe.
R6615_CHECKBOX_PREFERRED = [
    "td_business_name",      # Business Name
    "td_advertiser_name",    # Client Name (likely)
    "td_client_name",        # Client Name (alternate)
    "td_campaign_name",      # Campaign Name
    # Add common variations:
    "business_name",
    "advertiser_name",
    "campaign_name"
]

# ==== Report 5904 (Down For Payment & Revenue In Jeopardy - DFP-RIJ) ====
DFP_RIJ_URL = "https://corp.reachlocal.com/reports/run.php?db_node=maindb&db_type=unified&link_id=5904&favorite_id=15252&favorite_title=Partner%20-%20Down%20For%20Payment%20%26%20Revenue%20In%20Jeopardy&reportmenu=1&td_Office=1&td_Service_Assignment=1&td_Agent=1&td_idbusiness=1&td_business_name=1&td_Advertiser_Name=1&td_CID=1&td_Product=1&td_Campaign_Name=1&td_Campaign_Budget=1&td_Percent_Spent=1&td_Alert_Type=1&td_Expected_End_Date=1&td_Cycle=1&td_Days_Without_Revenue=1&td_Evergreen=1&td_Dormant=1&td_annotation1=1&td_annotation=1&td_annotation2=1&td_Last_Updated=1&td_country=1&td_idbusiness_sm=1&td_Paused=1&td_In_Jeopardy=1&td_Incomplete=1&td_Down=1&td_employee_id=1&td_New_In_Jeopardy=1&td_Auto_Renew_Type=1&td_include_monthly_AR=1&td_High_Alert=1&kx=67|702"

# ==== Report 4501 (Agreed CPL Performance) ====
AGREED_CPL_URL = "https://corp.reachlocal.com/reports/run.php?db_node=maindb&db_type=unified&link_id=4501&favorite_id=17918&favorite_title=Agreed%20CPL%20Performance&reportmenu=1&channel=Partner&sort_column=tr_MCID%20tr_Current_Budget%20tr_Total_Paid_Budgets%20tr_Net_New_Budgets&td_idadvertiser=on&td_advertiser_name=on&td_first_aid=on&td_MCID=on&td_idcampaign=on&td_campaign_name=on&td_idinsertionorder=on&td_idbusiness=on&td_business_name=on&td_product=on&td_idoffer=on&td_finance_product=on&td_campaign_budget=on&td_Live_Campaign_Status=on&td_Campaign_StartDate=on&td_Campaign_EndDate=on&td_auto_renew_type=on&td_Parent_Cycle_Status=on&td_Prior_cycle_paid=on&td_Prior_cycle_started=on&td_Prior_cycle_ended=on&td_Days_Elapsed=on&td_cpl_change_rank=on&td_cpl_change=on&td_Trending=on&td_Utilization=on&td_utilization_status=on&td_campaign_spend_rate=on&td_CPL_Last15_Days=on&td_CPL_15_to_30Days=on&td_CPL_30_to_60Days=on&td_CPL_MCID=on&td_CPL_BSC_Median=on&td_CPL_Agreed=on&td_CPL_vs_Median=on&td_CPL_vs_Agreed=on&td_CID_Spend_to_Date=on&td_Pct_Spent=on&td_CID_Clicks=on&td_Cycle_Clicks=on&td_CID_Calls=on&td_CID_GoodCVTs=on&td_CID_Emails=on&td_CID_Cost_per_Click=on&td_CID_Cost_per_Call=on&td_CID_CTR=on&td_MCID_Position_PriorWeek=on&td_MCID_Position_QTD=on&td_MCID_Position_PriorQuarter=on&td_MCID_QualityScore_PriorWeek=on&td_MCID_QualityScore_QTD=on&td_MCID_QualityScore_PriorQuarter=on&td_CM_MCID_Google_spend_percent=on&td_CM_MCID_YahooBing_spend_percent=on&td_CM_MCID_Yelp_spend_percent=on&td_CM_MCID_Other_spend_percent=on&td_PM_MCID_Google_spend_percent=on&td_PM_MCID_YahooBing_spend_percent=on&td_PM_MCID_Yelp_spend_percent=on&td_PM_MCID_Other_spend_percent=on&td_bsc=on&td_bc=on&td_vertical=on&td_Account_Owner_Payroll_ID=on&td_Account_Owner_Name=on&td_CSM=on&td_CSM_Manager=on&td_Service_Analyst=on&td_DPM=on&td_DPM_Manager=on&td_office=on&td_area=on&td_channel=on&td_Tier=on&td_Keyword_Conflict_Report=on&td_annotation=on&td_Campaign_Performance_Rating=on&td_RedAlerts=on&td_Tickets=on&td_MCID_Cycles=on&td_IO_length=on&td_IO_cycle=on&td_method_of_payment=on&td_Predictive_Churn_Score=on&td_Specialist=on&td_Specialist_id=on&td_annotation2=on&td_POC_FirstName=on&td_POC_LastName=on&td_POC_Phone=on&td_POC_Email=on&td_Child_Status=on&td_Child_Detail=on&td_SFU_AccountID_Link=on&td_cpl_change_alert=on&kx=46|97"

# ==== Report 4969 (BSC Standards) ====
BSC_STANDARDS_URL = "https://corp.reachlocal.com/reports/run.php?db_node=maindb&db_type=unified&link_id=4969&multiplatform=1&favorite_id=16460&favorite_title=BSC%20Standards%20by%20Vertical&reportmenu=1&Channel=Partner&td_Country=on&td_Currency=on&td_Category=on&td_SubCategory=on&td_vertical=on&td_Sample_Size=on&td_Budget_Median=on&td_Cost_Per_Call_Median=on&td_Cost_Per_Lead_Median=on&td_Cost_Per_Click_Median=on&td_CTR_Median=on&td_Strength=on&kx=178|503"

# Generic "Run" and "Export" finders are reused
R_RUN_REPORT_SELECTORS = [
    'input[name="go"]',
    'input[type="submit"][value="Run Report"]',
    'input[type="SUBMIT"][value="Run Report"]',
    'input[type="submit"][value*="Run"]',
    'button:has-text("Run")',
    'a:has-text("Run")',
]
R_SHOW_ALL_SELECTOR = 'a[href*="start_row=0"][href*="everything=1"]'
R_EXPORT_SELECTORS = ['a[href*="excel_version=1"]', 'a:has-text("Export")']

# Channel selectors for "Partner" selection
CHANNEL_PARTNER_SELECTORS = [
    'select[name="channel"] option[value="Partner"]',
    'select[name="Channel"] option[value="Partner"]',
    'select[name="Region_Channel"] option[value="NA - Partner"]',  # For Budget Waterfall reports
    'input[name="channel"][value="Partner"]',
    'input[name="Channel"][value="Partner"]',
    'input[name="Region_Channel"][value="NA - Partner"]',
    'input[type="radio"][value="Partner"]',
    'input[type="radio"][value="NA - Partner"]',
    'input[type="checkbox"][value="Partner"]',
    'option[value="Partner"]',
    'option[value="NA - Partner"]',  # For Budget Waterfall by Channel
]

def select_partner_channel(page):
    """Helper function to select Partner channel on report pages."""

    # First try Budget Waterfall specific Region_Channel dropdown
    try:
        region_channel_dropdown = page.locator('select[name="Region_Channel"]').first
        if region_channel_dropdown.is_visible(timeout=2000):
            # Try to select "NA - Partner" option
            try:
                region_channel_dropdown.select_option(value="NA - Partner")
                print("[DEBUG] Selected 'NA - Partner' from Region_Channel dropdown")
                return True
            except:
                # Try selecting by label if value doesn't work
                try:
                    region_channel_dropdown.select_option(label="NA - Partner")
                    print("[DEBUG] Selected 'NA - Partner' from Region_Channel dropdown by label")
                    return True
                except:
                    print("[DEBUG] Could not select 'NA - Partner' from Region_Channel dropdown")
    except Exception as e:
        print(f"[DEBUG] Region_Channel dropdown not found: {e}")

    # Then try to find and select Partner from standard Channel dropdown
    try:
        # Look for Channel dropdown
        channel_dropdown = page.locator('select[name*="hannel"], select[name*="Channel"]').first
        if channel_dropdown.is_visible(timeout=2000):
            channel_dropdown.select_option(label="Partner")
            print("[DEBUG] Selected Partner from Channel dropdown")

            # Try to trigger form submission to apply the filter
            try:
                # Look for a submit/apply button
                submit_btn = page.locator('input[type="submit"], button[type="submit"], input[value*="Apply"], input[value*="Go"], input[name="go"]').first
                if submit_btn.is_visible(timeout=1000):
                    print("[DEBUG] Clicking submit to apply Partner filter...")
                    submit_btn.click()
                    page.wait_for_load_state("networkidle", timeout=10000)
                    print("[DEBUG] Form submitted with Partner filter")
                else:
                    # Try triggering change event
                    channel_dropdown.dispatch_event("change")
                    page.wait_for_timeout(2000)
                    print("[DEBUG] Triggered change event on Channel dropdown")
            except Exception as e:
                print(f"[DEBUG] Could not submit form after Partner selection: {e}")

            return True
    except Exception as e:
        print(f"[DEBUG] Channel dropdown not found: {e}")

    # Try radio buttons for "NA - Partner" first, then "Partner"
    for radio_value in ["NA - Partner", "Partner"]:
        try:
            partner_radio = page.locator(f'input[type="radio"][value="{radio_value}"]').first
            if partner_radio.is_visible(timeout=2000):
                partner_radio.click()
                print(f"[DEBUG] Selected {radio_value} radio button")

                # Try to trigger form submission for radio button too
                try:
                    submit_btn = page.locator('input[type="submit"], button[type="submit"], input[value*="Apply"], input[name="go"]').first
                    if submit_btn.is_visible(timeout=1000):
                        submit_btn.click()
                        page.wait_for_load_state("networkidle", timeout=10000)
                        print(f"[DEBUG] Form submitted with {radio_value} radio selection")
                except Exception as e:
                    print(f"[DEBUG] Could not submit form after {radio_value} radio selection: {e}")

                return True
        except Exception as e:
            print(f"[DEBUG] {radio_value} radio button not found: {e}")

    # Try the original selectors
    for selector in CHANNEL_PARTNER_SELECTORS:
        try:
            if 'option' in selector:
                # For dropdown options, first select the parent dropdown then the option
                parent_select = selector.split(' option')[0]
                page.locator(parent_select).click(timeout=2000)
                page.locator(selector).click(timeout=1000)
                print(f"[DEBUG] Selected Partner channel using: {selector}")
                return True
            else:
                # For other inputs, check or click
                locator = page.locator(selector)
                if locator.is_visible(timeout=2000):
                    locator.check() if 'checkbox' in selector else locator.click()
                    print(f"[DEBUG] Selected Partner channel using: {selector}")
                    return True
        except Exception as e:
            print(f"[DEBUG] Selector {selector} failed: {e}")
            continue

    print("[DEBUG] No Partner channel selector found - may not be needed for this report")
    return False