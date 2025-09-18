# Unified Northlight Multi-Report Extractor

This directory contains a complete migration of the Heartbeat ETL extraction system, bringing all 10 report downloaders into the unified-northlight project with sophisticated error handling and MFA support.

## 📋 Reports Included

### Corporate Portal (7 reports)
1. **Ultimate DMS Campaign Performance** - Core campaign data with budgets, CPL, utilization
2. **Budget Waterfall Client** - Budget allocation and utilization by client
3. **Spend Revenue Performance** - Historical spend and revenue trends
4. **DFP-RIJ** - Down For Payment & Revenue In Jeopardy risk alerts
5. **Agreed CPL Performance** - CPL agreement tracking and compliance
6. **BSC Standards** - Benchmark standards by vertical and category
7. **Budget Waterfall Channel** - Budget flow analysis by channel

### Salesforce (3 reports)
8. **Partner Pipeline** - Partner sales pipeline and opportunities
9. **Tim King Partner Pipeline** - Specialized partner pipeline view
10. **Partner Calls** - Partner call logs and activity tracking

## 🔧 Setup

### 1. Install Dependencies
```bash
pip install playwright python-dotenv requests
playwright install chromium
```

### 2. Configure Environment
```bash
# Copy template and edit with your credentials
copy .env.template .env
# Edit .env with your actual usernames/passwords
```

### 3. Test Setup
```bash
# Verify everything is configured correctly
python test_setup.py
```

### 4. Run Extractors
```bash
# Run all 10 extractors
run_extractors.bat
```

## 🚀 Key Features

### **MFA Lockout Protection**
- Detects Salesforce MFA lockout attempts
- Implements 20-minute cooldown period
- Prevents account lockouts from repeated failed attempts

### **Persistent Browser Sessions**
- Reuses authentication across reports
- Stores session state in `secrets/sf_auth.json`
- Reduces login prompts and MFA requirements

### **Comprehensive Error Handling**
- Individual extractor isolation via subprocess
- Detailed error logging to `logs/etl.log`
- Alert notifications to `alerts/alerts.jsonl`
- Optional Slack notifications

### **Corporate Environment Compatibility**
- Handles SSL certificate issues
- Works with corporate proxies
- Bypasses automation detection

## 📁 File Structure

```
unified-northlight/
├── extractors/
│   ├── corp_portal/           # Corporate portal extractors
│   │   ├── auth.py           # Portal authentication
│   │   ├── portal_selectors.py # Portal UI selectors
│   │   ├── ultimate_dms.py   # Ultimate DMS extractor
│   │   └── ...               # Other corp portal extractors
│   ├── salesforce/           # Salesforce extractors
│   │   ├── auth_enhanced.py  # SF auth with MFA handling
│   │   ├── selectors.py      # SF Lightning UI selectors
│   │   ├── partner_pipeline.py # Partner pipeline extractor
│   │   └── ...               # Other SF extractors
│   ├── monitor/              # Monitoring and logging
│   │   └── monitoring.py     # Error handling utilities
│   └── playwright_bootstrap.py # Browser setup
├── data/raw/                 # Downloaded report files
├── logs/                     # Execution logs
├── secrets/                  # Session storage
├── tmp/                      # Temporary files
├── run_all_extractors.py     # Main orchestrator
├── run_extractors.bat        # Windows batch runner
├── test_setup.py             # Setup verification
└── .env                      # Environment configuration
```

## 📊 Output

Downloaded files are saved to:
- `data/raw/ultimate_dms/ultimate_dms_YYYY-MM-DD.csv`
- `data/raw/budget_waterfall_client/YYYY-MM-DD.csv`
- `data/raw/sf_partner_pipeline/YYYY-MM-DD.csv`
- etc.

## 🔍 Monitoring

- **Logs**: `logs/etl.log` - Detailed execution logs
- **Alerts**: `alerts/alerts.jsonl` - Error and status notifications
- **Console**: Real-time progress and status updates

## ⚠️ MFA Handling

For Salesforce reports with MFA enabled:
1. First run will prompt for TOTP code from authenticator app
2. Session will be saved for future runs
3. If MFA fails too many times, 20-minute cooldown activates
4. All SF extractions will be skipped during cooldown

## 🛠️ Troubleshooting

### **"MFA lockout cooldown active"**
- Wait 20 minutes before retrying Salesforce reports
- Or delete `tmp/sf_mfa_lock.json` to reset (not recommended)

### **"Could not find login form"**
- Check VPN connection
- Verify credentials in `.env` file
- Try running with `BROWSER_HEADLESS=false` for debugging

### **"No accounts match filters"**
- Data downloaded successfully but app needs different data
- Check if you need Ultimate DMS data vs. historical performance data

## 🔄 Migration Notes

This system preserves all the sophisticated handling from the original Heartbeat ETL:
- ✅ MFA lockout detection and cooldown
- ✅ Corporate environment compatibility
- ✅ Robust error handling and retry logic
- ✅ Session persistence across reports
- ✅ Individual extractor isolation
- ✅ Comprehensive logging and monitoring

The original `run_all_multi_report.py` functionality has been fully migrated and enhanced.