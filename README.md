# Unified Northlight Platform

**Integrated data pipeline and benchmarking platform for campaign risk assessment and performance analytics**

[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green.svg)](https://fastapi.tiangolo.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-blue.svg)](https://www.postgresql.org/)

---

## Overview

Unified Northlight is a comprehensive platform that combines:

- **Campaign Risk Assessment** - Churn probability calculation and FLARE scoring for priority ranking
- **Benchmarking System** - Industry benchmark analysis for campaign performance
- **ETL Pipeline** - Automated data extraction from multiple sources (Corporate Portal, Salesforce)
- **Real-time Monitoring** - Pacing, utilization, and performance tracking
- **Interactive Dashboards** - Book management frontend with risk visualization

### Key Features

- 🎯 **Account Priority Queue** - Automatically ranks accounts by retention risk and business value
- 📊 **Benchmark Tool** - Compare campaign performance against industry standards
- 🔄 **Automated ETL** - Daily extraction and loading of 10+ data sources
- 📈 **Risk Analytics** - Advanced diagnostics with headline generation and waterfall visualization
- 🚨 **Alert System** - Proactive monitoring with JSONL-based alert tracking
- 🔍 **Trajectory Analysis** - Performance trend analysis and forecasting

---

## Table of Contents

- [Quick Start](#quick-start)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Application](#running-the-application)
- [ETL Pipeline](#etl-pipeline)
- [API Documentation](#api-documentation)
- [Project Structure](#project-structure)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

---

## Quick Start

```bash
# Clone the repository
git clone https://github.com/erinheit451/northlight2.git
cd northlight2

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your credentials

# Set up PostgreSQL database
createdb unified_northlight

# Run database migrations
python -m database.init.run_migrations

# Start the application
python main.py

# Visit http://localhost:4000
```

---

## Prerequisites

### Required Software

- **Python 3.8+** - [Download](https://www.python.org/downloads/)
- **PostgreSQL 13+** - [Download](https://www.postgresql.org/download/)
- **Node.js 16+** (optional, for frontend development) - [Download](https://nodejs.org/)

### Required Accounts & Credentials

- **Corporate Portal** - ReachLocal corporate portal access
- **Salesforce** - Salesforce account with MFA
- **PostgreSQL** - Database credentials

---

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/erinheit451/northlight2.git
cd northlight2
```

### 2. Set Up Python Environment

**Option A: Using venv (recommended)**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

**Option B: Using conda**
```bash
conda create -n northlight python=3.11
conda activate northlight
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

**Core dependencies include:**
- FastAPI + Uvicorn (API framework)
- SQLAlchemy + asyncpg (Database ORM)
- Playwright (Browser automation for extractors)
- Pandas (Data processing)
- Python-dotenv (Environment management)

**Install Playwright browsers:**
```bash
playwright install chromium
```

### 4. Set Up PostgreSQL Database

**Create database:**
```bash
# Using psql
psql -U postgres
CREATE DATABASE unified_northlight;
CREATE USER northlight_user WITH PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE unified_northlight TO northlight_user;
\q
```

**Run migrations:**
```bash
python -m database.init.run_migrations
```

This creates the required schemas:
- `heartbeat_core` - Core campaign and performance data
- `heartbeat_performance` - Performance metrics
- `northlight_benchmarks` - Benchmark data and standards
- `unified_analytics` - Materialized views for analytics
- `book` - Book management data

---

## Configuration

### Environment Variables

Create a `.env` file in the root directory:

```bash
# Database Configuration
DATABASE_URL=postgresql+asyncpg://northlight_user:your_password@localhost:5432/unified_northlight
DATABASE_POOL_SIZE=20
DATABASE_MAX_OVERFLOW=30

# Corporate Portal Authentication
CORP_PORTAL_USERNAME=your.email@company.com
CORP_PORTAL_PASSWORD=your_password
CORP_PORTAL_URL=https://corp.reachlocal.com/common/logon.php

# Salesforce Authentication
SF_USERNAME=your.email@company.com
SF_PASSWORD=your_password
# SF_TOTP_CODE will be prompted interactively

# Application Configuration
API_HOST=0.0.0.0
API_PORT=4000
LOG_LEVEL=INFO
```

**⚠️ Security Note:** Never commit `.env` to git. It's already in `.gitignore`.

### Configuration Files

- **`core/config.py`** - Application settings and environment loading
- **`database/init/*.sql`** - Database schema definitions
- **`.gitignore`** - Excludes sensitive data, logs, and cache files

---

## Running the Application

### Start the Web Application

**Development mode:**
```bash
python main.py
```

**Production mode (with Gunicorn):**
```bash
gunicorn main:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:4000
```

The application will be available at:
- **Dashboard:** http://localhost:4000/
- **Book Management:** http://localhost:4000/book
- **API Docs:** http://localhost:4000/docs
- **Health Check:** http://localhost:4000/health

### Windows Launchers

For convenience, use the provided batch files:

**Start API Server:**
```bash
# (Create a start_api.bat file or run python main.py directly)
python main.py
```

---

## ETL Pipeline

The ETL pipeline extracts data from multiple sources, loads it into PostgreSQL, and runs transformations.

### Data Sources (10 Extractors)

**Corporate Portal (7 sources):**
1. Ultimate DMS Campaign Performance
2. Budget Waterfall Client
3. Budget Waterfall Channel
4. Spend Revenue Performance
5. DFP-RIJ (Down For Payment & Revenue In Jeopardy)
6. Agreed CPL Performance
7. BSC Standards

**Salesforce (3 sources):**
1. Partner Pipeline
2. Tim King Partner Pipeline
3. Partner Calls

### Running the ETL Pipeline

**Complete ETL (Extract + Load + Transform):**
```bash
# Windows
run_complete_etl.bat

# Linux/Mac
python scripts/runners/run_etl_pipeline.py
```

**Extract Only:**
```bash
# Windows
run_extractors.bat

# Linux/Mac
python scripts/runners/run_all_extractors.py
```

**Load Only (skip extraction):**
```bash
python scripts/runners/run_etl_pipeline.py --skip-extraction
```

**Book Sync Only:**
```bash
# Windows
sync_book.bat

# Linux/Mac
python scripts/run_book_sync.py
```

### ETL Pipeline Phases

1. **Database Setup** - Ensure schemas and tables exist
2. **Data Extraction** - Download CSV files from sources
3. **Data Loading** - Load CSVs into PostgreSQL
4. **Analytics Refresh** - Update materialized views
5. **Book Transformation** - Transform data for book dashboard
6. **Book Schema Sync** - Sync performance data to book tables

### Scheduling ETL (Windows)

**Set up Windows Task Scheduler:**
```bash
# Run as Administrator
setup_scheduled_task.bat
```

This creates a daily task that runs at 6:00 AM.

**For Linux/Mac, use cron:**
```bash
# Edit crontab
crontab -e

# Add daily ETL at 6 AM
0 6 * * * cd /path/to/northlight2 && /path/to/venv/bin/python scripts/runners/run_etl_pipeline.py
```

---

## API Documentation

### Interactive API Docs

Visit http://localhost:4000/docs for interactive Swagger UI documentation.

### Key Endpoints

**Benchmarking:**
- `GET /api/v1/benchmarks/meta` - Get available benchmark categories
- `POST /api/v1/benchmarks/diagnose` - Diagnose campaign performance
- `POST /api/v1/benchmarks/export/pptx` - Export diagnosis as PowerPoint

**Book Management:**
- `GET /api/v1/book/summary` - Get account summary statistics
- `GET /api/v1/book/accounts` - List all accounts with filters
- `GET /api/v1/book/metadata` - Get data freshness metadata
- `GET /api/v1/book/partners/{partner}/advertisers` - Get advertisers for partner

**Trajectory Analysis:**
- `GET /api/v1/trajectory/bulk` - Get trajectory data for multiple campaigns

**Health:**
- `GET /health` - Application health check

### Authentication

Currently, the API does not require authentication. For production deployment, consider adding:
- API keys
- OAuth2
- JWT tokens

---

## Project Structure

```
unified-northlight/
├── main.py                      # FastAPI application entry point
├── requirements.txt             # Python dependencies
├── README.md                    # This file
├── .env                         # Environment variables (create from .env.example)
├── .gitignore                   # Git ignore rules
│
├── api/                         # API endpoints
│   └── v1/
│       ├── book.py             # Book/account management API
│       ├── benchmarking.py     # Benchmarking API
│       └── trajectory.py       # Trajectory analysis API
│
├── core/                        # Core application modules
│   ├── config.py               # Configuration management
│   ├── database.py             # Database connection and session management
│   ├── shared.py               # Shared utilities (logging, etc.)
│   └── models/
│       └── book.py             # SQLAlchemy models
│
├── book_risk_model/             # Risk scoring and assessment
│   ├── core/
│   │   ├── churn.py            # Churn probability calculation
│   │   └── flare.py            # FLARE scoring algorithm
│   ├── presentation/
│   │   └── diagnostics.py      # Risk diagnostics and headline generation
│   ├── pg_ingest.py            # PostgreSQL data ingestion
│   └── advertiser_lifetime.py  # Advertiser lifetime value calculation
│
├── etl/                         # ETL pipeline
│   └── unified/
│       └── data_loader.py      # Unified data loader for all sources
│
├── extractors/                  # Data extraction modules
│   ├── corp_portal/            # Corporate portal extractors
│   │   ├── ultimate_dms.py
│   │   ├── spend_revenue_performance.py
│   │   ├── bsc_standards.py
│   │   └── ... (7 extractors total)
│   ├── salesforce/             # Salesforce extractors
│   │   ├── partner_pipeline.py
│   │   ├── partner_calls.py
│   │   └── auth_enhanced.py    # Salesforce authentication with MFA
│   └── monitor/
│       └── monitoring.py       # Extraction monitoring and alerts
│
├── frontend/                    # Frontend assets
│   ├── index.html              # Main benchmarking tool
│   ├── unified-script.js       # Benchmarking JavaScript
│   ├── styles.css              # Global styles
│   └── book/
│       ├── index.html          # Account priority dashboard
│       ├── script.js           # Book dashboard logic
│       ├── trajectory.js       # Trajectory visualization
│       └── risk_waterfall.js   # Risk waterfall charts
│
├── scripts/                     # Utility scripts
│   ├── runners/                # ETL orchestrators
│   │   ├── run_all_extractors.py
│   │   ├── run_etl_pipeline.py
│   │   └── run_complete_etl_sync.py
│   ├── check_book_data_freshness.py
│   ├── check_database_status.py
│   ├── sync_book_performance_data.py
│   └── transform_book_data.py
│
├── database/                    # Database schemas and migrations
│   ├── init/
│   │   ├── 01_heartbeat_core.sql
│   │   ├── 02_heartbeat_performance.sql
│   │   ├── 03_benchmarks.sql
│   │   ├── 04_book_schema.sql
│   │   ├── 05_analytics.sql
│   │   └── 06_etl_tables.sql
│   └── migrations/
│
├── data/                        # Data storage (not in git)
│   └── raw/                    # Raw CSV files from extractors
│       ├── ultimate_dms/
│       ├── spend_revenue_performance/
│       ├── bsc_standards/
│       └── ... (10 source folders)
│
├── logs/                        # Application logs (not in git)
│   ├── api.log
│   ├── etl.log
│   └── unified.log
│
└── alerts/                      # Alert system (not in git)
    └── alerts.jsonl
```

### Key Files Explained

**`main.py`** - FastAPI application entry point. Configures routes, CORS, static files, and starts the server.

**`core/config.py`** - Centralized configuration using Pydantic settings. Loads from environment variables.

**`core/database.py`** - Database connection pooling, session management, and async context managers.

**`book_risk_model/`** - Risk scoring algorithms including churn probability, FLARE scores, and diagnostic generation.

**`extractors/`** - Browser automation scripts using Playwright to download data from Corporate Portal and Salesforce.

**`etl/unified/data_loader.py`** - Unified data loader that processes CSV files and loads them into PostgreSQL.

---

## Development

### Setting Up Development Environment

```bash
# Install dev dependencies
pip install -r requirements-dev.txt  # (if exists)

# Or install common dev tools
pip install pytest black flake8 mypy
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=. --cov-report=html

# Run specific test file
pytest tests/test_api.py
```

### Code Formatting

```bash
# Format code with Black
black .

# Check linting with Flake8
flake8 .

# Type checking with mypy
mypy .
```

### Database Migrations

When making schema changes:

1. Create new SQL file in `database/init/` or `database/migrations/`
2. Run migration: `python -m database.migrations.your_migration`
3. Update models in `core/models/` if needed

### Adding New Extractors

1. Create extractor in `extractors/corp_portal/` or `extractors/salesforce/`
2. Add to `run_all_extractors.py` extractor list
3. Create corresponding loader in `etl/unified/data_loader.py`
4. Update `DATABASE_INIT.md` with new table schema

### Frontend Development

The frontend uses vanilla JavaScript (no build step required).

**Files:**
- `frontend/index.html` - Benchmarking tool
- `frontend/book/index.html` - Book dashboard
- `frontend/unified-script.js` - Benchmarking logic
- `frontend/book/script.js` - Book dashboard logic

**Hot reload:** FastAPI serves static files, so just refresh browser after changes.

---

## Troubleshooting

### Common Issues

**1. Database connection errors**
```
ERROR: could not connect to database
```
**Solution:** Check PostgreSQL is running and credentials in `.env` are correct.
```bash
# Test connection
psql -U northlight_user -d unified_northlight -h localhost
```

**2. Import errors**
```
ModuleNotFoundError: No module named 'playwright'
```
**Solution:** Install dependencies and Playwright browsers.
```bash
pip install -r requirements.txt
playwright install chromium
```

**3. Extractor authentication failures**
```
ERROR: MFA verification required
```
**Solution:** Run extractors interactively to enter MFA code. The code is cached for 24 hours.
```bash
python scripts/runners/run_all_extractors.py
# Enter MFA code when prompted
```

**4. Port already in use**
```
ERROR: [Errno 48] Address already in use
```
**Solution:** Kill process on port 4000 or change port in `.env`.
```bash
# Find and kill process
lsof -ti:4000 | xargs kill -9  # Mac/Linux
netstat -ano | findstr :4000   # Windows (then taskkill /PID <pid> /F)
```

**5. Data not showing in dashboard**
```
No accounts found / Empty dashboard
```
**Solution:** Run ETL pipeline to load data.
```bash
python scripts/runners/run_etl_pipeline.py
```

### Logs

Check logs for detailed error information:

- **API logs:** `logs/api.log`
- **ETL logs:** `logs/etl.log`
- **Unified logs:** `logs/unified.log`
- **Alerts:** `alerts/alerts.jsonl`

### Debug Mode

Enable debug logging in `.env`:
```bash
LOG_LEVEL=DEBUG
```

---

## Contributing

Contributions are welcome! Please follow these guidelines:

1. **Fork the repository**
2. **Create a feature branch** (`git checkout -b feature/amazing-feature`)
3. **Make your changes**
4. **Run tests** (`pytest`)
5. **Format code** (`black .`)
6. **Commit changes** (`git commit -m 'Add amazing feature'`)
7. **Push to branch** (`git push origin feature/amazing-feature`)
8. **Open a Pull Request**

### Code Style

- **Python:** PEP 8 (use Black formatter)
- **JavaScript:** Standard JS style
- **SQL:** Uppercase keywords, snake_case identifiers
- **Comments:** Docstrings for all functions/classes

---

## License

This project is proprietary software. All rights reserved.

---

## Support

For questions or issues:

1. Check the [Troubleshooting](#troubleshooting) section
2. Review logs in `logs/` directory
3. Open an issue on GitHub
4. Contact the development team

---

## Architecture Notes

### Data Flow

```
Corporate Portal / Salesforce
          ↓
    Extractors (Playwright)
          ↓
    data/raw/*.csv
          ↓
    ETL Data Loader
          ↓
    PostgreSQL Database
          ↓
    FastAPI Endpoints
          ↓
    Frontend Dashboard
```

### Technology Stack

- **Backend:** FastAPI + Python 3.11
- **Database:** PostgreSQL 15 with asyncpg
- **ORM:** SQLAlchemy (async)
- **Extractors:** Playwright (browser automation)
- **Frontend:** Vanilla JavaScript + HTML5 + CSS3
- **Data Processing:** Pandas + NumPy
- **Logging:** Python logging module
- **Alerts:** JSONL format

### Security Considerations

- **Credentials:** Stored in `.env` (never committed to git)
- **Database:** Use strong passwords and connection pooling
- **API:** Consider adding authentication for production
- **MFA:** Salesforce extractors support TOTP-based MFA
- **Logs:** Contain no sensitive information (credentials redacted)

---

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and release notes.

---

**Built with ❤️ for campaign performance optimization**
