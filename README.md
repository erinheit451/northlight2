# Unified Northlight Platform

> **Integrated Data Pipeline and Business Intelligence Platform**
> Combining Heartbeat's ETL capabilities with Northlight's benchmarking and analytics

## 🎯 Overview

The Unified Northlight Platform consolidates two previously separate systems:
- **Heartbeat**: Sophisticated ETL pipeline for extracting data from corporate portals, Salesforce, and Ultimate DMS
- **Northlight**: Benchmarking and campaign performance analysis web application

This unified platform provides a single source of truth for all business data with real-time analytics, automated reporting, and comprehensive data management.

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │    │  ETL Pipeline    │    │  PostgreSQL DB  │
│                 │───▶│                  │───▶│                 │
│ • Corp Portal   │    │ • Extract        │    │ • Campaign Data │
│ • Salesforce    │    │ • Transform      │    │ • Benchmarks    │
│ • Ultimate DMS  │    │ • Load           │    │ • Analytics     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Frontend UI   │    │   FastAPI App    │    │  Business Logic │
│                 │◀───│                  │◀───│                 │
│ • Dashboards    │    │ • REST APIs      │    │ • Benchmarking  │
│ • Reports       │    │ • Authentication │    │ • Analytics     │
│ • Analytics     │    │ • WebSockets     │    │ • Reporting     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- PostgreSQL 15+
- Redis 7+ (for caching)
- Docker & Docker Compose (recommended)

### Installation

1. **Clone and Setup**
   ```bash
   cd unified-northlight
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Database Setup**
   ```bash
   # Start PostgreSQL and Redis with Docker
   docker-compose up -d

   # Initialize database schema
   python scripts/init_database.py
   ```

3. **Configure Environment**
   ```bash
   # Copy and edit environment file
   cp .env.example .env
   # Edit .env with your credentials and settings
   ```

4. **Run Application**
   ```bash
   python main.py
   ```

   The application will be available at:
   - **API Documentation**: http://localhost:8000/docs
   - **Dashboard**: http://localhost:8000/dashboard
   - **Health Check**: http://localhost:8000/health

## 📁 Project Structure

```
unified-northlight/
├── 📄 main.py                     # FastAPI application entry point
├── 📄 requirements.txt            # Python dependencies
├── 📄 .env                        # Environment configuration
├── 📄 docker-compose.yml          # Container orchestration
│
├── 📁 api/                        # REST API endpoints
│   └── 📁 v1/                     # API version 1
│       ├── benchmarking.py        # Benchmarking endpoints
│       ├── etl_management.py      # ETL control endpoints
│       └── reporting.py           # Report generation endpoints
│
├── 📁 core/                       # Core platform modules
│   ├── config.py                  # Configuration management
│   ├── database.py                # Database connections
│   └── shared.py                  # Shared utilities
│
├── 📁 etl/                        # ETL pipeline (from Heartbeat)
│   ├── 📁 heartbeat/              # Preserved Heartbeat ETL modules
│   ├── 📁 extractors/             # Data extraction modules
│   ├── 📁 transformers/           # Data transformation modules
│   └── 📁 loaders/                # Data loading modules
│
├── 📁 frontend/                   # Web interface (from Northlight)
│   ├── index.html                 # Main dashboard
│   ├── static/                    # CSS, JS, images
│   └── components/                # UI components
│
├── 📁 database/                   # Database management
│   ├── 📁 init/                   # Schema initialization scripts
│   └── 📁 migrations/             # Database migrations
│
├── 📁 scripts/                    # Utility and deployment scripts
│   ├── init_database.py           # Database setup
│   ├── migrate_data.py            # Data migration utilities
│   └── deploy.py                  # Deployment automation
│
└── 📁 data/                       # Data storage
    ├── 📁 raw/                    # Raw extracted data
    ├── 📁 processed/              # Transformed data
    └── 📁 exports/                # Generated reports
```

## 🔧 Configuration

Key environment variables:

```bash
# Database
DATABASE_URL=postgresql://app_user:password@localhost:5432/unified_northlight

# Corporate Portal
CORP_PORTAL_USERNAME=your.email@company.com
CORP_PORTAL_PASSWORD=your_password

# Salesforce
SF_USERNAME=your.email@company.com
SF_PASSWORD=your_password

# Application
API_HOST=0.0.0.0
API_PORT=8000
DEBUG=true
```

## 📊 Features

### ETL Pipeline
- **Automated Data Extraction** from multiple sources
- **Intelligent Data Transformation** with validation
- **Real-time Loading** into PostgreSQL
- **Comprehensive Monitoring** and alerting
- **Error Recovery** and retry mechanisms

### Benchmarking & Analytics
- **Campaign Performance Analysis** with industry benchmarks
- **Interactive Dashboards** for KPI monitoring
- **Automated Report Generation** (PDF, PowerPoint, Excel)
- **Real-time Data Updates** and notifications
- **Advanced Filtering** and data exploration

### API & Integration
- **RESTful APIs** for all platform functions
- **WebSocket Support** for real-time updates
- **OpenAPI Documentation** with Swagger UI
- **Rate Limiting** and authentication
- **Webhook Support** for external integrations

## 🛠️ Development

### Running Tests
```bash
pytest tests/ -v --cov=.
```

### Code Quality
```bash
# Format code
black .
isort .

# Lint code
flake8 .
mypy .
```

### Database Migrations
```bash
# Create migration
alembic revision --autogenerate -m "Description"

# Apply migrations
alembic upgrade head
```

## 📈 Monitoring

- **Health Checks**: `/health` endpoint for application status
- **Metrics**: Prometheus metrics on `/metrics`
- **Logs**: Structured logging with rotation
- **Alerts**: Slack/email notifications for issues

## 🔒 Security

- **Environment-based Configuration** (no secrets in code)
- **Database Connection Pooling** with proper cleanup
- **CORS Protection** with configurable origins
- **Input Validation** and sanitization
- **Rate Limiting** on API endpoints

## 📝 Migration Guide

### From Legacy Systems

1. **Data Migration**: Run `python scripts/migrate_data.py` to transfer existing data
2. **Configuration**: Update legacy scripts to use new database connections
3. **Testing**: Validate all functionality with `python scripts/validate_migration.py`

### Breaking Changes

- Database changed from DuckDB to PostgreSQL
- API endpoints consolidated under `/api/v1/`
- Configuration format updated (see `.env.example`)

## 🤝 Contributing

1. Create feature branch: `git checkout -b feature/your-feature`
2. Make changes with tests
3. Ensure code quality: `make lint test`
4. Submit pull request

## 📋 Roadmap

- [ ] **Phase 1**: Core platform integration *(Current)*
- [ ] **Phase 2**: Advanced analytics and ML features
- [ ] **Phase 3**: Multi-tenant support
- [ ] **Phase 4**: Mobile application
- [ ] **Phase 5**: Third-party integrations

## 📞 Support

- **Documentation**: Available in `/docs/`
- **Issues**: GitHub Issues for bug reports
- **Questions**: Contact the development team

---

**Built with ❤️ by the Unified Northlight Team**