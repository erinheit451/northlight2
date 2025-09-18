-- ===========================================================================
-- UNIFIED NORTHLIGHT DATABASE INITIALIZATION
-- This script initializes the complete database schema for the unified platform
-- ===========================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create main application user
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'app_user') THEN
        CREATE USER app_user WITH PASSWORD 'app_secure_2024';
    END IF;
END
$$;

-- Create read-only user for reporting
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'readonly_user') THEN
        CREATE USER readonly_user WITH PASSWORD 'readonly_2024';
    END IF;
END
$$;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS heartbeat_core;
CREATE SCHEMA IF NOT EXISTS heartbeat_performance;
CREATE SCHEMA IF NOT EXISTS heartbeat_salesforce;
CREATE SCHEMA IF NOT EXISTS heartbeat_reporting;
CREATE SCHEMA IF NOT EXISTS heartbeat_standards;
CREATE SCHEMA IF NOT EXISTS northlight_benchmarks;
CREATE SCHEMA IF NOT EXISTS unified_analytics;

-- Grant schema permissions
GRANT USAGE ON SCHEMA heartbeat_core TO app_user;
GRANT USAGE ON SCHEMA heartbeat_performance TO app_user;
GRANT USAGE ON SCHEMA heartbeat_salesforce TO app_user;
GRANT USAGE ON SCHEMA heartbeat_reporting TO app_user;
GRANT USAGE ON SCHEMA heartbeat_standards TO app_user;
GRANT USAGE ON SCHEMA northlight_benchmarks TO app_user;
GRANT USAGE ON SCHEMA unified_analytics TO app_user;

-- Grant read-only access
GRANT USAGE ON SCHEMA heartbeat_core TO readonly_user;
GRANT USAGE ON SCHEMA heartbeat_performance TO readonly_user;
GRANT USAGE ON SCHEMA heartbeat_salesforce TO readonly_user;
GRANT USAGE ON SCHEMA heartbeat_reporting TO readonly_user;
GRANT USAGE ON SCHEMA heartbeat_standards TO readonly_user;
GRANT USAGE ON SCHEMA northlight_benchmarks TO readonly_user;
GRANT USAGE ON SCHEMA unified_analytics TO readonly_user;

-- Set default schema search path
ALTER USER app_user SET search_path TO heartbeat_core, heartbeat_performance, heartbeat_salesforce, heartbeat_reporting, heartbeat_standards, northlight_benchmarks, unified_analytics, public;
ALTER USER readonly_user SET search_path TO heartbeat_core, heartbeat_performance, heartbeat_salesforce, heartbeat_reporting, heartbeat_standards, northlight_benchmarks, unified_analytics, public;