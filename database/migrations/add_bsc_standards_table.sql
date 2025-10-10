-- Migration: Add BSC Standards table
-- Date: 2025-10-03
-- Description: Creates heartbeat_core.bsc_standards table for benchmark standards by category reference data

-- BSC Standards (Benchmark Standards by Category) - Reference Data
CREATE TABLE IF NOT EXISTS heartbeat_core.bsc_standards (
    id SERIAL PRIMARY KEY,
    country TEXT,
    currency TEXT,
    category TEXT,
    sub_category TEXT,
    vertical TEXT,
    budget_median NUMERIC(12,2),
    cost_per_call_median NUMERIC(12,2),
    cost_per_lead_median NUMERIC(12,2),
    cost_per_click_median NUMERIC(12,2),
    ctr_median NUMERIC(8,6),
    strength TEXT,
    snapshot_date DATE,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_file TEXT,
    UNIQUE(country, category, sub_category, vertical, snapshot_date)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_bsc_standards_snapshot_date ON heartbeat_core.bsc_standards(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_bsc_standards_category ON heartbeat_core.bsc_standards(category, sub_category);
CREATE INDEX IF NOT EXISTS idx_bsc_standards_vertical ON heartbeat_core.bsc_standards(vertical);
CREATE INDEX IF NOT EXISTS idx_bsc_standards_country ON heartbeat_core.bsc_standards(country);

-- Permissions are granted via the existing GRANT ALL TABLES statement in 06_etl_tables.sql
