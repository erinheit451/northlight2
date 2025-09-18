-- ===========================================================================
-- NORTHLIGHT BENCHMARK TABLES
-- Schema for migrating Northlight's JSON-based benchmark data to Postgres
-- ===========================================================================

-- Benchmark Categories and Metadata
CREATE TABLE northlight_benchmarks.benchmark_categories (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    category VARCHAR(255) NOT NULL,
    subcategory VARCHAR(255) NOT NULL,
    display_name TEXT,
    description TEXT,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(category, subcategory)
);

-- Benchmark Snapshots (versioned benchmark data)
CREATE TABLE northlight_benchmarks.benchmark_snapshots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    version VARCHAR(100) NOT NULL UNIQUE,
    snapshot_date DATE NOT NULL,
    description TEXT,
    records_count INTEGER,
    file_checksum VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Core Benchmark Data
CREATE TABLE northlight_benchmarks.benchmark_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    snapshot_id UUID NOT NULL REFERENCES northlight_benchmarks.benchmark_snapshots(id),
    category_id UUID NOT NULL REFERENCES northlight_benchmarks.benchmark_categories(id),
    key VARCHAR(255) NOT NULL,

    -- CPL (Cost Per Lead) metrics
    cpl_median DECIMAL(10,2),
    cpl_top10 DECIMAL(10,2),
    cpl_top25 DECIMAL(10,2),
    cpl_avg DECIMAL(10,2),
    cpl_bot25 DECIMAL(10,2),
    cpl_bot10 DECIMAL(10,2),

    -- CPC (Cost Per Click) metrics
    cpc_median DECIMAL(10,2),
    cpc_top10 DECIMAL(10,2),
    cpc_top25 DECIMAL(10,2),
    cpc_avg DECIMAL(10,2),
    cpc_bot25 DECIMAL(10,2),
    cpc_bot10 DECIMAL(10,2),

    -- CTR (Click Through Rate) metrics
    ctr_median DECIMAL(8,6),
    ctr_top10 DECIMAL(8,6),
    ctr_top25 DECIMAL(8,6),
    ctr_avg DECIMAL(8,6),
    ctr_bot25 DECIMAL(8,6),
    ctr_bot10 DECIMAL(8,6),

    -- Budget metrics
    budget_median DECIMAL(12,2),
    budget_p10_bottom DECIMAL(12,2),
    budget_p25_bottom DECIMAL(12,2),
    budget_avg DECIMAL(12,2),
    budget_p25_top DECIMAL(12,2),
    budget_p10_top DECIMAL(12,2),

    -- Metadata
    sample_size INTEGER,
    confidence_level DECIMAL(4,2),
    data_quality_score DECIMAL(4,2),

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(snapshot_id, key)
);

-- Diagnosis History (store user diagnosis requests)
CREATE TABLE northlight_benchmarks.diagnosis_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id VARCHAR(255),
    website VARCHAR(255),
    category VARCHAR(255) NOT NULL,
    subcategory VARCHAR(255) NOT NULL,
    budget DECIMAL(12,2) NOT NULL,
    clicks INTEGER NOT NULL,
    leads INTEGER NOT NULL,
    goal_cpl DECIMAL(10,2),
    impressions INTEGER,
    dash_enabled BOOLEAN,

    -- Calculated metrics
    derived_cpc DECIMAL(10,2),
    derived_cpl DECIMAL(10,2),
    derived_cr DECIMAL(8,6),
    derived_ctr DECIMAL(8,6),

    -- Diagnosis results
    primary_recommendation VARCHAR(100),
    goal_status VARCHAR(50),
    market_band VARCHAR(50),
    performance_tier VARCHAR(50),

    -- Request metadata
    user_agent TEXT,
    ip_address INET,
    requested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Export History (track PowerPoint and other exports)
CREATE TABLE northlight_benchmarks.export_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    diagnosis_id UUID REFERENCES northlight_benchmarks.diagnosis_history(id),
    export_type VARCHAR(50) NOT NULL, -- 'pptx', 'excel', 'pdf'
    filename VARCHAR(255),
    file_size INTEGER,
    download_count INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX idx_benchmark_data_snapshot_category ON northlight_benchmarks.benchmark_data(snapshot_id, category_id);
CREATE INDEX idx_benchmark_data_key ON northlight_benchmarks.benchmark_data(key);
CREATE INDEX idx_benchmark_categories_lookup ON northlight_benchmarks.benchmark_categories(category, subcategory);
CREATE INDEX idx_diagnosis_history_category ON northlight_benchmarks.diagnosis_history(category, subcategory);
CREATE INDEX idx_diagnosis_history_requested_at ON northlight_benchmarks.diagnosis_history(requested_at);

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA northlight_benchmarks TO app_user;
GRANT SELECT ON ALL TABLES IN SCHEMA northlight_benchmarks TO readonly_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA northlight_benchmarks TO app_user;