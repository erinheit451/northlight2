-- ===========================================================================
-- BOOK SYSTEM TABLES
-- Schema for campaign health, risk assessment, and partner management
-- ===========================================================================

-- Create book schema
CREATE SCHEMA IF NOT EXISTS book;

-- Campaigns/Accounts core data
CREATE TABLE book.campaigns (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    campaign_id VARCHAR(255) UNIQUE NOT NULL,
    maid VARCHAR(255),
    advertiser_name VARCHAR(255),
    partner_name VARCHAR(255),
    bid_name VARCHAR(255),
    campaign_name VARCHAR(255),

    -- Account Management
    am VARCHAR(255),
    optimizer VARCHAR(255),
    gm VARCHAR(255),
    business_category VARCHAR(255),

    -- Financial Data
    campaign_budget DECIMAL(12,2),
    amount_spent DECIMAL(12,2),

    -- Operational Metrics
    io_cycle INTEGER,
    avg_cycle_length DECIMAL(8,2),
    days_elapsed INTEGER,
    days_active INTEGER,
    utilization DECIMAL(5,2),

    -- Lead Metrics
    running_cid_leads INTEGER,
    running_cid_cpl DECIMAL(10,2),
    cpl_goal DECIMAL(10,2),
    bsc_cpl_avg DECIMAL(10,2),
    effective_cpl_goal DECIMAL(10,2),
    expected_leads_monthly INTEGER,
    expected_leads_to_date INTEGER,
    expected_leads_to_date_spend DECIMAL(12,2),

    -- Runtime Fields
    true_days_running INTEGER,
    true_months_running DECIMAL(4,2),
    cycle_label VARCHAR(100),

    -- Risk Scores
    age_risk DECIMAL(5,2),
    lead_risk DECIMAL(5,2),
    cpl_risk DECIMAL(5,2),
    util_risk DECIMAL(5,2),
    structure_risk DECIMAL(5,2),
    total_risk_score DECIMAL(5,2),
    value_score DECIMAL(5,2),
    final_priority_score DECIMAL(5,2),

    -- Priority Classification
    priority_index DECIMAL(8,4),
    priority_tier VARCHAR(50),
    primary_issue VARCHAR(255),

    -- Churn Risk Data
    churn_prob_90d DECIMAL(5,4),
    churn_risk_band VARCHAR(50),
    revenue_at_risk DECIMAL(12,2),
    risk_drivers_json JSONB,

    -- FLARE Scoring
    flare_score DECIMAL(8,4),
    flare_band VARCHAR(50),
    flare_breakdown_json JSONB,
    flare_score_raw DECIMAL(8,4),

    -- Diagnosis
    headline_diagnosis TEXT,
    headline_severity VARCHAR(50),
    diagnosis_pills JSONB,

    -- Account Structure
    campaign_count INTEGER,
    true_product_count INTEGER,
    is_safe BOOLEAN,

    -- Goal Advice
    goal_advice_json JSONB,

    -- UI State
    status VARCHAR(50) DEFAULT 'new',

    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Partners summary data for growth dashboard
CREATE TABLE book.partners (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    partner_name VARCHAR(255) UNIQUE NOT NULL,
    playbook VARCHAR(100) DEFAULT 'seo_dash',

    -- Metrics
    total_budget DECIMAL(12,2),
    single_product_count INTEGER DEFAULT 0,
    two_product_count INTEGER DEFAULT 0,
    three_plus_product_count INTEGER DEFAULT 0,
    cross_sell_ready_count INTEGER DEFAULT 0,
    upsell_ready_count INTEGER DEFAULT 0,

    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Partner opportunities detail data
CREATE TABLE book.partner_opportunities (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    partner_id UUID NOT NULL REFERENCES book.partners(id),
    partner_name VARCHAR(255) NOT NULL,
    playbook VARCHAR(100) DEFAULT 'seo_dash',

    -- Opportunity Groups (stored as JSONB for flexibility)
    single_ready JSONB, -- Single product advertisers ready for cross-sell
    two_ready JSONB,    -- Two product advertisers
    three_plus_ready JSONB, -- Three+ product advertisers
    scale_ready JSONB,  -- Campaigns ready for budget increase
    too_low JSONB,      -- Budget inadequate campaigns

    -- Playbook Configuration
    playbook_config JSONB,

    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(partner_name, playbook)
);

-- Data freshness tracking
CREATE TABLE book.data_snapshots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    snapshot_date DATE NOT NULL,
    file_name VARCHAR(255),
    file_size_bytes BIGINT,
    record_count INTEGER,
    last_modified TIMESTAMP WITH TIME ZONE,
    is_current BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX idx_campaigns_campaign_id ON book.campaigns(campaign_id);
CREATE INDEX idx_campaigns_partner_name ON book.campaigns(partner_name);
CREATE INDEX idx_campaigns_advertiser_name ON book.campaigns(advertiser_name);
CREATE INDEX idx_campaigns_priority_index ON book.campaigns(priority_index DESC);
CREATE INDEX idx_campaigns_churn_prob ON book.campaigns(churn_prob_90d DESC);
CREATE INDEX idx_campaigns_priority_tier ON book.campaigns(priority_tier);
CREATE INDEX idx_campaigns_status ON book.campaigns(status);

CREATE INDEX idx_partners_name ON book.partners(partner_name);
CREATE INDEX idx_partner_opportunities_partner ON book.partner_opportunities(partner_name, playbook);

CREATE INDEX idx_data_snapshots_current ON book.data_snapshots(is_current) WHERE is_current = true;
CREATE INDEX idx_data_snapshots_date ON book.data_snapshots(snapshot_date DESC);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_campaigns_updated_at BEFORE UPDATE ON book.campaigns
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_partners_updated_at BEFORE UPDATE ON book.partners
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_partner_opportunities_updated_at BEFORE UPDATE ON book.partner_opportunities
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions
GRANT USAGE ON SCHEMA book TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA book TO app_user;
GRANT SELECT ON ALL TABLES IN SCHEMA book TO readonly_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA book TO app_user;

-- Add some sample data for testing
INSERT INTO book.data_snapshots (snapshot_date, file_name, record_count, is_current, last_modified)
VALUES (CURRENT_DATE, 'initial-migration.csv', 0, true, NOW());