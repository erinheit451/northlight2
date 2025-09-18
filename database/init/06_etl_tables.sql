-- ===========================================================================
-- ETL DATA TABLES
-- Tables for all extracted data sources requiring daily incremental loading
-- ===========================================================================

-- ========================================
-- HEARTBEAT CORE TABLES
-- ========================================

-- Ultimate DMS Campaigns
CREATE TABLE IF NOT EXISTS heartbeat_core.ultimate_dms_campaigns (
    id SERIAL PRIMARY KEY,
    last_active DATE,
    channel TEXT,
    bid_name TEXT,
    bid TEXT,
    advertiser_name TEXT,
    primary_user_name TEXT,
    am TEXT,
    am_manager TEXT,
    optimizer_1_manager TEXT,
    optimizer_1 TEXT,
    optimizer_2_manager TEXT,
    optimizer_2 TEXT,
    maid TEXT,
    mcid_clicks NUMERIC,
    mcid_leads NUMERIC,
    mcid TEXT,
    campaign_name TEXT,
    campaign_id TEXT,
    product TEXT,
    offer_name TEXT,
    finance_product TEXT,
    tracking_method_name TEXT,
    sem_reviews_p30 TEXT,
    io_cycle NUMERIC,
    avg_cycle_length NUMERIC,
    running_cid_leads NUMERIC,
    amount_spent NUMERIC(12,2),
    days_elapsed NUMERIC,
    utilization NUMERIC(8,4),
    campaign_performance_rating TEXT,
    bc TEXT,
    bsc TEXT,
    campaign_budget NUMERIC(12,2),
    bsc_budget_bottom_10_pct NUMERIC(12,2),
    bsc_budget_bottom_25_pct NUMERIC(12,2),
    bsc_budget_average NUMERIC(12,2),
    bsc_budget_top_25_pct NUMERIC(12,2),
    bsc_budget_top_10_pct NUMERIC(12,2),
    cpl_goal NUMERIC(12,2),
    running_cid_cpl NUMERIC(12,2),
    cpl_mcid NUMERIC(12,2),
    cpl_last_15_days NUMERIC(12,2),
    cpl_15_to_30_days NUMERIC(12,2),
    bsc_cpl_top_10_pct NUMERIC(12,2),
    bsc_cpl_top_25_pct NUMERIC(12,2),
    bsc_cpl_avg NUMERIC(12,2),
    bsc_cpl_bottom_25_pct NUMERIC(12,2),
    bsc_cpl_bottom_10_pct NUMERIC(12,2),
    mcid_avg_cpc NUMERIC(12,2),
    bsc_cpc_top_10_pct NUMERIC(12,2),
    bsc_cpc_top_25_pct NUMERIC(12,2),
    bsc_cpc_average NUMERIC(12,2),
    bsc_cpc_bottom_25_pct NUMERIC(12,2),
    bsc_cpc_bottom_10_pct NUMERIC(12,2),
    snapshot_date DATE,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_file TEXT,
    UNIQUE(campaign_id, snapshot_date)
);

-- Budget Waterfall Client
CREATE TABLE IF NOT EXISTS heartbeat_core.budget_waterfall_client (
    id SERIAL PRIMARY KEY,
    from_sod DATE,
    to_eod DATE,
    channel TEXT,
    first_aid TEXT,
    advertiser_name TEXT,
    maturity NUMERIC,
    bid TEXT,
    business_name TEXT,
    office TEXT,
    area TEXT,
    currency TEXT,
    starting_clients NUMERIC,
    churned_clients NUMERIC,
    new_clients NUMERIC,
    winback_clients NUMERIC,
    ending_clients NUMERIC,
    som_budgets NUMERIC(15,2),
    budgets NUMERIC(15,2),
    net_new_budgets NUMERIC(15,2),
    net_change_pct NUMERIC(10,6),
    new_client_budgets NUMERIC(15,2),
    winback_client_budgets NUMERIC(15,2),
    flighted_acquired_client_budgets NUMERIC(15,2),
    total_cross_sales NUMERIC(15,2),
    total_upsells NUMERIC(15,2),
    total_increases NUMERIC(15,2),
    total_reverse_cross_sales NUMERIC(15,2),
    total_downsells NUMERIC(15,2),
    churned_existing_client_total_budgets NUMERIC(15,2),
    flighted_churned_existing_client_total_budgets NUMERIC(15,2),
    total_decreases NUMERIC(15,2),
    client_inflow_detail TEXT,
    client_outflow_detail TEXT,
    snapshot_date DATE,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_file TEXT,
    UNIQUE(advertiser_name, from_sod, to_eod)
);

-- Budget Waterfall Channel (NEW)
CREATE TABLE IF NOT EXISTS heartbeat_core.budget_waterfall_channel (
    id SERIAL PRIMARY KEY,
    from_sod DATE,
    to_eod DATE,
    channel TEXT,
    first_aid TEXT,
    advertiser_name TEXT,
    maturity NUMERIC,
    bid TEXT,
    business_name TEXT,
    office TEXT,
    area TEXT,
    currency TEXT,
    starting_clients NUMERIC,
    churned_clients NUMERIC,
    new_clients NUMERIC,
    winback_clients NUMERIC,
    ending_clients NUMERIC,
    som_budgets NUMERIC(15,2),
    budgets NUMERIC(15,2),
    net_new_budgets NUMERIC(15,2),
    net_change_pct NUMERIC(10,6),
    new_client_budgets NUMERIC(15,2),
    winback_client_budgets NUMERIC(15,2),
    flighted_acquired_client_budgets NUMERIC(15,2),
    total_cross_sales NUMERIC(15,2),
    total_upsells NUMERIC(15,2),
    total_increases NUMERIC(15,2),
    total_reverse_cross_sales NUMERIC(15,2),
    total_downsells NUMERIC(15,2),
    churned_existing_client_total_budgets NUMERIC(15,2),
    flighted_churned_existing_client_total_budgets NUMERIC(15,2),
    total_decreases NUMERIC(15,2),
    client_inflow_detail TEXT,
    client_outflow_detail TEXT,
    snapshot_date DATE,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_file TEXT,
    UNIQUE(channel, advertiser_name, from_sod, to_eod)
);

-- DFP RIJ Alerts (NEW)
CREATE TABLE IF NOT EXISTS heartbeat_core.dfp_rij_alerts (
    id SERIAL PRIMARY KEY,
    office TEXT,
    service_assignment TEXT,
    agent TEXT,
    business_id TEXT,
    business_name TEXT,
    alert_type TEXT,
    alert_description TEXT,
    alert_priority TEXT,
    created_date DATE,
    resolved_date DATE,
    status TEXT,
    resolution_notes TEXT,
    snapshot_date DATE,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_file TEXT,
    UNIQUE(business_id, alert_type, created_date)
);

-- Corporate Portal Budget Summary (NEW)
CREATE TABLE IF NOT EXISTS heartbeat_core.corporate_portal_budget_summary (
    id SERIAL PRIMARY KEY,
    budget_period TEXT,
    entity_name TEXT,
    entity_type TEXT,
    budget_category TEXT,
    planned_budget NUMERIC(15,2),
    actual_budget NUMERIC(15,2),
    variance NUMERIC(15,2),
    variance_pct NUMERIC(8,4),
    last_updated_date DATE,
    reporting_period DATE,
    snapshot_date DATE,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_file TEXT,
    UNIQUE(entity_name, budget_period, budget_category)
);

-- ========================================
-- HEARTBEAT SALESFORCE TABLES
-- ========================================

-- SF Partner Pipeline
CREATE TABLE IF NOT EXISTS heartbeat_salesforce.sf_partner_pipeline (
    id SERIAL PRIMARY KEY,
    account_owner TEXT,
    bid TEXT,
    account_name TEXT,
    opportunity_name TEXT,
    type_1 TEXT,
    type_2 TEXT,
    created_date DATE,
    last_stage_change_date DATE,
    all_tcv NUMERIC(15,2),
    mo_offer_amt NUMERIC(12,2),
    net_new_tcv NUMERIC(15,2),
    modification_amount NUMERIC(15,2),
    close_date DATE,
    solution TEXT,
    stage_duration NUMERIC(10,2),
    cycles INTEGER,
    processed_date DATE,
    stage_name TEXT,
    probability NUMERIC(5,2),
    partner_name TEXT,
    snapshot_date DATE,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_file TEXT,
    UNIQUE(opportunity_name, account_name, created_date)
);

-- SF Partner Calls (NEW)
CREATE TABLE IF NOT EXISTS heartbeat_salesforce.sf_partner_calls (
    id SERIAL PRIMARY KEY,
    call_id TEXT,
    partner_name TEXT,
    account_name TEXT,
    contact_name TEXT,
    call_date DATE,
    call_time TIME,
    call_duration_minutes NUMERIC,
    call_type TEXT,
    call_outcome TEXT,
    follow_up_required BOOLEAN,
    next_call_date DATE,
    notes TEXT,
    created_by TEXT,
    last_modified_date TIMESTAMP,
    snapshot_date DATE,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_file TEXT,
    UNIQUE(call_id, partner_name, call_date)
);

-- SF Tim King Partner Pipeline (NEW)
CREATE TABLE IF NOT EXISTS heartbeat_salesforce.sf_tim_king_partner_pipeline (
    id SERIAL PRIMARY KEY,
    account_name TEXT,
    account_owner TEXT,
    business_name TEXT,
    close_date DATE,
    created_date DATE,
    last_stage_change_date DATE,
    opportunity_name TEXT,
    campaign_duration NUMERIC,
    net_new_tcv NUMERIC(15,2),
    all_tcv NUMERIC(15,2),
    mo_offer_amt NUMERIC(12,2),
    modification_amount NUMERIC(15,2),
    offer_name TEXT,
    paid_on_date DATE,
    sale_type TEXT,
    stage_duration NUMERIC(10,2),
    stage_name TEXT,
    type TEXT,
    snapshot_date DATE,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_file TEXT,
    UNIQUE(opportunity_name, account_name, created_date)
);

-- SF Grader Opportunities (NEW)
CREATE TABLE IF NOT EXISTS heartbeat_salesforce.sf_grader_opportunities (
    id SERIAL PRIMARY KEY,
    opportunity_id TEXT,
    opportunity_name TEXT,
    account_name TEXT,
    account_owner TEXT,
    stage_name TEXT,
    probability NUMERIC(5,2),
    amount NUMERIC(15,2),
    close_date DATE,
    created_date DATE,
    last_modified_date TIMESTAMP,
    lead_source TEXT,
    campaign_source TEXT,
    grader_score NUMERIC(5,2),
    grader_grade TEXT,
    graded_date DATE,
    graded_by TEXT,
    grade_notes TEXT,
    snapshot_date DATE,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_file TEXT,
    UNIQUE(opportunity_id, graded_date)
);

-- ========================================
-- HEARTBEAT PERFORMANCE TABLES
-- ========================================

-- Agreed CPL Performance (NEW)
CREATE TABLE IF NOT EXISTS heartbeat_performance.agreed_cpl_performance (
    id SERIAL PRIMARY KEY,
    advertiser_id TEXT,
    advertiser_name TEXT,
    first_aid TEXT,
    mcid TEXT,
    campaign_name TEXT,
    campaign_id TEXT,
    agreed_cpl NUMERIC(12,2),
    actual_cpl NUMERIC(12,2),
    cpl_variance NUMERIC(12,2),
    cpl_variance_pct NUMERIC(8,4),
    leads_delivered NUMERIC,
    leads_target NUMERIC,
    performance_status TEXT,
    measurement_period_start DATE,
    measurement_period_end DATE,
    last_reviewed_date DATE,
    reviewed_by TEXT,
    snapshot_date DATE,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_file TEXT,
    UNIQUE(advertiser_id, mcid, measurement_period_start, measurement_period_end)
);

-- Spend Revenue Performance (NEW)
CREATE TABLE IF NOT EXISTS heartbeat_performance.spend_revenue_performance (
    id SERIAL PRIMARY KEY,
    maid TEXT,
    advertiser_name TEXT,
    partner_name TEXT,
    am TEXT,
    campaign_name TEXT,
    campaign_id TEXT,
    budget_allocated NUMERIC(15,2),
    amount_spent NUMERIC(15,2),
    revenue_generated NUMERIC(15,2),
    leads_generated NUMERIC,
    clicks_generated NUMERIC,
    impressions_generated NUMERIC,
    cpl NUMERIC(12,2),
    cpc NUMERIC(12,2),
    cpm NUMERIC(12,2),
    roas NUMERIC(8,4),
    roi_pct NUMERIC(8,4),
    performance_period_start DATE,
    performance_period_end DATE,
    snapshot_date DATE,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_file TEXT,
    UNIQUE(maid, campaign_id, performance_period_start, performance_period_end)
);

-- ========================================
-- INDEXES FOR PERFORMANCE
-- ========================================

-- Ultimate DMS Campaigns Indexes
CREATE INDEX IF NOT EXISTS idx_ultimate_dms_snapshot_date ON heartbeat_core.ultimate_dms_campaigns(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_ultimate_dms_advertiser_name ON heartbeat_core.ultimate_dms_campaigns(advertiser_name);
CREATE INDEX IF NOT EXISTS idx_ultimate_dms_campaign_name ON heartbeat_core.ultimate_dms_campaigns(campaign_name);

-- Budget Waterfall Indexes
CREATE INDEX IF NOT EXISTS idx_budget_waterfall_client_snapshot_date ON heartbeat_core.budget_waterfall_client(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_budget_waterfall_client_advertiser ON heartbeat_core.budget_waterfall_client(advertiser_name);
CREATE INDEX IF NOT EXISTS idx_budget_waterfall_channel_snapshot_date ON heartbeat_core.budget_waterfall_channel(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_budget_waterfall_channel_channel ON heartbeat_core.budget_waterfall_channel(channel);

-- Salesforce Indexes
CREATE INDEX IF NOT EXISTS idx_sf_partner_pipeline_snapshot_date ON heartbeat_salesforce.sf_partner_pipeline(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_sf_partner_pipeline_partner_name ON heartbeat_salesforce.sf_partner_pipeline(partner_name);
CREATE INDEX IF NOT EXISTS idx_sf_partner_calls_call_date ON heartbeat_salesforce.sf_partner_calls(call_date);
CREATE INDEX IF NOT EXISTS idx_sf_partner_calls_partner_name ON heartbeat_salesforce.sf_partner_calls(partner_name);
CREATE INDEX IF NOT EXISTS idx_sf_tim_king_pipeline_snapshot_date ON heartbeat_salesforce.sf_tim_king_partner_pipeline(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_sf_grader_opportunities_graded_date ON heartbeat_salesforce.sf_grader_opportunities(graded_date);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_agreed_cpl_performance_snapshot_date ON heartbeat_performance.agreed_cpl_performance(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_agreed_cpl_performance_advertiser ON heartbeat_performance.agreed_cpl_performance(advertiser_name);
CREATE INDEX IF NOT EXISTS idx_spend_revenue_performance_snapshot_date ON heartbeat_performance.spend_revenue_performance(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_spend_revenue_performance_advertiser ON heartbeat_performance.spend_revenue_performance(advertiser_name);

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA heartbeat_core TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA heartbeat_salesforce TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA heartbeat_performance TO app_user;
GRANT SELECT ON ALL TABLES IN SCHEMA heartbeat_core TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA heartbeat_salesforce TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA heartbeat_performance TO readonly_user;

-- Grant sequence permissions
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA heartbeat_core TO app_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA heartbeat_salesforce TO app_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA heartbeat_performance TO app_user;