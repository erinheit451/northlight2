-- ===========================================================================
-- UNIFIED ANALYTICS SCHEMA
-- Cross-platform analytics combining Heartbeat ETL data with Northlight benchmarks
-- ===========================================================================

-- Unified Campaign Performance View
CREATE OR REPLACE VIEW unified_analytics.campaign_performance AS
SELECT
    h.campaign_name,
    h.advertiser_name,
    h.channel,
    h.product,
    h.amount_spent as actual_spend,
    h.campaign_budget as budget,
    h.running_cid_leads as leads,
    h.mcid_clicks as clicks,
    h.cpl_mcid as actual_cpl,
    h.mcid_avg_cpc as actual_cpc,

    -- Calculate derived metrics
    CASE
        WHEN h.mcid_clicks > 0 THEN h.running_cid_leads::DECIMAL / h.mcid_clicks
        ELSE NULL
    END as conversion_rate,

    -- Benchmark comparison
    b.cpl_median as benchmark_cpl_median,
    b.cpc_median as benchmark_cpc_median,

    -- Performance scoring
    CASE
        WHEN h.cpl_mcid <= b.cpl_top25 THEN 'excellent'
        WHEN h.cpl_mcid <= b.cpl_median THEN 'good'
        WHEN h.cpl_mcid <= b.cpl_bot25 THEN 'average'
        ELSE 'below_average'
    END as cpl_performance_tier,

    h.snapshot_date,
    h.extracted_at
FROM heartbeat_core.ultimate_dms_campaigns h
LEFT JOIN northlight_benchmarks.benchmark_data b ON (
    -- Match on category/subcategory logic (to be refined based on business rules)
    LOWER(h.product) = b.key
)
WHERE h.running_cid_leads > 0;

-- Partner Pipeline Health View
CREATE OR REPLACE VIEW unified_analytics.partner_pipeline_health AS
SELECT
    pp.partner_name,
    pp.opportunity_name,
    pp.stage,
    pp.amount,
    pp.close_date,
    pp.probability,

    -- Partner metrics from budget waterfall
    bwc.budgets as partner_budget,
    bwc.starting_clients,
    bwc.ending_clients,
    bwc.new_clients,
    bwc.churned_clients,

    -- Performance indicators
    CASE
        WHEN pp.stage = 'Closed Won' THEN 'won'
        WHEN pp.stage = 'Closed Lost' THEN 'lost'
        WHEN pp.close_date < CURRENT_DATE THEN 'overdue'
        WHEN pp.close_date <= CURRENT_DATE + INTERVAL '30 days' THEN 'closing_soon'
        ELSE 'pipeline'
    END as pipeline_status,

    pp.created_date,
    pp.last_modified_date
FROM heartbeat_salesforce.sf_partner_pipeline pp
LEFT JOIN heartbeat_core.budget_waterfall_client bwc ON (
    pp.partner_name = bwc.advertiser_name
    AND pp.created_date::DATE = bwc.snapshot_date
);

-- Executive Dashboard Metrics
CREATE MATERIALIZED VIEW unified_analytics.executive_dashboard AS
SELECT
    DATE_TRUNC('month', snapshot_date) as month,

    -- Campaign Performance Metrics
    COUNT(DISTINCT campaign_name) as total_campaigns,
    SUM(actual_spend) as total_spend,
    SUM(leads) as total_leads,
    SUM(clicks) as total_clicks,
    AVG(actual_cpl) as avg_cpl,
    AVG(actual_cpc) as avg_cpc,

    -- Performance Distribution
    COUNT(CASE WHEN cpl_performance_tier = 'excellent' THEN 1 END) as excellent_campaigns,
    COUNT(CASE WHEN cpl_performance_tier = 'good' THEN 1 END) as good_campaigns,
    COUNT(CASE WHEN cpl_performance_tier = 'average' THEN 1 END) as average_campaigns,
    COUNT(CASE WHEN cpl_performance_tier = 'below_average' THEN 1 END) as below_avg_campaigns,

    -- Efficiency Metrics
    CASE
        WHEN SUM(clicks) > 0 THEN SUM(leads)::DECIMAL / SUM(clicks)
        ELSE NULL
    END as overall_conversion_rate,

    CASE
        WHEN SUM(leads) > 0 THEN SUM(actual_spend) / SUM(leads)
        ELSE NULL
    END as blended_cpl,

    -- Data freshness
    MAX(extracted_at) as last_updated
FROM unified_analytics.campaign_performance
WHERE snapshot_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY DATE_TRUNC('month', snapshot_date);

-- Create index on materialized view
CREATE UNIQUE INDEX idx_executive_dashboard_month ON unified_analytics.executive_dashboard(month);

-- Data Quality Monitoring View
CREATE OR REPLACE VIEW unified_analytics.data_quality_monitor AS
SELECT
    'heartbeat_campaigns' as source_table,
    COUNT(*) as record_count,
    COUNT(CASE WHEN amount_spent IS NULL THEN 1 END) as null_spend_count,
    COUNT(CASE WHEN running_cid_leads = 0 THEN 1 END) as zero_leads_count,
    MIN(snapshot_date) as earliest_date,
    MAX(snapshot_date) as latest_date,
    MAX(extracted_at) as last_extraction
FROM heartbeat_core.ultimate_dms_campaigns

UNION ALL

SELECT
    'benchmark_data' as source_table,
    COUNT(*) as record_count,
    COUNT(CASE WHEN cpl_median IS NULL THEN 1 END) as null_cpl_count,
    COUNT(CASE WHEN sample_size < 10 THEN 1 END) as low_sample_count,
    MIN(created_at::DATE) as earliest_date,
    MAX(created_at::DATE) as latest_date,
    MAX(updated_at) as last_extraction
FROM northlight_benchmarks.benchmark_data

UNION ALL

SELECT
    'partner_pipeline' as source_table,
    COUNT(*) as record_count,
    COUNT(CASE WHEN amount IS NULL THEN 1 END) as null_amount_count,
    COUNT(CASE WHEN stage IS NULL THEN 1 END) as null_stage_count,
    MIN(created_date::DATE) as earliest_date,
    MAX(created_date::DATE) as latest_date,
    MAX(last_modified_date) as last_extraction
FROM heartbeat_salesforce.sf_partner_pipeline;

-- Refresh function for materialized views
CREATE OR REPLACE FUNCTION unified_analytics.refresh_dashboards()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY unified_analytics.executive_dashboard;
    -- Add more materialized views here as they're created
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT SELECT ON ALL TABLES IN SCHEMA unified_analytics TO app_user;
GRANT SELECT ON ALL TABLES IN SCHEMA unified_analytics TO readonly_user;
GRANT EXECUTE ON FUNCTION unified_analytics.refresh_dashboards() TO app_user;