CREATE TABLE IF NOT EXISTS stg_adscribe_performance (
    date DATE,
    source_type VARCHAR(50),
    client_name VARCHAR(255),
    show_name VARCHAR(500),
    discount_code VARCHAR(255),
    campaign_name VARCHAR(500),
    campaign_item_id VARCHAR(255),
    revenue DOUBLE PRECISION,
    orders DOUBLE PRECISION,
    impressions DOUBLE PRECISION,
    revenue_per_order DOUBLE PRECISION,
    revenue_per_impression DOUBLE PRECISION,
    impressions_per_order DOUBLE PRECISION,
    batch_id VARCHAR(255),
    run_id VARCHAR(255),
    source_key VARCHAR(1000),
    processed_at TIMESTAMP
)
DISTSTYLE AUTO;

CREATE TABLE IF NOT EXISTS fact_adscribe_performance (
    date DATE,
    source_type VARCHAR(50),
    client_name VARCHAR(255),
    show_name VARCHAR(500),
    discount_code VARCHAR(255),
    campaign_name VARCHAR(500),
    campaign_item_id VARCHAR(255),
    revenue DOUBLE PRECISION,
    orders DOUBLE PRECISION,
    impressions DOUBLE PRECISION,
    revenue_per_order DOUBLE PRECISION,
    revenue_per_impression DOUBLE PRECISION,
    impressions_per_order DOUBLE PRECISION,
    batch_id VARCHAR(255),
    run_id VARCHAR(255),
    source_key VARCHAR(1000),
    processed_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
SORTKEY (date, client_name);
