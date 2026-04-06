CREATE TABLE IF NOT EXISTS public.fact_client_uploads_staging (
    discount_code VARCHAR(256),
    orders BIGINT,
    revenue DOUBLE PRECISION,
    order_date DATE,
    client_name VARCHAR(256) NOT NULL,
    event_name VARCHAR(128),
    load_id VARCHAR(64) NOT NULL,
    event_date DATE,
    created_at TIMESTAMP
)
DISTSTYLE AUTO
SORTKEY (client_name, order_date, load_id);

CREATE TABLE IF NOT EXISTS public.fact_client_uploads (
    discount_code VARCHAR(256),
    orders BIGINT NOT NULL,
    revenue DOUBLE PRECISION,
    order_date DATE,
    client_name VARCHAR(256) NOT NULL
)
DISTSTYLE AUTO
SORTKEY (client_name, order_date);
