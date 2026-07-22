-- Aqueduct analytics sink tables, written continuously by the Flink jobs.
-- They live in the same database as the source tables but are not captured by
-- Debezium, since they are not in the connector's table include list.

CREATE TABLE IF NOT EXISTS enriched_line_items (
    rec_id       bigint PRIMARY KEY,
    order_id     integer,
    country      varchar(255),
    brand        varchar(100),
    category     varchar(100),
    deal_price   double precision,
    goods_number integer,
    seen_at      timestamp DEFAULT now()
);

CREATE TABLE IF NOT EXISTS revenue_by_category (
    category   varchar(100) PRIMARY KEY,
    line_items bigint,
    revenue    double precision
);

CREATE TABLE IF NOT EXISTS revenue_by_window (
    window_start timestamp,
    window_end   timestamp,
    line_items   bigint,
    revenue      double precision,
    PRIMARY KEY (window_start, window_end)
);

CREATE TABLE IF NOT EXISTS pricing_anomalies (
    rec_id       bigint PRIMARY KEY,
    order_id     integer,
    brand        varchar(100),
    goods_price  double precision,
    deal_price   double precision,
    goods_number integer,
    detected_at  timestamp DEFAULT now()
);
