-- Aqueduct streaming pipeline (Flink SQL).
-- Reads the Debezium change-event topics and continuously writes results into
-- the Postgres analytics tables defined in sink_tables.sql.

-- Sources: the Debezium change-event topics.

CREATE TABLE order_info (
  order_id INT,
  user_id INT,
  country STRING,
  zipcode STRING,
  goods_amount DOUBLE,
  order_date INT,
  `year` INT,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'aqueduct.public.order_info',
  'properties.bootstrap.servers' = 'aqueduct-kafka:9092',
  'properties.group.id' = 'flink-pipeline-order-info',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'debezium-json'
);

CREATE TABLE order_goods (
  rec_id BIGINT,
  order_id INT,
  goods_id BIGINT,
  goods_price DOUBLE,
  deal_price DOUBLE,
  goods_number INT,
  proc_time AS PROCTIME(),
  PRIMARY KEY (rec_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'aqueduct.public.order_goods',
  'properties.bootstrap.servers' = 'aqueduct-kafka:9092',
  'properties.group.id' = 'flink-pipeline-order-goods',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'debezium-json'
);

CREATE TABLE goods_info (
  goods_id BIGINT,
  goods_name STRING,
  brand STRING,
  category_level1 STRING,
  category_level2 STRING,
  PRIMARY KEY (goods_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'aqueduct.public.goods_info',
  'properties.bootstrap.servers' = 'aqueduct-kafka:9092',
  'properties.group.id' = 'flink-pipeline-goods-info',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

-- Sinks: the Postgres analytics tables, via JDBC.

CREATE TABLE sink_enriched_line_items (
  rec_id BIGINT,
  order_id INT,
  country STRING,
  brand STRING,
  category STRING,
  deal_price DOUBLE,
  goods_number INT,
  PRIMARY KEY (rec_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://aqueduct-postgres:5432/yami',
  'table-name' = 'enriched_line_items',
  'username' = 'cdc',
  'password' = 'cdc_pw'
);

CREATE TABLE sink_revenue_by_category (
  category STRING,
  line_items BIGINT,
  revenue DOUBLE,
  PRIMARY KEY (category) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://aqueduct-postgres:5432/yami',
  'table-name' = 'revenue_by_category',
  'username' = 'cdc',
  'password' = 'cdc_pw'
);

CREATE TABLE sink_revenue_by_window (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  line_items BIGINT,
  revenue DOUBLE,
  PRIMARY KEY (window_start, window_end) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://aqueduct-postgres:5432/yami',
  'table-name' = 'revenue_by_window',
  'username' = 'cdc',
  'password' = 'cdc_pw'
);

CREATE TABLE sink_pricing_anomalies (
  rec_id BIGINT,
  order_id INT,
  brand STRING,
  goods_price DOUBLE,
  deal_price DOUBLE,
  goods_number INT,
  PRIMARY KEY (rec_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://aqueduct-postgres:5432/yami',
  'table-name' = 'pricing_anomalies',
  'username' = 'cdc',
  'password' = 'cdc_pw'
);

-- Run all four continuously as a single job.
EXECUTE STATEMENT SET
BEGIN

INSERT INTO sink_enriched_line_items
SELECT og.rec_id, og.order_id, oi.country, gi.brand, gi.category_level1, og.deal_price, og.goods_number
FROM order_goods og
JOIN order_info oi ON og.order_id = oi.order_id
JOIN goods_info gi ON og.goods_id = gi.goods_id;

INSERT INTO sink_revenue_by_category
SELECT gi.category_level1, COUNT(*), SUM(og.deal_price * og.goods_number)
FROM order_goods og
JOIN goods_info gi ON og.goods_id = gi.goods_id
GROUP BY gi.category_level1;

INSERT INTO sink_revenue_by_window
SELECT
  CAST(window_start AS TIMESTAMP(3)),
  CAST(window_end AS TIMESTAMP(3)),
  COUNT(*),
  SUM(deal_price * goods_number)
FROM TABLE(TUMBLE(TABLE order_goods, DESCRIPTOR(proc_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end;

INSERT INTO sink_pricing_anomalies
SELECT og.rec_id, og.order_id, gi.brand, og.goods_price, og.deal_price, og.goods_number
FROM order_goods og
JOIN goods_info gi ON og.goods_id = gi.goods_id
WHERE og.deal_price > og.goods_price;

END;
