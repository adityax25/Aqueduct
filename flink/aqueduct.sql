-- Aqueduct stream processing (Flink SQL).
--
-- Prerequisites in PostgreSQL:
--   ALTER TABLE order_info  REPLICA IDENTITY FULL;
--   ALTER TABLE order_goods REPLICA IDENTITY FULL;
--   ALTER TABLE goods_info  REPLICA IDENTITY FULL;
-- and seed the product-catalog topic once (it is otherwise never changed):
--   UPDATE goods_info SET goods_name = goods_name;

-- Source tables backed by the Debezium change-event topics.

CREATE TABLE order_info (
  order_id INT,
  user_id INT,
  country STRING,
  zipcode STRING,
  goods_amount DOUBLE,
  order_date INT,          -- Debezium encodes DATE as days since 1970-01-01
  `year` INT,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'aqueduct.public.order_info',
  'properties.bootstrap.servers' = 'aqueduct-kafka:9092',
  'properties.group.id' = 'flink-order-info',
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
  'properties.group.id' = 'flink-order-goods',
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
  'properties.group.id' = 'flink-goods-info',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

-- Three-way in-flight join: each line-item enriched with its order and product.
SELECT
  og.order_id,
  oi.country,
  gi.brand,
  gi.category_level1,
  og.deal_price,
  og.goods_number
FROM order_goods og
JOIN order_info oi ON og.order_id = oi.order_id
JOIN goods_info gi ON og.goods_id = gi.goods_id;

-- Running aggregation: revenue and line-item count per category.
SELECT
  gi.category_level1 AS category,
  COUNT(*) AS line_items,
  ROUND(SUM(og.deal_price * og.goods_number), 2) AS revenue
FROM order_goods og
JOIN goods_info gi ON og.goods_id = gi.goods_id
GROUP BY gi.category_level1;

-- Windowed aggregation: revenue per 10-second tumbling window.
SELECT
  window_start,
  window_end,
  COUNT(*) AS line_items,
  ROUND(SUM(deal_price * goods_number), 2) AS revenue
FROM TABLE(
  TUMBLE(TABLE order_goods, DESCRIPTOR(proc_time), INTERVAL '10' SECOND)
)
GROUP BY window_start, window_end;

-- Real-time anomaly detection: line-items priced above list price.
SELECT
  og.rec_id,
  og.order_id,
  gi.brand,
  og.goods_price,
  og.deal_price,
  og.goods_number
FROM order_goods og
JOIN goods_info gi ON og.goods_id = gi.goods_id
WHERE og.deal_price > og.goods_price;
