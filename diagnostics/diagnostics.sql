/*
 * Yami DB fitness diagnostics (MySQL syntax).
 * Run against the RDS source. The runner script sets the default schema (USE)
 * before executing this file.
 */

SELECT '== STEP 1: SERVER VERSION (need 8.0+ for binlog_row_metadata=FULL) ==' AS section;
SELECT VERSION() AS mysql_version;

SELECT '== STEP 1b: Available schemas (ignore system schemas) ==' AS section;
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name NOT IN ('mysql','information_schema','performance_schema','sys');

SELECT '== STEP 2: TABLES MISSING A PRIMARY KEY (Debezium needs a PK per table) ==' AS section;
SELECT t.table_name
FROM information_schema.tables t
LEFT JOIN information_schema.table_constraints tc
       ON t.table_schema = tc.table_schema
      AND t.table_name   = tc.table_name
      AND tc.constraint_type = 'PRIMARY KEY'
WHERE t.table_schema = DATABASE()
  AND t.table_type   = 'BASE TABLE'
  AND tc.constraint_name IS NULL
ORDER BY t.table_name;

SELECT '== STEP 3: ROW COUNTS + SIZES PER TABLE (throughput-story volume) ==' AS section;
SELECT table_name,
       table_rows                                          AS approx_rows,
       ROUND(data_length /1024/1024, 2)                    AS data_mb,
       ROUND(index_length/1024/1024, 2)                    AS index_mb,
       ROUND((data_length+index_length)/1024/1024, 2)      AS total_mb
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_type = 'BASE TABLE'
ORDER BY (data_length+index_length) DESC;

SELECT '== STEP 4a: TABLE LIST (schema shape, expect OLTP entities) ==' AS section;
SELECT table_name
FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
ORDER BY table_name;

SELECT '== STEP 4b: FOREIGN KEY RELATIONSHIPS (needed for Flink joins) ==' AS section;
SELECT table_name        AS child_table,
       column_name       AS fk_column,
       referenced_table_name  AS parent_table,
       referenced_column_name AS parent_column
FROM information_schema.key_column_usage
WHERE table_schema = DATABASE()
  AND referenced_table_name IS NOT NULL
ORDER BY table_name, column_name;

SELECT '== STEP 5: TIMESTAMP/DATETIME COLUMNS (windowing + anomaly detection) ==' AS section;
SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND data_type IN ('timestamp','datetime','date')
ORDER BY table_name, ordinal_position;

SELECT '== DONE ==' AS section;