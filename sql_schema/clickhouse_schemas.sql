-- ==========================================================
-- ClickHouse schema for station_traffic pipeline
-- Layers:
--   1) analytics           : raw ingestion tables (Silver -> ClickHouse)
--   2) analytics_reporting : reporting marts + materialized views
-- ==========================================================

CREATE DATABASE IF NOT EXISTS analytics;
CREATE DATABASE IF NOT EXISTS analytics_reporting;


-- ==========================================================
-- 1) RAW TABLES (targets for incremental_loading/silver_to_clickhouse.py)
-- ==========================================================

CREATE TABLE IF NOT EXISTS analytics.operators (
    id UInt32,
    name String,
    code String,
    is_deleted UInt8,
    created_at DateTime,
    updated_at DateTime,
    deleted_at Nullable(DateTime)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS analytics.stations (
    id UInt32,
    station_code String,
    operator_id UInt32,
    province String,
    district String,
    latitude Float64,
    longitude Float64,
    install_date Nullable(DateTime),
    status LowCardinality(String),
    is_deleted UInt8,
    created_at DateTime,
    updated_at DateTime,
    deleted_at Nullable(DateTime)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (operator_id, id);

CREATE TABLE IF NOT EXISTS analytics.station_traffic (
    id UInt64,
    station_id UInt32,
    technology LowCardinality(String),
    traffic_gb Float64,
    user_count UInt32,
    event_time DateTime,
    is_deleted UInt8,
    created_at DateTime,
    updated_at DateTime,
    deleted_at Nullable(DateTime)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (station_id, event_time, id);


-- ==========================================================
-- 2) REPORTING TABLES (pre-aggregated marts)
-- ==========================================================

CREATE TABLE IF NOT EXISTS analytics_reporting.station_traffic_hourly (
    hour_bucket DateTime,
    station_id UInt32,
    technology LowCardinality(String),
    traffic_gb_sum Float64,
    user_count_sum UInt64,
    sample_count UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour_bucket)
ORDER BY (hour_bucket, station_id, technology);

CREATE TABLE IF NOT EXISTS analytics_reporting.station_traffic_daily (
    day_bucket Date,
    station_id UInt32,
    technology LowCardinality(String),
    traffic_gb_sum Float64,
    user_count_sum UInt64,
    sample_count UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day_bucket)
ORDER BY (day_bucket, station_id, technology);

CREATE TABLE IF NOT EXISTS analytics_reporting.operator_traffic_daily (
    day_bucket Date,
    operator_id UInt32,
    operator_code LowCardinality(String),
    technology LowCardinality(String),
    traffic_gb_sum Float64,
    user_count_sum UInt64,
    sample_count UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day_bucket)
ORDER BY (day_bucket, operator_id, technology);

CREATE TABLE IF NOT EXISTS analytics_reporting.province_traffic_daily (
    day_bucket Date,
    province LowCardinality(String),
    technology LowCardinality(String),
    traffic_gb_sum Float64,
    user_count_sum UInt64,
    sample_count UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day_bucket)
ORDER BY (day_bucket, province, technology);

CREATE TABLE IF NOT EXISTS analytics_reporting.station_health_daily (
    day_bucket Date,
    operator_id UInt32,
    province LowCardinality(String),
    total_stations UInt64,
    active_stations UInt64,
    inactive_stations UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day_bucket)
ORDER BY (day_bucket, operator_id, province);


-- ==========================================================
-- 3) MATERIALIZED VIEWS (raw -> reporting)
-- ==========================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics_reporting.mv_station_traffic_hourly
TO analytics_reporting.station_traffic_hourly
AS
SELECT
    toStartOfHour(event_time) AS hour_bucket,
    station_id,
    technology,
    sum(traffic_gb) AS traffic_gb_sum,
    sum(toUInt64(user_count)) AS user_count_sum,
    count() AS sample_count
FROM analytics.station_traffic
WHERE is_deleted = 0
GROUP BY hour_bucket, station_id, technology;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics_reporting.mv_station_traffic_daily
TO analytics_reporting.station_traffic_daily
AS
SELECT
    toDate(event_time) AS day_bucket,
    station_id,
    technology,
    sum(traffic_gb) AS traffic_gb_sum,
    sum(toUInt64(user_count)) AS user_count_sum,
    count() AS sample_count
FROM analytics.station_traffic
WHERE is_deleted = 0
GROUP BY day_bucket, station_id, technology;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics_reporting.mv_operator_traffic_daily
TO analytics_reporting.operator_traffic_daily
AS
SELECT
    toDate(t.event_time) AS day_bucket,
    s.operator_id AS operator_id,
    o.code AS operator_code,
    t.technology AS technology,
    sum(t.traffic_gb) AS traffic_gb_sum,
    sum(toUInt64(t.user_count)) AS user_count_sum,
    count() AS sample_count
FROM analytics.station_traffic AS t
INNER JOIN analytics.stations AS s ON t.station_id = s.id
INNER JOIN analytics.operators AS o ON s.operator_id = o.id
WHERE t.is_deleted = 0
  AND s.is_deleted = 0
  AND o.is_deleted = 0
GROUP BY day_bucket, operator_id, operator_code, technology;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics_reporting.mv_province_traffic_daily
TO analytics_reporting.province_traffic_daily
AS
SELECT
    toDate(t.event_time) AS day_bucket,
    if(empty(s.province), 'UNKNOWN', s.province) AS province,
    t.technology AS technology,
    sum(t.traffic_gb) AS traffic_gb_sum,
    sum(toUInt64(t.user_count)) AS user_count_sum,
    count() AS sample_count
FROM analytics.station_traffic AS t
INNER JOIN analytics.stations AS s ON t.station_id = s.id
WHERE t.is_deleted = 0
  AND s.is_deleted = 0
GROUP BY day_bucket, province, technology;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics_reporting.mv_station_health_daily
TO analytics_reporting.station_health_daily
AS
SELECT
    toDate(updated_at) AS day_bucket,
    operator_id,
    if(empty(province), 'UNKNOWN', province) AS province,
    count() AS total_stations,
    countIf(status = 'active' AND is_deleted = 0) AS active_stations,
    countIf(status != 'active' OR is_deleted = 1) AS inactive_stations
FROM analytics.stations
GROUP BY day_bucket, operator_id, province;


-- ==========================================================
-- 4) SAMPLE REPORTING QUERIES
-- ==========================================================

-- Top operators by traffic in the last 7 days
-- SELECT
--     operator_code,
--     sum(traffic_gb_sum) AS total_traffic_gb,
--     sum(user_count_sum) AS total_users
-- FROM analytics_reporting.operator_traffic_daily
-- WHERE day_bucket >= today() - 7
-- GROUP BY operator_code
-- ORDER BY total_traffic_gb DESC;

-- Traffic by province and technology (last 30 days)
-- SELECT
--     province,
--     technology,
--     sum(traffic_gb_sum) AS total_traffic_gb
-- FROM analytics_reporting.province_traffic_daily
-- WHERE day_bucket >= today() - 30
-- GROUP BY province, technology
-- ORDER BY total_traffic_gb DESC;

-- Busiest stations by hourly traffic
-- SELECT
--     hour_bucket,
--     station_id,
--     sum(traffic_gb_sum) AS traffic_gb
-- FROM analytics_reporting.station_traffic_hourly
-- WHERE hour_bucket >= now() - INTERVAL 48 HOUR
-- GROUP BY hour_bucket, station_id
-- ORDER BY traffic_gb DESC
-- LIMIT 100;


-- ==========================================================
-- NOTE ABOUT STRICT CORRECTNESS
-- ==========================================================
-- Materialized views aggregate incoming rows. If source rows are updated later
-- (ReplacingMergeTree version changes, soft deletes), aggregates can drift.
-- For strict reconciliation, run periodic full refresh jobs from FINAL scans:
--   TRUNCATE TABLE analytics_reporting.station_traffic_daily;
--   INSERT INTO analytics_reporting.station_traffic_daily
--   SELECT toDate(event_time), station_id, technology,
--          sum(traffic_gb), sum(toUInt64(user_count)), count()
--   FROM analytics.station_traffic FINAL
--   WHERE is_deleted = 0
--   GROUP BY toDate(event_time), station_id, technology;
