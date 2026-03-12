-- ============================================
-- ClickHouse Reporting Schema
-- Silver -> Raw Tables -> Materialized Views (Reporting Layer)
-- ============================================

CREATE DATABASE IF NOT EXISTS analytics;

-- ============================================
-- RAW TABLES (Data from Silver S3)
-- ============================================

-- Orders: Transaction-level data
CREATE TABLE IF NOT EXISTS analytics.orders (
    order_id UInt64,
    user_id UInt64,
    status String,
    total_amount Float64,
    created_at DateTime,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (order_id, created_at);

-- Product Reviews: Customer feedback
CREATE TABLE IF NOT EXISTS analytics.product_reviews (
    review_id UInt64,
    product_id UInt64,
    user_id UInt64,
    rating UInt64,
    comment String,
    created_at DateTime
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (review_id, created_at, rating);

-- Categories: Product classification
CREATE TABLE IF NOT EXISTS analytics.categories (
    category_id UInt64,
    category_name String,
    description String,
    created_at DateTime
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (category_id, created_at);

-- Products: Product master data
CREATE TABLE IF NOT EXISTS analytics.products (
    product_id UInt64,
    category_id UInt64,
    product_name String,
    base_price Decimal(12, 2),
    stock_quantity UInt64,
    created_at DateTime
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (product_id, product_name, created_at);

-- Order Items: Line-level order details
CREATE TABLE IF NOT EXISTS analytics.order_items (
    item_id UInt32,
    order_id UInt32,
    product_id UInt32,
    quantity Int32,
    unit_price Decimal(12, 2)
)
ENGINE = MergeTree
ORDER BY (order_id, product_id, item_id);


-- ============================================
-- REPORTING LAYER: MATERIALIZED VIEWS
-- ============================================

-- ────────────────────────────────────────────
-- 1. DAILY ORDER METRICS
-- ────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.daily_order_metrics_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date)
POPULATE
AS SELECT
    toDate(created_at) as order_date,
    count() as total_orders,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_order_value
FROM analytics.orders
GROUP BY order_date;

-- Query example:
 SELECT order_date, total_orders, sum(total_revenue) as revenue, avg(avg_order_value) as avg_order_value
 FROM analytics.daily_order_metrics_mv
 WHERE order_date >= today() - INTERVAL 30 DAY
 GROUP BY order_date, total_orders
 ORDER BY order_date DESC;


-- ────────────────────────────────────────────
-- 2. PRODUCT SALES PERFORMANCE
-- ────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.product_sales_performance_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (category_name, product_id)
POPULATE
AS SELECT
    toDate(o.created_at) as order_date,
    p.product_id as product_id,
    p.product_name as product_name,
    c.category_name as category_name,
    p.base_price as base_price, 
    sum(oi.quantity) as total_quantity_sold,
    sum(oi.quantity * oi.unit_price) as total_revenue,
    count(DISTINCT o.order_id) as total_orders,
    avg(oi.unit_price) as avg_selling_price
FROM analytics.orders o
INNER JOIN analytics.order_items oi ON o.order_id = oi.order_id
INNER JOIN analytics.products p ON oi.product_id = p.product_id
LEFT JOIN analytics.categories c ON p.category_id = c.category_id
WHERE o.status = 'completed'
GROUP BY order_date, p.product_id, p.product_name, c.category_name, p.base_price;

-- Query example:
 SELECT category_name, product_name, sum(total_revenue) as revenue
 FROM analytics.product_sales_performance_mv
 WHERE order_date >= toStartOfMonth(today())
 GROUP BY category_name, product_name
 ORDER BY revenue DESC
 LIMIT 20;


-- ────────────────────────────────────────────
-- 3. CATEGORY PERFORMANCE
-- ────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.category_performance_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, category_name)
POPULATE
AS SELECT
    toDate(o.created_at) as order_date,
    c.category_id as category_id,
    c.category_name as category_name,
    count(DISTINCT oi.product_id) as unique_products_sold,
    sum(oi.quantity) as total_units_sold,
    sum(oi.quantity * oi.unit_price) as total_revenue,
    count(DISTINCT o.order_id) as total_orders,
    count(DISTINCT o.user_id) as unique_customers
FROM analytics.orders o
INNER JOIN analytics.order_items oi ON o.order_id = oi.order_id
INNER JOIN analytics.products p ON oi.product_id = p.product_id
LEFT JOIN analytics.categories c ON p.category_id = c.category_id
WHERE o.status = 'completed'
GROUP BY order_date, c.category_id, c.category_name;

-- Query example:
-- SELECT category_name, sum(total_revenue) as revenue, sum(total_orders) as orders
-- FROM analytics.category_performance_mv
-- WHERE order_date >= today() - INTERVAL 7 DAY
-- GROUP BY category_name
-- ORDER BY revenue DESC;


-- ────────────────────────────────────────────
-- 4. PRODUCT REVIEW ANALYTICS
-- ────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.product_review_analytics_mv
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(review_month)
ORDER BY (product_id, review_month)
POPULATE
AS SELECT
    toStartOfMonth(pr.created_at) as review_month,
    pr.product_id as product_id,
    p.product_name as product_name,
    c.category_name as category_name,
    count() as total_reviews,
    avg(pr.rating) as avg_rating,
    countIf(pr.rating >= 4) as positive_reviews,
    countIf(pr.rating <= 2) as negative_reviews,
    uniqState(pr.user_id) as unique_reviewers
FROM analytics.product_reviews pr
INNER JOIN analytics.products p ON pr.product_id = p.product_id
LEFT JOIN analytics.categories c ON p.category_id = c.category_id
GROUP BY review_month, pr.product_id, p.product_name, c.category_name;

-- Query example:
-- SELECT product_name, avg_rating, total_reviews
-- FROM analytics.product_review_analytics_mv
-- WHERE review_month >= today() - INTERVAL 3 MONTH
-- ORDER BY avg_rating DESC, total_reviews DESC
-- LIMIT 50;


-- ────────────────────────────────────────────
-- 5. USER PURCHASE BEHAVIOR
-- ────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.user_purchase_behavior_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(order_month)
ORDER BY (user_id, order_month)
POPULATE
AS SELECT
    toStartOfMonth(created_at) as order_month,
    user_id,
    count() as total_orders,
    sum(total_amount) as total_spent,
    avg(total_amount) as avg_order_value,
    min(created_at) as first_order_date,
    max(created_at) as last_order_date,
    countIf(status = 'completed') as completed_orders,
    countIf(status = 'cancelled') as cancelled_orders
FROM analytics.orders
GROUP BY order_month, user_id;

-- Query example (Customer Lifetime Value):
-- SELECT user_id, sum(total_spent) as ltv, sum(total_orders) as orders
-- FROM analytics.user_purchase_behavior_mv
-- GROUP BY user_id
-- ORDER BY ltv DESC
-- LIMIT 100;


-- ────────────────────────────────────────────
-- 6. INVENTORY TURNOVER (Product Stock Movement)
-- ────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.inventory_turnover_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (product_id, order_date)
POPULATE
AS SELECT
    toDate(o.created_at) as order_date,
    p.product_id as product_id,
    p.product_name as product_name,
    c.category_name as category_name,
    p.stock_quantity as current_stock,
    sum(oi.quantity) as units_sold,
    count(DISTINCT o.order_id) as order_count
FROM analytics.orders o
INNER JOIN analytics.order_items oi ON o.order_id = oi.order_id
INNER JOIN analytics.products p ON oi.product_id = p.product_id
LEFT JOIN analytics.categories c ON p.category_id = c.category_id
WHERE o.status = 'completed'
GROUP BY order_date, p.product_id, p.product_name, c.category_name, p.stock_quantity;

-- Query example (Low stock alert):
-- SELECT product_name, current_stock, sum(units_sold) as total_sold
-- FROM analytics.inventory_turnover_mv
-- WHERE order_date >= today() - INTERVAL 7 DAY
-- GROUP BY product_name, current_stock
-- HAVING current_stock < total_sold * 2
-- ORDER BY current_stock ASC;


-- ────────────────────────────────────────────
-- 7. HOURLY ORDER TRENDS (Real-time monitoring)
-- ────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.hourly_order_trends_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(order_hour)
ORDER BY (order_hour, status)
POPULATE
AS SELECT
    toStartOfHour(created_at) as order_hour,
    status,
    count() as total_orders,
    sum(total_amount) as total_revenue,
    count(DISTINCT user_id) as unique_customers
FROM analytics.orders
GROUP BY order_hour, status;

-- Query example (Last 24h performance):
-- SELECT order_hour, sum(total_orders) as orders, sum(total_revenue) as revenue
-- FROM analytics.hourly_order_trends_mv
-- WHERE order_hour >= now() - INTERVAL 24 HOUR
-- GROUP BY order_hour
-- ORDER BY order_hour DESC;


-- ============================================
-- UTILITY QUERIES
-- ============================================

-- Optimize tables to apply ReplacingMergeTree deduplication
-- OPTIMIZE TABLE analytics.orders FINAL;
-- OPTIMIZE TABLE analytics.product_reviews FINAL;
-- OPTIMIZE TABLE analytics.categories FINAL;
-- OPTIMIZE TABLE analytics.products FINAL;

-- Check table sizes
-- SELECT table, formatReadableSize(sum(bytes)) as size
-- FROM system.parts
-- WHERE database = 'analytics' AND active
-- GROUP BY table
-- ORDER BY sum(bytes) DESC;
