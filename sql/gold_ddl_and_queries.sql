-- ================================================================
-- RETAIL LAKEHOUSE — GOLD LAYER DDL SCRIPTS
-- Microsoft Fabric / Lakehouse SQL Analytics Endpoint
-- ================================================================
-- Run these in the Fabric Lakehouse SQL Analytics Endpoint
-- after the Gold PySpark notebooks have populated the Delta tables

-- ================================================================
-- 1. DIM_DATE — Calendar Dimension
-- ================================================================
CREATE TABLE IF NOT EXISTS gold.dim_date
(
    date_key        INT           NOT NULL,   -- e.g. 20230115
    full_date       DATE          NOT NULL,
    year            INT           NOT NULL,
    quarter         INT           NOT NULL,   -- 1..4
    month_num       INT           NOT NULL,   -- 1..12
    month_name      VARCHAR(20)   NOT NULL,   -- January
    month_short     VARCHAR(5)    NOT NULL,   -- Jan
    week_of_year    INT           NOT NULL,
    day_of_month    INT           NOT NULL,
    day_of_week     INT           NOT NULL,   -- 1=Sunday..7=Saturday
    day_name        VARCHAR(15)   NOT NULL,   -- Monday
    is_weekend      BOOLEAN       NOT NULL,
    year_month      VARCHAR(8)    NOT NULL,   -- 2023-01
    year_quarter    VARCHAR(8)    NOT NULL    -- 2023-Q1
)
USING DELTA
COMMENT 'Calendar dimension spanning 2020-2026';

-- ================================================================
-- 2. DIM_CUSTOMER — SCD Type-2 Customer Dimension
-- ================================================================
CREATE TABLE IF NOT EXISTS gold.dim_customer
(
    customer_key          VARCHAR(16)   NOT NULL,   -- Surrogate key (SHA2 hash)
    customer_id           VARCHAR(10)   NOT NULL,   -- Natural key (e.g. CUST0001)
    full_name             VARCHAR(100),
    first_name            VARCHAR(50),
    last_name             VARCHAR(50),
    email                 VARCHAR(100),
    email_domain          VARCHAR(50),
    city                  VARCHAR(50),
    segment               VARCHAR(20),             -- Premium / Standard / Budget
    signup_date           DATE,
    is_active             BOOLEAN,
    customer_tenure_days  INT,
    -- SCD Type-2 fields
    effective_start_date  DATE          NOT NULL,
    effective_end_date    DATE          NOT NULL,   -- 9999-12-31 = active record
    is_current            BOOLEAN       NOT NULL,
    -- Audit
    dw_created_at         TIMESTAMP,
    dw_updated_at         TIMESTAMP
)
USING DELTA
COMMENT 'Customer dimension with SCD Type-2 history tracking for city and segment changes';

-- ================================================================
-- 3. DIM_PRODUCT — Product Dimension
-- ================================================================
CREATE TABLE IF NOT EXISTS gold.dim_product
(
    product_key        VARCHAR(16)    NOT NULL,   -- Surrogate key
    product_id         VARCHAR(10)    NOT NULL,   -- Natural key (e.g. PROD0001)
    product_name       VARCHAR(100),
    category           VARCHAR(50),               -- Electronics, Clothing, etc.
    sub_category       VARCHAR(50),
    unit_price         DECIMAL(10,2),
    cost_price         DECIMAL(10,2),
    gross_margin_pct   DECIMAL(5,2),              -- (price - cost) / price * 100
    price_tier         VARCHAR(20),               -- Budget / Mid-Range / Premium
    supplier           VARCHAR(50),
    in_stock           BOOLEAN,
    dw_created_at      TIMESTAMP
)
USING DELTA
COMMENT 'Product dimension with pricing and margin attributes';

-- ================================================================
-- 4. DIM_STORE — Store Dimension
-- ================================================================
CREATE TABLE IF NOT EXISTS gold.dim_store
(
    store_key         VARCHAR(16)    NOT NULL,
    store_id          VARCHAR(10)    NOT NULL,
    store_name        VARCHAR(100),
    store_type        VARCHAR(20),               -- Physical / Online
    city              VARCHAR(50),
    region            VARCHAR(20),
    open_date         DATE,
    store_age_days    INT,
    is_online         BOOLEAN,
    dw_created_at     TIMESTAMP
)
USING DELTA
COMMENT 'Store dimension with physical and online channel attributes';

-- ================================================================
-- 5. FACT_SALES — Core Fact Table
-- ================================================================
CREATE TABLE IF NOT EXISTS gold.fact_sales
(
    sales_key          VARCHAR(16)   NOT NULL,    -- Degenerate key
    transaction_id     VARCHAR(15)   NOT NULL,    -- Source system key
    -- Foreign keys
    customer_key       VARCHAR(16),               -- → dim_customer
    product_key        VARCHAR(16),               -- → dim_product
    store_key          VARCHAR(16),               -- → dim_store
    date_key           INT,                       -- → dim_date
    -- Degenerate dimensions
    transaction_date   DATE,
    txn_year           INT,
    txn_month          INT,
    txn_quarter        INT,
    payment_method     VARCHAR(20),
    source_system      VARCHAR(20),               -- POS / WebApp / MobileApp
    status             VARCHAR(20),
    is_returned        BOOLEAN,
    -- Measures
    quantity           INT,
    unit_price         DECIMAL(10,2),
    discount_pct       DECIMAL(5,2),
    discount_amount    DECIMAL(10,2),
    gross_revenue      DECIMAL(12,2),
    total_amount       DECIMAL(12,2),
    -- Audit
    dw_created_at      TIMESTAMP
)
USING DELTA
COMMENT 'Retail sales fact table with 500K+ transactions';

-- ================================================================
-- USEFUL ANALYTICAL QUERIES
-- ================================================================

-- Monthly Revenue Trend
SELECT
    d.year_month,
    SUM(f.total_amount)        AS revenue,
    COUNT(f.transaction_id)    AS transactions,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    AVG(f.total_amount)        AS avg_order_value
FROM gold.fact_sales f
JOIN gold.dim_date d ON f.date_key = d.date_key
WHERE f.is_returned = FALSE
GROUP BY d.year_month
ORDER BY d.year_month;

-- Revenue by Category
SELECT
    p.category,
    p.price_tier,
    SUM(f.total_amount)     AS revenue,
    SUM(f.quantity)         AS units_sold,
    AVG(f.discount_pct)*100 AS avg_discount_pct
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.product_key = p.product_key
WHERE f.is_returned = FALSE
GROUP BY p.category, p.price_tier
ORDER BY revenue DESC;

-- Top 10 Customers by Revenue
SELECT
    c.customer_id,
    c.full_name,
    c.segment,
    c.city,
    COUNT(f.transaction_id) AS total_orders,
    SUM(f.total_amount)     AS lifetime_value,
    AVG(f.total_amount)     AS avg_order_value
FROM gold.fact_sales f
JOIN gold.dim_customer c ON f.customer_key = c.customer_key
WHERE f.is_returned = FALSE AND c.is_current = TRUE
GROUP BY c.customer_id, c.full_name, c.segment, c.city
ORDER BY lifetime_value DESC
LIMIT 10;

-- Return Rate by Product Category
SELECT
    p.category,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN f.is_returned THEN 1 ELSE 0 END) AS returned,
    ROUND(SUM(CASE WHEN f.is_returned THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS return_rate_pct
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY return_rate_pct DESC;

-- Online vs Physical Store Revenue
SELECT
    s.store_type,
    s.region,
    COUNT(DISTINCT f.transaction_id) AS transactions,
    SUM(f.total_amount)              AS revenue
FROM gold.fact_sales f
JOIN gold.dim_store s ON f.store_key = s.store_key
WHERE f.is_returned = FALSE
GROUP BY s.store_type, s.region
ORDER BY store_type, revenue DESC;

-- SCD Type-2: View Customer History
SELECT
    customer_id,
    full_name,
    city,
    segment,
    effective_start_date,
    effective_end_date,
    is_current
FROM gold.dim_customer
WHERE customer_id = 'CUST0001'
ORDER BY effective_start_date;
