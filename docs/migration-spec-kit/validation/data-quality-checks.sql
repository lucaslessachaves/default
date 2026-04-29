-- =============================================================================
-- Data Quality Checks — Run on Databricks after migration
-- =============================================================================

-- ==========================================
-- 1. NULL CHECKS
-- ==========================================

-- Customers: required fields should not be NULL
SELECT 'dim_customer NULL check' AS check_name,
       SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
       SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) AS null_customer_name,
       SUM(CASE WHEN is_current IS NULL THEN 1 ELSE 0 END) AS null_is_current,
       SUM(CASE WHEN record_hash IS NULL THEN 1 ELSE 0 END) AS null_record_hash
FROM migration_prod.gold.dim_customer;

-- Orders: required fields should not be NULL
SELECT 'fact_order NULL check' AS check_name,
       SUM(CASE WHEN OrderID IS NULL THEN 1 ELSE 0 END) AS null_order_id,
       SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS null_customer_key,
       SUM(CASE WHEN TotalAmount IS NULL THEN 1 ELSE 0 END) AS null_total_amount,
       SUM(CASE WHEN Status IS NULL THEN 1 ELSE 0 END) AS null_status
FROM migration_prod.gold.fact_order;

-- ==========================================
-- 2. DUPLICATE CHECKS
-- ==========================================

-- dim_customer: only one current record per CustomerID
SELECT 'dim_customer duplicate current check' AS check_name,
       customer_id,
       COUNT(*) AS current_count
FROM migration_prod.gold.dim_customer
WHERE is_current = true
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- fact_order: OrderID should be unique
SELECT 'fact_order duplicate check' AS check_name,
       OrderID,
       COUNT(*) AS dupe_count
FROM migration_prod.gold.fact_order
GROUP BY OrderID
HAVING COUNT(*) > 1;

-- ==========================================
-- 3. REFERENTIAL INTEGRITY
-- ==========================================

-- fact_order: every customer_key should exist in dim_customer
SELECT 'fact_order orphan customer_key' AS check_name,
       COUNT(*) AS orphan_count
FROM migration_prod.gold.fact_order f
LEFT JOIN migration_prod.gold.dim_customer d
  ON f.customer_key = d.customer_key
WHERE d.customer_key IS NULL;

-- fact_order: every order_date_key should exist in dim_date
SELECT 'fact_order orphan order_date_key' AS check_name,
       COUNT(*) AS orphan_count
FROM migration_prod.gold.fact_order f
LEFT JOIN migration_prod.gold.dim_date d
  ON f.order_date_key = d.date_key
WHERE f.order_date_key IS NOT NULL AND d.date_key IS NULL;

-- ==========================================
-- 4. RANGE / BUSINESS RULE CHECKS
-- ==========================================

-- Amounts should be non-negative
SELECT 'fact_order negative amounts' AS check_name,
       SUM(CASE WHEN TotalAmount < 0 THEN 1 ELSE 0 END) AS negative_total,
       SUM(CASE WHEN TaxAmount < 0 THEN 1 ELSE 0 END) AS negative_tax,
       SUM(CASE WHEN NetAmount < 0 THEN 1 ELSE 0 END) AS negative_net
FROM migration_prod.gold.fact_order;

-- SCD2: effective dates should be valid
SELECT 'dim_customer invalid SCD dates' AS check_name,
       COUNT(*) AS invalid_count
FROM migration_prod.gold.dim_customer
WHERE effective_start_date > effective_end_date;

-- OrderDate should be within reasonable range
SELECT 'fact_order date range check' AS check_name,
       MIN(order_year) AS min_year,
       MAX(order_year) AS max_year,
       COUNT(CASE WHEN order_year < 2000 OR order_year > 2030 THEN 1 END) AS out_of_range
FROM migration_prod.gold.fact_order;

-- ==========================================
-- 5. SCD TYPE 2 INTEGRITY
-- ==========================================

-- Check that history chain is continuous (no gaps)
SELECT 'dim_customer SCD2 gap check' AS check_name,
       c1.customer_id,
       c1.effective_end_date AS prev_end,
       c2.effective_start_date AS next_start
FROM migration_prod.gold.dim_customer c1
INNER JOIN migration_prod.gold.dim_customer c2
  ON c1.customer_id = c2.customer_id
  AND c1.effective_end_date < c2.effective_start_date
  AND c1.is_current = false
WHERE NOT EXISTS (
    SELECT 1 FROM migration_prod.gold.dim_customer c3
    WHERE c3.customer_id = c1.customer_id
      AND c3.effective_start_date > c1.effective_end_date
      AND c3.effective_start_date < c2.effective_start_date
);
