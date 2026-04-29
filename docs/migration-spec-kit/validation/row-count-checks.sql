-- =============================================================================
-- Row Count Validation: Source (SQL Server) vs Target (Databricks)
-- Run these queries side-by-side to compare record counts
-- =============================================================================

-- ==========================================
-- CUSTOMERS
-- ==========================================

-- Source (run on SQL Server)
SELECT 'Source: dbo.Customer' AS check_name,
       COUNT(*) AS row_count
FROM [AdventureWorks].[dbo].[Customer];

SELECT 'Source: dbo.Customer (Active)' AS check_name,
       COUNT(*) AS row_count
FROM [AdventureWorks].[dbo].[Customer]
WHERE IsActive = 1;

SELECT 'Source: dim.Customer (Current)' AS check_name,
       COUNT(*) AS row_count
FROM [DW_Staging].[dim].[Customer]
WHERE IsCurrent = 1;

-- Target (run on Databricks)
-- SELECT 'Bronze: raw_customers' AS check_name,
--        COUNT(*) AS row_count
-- FROM migration_prod.bronze.raw_customers;
--
-- SELECT 'Silver: cleansed_customers' AS check_name,
--        COUNT(*) AS row_count
-- FROM migration_prod.silver.cleansed_customers;
--
-- SELECT 'Gold: dim_customer (Current)' AS check_name,
--        COUNT(*) AS row_count
-- FROM migration_prod.gold.dim_customer
-- WHERE is_current = true;


-- ==========================================
-- ORDERS
-- ==========================================

-- Source (run on SQL Server)
SELECT 'Source: dbo.Orders' AS check_name,
       COUNT(*) AS row_count
FROM [AdventureWorks].[dbo].[Orders];

SELECT 'Source: dbo.Orders by Year' AS check_name,
       YEAR(OrderDate) AS order_year,
       COUNT(*) AS row_count
FROM [AdventureWorks].[dbo].[Orders]
GROUP BY YEAR(OrderDate)
ORDER BY order_year;

SELECT 'Source: fact.Orders' AS check_name,
       COUNT(*) AS row_count
FROM [DW_Staging].[fact].[Orders];

-- Target (run on Databricks)
-- SELECT 'Bronze: raw_orders' AS check_name,
--        COUNT(*) AS row_count
-- FROM migration_prod.bronze.raw_orders;
--
-- SELECT 'Gold: fact_order' AS check_name,
--        COUNT(*) AS row_count
-- FROM migration_prod.gold.fact_order;
--
-- SELECT 'Gold: fact_order by Year' AS check_name,
--        order_year,
--        COUNT(*) AS row_count
-- FROM migration_prod.gold.fact_order
-- GROUP BY order_year
-- ORDER BY order_year;


-- ==========================================
-- AGGREGATE CHECKS
-- ==========================================

-- Source (SQL Server)
SELECT 'Source: Total Revenue' AS check_name,
       SUM(TotalAmount) AS total_amount,
       AVG(TotalAmount) AS avg_amount,
       MIN(TotalAmount) AS min_amount,
       MAX(TotalAmount) AS max_amount
FROM [AdventureWorks].[dbo].[Orders];

-- Target (Databricks)
-- SELECT 'Target: Total Revenue' AS check_name,
--        SUM(TotalAmount) AS total_amount,
--        AVG(TotalAmount) AS avg_amount,
--        MIN(TotalAmount) AS min_amount,
--        MAX(TotalAmount) AS max_amount
-- FROM migration_prod.gold.fact_order;
