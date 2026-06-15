-- ============================================
-- Disable the cache for this session
-- Ensures Query Profile reflects full execution
-- ============================================
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- ============================================
-- Context Setup for Tasty Bytes Queries
-- ============================================
USE ROLE SNOWFLAKE_LEARNING_ROLE;
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;

-- Dynamically set schema based on current user
SET schema_name = CONCAT(current_user(), '_LOAD_SAMPLE_DATA_FROM_S3');
USE SCHEMA IDENTIFIER($schema_name);

-- ============================================
-- Stage 1: Scan
-- Reads all rows from the MENU table
-- ============================================
SELECT * 
FROM menu;

-- ============================================
-- Stage 2: Filter
-- Adds a WHERE clause to reduce row count
-- ============================================
SELECT * 
FROM menu
WHERE truck_brand_name = 'Freezing Point';

-- ============================================
-- Stage 3: Join (Self Join)
-- Joins MENU table to itself on menu_type_id
-- ============================================
SELECT
    a.menu_item_name,
    b.item_category
FROM menu a
JOIN menu b
  ON a.menu_type_id = b.menu_type_id
WHERE a.truck_brand_name = 'Freezing Point';

-- ============================================
-- Stage 4: Aggregate
-- Groups by brand and counts menu items
-- ============================================
SELECT
    truck_brand_name,
    COUNT(*) AS item_count
FROM menu
GROUP BY truck_brand_name;

-- ============================================
-- Context Setup for TPCH Join + Aggregate Query
-- (Uses built-in Snowflake Sample Database)
-- ============================================
USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCH_SF1;

-- ============================================
-- Stage 5: Join and Aggregate from TPCH dataset
-- Combines JOIN, FILTER, GROUP BY, and ORDER BY
-- Useful to explore complex query plans
-- ============================================
SELECT
    o.o_orderpriority,
    COUNT(*) AS order_count
FROM ORDERS o
JOIN LINEITEM l
  ON o.o_orderkey = l.l_orderkey
WHERE o.o_orderstatus = 'F'
  AND l.l_shipdate > o.o_orderdate
GROUP BY o.o_orderpriority
ORDER BY order_count DESC;
