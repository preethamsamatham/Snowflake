-- ============================================
-- Disable result cache so Profile shows full work
-- ============================================
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- ============================================
-- Context Setup
-- ============================================
USE ROLE SNOWFLAKE_LEARNING_ROLE;
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;

-- create and switch into a fixed schema called PS
CREATE SCHEMA IF NOT EXISTS PS;
USE SCHEMA PS;

-- ============================================
-- Stage 1: Prepare Demo Tables (using CTAS)
--   • orders_uncl — copy of the shared TPCH_SF1.ORDERS
--   • orders_cl   — a second copy to cluster
-- ============================================
CREATE OR REPLACE TABLE orders_uncl AS
  SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;

CREATE OR REPLACE TABLE orders_cl AS
  SELECT * FROM orders_uncl;

-- ============================================
-- Stage 2: Query Unclustered Table
--   • full micro-partition scan on orders_uncl  
-- ============================================
SELECT
  o_orderdate,
  COUNT(*) AS order_count
FROM orders_uncl
WHERE o_orderdate BETWEEN '1993-01-01' AND '1993-03-31'
GROUP BY o_orderdate
ORDER BY o_orderdate;

--   → Note History metrics: bytes scanned, execution time

-- ============================================
-- Stage 3: Define Clustering Key on orders_cl
--   • cluster by order date to enable partition pruning  
-- ============================================
ALTER TABLE orders_cl  
  CLUSTER BY (o_orderdate);

--   • Optional: inspect clustering info  
SELECT SYSTEM$CLUSTERING_INFORMATION('ORDERS_CL') AS clustering_info;

-- ============================================
-- Stage 4: Query Clustered Table
--   • now Snowflake prunes micro-partitions by date filter  
-- ============================================
SELECT
  o_orderdate,
  COUNT(*) AS order_count
FROM orders_cl
WHERE o_orderdate BETWEEN '1993-01-01' AND '1993-03-31'
GROUP BY o_orderdate
ORDER BY o_orderdate;

--   → Compare History metrics against Stage 2 to see reduced bytes scanned and faster execution  
