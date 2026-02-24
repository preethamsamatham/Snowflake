-- Module 3, Demo 2: Incremental MERGE with Dynamic Tables
-- Source: bronze.employee_data_parquet
-- Targets: silver and gold dynamic tables providing equivalent outcomes

-- Environment Setup
USE ROLE SYSADMIN;
USE DATABASE SNOW_ELT_COURSE;

-- Use existing warehouse for dynamic tables compute
USE WAREHOUSE COMPUTE_WH;

-- Step 1: Reset tables for demo
TRUNCATE TABLE bronze.employee_data_parquet;
-- ================================================================
-- DYNAMIC TABLES: Define refreshable pipeline objects
-- ================================================================

-- Silver-equivalent dynamic table parsing engagement_survey
CREATE OR REPLACE DYNAMIC TABLE silver.dt_employee_data_stg
  TARGET_LAG = '5 minutes'
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = AUTO
AS
SELECT
  employee_number,
  employee_name,
  gender,
  city,
  job_title,
  department,
  store_location,
  business_unit,
  division,
  age,
  length_of_service,
  hours_absent,
  engagement_survey,
  -- Parse engagement_survey from string representation to JSON before extracting fields
  TRY_TO_NUMBER(TO_VARCHAR(PARSE_JSON(engagement_survey):satisfaction_score))::INT AS satisfaction_score,
  TRY_TO_NUMBER(TO_VARCHAR(PARSE_JSON(engagement_survey):work_life_balance_score))::INT AS work_life_balance_score,
  TRY_TO_NUMBER(TO_VARCHAR(PARSE_JSON(engagement_survey):career_growth_score))::INT AS career_growth_score,
  TRY_TO_NUMBER(TO_VARCHAR(PARSE_JSON(engagement_survey):communication_score))::INT AS communication_score,
  TRY_TO_NUMBER(TO_VARCHAR(PARSE_JSON(engagement_survey):teamwork_score))::INT AS teamwork_score,
  -- Metadata columns for data lineage and auditing
  CURRENT_TIMESTAMP() AS staged_at,
  'bronze.employee_data_parquet' AS source_object,
  'dt_refresh' AS etl_process_type
FROM bronze.employee_data_parquet;

-- Gold-equivalent dynamic table: demographics by department (CTE inlined)
CREATE OR REPLACE DYNAMIC TABLE gold.dt_employee_demographics_by_department
  TARGET_LAG = '5 minutes'
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = AUTO
AS
SELECT
  department,
  COUNT(*) AS num_employees,
  AVG(age::NUMBER) AS avg_age,
  AVG(length_of_service::NUMBER) AS avg_length_of_service,
  SUM(IFF(UPPER(gender) = 'MALE', 1, 0)) AS num_male,
  SUM(IFF(UPPER(gender) = 'FEMALE', 1, 0)) AS num_female,
  SUM(IFF(UPPER(gender) NOT IN ('MALE','FEMALE'), 1, 0)) AS num_other_gender,
  -- Metadata columns for data lineage and auditing
  CURRENT_TIMESTAMP() AS materialized_at,
  'silver.dt_employee_data_stg' AS source_object,
  'dt_refresh' AS etl_process_type
FROM silver.dt_employee_data_stg
GROUP BY department;

-- Gold-equivalent dynamic table: survey results by department
CREATE OR REPLACE DYNAMIC TABLE gold.dt_survey_results_by_department
  TARGET_LAG = '5 minutes'
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = AUTO
AS
SELECT
  department,
  AVG(NULLIFZERO(satisfaction_score)) AS avg_satisfaction_score,
  AVG(NULLIFZERO(work_life_balance_score)) AS avg_work_life_balance_score,
  AVG(NULLIFZERO(career_growth_score)) AS avg_career_growth_score,
  AVG(NULLIFZERO(communication_score)) AS avg_communication_score,
  AVG(NULLIFZERO(teamwork_score)) AS avg_teamwork_score,
  COUNT(*) AS num_responses,
  -- Metadata columns for data lineage and auditing
  CURRENT_TIMESTAMP() AS materialized_at,
  'silver.dt_employee_data_stg' AS source_object,
  'dt_refresh' AS etl_process_type
FROM silver.dt_employee_data_stg
GROUP BY department;

-- ================================================================
-- DEMO RUN SEQUENCE
-- 1) Truncate bronze, re-upload files and refresh the pipe
-- 2) Manually REFRESH the dynamic tables to show propagation
-- 3) Inspect results
-- ================================================================

-- Recreate the pipe to clear the COPY history
-- Modify the pipe to refer to the schema
CREATE OR REPLACE PIPE bronze.employee_parquet_pipe
  AUTO_INGEST = FALSE  -- Set to FALSE for manual demo control
  AS
  COPY INTO bronze.employee_data_parquet
  FROM @bronze.raw_zone/parquet/
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  FILE_FORMAT = (TYPE = 'PARQUET')
  PATTERN = '.*employee_data_part_.*[.]parquet';

-- Re-upload the files and refresh the pipe (following same pattern as streams demo)
-- LIST @bronze.raw_zone/parquet;
ALTER PIPE bronze.employee_parquet_pipe REFRESH;

-- Wait for data to load
SELECT * FROM bronze.employee_data_parquet;

-- Step 2: Force refresh for demo
ALTER DYNAMIC TABLE silver.dt_employee_data_stg REFRESH;
ALTER DYNAMIC TABLE gold.dt_employee_demographics_by_department REFRESH;
ALTER DYNAMIC TABLE gold.dt_survey_results_by_department REFRESH;

-- Step 3: Inspect results
SELECT * FROM silver.dt_employee_data_stg;
SELECT * FROM gold.dt_employee_demographics_by_department;
SELECT * FROM gold.dt_survey_results_by_department;

-- ================================================================
-- DYNAMIC TABLE MONITORING: Track performance and status
-- Based on Snowflake documentation: https://docs.snowflake.com/en/user-guide/dynamic-tables-monitor
-- ================================================================

-- Monitor 1: List all dynamic tables in our demo with basic info
SHOW DYNAMIC TABLES LIKE 'DT_%' IN DATABASE SNOW_ELT_COURSE;

-- View the dependency graph of our dynamic tables
SELECT
  name,
  database_name,
  schema_name,
  target_lag_type,
  target_lag_sec,
  inputs,
  scheduling_state
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY())
WHERE name LIKE 'DT_%'
ORDER BY name;



-- Suspend all demo dynamic tables to stop automatic refreshes
ALTER DYNAMIC TABLE silver.dt_employee_data_stg SUSPEND;
ALTER DYNAMIC TABLE gold.dt_employee_demographics_by_department SUSPEND;
ALTER DYNAMIC TABLE gold.dt_survey_results_by_department SUSPEND;

