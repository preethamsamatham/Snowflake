-- Module 4, Demo 2: Tracking Data Quality
-- Demonstrates stored procedures to check nulls and range anomalies and log results
-- The stored procedure approach can be implemented on any edition of Snowflake
-- as opposed to Data Metric Functions which are Enterprise+ only

-- Environment Setup
USE ROLE SYSADMIN;
USE DATABASE SNOW_ELT_COURSE;
USE WAREHOUSE COMPUTE_WH;

USE SCHEMA ops;

CREATE OR REPLACE TABLE ops.data_quality_results (
  check_name STRING,
  layer STRING,
  table_name STRING,
  issue_count NUMBER,
  sample_details VARIANT,
  checked_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ================================================================
-- STORED PROCEDURE: Check for nulls in bronze.employee_number
-- ================================================================

CREATE OR REPLACE PROCEDURE ops.check_nulls_in_bronze_employee_number()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  v_issue_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO :v_issue_count
  FROM bronze.employee_data_parquet
  WHERE employee_number IS NULL;

  INSERT INTO ops.data_quality_results (check_name, layer, table_name, issue_count, sample_details)
  SELECT 'null_employee_number', 'bronze', 'bronze.employee_data_parquet', :v_issue_count,
         OBJECT_CONSTRUCT('sample_rows', ARRAY_AGG(OBJECT_CONSTRUCT('employee_name', employee_name)) )
  FROM (
    SELECT employee_name
    FROM bronze.employee_data_parquet
    WHERE employee_number IS NULL
    LIMIT 10
  );

  RETURN 'Null check complete. Issues=' || :v_issue_count;
END;
$$;

-- ================================================================
-- STORED PROCEDURE: Check engagement_survey score ranges in silver
-- Valid range assumed 1..5 for each score
-- ================================================================

CREATE OR REPLACE PROCEDURE ops.check_silver_survey_ranges()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  v_issue_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO :v_issue_count
  FROM silver.employee_data_stg
  WHERE COALESCE(satisfaction_score, 0) NOT BETWEEN 1 AND 5
     OR COALESCE(work_life_balance_score, 0) NOT BETWEEN 1 AND 5
     OR COALESCE(career_growth_score, 0) NOT BETWEEN 1 AND 5
     OR COALESCE(communication_score, 0) NOT BETWEEN 1 AND 5
     OR COALESCE(teamwork_score, 0) NOT BETWEEN 1 AND 5;

  INSERT INTO ops.data_quality_results (check_name, layer, table_name, issue_count, sample_details)
  SELECT 'invalid_survey_range', 'silver', 'silver.employee_data_stg', :v_issue_count,
         OBJECT_CONSTRUCT('bad_rows', ARRAY_AGG(OBJECT_CONSTRUCT(
           'employee_number', employee_number,
           'department', department,
           'satisfaction', satisfaction_score,
           'work_life', work_life_balance_score,
           'career', career_growth_score,
           'communication', communication_score,
           'teamwork', teamwork_score,
           'staged_at', staged_at,
           'source_object', source_object,
           'etl_run_id', etl_run_id
         )))
  FROM (
    SELECT employee_number, department, satisfaction_score, work_life_balance_score,
           career_growth_score, communication_score, teamwork_score,
           staged_at, source_object, etl_run_id
    FROM silver.employee_data_stg
    WHERE COALESCE(satisfaction_score, 0) NOT BETWEEN 1 AND 5
       OR COALESCE(work_life_balance_score, 0) NOT BETWEEN 1 AND 5
       OR COALESCE(career_growth_score, 0) NOT BETWEEN 1 AND 5
       OR COALESCE(communication_score, 0) NOT BETWEEN 1 AND 5
       OR COALESCE(teamwork_score, 0) NOT BETWEEN 1 AND 5
    LIMIT 10
  );

  RETURN 'Survey range check complete. Issues=' || :v_issue_count;
END;
$$;

-- ================================================================
-- DEMO FLOW
-- 1) Ensure silver is populated (run staging + gold procs if needed)
-- 2) Intentionally modify some rows to create data quality issues (for demo)
-- 3) Run the procedures and inspect results
-- ================================================================

-- Run checks
CALL ops.check_nulls_in_bronze_employee_number();
CALL ops.check_silver_survey_ranges();

select * from silver.employee_data_stg;
-- Inspect data quality results
SELECT * FROM ops.data_quality_results ORDER BY checked_at DESC LIMIT 50;

-- Intentionally inject a couple of bad values (for demo reset after)
UPDATE silver.employee_data_stg SET satisfaction_score = 99 WHERE employee_number 
IN (SELECT employee_number FROM silver.employee_data_stg LIMIT 1);
UPDATE bronze.employee_data_parquet SET employee_number = NULL WHERE employee_number 
IN (SELECT employee_number FROM bronze.employee_data_parquet LIMIT 1);

-- Run checks
CALL ops.check_nulls_in_bronze_employee_number();
CALL ops.check_silver_survey_ranges();

-- Inspect data quality results
SELECT * FROM ops.data_quality_results ORDER BY checked_at DESC LIMIT 50;