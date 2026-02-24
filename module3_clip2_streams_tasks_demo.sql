-- Module 3, Demo 1: Incremental MERGE with Streams and Tasks
-- Source: bronze.employee_data_parquet (loaded by Snowpipe from Module 1)
-- Targets: silver.employee_data_stg, gold core models (via stored procedure)

-- Environment Setup
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

--USE DATABASE SNOW_ELT_COURSE;
-- Use existing warehouse for tasks
USE WAREHOUSE COMPUTE_WH;

-- Ensure schemas exist
SHOW SCHEMAS;

-- Create dedicated schema for tasks (all tasks in a task graph must be in same schema)
CREATE SCHEMA IF NOT EXISTS tasks;
USE SCHEMA tasks;

-- Verify prerequisite objects
SHOW TABLES IN SCHEMA bronze STARTS WITH 'EMPLOYEE_DATA_PARQUET';
SHOW TABLES IN SCHEMA silver STARTS WITH 'EMPLOYEE_DATA_STG';

-- Step 1: Reset tables (Snowpipe will repopulate after files are re-uploaded)
TRUNCATE TABLE bronze.employee_data_parquet;
TRUNCATE TABLE silver.employee_data_stg;
TRUNCATE TABLE gold.employee_demographics_by_department;
TRUNCATE TABLE gold.survey_results_by_department;

-- ================================================================
-- STREAM: Track new rows arriving into bronze
-- ================================================================
-- employee_data_parquet_stream
CREATE OR REPLACE STREAM bronze.employee_data_parquet_stream
ON TABLE bronze.employee_data_parquet;
select * from bronze.employee_data_parquet;
-- Inspect stream status
SHOW STREAMS LIKE 'EMPLOYEE_DATA_PARQUET_STREAM' IN SCHEMA bronze;
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.EMPLOYEE_DATA_PARQUET_STREAM') AS stream_has_data;

-- ================================================================
-- STREAM-AWARE STORED PROCEDURE: Incremental silver load from stream
-- ================================================================

-- Create a stream-aware version of the silver staging procedure using MERGE
-- Create a stream-aware version of the silver staging procedure using MERGE
CREATE OR REPLACE PROCEDURE silver.load_employee_staging_from_stream(etl_run_id STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO silver.employee_data_stg AS target
  USING (
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
      TRY_TO_NUMBER(TO_VARCHAR(PARSE_JSON(engagement_survey):satisfaction_score))::INT AS satisfaction_score,
      TRY_TO_NUMBER(TO_VARCHAR(PARSE_JSON(engagement_survey):work_life_balance_score))::INT AS work_life_balance_score,
      TRY_TO_NUMBER(TO_VARCHAR(PARSE_JSON(engagement_survey):career_growth_score))::INT AS career_growth_score,
      TRY_TO_NUMBER(TO_VARCHAR(PARSE_JSON(engagement_survey):communication_score))::INT AS communication_score,
      TRY_TO_NUMBER(TO_VARCHAR(PARSE_JSON(engagement_survey):teamwork_score))::INT AS teamwork_score,
      METADATA$ACTION,
      METADATA$ISUPDATE
    FROM bronze.employee_data_parquet_stream
  ) AS source
  ON target.employee_number = source.employee_number
  
  WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' THEN
    DELETE
    
  WHEN MATCHED AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = TRUE THEN
    UPDATE SET
      employee_name = source.employee_name,
      gender = source.gender,
      city = source.city,
      job_title = source.job_title,
      department = source.department,
      store_location = source.store_location,
      business_unit = source.business_unit,
      division = source.division,
      age = source.age,
      length_of_service = source.length_of_service,
      hours_absent = source.hours_absent,
      engagement_survey = source.engagement_survey,
      satisfaction_score = source.satisfaction_score,
      work_life_balance_score = source.work_life_balance_score,
      career_growth_score = source.career_growth_score,
      communication_score = source.communication_score,
      teamwork_score = source.teamwork_score,
      staged_at = CURRENT_TIMESTAMP(),
      source_object = 'bronze.employee_data_parquet_stream',
      etl_run_id = :etl_run_id
      
  WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
    INSERT (
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
      satisfaction_score,
      work_life_balance_score,
      career_growth_score,
      communication_score,
      teamwork_score,
      staged_at,
      source_object,
      etl_run_id
    )
    VALUES (
      source.employee_number,
      source.employee_name,
      source.gender,
      source.city,
      source.job_title,
      source.department,
      source.store_location,
      source.business_unit,
      source.division,
      source.age,
      source.length_of_service,
      source.hours_absent,
      source.engagement_survey,
      source.satisfaction_score,
      source.work_life_balance_score,
      source.career_growth_score,
      source.communication_score,
      source.teamwork_score,
      CURRENT_TIMESTAMP(),
      'bronze.employee_data_parquet_stream',
      :etl_run_id
    );
  
  RETURN 'Processed stream records (INSERT/UPDATE/DELETE) into silver.employee_data_stg with etl_run_id=' || :etl_run_id || '. Rows affected: ' || SQLROWCOUNT;
END;
$$;

-- ================================================================
-- TASKS: Incremental load into silver, then materialize gold
-- ================================================================

-- Pause any existing tasks to edit safely
ALTER TASK IF EXISTS tasks.t_gold_refresh_after_silver SUSPEND;
ALTER TASK IF EXISTS tasks.t_silver_from_bronze_stream SUSPEND;

-- Create/replace the silver load task (calls stored procedure for clean encapsulation)
CREATE OR REPLACE TASK tasks.t_silver_from_bronze_stream
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 6 * * * UTC'
  WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.EMPLOYEE_DATA_PARQUET_STREAM')
AS
CALL silver.load_employee_staging_from_stream(UUID_STRING());

-- Create/replace the gold refresh task (runs after silver task)
CREATE OR REPLACE TASK tasks.t_gold_refresh_after_silver
  WAREHOUSE = COMPUTE_WH
  AFTER tasks.t_silver_from_bronze_stream
AS
CALL gold.materialize_core_models();

-- Verify tasks
SHOW TASKS LIKE 'T_SILVER_FROM_BRONZE_STREAM' IN SCHEMA tasks;
SHOW TASKS LIKE 'T_GOLD_REFRESH_AFTER_SILVER' IN SCHEMA tasks;

-- ================================================================
-- DEMO RUN SEQUENCE
-- 1) Reset bronze to simulate a fresh load, then re-upload parquet files to @raw_zone/parquet/ and refresh the pipe
-- 2) Resume tasks and optionally execute on-demand
-- 3) Inspect silver and gold results and task history
-- ===============================================================

-- re-upload the files
/* remove them if rerunning the demo
REMOVE @bronze.raw_zone/parquet/employee_data_part_01.parquet;
REMOVE @bronze.raw_zone/parquet/employee_data_part_02.parquet;
REMOVE @bronze.raw_zone/parquet/employee_data_part_03.parquet;
REMOVE @bronze.raw_zone/parquet/employee_data_part_04.parquet;
REMOVE @bronze.raw_zone/parquet/employee_data_part_05.parquet;
*/
LIST @bronze.raw_zone/PARQUET;

-- Modify the pipe to refer to the schema
CREATE OR REPLACE PIPE bronze.employee_parquet_pipe
  AUTO_INGEST = FALSE  -- Set to FALSE for manual demo control
  AS
  COPY INTO bronze.employee_data_parquet
  FROM @bronze.raw_zone/PARQUET/
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  FILE_FORMAT = (TYPE = 'PARQUET')
  PATTERN = '.*employee_data_part_.*[.]parquet';

-- Get pipe details 
SELECT SYSTEM$PIPE_STATUS('bronze.employee_parquet_pipe');

ALTER PIPE bronze.employee_parquet_pipe REFRESH; 

-- Monitor Snowpipe execution history
-- Check what files have been loaded
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'BRONZE.EMPLOYEE_DATA_PARQUET',
    START_TIME => DATEADD(MINUTE, -10, CURRENT_TIMESTAMP())
));

SELECT * FROM bronze.employee_data_parquet;

SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.EMPLOYEE_DATA_PARQUET_STREAM') AS stream_has_data;

-- Step 2: Resume tasks and optionally trigger an immediate run
ALTER TASK tasks.t_gold_refresh_after_silver RESUME;
ALTER TASK tasks.t_silver_from_bronze_stream RESUME;

-- Manually execute (for demo control); tasks also run on their schedule
EXECUTE TASK tasks.t_silver_from_bronze_stream;

-- Step 3: Inspect results
SELECT * FROM silver.employee_data_stg;
SELECT * FROM gold.employee_demographics_by_department;
SELECT * FROM gold.survey_results_by_department;

-- View task history for our demo tasks only
SELECT 
  NAME,
  DATABASE_NAME,
  SCHEMA_NAME,
  SCHEDULED_TIME,
  COMPLETED_TIME,
  STATE,
  RETURN_VALUE,
  ERROR_CODE,
  ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
  SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -2, CURRENT_TIMESTAMP()),
  RESULT_LIMIT => 50
))
WHERE DATABASE_NAME = 'SNOW_ELT_COURSE'
  AND SCHEMA_NAME = 'TASKS'
  AND NAME IN ('T_SILVER_FROM_BRONZE_STREAM', 'T_GOLD_REFRESH_AFTER_SILVER')
ORDER BY SCHEDULED_TIME DESC;

-- Pause tasks after the demo
ALTER TASK tasks.t_silver_from_bronze_stream SUSPEND;
ALTER TASK tasks.t_gold_refresh_after_silver SUSPEND;



