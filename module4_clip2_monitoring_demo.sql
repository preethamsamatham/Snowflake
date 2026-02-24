-- Module 4, Demo 1: Monitoring ELT Activity with Streams and Tasks Integration
-- Demonstrates comprehensive logging integrated with the real streams/tasks pipeline from Module 3
-- Uses existing stored procedures from Module 2 with logging wrappers

-- Environment Setup
USE ROLE SYSADMIN;
USE DATABASE SNOW_ELT_COURSE;
USE WAREHOUSE COMPUTE_WH;

-- ================================================================
-- EVENT TABLE and LOGGING INFRASTRUCTURE
-- ================================================================
-- Create operations schema for monitoring
CREATE SCHEMA IF NOT EXISTS ops;

-- Create event table to capture custom pipeline logs
CREATE OR REPLACE EVENT TABLE ops.pipeline_events;

-- Associate event table with database for automatic telemetry collection
ALTER DATABASE SNOW_ELT_COURSE SET EVENT_TABLE = SNOW_ELT_COURSE.ops.pipeline_events;

-- Enable comprehensive logging and tracing
ALTER DATABASE SNOW_ELT_COURSE SET LOG_LEVEL = 'INFO';
ALTER DATABASE SNOW_ELT_COURSE SET TRACE_LEVEL = 'ALWAYS';

-- Step 1: Reset tables (Snowpipe will repopulate after files are re-uploaded)
TRUNCATE TABLE bronze.employee_data_parquet;
TRUNCATE TABLE silver.employee_data_stg;
TRUNCATE TABLE gold.employee_demographics_by_department;
TRUNCATE TABLE gold.survey_results_by_department;

-- ================================================================
-- STREAM: Track new rows arriving into bronze
-- ================================================================

CREATE OR REPLACE STREAM bronze.employee_data_parquet_stream
ON TABLE bronze.employee_data_parquet;

-- Inspect stream status
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.EMPLOYEE_DATA_PARQUET_STREAM') AS stream_has_data;


-- ================================================================
-- LOGGED WRAPPER PROCEDURES: wrappers around existing Module 2 procedures
-- ================================================================

-- Logged wrapper for the existing silver staging procedure from Module 2
CREATE OR REPLACE PROCEDURE silver.load_employee_staging_logged(etl_run_id STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  result_msg STRING;
  start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
  -- Log pipeline stage start
  SYSTEM$LOG('INFO', OBJECT_CONSTRUCT(
    'component', 'ELT_Pipeline',
    'run_id', :etl_run_id,
    'pipeline', 'streams_tasks_elt',
    'stage', 'silver_load',
    'status', 'STARTED',
    'source', 'bronze.employee_data_parquet',
    'target', 'silver.employee_data_stg',
    'procedure', 'silver.load_employee_staging'
  ));

  -- Call the original procedure from Module 2
  CALL silver.load_employee_staging(:etl_run_id) INTO :result_msg;

  -- Log successful completion with metrics
  SYSTEM$LOG('INFO', OBJECT_CONSTRUCT(
    'component', 'ELT_Pipeline',
    'run_id', :etl_run_id,
    'pipeline', 'streams_tasks_elt',
    'stage', 'silver_load',
    'status', 'SUCCESS',
    'duration_seconds', DATEDIFF(SECOND, :start_time, CURRENT_TIMESTAMP()),
    'result', :result_msg,
    'procedure', 'silver.load_employee_staging'
  ));

  RETURN :result_msg;

EXCEPTION
  WHEN OTHER THEN
    -- Log failure with error details
    SYSTEM$LOG('ERROR', OBJECT_CONSTRUCT(
      'component', 'ELT_Pipeline',
      'run_id', :etl_run_id,
      'pipeline', 'streams_tasks_elt',
      'stage', 'silver_load',
      'status', 'FAILED',
      'error_code', :SQLCODE,
      'error_message', :SQLERRM,
      'procedure', 'silver.load_employee_staging'
    ));
    
    RETURN 'ERROR in silver load: ' || :SQLERRM;
END;
$$;

-- Logged wrapper for the existing gold materialization procedure from Module 2
CREATE OR REPLACE PROCEDURE gold.materialize_core_models_logged(etl_run_id STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  result_msg STRING;
  start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
  -- Log pipeline stage start
  SYSTEM$LOG('INFO', OBJECT_CONSTRUCT(
    'component', 'ELT_Pipeline',
    'run_id', :etl_run_id,
    'pipeline', 'streams_tasks_elt',
    'stage', 'gold_materialize',
    'status', 'STARTED',
    'source', 'silver.employee_data_stg',
    'target', 'gold.employee_demographics_by_department,gold.survey_results_by_department',
    'procedure', 'gold.materialize_core_models'
  ));

  -- Call the original procedure from Module 2
  CALL gold.materialize_core_models() INTO :result_msg;

  -- Log successful completion with metrics
  SYSTEM$LOG('INFO', OBJECT_CONSTRUCT(
    'component', 'ELT_Pipeline',
    'run_id', :etl_run_id,
    'pipeline', 'streams_tasks_elt',
    'stage', 'gold_materialize',
    'status', 'SUCCESS',
    'duration_seconds', DATEDIFF(SECOND, :start_time, CURRENT_TIMESTAMP()),
    'result', :result_msg,
    'procedure', 'gold.materialize_core_models'
  ));

  RETURN :result_msg;

EXCEPTION
  WHEN OTHER THEN
    -- Log failure with error details
    SYSTEM$LOG('ERROR', OBJECT_CONSTRUCT(
      'component', 'ELT_Pipeline',
      'run_id', :etl_run_id,
      'pipeline', 'streams_tasks_elt',
      'stage', 'gold_materialize',
      'status', 'FAILED',
      'error_code', :SQLCODE,
      'error_message', :SQLERRM,
      'procedure', 'gold.materialize_core_models'
    ));
    
    RETURN 'ERROR in gold materialization: ' || :SQLERRM;
END;
$$;

-- ================================================================
-- ENHANCED TASKS: Modify existing tasks from Module 3 to use logged procedures
-- ================================================================
USE SCHEMA tasks;

-- Update the existing silver task to use logged procedure and add task-level logging
ALTER TASK IF EXISTS tasks.t_silver_from_bronze_stream SUSPEND;
ALTER TASK tasks.t_silver_from_bronze_stream SET LOG_LEVEL = INFO;

CREATE OR REPLACE TASK tasks.t_silver_from_bronze_stream
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 6 * * * UTC'
  WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.EMPLOYEE_DATA_PARQUET_STREAM')
AS
DECLARE
  run_id STRING DEFAULT UUID_STRING();
  task_result STRING;
BEGIN
  -- Call the logged wrapper procedure
  CALL silver.load_employee_staging_logged(:run_id) INTO :task_result;
END;

-- Update the existing gold task to use logged procedure and add task-level logging
ALTER TASK IF EXISTS tasks.t_gold_refresh_after_silver SUSPEND;
ALTER TASK tasks.t_gold_refresh_after_silver SET LOG_LEVEL = INFO;

CREATE OR REPLACE TASK tasks.t_gold_refresh_after_silver
  WAREHOUSE = COMPUTE_WH
  AFTER tasks.t_silver_from_bronze_stream
AS
DECLARE
  run_id STRING DEFAULT UUID_STRING();
  task_result STRING;
BEGIN
  -- Call the logged wrapper procedure
  CALL gold.materialize_core_models_logged(:run_id) INTO :task_result;
END;
-- ================================================================
-- DEMO EXECUTION: Run the enhanced pipeline with logging
-- ================================================================

-- Modify the pipe
CREATE OR REPLACE PIPE bronze.employee_parquet_pipe
  AUTO_INGEST = FALSE  -- Set to FALSE for manual demo control
  AS
  COPY INTO bronze.employee_data_parquet
  FROM @bronze.raw_zone/parquet/
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  FILE_FORMAT = (TYPE = 'PARQUET')
  PATTERN = '.*employee_data_part_.*[.]parquet';

ALTER PIPE bronze.employee_parquet_pipe REFRESH;

SELECT * FROM bronze.employee_data_parquet;

SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.EMPLOYEE_DATA_PARQUET_STREAM') AS stream_has_data;

-- Step 2: Resume and execute tasks
ALTER TASK tasks.t_gold_refresh_after_silver RESUME;
ALTER TASK tasks.t_silver_from_bronze_stream RESUME;

-- Manually execute for demo purposes
EXECUTE TASK tasks.t_silver_from_bronze_stream;

SELECT * FROM silver.employee_data_stg;

-- ================================================================
-- EVENT TABLE MONITORING: Query the built-in event table
-- ================================================================

-- View all recent pipeline events
SELECT * FROM ops.pipeline_events
WHERE RECORD_TYPE IN('EVENT','LOG','INFO')
ORDER BY TIMESTAMP DESC LIMIT 50;

-- View structured pipeline events with extracted JSON data
SELECT 
  TIMESTAMP,
  SCOPE:name::STRING AS scope_name,
  PARSE_JSON(VALUE):run_id::STRING AS run_id,
  PARSE_JSON(VALUE):pipeline::STRING AS pipeline,
  PARSE_JSON(VALUE):stage::STRING AS stage,
  PARSE_JSON(VALUE):event::STRING AS event,
  PARSE_JSON(VALUE):status::STRING AS status,
  PARSE_JSON(VALUE):task_name::STRING AS task_name,
  PARSE_JSON(VALUE):duration_seconds::NUMBER AS duration_seconds,
  PARSE_JSON(VALUE):source::STRING AS source_object,
  PARSE_JSON(VALUE):target::STRING AS target_object,
  PARSE_JSON(VALUE):error_message::STRING AS error_message
FROM ops.pipeline_events 
WHERE RECORD_TYPE = 'LOG' 
  AND PARSE_JSON(VALUE):pipeline::STRING = 'streams_tasks_elt'
ORDER BY TIMESTAMP DESC 
LIMIT 20;

-- Pipeline execution summary from event table (last 7 days)
SELECT 
  PARSE_JSON(VALUE):stage::STRING AS pipeline_stage,
  PARSE_JSON(VALUE):status::STRING AS status,
  COUNT(*) AS execution_count,
  AVG(PARSE_JSON(VALUE):duration_seconds::NUMBER) AS avg_duration_seconds
FROM ops.pipeline_events
WHERE RECORD_TYPE = 'LOG' 
  AND PARSE_JSON(VALUE):pipeline::STRING = 'streams_tasks_elt'
  AND PARSE_JSON(VALUE):status::STRING IS NOT NULL
  AND TIMESTAMP >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY PARSE_JSON(VALUE):stage::STRING, PARSE_JSON(VALUE):status::STRING
ORDER BY pipeline_stage, status;

-- ================================================================
-- SYSTEM VIEWS: Task and Load History
-- ================================================================

-- Recent task executions (last 7 days)
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
  SCHEDULED_TIME_RANGE_START => DATEADD(DAY, -7, CURRENT_TIMESTAMP()),
  RESULT_LIMIT => 1000
)) ORDER BY SCHEDULED_TIME DESC;

-- Recent loads into bronze/silver (COPY or Snowpipe history - last 7 days)
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'BRONZE.EMPLOYEE_DATA_PARQUET',
  START_TIME => DATEADD(DAY, -7, CURRENT_TIMESTAMP())
)) ORDER BY LAST_LOAD_TIME DESC;

-- ================================================================
-- DYNAMIC TABLES MONITORING: Comprehensive monitoring for Dynamic Tables pipeline
-- Based on Snowflake documentation: https://docs.snowflake.com/en/user-guide/dynamic-tables-monitor
-- ================================================================

-- Monitor: Comprehensive status and lag metrics for our dynamic tables
SELECT
  name,
  database_name,
  schema_name,
  scheduling_state,
  target_lag_type,
  target_lag_sec,
  last_completed_refresh_state,
  last_completed_refresh_state_code,
  last_completed_refresh_state_message,
  latest_data_timestamp,
  time_within_target_lag_ratio,
  maximum_lag_sec,
  executing_refresh_query_id
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE database_name = 'SNOW_ELT_COURSE'
  AND name LIKE 'DT_%'
ORDER BY schema_name, name;

-- Monitor: View refresh history for our dynamic tables (last 7 days)
SELECT
  name,
  data_timestamp,
  state,
  state_code,
  state_message,
  refresh_action,
  query_id,
  statistics
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  DATA_TIMESTAMP_START => DATEADD(DAY, -6, CURRENT_TIMESTAMP())
))
WHERE name LIKE 'DT_%'
ORDER BY data_timestamp DESC, name;


-- Monitor: Performance summary - credits and efficiency
SELECT
  name,
  COUNT(*) AS total_refreshes,
  SUM(CASE WHEN state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS successful_refreshes,
  SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS failed_refreshes,
  SUM(statistics:numInsertedRows::NUMBER) AS total_rows_inserted,
  SUM(statistics:numDeletedRows::NUMBER) AS total_rows_deleted,
  SUM(statistics:numCopiedRows::NUMBER) AS total_rows_copied
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  DATA_TIMESTAMP_START => DATEADD(DAY, -1, CURRENT_TIMESTAMP())
))
WHERE name LIKE 'DT_%'
GROUP BY name
ORDER BY name;

-- ================================================================
-- CLEANUP: Pause tasks after demo
-- ================================================================

-- Pause the enhanced tasks to prevent unwanted executions
ALTER TASK tasks.t_silver_from_bronze_stream SUSPEND;
ALTER TASK tasks.t_gold_refresh_after_silver SUSPEND;

