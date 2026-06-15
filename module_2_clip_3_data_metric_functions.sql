-- ========================================================================
-- Module 2 Clip 3 Demo: Data Quality Using Data Metric Functions (DMFs)
-- Demo: Create and use Data Metric Functions for data quality monitoring
-- ========================================================================

-- ========================================================================
-- PERMISSIONS SETUP FOR DATA METRIC FUNCTIONS
-- ========================================================================

-- Grant necessary privileges for using DMFs
-- Note: These grants may require ACCOUNTADMIN role in a real environment
GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER TO ROLE sysadmin;
GRANT DATABASE ROLE SNOWFLAKE.USAGE_VIEWER TO ROLE sysadmin;
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE sysadmin;
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE sysadmin;

-- Set up the environment
USE ROLE sysadmin;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOW_COURSE;
USE SCHEMA data_quality;

-- ========================================================================
-- SETUP: CREATE TARGET TABLE FOR DMF DEMO
-- ========================================================================

-- Create a copy of our JSON table for DMF demonstrations
-- This makes it easier to follow the demo with a dedicated table
CREATE OR REPLACE TABLE employee_data_json_dmf AS 
SELECT * FROM employee_data_json;

-- Verify the data copied correctly
SELECT COUNT(*) as total_records FROM employee_data_json_dmf;

-- Review the data structure including the issue records
SELECT 
    employee_number,
    employee_name,
    engagement_survey:work_life_balance_score as wlb_score,
    engagement_survey:career_growth_score as career_score
FROM employee_data_json_dmf
WHERE employee_name IN ('John Smith', 'Olivia Richardson')
ORDER BY employee_name;



-- ========================================================================
-- CREATE CUSTOM DATA METRIC FUNCTIONS
-- ========================================================================

-- DMF 1: Check for incomplete engagement survey data (equivalent to sp_check_engagement_survey_completeness)
CREATE OR REPLACE DATA METRIC FUNCTION dmf_incomplete_engagement_survey_check(
    arg_table TABLE(
         employee_number VARCHAR,
        employee_name VARCHAR,
        engagement_survey VARIANT
    )
)
RETURNS NUMBER
COMMENT = 'Counts records with incomplete engagement survey data'
AS
$$
    WITH field_check AS (
        SELECT 
            employee_number,
            employee_name,
            engagement_survey,
            -- Check for missing key fields
            CASE WHEN engagement_survey:work_life_balance_score IS NULL THEN 1 ELSE 0 END as missing_wlb,
            CASE WHEN engagement_survey:career_growth_score IS NULL THEN 1 ELSE 0 END as missing_career,
            CASE WHEN engagement_survey:satisfaction_score IS NULL THEN 1 ELSE 0 END as missing_satisfaction,
            CASE WHEN engagement_survey:communication_score IS NULL THEN 1 ELSE 0 END as missing_communication,
            CASE WHEN engagement_survey:teamwork_score IS NULL THEN 1 ELSE 0 END as missing_teamwork
        FROM arg_table
        WHERE engagement_survey IS NOT NULL
    )
    SELECT COUNT(*)
    FROM field_check
    WHERE (missing_wlb + missing_career + missing_satisfaction + missing_communication + missing_teamwork) > 0
$$;


-- ========================================================================
-- TEST CUSTOM DATA METRIC FUNCTIONS MANUALLY
-- ========================================================================
SELECT CURRENT_VERSION();

-- Test DMF 1: Check for incomplete engagement surveys
SELECT dmf_incomplete_engagement_survey_check(
    SELECT 
        employee_number,
        employee_name, 
        engagement_survey 
    FROM employee_data_json_dmf
) as incomplete_survey_count;

-- ========================================================================
-- TEST A SYSTEM DATA METRIC FUNCTION
-- ========================================================================


-- Test system DMF: Count duplicate employee numbers (system function)
-- In the previous demo we had to code this ourselves in a stored procedure
SELECT SNOWFLAKE.CORE.DUPLICATE_COUNT(
    SELECT employee_number FROM employee_data_json_dmf
) as duplicate_employee_numbers;

-- ========================================================================
-- ASSOCIATE DATA METRIC FUNCTIONS WITH THE TABLE
-- ========================================================================

-- First, set up a schedule for the DMFs to run
-- Run every 5 minutes for demo purposes (in production, this might be daily or hourly)
ALTER TABLE employee_data_json_dmf 
SET DATA_METRIC_SCHEDULE = '5 MINUTE';

-- Associate custom DMF for checking incomplete engagement surveys
ALTER TABLE employee_data_json_dmf
ADD DATA METRIC FUNCTION dmf_incomplete_engagement_survey_check
    ON (employee_number, employee_name, engagement_survey);

-- Associate system DMF
ALTER TABLE employee_data_json_dmf
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT
    ON (employee_number);

-- ========================================================================
-- VIEW DATA METRIC FUNCTION ASSOCIATIONS
-- ========================================================================

-- Show the DMFs associated with our table
SELECT * FROM TABLE(
    INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
        REF_ENTITY_NAME => 'employee_data_json_dmf',
        REF_ENTITY_DOMAIN => 'table'
    )
);

-- Show all DMFs in our schema
SHOW DATA METRIC FUNCTIONS IN SCHEMA SNOW_COURSE.data_quality;


-- ========================================================================
-- VIEW DATA QUALITY MONITORING RESULTS
-- ========================================================================

-- Wait a few minutes after setting up the DMFs, then query the results
-- Note: Results may take a few minutes to appear due to the scheduling system

-- View recent DMF results from the monitoring view
SELECT 
    table_name,
    metric_name,
    value as metric_value,
    measurement_time,
    scheduled_time
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_name = 'EMPLOYEE_DATA_JSON_DMF'
    AND measurement_time >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
ORDER BY measurement_time DESC, metric_name;

-- ========================================================================
-- CREATE A DATA QUALITY DASHBOARD VIEW
-- ========================================================================

-- Create a view that provides a dashboard-like summary of data quality
CREATE OR REPLACE VIEW v_data_quality_dashboard AS
WITH latest_results AS (
    SELECT 
        table_name,
        metric_name,
        value as metric_value,
        measurement_time,
        ROW_NUMBER() OVER (PARTITION BY table_name, metric_name ORDER BY measurement_time DESC) as rn
    FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
    WHERE table_name = 'EMPLOYEE_DATA_JSON_DMF'
)
SELECT 
    table_name as "Table Name",
    metric_name as "Data Quality Check",
    metric_value as "Result",
    CASE 
        -- Custom DMF checks (expect 0 for passing)
        WHEN metric_name ILIKE '%incomplete%' AND metric_value = 0 THEN '✅ PASSED'
        WHEN metric_name ILIKE '%incomplete%' AND metric_value > 0 THEN '❌ FAILED'
        -- System DMF checks
        WHEN metric_name = 'DUPLICATE_COUNT' AND metric_value = 0 THEN '✅ PASSED'
        WHEN metric_name = 'DUPLICATE_COUNT' AND metric_value > 0 THEN '❌ FAILED'
        ELSE 'ℹ️ INFO'
    END as "Status",
    measurement_time as "Last Checked"
FROM latest_results
WHERE rn = 1
ORDER BY 
    CASE WHEN "Status" LIKE '%FAILED%' THEN 1 ELSE 2 END,
    metric_name;

-- View the dashboard
SELECT * FROM v_data_quality_dashboard;


-- ========================================================================
-- CLEANUP AND MANAGEMENT
-- ========================================================================

-- To remove DMF associations from the table:
ALTER TABLE employee_data_json_dmf DROP DATA METRIC FUNCTION dmf_incomplete_engagement_survey_check ON (employee_number, employee_name, engagement_survey);
ALTER TABLE employee_data_json_dmf DROP DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (employee_number);

-- To remove the schedule:
ALTER TABLE employee_data_json_dmf UNSET DATA_METRIC_SCHEDULE;