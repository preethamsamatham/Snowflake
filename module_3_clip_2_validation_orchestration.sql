-- ========================================================================
-- Module 3 Clip 2 Demo: Orchestrating Validation Tasks
-- Demo: Create task hierarchy that orchestrates data validation with conditional branching
-- ========================================================================

-- Set up the environment
USE ROLE sysadmin;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOW_COURSE;
USE SCHEMA data_quality;

-- ========================================================================
-- SETUP: STREAM AND AGGREGATION TABLE
-- ========================================================================
-- Truncate table to start fresh
TRUNCATE TABLE employee_data_json;

-- Create a stream to track changes in employee_data_json table
CREATE OR REPLACE STREAM employee_data_stream 
ON TABLE employee_data_json
COMMENT = 'Stream to track inserts/updates/deletes in employee_data_json for validation orchestration';

-- Verify stream is created and show current state
SELECT * FROM employee_data_stream;

-- Create aggregation table for successful validation outcomes
CREATE OR REPLACE TABLE employee_summary (
    department VARCHAR(100),
    total_employees INT,
    avg_age DECIMAL(5,2),
    avg_length_of_service DECIMAL(5,2),
    avg_satisfaction_score DECIMAL(3,2),
    avg_work_life_balance_score DECIMAL(3,2),
    total_hours_absent INT,
    data_quality_check_passed BOOLEAN,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ========================================================================
-- TASK 1: DATA QUALITY VALIDATION (ROOT TASK)
-- ========================================================================

-- Root task that triggers on stream changes and runs data quality checks
CREATE OR REPLACE TASK task_data_quality_check
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' -- suggested size for serverless task
    SCHEDULE = 'USING CRON 0 * * * * UTC'  -- Every 60 minutes (will be overridden by stream trigger)
    COMMENT = 'Serverless task that runs data quality checks when stream has data'
    WHEN SYSTEM$STREAM_HAS_DATA('employee_data_stream')
AS
EXECUTE IMMEDIATE
$$
DECLARE
    incomplete_survey_count INT;
    duplicate_count INT;
    validation_passed BOOLEAN DEFAULT TRUE;
    result_message STRING;
    return_value STRING;
BEGIN
    -- Check if stream has data
    IF (SYSTEM$STREAM_HAS_DATA('employee_data_stream')) THEN
        
        -- Check for incomplete engagement surveys (same logic as DMF from Module 2 Clip 3)
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
            FROM employee_data_json
            WHERE engagement_survey IS NOT NULL
        )
        SELECT COUNT(*)
        INTO incomplete_survey_count
        FROM field_check
        WHERE (missing_wlb + missing_career + missing_satisfaction + missing_communication + missing_teamwork) > 0;
        
        -- Check for duplicate employee numbers
        SELECT COUNT(*)
        INTO duplicate_count
        FROM (
            SELECT employee_number, COUNT(*) as cnt
            FROM employee_data_json
            GROUP BY employee_number
            HAVING COUNT(*) > 1
        );
        
        -- Determine overall validation status
        IF (incomplete_survey_count > 0 OR duplicate_count > 0) THEN
            validation_passed := FALSE;
            result_message := 'Data quality validation FAILED. Incomplete surveys: ' || 
                           incomplete_survey_count || ', Duplicates: ' || duplicate_count;
            return_value := 'FAILED';
        ELSE
            validation_passed := TRUE;
            result_message := 'Data quality validation PASSED. All checks successful.';
            return_value := 'SUCCESS';
        END IF;
        
        -- Log validation results
        SYSTEM$LOG('info', 'Task Orchestration: ' || result_message);
        
        -- Set return value for conditional task execution
        CALL SYSTEM$SET_RETURN_VALUE(:return_value);
        
    ELSE
        CALL SYSTEM$SET_RETURN_VALUE('NO_DATA');
    END IF;
END;
$$;

-- ========================================================================
-- TASK 2: CREATE AGGREGATION (SUCCESS PATH)
-- ========================================================================

-- Task that runs only when data quality validation succeeds
CREATE OR REPLACE TASK task_create_aggregation
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' -- suggested size for serverless task
    COMMENT = 'Serverless task that creates aggregation when validation passes'
    AFTER task_data_quality_check
    WHEN (SYSTEM$GET_PREDECESSOR_RETURN_VALUE() = 'SUCCESS')
AS
EXECUTE IMMEDIATE
$$
BEGIN
    -- Clear existing aggregation data
    TRUNCATE TABLE employee_summary;
    
    -- Create aggregation from validated data
    INSERT INTO employee_summary (
        department,
        total_employees,
        avg_age,
        avg_length_of_service,
        avg_satisfaction_score,
        avg_work_life_balance_score,
        total_hours_absent,
        data_quality_check_passed
    )
    SELECT 
        department,
        COUNT(*) as total_employees,
        AVG(TRY_CAST(age AS INT)) as avg_age,
        AVG(TRY_CAST(length_of_service AS INT)) as avg_length_of_service,
        AVG(TRY_CAST(engagement_survey:satisfaction_score::STRING AS DECIMAL(3,2))) as avg_satisfaction_score,
        AVG(TRY_CAST(engagement_survey:work_life_balance_score::STRING AS DECIMAL(3,2))) as avg_work_life_balance_score,
        SUM(TRY_CAST(hours_absent AS INT)) as total_hours_absent,
        TRUE as data_quality_check_passed
    FROM employee_data_json
    GROUP BY department;
    
    -- Log successful aggregation
    SYSTEM$LOG('info', 'Task Orchestration: Aggregation table populated successfully with ' || 
               (SELECT COUNT(*) FROM employee_summary) || ' department summaries.');
    
    RETURN 'AGGREGATION_COMPLETED';
END;
$$;

-- ========================================================================
-- TASK 3: LOG VALIDATION ERRORS (FAILURE PATH)
-- ========================================================================

-- Task that runs only when data quality validation fails
CREATE OR REPLACE TASK task_log_validation_errors
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' -- suggested size for serverless task
    COMMENT = 'Serverless task that logs detailed validation errors when checks fail'
    AFTER task_data_quality_check
    WHEN (SYSTEM$GET_PREDECESSOR_RETURN_VALUE() = 'FAILED')
AS
EXECUTE IMMEDIATE
$$
DECLARE
    uniqueness_result STRING;
    completeness_result STRING;
BEGIN
    -- Run detailed stored procedure validations from Module 2 Clip 2
    -- These will log detailed information to the event table
    
    -- Check employee ID uniqueness
    CALL sp_check_employee_id_uniqueness();
    
    -- Check engagement survey completeness  
    CALL sp_check_engagement_survey_completeness();
    
    RETURN 'ERROR_LOGGING_COMPLETED';
END;
$$;

-- ========================================================================
-- TASK MANAGEMENT
-- ========================================================================

-- Resume all tasks to enable execution
ALTER TASK task_log_validation_errors RESUME;
ALTER TASK task_create_aggregation RESUME;
ALTER TASK task_data_quality_check RESUME;


-- Show task dependency graph
SELECT 
    name,
    state,
    warehouse,
    schedule,
    predecessors,
    condition,
    comment
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    task_name => 'task_data_quality_check',
    recursive => true
))
ORDER BY name;

-- ========================================================================
-- DEMO SCENARIO 1: SUCCESS PATH (CLEAN DATA)
-- ========================================================================

-- Verify stream shows no changes initially
SELECT * FROM employee_data_stream;

-- Load clean data (this should pass all validation checks)
COPY INTO employee_data_json
FROM '@dataimports/employee_data.json'
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';

-- Verify stream now has data
SELECT COUNT(*) as stream_record_count FROM employee_data_stream;

-- Since we're in a demo, we will execute the root task manually to show immediate results
EXECUTE TASK task_data_quality_check;

-- Check task execution history
SELECT 
    name,
    state,
    scheduled_time,
    query_start_time,
    completed_time,
    return_value,
    error_code,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    task_name => 'task_data_quality_check',
    scheduled_time_range_start => DATEADD(MINUTE, -20, CURRENT_TIMESTAMP())
))
ORDER BY completed_time DESC;

-- Check child task status as well
SELECT 
    name,
    state,
    scheduled_time,
    query_start_time,
    completed_time,
    return_value,
    error_code,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    task_name => 'task_create_aggregation',
    scheduled_time_range_start => DATEADD(MINUTE, -20, CURRENT_TIMESTAMP())
))
ORDER BY completed_time DESC;

-- Check if aggregation table was populated (success path)
SELECT 
    'SUCCESS SCENARIO' as scenario,
    COUNT(*) as departments_created,
    SUM(total_employees) as total_employees_processed
FROM employee_summary;

-- View aggregation results
SELECT * FROM employee_summary ORDER BY department;

-- ========================================================================
-- DEMO SCENARIO 2: FAILURE PATH (PROBLEMATIC DATA)
-- ========================================================================

-- Truncate table to start fresh for failure scenario
TRUNCATE TABLE employee_data_json;
TRUNCATE TABLE employee_summary;

-- Re-create the stream to start fresh
CREATE OR REPLACE STREAM employee_data_stream 
ON TABLE employee_data_json
COMMENT = 'Stream to track inserts/updates/deletes in employee_data_json for validation orchestration';

-- Load problematic data (has missing work_life_balance_score for Olivia Richardson)
COPY INTO employee_data_json
FROM '@dataimports/employee_data - wrong_structure.json'
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';

-- Verify the problematic record
SELECT 
    employee_number,
    employee_name,
    engagement_survey:work_life_balance_score as wlb_score,
    engagement_survey:career_growth_score as career_score
FROM employee_data_json
WHERE employee_name = 'Olivia Richardson';

-- Execute root task manually to trigger validation
EXECUTE TASK task_data_quality_check;

-- Check task execution history for failure scenario
SELECT 
    name,
    state,
    scheduled_time,
    query_start_time,
    completed_time,
    return_value,
    error_code,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    task_name => 'task_data_quality_check',
    scheduled_time_range_start => DATEADD(MINUTE, -10, CURRENT_TIMESTAMP())
))
ORDER BY completed_time DESC;

-- Check child task status as well
SELECT 
    name,
    state,
    scheduled_time,
    query_start_time,
    completed_time,
    return_value,
    error_code,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    task_name => 'task_log_validation_errors',
    scheduled_time_range_start => DATEADD(MINUTE, -20, CURRENT_TIMESTAMP())
))
ORDER BY completed_time DESC;

-- Check that aggregation table is empty (failure path)
SELECT 
    'FAILURE SCENARIO' as scenario,
    COUNT(*) as departments_created,
    COALESCE(SUM(total_employees), 0) as total_employees_processed
FROM employee_summary;

-- ========================================================================
-- MONITORING AND VALIDATION RESULTS
-- ========================================================================

-- View recent validation events from the event table
SELECT 
    timestamp,
    resource_attributes,
    record_type,
    value::STRING as message
FROM data_quality_events
WHERE record_type = 'LOG'
    AND timestamp >= DATEADD(MINUTE, -20, CURRENT_TIMESTAMP())
ORDER BY timestamp DESC;


-- ========================================================================
-- CLEANUP
-- ========================================================================

-- To suspend tasks after demo:
ALTER TASK task_data_quality_check SUSPEND;
ALTER TASK task_create_aggregation SUSPEND;
ALTER TASK task_log_validation_errors SUSPEND;