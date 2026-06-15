-- ========================================================================
-- Module 2 Clip 2 Demo: Custom Scripting for Data Quality
-- Demo: Create stored procedures for data quality checks using event tables
-- ========================================================================

-- Set up the environment
USE ROLE sysadmin;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOW_COURSE;
USE SCHEMA data_quality;


-- ========================================================================
-- EVENT TABLE SETUP FOR DATA QUALITY MONITORING
-- ========================================================================

-- Create a custom event table for our data quality monitoring
CREATE OR REPLACE EVENT TABLE data_quality_events;

-- Associate the event table with our database to collect telemetry
ALTER DATABASE SNOW_COURSE SET EVENT_TABLE = SNOW_COURSE.data_quality.data_quality_events;

-- Set LOG_LEVEL to enable logging (INFO level captures INFO, WARN, ERROR, FATAL)
ALTER DATABASE SNOW_COURSE SET LOG_LEVEL = 'INFO';

-- Set TRACE_LEVEL to enable tracing
ALTER DATABASE SNOW_COURSE SET TRACE_LEVEL = 'ON_EVENT';

-- Verify the parameters are set
SHOW PARAMETERS LIKE 'event_table' IN DATABASE SNOW_COURSE;
SHOW PARAMETERS LIKE 'log_level' IN DATABASE SNOW_COURSE;
SHOW PARAMETERS LIKE 'trace_level' IN DATABASE SNOW_COURSE;

-- ========================================================================
-- SAMPLE DATA SETUP
-- ========================================================================

-- Ensure we have some data in our JSON table from Module 1
-- If table is empty, load some sample data
SELECT COUNT(*) FROM employee_data_json;

-- review the issue with our dataset
SELECT 
    *
FROM employee_data_json
WHERE employee_name = 'John Smith' -- complete record
UNION
SELECT 
* FROM employee_data_json
WHERE employee_name = 'Olivia Richardson'; -- missing work life balance score

-- ========================================================================
-- STORED PROCEDURE 1: CHECK EMPLOYEE ID UNIQUENESS
-- ========================================================================

CREATE OR REPLACE PROCEDURE sp_check_employee_id_uniqueness()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    duplicate_count INTEGER;
    total_records INTEGER;
    check_status VARCHAR;
    error_details VARCHAR;
    log_message VARCHAR;
BEGIN
    -- Count total records
    SELECT COUNT(*) INTO total_records FROM employee_data_json;
    
    -- Count duplicate employee numbers
    SELECT COUNT(*) INTO duplicate_count
    FROM (
        SELECT employee_number, COUNT(*) as cnt
        FROM employee_data_json
        GROUP BY employee_number
        HAVING COUNT(*) > 1
    );
    
    -- Determine check status and prepare details
    IF (:duplicate_count > 0) THEN
        -- Get details of duplicate records
        SELECT 'Duplicate employee_numbers found: ' || 
               LISTAGG(employee_number || ' (count: ' || cnt || ')', ', ') 
        INTO error_details
        FROM (
            SELECT employee_number, COUNT(*) as cnt
            FROM employee_data_json
            GROUP BY employee_number
            HAVING COUNT(*) > 1
        );
        
        check_status := 'FAILED';
        log_message := 'Data Quality Check: Employee ID Uniqueness - FAILED. ' || :error_details;
    ELSE
        check_status := 'PASSED';
        error_details := 'All employee IDs are unique';
        log_message := 'Data Quality Check: Employee ID Uniqueness - PASSED. Checked ' || :total_records || ' records.';
    END IF;
    
    -- Log to system using SYSTEM$LOG for event table capture
    SYSTEM$LOG('info', :log_message);
    
    -- Also log structured data using SYSTEM$LOG_INFO
    SYSTEM$LOG_INFO('Structured DQ Check: ' || OBJECT_CONSTRUCT(
        'check_type', 'employee_id_uniqueness',
        'table_name', 'employee_data_json',
        'status', :check_status,
        'total_records', :total_records,
        'duplicate_count', :duplicate_count,
        'details', :error_details,
        'timestamp', CURRENT_TIMESTAMP()
    )::STRING);
    
    RETURN 'Employee ID Uniqueness Check: ' || :check_status || '. Details: ' || :error_details;
END;
$$;

-- ========================================================================
-- STORED PROCEDURE 2: CHECK JSON FIELD COMPLETENESS
-- ========================================================================

CREATE OR REPLACE PROCEDURE sp_check_engagement_survey_completeness()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    incomplete_count INTEGER;
    total_records INTEGER;
    check_status VARCHAR;
    error_details VARCHAR;
    log_message VARCHAR;
    expected_fields ARRAY;
BEGIN
    -- Define expected fields in engagement_survey
    expected_fields := ARRAY_CONSTRUCT(
        'employee_number', 'employee_name', 'gender', 'age', 'department', 
        'job_title', 'satisfaction_score', 'work_life_balance_score', 
        'career_growth_score', 'communication_score', 'teamwork_score'
    );
    
    -- Count total records with engagement_survey data
    SELECT COUNT(*) INTO total_records 
    FROM employee_data_json 
    WHERE engagement_survey IS NOT NULL;
    
    -- Check for incomplete engagement_survey objects
    -- This query finds records where any expected field is missing
    WITH field_check AS (
        SELECT 
            employee_number,
            employee_name,
            engagement_survey,
            -- Check each expected field
            CASE WHEN engagement_survey:employee_number IS NULL THEN 'employee_number' END as missing_emp_num,
            CASE WHEN engagement_survey:employee_name IS NULL THEN 'employee_name' END as missing_emp_name,
            CASE WHEN engagement_survey:gender IS NULL THEN 'gender' END as missing_gender,
            CASE WHEN engagement_survey:age IS NULL THEN 'age' END as missing_age,
            CASE WHEN engagement_survey:department IS NULL THEN 'department' END as missing_dept,
            CASE WHEN engagement_survey:job_title IS NULL THEN 'job_title' END as missing_job,
            CASE WHEN engagement_survey:satisfaction_score IS NULL THEN 'satisfaction_score' END as missing_sat,
            CASE WHEN engagement_survey:work_life_balance_score IS NULL THEN 'work_life_balance_score' END as missing_wlb,
            CASE WHEN engagement_survey:career_growth_score IS NULL THEN 'career_growth_score' END as missing_career,
            CASE WHEN engagement_survey:communication_score IS NULL THEN 'communication_score' END as missing_comm,
            CASE WHEN engagement_survey:teamwork_score IS NULL THEN 'teamwork_score' END as missing_team
        FROM employee_data_json
        WHERE engagement_survey IS NOT NULL
    ),
    incomplete_records AS (
        SELECT 
            employee_number,
            employee_name,
            ARRAY_CONSTRUCT_COMPACT(
                missing_emp_num, missing_emp_name, missing_gender, missing_age, missing_dept,
                missing_job, missing_sat, missing_wlb, missing_career, missing_comm, missing_team
            ) as missing_fields
        FROM field_check
        WHERE ARRAY_SIZE(ARRAY_CONSTRUCT_COMPACT(
            missing_emp_num, missing_emp_name, missing_gender, missing_age, missing_dept,
            missing_job, missing_sat, missing_wlb, missing_career, missing_comm, missing_team
        )) > 0
    )
    SELECT COUNT(*) INTO incomplete_count FROM incomplete_records;
    
    -- Determine check status and prepare details
    IF (:incomplete_count > 0) THEN
        -- Get details of incomplete records
        WITH field_check AS (
            SELECT 
                employee_number,
                employee_name,
                engagement_survey,
                CASE WHEN engagement_survey:employee_number IS NULL THEN 'employee_number' END as missing_emp_num,
                CASE WHEN engagement_survey:employee_name IS NULL THEN 'employee_name' END as missing_emp_name,
                CASE WHEN engagement_survey:gender IS NULL THEN 'gender' END as missing_gender,
                CASE WHEN engagement_survey:age IS NULL THEN 'age' END as missing_age,
                CASE WHEN engagement_survey:department IS NULL THEN 'department' END as missing_dept,
                CASE WHEN engagement_survey:job_title IS NULL THEN 'job_title' END as missing_job,
                CASE WHEN engagement_survey:satisfaction_score IS NULL THEN 'satisfaction_score' END as missing_sat,
                CASE WHEN engagement_survey:work_life_balance_score IS NULL THEN 'work_life_balance_score' END as missing_wlb,
                CASE WHEN engagement_survey:career_growth_score IS NULL THEN 'career_growth_score' END as missing_career,
                CASE WHEN engagement_survey:communication_score IS NULL THEN 'communication_score' END as missing_comm,
                CASE WHEN engagement_survey:teamwork_score IS NULL THEN 'teamwork_score' END as missing_team
            FROM employee_data_json
            WHERE engagement_survey IS NOT NULL
        ),
        incomplete_records AS (
            SELECT 
                employee_number,
                employee_name,
                ARRAY_CONSTRUCT_COMPACT(
                    missing_emp_num, missing_emp_name, missing_gender, missing_age, missing_dept,
                    missing_job, missing_sat, missing_wlb, missing_career, missing_comm, missing_team
                ) as missing_fields
            FROM field_check
            WHERE ARRAY_SIZE(ARRAY_CONSTRUCT_COMPACT(
                missing_emp_num, missing_emp_name, missing_gender, missing_age, missing_dept,
                missing_job, missing_sat, missing_wlb, missing_career, missing_comm, missing_team
            )) > 0
        )
        SELECT 'Incomplete engagement_survey found for: ' || 
               LISTAGG(employee_name || ' (ID: ' || employee_number || ', missing: ' || 
                      ARRAY_TO_STRING(missing_fields, ', ') || ')', '; ')
        INTO error_details
        FROM incomplete_records;
        
        check_status := 'FAILED';
        log_message := 'Data Quality Check: Engagement Survey Completeness - FAILED. ' || :error_details;
    ELSE
        check_status := 'PASSED';
        error_details := 'All engagement_survey objects contain required fields';
        log_message := 'Data Quality Check: Engagement Survey Completeness - PASSED. Checked ' || :total_records || ' records.';
    END IF;
    
    -- Log to system using SYSTEM$LOG for event table capture  
    SYSTEM$LOG('info', :log_message);
    
    -- Also log structured data using SYSTEM$LOG_INFO
    SYSTEM$LOG_INFO('Structured DQ Check: ' || OBJECT_CONSTRUCT(
        'check_type', 'engagement_survey_completeness',
        'table_name', 'employee_data_json',
        'status', :check_status,
        'total_records', :total_records,
        'incomplete_count', :incomplete_count,
        'expected_fields', :expected_fields,
        'details', :error_details,
        'timestamp', CURRENT_TIMESTAMP()
    )::STRING);
    
    RETURN 'Engagement Survey Completeness Check: ' || :check_status || '. Details: ' || :error_details;
END;
$$;

-- ========================================================================
-- MANUAL EXECUTION OF DATA QUALITY CHECKS
-- ========================================================================

-- Execute the Employee ID uniqueness check
CALL sp_check_employee_id_uniqueness();

-- Execute the Engagement Survey completeness check
CALL sp_check_engagement_survey_completeness();

-- ========================================================================
-- VIEWING DATA QUALITY RESULTS FROM EVENT TABLE
-- ========================================================================

-- View recent log entries from our event table
SELECT 
    timestamp,
    resource_attributes,
    record_type,
    record,
    value
FROM data_quality_events
WHERE record_type = 'LOG'
    AND timestamp >= DATEADD(MINUTE, -5, CURRENT_TIMESTAMP())
ORDER BY timestamp DESC;


-- Parse the structured JSON data from our traces
SELECT 
    timestamp,
    resource_attributes,
    value::STRING as message,
    TRY_PARSE_JSON(SUBSTR(value::STRING, POSITION('{' IN value::STRING))) as structured_data
FROM data_quality_events
WHERE record_type = 'LOG'
    AND value::STRING LIKE '%Structured DQ Check:%'
    AND timestamp >= DATEADD(MINUTE, -5, CURRENT_TIMESTAMP())
ORDER BY timestamp DESC;

-- ========================================================================
-- SUMMARY QUERY: DATA QUALITY DASHBOARD VIEW
-- ========================================================================

-- Create a summary view of all data quality checks
WITH dq_results AS (
    SELECT 
        timestamp,
        value::STRING as message,
        CASE 
            WHEN value::STRING LIKE '%check_type%' THEN
                TRY_PARSE_JSON(SUBSTR(value::STRING, POSITION('{"check_type"' IN value::STRING)))
            ELSE NULL 
        END as parsed_data
    FROM data_quality_events
    WHERE record_type = 'LOG'
        AND value::STRING LIKE '%check_type%'
        AND timestamp >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
)
SELECT 
    parsed_data:check_type::STRING as "Data Quality Check",
    parsed_data:status::STRING as "Status", 
    parsed_data:total_records::INTEGER as "Records Checked",
    COALESCE(parsed_data:duplicate_count::INTEGER, parsed_data:incomplete_count::INTEGER, 0) as "Issues Found",
    parsed_data:details::STRING as "Details",
    timestamp as "Check Time"
FROM dq_results
WHERE parsed_data IS NOT NULL
ORDER BY timestamp DESC;

