-- ========================================================================
-- Module 3 Clip 3 Demo: Notifying on Validation Issues
-- Demo: Add email notifications to task workflow when validation fails
-- ========================================================================

-- IMPORTANT: Before running this demo, replace with your actual email address in the integration
-- and task definitions below (the email must be a verified email from a snowflake user account)

-- Set up the environment
USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOW_COURSE;
USE SCHEMA data_quality;

-- ========================================================================
-- SETUP: EMAIL NOTIFICATION CONFIGURATION
-- ========================================================================
USE ROLE accountadmin;
-- Create email notification integration
-- Replace 'your-email@company.com' with your actual email address
CREATE OR REPLACE NOTIFICATION INTEGRATION data_quality_email_integration
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = ('replace@youremail.com')
    COMMENT = 'Email integration for data quality validation alerts';

-- Grant usage on the integration
GRANT USAGE ON INTEGRATION data_quality_email_integration TO ROLE sysadmin;

-- Verify the integration was created
SHOW INTEGRATIONS LIKE 'data_quality_email_integration';

USE ROLE sysadmin;
-- Create a table to track email notifications sent
CREATE OR REPLACE TABLE email_notifications_log (
    notification_id STRING DEFAULT RANDSTR(10, RANDOM()),
    sent_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    notification_type STRING,
    recipient_email STRING,
    subject STRING,
    message_body STRING,
    task_name STRING,
    validation_errors STRING,
    email_status STRING
);

-- ========================================================================
-- EMAIL NOTIFICATION TASK
-- ========================================================================

-- Separate task for sending email notifications when validation fails
CREATE OR REPLACE TASK task_send_validation_failure_email
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' -- suggested size for serverless task
    COMMENT = 'Standalone serverless task that sends email notification when validation fails'
    AFTER task_data_quality_check
    WHEN (SYSTEM$GET_PREDECESSOR_RETURN_VALUE() = 'FAILED')
AS
EXECUTE IMMEDIATE
$$
DECLARE
    subject_text STRING;
    body_text STRING;
    recipient_addr STRING DEFAULT 'replace@youremail.com';
    validation_result STRING;
    notif_id STRING;
    status_text STRING DEFAULT 'PENDING';
BEGIN
        -- Get validation result status from the predecessor task
    validation_result := SYSTEM$GET_PREDECESSOR_RETURN_VALUE();
    
    -- Prepare email content
    subject_text := 'URGENT: Data Quality Validation Failed - Action Required';
    
    body_text := 'URGENT: Data Quality Alert' || CHR(10) || CHR(10) ||
                  'Our automated data quality validation has detected issues with the employee data.' || CHR(10) || CHR(10) ||
                  'Failure Details:' || CHR(10) ||
                  '- Validation Status: ' || validation_result || CHR(10) ||
                  '- Database: ' || CURRENT_DATABASE() || CHR(10) ||
                  '- Schema: ' || CURRENT_SCHEMA() || CHR(10) ||
                  '- Failed at: ' || CURRENT_TIMESTAMP()::STRING || CHR(10) || CHR(10) ||
                  'Immediate Actions Required:' || CHR(10) ||
                  '1. Check the data_quality_events table for detailed error information' || CHR(10) ||
                  '2. Review the source data for data quality issues' || CHR(10) ||
                  '3. Fix data issues and re-run the validation pipeline' || CHR(10) || CHR(10) ||
                  'Query to check details:' || CHR(10) ||
                  'SELECT * FROM data_quality_events WHERE record_type = ''LOG'' AND timestamp >= DATEADD(MINUTE, -10, CURRENT_TIMESTAMP()) ORDER BY timestamp DESC;' || CHR(10) || CHR(10) ||
                  'This email was sent automatically by the Snowflake Data Quality Monitoring System.';
    
    -- Generate notification ID
    SELECT RANDSTR(10, RANDOM()) into notif_id;
    
    -- Log the notification
    INSERT INTO email_notifications_log (
        notification_id,
        notification_type,
        recipient_email,
        subject,
        message_body,
        task_name,
        validation_errors
    ) VALUES (
        :notif_id,
        'URGENT_VALIDATION_FAILURE',
        :recipient_addr,
        :subject_text,
        :body_text,
        'task_send_validation_failure_email',
        'Task returned: ' || :validation_result
    );
    
    -- Send actual email notification
    BEGIN
        -- Send email using the notification integration
        CALL SYSTEM$SEND_EMAIL(
            'data_quality_email_integration',
            :recipient_addr,
            :subject_text,
            :body_text
        );
        
        status_text := 'SUCCESS';
        
        SYSTEM$LOG('info', 'Task Orchestration: URGENT email notification sent to ' || recipient_addr || 
                   ' for validation failure. Notification ID: ' || notif_id);
        
    EXCEPTION
        WHEN OTHER THEN
            status_text := 'FAILED: ' || SQLERRM;
            SYSTEM$LOG('error', 'Task Orchestration: Failed to send URGENT email notification. Error: ' || SQLERRM);
    END;
    
    -- Update email status
    UPDATE email_notifications_log 
    SET email_status = :status_text
    WHERE notification_id = :notif_id;
    
    RETURN 'EMAIL_NOTIFICATION_SENT';
END;
$$;

-- ========================================================================
-- TASK MANAGEMENT
-- ========================================================================

-- Resume the tasks
ALTER TASK task_send_validation_failure_email RESUME;
ALTER TASK task_create_aggregation RESUME;
ALTER TASK task_log_validation_errors RESUME;
ALTER TASK task_data_quality_check RESUME;

-- Show updated task dependency graph
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
-- DEMO SCENARIO: VALIDATION FAILURE WITH EMAIL NOTIFICATION
-- ========================================================================

-- Clear previous data to start fresh
TRUNCATE TABLE employee_data_json;
TRUNCATE TABLE email_notifications_log;

-- Load problematic data to trigger validation failure
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

-- Verify stream contents
SELECT * FROM employee_data_stream;

-- Execute root task manually to trigger validation
EXECUTE TASK task_data_quality_check;

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
    task_name => 'task_send_validation_failure_email',
    scheduled_time_range_start => DATEADD(MINUTE, -20, CURRENT_TIMESTAMP())
))
ORDER BY completed_time DESC;
-- ========================================================================
-- MONITORING EMAIL NOTIFICATIONS
-- ========================================================================

-- Check email notifications log
SELECT 
    notification_id,
    sent_timestamp,
    notification_type,
    recipient_email,
    subject,
    LEFT(message_body, 100) || '...' as message_preview,
    task_name,
    validation_errors,
    email_status
FROM email_notifications_log
ORDER BY sent_timestamp DESC;

-- ========================================================================
-- CLEANUP
-- ========================================================================

-- To suspend all tasks after demo:
ALTER TASK task_data_quality_check SUSPEND;
ALTER TASK task_create_aggregation SUSPEND;
ALTER TASK task_log_validation_errors SUSPEND;
ALTER TASK task_send_validation_failure_email SUSPEND;

-- To drop the email notification task:
DROP TASK task_send_validation_failure_email;
DROP TABLE email_notifications_log;
DROP INTEGRATION data_quality_email_integration; 