-- Module 2, Demo 2: Transforming into Gold Layer
-- Source: silver.employee_data_stg (created in Module 2, Demo 1)

-- Environment Setup
USE ROLE SYSADMIN;
USE DATABASE SNOW_ELT_COURSE;

-- Create and use the gold schema
CREATE SCHEMA IF NOT EXISTS gold;
USE SCHEMA gold;

-- Verify silver staging table exists and has data
SHOW TABLES IN SCHEMA silver STARTS WITH 'EMPLOYEE_DATA_STG';
SELECT COUNT(*) AS silver_row_count FROM silver.employee_data_stg;

-- ================================================================
-- APPROACH 1: CTE-based transformation
-- Create demographics by department table using a CTE
-- ================================================================

CREATE OR REPLACE TABLE employee_demographics_by_department AS
WITH employees AS (
  SELECT
    department,
    age::NUMBER AS age,
    length_of_service::NUMBER AS length_of_service,
    gender
  FROM silver.employee_data_stg
)
SELECT
  department,
  COUNT(*) AS num_employees,
  AVG(age) AS avg_age,
  AVG(length_of_service) AS avg_length_of_service,
  SUM(IFF(UPPER(gender) = 'MALE', 1, 0)) AS num_male,
  SUM(IFF(UPPER(gender) = 'FEMALE', 1, 0)) AS num_female,
  SUM(IFF(UPPER(gender) NOT IN ('MALE','FEMALE'), 1, 0)) AS num_other_gender,
  -- Metadata columns for data lineage and auditing
  CURRENT_TIMESTAMP() AS materialized_at,
  'silver.employee_data_stg' AS source_object,
  UUID_STRING() AS etl_run_id
FROM employees
GROUP BY department
ORDER BY department;

-- Inspect results
SELECT * FROM employee_demographics_by_department;

-- ================================================================
-- APPROACH 2: Temp table + final table
-- Create survey results by department using a temp table step
-- ================================================================

CREATE OR REPLACE TEMP TABLE tmp_survey_results_by_department AS
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
  'silver.employee_data_stg' AS source_object,
  UUID_STRING() AS etl_run_id
FROM silver.employee_data_stg
GROUP BY department;

CREATE OR REPLACE TABLE survey_results_by_department AS
SELECT * FROM tmp_survey_results_by_department;

-- Inspect results
SELECT * FROM survey_results_by_department;

-- ================================================================
-- APPROACH 3: View instead of table
-- Provide a dynamic view over the same aggregation
-- ================================================================

CREATE OR REPLACE VIEW v_survey_results_by_department AS
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
  'silver.employee_data_stg' AS source_object,
  UUID_STRING() AS etl_run_id
FROM silver.employee_data_stg
GROUP BY department;

-- Inspect the view
SELECT * FROM v_survey_results_by_department;

-- ================================================================
-- MODULAR STORED PROCEDURES: Each table gets its own procedure
-- This approach allows for better maintainability and scalability
-- ================================================================
-- USE ROLE ACCOUNTADMIN;
-- GRANT ALL PRIVILEGES ON SCHEMA SNOW_ETL_COURSE.GOLD TO ROLE SYSADMIN;
-- USE ROLE SYSADMIN;
-- Procedure 1: Build employee_demographics_by_department table
CREATE OR REPLACE PROCEDURE gold.build_employee_demographics_by_department()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  start_time TIMESTAMP := CURRENT_TIMESTAMP();
  row_count INTEGER;
  result_message STRING;
BEGIN
  BEGIN
    -- Build the demographics table
    CREATE OR REPLACE TABLE gold.employee_demographics_by_department AS
    WITH employees AS (
      SELECT
        department,
        age::NUMBER AS age,
        length_of_service::NUMBER AS length_of_service,
        gender
      FROM silver.employee_data_stg
    )
    SELECT
      department,
      COUNT(*) AS num_employees,
      AVG(age) AS avg_age,
      AVG(length_of_service) AS avg_length_of_service,
      SUM(IFF(UPPER(gender) = 'MALE', 1, 0)) AS num_male,
      SUM(IFF(UPPER(gender) = 'FEMALE', 1, 0)) AS num_female,
      SUM(IFF(UPPER(gender) NOT IN ('MALE','FEMALE'), 1, 0)) AS num_other_gender,
      -- Metadata columns for data lineage and auditing
      CURRENT_TIMESTAMP() AS materialized_at,
      'silver.employee_data_stg' AS source_object,
      UUID_STRING() AS etl_run_id
    FROM employees
    GROUP BY department
    ORDER BY department;

    -- Get row count for logging
    SELECT COUNT(*) INTO row_count FROM gold.employee_demographics_by_department;
    
    result_message := 'SUCCESS: Built employee_demographics_by_department with ' || 
                     row_count || ' rows in ' || 
                     DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) || ' seconds';
    
    RETURN result_message;
    
  EXCEPTION
    WHEN OTHER THEN
      result_message := 'ERROR: Failed to build employee_demographics_by_department - ' || SQLERRM;
      RETURN result_message;
  END;
END;
$$;

-- Procedure 2: Build survey_results_by_department table
CREATE OR REPLACE PROCEDURE gold.build_survey_results_by_department()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  start_time TIMESTAMP := CURRENT_TIMESTAMP();
  row_count INTEGER;
  result_message STRING;
BEGIN
  BEGIN
    -- Build survey results using temp table approach
    CREATE OR REPLACE TEMP TABLE tmp_survey_results_by_department AS
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
      'silver.employee_data_stg' AS source_object,
      UUID_STRING() AS etl_run_id
    FROM silver.employee_data_stg
    GROUP BY department;

    -- Create final table from temp table
    CREATE OR REPLACE TABLE gold.survey_results_by_department AS
    SELECT * FROM tmp_survey_results_by_department;

    -- Get row count for logging
    SELECT COUNT(*) INTO row_count FROM gold.survey_results_by_department;
    
    result_message := 'SUCCESS: Built survey_results_by_department with ' || 
                     row_count || ' rows in ' || 
                     DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) || ' seconds';
    
    RETURN result_message;
    
  EXCEPTION
    WHEN OTHER THEN
      result_message := 'ERROR: Failed to build survey_results_by_department - ' || SQLERRM;
      RETURN result_message;
  END;
END;
$$;

-- ================================================================
-- MASTER ORCHESTRATION PROCEDURE: Calls individual table procedures
-- This provides a single entry point while maintaining modularity
-- ================================================================

CREATE OR REPLACE PROCEDURE gold.materialize_core_models()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  overall_start_time TIMESTAMP := CURRENT_TIMESTAMP();
  demographics_result STRING;
  survey_result STRING;
  final_result STRING;
  error_count INTEGER := 0;
BEGIN
  -- Call individual table build procedures
  CALL gold.build_employee_demographics_by_department() INTO demographics_result;
  CALL gold.build_survey_results_by_department() INTO survey_result;
  
  -- Check for errors
  IF (demographics_result LIKE 'ERROR:%') THEN
    error_count := error_count + 1;
  END IF;
  
  IF (survey_result LIKE 'ERROR:%') THEN
    error_count := error_count + 1;
  END IF;
  
  -- Build final result message
  final_result := '=== GOLD LAYER BUILD SUMMARY ===\n' ||
                  'Total execution time: ' || DATEDIFF('second', overall_start_time, CURRENT_TIMESTAMP()) || ' seconds\n' ||
                  'Errors encountered: ' || error_count || '\n\n' ||
                  'DEMOGRAPHICS TABLE: ' || demographics_result || '\n' ||
                  'SURVEY TABLE: ' || survey_result;
  
  RETURN final_result;
END;
$$;

-- ================================================================
-- USAGE EXAMPLES AND BENEFITS OF MODULAR APPROACH
-- ================================================================

-- Option 1: Run individual table builds (useful for debugging or selective rebuilds)
CALL gold.build_employee_demographics_by_department();
CALL gold.build_survey_results_by_department();

-- Option 2: Run all tables via master procedure (recommended for production)
CALL gold.materialize_core_models();

-- Final checks - verify both tables were built successfully
SELECT COUNT(*) AS demographics_rows FROM employee_demographics_by_department;
SELECT COUNT(*) AS survey_rows FROM survey_results_by_department;


