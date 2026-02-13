-- Module 2, Demo 1: Building Staging Tables (Silver Layer)
-- Raw source: bronze.employee_data_parquet (loaded via Module 1 demos)

-- Environment Setup
USE ROLE SYSADMIN;
USE DATABASE SNOW_ELT_COURSE;

-- Create and use the silver schema
CREATE SCHEMA IF NOT EXISTS silver;
USE SCHEMA silver;

-- Verify raw bronze table exists
SHOW TABLES IN SCHEMA bronze STARTS WITH 'EMPLOYEE_DATA_PARQUET';
SELECT * FROM bronze.employee_data_parquet LIMIT 5;

-- ================================================================
-- STAGING TABLE CREATION
-- Create a staging table using the bronze table structure as a base
-- Then add parsed engagement_survey fields and metadata columns
-- ================================================================

-- Base structure from bronze
CREATE OR REPLACE TABLE employee_data_stg LIKE bronze.employee_data_parquet;

-- Add parsed survey fields (normalized into scalar columns)
ALTER TABLE employee_data_stg ADD COLUMN satisfaction_score INTEGER;
ALTER TABLE employee_data_stg ADD COLUMN work_life_balance_score INTEGER;
ALTER TABLE employee_data_stg ADD COLUMN career_growth_score INTEGER;
ALTER TABLE employee_data_stg ADD COLUMN communication_score INTEGER;
ALTER TABLE employee_data_stg ADD COLUMN teamwork_score INTEGER;

-- Add metadata columns
ALTER TABLE employee_data_stg ADD COLUMN staged_at TIMESTAMP_NTZ;
ALTER TABLE employee_data_stg ADD COLUMN source_object STRING DEFAULT 'bronze.employee_data_parquet';
ALTER TABLE employee_data_stg ADD COLUMN etl_run_id STRING;

-- ================================================================
-- STAND-ALONE INSERT INTO ... SELECT
-- Populate staging table by parsing the VARIANT engagement_survey field
-- ================================================================

INSERT INTO employee_data_stg (
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
  CURRENT_TIMESTAMP(),
  'bronze.employee_data_parquet',
  UUID_STRING()
FROM bronze.employee_data_parquet;

-- Verify results
SELECT * FROM employee_data_stg LIMIT 20;

-- ================================================================
-- STORED PROCEDURE: Encapsulate staging load logic
-- Allows re-running the staging step with a single call
-- ================================================================

TRUNCATE TABLE silver.employee_data_stg;

CREATE OR REPLACE PROCEDURE silver.load_employee_staging(etl_run_id STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  INSERT INTO silver.employee_data_stg (
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
    CURRENT_TIMESTAMP(),
    'bronze.employee_data_parquet',
    :etl_run_id
  FROM bronze.employee_data_parquet;

  RETURN 'Loaded ' || SQLROWCOUNT || ' rows into silver.employee_data_stg with etl_run_id=' || :etl_run_id;
END;
$$;

-- Call the procedure
CALL silver.load_employee_staging(UUID_STRING());

-- Inspect a few rows to verify parsing/metadata
SELECT
  employee_number,
  employee_name,
  department,
  satisfaction_score,
  work_life_balance_score,
  career_growth_score,
  communication_score,
  teamwork_score,
  staged_at,
  etl_run_id
FROM employee_data_stg
ORDER BY staged_at DESC
LIMIT 20;