-- ========================================================================
-- Module 1 Clip 2 Demo: Ingestion Checks
-- Demo: Common issues at load time using different file formats
-- ========================================================================

-- Set up the environment
USE ROLE sysadmin;
USE WAREHOUSE COMPUTE_WH;

-- ========================================================================
-- ENVIRONMENT SETUP
-- ========================================================================

-- Create the course database
CREATE DATABASE IF NOT EXISTS SNOW_COURSE;
USE DATABASE SNOW_COURSE;

-- Create a schema for our work
CREATE SCHEMA IF NOT EXISTS data_quality;
USE SCHEMA data_quality;

-- Create internal stage for data imports
CREATE STAGE IF NOT EXISTS dataimports;

-- List files in stage (after you upload them)
LIST @dataimports;

-- ========================================================================
-- TABLE DEFINITIONS
-- ========================================================================

-- Table for CSV employee absence data
CREATE OR REPLACE TABLE employee_absence (
    employee_number INT,
    employee_name VARCHAR(255),
    gender VARCHAR(10),
    city VARCHAR(100),
    job_title VARCHAR(100),
    department VARCHAR(100),
    store_location VARCHAR(100),
    business_unit VARCHAR(100),
    division VARCHAR(100),
    age INT,
    length_of_service INT,
    hours_absent INT
);

-- Table for JSON employee data
CREATE OR REPLACE TABLE employee_data_json (
    employee_number VARCHAR(50),
    employee_name VARCHAR(255),
    gender VARCHAR(50),
    city VARCHAR(255),
    job_title VARCHAR(255),
    department VARCHAR(255),
    store_location VARCHAR(255),
    business_unit VARCHAR(255),
    division VARCHAR(255),
    age INTEGER,
    length_of_service INTEGER,
    hours_absent INTEGER,
    engagement_survey VARIANT
);

-- Table for Parquet employee data
CREATE OR REPLACE TABLE employee_data_parquet (
    employee_number BIGINT,
    employee_name VARCHAR(255),
    gender VARCHAR(50),
    city VARCHAR(255),
    job_title VARCHAR(255),
    department VARCHAR(255),
    store_location VARCHAR(255),
    business_unit VARCHAR(255),
    division VARCHAR(255),
    age BIGINT,
    length_of_service BIGINT,
    hours_absent BIGINT,
    engagement_survey VARIANT
);

-- ========================================================================
-- DEMO 1: CSV FILE LOADING WITH VALIDATION
-- ========================================================================

-- Create file format for CSV
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- ========================================================================
-- 1A: Load CSV with Wrong Data Type (Error Case) - Compare Validation Modes
-- ========================================================================
-- This file has 'Sales' in a numeric field - let's see how different validation modes handle it

-- Option 1: RETURN_ERRORS
COPY INTO employee_absence
FROM @dataimports/employee_absence_wrong_type.csv
FILE_FORMAT = (FORMAT_NAME = csv_format)
VALIDATION_MODE = 'RETURN_ERRORS';

-- Option 3: RETURN_10_ROWS (See what valid rows would look like)
COPY INTO employee_absence
FROM @dataimports/employee_absence_wrong_type.csv
FILE_FORMAT = (FORMAT_NAME = csv_format)
VALIDATION_MODE = 'RETURN_10_ROWS';

-- Option 3: RETURN_2_ROWS (See what valid rows would look like)
COPY INTO employee_absence
FROM @dataimports/employee_absence_wrong_type.csv
FILE_FORMAT = (FORMAT_NAME = csv_format)
VALIDATION_MODE = 'RETURN_2_ROWS';

-- Now try actual load with different error handling
COPY INTO employee_absence
FROM @dataimports/employee_absence_wrong_type.csv
FILE_FORMAT = (FORMAT_NAME = csv_format)
ON_ERROR = 'SKIP_FILE';

-- Now try actual load with 'CONTINUE' option
COPY INTO employee_absence
FROM @dataimports/employee_absence_wrong_type.csv
FILE_FORMAT = (FORMAT_NAME = csv_format)
ON_ERROR = 'CONTINUE';

-- ========================================================================
-- DEMO 2: JSON FILE LOADING WITH VALIDATION
-- ========================================================================

-- Create file format for JSON
CREATE OR REPLACE FILE FORMAT json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;

-- ========================================================================
-- 2A: Load JSON with Wrong Data Type (Error Case)
-- ========================================================================
-- This file has "Operations" for Ava Wilson's age. 

-- Step 1: Validate the file to see the errors (no data loaded)
COPY INTO employee_data_json
FROM '@dataimports/employee_data - wrong_data_type.json'
FILE_FORMAT = (FORMAT_NAME = json_format)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
VALIDATION_MODE = 'RETURN_ERRORS';

COPY INTO employee_data_json
FROM '@dataimports/employee_data - wrong_data_type.json'
FILE_FORMAT = (FORMAT_NAME = json_format)
VALIDATION_MODE = 'RETURN_ERRORS';

COPY INTO employee_data_json
FROM '@dataimports/employee_data - wrong_data_type.json'
FILE_FORMAT = (FORMAT_NAME = json_format)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR='ABORT_STATEMENT';

-- Step 2: Load the file, continuing on errors, to inspect the problematic data
COPY INTO employee_data_json
FROM '@dataimports/employee_data - wrong_data_type.json'
FILE_FORMAT = (FORMAT_NAME = json_format)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';

-- Step 3: Check for data type issues in the loaded data
SELECT employee_number, employee_name, age, TRY_CAST(age AS INTEGER) as age_as_int
FROM employee_data_json
WHERE employee_name = 'Ava Wilson';

SELECT 
    *
FROM employee_data_json
LIMIT 5;

TRUNCATE TABLE employee_data_json;


-- ========================================================================
-- 2B: Load JSON with Wrong Structure (Incomplete Data)
-- ========================================================================
-- This will load but with incomplete engagement survey data for a particular record
COPY INTO employee_data_json
FROM '@dataimports/employee_data - wrong_structure.json'
FILE_FORMAT = (FORMAT_NAME = json_format)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'ABORT_STATEMENT';

-- Check for incomplete variant data
SELECT 
    *
FROM employee_data_json
WHERE employee_name = 'John Smith' -- missing work life balance score
UNION
SELECT 
* FROM employee_data_json
WHERE employee_name = 'Olivia Richardson'; -- complete record

-- ========================================================================
-- DEMO 3: PARQUET FILE LOADING WITH VALIDATION
-- ========================================================================

-- ========================================================================
-- 3A: Load Multiple Parquet Files with One Corrupted (Error Case)
-- ========================================================================
-- This will attempt to load 5 parquet files, where part_03 is intentionally corrupted.

-- Step 1: Validate the set of files (no data loaded yet)
COPY INTO employee_data_parquet
FROM @dataimports/parquet/
FILE_FORMAT = (TYPE = 'PARQUET')
PATTERN = '.*employee_data_part_.*\\.parquet'
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'ABORT_STATEMENT';

SELECT * FROM employee_data_parquet;

-- Step 2: Load the files, skipping the corrupted one
TRUNCATE TABLE IF EXISTS employee_data_parquet;
COPY INTO employee_data_parquet
FROM @dataimports/parquet/
FILE_FORMAT = (TYPE = 'PARQUET')
PATTERN = '.*employee_data_part_.*\\.parquet'
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'SKIP_FILE';

-- Step 3: Check which files were loaded and which were skipped
SELECT COUNT(*) AS loaded_records FROM employee_data_parquet;

SELECT 
    *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    table_name => 'EMPLOYEE_DATA_PARQUET',
    start_time => dateadd(minutes, -5, current_timestamp())
))
ORDER BY file_name;