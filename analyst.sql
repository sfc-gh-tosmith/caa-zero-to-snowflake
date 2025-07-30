/*
 * Overview of Snowflake Getting Started Lab Script
 * --------------------------------------------------------------------------------
 * This script demonstrates core Snowflake features through a hands-on lab following
 * the "Getting Started with Snowflake Quickstart" tutorial. It covers essential
 * concepts that make Snowflake unique in the cloud data platform landscape.
 *
 * Key learning objectives:
 * 1. Loading structured data (CSV files) using Snowflake's native data loading capabilities
 * 2. Working with semi-structured data (JSON) using Snowflake's VARIANT data type
 * 3. Accessing external data through Snowflake Marketplace
 * 4. Understanding Snowflake's automatic query result caching
 * 5. Exploring zero-copy cloning for instant data copies
 * 6. Using Time Travel for data recovery and historical analysis
 * 7. Managing security with roles and privileges
 * 8. Data sharing capabilities unique to Snowflake
 *
 * Unique Snowflake Features Highlighted:
 * - VARIANT data type for semi-structured data (JSON, XML, Avro, etc.)
 * - Automatic query result caching without configuration
 * - Zero-copy cloning for instant table/database copies
 * - Time Travel for accessing historical data states
 * - Native cloud architecture with separation of compute and storage
 * - Snowflake Marketplace for accessing third-party data
 * - Secure data sharing without data movement
 *
 * Use Command+F to search for sections using the following markers:
 * - SECTION: STRUCTURED DATA LOADING
 * - SECTION: SEMI-STRUCTURED DATA
 * - SECTION: MARKETPLACE DATA
 * - SECTION: QUERYING AND CACHING
 * - SECTION: TIME TRAVEL
 * - SECTION: SECURITY AND ROLES
 * - SECTION: DATA SHARING
 * - SECTION: CLEANUP
 */

-- This notebook follows the "Getting Started with Snowflake Quickstart" found at https://quickstarts.snowflake.com/guide/getting_started_with_snowflake/index.html?index=..%2F..index#5

-- Steps start at step 5 of the Quickstart

-- SECTION: STRUCTURED DATA LOADING
-- **** Step 5. Loading Structured Data into Snowflake: CSVs ****

-- Create the database in the UI, or use below command
--CREATE DATABASE CYBERSYN;

-- Set context to use CYBERSYN database
-- Context setting is important in Snowflake - it establishes the working database/schema
use database cybersyn;


-- Create table to hold the company metadata
-- Note: Snowflake supports standard SQL DDL with additional data types like VARIANT for semi-structured data
CREATE OR REPLACE TABLE company_metadata
(cybersyn_company_id string,
company_name string,
permid_security_id string,
primary_ticker string,
security_name string,
asset_class string,
primary_exchange_code string,
primary_exchange_name string,
security_status string,
global_tickers variant,        -- VARIANT: Snowflake's unique data type for semi-structured data
exchange_code variant,         -- Can store JSON, XML, Avro, ORC, Parquet data natively
permid_quote_id variant);


-- Here we use the UI to create an external stage referencing a public S3 bucket
-- When creating bucket, it's important to note that in a real world scenario, this bucket would be locked down with authentication policies
-- In the UI, go to Databases > CYBERSYN > Schemas > PUBLIC > Create Stage > Amazon S3
-- Bucket name: cybersyn_company_metadata
-- Bucket URL: s3://sfquickstarts/zero_to_snowflake/cybersyn-consumer-company-metadata-csv/
-- Then click Create


-- File Format: Snowflake's way to define how to parse files during loading
CREATE OR REPLACE FILE FORMAT csv -- FF tells Snowflake the "expected layout" https://docs.snowflake.com/en/sql-reference/sql/create-file-format#label-create-or-alter-file-format-syntax
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'  -- Snowflake automatically detects compression (gzip, bzip2, etc.)
    FIELD_DELIMITER = ','  -- Specifies comma as the field delimiter
    RECORD_DELIMITER = '\n'  -- Specifies newline as the record delimiter
    SKIP_HEADER = 1  -- Skip the first line (common for CSV headers)
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042'  -- Fields are optionally enclosed by double quotes (ASCII code 34)
    TRIM_SPACE = FALSE  -- Spaces are not trimmed from fields
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  -- Flexible loading - doesn't fail if column counts vary
    ESCAPE = 'NONE'  -- No escape character for special character escaping
    ESCAPE_UNENCLOSED_FIELD = '\134'  -- Backslash is the escape character for unenclosed fields
    DATE_FORMAT = 'AUTO'  -- Snowflake automatically detects date formats
    TIMESTAMP_FORMAT = 'AUTO'  -- Snowflake automatically detects timestamp formats
    NULL_IF = ('')  -- Treats empty strings as NULL values
    COMMENT = 'File format for ingesting data for zero to snowflake';

-- Show all file formats in the current database
SHOW FILE FORMATS IN DATABASE cybersyn;

-- COPY INTO: Snowflake's primary data loading command
-- This loads data from internal, Snowflake-managed stages, or external stages (S3, Azure, GCS) directly into tables
COPY INTO company_metadata FROM @cybersyn_company_metadata 
    file_format=csv 
    PATTERN = '.*csv.*'  -- Pattern matching for selective file loading
    ON_ERROR = 'CONTINUE';  -- Continue loading even if some records fail
    -- https://docs.snowflake.com/en/sql-reference/sql/copy-into-table

SELECT * FROM COMPANY_METADATA;
    
-- SECTION: SEMI-STRUCTURED DATA
-- **** Step 6. Loading Semi-Structured Data into Snowflake: JSONs ****

-- Create tables that will hold our semi-structured data. Using VARIANT data type - this allows native JSON querying
CREATE TABLE sec_filings_index (v variant);

CREATE TABLE sec_filings_attributes (v variant);

-- External Stage: Points to cloud storage (S3, Azure, GCS) without moving data
-- Snowflake can query data directly from cloud storage or load it into tables
-- This is the same thing we did before, but with SQL code instead of the UI
CREATE STAGE cybersyn_sec_filings
url = 's3://sfquickstarts/zero_to_snowflake/cybersyn_cpg_sec_filings/';

select $1, $2, $3 from @cybersyn_sec_filings/cybersyn_sec_report_index.json.gz limit 5;

-- Loading JSON data - notice the strip_outer_array parameter
-- This is useful when JSON files contain arrays at the root level
COPY INTO sec_filings_index
FROM @cybersyn_sec_filings/cybersyn_sec_report_index.json.gz
    file_format = (type = json strip_outer_array = true);

COPY INTO sec_filings_attributes -- https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
FROM @cybersyn_sec_filings/cybersyn_sec_report_attributes.json.gz
    file_format = (type = json strip_outer_array = true);

SELECT * FROM sec_filings_index LIMIT 10;
SELECT * FROM sec_filings_attributes LIMIT 10;

-- Create structured views over semi-structured data
-- Notice the v:FIELD_NAME::datatype syntax - this is Snowflake's JSON path notation. Docs: https://docs.snowflake.com/en/user-guide/querying-semistructured
CREATE OR REPLACE VIEW sec_filings_index_view AS -- CREATE VIEW docs: https://docs.snowflake.com/en/sql-reference/sql/create-view
SELECT
    v:CIK::string                   AS cik,           -- JSON path extraction with type casting
    v:COMPANY_NAME::string          AS company_name,  -- :: syntax converts VARIANT to specific types
    v:EIN::int                      AS ein,
    v:ADSH::string                  AS adsh,
    v:TIMESTAMP_ACCEPTED::timestamp AS timestamp_accepted,
    v:FILED_DATE::date              AS filed_date,
    v:FORM_TYPE::string             AS form_type,
    v:FISCAL_PERIOD::string         AS fiscal_period,
    v:FISCAL_YEAR::string           AS fiscal_year
FROM sec_filings_index;

CREATE OR REPLACE VIEW sec_filings_attributes_view AS
SELECT
    v:VARIABLE::string            AS variable,
    v:CIK::string                 AS cik,
    v:ADSH::string                AS adsh,
    v:MEASURE_DESCRIPTION::string AS measure_description,
    v:TAG::string                 AS tag,
    v:TAG_VERSION::string         AS tag_version,
    v:UNIT_OF_MEASURE::string     AS unit_of_measure,
    v:VALUE::string               AS value,
    v:REPORT::int                 AS report,
    v:STATEMENT::string           AS statement,
    v:PERIOD_START_DATE::date     AS period_start_date,
    v:PERIOD_END_DATE::date       AS period_end_date,
    v:COVERED_QTRS::int           AS covered_qtrs,
    TRY_PARSE_JSON(v:METADATA)    AS metadata  -- TRY_PARSE_JSON safely handles malformed JSON
FROM sec_filings_attributes;

SELECT *
FROM sec_filings_index_view
LIMIT 20;

SELECT *
FROM sec_filings_attributes_view
LIMIT 20;


-- SECTION: MARKETPLACE DATA
-- **** Step 7. Getting Data from Snowflake Marketplace **** 

-- Marketplace listing link: https://app.snowflake.com/marketplace/listing/GZTSZAS2KF7/snowflake-public-data-products-finance-economics
-- Search for 'snowflake economics'
-- Snowflake Marketplace allows access to third-party data without ETL
-- Data providers share live, always-fresh data that appears as databases in your account


-- SECTION: QUERYING AND CACHING
-- **** Step 8. Querying, the Results Cache, & Cloning ****

-- Simple query to show table contents
SELECT * FROM company_metadata;

-- Complex analytical query demonstrating window functions and joins
SELECT -- The daily return of a stock (the percent change in the stock price from the close of the previous day to the close of the current day) and 5-day moving average from closing prices
    meta.primary_ticker,
    meta.company_name,
    ts.date,
    ts.value AS post_market_close,
    -- LAG function: Access previous row's value within a partition
    (ts.value / LAG(ts.value, 1) OVER (PARTITION BY meta.primary_ticker ORDER BY ts.date))::DOUBLE AS daily_return,
    -- Moving average using window frame: ROWS BETWEEN gives precise control over window bounds
    AVG(ts.value) OVER (PARTITION BY meta.primary_ticker ORDER BY ts.date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS five_day_moving_avg_price
FROM Financial__Economic_Essentials.cybersyn.stock_price_timeseries ts  -- Marketplace data access
INNER JOIN company_metadata meta
ON ts.ticker = meta.primary_ticker
WHERE ts.variable_name = 'Post-Market Close';

-- **** Enhanced AI SQL Functions Demo ****
-- Demonstrating Snowflake's new AI-powered SQL functions for intelligent data analysis. See AISQL documentation https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql

-- AI_AGG can look across all records and synthesize information. See AI_AGG documentation https://docs.snowflake.com/en/sql-reference/functions/ai_agg
SELECT ai_agg(companies.company_name, 'Give me a breakdown of the industries represented by this list of companies: {0}') as industries_breakdown
FROM 
(
SELECT DISTINCT meta.company_name
FROM Financial__Economic_Essentials.cybersyn.stock_price_timeseries ts  -- Marketplace data access
INNER JOIN company_metadata meta
) companies; 


-- Results Cache Demo: Run this query twice to demonstrate Snowflake's automatic result caching
-- First run uses compute warehouse, second run returns cached results instantly (24-hour cache)
SELECT -- Trading volume change from one day to the next. Results cache https://docs.snowflake.com/en/user-guide/querying-persisted-results
    meta.primary_ticker,
    meta.company_name,
    ts.date,
    ts.value AS nasdaq_volume,
    (ts.value / LAG(ts.value, 1) OVER (PARTITION BY meta.primary_ticker ORDER BY ts.date))::DOUBLE AS volume_change
FROM Financial__Economic_Essentials.cybersyn.stock_price_timeseries ts
INNER JOIN company_metadata meta
ON ts.ticker = meta.primary_ticker
WHERE ts.variable_name = 'Nasdaq Volume';


-- Zero-Copy Cloning: Instantly create a copy without duplicating data
-- This is a unique Snowflake feature - clones share underlying data until modified
CREATE TABLE company_metadata_dev CLONE company_metadata; -- https://docs.snowflake.com/en/user-guide/tables-storage-considerations#label-cloning-tables
-- Use case: Create dev/test environments, data science experimentation, backup before changes

-- Complex query with CTE (Common Table Expression) and advanced window functions
-- Joining tables/views
WITH data_prep AS (
    SELECT 
        idx.cik,
        idx.company_name,
        idx.adsh,
        idx.form_type,
        att.measure_description,
        CAST(att.value AS DOUBLE) AS value,
        att.period_start_date,
        att.period_end_date,
        att.covered_qtrs,
        TRIM(att.metadata:"ProductOrService"::STRING) AS product  -- JSON notation for nested objects
    FROM sec_filings_attributes_view att
    JOIN sec_filings_index_view idx
        ON idx.cik = att.cik AND idx.adsh = att.adsh
    WHERE idx.cik = '0001637459'
        AND idx.form_type IN ('10-K', '10-Q')
        AND LOWER(att.measure_description) = 'net sales'
        AND (att.metadata IS NULL OR OBJECT_KEYS(att.metadata) = ARRAY_CONSTRUCT('ProductOrService')) -- OBJECT_KEYS: Get JSON object keys
        AND att.covered_qtrs IN (1, 4)
        AND value > 0
    QUALIFY ROW_NUMBER() OVER (  -- QUALIFY: Filter after window functions (like HAVING for window functions)
        PARTITION BY idx.cik, idx.company_name, att.measure_description, att.period_start_date, att.period_end_date, att.covered_qtrs, product
        ORDER BY idx.filed_date DESC
    ) = 1
)
SELECT
    company_name,
    measure_description,
    product,
    period_end_date,
    CASE
        WHEN covered_qtrs = 1 THEN value
        WHEN covered_qtrs = 4 THEN value - SUM(value) OVER (
            PARTITION BY cik, measure_description, product, YEAR(period_end_date)
            ORDER BY period_end_date
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        )
    END AS quarterly_value
FROM data_prep
ORDER BY product, period_end_date;


-- SECTION: TIME TRAVEL
-- **** Step 9. Using Time Travel**** 
-- Time travel documentation: https://docs.snowflake.com/en/user-guide/data-time-travel
-- Time Travel: Unique Snowflake feature allowing access to historical data states

-- Simulate accidental table drop
DROP TABLE sec_filings_index;

-- Prove that it's gone
SELECT * FROM sec_filings_index LIMIT 10;

-- UNDROP: Restore dropped objects (works for tables, schemas, databases)
-- This uses Snowflake's Time Travel feature under the hood
UNDROP TABLE sec_filings_index;

-- Prove that it's back
SELECT * FROM sec_filings_index LIMIT 10;


-- Demonstrate ability to roll back to previous state
UPDATE company_metadata SET company_name = 'oops';

SELECT *
FROM company_metadata;

-- Capture the query ID of the UPDATE statement for Time Travel reference
SET query_id = (
  SELECT query_id
  FROM TABLE(information_schema.query_history_by_session(result_limit=>5))
  WHERE query_text LIKE 'UPDATE%'
  ORDER BY start_time DESC
  LIMIT 1
);

-- Time Travel: Restore table to state BEFORE a specific statement
-- This recreates the table with data from before the UPDATE operation
CREATE OR REPLACE TABLE company_metadata AS
SELECT *
FROM company_metadata
BEFORE (STATEMENT => $query_id);        -- BEFORE clause: Access historical data states
-- https://docs.snowflake.com/en/sql-reference/constructs/at-before
-- Alternative syntax: AT(TIMESTAMP => '2023-01-01 12:00:00'::timestamp)

-- Verify the company names have been restored
SELECT * FROM company_metadata;

-- SECTION: SECURITY AND ROLES
-- ** Step 10. Working with Roles, Account Admin, & Account Usage **

-- It's likely that an Analyst persona would not have the privileges necessary to execute the below code. The code is given for demonstration purposes only. 

-- Admins can visualize roles in Admin -> Users & Roles -> Roles -> Graph

-- Role-Based Access Control (RBAC): Snowflake's security model
-- ACCOUNTADMIN: Highest privilege role, use sparingly
USE ROLE ACCOUNTADMIN;

-- Create a new role - roles are containers for privileges
CREATE ROLE junior_dba;

-- Grant role to user - users can have multiple roles
GRANT ROLE junior_dba TO USER TOSMITH;

-- Switch to the new role to test permissions
USE ROLE junior_dba;
SELECT * FROM COMPANY_METADATA;  -- This will fail - no warehouse access

-- Grant warehouse usage to the role
USE ROLE accountadmin;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE junior_dba;

-- Now the junior_dba role can use the warehouse
USE ROLE junior_dba;
USE WAREHOUSE compute_wh;


-- SECTION: DATA SHARING
-- **** Step 11. Sharing Data Securely via Snowflake Marketplace ****

-- It's likely that an Analyst persona would not have the privileges necessary to create a share. The code is given for demonstration purposes only.

-- Data Sharing: Unique Snowflake feature allowing secure data sharing without data movement
-- Shared data is live and always current - no ETL needed
-- Navigate to "Data Products" then "Private Sharing" then "Create a Direct Share". Must be ACCOUNTADMIN or have sharing privileges

-- SECTION: CLEANUP
-- **** Step 12. Resetting Your Snowflake Environment ****
USE ROLE accountadmin;

DROP DATABASE IF EXISTS CYBERSYN;

DROP WAREHOUSE IF EXISTS analytics_wh;

DROP ROLE IF EXISTS junior_dba;