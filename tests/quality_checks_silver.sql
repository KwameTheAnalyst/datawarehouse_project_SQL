/*
=====================================================================
Quality Checks
=====================================================================
Script Purpose:
  This script performs various quality checks for data consistency, 
  accuracy, and standardization across the Silver Schema. 
  It includes checks for:
    - Null and Duplicate Primary Keys
    - Unwanted spaces in String Fields
    - Data Standardization and Consistency
    - Invalid date range and orders
    - Data consistency between related fields

Usage Notes
  - Run these checks after loading the Silver Layer.
  - Investigate and resolve any discripancies found during the checks.
======================================================================
*/

-- =================================
-- Check silver.crm_cust_info
-- =================================

-- check for Null or Duplicates in Primary Key
-- Expectation: No Results
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- check for white spaces around values
-- Expectation: No Results
SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key)

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- Normalize gender values and handle unknown cases
-- Expectation: No Results
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT cst_material_status
FROM silver.crm_cust_info

-- =================================
-- Check silver.crm_prod_info
-- =================================

-- check for Null or Duplicates in Primary Key
-- Expectation: No Results
SELECT prd_id, COUNT(*)
FROM silver.crm_prod_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- check for white spaces around values
-- Expectation: No Results
SELECT prd_key
FROM silver.crm_prod_info
WHERE prd_key != TRIM(prd_key)

-- Check incorrect dates specification
-- Expectation: No Results
SELECT prd_start_dt, prd_end_dt
FROM bronze.crm_prod_info
WHERE prd_end_dt > prd_start_dt

-- Normalize gender values and handle unknown cases
-- Expectation: No Results
SELECT DISTINCT prd_line
FROM silver.crm_prod_info

SELECT DISTINCT prd_nm
FROM silver.crm_prod_info

-- =================================
-- Check silver.crm_sales_data
-- =================================

-- check for Null or Duplicates in Primary Key
-- Expectation: No Results
SELECT prd_id, COUNT(*)
FROM silver.crm_sales_data
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- check for white spaces around values
-- Expectation: No Results
SELECT prd_key
FROM silver.crm_prod_info
WHERE prd_key != TRIM(prd_key)

-- Check incorrect dates specification
-- Expectation: No Results
SELECT prd_start_dt, prd_end_dt
FROM bronze.crm_prod_info
WHERE prd_end_dt > prd_start_dt

-- Normalize gender values and handle unknown cases
-- Expectation: No Results
SELECT DISTINCT prd_line
FROM silver.crm_prod_info

SELECT DISTINCT prd_nm
FROM silver.crm_prod_info

SELECT *
FROM silver.crm_sales_data

-- Check Mathematical accuracy
-- Expectation: No Results
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_data
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- =================================
-- Check silver.erp_cust_az12
-- =================================
  
-- check for Null or Duplicates in Primary Key
-- Expectation: No Results
SELECT DISTINCT gen, COUNT(*)
FROM silver.erp_cust_az12
GROUP BY gen

-- handled invalid values or 
-- identify out of range dates
-- Expectation: No Results
SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate > GETDATE()

-- handled INVALID VALUES
-- Expectation: No Results
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid
	END AS cid,
bdate,
gen
FROM silver.erp_cust_az12

-- Data normalization by mapping the code to more friendly values
-- Expectation: No Results
SELECT DISTINCT gen, COUNT(*)
FROM silver.erp_cust_az12
GROUP BY gen

-- =================================
-- Check silver.erp_loc_a101
-- =================================

-- DATA STANDARDIZATION AND CONSISTENCY
-- Expectation: No Results
SELECT DISTINCT cntry, COUNT(*)
FROM silver.erp_loc_a101
GROUP BY cntry
ORDER BY cntry

