/*
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
Stored Procedure: Load Silver Layer (Broze -> Silver)
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
Script Purpose:
  This script performs ETL (Extract, Transform, Load) process to populate the 
  Silver Schema tables from the Bronze Schema

Actions Performed
  - Truncate the Silver tables
  - Insert Transformed and Cleansed Data from Bronze into the Silver Tables.

Parameters:
  None, 
  This stored procedure does not accept or return any values.

Usage
  EXEC silver.load_silver
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time AS DATETIME, @end_time AS DATETIME, @start_batch_time AS DATETIME, @end_batch_time AS DATETIME
	BEGIN TRY
		/* insert into the silver table the cleaned data from bronze table */
		SET @start_batch_time = GETDATE();
		PRINT '=================================================================';
		PRINT 'LOADING THE SILVER LAYER';
		PRINT '=================================================================';

		PRINT '-----------------------------------------------------------------';
		PRINT 'LOADING THE CRM TABLES';
		PRINT '-----------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING silver.crm_cust_info TABLE'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> INSERTING DATA INTO silver.crm_cust_info TABLE'
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date
			)
			SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
				END cst_material_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' Then 'male'
				ELSE 'n/a'
				END cst_gndr,
			cst_create_date
			FROM (
				SELECT *, 
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
				FROM bronze.crm_cust_info
				WHERE cst_id IS NOT NULL)t
			WHERE flag_last = 1;
		SET @end_time = GETDATE();
		PRINT '>> Loading Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';	
		SELECT * FROM silver.crm_cust_info;
		SELECT COUNT(*) FROM silver.crm_cust_info;
		PRINT '----------------------------------';


		/* INSERT INTO THE SILVER (Product Info) TABLE */
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING silver.crm_prod_info TABLE'
		TRUNCATE TABLE silver.crm_prod_info;
		PRINT '>> INSERTING DATA INTO silver.crm_prod_info TABLE'
		INSERT INTO silver.crm_prod_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT 
			prd_id,	
			REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				 WHEN 'M' THEN 'Mountain'
				 WHEN 'R' THEN 'Road'
				 WHEN 'S' THEN 'Other Sales'
				 WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
			END AS prd_line,
			prd_start_dt,
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt
		FROM bronze.crm_prod_info;
		SET @end_time = GETDATE();
		SELECT * FROM silver.crm_prod_info;
		SELECT COUNT(*) FROM silver.crm_prod_info;
		PRINT '----------------------------------';


		-- INSERT INTO THE SILVER (SALES DATA) TABLE
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING silver.crm_sales_data TABLE'
		TRUNCATE TABLE silver.crm_sales_data;
		PRINT '>> INSERTING DATA INTO silver.crm_sales_data TABLE'
		INSERT INTO silver.crm_sales_data (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt)!=8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS varchar(50)) AS date)
			END sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt)!=8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS varchar(50)) AS date)
			END sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt)!=8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS varchar(50)) AS date)
			END sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity,0)
				ELSE sls_price
				END AS sls_price
		FROM bronze.crm_sales_data;
		SET @end_time = GETDATE();
		SELECT * FROM silver.crm_sales_data;
		SELECT COUNT(*) FROM silver.crm_sales_data;
		PRINT '----------------------------------';


		PRINT '-----------------------------------------------------------------';
		PRINT 'LOADING THE ERP TABLES';
		PRINT '-----------------------------------------------------------------';


		-- INSERTING DATA INTO silver.erp_cust_az12 TABLE
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING silver.erp_cust_az12 TABLE'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> INSERTING DATA INTO silver.erp_cust_az12 TABLE'
		INSERT INTO silver.erp_cust_az12 (
		cid, 
		bdate, 
		gen)
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
			END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		SELECT * FROM silver.erp_cust_az12;
		SELECT COUNT(*) FROM bronze.erp_cust_az12;
		PRINT '----------------------------------';


		-- INSERTING DATA INTO silver.erp_cust_az12 TABLE
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING silver.erp_loc_a101 TABLE'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> INSERTING DATA INTO silver.erp_loc_a101 TABLE'
		INSERT INTO silver.erp_loc_a101(
		cid, cntry)
		SELECT 
		REPLACE(cid, '-','') AS cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
			END AS cntry	-- REMOVED UNWANTED SPACES, NORMALIZED AND HANDLED MISSING OR BLANK COUNTY CODES
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		SELECT * FROM silver.erp_loc_a101;
		SELECT COUNT(*) FROM silver.erp_loc_a101;
		PRINT '----------------------------------';

		-- INSERTING DATA INTO silver.erp_px_cat_g1v2 TABLE
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING silver.erp_px_cat_g1v2 TABLE'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> INSERTING DATA INTO silver.erp_px_cat_g1v2 TABLE'
		INSERT INTO silver.erp_px_cat_g1v2(
		id,cat,subcat,maintenance)
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();		
		SELECT * FROM silver.erp_px_cat_g1v2;
		SELECT COUNT(*) FROM silver.erp_px_cat_g1v2;
		PRINT '----------------------------------';

		SET @end_batch_time = GETDATE();
		PRINT 'SILVER LAYER IS COMPLETED';
		PRINT '>> Total Loading Duration for Bronze layer: ' + CAST(DATEDIFF(second, @start_batch_time, @end_batch_time) AS NVARCHAR) + ' seconds';
		PRINT '****************************************************************************';
	END TRY
	BEGIN CATCH
		PRINT '============================================================';
		PRINT 'ERROR OCCURED WHILST LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '============================================================';
	END CATCH
END
