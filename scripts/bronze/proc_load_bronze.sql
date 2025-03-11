/*
===============================================================
Stored Procedure - Load Bronze layer, Source -> Bronze
===============================================================
This script load data from External csv into the BRONZE schema
It performs the following actions
  - Truncate Bronze Table before loading
  - Uses the BULK INSERT command to load data from the csv files into the Bronze Table

Parameter: None
  This stored procedure does not accept or return any values.

Example Usage:
  EXEC bronze.load_bronze
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time AS DATETIME, @end_time AS DATETIME, @start_batch_time AS DATETIME, @end_batch_time AS DATETIME;
	BEGIN TRY
	SET @start_batch_time = GETDATE();
		PRINT '=================================================================';
		PRINT 'LOADING THE BRONZE LAYER';
		PRINT '=================================================================';

		PRINT '-----------------------------------------------------------------';
		PRINT 'LOADING THE CRM TABLES';
		PRINT '-----------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info -- Empty the table before loading in data
		PRINT '>> INSERTING DATA INTO: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM '~\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';		
		SELECT * FROM bronze.crm_cust_info;
		SELECT COUNT(*) FROM bronze.crm_cust_info;
		PRINT '----------------------------------';

		-- 2
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.crm_prod_info';
		TRUNCATE TABLE bronze.crm_prod_info -- Empty the table before loading in data
		PRINT '>> INSERTING DATA INTO: bronze.crm_prod_info';
		BULK INSERT bronze.crm_prod_info
		FROM '~\datasets\source_crm\prd_info.csv'  -- use the full path to the file
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		SELECT * FROM bronze.crm_prod_info;
		SELECT COUNT(*) FROM bronze.crm_prod_info;
		PRINT '----------------------------------';

		-- 3
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.crm_sales_data';
		TRUNCATE TABLE bronze.crm_sales_data -- Empty the table before loading in data
		PRINT '>> INSERTING DATA INTO: bronze.crm_sales_data';
		BULK INSERT bronze.crm_sales_data
		FROM '~\datasets\source_crm\sales_details.csv'  -- use the full path to the file
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		SELECT * FROM bronze.crm_sales_data;
		SELECT COUNT(*) FROM bronze.crm_sales_data;
		PRINT '----------------------------------'


		PRINT '-----------------------------------------------------------------';
		PRINT 'LOADING THE ERP TABLES';
		PRINT '-----------------------------------------------------------------';

		-- 4
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12 -- Empty the table before loading in data
		PRINT '>> INSERTING DATA INTO: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM '~\datasets\source_erp\CUST_AZ12.csv'  -- use the full path to the file
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		SELECT * FROM bronze.erp_cust_az12;
		SELECT COUNT(*) FROM bronze.erp_cust_az12;
		PRINT '----------------------------------';
		
		-- 5
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101 -- Empty the table before loading in data
		PRINT '>> INSERTING DATA INTO: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM '~\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		SELECT * FROM bronze.erp_loc_a101;
		SELECT COUNT(*) FROM bronze.erp_loc_a101
		PRINT '----------------------------------';

		-- 6
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2 -- Empty the table before loading in data
		PRINT '>> INSERTING DATA INTO: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM '~\datasets\source_erp\PX_CAT_G1V2.csv'  -- use the full path to the file
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		SELECT * FROM bronze.erp_px_cat_g1v2;
		SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;
		PRINT '----------------------------------';

		SET @end_batch_time = GETDATE();

		PRINT '***************************************************************************';
		SET @end_time = GETDATE();
		PRINT 'BRONZE LAYER IS COMPLETED';
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
