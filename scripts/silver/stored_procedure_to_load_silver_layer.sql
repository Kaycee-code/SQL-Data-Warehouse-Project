/*
Stored Procedure: Load Silver Layer (Bronze to Silver)
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
Script Purpose: 
This stored procedure performs the ETL (Extract, Transform and Load) process to populate the silver schema tables from 
the bronze schema.
Action Performed:
- Truncates Silver tables
- Inserts transformed and cleaned date from Bronze into Silver tables.
Parameters:
None
This stored procedure does not accept any parameters or return any values.
Usage Example:
EXEC silver.load_silver 
This is also part of the script.

*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME

	BEGIN TRY
	SET @batch_start_time = GETDATE()
		
		SET @start_time = GETDATE() 
		PRINT '>>Truncating Table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info 
		PRINT '>>Inserting Data into: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
		cst_id, 
		cst_key, 
		cst_firstname, 
		cst_lastname, 
		cst_marital_status, 
		cst_gndr, 
		cst_create_date)

		SELECT cst_id,
			  TRIM(cst_key) AS cst_key,
			  TRIM(cst_firstname) AS cst_firstname,
			  TRIM(cst_lastname) AS cst_lastname,

			  CASE	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
					WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
					ELSE 'N/A'
			  END AS cst_marital_status,
      
			  CASE	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
					WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					ELSE 'N/A'
			  END AS cst_gndr,

			  cst_create_date
		FROM(
			SELECT * 
			FROM (
					SELECT *,
					ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS entry_flag
					FROM bronze.crm_cust_info
					WHERE cst_id IS NOT NULL) no_duplicate_or_null
			WHERE entry_flag =1 
			) trimmed_and_standard
		SET @end_time = GETDATE()
		PRINT 'Loading Duration for crm_cust_info table: ' + CAST(DATEDIFF(Second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '------------------------------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT '>> Inserting Data Into: silver.crm_prd_info'
		INSERT INTO 
		silver.crm_prd_info(
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
			REPLACE (SUBSTRING(prd_key, 0, 6),'-','_')  AS cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'N/A'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_nm ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE()
		PRINT 'Loading Duration for bronze.crm_prd_info: ' + CAST(DATEDIFF(Second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '------------------------------------------------------------------------'


		SET @start_time = GETDATE()
		PRINT '>>Truncating Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT '>> Inserting Data Into: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details(
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
			TRIM(sls_ord_num) AS sls_ord_num,
			TRIM(sls_prd_key) AS sls_prd_key,
			sls_cust_id,
			--To clean up the date columns:
			CASE 
			WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
			END AS sls_order_dt,

			CASE 
			WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST (sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,

			CASE 
			WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST( CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,

			CASE 
			WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales !=  ABS(sls_price) * sls_quantity
			THEN  ABS(sls_price) * sls_quantity
			ELSE sls_sales
			END AS sls_sales,

			sls_quantity,

			CASE 
			WHEN sls_price = 0 OR sls_price IS NULL THEN (sls_sales / NULLIF(sls_quantity, 0)) 
			WHEN sls_price < 0 THEN ABS(sls_price)
			ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE()
		PRINT 'Loading Duration for bronze.crm_sales_details: ' + CAST(DATEDIFF(Second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '------------------------------------------------------------------------'


		SET @start_time = GETDATE()
		PRINT '>>Truncating Table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT '>>Inserting Data into: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12(
		CID,
		BDATE,
		GEN)

		SELECT 
			RIGHT(TRIM(CID), LEN(TRIM(CID))-3) AS CID,

			CASE
			WHEN BDATE < '1900-01-01' OR BDATE > GETDATE() THEN NULL
			ELSE BDATE
			END AS BDATE,

			CASE 
			WHEN GEN LIKE 'F%' THEN 'Female'
			WHEN GEN LIKE 'M%' THEN 'Male'
			ELSE NULL
			END AS GEN
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE()
		PRINT 'Loading Duration for bronze.erp_cust_az12: ' + CAST(DATEDIFF(Second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '------------------------------------------------------------------------'


		SET @start_time = GETDATE()
		PRINT'>>Truncating Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT '>>Inserting Data Into: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101 (
		CID, 
		CNTRY)

		SELECT
			LEFT(CID, 2) + SUBSTRING(CID, 4, LEN(CID)) AS CID,

			CASE
			WHEN CNTRY = '' OR CNTRY IS NULL THEN 'N/A'
			WHEN CNTRY = 'DE' THEN 'Germany'
			WHEN CNTRY IN ('USA', 'US') THEN 'United States'
			ELSE CNTRY
			END AS CNTRY

		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE()
		PRINT 'Loading Duration for bronze.erp_loc_a101: ' + CAST(DATEDIFF(Second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '------------------------------------------------------------------------'



		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE)

		SELECT 
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		FROM bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE()
		PRINT 'Loading Duration for bronze.erp_px_cat_g1v2: ' + CAST(DATEDIFF(Second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '------------------------------------------------------------------------'

	
	SET @batch_end_time = GETDATE()
	PRINT '=================================================================================='
	PRINT 'Silver Schema Loading Duration: ' + CAST(DATEDIFF(Second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds'
	END TRY
	
	BEGIN CATCH
	PRINT 'AN ERROR OCCURRED WHILE LOADING THE SILVER SCHEMA TABLE'
	PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE()
	PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR)
	PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS NVARCHAR)
	END CATCH
END
GO

EXEC silver.load_silver



