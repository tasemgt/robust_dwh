/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver_data AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '==============================================================';
        PRINT 'Loading data into Bronze Schema Tables';
        PRINT '==============================================================';
        
        PRINT '--------------------------------------------------------------';
        PRINT 'Loading data into CRM System Tables';
        PRINT '--------------------------------------------------------------';
        
        SET @start_time = GETDATE();
        PRINT '1. Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info
        PRINT '>> Inserting Data into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info(
            cst_id, 
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_created_date
        )
        SELECT
            cst_id, 
            cst_key,
            TRIM(cst_firstname) as cst_firstname,
            TRIM(cst_lastname) as cst_lastname,
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                ELSE 'N/A'
            END as cst_marital_status,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN  'Female'
                ELSE 'N/A'
            END as cst_gndr,
            cst_created_date
        FROM
        (SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_created_date DESC) as flag_last
        FROM bronze.crm_cust_info)t
        WHERE flag_last = 1 AND cst_id IS NOT NULL;
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '-------------------------------------------------------------------------------------------------';

        ----------------------------------------------------------------------------------------

        SET @start_time = GETDATE();
        PRINT '2. Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info
        PRINT '>> Inserting Data into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info(
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
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id, -- New Column to match with ERP system 
            SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) as prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A'
            END as prd_line,
            prd_start_dt,
            DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
        FROM bronze.crm_prd_info
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '-------------------------------------------------------------------------------------------------';

        ----------------------------------------------------------------------------------------

        SET @start_time = GETDATE();
        PRINT '3. Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details
        PRINT '>> Inserting Data into: silver.crm_sales_details';
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
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            DATEADD(DAY, -7, sls_ship_dt) AS sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            CASE
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price <= 0
                    THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '-------------------------------------------------------------------------------------------------';

        ----------------------------------------------------------------------------------------

        PRINT '--------------------------------------------------------------';
        PRINT 'Loading data into ERP System Tables';
        PRINT '--------------------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '4. Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12
        PRINT '>> Inserting Data into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12(
            cid,
            bdate,
            gen
        )
        SELECT
            CASE 
                WHEN cid LIKE '%NAS%'
                    THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            CASE WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE WHEN UPPER(REPLACE(TRIM(gen), CHAR(13), '')) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(REPLACE(TRIM(gen), CHAR(13), '')) IN ('M', 'MALE') THEN 'Male'
            ELSE 'N/A'
        END AS gen
        FROM bronze.erp_cust_az12
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '-------------------------------------------------------------------------------------------------';

        ----------------------------------------------------------------------------------------

        SET @start_time = GETDATE();
        PRINT '5. Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101
        PRINT '>> Inserting Data into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101(
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE
                WHEN UPPER(REPLACE(TRIM(cntry), CHAR(13), '')) = 'DE' THEN 'Germany'
                WHEN UPPER(REPLACE(TRIM(cntry), CHAR(13), '')) IN ('US', 'USA') THEN 'United States'
                WHEN UPPER(REPLACE(TRIM(cntry), CHAR(13), '')) = '' OR cntry IS NULL THEN 'N/A'
            END AS cntry
        FROM bronze.erp_loc_a101
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '-------------------------------------------------------------------------------------------------';

        ----------------------------------------------------------------------------------------

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2
        PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2(
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT 
            id,
            cat,
            subcat,
            REPLACE(TRIM(maintenance), CHAR(13), '') AS maintenance
        FROM bronze.erp_px_cat_g1v2
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '-------------------------------------------------------------------------------------------------';

        SET @batch_end_time = GETDATE();
        PRINT '==============================================================';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '==============================================================';
        PRINT 'Data loading into Silver Schema Tables completed successfully';
        PRINT '==============================================================';
    END TRY

    BEGIN CATCH
        PRINT '==============================================================';
        PRINT 'Error occurred while loading data into Silver Schema Tables';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT '==============================================================';
    END CATCH
END