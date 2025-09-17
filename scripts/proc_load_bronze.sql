/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data. (Full load technique)
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.
    - Handles data type conversions and basic data validation during the load process.
    - Logs the start and end time of each load operation for performance monitoring.
    - Implements error handling to catch and log any issues during the load process.
    - Uses a staging table for the `crm_sales_details` table to manage data type conversions.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze_data AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @total_start_time DATETIME, @total_end_time DATETIME;
    BEGIN TRY
        SET @total_start_time = GETDATE();
        PRINT '==============================================================';
        PRINT 'Loading data into Bronze Schema Tables';
        PRINT '==============================================================';
        
        PRINT '--------------------------------------------------------------';
        PRINT 'Loading data into CRM System Tables';
        PRINT '--------------------------------------------------------------';
        
        SET @start_time = GETDATE();
        PRINT '1) Truncating Table: bronze.crm_cust_info';
        Truncate table bronze.crm_cust_info; -- Full loading

        PRINT '>> Inserting data into Table: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        from '/datasets/source_crm/cust_info.csv'
        with (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '----------------------------------------------';


        SET @start_time = GETDATE();
        PRINT '2) Truncating Table: bronze.crm_prd_info';
        Truncate table bronze.crm_prd_info;

        PRINT '>> Inserting data into Table: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        from '/datasets/source_crm/prd_info.csv'
        with (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK --locks the table for the duration of the bulk insert operation
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '----------------------------------------------';


        SET @start_time = GETDATE();
        PRINT '3) Loading Table: bronze.crm_sales_details';
        -- For sales_details, we will use a staging table to handle data type conversions
        -- 1: Create staging table
        PRINT 'Creating staging table: bronze.crm_sales_details_staging';
        if OBJECT_ID('bronze.crm_sales_details_staging', 'U') is not null
            drop table bronze.crm_sales_details_staging;
        CREATE TABLE bronze.crm_sales_details_staging (
            sls_ord_num NVARCHAR(50),
            sls_prd_key NVARCHAR(50),
            sls_cust_id NVARCHAR(50),
            sls_order_dt NVARCHAR(50),
            sls_ship_dt NVARCHAR(50),
            sls_due_dt NVARCHAR(50),
            sls_sales INT,
            sls_quantity INT,
            sls_price INT
        );

        -- 2: Bulk insert into staging
        PRINT 'Bulk inserting data into staging table: bronze.crm_sales_details_staging';
        BULK INSERT bronze.crm_sales_details_staging
        FROM '/datasets/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        -- 3: Insert and cast into final table
        PRINT 'Inserting and transforming data into final table: bronze.crm_sales_details';
        Truncate table bronze.crm_sales_details ; -- Full loading
        INSERT INTO bronze.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt,
            sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            TRY_CAST(sls_cust_id AS INT),
            TRY_CAST(CONVERT(DATE, sls_order_dt, 112) AS DATE),  -- 112 = ISO (yyyymmdd)
            TRY_CAST(CONVERT(DATE, sls_ship_dt, 112) AS DATE),
            TRY_CAST(CONVERT(DATE, sls_due_dt, 112) AS DATE),
            sls_sales,
            sls_quantity,
            sls_price
        FROM bronze.crm_sales_details_staging
        WHERE ISDATE(sls_order_dt) = 1;

        -- 4: Drop staging table
        DROP TABLE bronze.crm_sales_details_staging;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';

        PRINT '--------------------------------------------------------------';
        PRINT 'Loading data into ERP System Tables';
        PRINT '--------------------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '4) Truncating Table: bronze.erp_cust_az12';
        Truncate table bronze.erp_cust_az12;

        PRINT '>> Inserting data into Table: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        from '/datasets/source_erp/cust_az12.csv'
        with (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '----------------------------------------------';


        SET @start_time = GETDATE();
        PRINT '5) Truncating Table: bronze.erp_px_cat_g1v2';
        Truncate table bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting data into Table: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        from '/datasets/source_erp/px_cat_g1v2.csv'
        with (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '----------------------------------------------';


        SET @start_time = GETDATE();
        PRINT '6) Truncating Table: bronze.erp_loc_a101';
        Truncate table bronze.erp_loc_a101;

        PRINT '>> Inserting data into Table: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        from '/datasets/source_erp/loc_a101.csv'
        with (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        
        SET @total_end_time = GETDATE();
        PRINT '==============================================================';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @total_start_time, @total_end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '==============================================================';
        PRINT 'Data loading into Bronze Schema Tables completed successfully';
        PRINT '==============================================================';
    END TRY
    BEGIN CATCH
        PRINT '==============================================================';
        PRINT 'Error occurred while loading data into Bronze Schema Tables';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT '==============================================================';
    END CATCH
END;
