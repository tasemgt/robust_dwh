

-- SELECT name FROM sys.databases;

-- SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='bronze';


USE robust_dwh;
GO
-- SELECT TABLE_NAME 
-- FROM INFORMATION_SCHEMA.TABLES 
-- WHERE TABLE_TYPE = 'BASE TABLE'
-- ORDER BY TABLE_NAME;

-- Data Quality Check

-- Check for nulls or Duplicates in Primary key
-- Expecting 0 records from below query
select 
    cst_id, 
    count(*) as count
from 
    bronze.crm_cust_info 
group by cst_id 
having count(*) > 1 or cst_id is null;

-- Check unwanted spaces in string columns
-- Expecting 0 records from below query
SELECT cst_firstname -- Also for cst_lastname, cst_marital_status, etc
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr 
FROM bronze.crm_cust_info

SELECT TOP 10 * FROM bronze.crm_prd_info;

-- Overall Query to check data quality and perform cleaning

-- 1ST Silver Insert!!!
PRINT '>> Truncating Table: silver.crm_cust_info';
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
where flag_last = 1 and cst_id is not null;

select count(*) from silver.crm_cust_info


-- To find unmatched categories
-- WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN 
-- (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2); -- To find unmatched categories


-- Check for Null or Negative costs
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL; -- To find invalid costs


-- Check for default dates that should be nulls
SELECT prd_id, prd_start_dt, prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_end_dt = '1900-01-01'


SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt; --All data have bad end dates so we need to rebuild it


-- Fix end dates first for default nulls
UPDATE bronze.crm_prd_info
SET prd_end_dt = NULL
WHERE prd_end_dt = '1900-01-01';


-- 2ND Silver Insert!!!
PRINT '>> Truncating Table: silver.crm_prd_info';
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


-- SALES DETAILS!!

SELECT * FROM bronze.crm_sales_details
-- WHERE sls_order_dt = '1900-01-01'
-- WHERE sls_order_dt IN ('1900-01-01', '1970-01-01', '1899-12-30', '9999-12-31');

-- Check invalid date periods
SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt != DATEADD(DAY, -7, sls_ship_dt)
-- WHERE sls_order_dt > sls_ship_dt OR sls_ship_dt > sls_due_dt

SELECT COUNT(*) FROM bronze.crm_sales_details

SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


-- 3RD Silver Insert!!!

PRINT '>> Truncating Table: silver.crm_sales_details';
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

SELECT count(*) from silver.crm_sales_details


-- ---------------ERP QUERIES!!--------------------------

-- SELECT 
--     gen,
--     CASE 
--         WHEN gen LIKE '%' + CHAR(13) + '%' THEN 'Has CR'
--         WHEN gen LIKE '%' + CHAR(10) + '%' THEN 'Has LF'
--         ELSE 'Clean'
--     END AS line_break_check
-- FROM bronze.erp_cust_az12

-- Data Standardization & Consistency
SELECT DISTINCT gen,
CASE WHEN UPPER(REPLACE(TRIM(gen), CHAR(13), '')) IN ('F', 'FEMALE') THEN 'Female'
     WHEN UPPER(REPLACE(TRIM(gen), CHAR(13), '')) IN ('M', 'MALE') THEN 'Male'
     ELSE 'N/A'
END AS gen
FROM bronze.erp_cust_az12

-- 4TH Silver Insert!!!
PRINT '>> Truncating Table: silver.erp_cust_az12';
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

---------------------------------------------------------------

-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101


-- 5TH Silver Insert!!!
PRINT '>> Truncating Table: silver.erp_loc_a101';
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

-- Check that cat_id marches id in erp_px_cat
SELECT cat_id
FROM silver.crm_prd_info
WHERE cat_id NOT IN (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)

-- Checking standardizations
-- SELECT DISTINCT -- Everything is cool in last qty.
-- 6TH Silver Insert!!!
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

SELECT * FROM silver.erp_px_cat_g1v2









