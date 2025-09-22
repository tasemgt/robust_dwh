/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

-- Customer Relationship Management (CRM) System
-- 1st Table
if OBJECT_ID('bronze.crm_cust_info', 'U') is not null
    drop table bronze.crm_cust_info;
create table bronze.crm_cust_info(
    cst_id int,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_created_date DATE
);

-- 2nd Table
if OBJECT_ID('bronze.crm_prd_info', 'U') is not null
    drop table bronze.crm_prd_info;
create table bronze.crm_prd_info(
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE NULL,
    prd_end_dt DATE NULL
);

-- 3rd Table
if OBJECT_ID('bronze.crm_sales_details', 'U') is not null
    drop table bronze.crm_sales_details;
create table bronze.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-- Enterprise Resource Planning (ERP) System

-- 4th Table
if OBJECT_ID('bronze.erp_cust_az12', 'U') is not null
    drop table bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
)

-- 5th Table
if OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') is not null
    drop table bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
)

-- 6th Table
if OBJECT_ID('bronze.erp_loc_a101', 'U') is not null
    drop table bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
    cid NVARCHAR(50),
    cntry NVARCHAR(50),
)