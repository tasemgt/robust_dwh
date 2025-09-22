
EXEC bronze.load_bronze_data; -- Load data into Bronze tables

EXEC silver.load_silver_data; -- Load data into Bronze tables


Select distinct gender from gold.dim_customers;

Select * from gold.dim_products;

Select * from gold.fact_sales;