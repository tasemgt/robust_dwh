/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'robust_dwh' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'robust_dwh')
BEGIN
    ALTER DATABASE robust_dwh SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE robust_dwh;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE robust_dwh;
GO

USE robust_dwh;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
