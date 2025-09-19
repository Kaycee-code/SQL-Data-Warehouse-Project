/*
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
DDL Script: Create Bronze Layer Tables
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
Purpose:
This script creates tables in the bronze schema, by initially dropping existing tables if they are present.
Run this script to re-define the DDL structure of 'bronze' tables
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
*/

--Switch to Master Database to enable you create a new database
USE master;
GO

--Check for the existence of 'DataWarehouse' database and drop it if it exists:

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE  DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse
END;
GO

--Create the 'DataWarehouse' Database and switch over to it:
CREATE DATABASE DataWarehouse
GO
USE DataWarehouse;
GO

--Create new schemas in the DataWarehouse Database:
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

---Create DDL for bronze layer tables after previewing the csv files for this project
--First, we check if the table already exists

IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
DROP TABLE bronze.crm_cust_info
CREATE TABLE bronze.crm_cust_info (
cst_id INT,
cst_key NVARCHAR (50),
cst_firstname NVARCHAR (50),
cst_lastname NVARCHAR (50),
cst_marital_status NVARCHAR (50),
cst_gndr NVARCHAR (50),
cst_create_date DATE
);
GO

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
DROP TABLE bronze.crm_prd_info
CREATE TABLE bronze.crm_prd_info (
prd_id INT,
prd_key NVARCHAR (50),
prd_nm NVARCHAR (50),
prd_cost INT,
prd_line NVARCHAR (50),
prd_start_dt DATETIME,
prd_end_dt DATETIME,
)
GO

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
DROP TABLE bronze.crm_sales_details
CREATE TABLE bronze.crm_sales_details (
sls_ord_num NVARCHAR (50),
sls_prd_key NVARCHAR (50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
)
GO

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
DROP TABLE bronze.erp_cust_az12
CREATE TABLE bronze.erp_cust_az12 (
CID NVARCHAR (50),
BDATE DATE,
GEN NVARCHAR (50)
)
GO

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
DROP TABLE bronze.erp_loc_a101
CREATE TABLE bronze.erp_loc_a101 (
CID NVARCHAR (50),
CNTRY NVARCHAR (50)
)
GO

IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'erp_px_cat_g1v2')
BEGIN 
	DROP TABLE bronze.erp_px_cat_g1v2
END
CREATE TABLE bronze.erp_px_cat_g1v2 (
ID NVARCHAR (50),
CAT NVARCHAR (50),
SUBCAT NVARCHAR (50),
MAINTENANCE NVARCHAR (50)
)
GO
