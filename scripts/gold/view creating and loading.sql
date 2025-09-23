IF OBJECT_ID ('gold.dim_customers', 'V') IS NOT NULL
DROP VIEW gold.fact_sales
GO
CREATE VIEW gold.dim_customers AS 
	SELECT 
		ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, --Surrogate key which was generated to serve as the primary key
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		la.CNTRY AS country,
		ci.cst_marital_status AS marital_status,
	
		CASE 
		WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr --CRM is the master table
		WHEN ci.cst_gndr = 'N/A' AND ca.gen IS NOT NULL THEN ca.gen
		ELSE cst_gndr
		END AS gender,

		ca.bdate AS birth_date,
		ci.cst_create_date AS create_date		
	FROM silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.CID;


IF OBJECT_ID ('gold.dim_products', 'V') IS NOT NULL
DROP VIEW gold.fact_sales
GO
CREATE VIEW gold.dim_products AS

	SELECT 
		ROW_NUMBER () OVER (ORDER BY pd.prd_start_dt, pd.prd_key) AS product_key, 
		pd.prd_id AS product_id,
		pd.prd_key AS product_number,
		pd.prd_nm AS product_name,
		pd.prd_line AS product_line,
		pd.cat_id AS category_id,
		COALESCE(px.cat, 'N/A') AS category,
		COALESCE(px.subcat, 'N/A') AS subcategory,
		COALESCE(px.MAINTENANCE, 'N/A') AS maintenance,	
		pd.prd_cost AS cost,
		pd.prd_start_dt AS product_start_date
	
	FROM silver.crm_prd_info AS pd
	LEFT JOIN silver.erp_px_cat_g1v2 AS px
	ON pd.cat_id = px.ID
	WHERE pd.prd_end_dt IS NULL --To stick to current data only, ignoring historical data


--To create a fact table, join the dimension tables to the fact table and substitute the primary/foreign key with 
--the surrogate key for each dimension table.
IF OBJECT_ID ('gold.fact_sales', 'V') IS NOT NULL
DROP VIEW gold.fact_sales
GO

CREATE VIEW gold.fact_sales AS
SELECT 
sls_ord_num AS order_number,
pr.product_key AS product_key,
cs.customer_key AS customer_key,
sls_order_dt AS order_date,
sls_ship_dt AS shipping_date,
sls_due_dt AS due_date,
sls_sales AS sales,
sls_quantity AS quantity,
sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cs
ON sd.sls_cust_id = cs.customer_id

