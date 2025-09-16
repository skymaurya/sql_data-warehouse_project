/*
=============================================================================================================
DDL Script: Create Gold Views
=============================================================================================================
Scripts Purpose:
      This scripts creates views for the Gold Layer in the data warehouse.
      The Gold layer respresents the final dimension and the facts tables (Star Schema)

      Each view performs transformation and combines data from the Silver layer to produce a clean, encrihed
      and business ready dataset.
Usage:-
      -These views can be queried directly for analytics and reporting.
==============================================================================================================
*/

-- ===========================================================================================================
--create dimenssion: gold.dim_customers
-- ======================================================================================================
IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
  DROP VIEW gold.dim_customers;
GO
  
CREATE VIEW gold.dim_customers AS 
SELECT 
       ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key
       ,ci.cst_id as customer_id  
      ,ci.cst_key as customer_number
      ,ci.cst_firstname as first_name
      ,ci.cst_lastname as last_name
       ,la.cntry as country
      ,ci.cst_material_status marital_status
      ,CASE 
            WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr
            ELSE coalesce (ca.gen,'n/a')
        END as gender
       ,ca.bdate as birthday
      ,ci.cst_create_date as create_date
FROM silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on  ci.cst_key=ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key=la.cid

-- ===========================================================================================================
--create dimenssion: gold.dim_products
-- ======================================================================================================
IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
  DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY po.prd_start_dt, po.prd_key) AS product_key,
    po.prd_id AS product_id,
    po.prd_key AS product_number,
    po.prd_nm AS product_name,
    po.cat_id AS category_id,
    px.cat AS category,
    px.subcat AS subcategory,
    px.maintenance,
    po.prd_cost AS cost,
    po.prd_line AS product_line,
    po.prd_start_dt AS start_date
FROM silver.crm_prd_info AS po
LEFT JOIN silver.erp_px_cat_g1v2 AS px
    ON po.cat_id = px.id
WHERE po.prd_end_dt IS NULL;

-- ===========================================================================================================
--create dimenssion: gold.facts_sales
-- ==========================================================================================================
IF OBJECT_ID('gold.facts_sales','V') IS NOT NULL
  DROP VIEW gold.facts_sales;
GO

CREATE VIEW gold.facts_sales AS

SELECT  
      sls_ord_num as order_number
      ,pr.product_key 
      ,cu.customer_key
      ,sls_order_dt order_date
      ,sls_ship_dt as shipping_date
      ,sls_due_dt as due_date
      ,sls_sales as sales_amount
      ,sls_quantity as quantity
      ,sls_price as price
 FROM silver.crm_sales_details as  sd
 left join gold.dim_products as pr
 on sd.sls_prd_key=pr.product_number
 left join gold.dim_customers as  cu
 on sd.sls_cust_id=cu.customer_id


