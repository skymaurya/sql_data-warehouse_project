/*
===============================================================================================
Stored Procedure: Load Bronze Layer (Bronze -> Silver)
=============================================================================================
Script Purpose:
		This stored procedure performs the ETL (Extract, Transform, Load) process to  populate
    the 'Silver ' Schema tables from the 'bronze' schema.

Action performed:
      - Truncate silver tables.
      - Insert transformed and cleansed data from Bronze into Silver tables.
Parameters:
	None.
	THIS stored procedure does not accepts any parameters or return any values.


	Usage Example:
		EXEC silver.load_silver;
===============================================================================================


*/
--MAKING THE STORE PROCEDURE FOR Silver Layer


--insering all data of all 6 tbles from bronze layer after clasing and transformation


CREATE OR   ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time	DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time=GETDATE();
		PRINT'===================================================';
		PRINT 'Loading Silver Layer';
		print '================================================='; 

		PRINT'---------------------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'---------------------------------------------------';

		SET   @start_time=GETDATE();

		PRINT'>>Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT'>>Inserting Data Into silver.crm_cust_info:'

		INSERT INTO silver.crm_cust_info
		(cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date)

		select
			cst_id,
			cst_key,
			trim(cst_firstname) as cst_firstname,
			trim(cst_lastname) as cst_lastname,
			case 
				when UPPER(cst_material_status)='M' THEN 'Married'
				when UPPER(cst_material_status)='S' THEN 'Single'
				else 'n/a' 
			END AS cst_material_status,
			case 
				when UPPER(cst_gndr)='M' THEN 'Male'
				when UPPER(cst_gndr)='S' THEN 'Female'
				else 'n/a' 
			END AS cst_gndr,
			cst_create_date
		from(
			select 
			*,
			ROW_NUMBER()over (partition by cst_id order by cst_create_date desc) as flag_last
			from [bronze].[crm_cust_info]
			where cst_id is not null
		) t 
		where flag_last=1

		SET @end_time = GETDATE();
		PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds';
		PRINT'----------------------------';

		---2 Tables
		SET   @start_time=GETDATE();
		PRINT'>>Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT'>>Inserting Data Into silver.crm_prd_info:';
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

		select 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,--extract category ID
		SUBSTRING(prd_key ,7,len(prd_key)) as prd_key, -- Extract product key
		prd_nm,  
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
				WHEN  'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
		end as prd_line, --- map product linr codes to descriptive values
		prd_start_dt,
		DATEADD(DAY, -1, LEAD(prd_start_dt) 
						  OVER (PARTITION BY prd_key ORDER BY prd_start_dt))
						  AS prd_end_dt  --calculate end date as one day before the next start date 
		from bronze.crm_prd_info 
		SET @end_time = GETDATE();
		PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds';
		PRINT'----------------------------';

		---3 Tables
		SET   @start_time=GETDATE();

		PRINT'>>Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT'>>Inserting Data Into silver.crm_sales_details:';


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
 
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN sls_order_dt =0 or len(sls_ord_num)!=8 THEN NULL
			ELSE cast( CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE 
			WHEN sls_ship_dt =0 or len(sls_ship_dt)!=8 THEN NULL
			ELSE cast( CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,

		CASE 
			WHEN sls_due_dt =0 or len(sls_due_dt)!=8 THEN NULL
			ELSE cast( CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE
			WHEN sls_sales is null or sls_sales<=0 or sls_sales!=sls_quantity*ABS(sls_price) 
			THEN sls_quantity *ABS(sls_price)
			ELSE sls_sales
		END sls_sales,--Recalculating sales if original vlaue is missing or incorrect
		sls_quantity,
		CASE 
			WHEN  sls_price is null or sls_price<=0 THEN sls_sales /nullif(sls_quantity,0)
			ELSE sls_price 
		END as sls_price -- derive price if the original value is invalid

		FROM bronze.crm_sales_details

		SET @end_time = GETDATE();
		PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds';
		PRINT'----------------------------';

		--4 tables
		SET   @start_time=GETDATE();
		PRINT'---------------------------------------------------';
		PRINT'Loading ERP Tables';
		PRINT'---------------------------------------------------';


		PRINT'>>Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT'>>Inserting Data Into silver.erp_cust_az12:';

		 --- inserting data from bronze.erp_cust_az12 into  silver layer after cleaning

		 INSERT  INTO silver.erp_cust_az12(cid ,bdate,gen)

		select
		CASE 
			WHEN cid like 'NAS%' then substring(cid,4, len(cid))
			else cid
		end  cid,   -- remaove ''nas prefix  if present
		CASE
			WHEN bdate> GETDATE() then null
			else bdate
		end as bdate, -- set future birthdate to null
		CASE 
			WHEN upper(trim(gen)) in ('F','FEMALE') THEN 'Female'
			WHEN upper(trim(gen)) in ('M','MALE') THEN 'Male'
			ELSE 'n/a'
		end as gen -- data standardisation consitency , normalisation the gendaer unknown values
		 FROM bronze.erp_cust_az12

		SET @end_time = GETDATE();
		PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds';
		PRINT'----------------------------';

		 ---5 tables
		SET   @start_time=GETDATE();
		PRINT'>>Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT'>>Inserting Data Into silver.erp_loc_a101:';
		INSERT INTO silver.erp_loc_a101( cid,cntry)

		select 
		replace(cid,'-','')as cid,
		CASE
			WHEN  TRIM(cntry) ='DE' THEN 'Germany'
			WHEN  TRIM(cntry) in ('US','USA') THEN 'United States'
			WHEN  TRIM(cntry) ='' THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry -- normalise and handle missing or the blank country codes
		from bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds';
		PRINT'----------------------------';

		---6th tables
		SET   @start_time=GETDATE();
		PRINT'>>Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT'>>Inserting Data Into silver.erp_px_cat_g1v2:';

		INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		select
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds';
		PRINT'----------------------------';



		SET @batch_end_time=GETDATE();
		PRINT'======================================='
		PRINT'Loading Silver Layer is Completed';
		PRINT' - Total Load Duration:' +CAST(DATEDIFF(SECOND,@batch_start_time, @batch_end_time)AS NVARCHAR )+'seconds';

		END TRY

		BEGIN CATCH
			PRINT '============================================================';
			PRINT'Error occured  During Loading SILVER LAYER';
			PRINT'Error Message '+ ERROR_MESSAGE();
			PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
			
			PRINT '============================================================';
	END CATCH
END
