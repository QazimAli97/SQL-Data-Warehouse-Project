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

--Creating Store procedure
create or alter procedure silver.load_silver AS
begin

DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY

	SET @batch_start_time = GETDATE();
	PRINT '==========================================================';
	PRINT 'Loading Sliver Layer';
	PRINT '==========================================================';

	PRINT '----------------------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '----------------------------------------------------------';


SET @start_time = GETDATE();
--Table no 1: [silver].[crm_cust_info]
--Data transfered into [Silver.crm_cust_info ] after cleaning 
Print '>> Truncating Table: [silver].[crm_cust_info]';
Truncate table [silver].[crm_cust_info]
Print '>> Inserting Data Into: [silver].[crm_cust_info]';
insert into [silver].[crm_cust_info] (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)
select 
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname, --Remove unwanted spaces
	trim(cst_lastname) as cst_lastname,
case when upper(trim(cst_marital_status)) = 'S' then 'Single'
	 when upper(trim(cst_marital_status)) = 'M' then 'Married'
	 else 'n/a'
	 end cst_marital_status, --Normalize Marital status values to readable format 
case when upper(trim(cst_gndr)) = 'F' then 'Female'
	 when upper(trim(cst_gndr)) = 'M' then 'Male'
	 else 'n/a'
	 end cst_gndr,--Normalize gender values to readable format 
cst_create_date
from(
select *,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc )[flag_last]
from [bronze].[crm_cust_info]
where cst_id is not null 
)t where flag_last  = 1 

SET @end_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> --------------------'
--select * from [silver].[crm_cust_info]
--select * from [bronze].[crm_cust_info]


--Table no : 2 [silver].[crm_prd_info]'
-->> Date transfered into  [silver].[crm_prd_info] 
SET @start_time = GETDATE();
Print '>> Truncating Table: [silver].[crm_prd_info]';
Truncate table [silver].[crm_prd_info]
Print '>> Inserting Data Into: [silver].[crm_prd_info]';
insert into [silver].[crm_prd_info] (
	prd_id,
	cat_id ,
	prd_key ,
	prd_nm ,
	prd_cost ,
	prd_line ,
	prd_start_dt ,
	prd_end_dt )
SELECT [prd_id],         --Check duplicates & null
      replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id, --extracts category id           
      SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,      --extracts product key
      [prd_nm],          --checks for unwanted space
      isnull(prd_cost, 0) [prd_cost], --transform null value into 0
      case 
           UPPER(TRIM(prd_line))     
           when 'M' then 'Mountain'
           when 'R' then 'Road'
           when 'S' then 'Other Sales'      --transform abbreviation into full names
           when 'T' then 'Touring'
           else 'n/a'
        End as [prd_line],
      CAST([prd_start_dt] AS DATE)[prd_start_dt], --transform the datetime to date only
      CAST(
            LEAD(prd_start_dt) over( partition by prd_key order by prd_start_dt )-1
            AS DATE) [prd_end_dt]              --Calculate end date as one day before the next start date
  FROM [bronze].[crm_prd_info]

SET @end_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> --------------------'

--select * from [silver].[crm_prd_info]
--select * from [bronze].[crm_prd_info]


--Table no : 3 [silver].[crm_sales_details]'
--Data inserted into silver.crm_sales_details >> After transforming
SET @start_time = GETDATE();
Print '>> Truncating Table: silver.crm_sales_details';
Truncate table silver.crm_sales_details
Print '>> Inserting Data Into: silver.crm_sales_details';
insert into silver.crm_sales_details (
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
case 
	when  sls_order_dt = 0 or len(sls_order_dt) != 8 then null      
	else cast(cast(sls_order_dt as varchar) as date)		--->> handling invalid data & transform correct data type
end as sls_order_dt,
case 
	when  sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
	else cast(cast(sls_ship_dt as varchar) as date)			--->> handling invalid data & transform correct data type
end as sls_ship_dt,
case 
	when  sls_due_dt = 0 or len(sls_due_dt) != 8 then null
	else cast(cast(sls_due_dt as varchar) as date)			--->> handling invalid data & transform correct data type
end as sls_due_dt,
case 
	when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
	then sls_quantity * abs(sls_price)
	else sls_sales											-->> handling missing data & invalid data
end as sls_sales,											-->> Recalculate sales if original value is missing or incorrect
	sls_quantity,
case 
	when sls_price is null or sls_price <= 0							
	then sls_sales / nullif(sls_quantity, 0)				-->> handling missing data & invalid data
	else sls_price											-->> Recalculate Price if original value is missing or incorrect
end as sls_price
from [bronze].[crm_sales_details]


SET @end_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> --------------------'
--select * from [silver].[crm_sales_details]
--select * from [bronze].[crm_sales_details]

	PRINT '----------------------------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '----------------------------------------------------------';

--Table no : 4 [silver].[erp_cust_az12]'
--Data Transfered into [silver].[erp_cust_az12] >>> After Transforming
SET @start_time = GETDATE();
Print '>> Truncating Table: [silver].[erp_cust_az12]';
Truncate table [silver].[erp_cust_az12]
Print '>> Inserting Data Into: [silver].[erp_cust_az12]';
insert into [silver].[erp_cust_az12] (cid,bdate, gen)
select 
case 
	when cid like 'NAS%' then SUBSTRING(cid,4, len(cid))  -->>Remove 'NAS' prefix if present
	else cid
end cid,
case when bdate > GETDATE() then NULL
	else bdate								-->>Set Future birthdates to null
end as bdate,							
case when upper(trim(gen)) in ('F', 'Female') then 'Female'
	when upper(trim(gen)) in ('M', 'Male') then 'Male'
	else 'n/a'								-->> Normalize gender values and handle unknown cases
end as gen from [bronze].[erp_cust_az12]

SET @end_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> --------------------'

--select * from [silver].[erp_cust_az12]
--select * from [bronze].[erp_cust_az12]


--Table no : 5 [silver].[erp_loc_a101]
--Data transfered into [silver].[erp_loc_a101]  >>>After transforming
SET @start_time = GETDATE();
Print '>> Truncating Table: [silver].[erp_loc_a101]';
Truncate table [silver].[erp_loc_a101]
Print '>> Inserting Data Into: [silver].[erp_loc_a101]';
insert into [silver].[erp_loc_a101](cid,cntry)
select 
	REPLACE(cid, '-','')cid,                 --> Handle invalid values
	case 
		when trim(cntry) = 'DE' then 'Germany'
		when trim(cntry) in ('US', 'USA') then 'United States'
		when trim(cntry) = '' or cntry is null then 'n/a'
		else trim(cntry)
	end as cntry                             --> Normalize and Handle missing or blank country codes
from [bronze].[erp_loc_a101]

SET @end_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> --------------------'

--select * from [silver].[erp_loc_a101]
--select * from [bronze].[erp_loc_a101]



--Table no : 6 [silver].[erp_px_cat_g1v2]
--Data Transferred into [Silver].[erp_px_cat_g1v2]
SET @start_time = GETDATE();
Print '>> Truncating Table: Silver.erp_px_cat_g1v2';
Truncate table Silver.[erp_px_cat_g1v2]
Print '>> Inserting Data Into: Silver.erp_px_cat_g1v2';
insert into [silver].[erp_px_cat_g1v2](id,cat,subcat,maintenance)
select * from [bronze].[erp_px_cat_g1v2]	


SET @end_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> --------------------'
--select * from [silver].[erp_px_cat_g1v2]
--select * from [bronze].[erp_px_cat_g1v2]
SET @batch_end_time = GETDATE();
	PRINT '=========================================================='
	PRINT 'Loading Silver Layer is Completed';
	PRINT '>>  Total Load Duration:' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
	PRINT '=========================================================='
	END TRY
	BEGIN CATCH
	PRINT '==========================================================='
	PRINT 'Error OCCURED DURING LOADING BRONZE LAYER'
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
	PRINT '==========================================================='
END CATCH



END

EXEC silver.load_silver
