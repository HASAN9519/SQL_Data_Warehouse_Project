-- code for postgres sql without procedure
-- can use this code or E:\7_Data_Engineering_Projects\1_SQL_Data_WarehouseProject\scripts\silver\proc_load_silver.sql

-- run this file in the psql:   \i E:/7_Data_Engineering_Projects/1_SQL_Data_WarehouseProject/scripts/silver/proc_load_silver_without_PROCEDURE.sql

-- connecting to database
\c my_datawarehouse;

DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP := clock_timestamp();
    batch_end_time TIMESTAMP;
BEGIN
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';

	RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '================================================';

    -- Loading Silver.crm_cust_info
    start_time := clock_timestamp();
    TRUNCATE TABLE silver.crm_cust_info;
    RAISE NOTICE '>> Truncated: silver.crm_cust_info';

    INSERT INTO silver.crm_cust_info (
        cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE UPPER(TRIM(cst_marital_status))
            WHEN 'S' THEN 'Single'
            WHEN 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status, -- Normalize marital status values to readable format
        CASE UPPER(TRIM(cst_gndr))
            WHEN 'F' THEN 'Female'
            WHEN 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr, -- Normalize gender values to readable format
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;	-- Select the most recent record per customer

    end_time := clock_timestamp();
    RAISE NOTICE '>> Loaded silver.crm_cust_info in % seconds', EXTRACT(EPOCH FROM end_time - start_time);


	-- Loading silver.crm_prd_info
	start_time := clock_timestamp();
	TRUNCATE TABLE silver.crm_prd_info;
	RAISE NOTICE '>> Truncated: silver.crm_prd_info';

	INSERT INTO silver.crm_prd_info (
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
		REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') AS cat_id, -- Extract category ID
		SUBSTRING(prd_key FROM 7) AS prd_key,        -- Extract product key, PostgreSQL uses this style for substring
		prd_nm,
		COALESCE(prd_cost, 0),	-- COALESCE(x, y) is used in PostgreSQL instead of ISNULL(x, y)
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END,
		prd_start_dt::DATE,
		(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day')::DATE
	FROM bronze.crm_prd_info;

	end_time := clock_timestamp();
	RAISE NOTICE '>> Loaded silver.crm_prd_info in % seconds', EXTRACT(EPOCH FROM end_time - start_time);


	-- Loading silver.crm_sales_details
	start_time := clock_timestamp();
	TRUNCATE TABLE silver.crm_sales_details;
	RAISE NOTICE '>> Truncated: silver.crm_sales_details';

	INSERT INTO silver.crm_sales_details (
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

		CASE 
			WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
			ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
		END,
		CASE 
			WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
			ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
		END,
		CASE 
			WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
			ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
		END,
		CASE 
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
		sls_quantity,
		CASE 
			WHEN sls_price IS NULL OR sls_price <= 0 
				THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price
		END	AS sls_sales -- Derive price if original value is invalid
	FROM bronze.crm_sales_details;

	end_time := clock_timestamp();
	RAISE NOTICE '>> Loaded silver.crm_sales_details in % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '================================================';

	-- Loading silver.erp_cust_az12
	start_time := clock_timestamp();
	TRUNCATE TABLE silver.erp_cust_az12;
	RAISE NOTICE '>> Truncated: silver.erp_cust_az12';

	INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
	)
	SELECT
		CASE
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4) -- Remove 'NAS' prefix
			ELSE cid
		END,
		CASE
			WHEN bdate > CURRENT_DATE THEN NULL
			ELSE bdate
		END AS bdate, -- Set future birthdates to NULL
		CASE 
			WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen -- Normalize gender values and handle unknown cases
	FROM bronze.erp_cust_az12;

	end_time := clock_timestamp();
	RAISE NOTICE '>> Loaded silver.erp_cust_az12 in % seconds', EXTRACT(EPOCH FROM end_time - start_time);


	-- Loading silver.erp_loc_a101
	start_time := clock_timestamp();
	TRUNCATE TABLE silver.erp_loc_a101;
	RAISE NOTICE '>> Truncated: silver.erp_loc_a101';

	INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
	)
	SELECT
		REPLACE(cid, '-', ''),
		CASE
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry -- Normalize and Handle missing or blank country codes
	FROM bronze.erp_loc_a101;

	end_time := clock_timestamp();
	RAISE NOTICE '>> Loaded silver.erp_loc_a101 in % seconds', EXTRACT(EPOCH FROM end_time - start_time);

	-- Loading silver.erp_px_cat_g1v2

	start_time := clock_timestamp();
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	RAISE NOTICE '>> Truncated: silver.erp_px_cat_g1v2';

	INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;

	end_time := clock_timestamp();
	RAISE NOTICE '>> Loaded silver.erp_px_cat_g1v2 in % seconds', EXTRACT(EPOCH FROM end_time - start_time);


    batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Silver Layer Completed in % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        RAISE NOTICE 'Message: %', SQLERRM;
        RAISE NOTICE '==========================================';
END $$;