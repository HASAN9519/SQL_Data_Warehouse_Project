/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - PostgreSQL’s COPY needs proper file permissions and usually requires superuser access or configuration of COPY FROM PROGRAM.
	- This script uses RAISE NOTICE for logging. You can switch to RAISE INFO or RAISE EXCEPTION depending on severity.
	- SQLERRM stands for SQL Error Message—it's a special variable used inside an EXCEPTION block to capture and 
	return the most recent error message that occurred.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any value

===============================================================================
*/

-- ************************************************************              code for PostgreSQL

/* 

-- first run following code on psql, it will Create a procedure in Procedures section of a schema
\c my_datawarehouse; 
\i E:/7_Data_Engineering_Projects/1_SQL_Data_WarehouseProject/scripts/bronze/proc_load_bronze.sql

-- execute command for running PostgreSQL procedure:		 
\c my_datawarehouse; 
CALL bronze.load_bronze();

*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql -- plpgsql tells database that a block of code—like a function or DO statement—is written in PL/pgSQL, which stands for Procedural
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP := clock_timestamp();
    batch_end_time TIMESTAMP;
BEGIN

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- bronze.crm_cust_info
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_cust_info;
    RAISE NOTICE '>> Truncated: bronze.crm_cust_info';
	
    -- Import data (requires superuser or appropriate role + file permissions)
    COPY bronze.crm_cust_info FROM 'E:/7_Data_Engineering_Projects/1_SQL_Data_WarehouseProject/datasets/source_crm/cust_info.csv' WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE '>> Loaded bronze.crm_cust_info in % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- bronze.crm_prd_info
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info;
    RAISE NOTICE '>> Truncated: bronze.crm_prd_info';

    COPY bronze.crm_prd_info FROM 'E:/7_Data_Engineering_Projects/1_SQL_Data_WarehouseProject/datasets/source_crm/prd_info.csv' WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE '>> Loaded bronze.crm_prd_info in % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- bronze.crm_sales_details
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details;
    RAISE NOTICE '>> Truncated: bronze.crm_sales_details';

    COPY bronze.crm_sales_details FROM 'E:/7_Data_Engineering_Projects/1_SQL_Data_WarehouseProject/datasets/source_crm/sales_details.csv' WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE '>> Loaded bronze.crm_sales_details in % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -- bronze.erp_loc_a101
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101;
    RAISE NOTICE '>> Truncated: bronze.erp_loc_a101';

    COPY bronze.erp_loc_a101 FROM 'E:/7_Data_Engineering_Projects/1_SQL_Data_WarehouseProject/datasets/source_erp/LOC_A101.csv' WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE '>> Loaded bronze.erp_loc_a101 in % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- bronze.erp_cust_az12
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    RAISE NOTICE '>> Truncated: bronze.erp_cust_az12';

    COPY bronze.erp_cust_az12 FROM 'E:/7_Data_Engineering_Projects/1_SQL_Data_WarehouseProject/datasets/source_erp/CUST_AZ12.csv' WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE '>> Loaded bronze.erp_cust_az12 in % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- bronze.erp_px_cat_g1v2
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    RAISE NOTICE '>> Truncated: bronze.erp_px_cat_g1v2';

    COPY bronze.erp_px_cat_g1v2 FROM 'E:/7_Data_Engineering_Projects/1_SQL_Data_WarehouseProject/datasets/source_erp/PX_CAT_G1V2.csv' WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE '>> Loaded bronze.erp_px_cat_g1v2 in % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Bronze Layer Load Completed in % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'Error During Bronze Layer Load';
        RAISE NOTICE 'Message: %', SQLERRM;
        RAISE NOTICE '==========================================';
END $$;



-- ************************************************************              code for sql server

/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

-- execute command for sql server procedure:		EXEC bronze.load_bronze;
    
===============================================================================
*/

-- CREATE OR ALTER PROCEDURE bronze.load_bronze AS
-- BEGIN
-- 	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
-- 	BEGIN TRY
-- 		SET @batch_start_time = GETDATE();
-- 		PRINT '================================================';
-- 		PRINT 'Loading Bronze Layer';
-- 		PRINT '================================================';

-- 		PRINT '------------------------------------------------';
-- 		PRINT 'Loading CRM Tables';
-- 		PRINT '------------------------------------------------';

-- 		SET @start_time = GETDATE();
-- 		PRINT '>> Truncating Table: bronze.crm_cust_info';
-- 		TRUNCATE TABLE bronze.crm_cust_info;
-- 		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
-- 		BULK INSERT bronze.crm_cust_info
-- 		FROM 'C:\sql\dwh_project\datasets\source_crm\cust_info.csv'
-- 		WITH (
-- 			FIRSTROW = 2,
-- 			FIELDTERMINATOR = ',',
-- 			TABLOCK
-- 		);
-- 		SET @end_time = GETDATE();
-- 		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
-- 		PRINT '>> -------------';

--         SET @start_time = GETDATE();
-- 		PRINT '>> Truncating Table: bronze.crm_prd_info';
-- 		TRUNCATE TABLE bronze.crm_prd_info;

-- 		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
-- 		BULK INSERT bronze.crm_prd_info
-- 		FROM 'C:\sql\dwh_project\datasets\source_crm\prd_info.csv'
-- 		WITH (
-- 			FIRSTROW = 2,
-- 			FIELDTERMINATOR = ',',
-- 			TABLOCK
-- 		);
-- 		SET @end_time = GETDATE();
-- 		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
-- 		PRINT '>> -------------';

--         SET @start_time = GETDATE();
-- 		PRINT '>> Truncating Table: bronze.crm_sales_details';
-- 		TRUNCATE TABLE bronze.crm_sales_details;
-- 		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
-- 		BULK INSERT bronze.crm_sales_details
-- 		FROM 'C:\sql\dwh_project\datasets\source_crm\sales_details.csv'
-- 		WITH (
-- 			FIRSTROW = 2,
-- 			FIELDTERMINATOR = ',',
-- 			TABLOCK
-- 		);
-- 		SET @end_time = GETDATE();
-- 		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
-- 		PRINT '>> -------------';

-- 		PRINT '------------------------------------------------';
-- 		PRINT 'Loading ERP Tables';
-- 		PRINT '------------------------------------------------';
		
-- 		SET @start_time = GETDATE();
-- 		PRINT '>> Truncating Table: bronze.erp_loc_a101';
-- 		TRUNCATE TABLE bronze.erp_loc_a101;
-- 		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
-- 		BULK INSERT bronze.erp_loc_a101
-- 		FROM 'C:\sql\dwh_project\datasets\source_erp\loc_a101.csv'
-- 		WITH (
-- 			FIRSTROW = 2,
-- 			FIELDTERMINATOR = ',',
-- 			TABLOCK
-- 		);
-- 		SET @end_time = GETDATE();
-- 		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
-- 		PRINT '>> -------------';

-- 		SET @start_time = GETDATE();
-- 		PRINT '>> Truncating Table: bronze.erp_cust_az12';
-- 		TRUNCATE TABLE bronze.erp_cust_az12;
-- 		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
-- 		BULK INSERT bronze.erp_cust_az12
-- 		FROM 'C:\sql\dwh_project\datasets\source_erp\cust_az12.csv'
-- 		WITH (
-- 			FIRSTROW = 2,
-- 			FIELDTERMINATOR = ',',
-- 			TABLOCK
-- 		);
-- 		SET @end_time = GETDATE();
-- 		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
-- 		PRINT '>> -------------';

-- 		SET @start_time = GETDATE();
-- 		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
-- 		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
-- 		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
-- 		BULK INSERT bronze.erp_px_cat_g1v2
-- 		FROM 'C:\sql\dwh_project\datasets\source_erp\px_cat_g1v2.csv'
-- 		WITH (
-- 			FIRSTROW = 2,
-- 			FIELDTERMINATOR = ',',
-- 			TABLOCK
-- 		);
-- 		SET @end_time = GETDATE();
-- 		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
-- 		PRINT '>> -------------';

-- 		SET @batch_end_time = GETDATE();
-- 		PRINT '=========================================='
-- 		PRINT 'Loading Bronze Layer is Completed';
--         PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
-- 		PRINT '=========================================='
-- 	END TRY
-- 	BEGIN CATCH
-- 		PRINT '=========================================='
-- 		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
-- 		PRINT 'Error Message' + ERROR_MESSAGE();
-- 		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
-- 		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
-- 		PRINT '=========================================='
-- 	END CATCH
-- END
