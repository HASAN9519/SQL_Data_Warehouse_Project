-- code for postgres sql without procedure
-- can use this code or E:\7_Data_Engineering_Projects\1_SQL_Data_WarehouseProject\scripts\bronze\proc_load_bronze.sql

-- run this file in the psql:   \i E:/7_Data_Engineering_Projects/1_SQL_Data_WarehouseProject/scripts/bronze/proc_load_bronze_without_PROCEDURE.sql

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