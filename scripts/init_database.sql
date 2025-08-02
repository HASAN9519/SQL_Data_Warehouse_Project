/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'my_datawarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'my_datawarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- following code is for postgres sql
-- it's the one used in this project
-- Drop and recreate the 'my_datawarehouse' database
-- NOTE: Requires superuser privileges

-- run this file in the psql:   \i E:/7_Data_Engineering_Projects/1_SQL_Data_WarehouseProject/scripts/init_database.sql

DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_database WHERE datname = 'my_datawarehouse') THEN
        -- Terminate connections to allow dropping
        PERFORM pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = 'my_datawarehouse' AND pid <> pg_backend_pid();
        
        DROP DATABASE my_datawarehouse;
    END IF;
END $$;

-- Create the database
CREATE DATABASE my_datawarehouse;
-- connecting database
\c my_datawarehouse;

-- Now connect to the new database manually via CLI or tool (outside SQL code)
-- For example, in psql: \c my_datawarehouse

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;




-- -- following code is for sql server
-- USE master;
-- GO

-- -- Drop and recreate the 'my_datawarehouse' database
-- IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'my_datawarehouse')
-- BEGIN
--     ALTER DATABASE my_datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
--     DROP DATABASE my_datawarehouse;
-- END;
-- GO

-- -- Create the 'my_datawarehouse' database
-- CREATE DATABASE my_datawarehouse;
-- GO

-- USE my_datawarehouse;
-- GO

-- -- Create Schemas
-- CREATE SCHEMA bronze;
-- GO

-- CREATE SCHEMA silver;
-- GO

-- CREATE SCHEMA gold;
-- GO