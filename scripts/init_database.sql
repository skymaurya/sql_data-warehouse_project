/*
=================================================

Create Database and Schemas

=================================================

Script purpose:
This script creates a new database named 'DataWarehouse' after checking if already exists.
If the database exists, it is dropped and recreated.
Additionally, the scripts sets up three schemas within the database: 'bronze','silver'and 'gold'.

warning:
	Running this scripts will drop the entire 'DataWarehouse' database if it exists.
	all data in the database will be permanently deleted. proceed with caution and
  ensure you have the proper backups before running this script.

*/


USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS(SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN
	ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;

END;
GO


-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
G0

USE DataWarehouse;
GO
 -- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
