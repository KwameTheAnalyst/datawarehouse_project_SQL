
/*
	================================================================
		Create Database, DataWarehouse
	==================================================================
	
	SCRIPT PURPOSE:
	The scripts checks if DataWarehouse exists, then drop it and create a new one.
	it permanantly deletes all data.
	The Script also set up three new schemas, bronze, silver and gold.

	WARNING:
	running the script drops the entire DataWarehouse database if it exists.
	All the data in the database will be permanantly deleted.
	Proceed with caution, and ensure you have a backup before running the script.
*/



USE master;
Go

-- Drop and recreate the DataWarehouse if it exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create Database DataWarehouse
CREATE DATABASE DataWarehouse;
Go

USE DataWarehouse;
Go

-- Create Schema
CREATE SCHEMA bronze;
Go
CREATE SCHEMA silver;
Go
CREATE SCHEMA gold;
