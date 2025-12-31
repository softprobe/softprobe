-- Test different Iceberg catalog syntax for DuckDB
INSTALL iceberg;
LOAD iceberg;

-- Try method 1: ATTACH DATABASE
.print 'Method 1: ATTACH DATABASE'
ATTACH DATABASE IF NOT EXISTS 'iceberg_catalog' (TYPE iceberg);

-- Show attached databases
.print 'Attached databases:'
SHOW DATABASES;
