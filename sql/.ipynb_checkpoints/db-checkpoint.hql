DROP DATABASE IF EXISTS team6_projectdb CASCADE;
CREATE DATABASE team6_projectdb LOCATION '/user/team6/project/hive/warehouse';
USE team6_projectdb;

-- Create external table for dim_base
CREATE EXTERNAL TABLE dim_base
STORED AS AVRO
LOCATION '/user/team6/project/warehouse/dim_base'
TBLPROPERTIES ('avro.schema.url'='/user/team6/project/warehouse/avsc');

-- Check table contents
SELECT * FROM dim_base LIMIT 10;