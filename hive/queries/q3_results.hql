USE team6_projectdb;

-- Drop existing table
DROP TABLE IF EXISTS q3_results;

-- Create partitioned and bucketed table
CREATE EXTERNAL TABLE q3_results (
    zone STRING,
    trip_count BIGINT,
    total_airport_fees DOUBLE,
    avg_company_profit DOUBLE,
    avg_driver_income DOUBLE
)
PARTITIONED BY (month INT)
CLUSTERED BY (zone) INTO 4 BUCKETS
STORED AS ORC
LOCATION '/user/team6/project/hive/warehouse/q3_results';

-- Enable dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Insert data
INSERT OVERWRITE TABLE q3_results
PARTITION (month)
SELECT 
    l.zone,
    COUNT(*) AS trip_count,
    ROUND(SUM(f.airport_fee), 2) AS total_airport_fees,
    ROUND(AVG(f.base_passenger_fare + f.tolls + f.airport_fee - f.driver_pay), 2) AS avg_company_profit,
    ROUND(AVG(f.driver_pay + f.tips), 2) AS avg_driver_income,
    MONTH(FROM_UNIXTIME(CAST(f.request_datetime/1000 AS BIGINT))) AS month
FROM fact_fhv_trips_cleans f
JOIN dim_location l ON f.pu_location_id = l.location_id
WHERE 
    (l.zone LIKE '%Airport%' OR f.airport_fee > 0)
    AND f.request_datetime IS NOT NULL
GROUP BY 
    l.zone,
    MONTH(FROM_UNIXTIME(CAST(f.request_datetime/1000 AS BIGINT)))
ORDER BY 
    avg_company_profit DESC;