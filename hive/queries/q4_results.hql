USE team6_projectdb;

-- Drop existing table
DROP TABLE IF EXISTS q4_results;

-- Create partitioned and bucketed table
CREATE EXTERNAL TABLE q4_results (
    from_zone STRING,
    to_zone STRING,
    empty_runs_count BIGINT,
    avg_distance_miles DOUBLE
)
PARTITIONED BY (month INT)
CLUSTERED BY (from_zone) INTO 4 BUCKETS
STORED AS ORC
LOCATION '/user/team6/project/hive/warehouse/q4_results';

-- Enable dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Insert data
WITH vehicle_movements AS (
    SELECT 
        dispatching_base_num,
        pu_location_id,
        do_location_id,
        request_datetime,
        LEAD(pu_location_id) OVER(PARTITION BY dispatching_base_num ORDER BY request_datetime) AS next_pu_location,
        MONTH(FROM_UNIXTIME(CAST(request_datetime/1000 AS BIGINT))) AS month
    FROM fact_fhv_trips_clean
)
INSERT OVERWRITE TABLE q4_results
PARTITION (month)
SELECT 
    l1.zone AS from_zone,
    l2.zone AS to_zone,
    COUNT(*) AS empty_runs_count,
    ROUND(AVG(f.trip_miles), 2) AS avg_distance_miles,
    v.month
FROM vehicle_movements v
JOIN fact_fhv_trips_part f 
    ON v.dispatching_base_num = f.dispatching_base_num 
    AND v.request_datetime = f.request_datetime
JOIN dim_location l1 ON v.do_location_id = l1.location_id
JOIN dim_location l2 ON v.next_pu_location = l2.location_id
WHERE 
    v.do_location_id != v.next_pu_location
    AND v.request_datetime IS NOT NULL
GROUP BY 
    l1.zone,
    l2.zone,
    v.month
ORDER BY 
    empty_runs_count DESC
LIMIT 1000;