USE team6_projectdb;

DROP TABLE IF EXISTS q2_results;

CREATE EXTERNAL TABLE q2_results (
    zone STRING,
    borough STRING,
    trip_count BIGINT,
    avg_tip DOUBLE,
    tip_percentage DOUBLE,
    avg_fare DOUBLE,
    fare_to_tip_ratio DOUBLE,
    zone_category STRING
)
PARTITIONED BY (month INT)
CLUSTERED BY (zone) INTO 4 BUCKETS
STORED AS ORC
LOCATION '/user/team6/project/hive/warehouse/q2_results';

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE q2_results
PARTITION (month)
SELECT 
    l.zone,
    l.borough,
    COUNT(*) AS trip_count,
    ROUND(AVG(f.tips), 2) AS avg_tip,
    ROUND(AVG(f.tips/f.base_passenger_fare)*100, 2) AS tip_percentage,
    ROUND(AVG(f.base_passenger_fare), 2) AS avg_fare,
    ROUND(AVG(f.base_passenger_fare/f.tips), 2) AS fare_to_tip_ratio,
    CASE
        WHEN l.zone LIKE '%Airport%' THEN 'Airport'
        WHEN l.zone LIKE '%Downtown%' OR l.zone LIKE '%Financial District%' THEN 'Business'
        WHEN l.zone LIKE '%Theatre District%' OR l.zone LIKE '%Times Sq%' THEN 'Tourist'
        ELSE 'Residential'
    END AS zone_category,
    MONTH(FROM_UNIXTIME(CAST(f.request_datetime/1000 AS BIGINT))) AS month
FROM fact_fhv_trips_clean f
JOIN dim_location l ON f.pu_location_id = l.location_id
WHERE 
    f.tips > 0 
    AND f.base_passenger_fare > 0
    AND f.trip_time > 60
    AND l.location_id IS NOT NULL
    AND l.borough IS NOT NULL
    AND f.request_datetime IS NOT NULL
    AND f.on_scene_datetime IS NOT NULL
    AND f.on_scene_datetime > f.request_datetime
GROUP BY 
    l.zone,
    l.borough,
    CASE
        WHEN l.zone LIKE '%Airport%' THEN 'Airport'
        WHEN l.zone LIKE '%Downtown%' OR l.zone LIKE '%Financial District%' THEN 'Business'
        WHEN l.zone LIKE '%Theatre District%' OR l.zone LIKE '%Times Sq%' THEN 'Tourist'
        ELSE 'Residential'
    END,
    MONTH(FROM_UNIXTIME(CAST(f.request_datetime/1000 AS BIGINT)))
ORDER BY 
    avg_tip DESC;