USE team6_projectdb;

SET hive.resultset.use.unique.column.names=false;

-- 1. Checking NULL values and anomalies in numeric fields
SELECT 'Numeric Anomalies' AS analysis_type, 
       'trip_miles' AS metric, 
       SUM(CASE WHEN trip_miles <= 0 THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN trip_miles <= 0 THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12
UNION ALL
SELECT 'Numeric Anomalies' AS analysis_type, 
       'trip_time' AS metric, 
       SUM(CASE WHEN trip_time <= 0 THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN trip_time <= 0 THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12
UNION ALL
SELECT 'Numeric Anomalies' AS analysis_type, 
       'base_passenger_fare' AS metric, 
       SUM(CASE WHEN base_passenger_fare < 0 THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN base_passenger_fare < 0 THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12
UNION ALL
SELECT 'Numeric Anomalies' AS analysis_type, 
       'tolls' AS metric, 
       SUM(CASE WHEN tolls < 0 THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN tolls < 0 THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12
UNION ALL
SELECT 'Numeric Anomalies' AS analysis_type, 
       'bcf' AS metric, 
       SUM(CASE WHEN bcf < 0 THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN bcf < 0 THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12
UNION ALL
SELECT 'Numeric Anomalies' AS analysis_type, 
       'sales_tax' AS metric, 
       SUM(CASE WHEN sales_tax < 0 THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN sales_tax < 0 THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12
UNION ALL
SELECT 'Numeric Anomalies' AS analysis_type, 
       'congestion_surcharge' AS metric, 
       SUM(CASE WHEN congestion_surcharge < 0 THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN congestion_surcharge < 0 THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12
UNION ALL
SELECT 'Numeric Anomalies' AS analysis_type, 
       'airport_fee' AS metric, 
       SUM(CASE WHEN airport_fee < 0 THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN airport_fee < 0 THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12
UNION ALL
SELECT 'Numeric Anomalies' AS analysis_type, 
       'tips' AS metric, 
       SUM(CASE WHEN tips < 0 THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN tips < 0 THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12
UNION ALL
SELECT 'Numeric Anomalies' AS analysis_type, 
       'driver_pay' AS metric, 
       SUM(CASE WHEN driver_pay < 0 THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN driver_pay < 0 THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12

-- 2. Checking for anomalies in time fields
UNION ALL
SELECT 'Time Anomalies' AS analysis_type, 
       'pickup_dropoff' AS metric, 
       SUM(CASE WHEN pickup_datetime >= dropoff_datetime THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN pickup_datetime >= dropoff_datetime THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12
UNION ALL
SELECT 'Time Anomalies' AS analysis_type, 
       'request_on_scene' AS metric, 
       SUM(CASE WHEN on_scene_datetime IS NOT NULL AND on_scene_datetime < request_datetime THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN on_scene_datetime IS NOT NULL AND on_scene_datetime < request_datetime THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12
UNION ALL
SELECT 'Time Anomalies' AS analysis_type, 
       'on_scene_pickup' AS metric, 
       SUM(CASE WHEN on_scene_datetime IS NOT NULL AND on_scene_datetime > pickup_datetime THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN on_scene_datetime IS NOT NULL AND on_scene_datetime > pickup_datetime THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12
UNION ALL
SELECT 'Time Anomalies' AS analysis_type, 
       'year' AS metric, 
       SUM(CASE WHEN YEAR(FROM_UNIXTIME(CAST(request_datetime / 1000 AS BIGINT))) != 2024 THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN YEAR(FROM_UNIXTIME(CAST(request_datetime / 1000 AS BIGINT))) != 2024 THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12

-- 3. Checking invalid hvfhs_license_num values
UNION ALL
SELECT 'License Anomalies' AS analysis_type, 
       COALESCE(hvfhs_license_num, 'NULL') AS metric, 
       COUNT(*) AS invalid_count,
       ROUND(COUNT(*) / CASE WHEN total_rows = 0 THEN 1 ELSE total_rows END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part,
     (SELECT COUNT(*) AS total_rows FROM fact_fhv_trips_part WHERE month BETWEEN 1 AND 12) t
WHERE month BETWEEN 1 AND 12
  AND hvfhs_license_num NOT IN ('HV0002', 'HV0003', 'HV0004', 'HV0005')
GROUP BY hvfhs_license_num, total_rows

-- 4. Checking for pu_location_id and do_location_id mismatches
UNION ALL
SELECT 'Location Anomalies' AS analysis_type, 
       'pu_location' AS metric, 
       SUM(CASE WHEN l1.location_id IS NULL THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN l1.location_id IS NULL THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part f
LEFT JOIN dim_location l1 ON f.pu_location_id = l1.location_id
LEFT JOIN dim_location l2 ON f.do_location_id = l2.location_id
WHERE f.month BETWEEN 1 AND 12
  AND (f.pu_location_id IS NOT NULL AND f.do_location_id IS NOT NULL)
UNION ALL
SELECT 'Location Anomalies' AS analysis_type, 
       'do_location' AS metric, 
       SUM(CASE WHEN l2.location_id IS NULL THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN l2.location_id IS NULL THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part f
LEFT JOIN dim_location l1 ON f.pu_location_id = l1.location_id
LEFT JOIN dim_location l2 ON f.do_location_id = l2.location_id
WHERE f.month BETWEEN 1 AND 12
  AND (f.pu_location_id IS NOT NULL AND f.do_location_id IS NOT NULL)

-- 5. Checking for dispatching_base_num and originating_base_num mismatches
UNION ALL
SELECT 'Base Anomalies' AS analysis_type, 
       'dispatching_base' AS metric, 
       SUM(CASE WHEN b1.base_num IS NULL THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN b1.base_num IS NULL THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part f
LEFT JOIN dim_base b1 ON f.dispatching_base_num = b1.base_num
LEFT JOIN dim_base b2 ON f.originating_base_num = b2.base_num
WHERE f.month BETWEEN 1 AND 12
  AND (f.dispatching_base_num IS NOT NULL AND f.originating_base_num IS NOT NULL)
UNION ALL
SELECT 'Base Anomalies' AS analysis_type, 
       'originating_base' AS metric, 
       SUM(CASE WHEN b2.base_num IS NULL THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN b2.base_num IS NULL THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part f
LEFT JOIN dim_base b1 ON f.dispatching_base_num = b1.base_num
LEFT JOIN dim_base b2 ON f.originating_base_num = b2.base_num
WHERE f.month BETWEEN 1 AND 12
  AND (f.dispatching_base_num IS NOT NULL AND f.originating_base_num IS NOT NULL)

-- 6. Checking invalid flags
UNION ALL
SELECT 'Flag Anomalies' AS analysis_type, 
       'shared_request_flag' AS metric, 
       SUM(CASE WHEN shared_request_flag NOT IN ('Y', 'N') THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN shared_request_flag NOT IN ('Y', 'N') THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12
UNION ALL
SELECT 'Flag Anomalies' AS analysis_type, 
       'shared_match_flag' AS metric, 
       SUM(CASE WHEN shared_match_flag NOT IN ('Y', 'N') THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN shared_match_flag NOT IN ('Y', 'N') THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12
UNION ALL
SELECT 'Flag Anomalies' AS analysis_type, 
       'access_a_ride_flag' AS metric, 
       SUM(CASE WHEN access_a_ride_flag NOT IN ('Y', 'N') THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN access_a_ride_flag NOT IN ('Y', 'N') THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12
UNION ALL
SELECT 'Flag Anomalies' AS analysis_type, 
       'wav_request_flag' AS metric, 
       SUM(CASE WHEN wav_request_flag NOT IN ('Y', 'N') THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN wav_request_flag NOT IN ('Y', 'N') THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12
UNION ALL
SELECT 'Flag Anomalies' AS analysis_type, 
       'wav_match_flag' AS metric, 
       SUM(CASE WHEN wav_match_flag NOT IN ('Y', 'N') THEN 1 ELSE 0 END) AS invalid_count,
       ROUND(SUM(CASE WHEN wav_match_flag NOT IN ('Y', 'N') THEN 1 ELSE 0 END) / CASE WHEN COUNT(*) = 0 THEN 1 ELSE COUNT(*) END * 100, 4) AS invalid_pct
FROM fact_fhv_trips_part
WHERE month BETWEEN 1 AND 12;