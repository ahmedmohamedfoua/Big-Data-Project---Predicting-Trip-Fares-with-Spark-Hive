-- sql/test_database.sql

-- 1. Checking the number of rows in the fact table
SELECT COUNT(*) AS total_fhv_trips FROM fact_fhv_trips;

-- 2. View the first 10 records
SELECT *
  FROM fact_fhv_trips
 LIMIT 10;

-- 3. Analytical example: average distance and travel time
SELECT
  AVG(trip_miles) AS avg_trip_miles,
  AVG(trip_time)  AS avg_trip_time
FROM fact_fhv_trips;

-- 4. Grouping example: top 5 dispatching_base_num by number of trips
SELECT
  dispatching_base_num,
  COUNT(*) AS trip_count
FROM fact_fhv_trips
GROUP BY dispatching_base_num
ORDER BY trip_count DESC
LIMIT 5;

SELECT 'dim_calendar' AS table_name, COUNT(*) FROM dim_calendar;