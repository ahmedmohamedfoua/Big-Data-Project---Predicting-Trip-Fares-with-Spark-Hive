USE team6_projectdb;

-- Check fact_fhv_trips_part
SELECT * FROM fact_fhv_trips_part WHERE month = 6 LIMIT 10;

-- Check dim_location
SELECT * FROM dim_location LIMIT 10;

-- Check dim_calendar
SELECT * FROM dim_calendar WHERE year = 2024 LIMIT 10;

-- Check dim_base
SELECT * FROM dim_base LIMIT 10;

-- Check JOIN
SELECT f.pu_location_id, dl.zone, COUNT(*) AS trip_count
FROM fact_fhv_trips_part f
JOIN dim_location dl ON f.pu_location_id = dl.location_id
WHERE f.month = 6
GROUP BY f.pu_location_id, dl.zone
LIMIT 10;