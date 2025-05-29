USE team6_projectdb;

DROP TABLE IF EXISTS q6_results;

CREATE TABLE q6_results (
    month INT,
    avg_driver_income DOUBLE,
    avg_company_profit DOUBLE
)
STORED AS ORC;

INSERT OVERWRITE TABLE q6_results
SELECT 
    dc.month,
    ROUND(AVG(f.driver_pay + f.tips), 2) AS avg_driver_income,
    ROUND(AVG(f.base_passenger_fare + f.tolls + f.bcf + f.sales_tax + f.congestion_surcharge + f.airport_fee - f.driver_pay - f.tips - (0.2 * (f.base_passenger_fare + f.tolls + f.bcf + f.sales_tax + f.congestion_surcharge + f.airport_fee))), 2) AS avg_company_profit
FROM fact_fhv_trips_part f
JOIN dim_location dl_pu
    ON f.pu_location_id = dl_pu.location_id
JOIN dim_location dl_do
    ON f.do_location_id = dl_do.location_id
JOIN dim_calendar dc
    ON f.request_datetime = dc.date_key
WHERE 
    f.trip_time > 60 
    AND f.driver_pay > 0 
    AND f.base_passenger_fare > 0 
    AND f.request_datetime IS NOT NULL
    AND f.on_scene_datetime IS NOT NULL
    AND f.on_scene_datetime > f.request_datetime
GROUP BY dc.month
ORDER BY dc.month ASC;