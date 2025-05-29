USE team6_projectdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE fact_fhv_trips_clean (
    hvfhs_license_num STRING,
    dispatching_base_num STRING,
    originating_base_num STRING,
    request_datetime BIGINT,
    on_scene_datetime BIGINT,
    pickup_datetime BIGINT,
    dropoff_datetime BIGINT,
    pu_location_id INT,
    do_location_id INT,
    trip_miles DOUBLE,
    trip_time BIGINT,
    base_passenger_fare DOUBLE,
    tolls DOUBLE,
    bcf DOUBLE,
    sales_tax DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    tips DOUBLE,
    driver_pay DOUBLE,
    shared_request_flag STRING,
    shared_match_flag STRING,
    access_a_ride_flag STRING,
    wav_request_flag STRING,
    wav_match_flag STRING,
    index_level_0 BIGINT
)
PARTITIONED BY (
    month INT
)
STORED AS AVRO
LOCATION '/user/team6/project/hive/warehouse/fact_fhv_trips_clean'
TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');

INSERT INTO TABLE fact_fhv_trips_clean
PARTITION (month)
SELECT
    f.hvfhs_license_num,
    f.dispatching_base_num,
    f.originating_base_num,
    f.request_datetime,
    f.on_scene_datetime,
    f.pickup_datetime,
    f.dropoff_datetime,
    f.pu_location_id,
    f.do_location_id,
    f.trip_miles,
    f.trip_time,
    f.base_passenger_fare,
    f.tolls,
    f.bcf,
    f.sales_tax,
    f.congestion_surcharge,
    f.airport_fee,
    f.tips,
    f.driver_pay,
    f.shared_request_flag,
    f.shared_match_flag,
    f.access_a_ride_flag,
    f.wav_request_flag,
    f.wav_match_flag,
    f.index_level_0,
    f.month
FROM fact_fhv_trips_part f
JOIN dim_base b1 ON f.dispatching_base_num = b1.base_num
JOIN dim_base b2 ON f.originating_base_num = b2.base_num
JOIN dim_location l1 ON f.pu_location_id = l1.location_id
JOIN dim_location l2 ON f.do_location_id = l2.location_id
WHERE f.month BETWEEN 1 AND 12
  AND f.hvfhs_license_num IN ('HV0002', 'HV0003', 'HV0004', 'HV0005')
  AND f.request_datetime IS NOT NULL
  AND f.pickup_datetime IS NOT NULL
  AND f.dropoff_datetime IS NOT NULL
  AND f.pu_location_id IS NOT NULL
  AND f.do_location_id IS NOT NULL
  AND f.trip_miles > 0
  AND f.trip_time > 0
  AND f.base_passenger_fare >= 0
  AND f.tolls >= 0
  AND f.bcf >= 0
  AND f.sales_tax >= 0
  AND f.congestion_surcharge >= 0
  AND f.airport_fee >= 0
  AND f.tips >= 0
  AND f.driver_pay >= 0
  AND f.shared_request_flag IN ('Y', 'N')
  AND f.shared_match_flag IN ('Y', 'N')
  AND f.access_a_ride_flag IN ('Y', 'N')
  AND f.wav_request_flag IN ('Y', 'N')
  AND f.wav_match_flag IN ('Y', 'N')
  AND f.index_level_0 IS NOT NULL
  AND f.month IS NOT NULL
  AND f.pickup_datetime < f.dropoff_datetime
  AND (f.on_scene_datetime IS NULL OR (f.on_scene_datetime >= f.request_datetime AND f.on_scene_datetime <= f.pickup_datetime))
  AND YEAR(FROM_UNIXTIME(CAST(f.request_datetime / 1000 AS BIGINT))) = 2024;