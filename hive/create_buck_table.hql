USE team6_projectdb;
CREATE EXTERNAL TABLE dim_location_bucketed (
    location_id INT,
    borough STRING,
    zone STRING,
    service_zone STRING
)
CLUSTERED BY (location_id) INTO 8 BUCKETS
STORED AS AVRO
LOCATION '/user/team6/project/hive/warehouse/dim_location_bucketed';
INSERT INTO dim_location_bucketed
SELECT location_id, zone, borough, service_zone
FROM dim_location;