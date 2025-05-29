-- sql/create_tables.sql

-- Drop tables if they exist (idempotent runs)
DROP TABLE IF EXISTS fact_fhv_trips CASCADE;
DROP TABLE IF EXISTS dim_calendar CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_base CASCADE;

-- 1. Dimensions (references)
CREATE TABLE IF NOT EXISTS dim_base(
    base_num   VARCHAR(50) NOT NULL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_location (
  location_id   INTEGER      PRIMARY KEY,
  zone          VARCHAR(100),
  borough       VARCHAR(50),
  service_zone  VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_calendar (
  date_key     DATE      PRIMARY KEY,
  year         SMALLINT,
  quarter      SMALLINT,
  month        SMALLINT,
  day          SMALLINT,
  day_of_week  SMALLINT,
  is_weekend   BOOLEAN
);

-- 2. FHV travel fact table
CREATE TABLE IF NOT EXISTS fact_fhv_trips (
  trip_id                 SERIAL       PRIMARY KEY,
  hvfhs_license_num       VARCHAR(100) NOT NULL,
  dispatching_base_num    VARCHAR(50)  NOT NULL,
  originating_base_num    VARCHAR(50),
  request_datetime        TIMESTAMP    NOT NULL,
  on_scene_datetime       TIMESTAMP,
  pickup_datetime         TIMESTAMP,
  dropoff_datetime        TIMESTAMP,
  pu_location_id          INTEGER,
  do_location_id          INTEGER,
  trip_miles              DOUBLE PRECISION,
  trip_time               BIGINT,
  base_passenger_fare     DOUBLE PRECISION,
  tolls                   DOUBLE PRECISION,
  bcf                     DOUBLE PRECISION,
  sales_tax               DOUBLE PRECISION,
  congestion_surcharge    DOUBLE PRECISION,
  airport_fee             DOUBLE PRECISION,
  tips                    DOUBLE PRECISION,
  driver_pay              DOUBLE PRECISION,
  shared_request_flag     CHAR(1),
  shared_match_flag       CHAR(1),
  access_a_ride_flag      CHAR(1),
  wav_request_flag        CHAR(1),
  wav_match_flag          CHAR(1)
);
