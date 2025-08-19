-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Create catalog and scheme, grant permission

-- COMMAND ----------

create catalog "rsherstiak_nyc_catalog"
managed location 's3://databricks-rsherstiak-homework-7';

GRANT ALL PRIVILEGES ON CATALOG `rsherstiak_nyc_catalog` TO `deniskulemza1@gmail.com`;

CREATE SCHEMA IF NOT EXISTS rsherstiak_nyc_catalog.trips_schema;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create tables

-- COMMAND ----------

-- DROP TABLE IF EXISTS rsherstiak_nyc_catalog.trips_schema.raw_trips;

CREATE TABLE IF NOT EXISTS rsherstiak_nyc_catalog.trips_schema.raw_trips (
  DOLocationID           BIGINT,
  PULocationID           BIGINT,
  VendorID               BIGINT,
  pickup_datetime        TIMESTAMP_NTZ,
  dropoff_datetime       TIMESTAMP_NTZ,
  passenger_count        DOUBLE,
  trip_distance          DOUBLE,
  RatecodeID             DOUBLE,
  store_and_fwd_flag     STRING,
  payment_type           DOUBLE,
  fare_amount            DOUBLE,
  extra                  DOUBLE,
  mta_tax                DOUBLE,
  tip_amount             DOUBLE,
  tolls_amount           DOUBLE,
  improvement_surcharge  DOUBLE,
  total_amount           DOUBLE,
  congestion_surcharge   DOUBLE,
  airport_fee            DOUBLE,
  ehail_fee              DOUBLE,
  trip_type              DOUBLE,
  service_type           STRING,
  taxi_type              STRING,
  pickup_hour            BIGINT,
  pickup_day_of_week     STRING,
  duration_min           DOUBLE,
  pickup_zone            STRING,
  dropoff_zone           STRING
)
USING DELTA
LOCATION 's3://databricks-rsherstiak-homework-7/external/trips_schema/raw_trips';

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS rsherstiak_nyc_catalog.trips_schema.raw_trips (
  DOLocationID           BIGINT,
  PULocationID           BIGINT,
  VendorID               BIGINT,
  pickup_datetime        TIMESTAMP_NTZ,
  dropoff_datetime       TIMESTAMP_NTZ,
  passenger_count        DOUBLE,
  trip_distance          DOUBLE,
  RatecodeID             DOUBLE,
  store_and_fwd_flag     STRING,
  payment_type           DOUBLE,
  fare_amount            DOUBLE,
  extra                  DOUBLE,
  mta_tax                DOUBLE,
  tip_amount             DOUBLE,
  tolls_amount           DOUBLE,
  improvement_surcharge  DOUBLE,
  total_amount           DOUBLE,
  congestion_surcharge   DOUBLE,
  airport_fee            DOUBLE,
  ehail_fee              DOUBLE,
  trip_type              DOUBLE,
  service_type           STRING,
  taxi_type              STRING,
  pickup_hour            BIGINT,
  pickup_day_of_week     STRING,
  duration_min           DOUBLE,
  pickup_zone            STRING,
  dropoff_zone           STRING
)
USING DELTA
PARTITIONED BY (DOLocationID)
LOCATION 's3://databricks-rsherstiak-homework-7/external/trips_schema/raw_trips'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- COMMAND ----------

DROP TABLE IF EXISTS rsherstiak_nyc_catalog.trips_schema.zone_summary;

CREATE EXTERNAL TABLE rsherstiak_nyc_catalog.trips_schema.zone_summary (
  pickup_zone STRING,
  total_trips BIGINT,
  avg_trip_distance DOUBLE,
  avg_total_amount DOUBLE,
  avg_tip_amount DOUBLE,
  yellow_share DOUBLE,
  green_share DOUBLE,
  max_trip_distance DOUBLE,
  min_tip_amount DOUBLE,
  total_trip_amount DOUBLE
)
USING DELTA
LOCATION 's3://databricks-rsherstiak-homework-7/external/trips_schema/zone_summary_delta';

-- COMMAND ----------

-- DROP TABLE IF EXISTS rsherstiak_nyc_catalog.trips_schema.zone_days_summary;

CREATE TABLE IF NOT EXISTS rsherstiak_nyc_catalog.trips_schema.zone_days_summary (
  pickup_zone             STRING,
  pickup_date             DATE,
  Total_trips_per_day     BIGINT,
  Avg_duration_per_zone   DOUBLE,
  high_fare_share         DOUBLE
)
USING DELTA
LOCATION 's3://databricks-rsherstiak-homework-7/external/trips_schema/zone_days_summary';


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create mount

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC aws_bucket_name = "robot-dreams-source-data"
-- MAGIC mount_name = "robot-dreams-source-mount-romsh"
-- MAGIC dbutils.fs.mount(f"s3a://{aws_bucket_name}", f"/mnt/{mount_name}")
-- MAGIC display(dbutils.fs.ls(f"/mnt/{mount_name}/home-work-1"))