# Databricks notebook source
# MAGIC %md
# MAGIC ####Імпорт бібліотек

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, TimestampType, StringType, DoubleType, IntegerType
)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Оголошення змінних

# COMMAND ----------

base = "s3://robot-dreams-source-data"
path_green = f"{base}/home-work-1/nyc_taxi/green/"
path_yellow  = f"{base}/home-work-1/nyc_taxi/yellow/"
path_zone_lookup  = f"{base}/home-work-1/nyc_taxi/taxi_zone_lookup.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Завдання 2: Імпорт, уніфікація та об’єднання

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Імпортувати дані жовтих та зелених поїздок:

# COMMAND ----------

#read patches
df_files_green = (spark.read
    .option("recursiveFileLookup", "true")
    .parquet(path_green)
    .select("_metadata.file_path")
    .distinct())

df_files_yellow = (spark.read
    .option("recursiveFileLookup", "true")
    .parquet(path_yellow)
    .select("_metadata.file_path")
    .distinct())

keys_g = [r.file_path for r in df_files_green.collect()]
keys_y = [r.file_path for r in df_files_yellow.collect()]

# GREEN per-file -> cast -> union
green_all = None
for k in sorted(keys_g):
    df = spark.read.parquet(f"{k}")
    df_cast = df.select(
        F.col("VendorID").cast("long").alias("VendorID"),
        F.col("lpep_pickup_datetime").cast("timestamp_ntz").alias("lpep_pickup_datetime"),
        F.col("lpep_dropoff_datetime").cast("timestamp_ntz").alias("lpep_dropoff_datetime"),
        F.col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
        F.col("RatecodeID").cast("double").alias("RatecodeID"),
        F.col("PULocationID").cast("long").alias("PULocationID"),
        F.col("DOLocationID").cast("long").alias("DOLocationID"),
        F.col("passenger_count").cast("double").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("extra").cast("double").alias("extra"),
        F.col("mta_tax").cast("double").alias("mta_tax"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("tolls_amount").cast("double").alias("tolls_amount"),
        F.col("ehail_fee").cast("double").alias("ehail_fee"),
        F.col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("payment_type").cast("double").alias("payment_type"),
        F.col("trip_type").cast("double").alias("trip_type"),
        F.col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
        F.lit(None).cast("double").alias("airport_fee")
    )
    green_all = df_cast if green_all is None else green_all.unionByName(df_cast, allowMissingColumns=True)

# YELLOW per-file -> cast -> union
yellow_all = None
for k in sorted(keys_y):
    df = spark.read.parquet(f"{k}")
    df_cast = df.select(
        F.col("VendorID").cast("long").alias("VendorID"),
        F.col("tpep_pickup_datetime").cast("timestamp_ntz").alias("tpep_pickup_datetime"),
        F.col("tpep_dropoff_datetime").cast("timestamp_ntz").alias("tpep_dropoff_datetime"),
        F.col("passenger_count").cast("double").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("RatecodeID").cast("double").alias("RatecodeID"),
        F.col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
        F.col("PULocationID").cast("long").alias("PULocationID"),
        F.col("DOLocationID").cast("long").alias("DOLocationID"),
        F.col("payment_type").cast("double").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("extra").cast("double").alias("extra"),
        F.col("mta_tax").cast("double").alias("mta_tax"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("tolls_amount").cast("double").alias("tolls_amount"),
        F.col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
        F.col("airport_fee").cast("double").alias("airport_fee"),
        F.lit(None).cast("double").alias("ehail_fee"),
        F.lit(None).cast("double").alias("trip_type")
    )
    yellow_all = df_cast if yellow_all is None else yellow_all.unionByName(df_cast, allowMissingColumns=True) 

    #read zones
    df_zone_lookup = spark.read.csv(path_zone_lookup, header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Уніфікувати схеми:

# COMMAND ----------

# ===== GREEN -> aligned schema via plain select =====
green_ready = green_all.select(
        # ids
        F.col("VendorID").cast("long").alias("VendorID"),
        # datetimes -> unified names
        F.col("lpep_pickup_datetime").cast("timestamp_ntz").alias("pickup_datetime"),
        F.col("lpep_dropoff_datetime").cast("timestamp_ntz").alias("dropoff_datetime"),
        # measures
        F.col("passenger_count").cast("double").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("RatecodeID").cast("double").alias("RatecodeID"),
        F.col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
        F.col("PULocationID").cast("long").alias("PULocationID"),
        F.col("DOLocationID").cast("long").alias("DOLocationID"),
        F.col("payment_type").cast("double").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("extra").cast("double").alias("extra"),
        F.col("mta_tax").cast("double").alias("mta_tax"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("tolls_amount").cast("double").alias("tolls_amount"),
        F.col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
        # fields missing in green -> nulls with correct types
        F.lit(None).cast("double").alias("airport_fee"),
        # green-only fields kept
        F.col("ehail_fee").cast("double").alias("ehail_fee"),
        F.col("trip_type").cast("double").alias("trip_type"),
        # optional marker
        F.lit("green").alias("service_type")
).withColumn("taxi_type", F.lit("green"))

# ===== YELLOW -> aligned schema via plain select =====
yellow_ready = yellow_all.select(
        # ids
        F.col("VendorID").cast("long").alias("VendorID"),
        # datetimes -> unified names
        F.col("tpep_pickup_datetime").cast("timestamp_ntz").alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").cast("timestamp_ntz").alias("dropoff_datetime"),
        # measures
        F.col("passenger_count").cast("double").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("RatecodeID").cast("double").alias("RatecodeID"),
        F.col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
        F.col("PULocationID").cast("long").alias("PULocationID"),
        F.col("DOLocationID").cast("long").alias("DOLocationID"),
        F.col("payment_type").cast("double").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("extra").cast("double").alias("extra"),
        F.col("mta_tax").cast("double").alias("mta_tax"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("tolls_amount").cast("double").alias("tolls_amount"),
        F.col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
        # yellow field present
        F.col("airport_fee").cast("double").alias("airport_fee"),
        # fields missing in yellow -> nulls with correct types
        F.lit(None).cast("double").alias("ehail_fee"),
        F.lit(None).cast("double").alias("trip_type"),
        # optional marker
        F.lit("yellow").alias("service_type")
).withColumn("taxi_type", F.lit("yellow"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Об’єднати дані в один датафрейм raw_trips_df.

# COMMAND ----------

# Union into one final DataFrame
raw_trips_df  = green_ready.unionByName(yellow_ready, allowMissingColumns=True)


# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Виконати фільтрацію аномальних записів: * Вилучити поїздки з відстанню < 0.1 км, тарифом < $2, тривалістю < 1 хв.
# MAGIC   

# COMMAND ----------

filtered_trips_df = raw_trips_df.filter(
    (F.col("trip_distance") >= 0.1) &
    (F.col("total_amount")  >= 2.0) &
    (
        (
            F.col("dropoff_datetime").cast("timestamp").cast("long")
            - F.col("pickup_datetime").cast("timestamp").cast("long")
        ) / 60.0 >= 1.0
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Додати наступні колонки:   * pickup_hour   * pickup_day_of_week   * duration_min 

# COMMAND ----------

featured_trips_p_df = filtered_trips_df.select(
    "*",
    F.hour("pickup_datetime").alias("pickup_hour"),
    F.date_format("pickup_datetime", "EEEE").alias("pickup_day_of_week"),
    (
        (
            F.col("dropoff_datetime").cast("timestamp").cast("long")
            - F.col("pickup_datetime").cast("timestamp").cast("long")
        ).cast("double") / F.lit(60.0)
    ).alias("duration_min")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Виконати JOIN з taxi_zone_lookup.csv, додавши поля pickup_zone, dropoff_zone.

# COMMAND ----------

# Join zones -> final RAF-TRIPS-DF
raf_trips_df = (featured_trips_p_df
    .join(
        df_zone_lookup.select(
            F.col("LocationID").alias("PULocationID"),
            F.col("Zone").alias("pickup_zone")
        ),
        on="PULocationID", how="left"
    )
    .join(
        df_zone_lookup.select(
            F.col("LocationID").alias("DOLocationID"),
            F.col("Zone").alias("dropoff_zone")
        ),
        on="DOLocationID", how="left"
    )
)



# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Збереження в Delta Lake:
# MAGIC * Запишіть результат у Unity Catalog:
# MAGIC * Формат: Delta Lake

# COMMAND ----------


df_out = (raf_trips_df
    .select(
        F.col("DOLocationID").cast("bigint").alias("DOLocationID"),
        F.col("PULocationID").cast("bigint").alias("PULocationID"),
        F.col("VendorID").cast("bigint").alias("VendorID"),
        F.col("pickup_datetime").cast("timestamp_ntz").alias("pickup_datetime"),
        F.col("dropoff_datetime").cast("timestamp_ntz").alias("dropoff_datetime"),
        F.col("passenger_count").cast("double").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("RatecodeID").cast("double").alias("RatecodeID"),
        F.col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
        F.col("payment_type").cast("double").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("extra").cast("double").alias("extra"),
        F.col("mta_tax").cast("double").alias("mta_tax"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("tolls_amount").cast("double").alias("tolls_amount"),
        F.col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
        F.col("airport_fee").cast("double").alias("airport_fee"),
        F.col("ehail_fee").cast("double").alias("ehail_fee"),
        F.col("trip_type").cast("double").alias("trip_type"),
        F.col("service_type").cast("string").alias("service_type"),
        F.col("taxi_type").cast("string").alias("taxi_type"),
        F.col("pickup_hour").cast("bigint").alias("pickup_hour"),
        F.col("pickup_day_of_week").cast("string").alias("pickup_day_of_week"),
        F.col("duration_min").cast("double").alias("duration_min"),
        F.col("pickup_zone").cast("string").alias("pickup_zone"),
        F.col("dropoff_zone").cast("string").alias("dropoff_zone"),
    )
)


# df_out.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .option("mergeSchema", "true")\
#     .saveAsTable("rsherstiak_nyc_catalog.trips_schema.raw_trips")


# COMMAND ----------

# display(spark.sql("SELECT COUNT(*) AS rows FROM rsherstiak_nyc_catalog.trips_schema.raw_trips"))

# COMMAND ----------

display(spark.sql("SELECT * FROM rsherstiak_nyc_catalog.trips_schema.raw_trips LIMIT 100"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Завдання 3: Зведена аналітика zone_summary
# MAGIC
# MAGIC ####Агрегація zone_summary:
# MAGIC * Створіть датафрейм з колонками:
# MAGIC pickup_zone
# MAGIC total_trips
# MAGIC avg_trip_distance
# MAGIC avg_total_amount
# MAGIC avg_tip_amount
# MAGIC yellow_share / green_share
# MAGIC max_trip_distance
# MAGIC min_tip_amount
# MAGIC total_trip\amount

# COMMAND ----------

agg_df = (
    df_out.groupBy("pickup_zone")
    .agg(
        F.count("*").alias("total_trips"),  # total trips
        F.avg("trip_distance").alias("avg_trip_distance"),  # average trip distance
        F.avg("total_amount").alias("avg_total_amount"),  # average total amount
        F.avg("tip_amount").alias("avg_tip_amount"),  # average tip
        (F.sum(F.when(F.col("taxi_type") == "yellow", 1).otherwise(0)) 
         / F.count("*")).alias("yellow_share"),  # yellow taxi share
        (F.sum(F.when(F.col("taxi_type") == "green", 1).otherwise(0)) 
         / F.count("*")).alias("green_share"),  # green taxi share
        F.max("trip_distance").alias("max_trip_distance"),  # maximum trip distance
        F.min("tip_amount").alias("min_tip_amount"),  # minimum tip
        F.sum("total_amount").alias("total_trip_amount")  # total amount sum
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Збереження результатів:
# MAGIC   * Формат: Delta
# MAGIC   * Таблиця: zone_summary
# MAGIC   * Розміщення: Unity Catalog + S3 шлях (як external location або managed)

# COMMAND ----------

 agg_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true")\
    .saveAsTable("rsherstiak_nyc_catalog.trips_schema.zone_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Завдання 4: Додаткові аналітичні розрахунки
# MAGIC
# MAGIC ####1. Агрегація по днях тижня:
# MAGIC     * Використайте raw_trips або zone_summary
# MAGIC     * Розрахуйте:
# MAGIC     * Total_trips_per_day
# MAGIC     * Avg_duration_per_zone
# MAGIC     * high_fare_share (fare > $30)
# MAGIC
# MAGIC

# COMMAND ----------

df_with_date = df_out.withColumn("pickup_date", F.to_date(F.col("pickup_datetime")))

# 2) build per-zone-per-day summary
df_zone_days_summary = (
    df_with_date
    .groupBy("pickup_zone", "pickup_date")
    .agg(
        F.count("*").alias("Total_trips_per_day"),                         
        F.avg(F.col("duration_min")).alias("Avg_duration_per_zone"),       
        F.avg(F.when(F.col("fare_amount") > 30, 1.0).otherwise(0.0))       
          .alias("high_fare_share")    )

    )

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Збереження результату:
# MAGIC   * Таблиця: zone_days_summary
# MAGIC   * Формат: Delta
# MAGIC   * Розміщення: Unity Catalog + S3 шлях (як external location або managed)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS rsherstiak_nyc_catalog.trips_schema.zone_days_summary;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS rsherstiak_nyc_catalog.trips_schema.zone_days_summary (
# MAGIC   pickup_zone             STRING,
# MAGIC   pickup_date             DATE,
# MAGIC   Total_trips_per_day     BIGINT,
# MAGIC   Avg_duration_per_zone   DOUBLE,
# MAGIC   high_fare_share         DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://databricks-rsherstiak-homework-7/external/trips_schema/zone_days_summary';
# MAGIC

# COMMAND ----------

df_zone_days_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("rsherstiak_nyc_catalog.trips_schema.zone_days_summary")

# COMMAND ----------

display(spark.sql("SELECT * FROM rsherstiak_nyc_catalog.trips_schema.zone_days_summary LIMIT 100"))