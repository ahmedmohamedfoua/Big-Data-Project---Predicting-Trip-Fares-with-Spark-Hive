#!/usr/bin/env python3
"""
scripts/build_projectdb.py

Builds the PostgreSQL project database by:
 1. Dropping and creating schema (dimensions + fact table)
 2. Writing fact data from Parquet directly via Spark JDBC
 3. Populating dimension tables (base and location)
 4. Running test queries
"""
import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Connection parameters
db_host = 'hadoop-04.uni.innopolis.ru'
db_port = '5432'
db_name = 'team6_projectdb'
db_user = 'team6'
db_pass = open(os.path.join('secrets', '.psql.pass')).read().strip()

# JDBC URL and properties
jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
jdbc_props = {
    "user": db_user,
    "password": db_pass,
    "driver": "org.postgresql.Driver",
    "batchsize": "50000",  # Optimal for 4MB work_mem
    "rewriteBatchedInserts": "true",
    "numPartitions": "16",  # Don't exceed max_connections/6
    "fetchsize": "10000",
    "isolationLevel": "READ_COMMITTED"  # Reduces locking
}

# Paths
BASE_DIR    = os.path.dirname(__file__)
SQL_DIR     = os.path.join(BASE_DIR, '..', 'sql')
DATA_DIR    = os.path.join(BASE_DIR, '..', 'data')

CREATE_SQL  = os.path.join(SQL_DIR, 'create_tables.sql')
TEST_SQL    = os.path.join(SQL_DIR, 'test_database.sql')

# Utility: run SQL file via psycopg2
def run_sql_file(cur, path):
    with open(path, 'r') as f:
        sql_queries = f.read().split(';')

        for query in sql_queries:
            query = query.strip()
            if query:
                print(f"\nExecuting: {query[:50]}...")
                cur.execute(query)
                try:
                    results = cur.fetchall()
                    if results:
                        print("Result:")
                        for row in results:
                            print(row)
                except psycopg2.ProgrammingError:
                    pass
# === STEP 1: Initialize the circuit ===
print('=== Connecting to Postgres ===')
conn = psycopg2.connect(
    host=db_host, port=db_port, dbname=db_name,
    user=db_user, password=db_pass
)
conn.autocommit = True
cur = conn.cursor()

print('=== Running create_tables.sql ===')
run_sql_file(cur, CREATE_SQL)

# === STEP 2: Spark to record dimensions + facts ===
print('=== Starting Spark session for JDBC write ===')
spark = SparkSession.builder \
    .appName('FHV Trips Load') \
    .config('spark.jars', '/shared/postgresql-42.6.1.jar') \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.executor.cores", "4") \
    .config("spark.default.parallelism", "100") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.3") \
    .getOrCreate()

print('=== Reading Parquet files ===')
parquet_path = os.path.join(DATA_DIR, '*.parquet')
df = spark.read.parquet(f"file://{os.path.abspath(parquet_path)}")

# 2.1) Fill in dim_base
print("=== Writing dim_base ===")
bases_df = (
    df.select(col("dispatching_base_num").alias("base_num"))
      .union(df.select(col("originating_base_num").alias("base_num")))
      .distinct()
      .filter(
          (col("base_num").isNotNull()) &
          (col("base_num") != "nan") &
          (col("base_num") != "")
      )
)
bases_df.write.jdbc(
    url=jdbc_url,
    table="dim_base",
    mode="append",
    properties=jdbc_props
)

# 2.2) Fill in dim_location
print("=== Writing dim_location ===")
zones = spark.read.csv(
    f"file://{os.path.abspath(DATA_DIR)}/taxi_zone_lookup.csv",
    header=True, inferSchema=True
).select(
    col("LocationID").alias("location_id"),
    col("Zone").alias("zone"),
    col("Borough").alias("borough"),
    col("service_zone")
)
zones.write.jdbc(
    url=jdbc_url,
    table="dim_location",
    mode="append",
    properties=jdbc_props
)

# 2.3) Now we write a fact table
print('=== Writing fact_fhv_trips ===')
print(f"Estimated row count: {df.count()}")  # This will show progress
df_fact = df.withColumnRenamed("PULocationID", "pu_location_id") \
            .withColumnRenamed("DOLocationID", "do_location_id")

df_fact.write.jdbc(
    url=jdbc_url,
    table='fact_fhv_trips',
    mode="overwrite",  # for initial load
    # mode = "append",   # for incremental loads
    properties=jdbc_props
)

# Filling dim_calendar from the initial data
print("=== Writing dim_calendar ===")
from pyspark.sql.functions import col, explode, to_date, array

date_cols = [
    to_date(col("request_datetime")).alias("request_date"),
    to_date(col("on_scene_datetime")).alias("on_scene_date"),
    to_date(col("pickup_datetime")).alias("pickup_date"),
    to_date(col("dropoff_datetime")).alias("dropoff_date")
]

date_df = df.select(*date_cols)

calendar_df = date_df.select(
    explode(array(
        col("request_date"),
        col("on_scene_date"),
        col("pickup_date"),
        col("dropoff_date")
    )).alias("dt")
).distinct().filter(col("dt").isNotNull())

from pyspark.sql.functions import year, quarter, month, dayofmonth, dayofweek
from pyspark.sql.types import ShortType

calendar_df = calendar_df.select(
    col("dt").alias("date_key"),
    year("dt").cast(ShortType()).alias("year"),
    quarter("dt").cast(ShortType()).alias("quarter"),
    month("dt").cast(ShortType()).alias("month"),
    dayofmonth("dt").cast(ShortType()).alias("day"),
    dayofweek("dt").cast(ShortType()).alias("day_of_week"),
    dayofweek("dt").isin(1, 7).alias("is_weekend")
)

calendar_df.write.jdbc(
    url=jdbc_url,
    table="dim_calendar",
    mode="append",
    properties=jdbc_props
)

print("JDBC write operation submitted to Spark")
spark.stop()

# === STEP 3: Test queries ===
print('=== Running test queries ===')
run_sql_file(cur, TEST_SQL)
for row in cur.fetchall():
    print(row)

cur.close()
conn.close()
print('=== Database build completed successfully ===')