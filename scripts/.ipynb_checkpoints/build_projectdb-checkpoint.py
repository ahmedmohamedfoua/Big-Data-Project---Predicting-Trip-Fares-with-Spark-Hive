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
    "driver": "org.postgresql.Driver"
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
        cur.execute(f.read())

# === STEP 1: инициализация схемы ===
print('=== Connecting to Postgres ===')
conn = psycopg2.connect(
    host=db_host, port=db_port, dbname=db_name,
    user=db_user, password=db_pass
)
conn.autocommit = True
cur = conn.cursor()

print('=== Running create_tables.sql ===')
run_sql_file(cur, CREATE_SQL)

# === STEP 2: Spark для записи размерностей + фактов ===
print('=== Starting Spark session for JDBC write ===')
spark = SparkSession.builder \
    .appName('WriteParquetToPostgres') \
    .config('spark.jars', '/shared/postgresql-42.6.1.jar') \
    .getOrCreate()

print('=== Reading Parquet files ===')
parquet_path = os.path.join(DATA_DIR, '*.parquet')
df = spark.read.parquet(f"file://{os.path.abspath(parquet_path)}")

# (Если нужно, тут .withColumnRenamed(...) для переименования,
#  но в примере имена в Parquet уже совпадают с DDL.)

# 2.1) Заполняем dim_base
print("=== Writing dim_base ===")
bases_df = (
    df.select(col("dispatching_base_num").alias("base_num"))
      .union(df.select(col("originating_base_num").alias("base_num")))
      .distinct()
      .filter(col("base_num").isNotNull())
)
bases_df.write.jdbc(
    url=jdbc_url,
    table="dim_base",
    mode="append",
    properties=jdbc_props
)

# 2.2) Заполняем dim_location
print("=== Writing dim_location ===")
locations_df = (
    df.select(col("PULocationID").alias("location_id"))
      .union(df.select(col("DOLocationID").alias("location_id")))
      .distinct()
      .filter(col("location_id").isNotNull())       # убираем NULL
)
locations_df.write.jdbc(
    url=jdbc_url,
    table="dim_location",
    mode="append",
    properties=jdbc_props
)

# 2.3) Теперь пишем фактовую таблицу
print('=== Writing fact_fhv_trips ===')
df.write.jdbc(
    url=jdbc_url,
    table='fact_fhv_trips',
    mode='append',
    properties=jdbc_props
)

spark.stop()

# === STEP 3: тестовые запросы ===
print('=== Running test queries ===')
run_sql_file(cur, TEST_SQL)
# предположим, что тестовый SQL делает один SELECT,
# тогда:
for row in cur.fetchall():
    print(row)

cur.close()
conn.close()
print('=== Database build completed successfully ===')