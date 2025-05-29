#!/usr/bin/env bash
set -euo pipefail

# Configuration
PG_HOST="hadoop-04.uni.innopolis.ru"
PG_DB="team6_projectdb"
PG_USER="team6"
PG_PASS=$(head -n 1 secrets/.psql.pass)
HDFS_WAREHOUSE="/user/team6/project/warehouse"
OUTPUT_DIR="output/schemas"

# Clean and prepare HDFS warehouse
echo "=== Processing HDFS warehouse ($HDFS_WAREHOUSE) ==="
if hadoop fs -test -d "$HDFS_WAREHOUSE"; then
  echo "HDFS warehouse exists. Cleaning..."
  hadoop fs -rm -r -f "$HDFS_WAREHOUSE"
else
  echo "HDFS warehouse does not exist. Will be created automatically"
fi

# Clean and prepare local output
echo "=== Processing local output directory ($OUTPUT_DIR) ==="
if [ -d "$OUTPUT_DIR" ]; then
  echo "Local output directory exists. Cleaning..."
  rm -rf "$OUTPUT_DIR"
fi
mkdir -p "$OUTPUT_DIR"

# Import function
import_table() {
    local table=$1
    echo "=== Importing $table ==="

    sqoop import \
        --connect "jdbc:postgresql://${PG_HOST}/${PG_DB}" \
        --username "$PG_USER" \
        --password "$PG_PASS" \
        --table "$table" \
        --compress \
        --compression-codec snappy \
        --as-avrodatafile \
        --warehouse-dir "$HDFS_WAREHOUSE" \
        --num-mappers 1 \
        --outdir "$OUTPUT_DIR"
}

# Import tables
import_table "dim_location"
import_table "dim_base"
import_table "dim_calendar"
import_table "fact_fhv_trips"

# Checking
echo "=== Import verification ==="
hadoop fs -ls -R "$HDFS_WAREHOUSE"
echo "=== AVRO schemas saved to $OUTPUT_DIR ==="
ls -lh "$OUTPUT_DIR"

echo "=== Sqoop import completed successfully ==="