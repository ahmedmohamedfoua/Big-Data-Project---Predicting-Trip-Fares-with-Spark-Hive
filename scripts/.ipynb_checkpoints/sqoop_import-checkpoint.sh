#!/usr/bin/env bash
set -euo pipefail

# Final working Sqoop import script for version 1.4.7
# Verified syntax for your cluster environment

# Configuration
PG_HOST="hadoop-04.uni.innopolis.ru"
PG_DB="team6_projectdb"
PG_USER="team6"
PG_PASS=$(head -n 1 secrets/.psql.pass)
HDFS_WAREHOUSE="/user/team6/project/warehouse"
OUTPUT_DIR="output/schemas"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Clean HDFS warehouse
echo "=== Cleaning HDFS warehouse ==="
hadoop fs -rm -r -f "$HDFS_WAREHOUSE" || true

# Generic import function
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
        --direct \
        --num-mappers 1 \
        --outdir "$OUTPUT_DIR"
}

# Import all tables
import_table "dim_location"
import_table "dim_base"
import_table "dim_calendar"
import_table "fact_fhv_trips"

# Verify the import
echo "=== Import verification ==="
hadoop fs -ls -R "$HDFS_WAREHOUSE"
echo "=== AVRO schemas saved to $OUTPUT_DIR ==="
ls -lh "$OUTPUT_DIR"

echo "=== Sqoop import completed successfully ==="