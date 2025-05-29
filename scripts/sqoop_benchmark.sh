#!/usr/bin/env bash
set -euo pipefail

# File: scripts/sqoop_benchmark.sh
# Usage: bash scripts/sqoop_benchmark.sh

# Configuration
PG_HOST="hadoop-04.uni.innopolis.ru"
PG_DB="team6_projectdb"
PG_USER="team6"
PG_PASS=$(head -n 1 secrets/.psql.pass)
HDFS_BASE="/user/team6/project/benchmark"
OUTPUT_DIR="output/benchmark"
REPORT_FILE="${OUTPUT_DIR}/compression_report.csv"
TEST_TABLE="fact_fhv_trips"

# Compression codecs to test
COMPRESSION_CODECS=("snappy" "deflate")

# Initialize benchmark environment
mkdir -p "$OUTPUT_DIR"
echo "codec,import_time_sec,data_size_mb,query_time_sec" > "$REPORT_FILE"

# Clean previous benchmark data
echo "=== Cleaning previous benchmark data ==="
hadoop fs -rm -r -f "${HDFS_BASE}_*" || true
rm -rf "${OUTPUT_DIR}/*" || true

# Benchmark function
run_benchmark() {
    local codec=$1
    local hdfs_path="${HDFS_BASE}_${codec}"
    local log_prefix="${OUTPUT_DIR}/${codec}"

    echo -e "\n=== Starting benchmark for $codec ==="

    # Time the import
    import_start=$(date +%s)

    sqoop import \
        --connect "jdbc:postgresql://${PG_HOST}/${PG_DB}" \
        --username "$PG_USER" \
        --password "$PG_PASS" \
        --table "$TEST_TABLE" \
        --compress \
        --compression-codec "$codec" \
        --as-avrodatafile \
        --warehouse-dir "$hdfs_path" \
        --num-mappers 1 \
        --outdir "$log_prefix" 2>&1 | tee "${log_prefix}_import.log"

    import_end=$(date +%s)
    import_time=$((import_end - import_start))

    # Calculate data size in bytes and convert to MB
    hdfs_size_bytes=$(hadoop fs -du -s "$hdfs_path" | awk '{print $1}')
    size_mb=$(echo "scale=2; $hdfs_size_bytes / (1024 * 1024)" | bc)

    # Time sample query
    query_start=$(date +%s)
    hadoop jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar \
        grep "$hdfs_path/$TEST_TABLE" \
        "${hdfs_path}_results" \
        ".*" 2>&1 | tee "${log_prefix}_query.log"
    query_end=$(date +%s)
    query_time=$((query_end - query_start))

    # Save results
    echo "${codec},${import_time},${size_mb},${query_time}" >> "$REPORT_FILE"
}

# Run benchmarks
for codec in "${COMPRESSION_CODECS[@]}"; do
    run_benchmark "$codec"
done

# Generate final report
echo -e "\n=== Benchmark Results ==="
column -t -s',' "$REPORT_FILE"

echo -e "\n=== Detailed logs saved to $OUTPUT_DIR ==="
echo "=== CSV report: $REPORT_FILE ==="