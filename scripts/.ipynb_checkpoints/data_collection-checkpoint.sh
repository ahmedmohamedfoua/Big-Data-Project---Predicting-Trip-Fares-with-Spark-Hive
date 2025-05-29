#!/usr/bin/env bash
set -euo pipefail

# Data Collection Script (Parquet files)
# Downloads a ZIP from Yandex.Disk, extracts all Parquet files into the data/ directory.

# Ensure data directory exists
mkdir -p data

# Configuration
DATA_DIR="data"
PARQUET_URL="https://disk.yandex.ru/d/U34WREGpmvRMQA"
ZONE_URL="https://disk.yandex.ru/d/fE3V3Y758e7LTw"

echo "=== Starting Data Pipeline ==="

# 1. Clean previous data files
echo "Cleaning up previous data files..."
find "$DATA_DIR" -type f \( -name '*.parquet' -o -name '*.csv' -o -name '*.zip' \) -delete
find "$DATA_DIR" -type d -empty -delete
echo "Cleanup complete."

# 2. Download and prepare new data
echo "Downloading main dataset..."
mkdir -p "$DATA_DIR"
wget -qO "$DATA_DIR/data.zip" "$(yadisk-direct "$PARQUET_URL")"

echo "Unzipping data..."
unzip -q "$DATA_DIR/data.zip" -d "$DATA_DIR/"

echo "Organizing Parquet files..."
find "$DATA_DIR" -type f -name '*.parquet' -exec cp {} "$DATA_DIR/" \;

echo "Downloading taxi zone data..."
wget -O "$DATA_DIR/taxi_zone_lookup.csv" "$(yadisk-direct "$ZONE_URL")"

echo "Cleaning up temporary files..."
rm -f "$DATA_DIR/data.zip"
find "$DATA_DIR" -type d -empty -delete