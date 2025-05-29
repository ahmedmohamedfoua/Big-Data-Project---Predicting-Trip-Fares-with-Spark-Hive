#!/bin/bash
# scripts/data_storage.sh

# Set working directory to project root
cd "$(dirname "$0")/.." || exit

# Activate Python virtual environment (if using one)
source ~/miniconda3/etc/profile.d/conda.sh
conda activate py38

# Run the database build script
echo "=== Building Project Database ==="
python scripts/build_projectdb.py

# Check if the script succeeded
if [ $? -eq 0 ]; then
    echo "Database build completed successfully"
else
    echo "Database build failed" >&2
    exit 1
fi