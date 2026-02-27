#!/bin/bash
# Load script - builds a dataset package directly from pre-built parquet tables in S3
# Requires S3 access via PARQUET_DATASETS_BUCKET

set -e

TODAY=$(date +%Y-%m-%d)
echo "Running load for $COLLECTION_NAME dataset $DATASET_NAME on $TODAY"

# Check required environment variables
if [ -z "$COLLECTION_NAME" ]; then
    echo "Error: COLLECTION_NAME environment variable must be set"
    exit 1
fi

if [ -z "$DATASET_NAME" ]; then
    echo "Error: DATASET_NAME environment variable must be set"
    exit 1
fi

if [ -z "$PARQUET_DATASETS_BUCKET" ]; then
    echo "Error: PARQUET_DATASETS_BUCKET environment variable must be set"
    exit 1
fi

if [ -z "$COLLECTION_DATA_BUCKET" ]; then
    echo "Error: COLLECTION_DATA_BUCKET environment variable must be set"
    exit 1
fi

# Optional environment variables with defaults
SPECIFICATION_DIR=${SPECIFICATION_DIR:-"specification/"}
PIPELINE_DIR=${PIPELINE_DIR:-"pipeline/"}
CACHE_DIR=${CACHE_DIR:-"var/cache/"}
ISSUE_DIR=${ISSUE_DIR:-"issue/"}
COLUMN_FIELD_DIR=${COLUMN_FIELD_DIR:-"var/column-field/"}
DATASET_RESOURCE_DIR=${DATASET_RESOURCE_DIR:-"var/dataset-resource/"}
DATASET_DIR=${DATASET_DIR:-"dataset/"}
FLATTENED_DIR=${FLATTENED_DIR:-"flattened/"}
OUTPUT_LOG_DIR=${OUTPUT_LOG_DIR:-"log/"}

# Optional: bucket to save outputs to
COLLECTION_DATASET_BUCKET_NAME=${COLLECTION_DATASET_BUCKET_NAME:-""}

SQLITE_FILE="${DATASET_DIR}${DATASET_NAME}.sqlite3"
CSV_FILE="${DATASET_DIR}${DATASET_NAME}.csv"

# Step 1: Update makerules
echo "Step 1: Updating makerules..."
make makerules

# Step 2: Initialize dependencies and download config files
echo "Step 2: Initializing dependencies and configuration..."
make init

# Step 3: Build dataset SQLite from parquet tables in S3
echo "Step 3: Building dataset package for $DATASET_NAME from $PARQUET_DATASETS_PATH..."
mkdir -p "$DATASET_DIR"

BUILD_CMD="python bin/build_dataset_package.py \
    --dataset $DATASET_NAME \
    --parquet-datasets-path $PARQUET_DATASETS_BUCKET \
    --collection-data-path $COLLECTION_DATA_BUCKET \
    --collection $COLLECTION_NAME \
    --output-path $SQLITE_FILE \
    --specification-dir $SPECIFICATION_DIR"

if [ -n "$VERBOSE" ]; then
    BUILD_CMD="$BUILD_CMD --debug"
fi

echo "Command: $BUILD_CMD"
eval $BUILD_CMD

echo "Dataset package created: $SQLITE_FILE"

# Step 4: Run datasette inspect
echo "Step 4: Creating datasette metadata..."
JSON_FILE="${SQLITE_FILE}.json"
datasette inspect "$SQLITE_FILE" --inspect-file="$JSON_FILE"

# Step 5: Dump dataset to CSV
echo "Step 5: Dumping dataset to CSV..."
digital-land --dataset "$DATASET_NAME" --pipeline-dir "$PIPELINE_DIR" --specification-dir "$SPECIFICATION_DIR" \
    dataset-entries \
    "$SQLITE_FILE" \
    "$CSV_FILE"

# Step 6: Dump dataset to flattened files
echo "Step 6: Dumping dataset to flattened files..."
mkdir -p "$FLATTENED_DIR"
digital-land --dataset "$DATASET_NAME" --pipeline-dir "$PIPELINE_DIR" --specification-dir "$SPECIFICATION_DIR" \
    dataset-entries-flattened \
    "$CSV_FILE" \
    "$FLATTENED_DIR"

# Step 7: Run dataset expectations
echo "Step 7: Running dataset expectations..."
digital-land expectations-dataset-checkpoint \
    --dataset "$DATASET_NAME" \
    --file-path "$SQLITE_FILE" \
    --log-dir "$OUTPUT_LOG_DIR" \
    --configuration-path "${CACHE_DIR}config.sqlite3" \
    --organisation-path "${CACHE_DIR}organisation.csv" \
    --specification-dir "$SPECIFICATION_DIR"

echo "Dataset $DATASET_NAME built successfully"

# Step 8: Save outputs to S3 if bucket is configured
if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "Step 8: Saving outputs to S3 bucket: $COLLECTION_DATASET_BUCKET_NAME"
    make save-dataset
    make save-expectations
    make save-performance
    echo "All outputs saved to S3 successfully"
else
    echo "Step 8: Skipping S3 upload (no COLLECTION_DATASET_BUCKET_NAME configured)"
fi

echo "Load complete!"
