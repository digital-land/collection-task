#!/bin/bash
# Assemble script - downloads transformed resources and builds dataset SQLite file

set -e

TODAY=$(date +%Y-%m-%d)
echo "Running assemble for $COLLECTION_NAME dataset $DATASET_NAME on $TODAY"

# Check required environment variables
if [ -z "$COLLECTION_NAME" ]; then
    echo "Error: COLLECTION_NAME environment variable must be set"
    exit 1
fi

if [ -z "$DATASET_NAME" ]; then
    echo "Error: DATASET_NAME environment variable must be set"
    exit 1
fi

# Either COLLECTION_DATASET_BUCKET_NAME or DATASTORE_URL must be set
if [ -z "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    DATASTORE_URL=${DATASTORE_URL:-"https://files.planning.data.gov.uk/"}
    echo " no COLLECTION_DATASET_BUCKET_NAME set so using $DATASTORE_URL"
fi

if [ -z "$COLLECTION_DIR" ]; then
    COLLECTION_DIR="collection/"
    echo "COLLECTION_DIR not set, using default: $COLLECTION_DIR"
fi

# Optional environment variables with defaults (matching makefile conventions)
DOWNLOAD_THREADS=${DOWNLOAD_THREADS:-4}
SPECIFICATION_DIR=${SPECIFICATION_DIR:-"specification/"}
PIPELINE_DIR=${PIPELINE_DIR:-"pipeline/"}
CACHE_DIR=${CACHE_DIR:-"var/cache/"}
TRANSFORMED_DIR=${TRANSFORMED_DIR:-"transformed/"}
ISSUE_DIR=${ISSUE_DIR:-"issue/"}
COLUMN_FIELD_DIR=${COLUMN_FIELD_DIR:-"var/column-field/"}
DATASET_RESOURCE_DIR=${DATASET_RESOURCE_DIR:-"var/dataset-resource/"}
CONVERTED_RESOURCE_DIR=${CONVERTED_RESOURCE_DIR:-"var/converted-resource/"}
DATASET_DIR=${DATASET_DIR:-"dataset/"}
FLATTENED_DIR=${FLATTENED_DIR:-"flattened/"}
OUTPUT_LOG_DIR=${OUTPUT_LOG_DIR:-"log/"}

# Step 1: Update makerules
echo "Step 1: Updating makerules..."
make makerules

# Step 2: Initialize dependencies and download config files
echo "Step 2: Initializing dependencies and configuration..."
make init

# Step 3: Build the collection database
echo "Step 3: Building collection database..."
make collection

# Step 4: Download transformed resources for the specified dataset from S3 or HTTP(S)
if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "Step 4: Downloading transformed resources for dataset $DATASET_NAME from S3..."
    SOURCE_TYPE="S3 bucket: $COLLECTION_DATASET_BUCKET_NAME"
else
    echo "Step 4: Downloading transformed resources for dataset $DATASET_NAME from HTTP(S)..."
    SOURCE_TYPE="Base URL: $DATASTORE_URL"
fi

# Build the download command with dataset filter
DOWNLOAD_CMD="python bin/download_transformed.py --collection-dir $COLLECTION_DIR --collection-name $COLLECTION_NAME --dataset $DATASET_NAME"

if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    DOWNLOAD_CMD="$DOWNLOAD_CMD --bucket $COLLECTION_DATASET_BUCKET_NAME"
else
    DOWNLOAD_CMD="$DOWNLOAD_CMD --base-url $DATASTORE_URL"
fi

if [ -n "$DOWNLOAD_THREADS" ]; then
    DOWNLOAD_CMD="$DOWNLOAD_CMD --max-threads $DOWNLOAD_THREADS"
fi

# Add verbose flag if needed
if [ -n "$VERBOSE" ]; then
    DOWNLOAD_CMD="$DOWNLOAD_CMD --verbose"
fi

echo "Source: $SOURCE_TYPE"
echo "Command: $DOWNLOAD_CMD"
eval $DOWNLOAD_CMD

echo "Transformed resources for $DATASET_NAME downloaded successfully"

# Step 5: Remove original resource files to save disk space
if [ -d "${COLLECTION_DIR}resource" ]; then
    echo "Step 5: Removing original resource files to save disk space..."
    rm -rf "${COLLECTION_DIR}resource"
    echo "Disk space after removing resources:"
    df -h / | tail -1 | awk '{print "Available: " $4 " / Total: " $2}'
fi

# Step 6: Build dataset from transformed files
echo "Step 6: Building dataset $DATASET_NAME from transformed files..."

# Set up paths
INPUT_DIR="${TRANSFORMED_DIR}${DATASET_NAME}"
SQLITE_FILE="${DATASET_DIR}${DATASET_NAME}.sqlite3"
CSV_FILE="${DATASET_DIR}${DATASET_NAME}.csv"
ORGANISATION_PATH="${CACHE_DIR}organisation.csv"
RESOURCE_PATH="${COLLECTION_DIR}resource.csv"

# Check if transformed directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo "Error: Transformed directory not found: $INPUT_DIR"
    echo "Make sure you've downloaded transformed resources for dataset: $DATASET_NAME"
    exit 1
fi

# Check if there are any parquet files
PARQUET_COUNT=$(find "$INPUT_DIR" -name "*.parquet" -type f | wc -l)
if [ "$PARQUET_COUNT" -eq 0 ]; then
    echo "Error: No parquet files found in: $INPUT_DIR"
    echo "Make sure transformed resources include .parquet files"
    exit 1
fi

echo "Found $PARQUET_COUNT transformed parquet files"

# Step 6.1: Create dataset package (dataset-create)
echo "[1/5] Creating dataset package..."
mkdir -p "$(dirname "$SQLITE_FILE")"
digital-land --pipeline-dir "$PIPELINE_DIR" --dataset "$DATASET_NAME" --specification-dir specification/ dataset-create "$INPUT_DIR" \
    --output-path "$SQLITE_FILE" \
    --organisation-path "$ORGANISATION_PATH" \
    --issue-dir "$ISSUE_DIR" \
    --column-field-dir "$COLUMN_FIELD_DIR" \
    --dataset-resource-dir "$DATASET_RESOURCE_DIR" \
    --cache-dir "$CACHE_DIR" \
    --resource-path "$RESOURCE_PATH"

echo "Dataset package created: $SQLITE_FILE"

# Step 6.2: Run datasette inspect
echo "[2/5] Creating datasette metadata..."
JSON_FILE="${SQLITE_FILE}.json"
datasette inspect "$SQLITE_FILE" --inspect-file="$JSON_FILE"
echo "Datasette metadata created: $JSON_FILE"

# Step 6.3: Dump dataset to CSV (dataset-entries)
echo "[3/5] Dumping dataset to CSV..."
digital-land --dataset "$DATASET_NAME" --pipeline-dir "$PIPELINE_DIR" --specification-dir specification/ \
    dataset-entries \
    "$SQLITE_FILE" \
    "$CSV_FILE"
echo "Dataset CSV created: $CSV_FILE"

echo "Dataset $DATASET_NAME built successfully"

# Step 6.4: Dump dataset to flattened files (dataset-entries-flattened)
echo "[4/5] Dumping dataset to flattened files..."
mkdir -p "$FLATTENED_DIR"
digital-land --dataset "$DATASET_NAME" --pipeline-dir "$PIPELINE_DIR" --specification-dir specification/ \
    dataset-entries-flattened \
    "$CSV_FILE" \
    "$FLATTENED_DIR"
echo "Dataset flattened files created"

echo "[5/5] Run dataset expectations..."
digital-land expectations-dataset-checkpoint \
    --dataset "$DATASET_NAME" \
    --file-path "$SQLITE_FILE" \
    --log-dir "$OUTPUT_LOG_DIR" \
    --configuration-path "$CACHE_DIR"config.sqlite3 \
    --organisation-path "$CACHE_DIR"organisation.csv \
    --specification-dir "$SPECIFICATION_DIR"

echo "Dataset $DATASET_NAME built successfully"


echo "Disk space after assembling dataset sqlite:"
df -h / | tail -1 | awk '{print "Available: " $4 " / Total: " $2}'

# Step 7: Save outputs to S3 (only if using S3 bucket)
if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "Step 7: Saving outputs to S3 bucket: $COLLECTION_DATASET_BUCKET_NAME"

    echo "Saving datasets..."
    make save-dataset

    echo "Saving expectations..."
    make save-expectations

    echo "All outputs saved to S3 successfully"
else
    echo "Step 7: Skipping S3 upload (no bucket configured)"
    echo "Outputs remain in local directories:"
    echo "  - Datasets: $DATASET_DIR"
    echo "  - Flattened: $FLATTENED_DIR"
fi

echo "Assemble complete!"
