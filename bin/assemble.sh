#!/bin/bash
# Assemble script - downloads transformed resources and builds dataset SQLite files

set -e

TODAY=$(date +%Y-%m-%d)
echo "Running assemble for $COLLECTION_NAME on $TODAY"

# Check required environment variables
if [ -z "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "Error: COLLECTION_DATASET_BUCKET_NAME must be set"
    exit 1
fi

if [ -z "$COLLECTION_DIR" ]; then
    COLLECTION_DIR="collection/"
    echo "COLLECTION_DIR not set, using default: $COLLECTION_DIR"
fi

# Optional environment variables with defaults (matching makefile conventions)
DATASET_JOBS=${DATASET_JOBS:-8}
DOWNLOAD_THREADS=${DOWNLOAD_THREADS:-4}
PIPELINE_DIR=${PIPELINE_DIR:-"pipeline/"}
CACHE_DIR=${CACHE_DIR:-"var/cache/"}
TRANSFORMED_DIR=${TRANSFORMED_DIR:-"transformed/"}
ISSUE_DIR=${ISSUE_DIR:-"issue/"}
COLUMN_FIELD_DIR=${COLUMN_FIELD_DIR:-"var/column-field/"}
DATASET_RESOURCE_DIR=${DATASET_RESOURCE_DIR:-"var/dataset-resource/"}
CONVERTED_RESOURCE_DIR=${CONVERTED_RESOURCE_DIR:-"var/converted-resource/"}
DATASET_DIR=${DATASET_DIR:-"dataset/"}
FLATTENED_DIR=${FLATTENED_DIR:-"flattened/"}

# Step 1: Update makerules
echo "Step 1: Updating makerules..."
make makerules

# Step 2: Initialize dependencies and download config files
echo "Step 2: Initializing dependencies and configuration..."
make init

# Step 3: Build the collection database
echo "Step 3: Building collection database..."
make collection

# Step 4: Download ALL transformed resources from S3
# Note: We need all transformed files to build complete datasets, no offset/limit
echo "Step 4: Downloading all transformed resources from S3..."

# Build the download command
DOWNLOAD_CMD="python bin/download_transformed.py --collection-dir $COLLECTION_DIR --bucket $COLLECTION_DATASET_BUCKET_NAME"

if [ -n "$DOWNLOAD_THREADS" ]; then
    DOWNLOAD_CMD="$DOWNLOAD_CMD --max-threads $DOWNLOAD_THREADS"
fi

# Add verbose flag if needed
if [ -n "$VERBOSE" ]; then
    DOWNLOAD_CMD="$DOWNLOAD_CMD --verbose"
fi

echo "Command: $DOWNLOAD_CMD"
eval $DOWNLOAD_CMD

echo "Transformed resources downloaded successfully"

# Step 5: Remove original resource files to save disk space
if [ -d "${COLLECTION_DIR}resource" ]; then
    echo "Step 5: Removing original resource files to save disk space..."
    rm -rf "${COLLECTION_DIR}resource"
    echo "Disk space after removing resources:"
    df -h / | tail -1 | awk '{print "Available: " $4 " / Total: " $2}'
fi

# Step 6: Build datasets from transformed files
echo "Step 6: Building datasets from transformed files..."
gmake dataset -j $DATASET_JOBS

echo "Disk space after assembling dataset sqlite:"
df -h / | tail -1 | awk '{print "Available: " $4 " / Total: " $2}'

# Step 7: Save outputs to S3
echo "Step 7: Saving outputs to S3 bucket: $COLLECTION_DATASET_BUCKET_NAME"

echo "Saving datasets..."
make save-dataset

echo "Saving expectations..."
make save-expectations

echo "Saving performance metrics..."
make save-performance

echo "Saving state..."
make save-state

echo "All outputs saved to S3 successfully"

echo "Assemble complete!"
