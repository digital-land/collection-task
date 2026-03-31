#!/bin/bash
# Transform script - initializes environment, downloads resources and processes them

set -e

TODAY=$(date +%Y-%m-%d)
echo "Running transform for $COLLECTION_NAME on $TODAY"

# Check required environment variables
if [ -z "$COLLECTION_DIR" ]; then
    COLLECTION_DIR="collection/"
    echo "COLLECTION_DIR not set, using default: $COLLECTION_DIR"
fi

# Optional environment variables with defaults (matching makefile conventions)
COLLECTION_DATASET_BUCKET_NAME=${COLLECTION_DATASET_BUCKET_NAME:-""}
DATASTORE_URL=${DATASTORE_URL:-"https://files.planning.data.gov.uk/"}
DATASET=${DATASET:-""}
TRANSFORMATION_OFFSET=${TRANSFORMATION_OFFSET:-""}
TRANSFORMATION_LIMIT=${TRANSFORMATION_LIMIT:-""}
DOWNLOAD_THREADS=${DOWNLOAD_THREADS:-4}
TRANSFORMED_JOBS=${TRANSFORMED_JOBS:-8}
PIPELINE_DIR=${PIPELINE_DIR:-"pipeline/"}
CACHE_DIR=${CACHE_DIR:-"var/cache/"}
TRANSFORMED_DIR=${TRANSFORMED_DIR:-"transformed/"}
DATASET_RESOURCE_DIR=${DATASET_RESOURCE_DIR:-"var/dataset-resource/"}
REPROCESS=${REPROCESS:-""}

# Step 1: Update makerules
echo "Step 1: Updating makerules..."
make makerules

# Step 2: Initialize dependencies and download config files
echo "Step 2: Initializing dependencies and configuration..."
make init

# Step 3: Build the collection database
echo "Step 3: Building collection database..."
make collection

# Step 4: Download state.json (used for stable batch ordering) and dataset resource logs
# Use local state.json if it exists, allowing manual edits
STATE_PATH=${STATE_PATH:-"state.json"}

if [ -f "$STATE_PATH" ]; then
    echo "Step 4a: Using existing state.json at $STATE_PATH"
else
    echo "Step 4a: Downloading state.json..."
    if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
        aws s3 cp "s3://${COLLECTION_DATASET_BUCKET_NAME}/${COLLECTION_NAME}-collection/state.json" "$STATE_PATH"
    elif [ -n "$DATASTORE_URL" ]; then
        base=$(echo "$DATASTORE_URL" | sed 's:/*$::')
        curl --fail -o "$STATE_PATH" "${base}/${COLLECTION_NAME}-collection/state.json"
    else
        echo "Error: Either COLLECTION_DATASET_BUCKET_NAME or DATASTORE_URL must be set"
        exit 1
    fi

    if [ ! -f "$STATE_PATH" ]; then
        echo "Error: Failed to download state.json"
        exit 1
    fi
fi

# Download dataset resource logs (used to skip already up-to-date resources within a batch)
# Skipped when REPROCESS is set - logs will be freshly written by the transform step
if [ -z "$REPROCESS" ]; then
    echo "Step 4b: Downloading dataset resource logs..."

    DATASET_RESOURCE_CMD="python bin/download_dataset_resource.py --collection-dir $COLLECTION_DIR --collection-name $COLLECTION_NAME --dataset-resource-dir $DATASET_RESOURCE_DIR"

    if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
        DATASET_RESOURCE_CMD="$DATASET_RESOURCE_CMD --bucket $COLLECTION_DATASET_BUCKET_NAME"
    elif [ -n "$DATASTORE_URL" ]; then
        DATASET_RESOURCE_CMD="$DATASET_RESOURCE_CMD --base-url $DATASTORE_URL"
    else
        echo "Error: Either COLLECTION_DATASET_BUCKET_NAME or DATASTORE_URL must be set"
        exit 1
    fi

    if [ -n "$DATASET" ]; then
        DATASET_RESOURCE_CMD="$DATASET_RESOURCE_CMD --dataset $DATASET"
    fi

    echo "Command: $DATASET_RESOURCE_CMD"
    eval $DATASET_RESOURCE_CMD
else
    echo "Step 4b: Skipping dataset resource log download - REPROCESS is set, logs will be written fresh"
fi

# Step 5: Download resources
echo "Step 5: Downloading resources..."

# Build the download command
DOWNLOAD_CMD="python bin/download_resources.py --collection-dir $COLLECTION_DIR --state-path $STATE_PATH"

# Add bucket or base URL (bucket takes precedence, matching makefile convention)
if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    DOWNLOAD_CMD="$DOWNLOAD_CMD --bucket $COLLECTION_DATASET_BUCKET_NAME"
    echo "Using S3 bucket: $COLLECTION_DATASET_BUCKET_NAME"
elif [ -n "$DATASTORE_URL" ]; then
    DOWNLOAD_CMD="$DOWNLOAD_CMD --base-url $DATASTORE_URL"
    echo "Using base URL: $DATASTORE_URL"
else
    echo "Error: Either COLLECTION_DATASET_BUCKET_NAME or DATASTORE_URL must be set"
    exit 1
fi

if [ -n "$DATASET" ]; then
    DOWNLOAD_CMD="$DOWNLOAD_CMD --dataset $DATASET"
fi

if [ -n "$TRANSFORMATION_OFFSET" ]; then
    DOWNLOAD_CMD="$DOWNLOAD_CMD --offset $TRANSFORMATION_OFFSET"
fi

if [ -n "$TRANSFORMATION_LIMIT" ]; then
    DOWNLOAD_CMD="$DOWNLOAD_CMD --limit $TRANSFORMATION_LIMIT"
fi

if [ -n "$DOWNLOAD_THREADS" ]; then
    DOWNLOAD_CMD="$DOWNLOAD_CMD --max-threads $DOWNLOAD_THREADS"
fi

if [ -n "$REPROCESS" ]; then
    DOWNLOAD_CMD="$DOWNLOAD_CMD --reprocess"
fi

echo "Downloading resources..."
echo "Command: $DOWNLOAD_CMD"
eval $DOWNLOAD_CMD

echo "Resources downloaded successfully"

# Step 6: Transform resources using Python multiprocessing
echo "Step 6: Transforming resources..."

TRANSFORM_CMD="python bin/transform_resources.py --collection-dir $COLLECTION_DIR --state-path $STATE_PATH"

# Add directory parameters
TRANSFORM_CMD="$TRANSFORM_CMD --pipeline-dir $PIPELINE_DIR"
TRANSFORM_CMD="$TRANSFORM_CMD --cache-dir $CACHE_DIR"
TRANSFORM_CMD="$TRANSFORM_CMD --transformed-dir $TRANSFORMED_DIR"
TRANSFORM_CMD="$TRANSFORM_CMD --dataset-resource-dir $DATASET_RESOURCE_DIR"

# Add dataset filter if specified
if [ -n "$DATASET" ]; then
    TRANSFORM_CMD="$TRANSFORM_CMD --dataset $DATASET"
fi

# Add offset and limit if specified
if [ -n "$TRANSFORMATION_OFFSET" ]; then
    TRANSFORM_CMD="$TRANSFORM_CMD --offset $TRANSFORMATION_OFFSET"
fi

if [ -n "$TRANSFORMATION_LIMIT" ]; then
    TRANSFORM_CMD="$TRANSFORM_CMD --limit $TRANSFORMATION_LIMIT"
fi

# Add max workers if specified (matching makefile TRANSFORMED_JOBS convention)
if [ -n "$TRANSFORMED_JOBS" ]; then
    TRANSFORM_CMD="$TRANSFORM_CMD --max-workers $TRANSFORMED_JOBS"
fi

# Add reprocess flag if set
if [ -n "$REPROCESS" ]; then
    TRANSFORM_CMD="$TRANSFORM_CMD --reprocess"
fi

# Add verbose flag if needed
if [ -n "$VERBOSE" ]; then
    TRANSFORM_CMD="$TRANSFORM_CMD --verbose"
fi

echo "Command: $TRANSFORM_CMD"
eval $TRANSFORM_CMD

# Step 7: Save outputs to S3 if bucket is configured
if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "Step 7: Saving outputs to S3 bucket: $COLLECTION_DATASET_BUCKET_NAME"

    # Use existing make targets for S3 syncing
    echo "Saving transformed files and related outputs..."
    make save-transformed

    echo "All outputs saved to S3 successfully"
else
    echo "Step 7: Skipping S3 sync - no COLLECTION_DATASET_BUCKET_NAME defined"
fi

echo "Transform pipeline complete!"
