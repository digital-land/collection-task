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
TRANSFORMATION_OFFSET=${TRANSFORMATION_OFFSET:-""}
TRANSFORMATION_LIMIT=${TRANSFORMATION_LIMIT:-""}
DOWNLOAD_THREADS=${DOWNLOAD_THREADS:-4}
TRANSFORMED_JOBS=${TRANSFORMED_JOBS:-8}
PIPELINE_DIR=${PIPELINE_DIR:-"pipeline/"}
CACHE_DIR=${CACHE_DIR:-"var/cache/"}
TRANSFORMED_DIR=${TRANSFORMED_DIR:-"transformed/"}

# Step 1: Update makerules
echo "Step 1: Updating makerules..."
make makerules

# Step 2: Initialize dependencies and download config files
echo "Step 2: Initializing dependencies and configuration..."
make init

# Step 3: Build the collection database
echo "Step 3: Building collection database..."
make collection

# Step 4: Download resources
echo "Step 4: Downloading resources..."

# Build the download command
DOWNLOAD_CMD="python bin/download_resources.py --collection-dir $COLLECTION_DIR"

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

if [ -n "$TRANSFORMATION_OFFSET" ]; then
    DOWNLOAD_CMD="$DOWNLOAD_CMD --offset $TRANSFORMATION_OFFSET"
fi

if [ -n "$TRANSFORMATION_LIMIT" ]; then
    DOWNLOAD_CMD="$DOWNLOAD_CMD --limit $TRANSFORMATION_LIMIT"
fi

if [ -n "$DOWNLOAD_THREADS" ]; then
    DOWNLOAD_CMD="$DOWNLOAD_CMD --max-threads $DOWNLOAD_THREADS"
fi

# Add verbose flag if needed
if [ -n "$VERBOSE" ]; then
    DOWNLOAD_CMD="$DOWNLOAD_CMD --verbose"
fi

echo "Downloading resources..."
echo "Command: $DOWNLOAD_CMD"
eval $DOWNLOAD_CMD

echo "Resources downloaded successfully"

# Step 5: Transform resources using Python multiprocessing
echo "Step 5: Transforming resources..."

TRANSFORM_CMD="python bin/transform_resources.py --collection-dir $COLLECTION_DIR"

# Add directory parameters
TRANSFORM_CMD="$TRANSFORM_CMD --pipeline-dir $PIPELINE_DIR"
TRANSFORM_CMD="$TRANSFORM_CMD --cache-dir $CACHE_DIR"
TRANSFORM_CMD="$TRANSFORM_CMD --transformed-dir $TRANSFORMED_DIR"

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

# Add verbose flag if needed
if [ -n "$VERBOSE" ]; then
    TRANSFORM_CMD="$TRANSFORM_CMD --verbose"
fi

echo "Command: $TRANSFORM_CMD"
eval $TRANSFORM_CMD

echo "Transform complete!"

# Step 6: Save outputs to S3 if bucket is configured
if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "Step 6: Saving outputs to S3 bucket: $COLLECTION_DATASET_BUCKET_NAME"

    # Use existing make targets for S3 syncing
    echo "Saving transformed files and related outputs..."
    make save-transformed

    echo "All outputs saved to S3 successfully"
else
    echo "Step 6: Skipping S3 sync - no COLLECTION_DATASET_BUCKET_NAME defined"
fi

echo "Transform pipeline complete!"
