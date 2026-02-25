#!/bin/bash
# Collection script - runs the collection process to fetch resources from endpoints

set -e

TODAY=$(date +%Y-%m-%d)
echo "Running collection for $COLLECTION_NAME on $TODAY"

# Check required environment variables
if [ -z "$COLLECTION_DIR" ]; then
    COLLECTION_DIR="collection/"
    echo "COLLECTION_DIR not set, using default: $COLLECTION_DIR"
fi

# Optional environment variables with defaults (matching makefile conventions)
COLLECTION_DATASET_BUCKET_NAME=${COLLECTION_DATASET_BUCKET_NAME:-""}
DATASTORE_URL=${DATASTORE_URL:-"https://files.planning.data.gov.uk/"}
REGENERATE_LOG_OVERRIDE=${REGENERATE_LOG_OVERRIDE:-""}
INCREMENTAL_LOADING_OVERRIDE=${INCREMENTAL_LOADING_OVERRIDE:-"false"}
PIPELINE_DIR=${PIPELINE_DIR:-"pipeline/"}
DATASET_RESOURCE_DIR=${DATASET_RESOURCE_DIR:-"var/dataset-resource/"}
REPROCESS=${REPROCESS:-""}

# Set REFILL_TODAYS_LOGS based on REGENERATE_LOG_OVERRIDE
if [ "$REGENERATE_LOG_OVERRIDE" = "True" ]; then
    REFILL_TODAYS_LOGS="false"
else
    REFILL_TODAYS_LOGS="true"
fi

# Step 1: Update makerules
echo "Step 1: Updating makerules..."
make makerules

# Step 2: Initialize dependencies and download config files
echo "Step 2: Initializing dependencies and configuration..."
make init

# Step 4: Run the collector to fetch resources from endpoints
echo "Step 3: Running collector..."
make collect

# Step 4a: Show disk space after collection
if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "Step 4:Saving logs and resources to $COLLECTION_DATASET_BUCKET_NAME"
    make save-resources
    make save-logs
else
    echo "STEP 4: Skipped saving logs and resources - no COLLECTION_DATASET_BUCKET_NAME defined"
fi

# now create the colection database and push
echo "STEP 5: Build the collection database"
make collection

echo "STEP 6: Create state"
DATASET_RESOURCE_CMD="python bin/download_dataset_resource.py --collection-dir $COLLECTION_DIR --collection-name $COLLECTION_NAME --dataset-resource-dir $DATASET_RESOURCE_DIR"

if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    DATASET_RESOURCE_CMD="$DATASET_RESOURCE_CMD --bucket $COLLECTION_DATASET_BUCKET_NAME"
elif [ -n "$DATASTORE_URL" ]; then
    DATASET_RESOURCE_CMD="$DATASET_RESOURCE_CMD --base-url $DATASTORE_URL"
fi

if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ] || [ -n "$DATASTORE_URL" ]; then
    if [ -z "$REPROCESS" ]; then
        echo "Downloading dataset resource logs for skip-aware transform counts..."
        eval $DATASET_RESOURCE_CMD
    else
        echo "Skipping dataset resource log download - REPROCESS is set, logs will be written fresh"
    fi

    digital-land save-state \
        --specification-dir=specification \
        --collection-dir="$COLLECTION_DIR" \
        --pipeline-dir="$PIPELINE_DIR" \
        --resource-dir="${COLLECTION_DIR}resource/" \
        --incremental-loading-override="$INCREMENTAL_LOADING_OVERRIDE" \
        --dataset-resource-dir="$DATASET_RESOURCE_DIR" \
        --output-path=state.json
else
    make state.json
fi


if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "STEP 7: Push collection database and state to $ENVIRONMENT S3"
    make save-collection
    make save-state
else
    echo "STEP 7: Skipped pushing collection database and state - no COLLECTION_DATASET_BUCKET_NAME defined"
fi

