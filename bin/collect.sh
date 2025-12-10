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

# Step 3: Load state if available (for incremental processing)
if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "Step 3: Loading previous state from S3..."
    make load-state || echo "No previous state found, will create new state"
else
    echo "Step 3: Skipping state loading - no COLLECTION_DATASET_BUCKET_NAME defined"
fi

# Step 4: Run the collector to fetch resources from endpoints
echo "Step 4: Running collector..."
make collect

# Step 5: Build the collection database (save log.csv and resource.csv)
echo "Step 5: Building collection database..."
make collection

# Step 6: Detect new resources (if using S3)
if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "Step 6: Detecting new resources..."
    make detect-new-resources

    NEW_RESOURCE_COUNT=$(wc -l < new_resources.txt | tr -d ' ')
    echo "Detected $NEW_RESOURCE_COUNT new resources"
else
    echo "Step 6: Skipping new resource detection - no COLLECTION_DATASET_BUCKET_NAME defined"
fi

# Step 7: Save outputs to S3 if bucket is configured
if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "Step 7: Saving collection outputs to S3 bucket: $COLLECTION_DATASET_BUCKET_NAME"

    echo "Saving resources..."
    make save-resources

    echo "Saving logs..."
    make save-logs

    echo "Saving collection database files..."
    make save-collection

    echo "Saving state..."
    make save-state

    echo "All collection outputs saved to S3 successfully"
else
    echo "Step 7: Skipping S3 sync - no COLLECTION_DATASET_BUCKET_NAME defined"
fi

echo "Collection complete!"
