#!/bin/bash
# Retire script - lists a dataset's user-facing files in the collection data bucket and,
# unless DRY_RUN is "false", removes them. Run by the retire-dataset DAG.

set -e

echo "Retiring $DATASET_NAME files from the $COLLECTION_NAME collection (DRY_RUN=${DRY_RUN:-unset})"

# Check required environment variables
if [ -z "$COLLECTION_NAME" ]; then
    echo "Error: COLLECTION_NAME environment variable must be set"
    exit 1
fi

if [ -z "$DATASET_NAME" ]; then
    echo "Error: DATASET_NAME environment variable must be set"
    exit 1
fi

# Unlike assemble.sh there is no CDN fallback: files can only be removed from the bucket itself
if [ -z "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "Error: COLLECTION_DATASET_BUCKET_NAME environment variable must be set"
    exit 1
fi

python bin/retire_dataset_files.py \
    --bucket "$COLLECTION_DATASET_BUCKET_NAME" \
    --dataset "$DATASET_NAME" \
    --collection "$COLLECTION_NAME" \
    --dry-run "${DRY_RUN:-true}"
