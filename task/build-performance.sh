#!/bin/bash

set -e

ENTRY_DATE=$(date +%Y-%m-%d)
echo "Performance build for entry date: $ENTRY_DATE"

# run the script to generate the provision-quality dataset
echo "Running generate_provision_quality.py for entry date: $ENTRY_DATE"
python3 src/generate_provision_quality.py

OUTPUT_FILE="task/performance/provision-quality/entry-date=${ENTRY_DATE}/provision-quality.parquet"

if [[ -n "$COLLECTION_DATASET_BUCKET_NAME" ]]; then
  if [[ -f "$OUTPUT_FILE" ]]; then
    echo "Uploading $OUTPUT_FILE to s3"
    aws s3 cp "$OUTPUT_FILE" "s3://${COLLECTION_DATASET_BUCKET_NAME}/performance/provision-quality/entry-date=${ENTRY_DATE}/provision-quality.parquet"
  else
    echo "ERROR: Output file $OUTPUT_FILE not found. Skipping S3 upload."
    exit 1
  fi
else
  echo "COLLECTION_DATASET_BUCKET_NAME not set. Skipping S3 upload."
fi

echo "Performance build complete"