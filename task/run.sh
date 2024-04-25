#! /bin/bash

set -e

TODAY=$(date +%Y-%m-%d)
echo "Running collector for $COLLECTION_NAME on $TODAY"

if [ -z "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "assign value to COLLECTION_DATASET_BUCKET_NAME to save to a bucket" 
fi

echo Install dependencies
make init

echo Run the collector
make collect

if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo Saving logs and resources to $COLLECTION_DATASET_BUCKET_NAME
    make save-resources
    make save-logs
else
    echo "no"
fi

echo Build the collection database
make collection

if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo Push collection database to $COLLECTION_DATASET_BUCKET_NAME
    make save-collection
fi

echo Transform collected files
make transformed -j 2

if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo Save transformed files to $COLLECTION_DATASET_BUCKET_NAME
    make save-transformed
fi

echo Build datasets from the transformed files
make dataset

if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo Save datasets and expecations to $COLLECTION_DATASET_BUCKET_NAME
    make save-dataset
    make save-expectations
fi
   
# TODO: send notifications of errors

# if [ -d /data ]; then
#   echo Copying to /data/$COLLECTION_NAME
#   mkdir -p /data/$COLLECTION_NAME
#   for dir in collection expectations issue pipeline dataset flattened specification transformed; do
#     cp -R /collector/$dir /data/$COLLECTION_NAME
#   done
# fi
