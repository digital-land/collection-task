#! /bin/bash

set -e

TODAY=$(date +%Y-%m-%d)
echo "Running collector for $COLLECTION_NAME on $TODAY"

if [ -z "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "assign value to COLLECTION_DATASET_BUCKET_NAME to save to a bucket" 
fi

if [ -z "$TRANSFORMED_JOBS" ]; then
    TRANSFORMED_JOBS=8
fi

if [ -z "$DATASET_JOBS" ]; then
    DATASET_JOBS=8
fi

echo Update makerules
make makerules

echo Install dependencies
make init

echo Run the collector
make collect

if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "Saving logs and resources to $COLLECTION_DATASET_BUCKET_NAME"
    make save-resources
    make save-logs
else
    echo "No COLECTION_DATASET_BUCKET_NAME defined so collection files not pushed to s3"
fi

echo "Incremental loading override: $INCREMENTAL_LOADING_OVERRIDE"

if [ "$INCREMENTAL_LOADING_OVERRIDE" = "True" ]; then
    echo Incremental loading disabled as override flag is set.
else
    make load-state
    if [ -f "state.json" ]; then
        digital-land check-state \
            --specification-dir=specification \
            --collection-dir=collection \
            --pipeline-dir=pipeline \
            --state-path=state.json \
        && {
            echo "Stopping processing as state hasn't changed."
            exit 0
        }
    fi
fi

echo Build the collection database
make collection

if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo Push collection database to $ENVIRONMENT S3
    make save-collection
fi

echo Transform collected files
gmake transformed -j $TRANSFORMED_JOBS

if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo Save transformed files to $ENVIRONMENT S3
    make save-transformed
else
    echo "No COLECTION_DATASET_BUCKET_NAME defined so transformed files not pushed to s3"
fi

echo Build datasets from the transformed files
gmake dataset -j $DATASET_JOBS

if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo Save datasets and expecations to $ENVIRONMENT S3
    make save-dataset
    make save-expectations
    make save-performance
else
    echo "No COLECTION_DATASET_BUCKET_NAME defined so dataset and expectation files not pushed to s3"
fi
   
make save-state

# TODO: send notifications of errors

# if [ -d /data ]; then
#   echo Copying to /data/$COLLECTION_NAME
#   mkdir -p /data/$COLLECTION_NAME
#   for dir in collection expectations issue pipeline dataset flattened specification transformed; do
#     cp -R /collector/$dir /data/$COLLECTION_NAME
#   done
# fi
