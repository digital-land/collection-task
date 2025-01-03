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

if [ "$REGENERATE_LOG_OVERRIDE" = "True" ]; then
  echo Regenerate log enabled so downloading all log files
    if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
        make load-logs
        echo "Contents of collection/log"
        ls collection/log
        echo "File exists before deletion: $(ls collection/log.csv 2>/dev/null || echo 'No')"
        rm -f collection/log.csv collection/resource.csv
        echo "File exists after deletion: $(ls collection/log.csv 2>/dev/null || echo 'No')"
    else
        echo "Regenerate log is disabled as no COLLECTION_DATASET_BUCKET_NAME defined to get previous log files"
    fi
else
  echo Regenerate log disabled as override flag is set to False
fi

echo Run the collector
make collect

if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "Saving logs and resources to $COLLECTION_DATASET_BUCKET_NAME"
    make save-resources
    make save-logs
else
    echo "No COLLECTION_DATASET_BUCKET_NAME defined so collection files not pushed to s3"
fi

echo Build the collection database
make collection

if [ "$INCREMENTAL_LOADING_OVERRIDE" = "True" ]; then
    echo Incremental loading disabled as override flag is set.
else
    if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
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
            else
                echo "Incremental loading disabled as no state.json found."
        fi
        # Generate a new state file
        rm -f state.json
        make state.json
    else
        echo "No COLLECTION_DATASET_BUCKET_NAME defined to get previous state.json"
    fi
fi

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
    make save-state
else
    echo "No COLLECTION_DATASET_BUCKET_NAME defined so dataset and expectation files not pushed to s3"
fi

# TODO: send notifications of errors

# if [ -d /data ]; then
#   echo Copying to /data/$COLLECTION_NAME
#   mkdir -p /data/$COLLECTION_NAME
#   for dir in collection expectations issue pipeline dataset flattened specification transformed; do
#     cp -R /collector/$dir /data/$COLLECTION_NAME
#   done
# fi
