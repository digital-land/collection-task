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

# we need a make save-state here as we need current_state.json for make collection
# state from s3 should be named something like historical_state.json

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
                --resource-dir=collection/resource \
                --incremental-override=$INCREMENTAL_LOADING_OVERRIDE \
                --state-path=state.json \
            && { \
            echo "Incremental loading enabled. Saving log.csv and resource.csv to $COLLECTION_DATASET_BUCKET_NAME."; \
            make save-collection-log-resource; \
            echo "No state change and no new resources. Stopping processing."; \
            # Generate a new state file and upload to s3
            rm -f state.json; \
            make state.json; \
            make save-state; \
            exit 0; \
		}; \
        else \
            echo "No state.json found."; \
        fi

    # Generate a new state file
    # We will have to move this earlier in the future
    rm -f state.json
    make state.json
    else
        echo "No COLLECTION_DATASET_BUCKET_NAME defined to get previous state.json"
    fi
fi

echo Build the collection database
make collection # we will read from current_state.json here

echo Detect new resources that have been downloaded
make detect-new-resources

if [ -n "$COLLECTION_DATASET_BUCKET_NAME" ]; then
    echo "Saving logs and resources to $COLLECTION_DATASET_BUCKET_NAME"
    make save-resources
    make save-logs
else
    echo "No COLLECTION_DATASET_BUCKET_NAME defined so collection files not pushed to s3"
fi

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
                --resource-dir=collection/resource \
                --incremental-override=$INCREMENTAL_LOADING_OVERRIDE \
                --state-path=state.json \
            && { \
            echo "Incremental loading enabled. Saving log.csv and resource.csv to $COLLECTION_DATASET_BUCKET_NAME."; \
            make save-collection-log-resource; \
            echo "No state change and no new resources. Stopping processing."; \
            # Generate a new state file and upload to s3
            rm -f state.json; \
            make state.json; \
            make save-state; \
            exit 0; \
		}; \
        else \
            echo "No state.json found."; \
        fi

    # Generate a new state file
    # We will have to move this earlier in the future
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
    echo "No COLLECTION_DATASET_BUCKET_NAME defined so transformed files not pushed to s3"
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
