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

echo Build the collection database
make collection

make new-resources-list

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
                --state-path=state.json \
            && { \
			echo "State is unchanged."; \
			STATE_CHANGED=False; \
		} || { \
            echo "State has changed."; \
			STATE_CHANGED=True; \
		}; \
        else \
            echo "Incremental loading disabled as no state.json found."; \
            STATE_CHANGED=True; \
        fi

	if [ `wc -l < new_resources.txt` -eq 0 ]; then \
		echo "No new resources detected."; \
		NEW_RESOURCES=False; \
	else \
		echo "New resources detected."; \
		NEW_RESOURCES=True; \
	fi

	# Exit if both STATE_CHANGED=False and NEW_RESOURCES=False
	if [ "$$STATE_CHANGED" = "False" ] && [ "$$NEW_RESOURCES" = "False" ]; then \
		echo "No state change and no new resources. Exiting early."; \
		exit 0; \
	fi


    # Generate a new state file
    rm -f state.json
    make state.json
    else
        echo "No COLLECTION_DATASET_BUCKET_NAME defined to get previous state.json"
    fi
fi

echo Hello after incremental loading


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
