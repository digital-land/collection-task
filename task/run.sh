#! /bin/bash

set -e

TODAY=$(date +%Y-%m-%d)
echo "Running collector for $COLLECTION_NAME on $TODAY"

# shouldn't b. hardcoded into this file
# export AWS_ENDPOINT_URL=http://localhost:4566


# echo Update makerules
# make makerules

echo Install dependencies
make init

echo Run the collector
make collect


# echo Save resources to prod S3
# make save-resources

# echo Save logs to prod S3
# make save-logs

echo Build the collection database
make collection

# echo Push collection database to Prod S3
# make save-collection

echo Transform collected files
make transformed -j 2

# echo Save transformed files to Prod S3
# make save-transformed

echo Build datasets from the transformed files
make dataset

# echo Save datasets and expecations to Prod S3
# aws_credentials_prod
# make save-dataset
# make save-expectations

# TODO: send notifications of errors

# if [ -d /data ]; then
#   echo Copying to /data/$COLLECTION_NAME
#   mkdir -p /data/$COLLECTION_NAME
#   for dir in collection expectations issue pipeline dataset flattened specification transformed; do
#     cp -R /collector/$dir /data/$COLLECTION_NAME
#   done
# fi
