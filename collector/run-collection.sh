#! /bin/bash

TODAY=$(date +%Y-%m-%d)
echo "Running collector for $COLLECTION_NAME on $TODAY"

export AWS_ENDPOINT_URL=http://localhost:4566

aws_credentials_dev () {
    echo "Using DEVELOPMENT aws credentials"
    export AWS_ACCESS_KEY_ID="test"
    export AWS_SECRET_ACCESS_KEY="test"
    export AWS_DEFAULT_REGION="eu-west-2"
    export COLLECTION_DATASET_BUCKET_NAME="digital-land-dev"
}

aws_credentials_staging () {
    echo "Using STAGING aws credentials"
    export AWS_ACCESS_KEY_ID="test"
    export AWS_SECRET_ACCESS_KEY="test"
    export AWS_DEFAULT_REGION="eu-west-2"
    export COLLECTION_DATASET_BUCKET_NAME="digital-land-staging"
}

aws_credentials_prod () {
    echo "Using PRODUCTION aws credentials"
    export AWS_ACCESS_KEY_ID="test"
    export AWS_SECRET_ACCESS_KEY="test"
    export AWS_DEFAULT_REGION="eu-west-2"
    export COLLECTION_DATASET_BUCKET_NAME="digital-land-prod"
}


echo Update makerules
make makerules

echo Install dependencies
make init

echo Run the collector
make collect

echo Save resources to dev S3
aws_credentials_dev
make save-resources

echo Save resources to staging S3
aws_credentials_staging
make save-resources

echo Save resources to prod S3
aws_credentials_prod
make save-resources

echo Save logs to prod S3
aws_credentials_prod
make save-logs

echo Build the collection database
make collection

echo Push collection database to Development S3
aws_credentials_dev
make save-collection

echo Push collection database to Staging S3
aws_credentials_staging
make save-collection

echo Push collection database to Prod S3
aws_credentials_prod
make save-collection

ehco Transform collected files
make transformed -j 2

echo Save transformed files to Development S3
aws_credentials_dev
make save-transformed

echo Save transformed files to Staging S3
aws_credentials_staging
make save-transformed

echo Save transformed files to Prod S3
aws_credentials_prod
make save-transformed

echo Build datasets from the transformed files
make dataset

echo Save datasets and expecations to Development S3
aws_credentials_dev
make save-dataset
make save-expectations

echo Save datasets and expecations to Staging S3
aws_credentials_staging
make save-dataset
make save-expectations

echo Save datasets and expecations to Prod S3
aws_credentials_prod
make save-dataset
make save-expectations

# TODO: send notifications of errors

if [ -d /data ]; then
  echo Copying to /data/$COLLECTION_NAME
  mkdir -p /data/$COLLECTION_NAME
  for dir in collection expectations issue pipeline dataset flattened specification transformed; do
    cp -R /collector/$dir /data/$COLLECTION_NAME
  done
fi
