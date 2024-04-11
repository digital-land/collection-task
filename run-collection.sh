rm -rf home/collector
docker run --rm \
  --user collector --net=host \
  -e COLLECTION_NAME=central-activities-zone \
  -e REPOSITORY=central-activities-zone-collection \
  -e AWS_ENDPOINT_URL=http://localhost:4566 \
  -v "$(pwd)"/data:/data \
  digital-land-collector bash /collector/run-collection.sh

