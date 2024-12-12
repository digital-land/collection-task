ifeq ($(COLLECTION_NAME),)
$(error Environment variable COLLECTION_NAME is not set)
endif

REPOSITORY=$(COLLECTION_NAME)-collection

BRANCH=not-a-real-branch

get-config::
	aws s3 sync s3://$(COLLECTION_DATASET_BUCKET_NAME)/config/$(COLLECTION_DIR)$(COLLECTION_NAME) $(COLLECTION_DIR) --no-progress
	aws s3 sync s3://$(COLLECTION_DATASET_BUCKET_NAME)/config/$(PIPELINE_DIR)$(COLLECTION_NAME) $(PIPELINE_DIR) --no-progress

include makerules/makerules.mk
include makerules/development.mk
include makerules/collection.mk
include makerules/pipeline.mk
