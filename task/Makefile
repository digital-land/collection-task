ifeq ($(COLLECTION_NAME),)
$(error Environment variable COLLECTION_NAME is not set)
endif

REPOSITORY=$(COLLECTION_NAME)-collection

BRANCH=not-a-real-branch

include makerules/makerules.mk
include makerules/development.mk
include makerules/collection.mk
include makerules/pipeline.mk
