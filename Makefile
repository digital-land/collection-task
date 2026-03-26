ifeq ($(COLLECTION_NAME),)
$(error Environment variable COLLECTION_NAME is not set)
endif

.PHONY: \
	compose-up \
	compose-down \
	build-image

compose-up::
	docker compose up -d --build

compose-down::
	docker compose down  --rmi 'all'

build-image::
	docker build -t collection-task:latest .

REPOSITORY=$(COLLECTION_NAME)-collection

BRANCH=not-a-real-branch

# useful parameters to alter for development
# CONFIG_URL allows you to change where all config is downloaded from it's default is the files cdn in production
# CONFIG_URL=https://raw.githubusercontent.com/digital-land/config/refs/heads/feat/tb-lookup-rule/
# MAKERULES_URL allows you to change where the makerules are downloaded from it's default is the makerules repo in production
# MAKERULES_URL=https://raw.githubusercontent.com/digital-land/makerules/feat/change_state/



include makerules/makerules.mk
include makerules/development.mk
include makerules/collection.mk
include makerules/pipeline.mk

make clean::
	make clobber
	rm -rf performance
	rm -rf collection
	rm -rf specification
	rm -rf log
	rm -rf state.json