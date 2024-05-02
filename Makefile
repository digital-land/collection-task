.PHONY: \
	compose-up \
	compose-down \
	build-image

compose-up::
	docker compose up -d --build

compose-down::
	docker compose down  --rmi 'all'

clobber::
	rm -rf ./task/collection ./task/pipeline ./task/dataset ./task/transformed ./task/var

build-image::
	docker build -t collection-workflow:latest .
 
