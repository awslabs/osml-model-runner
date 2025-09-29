AWS_ACCOUNT_ID		:= $(shell aws sts get-caller-identity --query Account --output text)
TAG 				:= stable 

local_build: ecr_login
	docker build \
		--target runner \
		-f docker/Dockerfile \
		--build-arg BASE_IMG=$(GDAL_BASE_IMG) \
		-t $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_DEFAULT_REGION).amazonaws.com/ie-model-runner:$(TAG) \
		.

publish_local_build: local_build
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_DEFAULT_REGION).amazonaws.com/ie-model-runner:$(TAG) 

ecr_login:
	aws ecr get-login-password --region $(AWS_DEFAULT_REGION) | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_DEFAULT_REGION).amazonaws.com

docker_test: 
	docker build --target unit-test -f docker/Dockerfile -t ie-model-runner:test .
	docker run --rm -it ie-model-runner:test

run_interactive: local_build
	docker run -it \
		--rm \
		--entrypoint /bin/bash \
		-e AWS_DEFAULT_REGION="us-west-2" \
		-e WORKERS="4" \
		-e WORKERS_PER_CPU="1" \
		-e JOB_TABLE="TEST-JOB-TABLE" \
		-e ENDPOINT_TABLE="TEST-ENDPOINT-STATS-TABLE" \
		-e FEATURE_TABLE="TEST-FEATURE-TABLE" \
		-e REGION_REQUEST_TABLE="TEST-REGION-REQUEST-TABLE" \
		-e TILE_REQUEST_TABLE="TEST-TILE-REQUEST-TABLE" \
		-e IMAGE_QUEUE="TEST-IMAGE-QUEUE" \
		-e REGION_QUEUE="TEST-REGION-QUEUE" \
		-e IMAGE_STATUS_TOPIC="TEST-IMAGE-STATUS-TOPIC" \
		-e REGION_STATUS_TOPIC="TEST-REGION-STATUS-TOPIC" \
		-e SM_SELF_THROTTLING="true" \
		-e AWS_ACCESS_KEY_ID="testing" \
		-e AWS_SECRET_ACCESS_KEY="testing" \
		-e AWS_SECURITY_TOKEN="testing" \
		-e AWS_SESSION_TOKEN="testing" \
		-e USE_EXTENSIONS="true" \
		-e ARTIFACT_BUCKET="dummy-artifact-bucket" \
		$(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_DEFAULT_REGION).amazonaws.com/ie-model-runner:$(TAG)

# add input and output bucket into cdk dpeloyment