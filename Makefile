AWS_ACCOUNT_ID		:= $(shell aws sts get-caller-identity --query Account --output text)

publish_local_build: ecr_login
	docker build --target runner -f docker/Dockerfile -t $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_DEFAULT_REGION).amazonaws.com/model-runner:latest .
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_DEFAULT_REGION).amazonaws.com/model-runner:latest 

ecr_login:
	aws ecr get-login-password --region $(AWS_DEFAULT_REGION) | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_DEFAULT_REGION).amazonaws.com

docker_test: 
	docker build --target unit-test -f docker/Dockerfile -t ie-mr-container-test:latest .
	docker run --rm -it ie-mr-container-test:latest