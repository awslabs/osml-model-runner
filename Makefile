AWS_ACCOUNT_ID		:= $(shell aws sts get-caller-identity --query Account --output text)
OSML_GITHUB			:= https://github.com/aws-solutions-library-samples/osml-model-runner.git
OSML_VERSION		:= $(shell cat setup.cfg | grep "version =" | awk -F' = ' '{print $$2}')

publish_local_build: ecr_login
	docker build --target runner -f docker/Dockerfile -t $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_DEFAULT_REGION).amazonaws.com/model-runner:latest .
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_DEFAULT_REGION).amazonaws.com/model-runner:latest 

ecr_login:
	aws ecr get-login-password --region $(AWS_DEFAULT_REGION) | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_DEFAULT_REGION).amazonaws.com

docker_test: 
	docker build --target unit-test -f docker/Dockerfile -t aip-mr-container-test:latest .
	docker run --rm -it aip-mr-container-test:latest

update_osml:
	rm -rf tmp src test
	git clone --branch v$(OSML_VERSION) --depth 1 $(OSML_GITHUB) tmp
	mv tmp/test .
	mv tmp/src .
	rm -rf tmp
