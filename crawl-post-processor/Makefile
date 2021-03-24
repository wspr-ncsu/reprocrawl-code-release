TAG_ROOT = vpcpp-syncdb2020-webhook
TAG_HASH = $(shell git rev-parse --verify --short=0 HEAD:crawl-post-processor)
IMAGE_TAG = $(TAG_ROOT):$(TAG_HASH)
DOCKER_REPO = localhost:5000

# Default: build the local CLI binary with version tag embedded
binary:
	go build -ldflags "-X main.Version=$(TAG_HASH)"

# Image: build the Docker image locally
image:
	docker build -t $(IMAGE_TAG) .
	docker tag $(IMAGE_TAG) $(TAG_ROOT):latest

# Upstream: tag and push the current git-hash's build (if created) to the upstream repository
up:
	docker tag $(IMAGE_TAG) $(DOCKER_REPO)/$(IMAGE_TAG)
	docker push $(DOCKER_REPO)/$(IMAGE_TAG)

