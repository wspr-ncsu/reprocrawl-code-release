# Where to push the image for "make up" (and where we may get the base image)
DOCKER_REPO = localhost:5000

# This is the docker image used as the base image (e.g., a VV8 build image)
BASE_IMAGE = $(DOCKER_REPO)/vv8-chrome-80.0.3987.163-1_amd64

# This is the given name of this vpc build (should include the base browser build version/features)
TAG_ROOT = vpc-crawl-worker-vv8-chrome80

# Computed tag name based on Git hash and given name
TAG_HASH = $(shell git rev-parse --verify --short=0 HEAD:crawler)
IMAGE_TAG = $(TAG_ROOT):$(TAG_HASH)

# Default: build the Docker image locally
image:
	docker build -t $(IMAGE_TAG) --build-arg BASE_IMAGE=$(BASE_IMAGE) .
	docker tag $(IMAGE_TAG) $(TAG_ROOT):latest

# Upstream: tag and push the current git-hash's build (if created) to the upstream repository
up:
	docker tag $(IMAGE_TAG) $(DOCKER_REPO)/$(IMAGE_TAG)
	docker push $(DOCKER_REPO)/$(IMAGE_TAG)

