IMAGE_BASE = cloud-gost-onramp
IMAGE_TAG = `git rev-parse --verify --short=0 HEAD:misc/cloud-gost-onramp`
IMAGE_NAME = $(IMAGE_BASE):$(IMAGE_TAG)
IMAGE_REPO = localhost:5000

image:
	docker build -t $(IMAGE_NAME) .
	docker tag $(IMAGE_NAME) $(IMAGE_BASE):latest
	which xclip 2>/dev/null \
		&& (echo "$(IMAGE_NAME)" | xclip -r -sel clipboard; echo "(tag copied to clipboard)") \
		|| echo "no xclip found; cannot copy tag to X clipboard"

up:
	docker tag $(IMAGE_NAME) $(IMAGE_REPO)/$(IMAGE_NAME)
	docker push $(IMAGE_REPO)/$(IMAGE_NAME)
