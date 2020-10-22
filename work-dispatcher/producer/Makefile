IMAGE_BASE = kpw-producer
IMAGE_TAG = `git rev-parse --verify --short=0 HEAD`
IMAGE_NAME = $(IMAGE_BASE):$(IMAGE_TAG)

image:
	docker build -t $(IMAGE_NAME) .
	docker tag $(IMAGE_NAME) $(IMAGE_BASE):latest

up:
	docker tag $(IMAGE_NAME) localhost:5000/$(IMAGE_NAME)
	docker push localhost:5000/$(IMAGE_NAME)

