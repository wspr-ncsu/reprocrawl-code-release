image:
	docker build -t kpw-consumer:`git rev-parse --verify --short=0 HEAD` .
	docker tag kpw-consumer:`git rev-parse --verify --short=0 HEAD` kpw-consumer:latest

up:
	docker tag kpw-consumer:`git rev-parse --verify --short=0 HEAD` localhost:5000/kpw-consumer:`git rev-parse --verify --short=0 HEAD`
	docker push localhost:5000/kpw-consumer:`git rev-parse --verify --short=0 HEAD`

