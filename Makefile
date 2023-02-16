BINARY := generate
DOCKER_IMAGE := moficodes/generate
DOCKER_TAG := v0.0.7

clean:
	rm -f $(BINARY)
	rm -f input*.txt

build: clean
	CGO_ENABLED=0 go build -ldflags="-s -w" .

run: build
	./$(BINARY) -h

docker-build:
	docker buildx build --platform linux/amd64 -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

docker-push: docker-build
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)