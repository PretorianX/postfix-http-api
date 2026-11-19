BINARY := postfix-api
PKG := ./cmd/postfix-api
LDFLAGS := -s -w
DOCKER_BOKY_IMAGE ?= ghcr.io/pretorianx/postfix-http-api:boky-v4.4.0
TARGET ?= boky

.PHONY: all build build-linux test test-integration docker-boky lint clean

all: lint test build

build:
	@mkdir -p bin
	go build -trimpath -ldflags="$(LDFLAGS)" -o bin/$(BINARY) $(PKG)

build-linux:
	@mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="$(LDFLAGS)" -o dist/$(BINARY)-linux-amd64 $(PKG)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="$(LDFLAGS)" -o dist/$(BINARY)-linux-arm64 $(PKG)

test:
	go test -race -cover -count=1 ./...

lint:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		echo "Unformatted Go files:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi
	go vet ./...

test-integration:
	bash tests/integration/run_integration.sh $(TARGET)

docker-boky:
	docker build -f docker/boky/Dockerfile -t $(DOCKER_BOKY_IMAGE) .

clean:
	rm -rf bin dist coverage.txt *.coverprofile
