.PHONY: build release

test:
	go test ./...

build:
	goreleaser build --single-target --skip=validate --clean --snapshot

release:
	goreleaser release --clean
