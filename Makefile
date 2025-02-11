.PHONY: build release gen

test:
	go test ./...

build:
	goreleaser build --single-target --skip=validate --clean --snapshot

release:
	goreleaser release --clean

gen:
	protoc -I="." --go_out=. --go_opt=module=hareku/s3zip proto/metadata.proto
