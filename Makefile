.PHONY: dev build generate test

CGO_ENABLED=0
GOCMD=go
BINDIR=bin

dev: build
	@$(BINDIR)/mlctl help

build: generate
	@$(GOCMD) build -o $(BINDIR)/ ./cmd/mlctl/...

generate:
	@$(GOCMD) generate $(shell go list)/...

test:
	@$(GOCMD) test -v -cover -race .

