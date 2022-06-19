ROOT_PKG=appmeta

ROOT_DIR = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

UNAME ?= $(shell uname | tr A-Z a-z)
ARCH ?= $(shell uname -m)

INSTALL_DIR = bin
TARGET_DIR = bin/$(UNAME)
SOURCE_FILES=$(shell find . -name "*.go" -not -path "*_test.go" -not -path "*/.tmp/*")
TEST_PKGS=$(shell find . -name "*_test.go" -not -path "*/.tmp/*"| \
	cut -d / -f 2- | \
	xargs -I% dirname % | uniq | sed -e 's|^|$(ROOT_PKG)/|')
SRC_PKGS=$(shell find . -name "*.go" -not -path "*/.tmp/*"| \
  cut -d / -f 2- | \
  xargs -I% dirname % | uniq | sed -e 's|^|$(ROOT_PKG)/|')
BINS=$(shell ls $(ROOT_DIR)/cmd | xargs -I{} echo "$(TARGET_DIR)/{}")

# golang
GO = GOPATH= GO111MODULE=on go
GOBUILD = GOOS=$(UNAME) $(GO) build

COVERAGE_PROFILE=$(ROOT_DIR)/coverage.out
RACE ?= -race
GOTEST = $(GO) test -cover -coverprofile $(COVERAGE_PROFILE) $(RACE) -v

RM_DIR = rm -rf

all: clean build test

clean:
	@test $(ROOT_DIR)/bin && $(RM_DIR) $(ROOT_DIR)/bin || true
	@test $(ROOT_DIR)/log && $(RM_DIR) $(ROOT_DIR)/log || true
	@$(GO) clean -testcache

build: $(BINS)

test: vet
	@$(GOTEST) $(TEST_PKGS) && $(GO) tool cover -func=$(COVERAGE_PROFILE)

vet:
	@$(GO) vet $(SRC_PKGS)

proto-gen:
	./build/proto-gen.sh
