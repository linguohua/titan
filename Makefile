SHELL=/usr/bin/env bash

all: build
.PHONY: all

unexport GOFLAGS

GOCC?=go

GOVERSION:=$(shell $(GOCC) version | tr ' ' '\n' | grep go1 | sed 's/^go//' | awk -F. '{printf "%d%03d%03d", $$1, $$2, $$3}')
ifeq ($(shell expr $(GOVERSION) \< 1017001), 1)
$(warning Your Golang version is go$(shell expr $(GOVERSION) / 1000000).$(shell expr $(GOVERSION) % 1000000 / 1000).$(shell expr $(GOVERSION) % 1000))
$(error Update Golang to version to at least 1.17.1)
endif

# git modules that need to be loaded
MODULES:=

CLEAN:=
BINS:=

ldflags=-X=github.com/linguohua/titan.CurrentCommit=+git.$(subst -,.,$(shell git describe --always --match=NeVeRmAtCh --dirty 2>/dev/null || git rev-parse --short HEAD 2>/dev/null))
ifneq ($(strip $(LDFLAGS)),)
	ldflags+=-extldflags=$(LDFLAGS)
endif

GOFLAGS+=-ldflags="$(ldflags)"


titan-scheduler: $(BUILD_DEPS)
	rm -f titan-scheduler
	$(GOCC) build $(GOFLAGS) -o titan-scheduler ./cmd/titan-scheduler
.PHONY: titan-scheduler


titan-candidate: $(BUILD_DEPS)
	rm -f titan-candidate
	$(GOCC) build $(GOFLAGS) -o titan-candidate ./cmd/titan-candidate
.PHONY: titan-candidate


titan-edge: $(BUILD_DEPS)
	rm -f titan-edge
	$(GOCC) build $(GOFLAGS) -o titan-edge ./cmd/titan-edge
.PHONY: titan-edge


api-gen:
	$(GOCC) run ./gen/api
	goimports -w api
	goimports -w api
.PHONY: api-gen
	
build: titan-scheduler titan-candidate titan-edge
.PHONY: build
