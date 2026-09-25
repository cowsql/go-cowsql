GO ?= go
HASH := \#
TAG_SQLITE3=$(shell printf "$(HASH)include <cowsql.h>\nvoid main(){cowsql_node_id n = 1;}" | $(CC) ${CGO_CFLAGS} -o /dev/null -xc - >/dev/null 2>&1 && echo "libsqlite3")
CGO_LDFLAGS_ALLOW ?= (-Wl,-wrap,pthread_create)|(-Wl,-z,now)

default: build

.PHONY: build
build:
ifeq "$(TAG_SQLITE3)" ""
	@echo "Missing cowsql, run \"make deps\" to setup."
	exit 1
endif
	mkdir -p ./bin/
	CGO_ENABLED=1 CC="$(CC)" CGO_LDFLAGS_ALLOW="$(CGO_LDFLAGS_ALLOW)" $(GO) build -o ./bin/cluster -v -tags "$(TAG_SQLITE3)" $(DEBUG) ./cmd
