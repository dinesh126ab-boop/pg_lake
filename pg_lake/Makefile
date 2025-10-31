EXTENSION = pg_lake

MODULE_big = $(EXTENSION)

DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--3.0.sql
SOURCES := $(wildcard src/*.c) $(wildcard src/*/*.c)
OBJS := $(patsubst %.c,%.o,$(sort $(SOURCES)))

# Use the tag name
PG_LAKE_GIT_VERSION := $(shell git describe --tags --exact-match --abbrev=0 2>/dev/null)
ifeq ($(PG_LAKE_GIT_VERSION),)
    # Otherwise, use the branch name and commit hash
    PG_LAKE_GIT_VERSION := "$(shell git rev-parse --abbrev-ref HEAD) ($(shell git rev-parse --short HEAD))"
endif

PG_CPPFLAGS += -DPG_LAKE_GIT_VERSION=\"$(PG_LAKE_GIT_VERSION)\"

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
USE_PGXS=1
include $(PGXS)

include ../shared.mk

# Custom target for running Python tests
.PHONY: check
check:
	@echo "Running Python tests..."
	PG_LAKE_GIT_VERSION=$(PG_LAKE_GIT_VERSION) \
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests

.PHONY: installcheck
installcheck:
	@echo "Running Python tests..."
	PG_LAKE_GIT_VERSION=$(PG_LAKE_GIT_VERSION) \
	PGDUCK_PORT=5332 PYTHONPATH=../test_common pipenv run pytest -v tests/pytests --installcheck
