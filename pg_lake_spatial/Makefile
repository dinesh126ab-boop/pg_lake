EXTENSION = pg_lake_spatial

MODULE_big = $(EXTENSION)

DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--3.0.sql
SOURCES := $(wildcard src/*.c) $(wildcard src/*/*.c)
OBJS := $(patsubst %.c,%.o,$(sort $(SOURCES)))

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
USE_PGXS=1
include $(PGXS)

include ../shared.mk

# Custom target for running Python tests
.PHONY: check
check:
	@echo "Running Python tests..."
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests

.PHONY: installcheck
installcheck:
	@echo "Running Python tests..."
	
	PGDUCK_PORT=5332 PYTHONPATH=../test_common pipenv run pytest -v tests/pytests --installcheck
