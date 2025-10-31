PROGRAM = pgduck_server
PGFILEDESC = "pgduck_server exposes DuckDB via the PostgreSQL protocol"

# we cache the libduckdb in this folder
LIBDUCKDB_DIR := ../duckdb_pglake/

PG_CONFIG ?= pg_config

# Get LIBDIR INCLUDE_DIR from pg_config
PG_LIBDIR := $(shell $(PG_CONFIG) --libdir)
PG_PKGLIBDIR := -L$(shell $(PG_CONFIG) --pkglibdir)
PG_INCLUDE_DIR = $(shell $(PG_CONFIG) --includedir)

# PGXS: Locates PostgreSQL extension building infrastructure using 'pg_config'.
PGXS := $(shell $(PG_CONFIG) --pgxs)

# compile with C11 option to use modern C
# use duckdb.h from the duckdb submodule
PG_CPPFLAGS = -std=c11 -I$(PG_INCLUDE_DIR) -Iinclude -I../duckdb_pglake/duckdb/src/include/

# Add dependencies
PG_LIBS = $(PG_PKGLIBDIR) -lpq

# Set the library path to the locally-built libduckdb.so
#
# We use PG_LIBS_INTERNAL to take priority over regular installation paths,
# which ensures the current build takes prioirty over the installed version.
#
# We add the rpath to be able to run tests without install.
PG_LIBS_INTERNAL = -Wl,-rpath,$(LIBDUCKDB_DIR) -L$(LIBDUCKDB_DIR) -lduckdb

# capture all objects in the current directory and in subdirectories
OBJS = $(patsubst %.c,%.o,$(wildcard src/*.c)) $(patsubst %.c,%.o,$(wildcard src/*/*.c))

USE_PGXS=1
include $(PGXS)

# postgres build flags explicitly disallow declarations after
# the first statement, but we prefer it
include ../shared.mk


# Custom target for running Python tests
.PHONY: check
check:
	@echo "Running Python tests..."
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests
	$(MAKE) -C tests/ctests check

.PHONY: installcheck
installcheck:
	@echo "Running Python tests..."
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests --installcheck
	#TODO: $(MAKE) -C tests/ctests check installcheck
