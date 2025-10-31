PGFILEDESC = "pg_map - associative array"
MODULE_big = pg_map

OBJS = \
	$(patsubst %.c,%.o,$(wildcard src/*.c))

HEADERS = $(wildcard src/*.h)

EXTENSION = pg_map
DATA = $(wildcard pg_map--*.sql)
EXTRA_CLEAN =
PG_CONFIG ?= pg_config

YACC = bison
LEX = flex

#CCFLAGS = -std=c99

ifdef CCFLAGS
	override PG_CXXFLAGS += $(CCFLAGS)
	override PG_CFLAGS += $(CCFLAGS)
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Rebuild when headers change
$(OBJS): $(HEADERS)

check:
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests

installcheck:
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests --installcheck

.PHONY: installcheck
