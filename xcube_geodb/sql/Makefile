GEODB_VERSION := 1.0.0.dev2
EXTENSION = geodb
DATA = geodb--$(GEODB_VERSION).sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
