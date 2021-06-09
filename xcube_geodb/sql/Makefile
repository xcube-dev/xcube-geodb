VERSION := 1.0.0.dev0
EXTENSION = geodb
DATA = geodb--$(VERSION).sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
