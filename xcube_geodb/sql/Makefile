GEODB_VERSION ?= "unkown"
EXTENSION = geodb
DATA = $(EXTENSION)--$(GEODB_VERSION).sql

release_version: $(EXTENSION).sql
ifeq ($(GEODB_VERSION), "unkown")
	echo "Please set version in shell using export GEODB_VERSION=myversion"
	exit 1
else
	echo "GEODB_VERSION=$(GEODB_VERSION)"
	cp $(EXTENSION).sql $(EXTENSION)--$(GEODB_VERSION).sql
	sed -i "s/<version>/$(GEODB_VERSION)/g" geodb.control
endif


PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

include $(PGXS)
