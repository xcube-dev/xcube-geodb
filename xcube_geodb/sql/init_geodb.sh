#!/bin/sh

set -e

# Perform all actions as $POSTGRES_USER
export PGUSER="$POSTGRES_USER"

# Create the 'geodb' template db and extension
"${psql[@]}" <<- 'EOSQL'
CREATE DATABASE geodb;
CREATE EXTENSION IF NOT EXISTS geodb;
EOSQL
