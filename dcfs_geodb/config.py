import os

PG_DEFAULT_CONNECTION_PARAMETERS = {
    'host': 'db-dcfs-geodb.cbfjgqxk302m.eu-central-1.rds.amazonaws.com',
    'database': 'postgres',
    'user': "postgres",
    'password': os.environ.get("PG_GEODB_PASSWORD")
}

DEFAULT_LOAD_SQL = "SELECT raba_pid, raba_id, d_od, geometry FROM land_use"

DEFAULT_DELETE_SQL = "DELETE FROM land_use"
