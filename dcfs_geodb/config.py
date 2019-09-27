import os

PG_DEFAULT_CONNECTION_PARAMETERS = {
    'host': 'db-dcfs-geodb.cbfjgqxk302m.eu-central-1.rds.amazonaws.com',
    'database': 'postgres',
    'user': "postgres",
    'password': os.environ.get("PG_GEODB_PASSWORD")
}

DEFAULT_SQL = "select RABA_PID, RABA_ID, D_OD, geometry from land_use"
