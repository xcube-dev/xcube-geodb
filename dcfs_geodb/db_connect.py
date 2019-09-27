import psycopg2


def db_connect(host: str = 'db-dcfs-geodb.cbfjgqxk302m.eu-central-1.rds.amazonaws.com'):
    connection_parameters = {
        'host': host,
        'database': 'postgres',
        'user': "postgres",
        'password': "Oeckel6b&z"
    }

    return psycopg2.connect(**connection_parameters)
