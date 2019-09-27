from typing import Union
import geopandas
import psycopg2
from psycopg2 import extensions
from dcfs_geodb import PG_DEFAULT_CONNECTION_PARAMETERS, DEFAULT_DELETE_SQL


def validate_dataframe(df: geopandas.GeoDataFrame) -> bool:
    cols = set([x.lower() for x in df.columns])
    valid_columns = {"raba_pid", 'raba_id', 'd_od', 'geometry'}

    return len(list(valid_columns - cols)) == 0


def geodb_connect(con: Union[dict, extensions.connection, type(None)] = None):
    if isinstance(con, dict):
        con = psycopg2.connect(**con)
    elif con is None:
        con = psycopg2.connect(**PG_DEFAULT_CONNECTION_PARAMETERS)

    return con


def raw_query(con: extensions.connection, sql: str = "SELECT COUNT(*) FROM land_use"):
    cur = con.cursor()

    try:
        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        print(f"Executing {sql}")
        cur.execute(sql)
    except SyntaxError as e:
        print(e)

    return cur.fetchall()


def write_to_geodb(con: extensions.connection, data_source: Union[str, geopandas.GeoDataFrame]):
    """

    :param con: Connection to teh postgres database
    :type data_source: Shapefile to load into the Database. Requires attributes 'RABA_PID', 'RABA_ID', 'D_OD' and 'geometry'
    """

    cur = con.cursor()

    gdf = data_source
    if isinstance(gdf, str):
        gdf = geopandas.read_file(gdf)

    if not validate_dataframe(gdf):
        raise ValueError("This function only accepts Columns 'RABA_PID', 'RABA_ID', 'D_OD' and 'geometry'")

    for index, row in gdf.iterrows():
        raba_pid = row["RABA_PID"]
        raba_id = row["RABA_ID"]
        d_od = row["D_OD"]
        wkt = row["geometry"]

        try:
            # noinspection SqlDialectInspection,SqlNoDataSourceInspection
            cur.execute(f"INSERT INTO land_use(RABA_PID, RABA_ID, D_OD, GEOMETRY) "
                        f"VALUES ({raba_pid}, {raba_id}, '{d_od}', ST_GeometryFromText('{wkt}', 4326))")
        except SyntaxError as e:
            print(e)

    con.commit()


def delete_from_geodb(con: extensions.connection, sql: str = DEFAULT_DELETE_SQL):
    if isinstance(con, dict):
        con = psycopg2.connect(**con)
    elif con is None:
        con = psycopg2.connect(**PG_DEFAULT_CONNECTION_PARAMETERS)

    cur = con.cursor()

    try:
        cur.execute(sql)
        con.commit()
        print(f"Deleted entries using: {sql}")
    except SyntaxError as e:
        print(e)
