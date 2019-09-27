from typing import Union
import geopandas
import psycopg2
from psycopg2 import extensions
from dcfs_geodb import PG_DEFAULT_CONNECTION_PARAMETERS


def validate_dataframe(df: geopandas.GeoDataFrame) -> bool:
    cols = set(df.columns)
    valid_columns = {"RABA_PID", 'RABA_ID', 'D_OD', 'geometry'}

    return len(list(valid_columns - cols)) == 0


def write_to_geodb(data_source: Union[str, geopandas.GeoDataFrame],
                   con: Union[dict, extensions.connection, type(None)] = None) -> bool:
    """

    :param con: Connection to teh postgres database
    :type data_source: Shapefile to load into the Database. Requires attributes 'RABA_PID', 'RABA_ID', 'D_OD' and 'geometry'
    """

    if isinstance(con, dict):
        con = psycopg2.connect(**con)
    elif con is None:
        con = psycopg2.connect(**PG_DEFAULT_CONNECTION_PARAMETERS)

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

        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        cur.execute(f"INSERT INTO land_use(RABA_PID, RABA_ID, D_OD, GEOMETRY) "
                    f"VALUES ({raba_pid}, {raba_id}, '{d_od}', ST_GeometryFromText('{wkt}', 4326))")

    con.commit()
    return True
