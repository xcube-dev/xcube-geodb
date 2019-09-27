import psycopg2
from psycopg2 import extensions
import geopandas as gpd

from dcfs_geodb import DEFAULT_LOAD_SQL, PG_DEFAULT_CONNECTION_PARAMETERS
from dcfs_geodb.manage_geodb import validate_dataframe


def load_geodb(con: extensions.connection, sql: str = DEFAULT_LOAD_SQL) -> gpd.GeoDataFrame:
    """
    Return the content of the geodb. default connection is located in config.py
    :param sql: sql to the geodb. Default loads everything
    :type con: Connection to database. Can be a dict with connection parameters, a psycopg2 connection, or None then the default connection will be used.
    """

    if isinstance(con, dict):
        con = psycopg2.connect(**con)
    elif con is None:
        con = psycopg2.connect(**PG_DEFAULT_CONNECTION_PARAMETERS)

    gdf = gpd.GeoDataFrame.from_postgis(sql, con, geom_col='geometry')

    if not validate_dataframe(gdf):
        raise ValueError("The data frame is invalid: Should have columns 'RABA_PID', 'RABA_ID', 'D_OD' and 'geometry'")

    return gdf
