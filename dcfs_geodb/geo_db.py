from typing import Union

import psycopg2
from psycopg2 import extensions
import geopandas as gpd

from dcfs_geodb import PG_DEFAULT_CONNECTION_PARAMETERS, DEFAULT_LOAD_SQL, DEFAULT_DELETE_SQL


class GeoDB(object):
    def __init__(self, con):
        self._con = self._connect(con)

    # noinspection PyMethodMayBeStatic
    def _connect(self, con: Union[dict, extensions.connection, type(None)] = None):
        if isinstance(con, dict):
            con = psycopg2.connect(**con)
        elif con is None:
            con = psycopg2.connect(**PG_DEFAULT_CONNECTION_PARAMETERS)

        return con

    # noinspection PyMethodMayBeStatic
    def _validate(self, df: gpd.GeoDataFrame) -> bool:
        cols = set([x.lower() for x in df.columns])
        valid_columns = {"raba_pid", 'raba_id', 'd_od', 'geometry'}

        return len(list(valid_columns - cols)) == 0

    def load(self, sql: str = DEFAULT_LOAD_SQL) -> gpd.GeoDataFrame:
        """
        Return the content of the geodb. default connection is located in config.py
        :param sql: sql to the geodb. Default loads everything
        """

        gdf = gpd.GeoDataFrame.from_postgis(sql, self._con, geom_col='geometry')

        if not self._validate(gdf):
            raise ValueError(
                "The data frame is invalid: Should have columns 'RABA_PID', 'RABA_ID', 'D_OD' and 'geometry'")

        return gdf

    def write(self, data_source: Union[str, gpd.GeoDataFrame]):
        """
        :type data_source: Shapefile to load into the Database. Requires attributes 'RABA_PID', 'RABA_ID', 'D_OD' and 'geometry'
        """

        cur = self._con.cursor()

        gdf = data_source
        if isinstance(gdf, str):
            gdf = gpd.read_file(gdf)

        if not self._validate(gdf):
            raise ValueError("This function only accepts Columns 'RABA_PID', 'RABA_ID', 'D_OD' and 'geometry'")

        for index, row in gdf.iterrows():
            raba_pid = row["RABA_PID"]
            raba_id = row["RABA_ID"]
            d_od = row["D_OD"]
            wkt = row["geometry"]

            # noinspection SqlDialectInspection,SqlNoDataSourceInspection
            cur.execute(f"INSERT INTO land_use(RABA_PID, RABA_ID, D_OD, GEOMETRY) "
                        f"VALUES ({raba_pid}, {raba_id}, '{d_od}', ST_GeometryFromText('{wkt}', 4326))")

        self._con.commit()

    def delete(self, sql: str = DEFAULT_DELETE_SQL):
        cur = self._con.cursor()

        cur.execute(sql)
        self._con.commit()
        print(f"Deleted entries using: {sql}")

    def query(self, sql="SELECT COUNT(*) FROM land_use"):
        cur = self._con.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def query_by_bbox(self, minx, miny, maxx, maxy):
        p = f"POLYGON(({minx} {miny},{minx} {maxy},{maxx} {maxy},{maxx} {miny},{minx} {miny}))"

        sql = f"SELECT * FROM land_use " \
            f"WHERE ST_Contains('SRID=4326;{p}', geometry)"

        return self.load(sql=sql)
