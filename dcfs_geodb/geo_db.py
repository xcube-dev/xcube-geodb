from typing import Union

import numpy
import psycopg2
# noinspection PyProtectedMember
from psycopg2._psycopg import AsIs
from psycopg2.extensions import register_adapter
from psycopg2.extras import execute_batch
from psycopg2 import extensions
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

from dcfs_geodb import PG_DEFAULT_CONNECTION_PARAMETERS, DEFAULT_LOAD_SQL, DEFAULT_DELETE_SQL
from dcfs_geodb.config import DEFAULT_DROP_AGG_SQL, DEFAULT_LOAD_AGG_SQL, DEFAULT_TABLE_EXISTS_SQL, DEFAULT_WRITE_SQL, \
    DEFAULT_QUERY_BBOX_SQL, DEFAULT_CREATE_AGG_SQL, DEFAULT_QUERY_SQL, \
    DEFAULT_WRITE_AGG_SQL_BATCH, DEFAULT_WRITE_SQL_BATCH, DEFAULT_QUERY_BBOX_SQL_AGG


class GeoDB(object):
    def __init__(self, con):
        self._con = self._connect(con)

    # noinspection PyMethodMayBeStatic
    def _connect(self, con: Union[dict, extensions.connection, type(None)] = None) -> extensions.connection:
        """

        Args:
            con: connection. Can be an existing connection (particularly for test mocks, None for using the
            default params or a connection dict)

        Returns:
            A connection of type psycopg2.extensions.connection

        """
        if isinstance(con, dict):
            con = psycopg2.connect(**con)
        elif con is None:
            con = psycopg2.connect(**PG_DEFAULT_CONNECTION_PARAMETERS)

        return con

    # noinspection PyMethodMayBeStatic
    def _validate(self, df: gpd.GeoDataFrame) -> bool:
        """

        Args:
            df: A geopands dataframe to validate columns. Must be "raba_pid", 'raba_id', 'd_od' or 'geometry'

        Returns:
            bool whether validation succeeds
        """
        cols = set([x.lower() for x in df.columns])
        valid_columns = {"raba_pid", 'raba_id', 'd_od', 'geometry'}

        return len(list(valid_columns - cols)) == 0

    # noinspection PyMethodMayBeStatic
    def _validate_agg(self, df: pd.DataFrame) -> bool:
        """

        Args:
            df: A pands aggregation dataframe to validate columns. Must be "land_use_id", 'mean', 'std'

        Returns:
            bool whether validation succeeds
        """
        cols = set([x.lower() for x in df.columns])
        valid_columns = {"land_use_id", 'mean', 'std'}

        return len(list(valid_columns - cols)) == 0

    def _table_exists(self, table: str) -> bool:
        """

        Args:
            table: A table name to check

        Returns:
            bool whether table exists in DB

        """
        sql = DEFAULT_TABLE_EXISTS_SQL.format(table=table)

        result = self.query(sql)

        return result[0] == 1

    def load_from_land_use(self, where: str = 'TRUE') -> gpd.GeoDataFrame:
        """

        Args:
            where: A where statement (without keyword WHERE) in PostreSQL SQL dialect

        Returns:
            A GeopandasDataFrame containing the land_use polygons

        """
        sql = DEFAULT_LOAD_SQL.format(where=where)
        gdf = gpd.GeoDataFrame.from_postgis(sql, self._con, geom_col='geometry')

        if not self._validate(gdf):
            raise ValueError(
                "The data frame is invalid: Should have columns 'RABA_PID', 'RABA_ID', 'D_OD' and 'geometry'")

        return gdf

    def load_agg(self, variable: str, where: str = 'TRUE') -> gpd.GeoDataFrame:
        """

        Args:
            variable: The variable you want to query
            where: A where statement (without keyword WHERE) in PostreSQL SQL dialect

        Returns:
            A GeopandasDataFrame containing the by land_use polygons aggregated values and land_use polygons
        """
        if self._table_exists(f'land_use_agg_{variable}'):
            sql = DEFAULT_LOAD_AGG_SQL.format(variable=variable, where=where)

            return gpd.GeoDataFrame.from_postgis(sql, self._con, geom_col='geometry')
        else:
            raise ValueError(f'Table for variable {variable} does not exist.')

    def drop_agg(self, variable: str):
        """

        Args:
            variable: The variable's table to be dropped
        """
        if self._table_exists(f'land_use_agg_{variable}'):
            sql = DEFAULT_DROP_AGG_SQL.format(variable=variable)
            self.query(sql)

    def write_to_land_use(self, data_source: Union[str, gpd.GeoDataFrame]):
        """

        Args:
            data_source: Shapefile /GeopandasDataframe to load into the Database.
            Requires attributes 'RABA_PID', 'RABA_ID', 'D_OD' and 'geometry'
        """
        cur = self._con.cursor()

        gdf = data_source
        if isinstance(gdf, str):
            gdf = gpd.read_file(gdf)

        if not self._validate(gdf):
            raise ValueError("This function only accepts Columns 'RABA_PID', 'RABA_ID', 'D_OD' and 'geometry'")

        register_adapter(numpy.int64, AsIs)
        register_adapter(Polygon, AsIs)
        ds_dict = gdf[['RABA_PID', 'RABA_ID', 'D_OD', 'geometry']].to_records(index=False)
        table = 'land_use'
        execute_batch(cur=cur, sql=DEFAULT_WRITE_SQL_BATCH.format(table=table), argslist=ds_dict)
        self._con.commit()

    def write_agg(self, variable: str, data_source: pd.DataFrame):
        """

        Args:
            variable: The variable you want to store means and stds for
            data_source: pandas DataFrame to be loaded into the GeoDB. Requires attributes
            'land_use_id', 'mean' and 'std'
        """
        if not self._table_exists(f'land_use_agg_{variable}'):
            self.create_agg_table(variable)

        cur = self._con.cursor()

        if not self._validate_agg(data_source):
            raise ValueError("This function only accepts Columns 'land_use_id', 'mean', 'std'")

        register_adapter(numpy.int64, AsIs)
        ds_dict = data_source[['land_use_id', 'mean', 'std']].to_records(index=False)
        table = f'land_use_agg_{variable}'
        execute_batch(cur=cur, sql=DEFAULT_WRITE_AGG_SQL_BATCH.format(table=table), argslist=ds_dict)

    def _delete(self, table: str, where: str = 'id=-1'):
        """

        Args:
            table: The table you want to delete data from
            where: A where statement (without keyword WHERE) in PostreSQL SQL dialect
        """
        sql = DEFAULT_DELETE_SQL.format(table=table, where=where)
        self.query(sql)
        print(f"Deleted entries in {table} using: {where}")

    def delete_from_land_use(self, where: str = 'id=-1'):
        """

        Args:
            where: A where statement (without keyword WHERE) in PostreSQL SQL dialect
        """
        self._delete(table='land_use', where=where)

    def delete_agg(self, variable: str, where: str = 'id=-1'):
        """

        Args:
            variable: The variable you want to delete data from
            where: A where statement (without keyword WHERE) in PostreSQL SQL dialect. Default: -1 to enforce usage.
        """
        self._delete(table=f'land_use_agg_{variable}', where=where)

    def purge(self, table: str):
        """

        Args:
            table: table: The table you want to purge
        """
        self._delete(table=table, where='TRUE')

        print(f"Deleted all entries using: in {table}")

    def query(self, sql: str = DEFAULT_QUERY_SQL):
        """

        Args:
            sql: The raw SQL statement in PostgreSQL dialect

        Returns:
            A list of tuples if the number of returned rows is larger than one or a single tuple otherwise, or
            nothing if the query is not a SELECT statement

        """
        cur = self._con.cursor()
        cur.execute(sql)
        if "SELECT" in sql:
            if cur.rowcount == 1:
                return cur.fetchone()
            else:
                return cur.fetchall()
        else:
            self._con.commit()
            return True

    def query_by_bbox(self, minx, miny, maxx, maxy, table: str = None, variable: str = None, mode: str = 'contains'):
        """
        Args:
            variable:
            table:
            minx: left side of bbox
            miny: bottom side of bbox
            maxx: right side of bbox
            maxy: top side of bbox
            mode: selection mode. 'within': the geometry A is completely hmhmhm in B,
        Returns:
            A GeopandasDataFrame with tge result
        """
        bbox = f"POLYGON(({minx} {miny},{minx} {maxy},{maxx} {maxy},{maxx} {miny},{minx} {miny}))"

        func = 'ST_Contains'
        if mode == 'within':
            func = 'ST_Within'

        sql = "SELECT * FROM land_use LIMIT 1"
        if table is not None:
            sql = DEFAULT_QUERY_BBOX_SQL.format(table=table, func=func, bbox=bbox)
        if variable is not None:
            sql = DEFAULT_QUERY_BBOX_SQL_AGG.format(variable=variable, func=func, bbox=bbox)

        return gpd.GeoDataFrame.from_postgis(sql, self._con, geom_col='geometry')

    def create_agg_table(self, variable: str):
        """

        Args:
            variable: The aggretation variable the table is to be created for. Usually done automatically
                    during the write_agg process
        """
        sql = DEFAULT_CREATE_AGG_SQL.format(variable=variable)
        self.query(sql)
