import unittest

import geopandas as gpd
import pandas as pd

from dcfs_geodb import GeoDB
from tests import psycopg2_mock

PG_DEFAULT_CONNECTION_PARAMETERS = {
    'host': 'db-dcfs-geodb.cbfjgqxk302m.eu-central-1.rds.amazonaws.com',
    'database': 'postgres',
    'user': "postgres",
    'password': "Oeckel6b&z"
}


class GeoDBTest(unittest.TestCase):
    def test_write_to_geodb(self):
        con = psycopg2_mock.connect("postgres://mock")
        geodb = GeoDB(con)

        result = geodb.write_to_land_use(data_source='notebooks/data/sample/land_use.shp')
        self.assertTrue(result)

    def test_load_geodb(self):
        con = psycopg2_mock.connect("postgres://mock")
        geodb = GeoDB(con)

        result = geodb.load_from_land_use()
        self.assertTrue(isinstance(result, gpd.GeoDataFrame))

    def test_query_by_bbox(self):
        geodb = GeoDB(PG_DEFAULT_CONNECTION_PARAMETERS)

        gdf = geodb.query_by_bbox(452750.0, 88909.549, 464000.0, 102486.299)

        print(gdf)

    def test_write_agg(self):
        geodb = GeoDB(PG_DEFAULT_CONNECTION_PARAMETERS)

        df = pd.read_csv('test_agg.csv')

        geodb.write_agg('mock', df)

        print('success')

    def test_load_agg(self):
        geodb = GeoDB(PG_DEFAULT_CONNECTION_PARAMETERS)

        gdf = geodb.load_agg('mock')

        self.assertEqual(2, len(gdf))

    def test_drop_agg(self):
        geodb = GeoDB(PG_DEFAULT_CONNECTION_PARAMETERS)

        geodb.drop_agg('mock')

