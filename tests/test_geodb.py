import unittest

import geopandas as gpd

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

        result = geodb.write(data_source='notebooks/data/sample/land_use.shp')
        self.assertTrue(result)

    def test_load_geodb(self):
        con = psycopg2_mock.connect("postgres://mock")
        geodb = GeoDB(con)

        result = geodb.load()
        self.assertTrue(isinstance(result, gpd.GeoDataFrame))

    def test_query_by_bbox(self):
        geodb = GeoDB(PG_DEFAULT_CONNECTION_PARAMETERS)

        gdf = geodb.query_by_bbox(452750.0, 88909.549, 464000.0, 102486.299)

        print(gdf)



