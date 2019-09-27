import unittest

import geopandas as gpd

from dcfs_geodb import load_geodb
from dcfs_geodb import write_to_geodb
from tests import psycopg2_mock


class GeoDBTest(unittest.TestCase):
    def test_write_to_geodb(self):
        con = psycopg2_mock.connect("postgres://mock")

        result = write_to_geodb(con=con, data_source='notebooks/data/sample/land_use.shp')
        self.assertTrue(result)

    def test_load_geodb(self):
        con = psycopg2_mock.connect("postgres://mock")

        result = load_geodb(con=con)
        self.assertTrue(isinstance(result, gpd.GeoDataFrame))


