import os
import unittest
from io import StringIO
import pandas as pd
from geopandas import GeoDataFrame
from shapely import wkt

from dcfs_geodb.core.geo_db import GeoDB

GEODB_TEST_CONNECTION_PARAMETERS = {
    'server_url': "http://10.3.0.63",
    'server_port': 3000
}


@unittest.skipIf(os.environ.get('SKIP_PSQL_TESTS', False), 'DB Tests skipped')
class MyTestCase(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        if os.environ.get('SKIP_PSQL_TESTS', False):
            return

        cls._geodb = GeoDB(
            server_url=GEODB_TEST_CONNECTION_PARAMETERS['server_url'],
            server_port=GEODB_TEST_CONNECTION_PARAMETERS['server_port']
        )

        cls._test_csv = ("id,test_col_int,test_col_varchar,geometry\n\n"
                         "1,1,'blablubb',\"POINT(25 25)\"\n")

        cls._test_df = pd.read_csv(StringIO(cls._test_csv))
        cls._test_df['geometry'] = cls._test_df['geometry'].apply(wkt.loads)


    def test_add_dataset(self):
        props = [{'name': 'test_col_int', 'type': 'integer'}, {'name': 'test_col_varchar', 'type': 'VARCHAR(255)'}]

        r = self._geodb.create_dataset(dataset='test', properties=props, crs='4326')

        self.assertEqual(200, r.status_code)

    def test_add_datasets(self):
        props = [{'name': 'test1', 'crs': '4326',
                  'properties': [{'name': 'test_col_int', 'type': 'integer'},
                                 {'name': 'test_col_varchar', 'type': 'VARCHAR(255)'}]}]

        r = self._geodb.create_datasets(props)

        self.assertEqual(200, r.status_code)

    def test_drop_datasets(self):
        r = self._geodb.drop_dataset('test1')

        self.assertEqual(200, r.status_code)

    def test_add_properties(self):
        props = [{'name': 'test_col_int2', 'type': 'integer'},
                 {'name': 'test_col_varchar2', 'type': 'VARCHAR(255)'}]

        r = self._geodb.add_properties('test', props)
        self.assertEqual(200, r.status_code)

    def test_add_property(self):
        r = self._geodb.add_property('test','test_col_int3', 'integer')

        self.assertEqual(200, r.status_code)

    def test_drop_property(self):
        r = self._geodb.drop_property('test', 'test_col_int3')

        self.assertEqual(200, r.status_code)

    def test_drop_property(self):
        r = self._geodb.drop_properties('test', ['test_col_int2', 'test_col_varchar2'])

        self.assertEqual(200, r.status_code)

    def test_insert_into_dataset(self):
        values = GeoDataFrame(self._test_df, geometry=self._test_df['geometry'])

        r = self._geodb.insert_into_dataset(dataset='test', values=values, crs=4326)
        
        self.assertEqual(201, r.status_code)

    def test_update_dataset(self):
        r = self._geodb.update_dataset(dataset='test', values={'test_col_int': '3000'}, query='id=eq.1')

        self.assertEqual(200, r.status_code)

    def test_filter(self):
        r = self._geodb.delete_from_dataset(dataset='test', query='id=eq.1')
        self.assertEqual(200, r.status_code)

        values = GeoDataFrame(self._test_df, geometry=self._test_df['geometry'])

        r = self._geodb.insert_into_dataset(dataset='test', values=values, crs=4326)

        gpd = self._geodb.filter('test', 'id=eq.1')

        self.assertEqual(1, gpd.id[0])

    def test_filter_by_bbox(self):
        r = self._geodb.delete_from_dataset(dataset='test', query='id=eq.1')
        self.assertEqual(200, r.status_code)

        gpd = self._geodb.filter_by_bbox('test', 10, 10, 30, 30)

        self.assertEqual(1, gpd.id[0])


if __name__ == '__main__':
    unittest.main()
