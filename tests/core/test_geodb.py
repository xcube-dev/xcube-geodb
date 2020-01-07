import json
import unittest
from io import StringIO
import pandas as pd

import requests_mock
from geopandas import GeoDataFrame
from shapely import wkt

from dcfs_geodb.core.geo_db import GeoDB


class GeoDBTest(unittest.TestCase):
    def setUp(self) -> None:
        self._server_test_url = "http://test"
        self._server_test_port = 3000
        self._server_full_address = self._server_test_url

        if self._server_test_port > 0:
            self._server_full_address += ':' + str(self._server_test_port)

        self._api = GeoDB(server_url=self._server_test_url, server_port=self._server_test_port)

    def tearDown(self) -> None:
        pass

    @requests_mock.mock()
    def test_create_dataset(self, m):
        expected_response = 'Success'
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_create_datasets"
        m.post(url, text=json.dumps(expected_response))

        res = self._api.create_dataset(dataset='test', properties=[{'name': 'test_col', 'type': 'inger'}])
        self.assertTrue(res)

    @requests_mock.mock()
    def test_drop_dataset(self, m):
        expected_response = 'Success'
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_drop_datasets"
        m.post(url, text=json.dumps(expected_response))

        res = self._api.drop_dataset('test')
        self.assertTrue(res)

    @requests_mock.mock()
    def test_add_properties(self, m):
        expected_response = 'Success'
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_add_properties"
        m.post(url, text=json.dumps(expected_response))

        res = self._api.add_properties('test', [{'name': 'test_col', 'type': 'insssssteger'}])
        self.assertTrue(res)

    @requests_mock.mock()
    def test_query_by_bbox(self, m):
        expected_response = {'src': [{'id': 'dd', 'geometry': "0103000020D20E000001000000110000007593188402B51B4"
                                                              "1B6F3FDD4423FF6405839B4C802B51B412B8716D9EC3EF6406"
                                                              "F1283C0EBB41B41A8C64B37C53EF640B6F3FDD4E4B41B419A999"
                                                              "999A33EF6400E2DB29DCFB41B41EE7C3F35B63EF6407F6ABC"
                                                              "74C0B41B41EE7C3F35B63EF6407B14AE47BDB41B41AAF1D24D"
                                                              "043FF6408B6CE77B64B41B413F355EBA8F3FF6402B8716D970"
                                                              "B41B41986E1283EC3FF640A4703D0A76B41B4179E92631AE3F"
                                                              "F6404260E5D08AB41B4123DBF97E923FF6409EEFA7C69CB41"
                                                              "B4100000000AC3FF6405839B448B3B41B411D5A643B973FF6"
                                                              "408195438BC6B41B41666666666C3FF640D122DBF9E3B41B4"
                                                              "139B4C876383FF640E9263188F8B41B41333333333D3FF64075"
                                                              "93188402B51B41B6F3FDD4423FF640"}]}

        url = "http://test:3000"
        path = "/rpc/geodb_get_by_bbox"
        m.get(url="http://test:3000/", text=json.dumps({'paths': ['/rpc/geodb_filter_by_bbox'],
                                                        'definitions': ['dataset']}))
        m.post(url + path, text=json.dumps(expected_response))

        gdf = self._api.filter_by_bbox(dataset='dataset', minx=452750.0, miny=88909.549, maxx=464000.0, maxy=102486.299)

        res = gdf.to_dict()
        ident = res['id'][0]
        geo = res['geometry'][0]
        exp_geo = "POLYGON ((453952.629 91124.177, 453952.696 91118.803, 453946.938 91116.326, " \
                  "453945.208 91114.22500000001, 453939.904 91115.38800000001, 453936.114 91115.38800000001, " \
                  "453935.32 91120.269, 453913.121 91128.98299999999, 453916.212 91134.78200000001, " \
                  "453917.51 91130.887, 453922.704 91129.156, 453927.194 91130.75, 453932.821 91129.452, " \
                  "453937.636 91126.77499999999, 453944.994 91123.52899999999, 453950.133 91123.825, " \
                  "453952.629 91124.177))"
        self.assertIsInstance(gdf, GeoDataFrame)
        self.assertEqual(ident, 'dd')
        self.assertEqual(str(geo), exp_geo)

    @requests_mock.mock()
    def test_delete_from_dataset(self, m):
        path = '/tt?id=eq.1'
        expected_response = 'success'

        m.delete(self._server_full_address + path, text=expected_response)

        r = self._api.delete_from_dataset('tt', 'id=eq.1')

        self.assertEqual(r.status_code, 200)
        self.assertEqual(expected_response, r.text)

    @requests_mock.mock()
    def test_update_dataset(self, m):
        path = '/tt?id=eq.1'
        expected_response = 'success'

        m.get(url="http://test:3000/", text=json.dumps({'definitions': ['tt']}))
        m.patch(self._server_full_address + path, text=expected_response)

        values = {'column': 200}
        r = self._api.update_dataset('tt', values, 'id=eq.1')

        self.assertEqual(r.status_code, 200)
        self.assertEqual(expected_response, r.text)

        with self.assertRaises(ValueError) as e:
            # noinspection PyTypeChecker
            self._api.update_dataset('tt', [1, 2, 3], 'id=eq.1')

        self.assertEqual(str(e.exception), "Format <class 'list'> not supported.")

    # noinspection PyMethodMayBeStatic
    def make_test_df(self):
        csv = """
        column1, column2,geometry\n
        1, 2,POINT(10 10)\n
        3, 4,POINT(10 10)\n
        """
        file = StringIO(csv)
        df = pd.read_csv(file)
        df['geometry'] = df['geometry'].apply(wkt.loads)
        return df

    @requests_mock.mock()
    def test_insert_into_dataset(self, m):
        path = '/tt'
        expected_response = 'success'

        m.get(url="http://test:3000/", text=json.dumps({'definitions': ['tt']}))
        m.post(self._server_full_address + path, text=expected_response)

        df = self.make_test_df()
        values = GeoDataFrame(df, crs={'init': 'epsg:4326'}, geometry=df['geometry'])

        r = self._api.insert_into_dataset('tt', values)

        self.assertEqual(r.status_code, 200)
        self.assertEqual(expected_response, r.text)

        with self.assertRaises(ValueError) as e:
            # noinspection PyTypeChecker
            self._api.insert_into_dataset('tt', [1, 2, 3])

        self.assertEqual(str(e.exception), "Format <class 'list'> not supported.")

        values = GeoDataFrame(df, geometry=df['geometry'])
        with self.assertRaises(ValueError) as e:
            self._api.insert_into_dataset('tt', values)

        self.assertEqual(str(e.exception), "Could not guess the dataframe's crs. Please specify.")

    @requests_mock.mock()
    def test_create_dataset(self, m):
        props = [{'name': 'tt', 'type': 'VARCHAR(255)'}]

        expected_response = 'Success'
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_create_datasets"
        m.post(url, text=json.dumps(expected_response))

        res = self._api.create_dataset('tt3', props)
        self.assertEqual(expected_response, res.json())

    # def test_real(self):
    #     api = GeoDB()
    #
    #     csv = """
    #             id,tt,geometry\n
    #             2,'blablubb',"POINT(25 25)"\n
    #             """
    #     file = StringIO(csv)
    #     df = pd.read_csv(file)
    #     df['geometry'] = df['geometry'].apply(wkt.loads)
    #
    #     values = GeoDataFrame(df, geometry=df['geometry'])
    #     values = {'column': 200}
    #
    #     r = api.update_dataset('tt2', values, "id=eq.2")
    #     print(r)
    #
    #     print('Finished')
