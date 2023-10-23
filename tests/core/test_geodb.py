import json
import unittest
from io import StringIO
from unittest.mock import MagicMock

import pandas as pd
import requests
import requests_mock
from geopandas import GeoDataFrame
from psycopg2 import OperationalError
from shapely import wkt, Polygon

from tests.utils import del_env
from xcube_geodb.core.geodb import GeoDBClient, GeoDBError, warn, check_crs, \
    EventType
from xcube_geodb.core.message import Message

TEST_GEOM = "0103000020D20E000001000000110000007593188402B51B4" \
            "1B6F3FDD4423FF6405839B4C802B51B412B8716D9EC3EF6406" \
            "F1283C0EBB41B41A8C64B37C53EF640B6F3FDD4E4B41B419A999" \
            "999A33EF6400E2DB29DCFB41B41EE7C3F35B63EF6407F6ABC" \
            "74C0B41B41EE7C3F35B63EF6407B14AE47BDB41B41AAF1D24D" \
            "043FF6408B6CE77B64B41B413F355EBA8F3FF6402B8716D970" \
            "B41B41986E1283EC3FF640A4703D0A76B41B4179E92631AE3F" \
            "F6404260E5D08AB41B4123DBF97E923FF6409EEFA7C69CB41" \
            "B4100000000AC3FF6405839B448B3B41B411D5A643B973FF6" \
            "408195438BC6B41B41666666666C3FF640D122DBF9E3B41B4" \
            "139B4C876383FF640E9263188F8B41B41333333333D3FF64075" \
            "93188402B51B41B6F3FDD4423FF640"


# noinspection DuplicatedCode
@requests_mock.mock(real_http=False)
class GeoDBClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self._api = GeoDBClient(dotenv_file="tests/envs/.env_test", config_file="tests/.geodb", raise_it=True)

        self._server_test_url = self._api._server_url
        self._server_test_port = self._api._server_port
        self._base_url = self._server_test_url
        if self._server_test_port:
            self._base_url += ':' + str(self._server_test_port)

        self._server_test_auth_domain = "https://auth"

    def tearDown(self) -> None:
        del_env(dotenv_path="tests/envs/.env_test")

    def set_global_mocks(self, m):
        m.post(self._server_test_auth_domain + "/oauth/token", json={
            "access_token": "A long lived token",
            "expires_in": 12345
        })

        url = f"{self._base_url}/rpc/geodb_whoami"
        m.get(url, text=json.dumps("helge"))

        url = f"{self._base_url}/rpc/geodb_get_collection_srid"
        m.post(url, json=[{"src": [{"srid": 4326}]}])

        url = f'{self._base_url}/rpc/geodb_log_event'
        m.post(url, text=json.dumps(''))

    def set_auth_change_mocks(self, m):
        m.post(self._server_test_auth_domain + "/oauth/token", json={
            "access_token": "A long lived token but a different user",
            "expires_in": 12345
        })

        url = f"{self._base_url}/rpc/geodb_whoami"
        m.get(url, text=json.dumps("pope"))

        self._api._auth_client_id = "fsvsdv"

    def test_server_url(self, m):
        self.assertEqual(self._api._server_url, self._api.server_url)

    def test_my_usage(self, m):
        # self.set_global_mocks(m)

        m.post(self._server_test_auth_domain + "/oauth/token", json={
            "access_token": "A long lived token",
            "expires_in": 12345
        })

        expected_response = {'usage': "10MB"}
        server_response = [{'src': [expected_response]}]
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_get_my_usage"
        m.post(url, text=json.dumps(server_response))

        res = self._api.get_my_usage()
        self.assertDictEqual(expected_response, res)

        expected_response = {'usage': "10000"}
        server_response = [{'src': [expected_response]}]
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_get_my_usage"
        m.post(url, text=json.dumps(server_response))

        res = self._api.get_my_usage(pretty=False)
        self.assertDictEqual(expected_response, res)

    def test_get_my_collections(self, m):
        self.set_global_mocks(m)

        server_response = [
            {
                "collection": "geodb_admin_land_use",
                "grantee": "geodb_admin"
            },
            {
                "collection": "geodb_admin_land_use",
                "grantee": "PUBLIC"
            },
        ]

        server_response = [{"src": server_response}]

        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_get_my_collections"
        m.post(url, text=json.dumps(server_response))

        res = self._api.get_my_collections()
        self.assertIsInstance(res, pd.DataFrame)
        res = res.to_dict()
        expected_response = {'collection': {0: 'geodb_admin_land_use', 1: 'geodb_admin_land_use'},
                             'grantee': {0: 'geodb_admin', 1: 'PUBLIC'}}
        self.assertDictEqual(expected_response, res)

        m.post(url, json=[{'src': []}])

        res = self._api.get_my_collections()

        self.assertIsInstance(res, pd.DataFrame)
        self.assertEqual(len(res), 0)

    # noinspection PyTypeChecker
    def test_get_collection(self, m):
        self.set_global_mocks(m)
        global TEST_GEOM
        test_collection = [
            {"id": 1, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 2, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 3, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 4, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 5, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 6, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 7, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 8, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 9, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 10, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
        ]
        url = f"{self._server_test_url}:{self._server_test_port}/helge_test"
        m.get(url, text=json.dumps(test_collection))

        url_with_limits = f"{self._server_test_url}:" \
                          f"{self._server_test_port}" \
                          f"/helge_test?limit=2&offset=3"
        m.get(url_with_limits, text=json.dumps(test_collection[3:5]))

        url_with_limit_and_query = f"{self._server_test_url}:" \
                          f"{self._server_test_port}" \
                          f"/helge_test?id=10&limit=2&offset=0"
        m.get(url_with_limit_and_query, text=json.dumps([test_collection[9]]))

        r = self._api.get_collection('test')
        self.assertIsInstance(r, GeoDataFrame)
        self.assertTrue('geometry' in r)

        r = self._api.get_collection('test', limit=2, offset=3)
        self.assertIsInstance(r, GeoDataFrame)
        self.assertTrue('geometry' in r)
        self.assertEqual(2, r.shape[0])
        it = r.iterrows()
        self.assertEqual(4, dict(next(it)[1])['id'])
        self.assertEqual(5, dict(next(it)[1])['id'])

        r = self._api.head_collection('test')
        self.assertIsInstance(r, GeoDataFrame)
        self.assertTrue('geometry' in r)
        self.assertEqual(10, r.shape[0])

        r = self._api.get_collection('test', 'id=10', limit=2)
        self.assertIsInstance(r, GeoDataFrame)
        self.assertTrue('geometry' in r)
        self.assertEqual(1, r.shape[0])
        it = r.iterrows()
        self.assertEqual(10, dict(next(it)[1])['id'])


        url = f"{self._server_test_url}:{self._server_test_port}/helge_test"
        m.get(url, json=[])
        r = self._api.get_collection('test')
        self.assertIsInstance(r, pd.DataFrame)
        self.assertEqual(len(r), 0)

    def test_get_collection_bbox(self, m):
        self.set_global_mocks(m)
        url = f"{self._server_test_url}:" \
              f"{self._server_test_port}/rpc/geodb_get_collection_bbox"
        m.post(url, json="BOX(-6 9,5 11)")

        bbox = json.dumps(self._api.get_collection_bbox('any', exact=True))
        self.assertEqual(str([9, -6, 11, 5]), str(bbox))

        url = f"{self._server_test_url}:" \
              f"{self._server_test_port}/rpc/geodb_estimate_collection_bbox"
        m.post(url, json="BOX(-5 8,2 10)")

        bbox = json.dumps(self._api.get_collection_bbox('any'))
        self.assertEqual(str([8, -5, 10, 2]), str(bbox))

        m.post(url, text='null')
        bbox = self._api.get_collection_bbox('any')
        self.assertIsNone(bbox)

    def test_get_geometry_types(self, m):
        self.set_global_mocks(m)

        url = f"{self._base_url}/rpc/geodb_geometry_types"
        m.post(url, json=[{'types': [{'geometrytype': 'POLYGON'},
                                    {'geometrytype': 'POLYGON'},
                                    {'geometrytype': 'POINT'}]}])

        res = self._api.get_geometry_types('test', aggregate=False,
                                           database='test_db')
        expected = ['POLYGON', 'POLYGON', 'POINT']
        self.assertListEqual(res, expected)

        url = f"{self._base_url}/rpc/geodb_geometry_types"
        m.post(url, json=[{'types': [{'geometrytype': 'POINT'},
                                    {'geometrytype': 'POLYGON'}]}])

        res = self._api.get_geometry_types('test', aggregate=True,
                                           database='test_db')
        expected = ['POINT', 'POLYGON']
        self.assertListEqual(res, expected)

    def test_rename_collection(self, m):
        self.set_global_mocks(m)

        url = f"{self._base_url}/rpc/geodb_rename_collection"
        m.post(url, text="success")

        url = f'{self._base_url}/rpc/geodb_log_event'
        log_event_endpoint = m.post(url, text=json.dumps('something'))

        self.assertEqual(0, log_event_endpoint.call_count)

        res = self._api.rename_collection('test', 'test_new')
        expected = {'Message': "Collection renamed from test to test_new"}
        self.check_message(res, expected)

        self.assertEqual(1, log_event_endpoint.call_count)
        self.assertDictEqual(
            {"event_type": "renamed",
             "message": "collection None_test to None_test_new",
             "user": "helge"},
            json.loads(log_event_endpoint.last_request.text))

        url = f"{self._base_url}/rpc/geodb_rename_collection"
        m.post(url, text="error", status_code=400)

        with self.assertRaises(GeoDBError) as e:
            self._api.rename_collection('test', 'test_new')
        self.assertEqual("error", str(e.exception))

    def test_move_collection(self, m):
        self.set_global_mocks(m)

        url = f'{self._base_url}/rpc/geodb_rename_collection'
        m.post(url, text="success")

        url = f'{self._base_url}/rpc/geodb_log_event'
        log_event_endpoint = m.post(url, text=json.dumps('something'))

        self.assertEqual(0, log_event_endpoint.call_count)

        res = self._api.move_collection('test', 'db_new', 'db_old')
        expected = {'Message': "Collection moved from db_old to db_new"}
        self.check_message(res, expected)

        self.assertEqual(1, log_event_endpoint.call_count)
        self.assertDictEqual(
            {"event_type": "moved",
             "message": "collection db_old_test to db_new_test",
             "user": "helge"},
            json.loads(log_event_endpoint.last_request.text))

        m.post(url, text="error", status_code=400)

        with self.assertRaises(GeoDBError) as e:
            self._api.move_collection('test', 'db_old', 'db_new')
        self.assertEqual("error", str(e.exception))

    def test_copy_collection(self, m):
        self.set_global_mocks(m)

        url = f'{self._base_url}/rpc/geodb_copy_collection'
        m.post(url, text="success")
        url = f'{self._base_url}/rpc/geodb_log_event'
        log_event_endpoint = m.post(url, text=json.dumps('something'))

        self.assertEqual(0, log_event_endpoint.call_count)

        res = self._api.copy_collection('col', 'col_new', 'db_new')

        expected = {
            'Message': 'Collection copied from None_col to db_new_col_new'
        }
        self.check_message(res, expected)
        self.assertEqual(1, log_event_endpoint.call_count)
        self.assertDictEqual(
            {"event_type": "copied",
             "message": "collection None_col to db_new_col_new",
             "user": "helge"},
            json.loads(log_event_endpoint.last_request.text))

        m.post(url, text="error", status_code=400)

        with self.assertRaises(GeoDBError) as e:
            self._api.copy_collection('col', 'col_new', 'db_new')
        self.assertEqual("error", str(e.exception))

    def test_auth(self, m):
        self._api.use_auth_cache = False
        self.set_global_mocks(m)

        auth_access_token = self._api.auth_access_token
        self.assertEqual("A long lived token", auth_access_token)

        self.set_auth_change_mocks(m)
        auth_access_token = self._api.auth_access_token
        self.assertEqual('A long lived token', auth_access_token)

    def test_create_database(self, m):
        expected_response = True
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_create_database"
        m.post(url, text=json.dumps(expected_response))
        self.set_global_mocks(m)

        res = self._api.create_database(database='test')

        expected = {'Message': 'Database test created'}
        self.check_message(res, expected)

        m.post(url, text="Response invalid", status_code=400)

        with self.assertRaises(GeoDBError) as e:
            self._api.create_database(database='test')

        self.assertEqual("Response invalid", str(e.exception))

    def test_truncate_database(self, m):
        expected_response = True
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_truncate_database"
        m.post(url, text=json.dumps(expected_response))
        self.set_global_mocks(m)

        res = self._api.truncate_database(database='test')
        expected = {'Message': 'Database test truncated'}
        self.check_message(res, expected)

        m.post(url, text="Response invalid", status_code=400)
        with self.assertRaises(GeoDBError) as e:
            self._api.truncate_database(database='test')

        self.assertEqual("Response invalid", str(e.exception))

    def test_create_collection_if_not_exist(self, m):
        self.set_global_mocks(m)

        self._api.collection_exists = MagicMock(name='collection_exists', return_value=True)
        res = self._api.create_collection_if_not_exists('test', {})
        self.assertIsNone(res)

        self._api.collection_exists = MagicMock(name='collection_exists', return_value=False)
        self._api.create_collection = MagicMock(name='create_collection', return_value={'name': 'test'})
        res = self._api.create_collection_if_not_exists('test', {})
        self.assertDictEqual({'name': 'test'}, res)
        self.assertIsInstance(res, dict)

    def test_create_collections_if_not_exist(self, m):
        self.set_global_mocks(m)

        self._api.collection_exists = MagicMock(name='collection_exists', return_value=True)
        self._api.create_collections = MagicMock(name='create_collections', return_value={})
        res = self._api.create_collections_if_not_exist({'name': 'test'})
        self.assertDictEqual({}, res)
        self.assertIsInstance(res, dict)

        self._api.collection_exists = MagicMock(name='collection_exists', return_value=None)
        self._api.create_collections = MagicMock(name='create_collections', return_value={'name': 'test'})
        res = self._api.create_collections_if_not_exist({'name': 'test'})
        self.assertDictEqual({'name': 'test'}, res)
        self.assertIsInstance(res, dict)

    # noinspection DuplicatedCode
    def test_create_collection(self, m):
        self.set_global_mocks(m)

        response = 'Success'
        url = f'{self._base_url}/rpc/geodb_create_database'
        m.post(url, text=json.dumps(response))
        url = f'{self._base_url}/rpc/geodb_create_collections'
        m.post(url, text=json.dumps(response))
        url = f'{self._base_url}/geodb_user_databases?name=eq.helge'
        m.get(url, text=json.dumps('helge'))
        url = f'{self._base_url}/rpc/geodb_log_event'
        log_event_endpoint = m.post(url, text=json.dumps(response))

        self.assertEqual(0, log_event_endpoint.call_count)
        res = self._api.create_collection(collection='test',
                                          properties={'test_col': 'inger'})
        self.assertTrue(res)

        self.assertEqual(1, log_event_endpoint.call_count)
        self.assertDictEqual({"event_type": "created",
                              "message": "collection helge_test",
                              "user": "helge"},
                             json.loads(log_event_endpoint.last_request.text))

    def test_create_collections(self, m):
        expected_response = {'collections': {'helge_land_use3': {'crs': 3794,
                                                                 'properties': {'D_OD': 'date',
                                                                                'RABA_ID': 'float',
                                                                                'RABA_PID': 'float'}}}}
        # noinspection DuplicatedCode
        url = f'{self._base_url}/rpc/geodb_create_database'
        m.post(url, text=json.dumps(expected_response))
        url = f'{self._base_url}/rpc/geodb_create_collections'
        m.post(url, text=json.dumps(expected_response))
        url = f'{self._base_url}/geodb_user_databases?name=eq.helge'
        m.get(url, text=json.dumps('helge'))
        url = f'{self._base_url}/rpc/geodb_log_event'
        m.post(url, text=json.dumps(''))
        self.set_global_mocks(m)

        collections = {
            "land_use3":
                {
                    "crs": 3794,
                    "properties":
                        {
                            "RABA_PID": "float",
                            "RABA_ID": "float",
                            "D_OD": "date"
                        }
                }
        }

        res = self._api.create_collections(collections=collections)
        self.assertIsInstance(res, Message)
        self.assertDictEqual(expected_response, res.to_dict())

        self._api.database_exists = MagicMock(name='database_exists', return_value=False)
        res = self._api.create_collections(collections=collections, database='helge')
        expected = {'Message': "Database does not exist."}
        self.check_message(res, expected)

        self._api.database_exists = MagicMock(name='database_exists', return_value=True)
        self._api.drop_collections = MagicMock(name='drop_collections')
        res = self._api.create_collections(collections=collections, database='helge', clear=True)
        self._api.drop_collections.assert_called_once()

        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_create_collections"
        m.post(url=url, json=collections, status_code=400)

        with self.assertRaises(GeoDBError) as e:
             self._api.create_collections(collections=collections, database='helge', clear=True)

        expected = {'collections': {'helge_land_use3': {'crs': 3794, 'properties':
            {'RABA_PID': 'float', 'RABA_ID': 'float', 'D_OD': 'date'}}}}

        self.assertDictEqual(expected, res.to_dict())

    def test_drop_collections(self, m):
        self.set_global_mocks(m)
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_drop_collections"

        m.post(url=url, json={'name': 'test'}, status_code=400)

        with self.assertRaises(GeoDBError) as e:
            self._api.drop_collections(['test'])

        self.assertEqual('{"name": "test"}', str(e.exception))

    def test_create_collections_epsg_string(self, m):
        expected_response = {'collections': {'helge_land_use3': {'crs': 3794,
                                                                 'properties': {'D_OD': 'date',
                                                                                'RABA_ID': 'float',
                                                                                'RABA_PID': 'float'}}}}
        url = f'{self._base_url}/rpc/geodb_create_database'
        m.post(url, text=json.dumps(expected_response))
        url = f'{self._base_url}/rpc/geodb_create_collections'
        m.post(url, text=json.dumps(expected_response))
        url = f'{self._base_url}/geodb_user_databases?name=eq.helge'
        m.get(url, text=json.dumps('helge'))
        url = f'{self._base_url}/rpc/geodb_log_event'
        m.post(url, text=json.dumps(''))
        self.set_global_mocks(m)

        collections = {
            "land_use3":
                {
                    "crs": "epsg:3794",
                    "properties":
                        {
                            "RABA_PID": "float",
                            "RABA_ID": "float",
                            "D_OD": "date"
                        }
                }
        }

        res = self._api.create_collections(collections=collections)
        self.assertIsInstance(res.to_dict(), dict)
        self.assertDictEqual(expected_response, res.to_dict())

    def test_drop_collection(self, m):
        self.set_global_mocks(m)

        response = 'Success'
        url = f'{self._base_url}/rpc/geodb_drop_collections'
        m.post(url, text=json.dumps(response))
        url = f'{self._base_url}/rpc/geodb_log_event'
        log_event_endpoint = m.post(url, text=json.dumps(response))

        self.assertEqual(0, log_event_endpoint.call_count)
        res = self._api.drop_collection('test')
        self.assertTrue(res)

        self.assertEqual(1, log_event_endpoint.call_count)
        self.assertDictEqual({"event_type": "dropped",
                              "message": "collection helge_test",
                              "user": "helge"},
                             json.loads(log_event_endpoint.last_request.text))

    def test_add_properties(self, m):
        self.set_global_mocks(m)

        url = f'{self._base_url}/rpc/geodb_add_properties'
        m.post(url, text=json.dumps(''))
        url = f'{self._base_url}/rpc/geodb_log_event'
        log_event_endpoint = m.post(url, text=json.dumps(''))

        self.assertEqual(0, log_event_endpoint.call_count)
        res = self._api.add_properties('test_collection',
                                       {'test_prop': 'INT',
                                        'test_prop2': 'TEXT'})
        self.assertEqual(2, log_event_endpoint.call_count)
        self.assertDictEqual({'event_type': 'added property',
                              'message': '{name: test_prop2, type: TEXT} to '
                                         'collection helge_test_collection',
                              'user': 'helge'},
                             json.loads(log_event_endpoint.last_request.text))
        self.check_message(res, {'Message': 'Properties added'})

    @unittest.skip
    def test_filter_by_bbox(self, m):
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

        url = f"{self._server_test_url}:{self._server_test_port}"
        path = "/rpc/geodb_get_by_bbox"

        self.set_global_mocks(m)

        m.get(url=url, text=json.dumps({'paths': ['/rpc/geodb_get_by_bbox'],
                                        'definitions': ['helge_collection']}))

        m.get(url=url + '/helge_collection?limit=10', json={'test': 1})

        m.post(url + path, text=json.dumps(expected_response))

        gdf = self._api.get_collection_by_bbox(collection='collection',
                                               bbox=(452750.0, 88909.549, 464000.0, 102486.299))

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

        m.post(url + path, text=json.dumps({'src': []}))

        gdf = self._api.get_collection_by_bbox(collection='collection',
                                               bbox=(452750.0, 88909.549, 464000.0, 102486.299))

        self.assertIsInstance(gdf, pd.DataFrame)
        self.assertEqual(0, len(gdf))

        gdf = self._api.get_collection_by_bbox(collection='collection',
                                               bbox=(452750.0, 88909.549, 464000.0, 102486.299),
                                               bbox_crs=3307)

        self.assertIsInstance(gdf, pd.DataFrame)
        self.assertEqual(0, len(gdf))

    def test_get_collection_count(self, m):
        self.set_global_mocks(m)

        m.post(self._base_url + '/rpc/geodb_estimate_collection_count',
               text='12')
        m.post(self._base_url + '/rpc/geodb_count_collection', text='10')

        res = self._api.count_collection_rows('test')
        self.assertEqual(12, res)

        res = self._api.count_collection_rows('test', exact_count=True)
        self.assertEqual(10, res)

    def test_reproject_bbox(self, m):
        bbox_4326 = (9.8, 53.51, 10.0, 53.57)
        crs_4326 = 4326

        bbox_3857 = (1090931.0097740812, 7077896.970141199, 1113194.9079327357, 7089136.418602032)
        crs_3857 = 3857

        bbox = GeoDBClient.transform_bbox_crs(bbox=bbox_4326,
                                              from_crs=crs_4326, to_crs=crs_3857, wsg84_order="lat_lon")

        for i in range(4):
            self.assertAlmostEqual(bbox_3857[i], bbox[i])

        bbox = GeoDBClient.transform_bbox_crs(bbox=bbox_3857,
                                              from_crs=crs_3857, to_crs=crs_4326, wsg84_order="lat_lon")

        for i in range(4):
            self.assertAlmostEqual(bbox_4326[i], bbox[i])

    def test_reproject_bbox_epsg_string(self, m):
        bbox_4326 = (9.8, 53.51, 10.0, 53.57)
        crs_4326 = 'EPSG:4326'

        bbox_3857 = (1090931.0097740812, 7077896.970141199, 1113194.9079327357, 7089136.418602032)
        crs_3857 = 'EPSG:3857'

        bbox = GeoDBClient.transform_bbox_crs(bbox=bbox_4326,
                                              from_crs=crs_4326, to_crs=crs_3857, wsg84_order="lat_lon")

        for i in range(4):
            self.assertAlmostEqual(bbox_3857[i], bbox[i])

        bbox = GeoDBClient.transform_bbox_crs(bbox=bbox_3857,
                                              from_crs=crs_3857, to_crs=crs_4326, wsg84_order="lat_lon")

        for i in range(4):
            self.assertAlmostEqual(bbox_4326[i], bbox[i])

    def test_reproject_bbox_lon_lat(self, m):
        bbox_4326 = (53.51, 9.8, 53.57, 10.0)
        crs_4326 = 4326

        bbox_3857 = (1090931.0097740812, 7077896.970141199, 1113194.9079327357, 7089136.418602032)
        crs_3857 = 3857

        bbox = GeoDBClient.transform_bbox_crs(bbox=bbox_4326,
                                              from_crs=crs_4326, to_crs=crs_3857, wsg84_order="lon_lat")

        for i in range(4):
            self.assertAlmostEqual(bbox_3857[i], bbox[i])

        bbox = GeoDBClient.transform_bbox_crs(bbox=bbox_3857,
                                              from_crs=crs_3857, to_crs=crs_4326, wsg84_order="lon_lat")

        for i in range(4):
            self.assertAlmostEqual(bbox_4326[i], bbox[i])

    def test_delete_from_collection(self, m):
        path = '/helge_tt?id=eq.1'
        expected_response = 'success'

        self.set_global_mocks(m)

        url = self._base_url + path
        m.delete(url, text=expected_response)

        url = f'{self._base_url}/rpc/geodb_log_event'
        log_event_endpoint = m.post(url, text=json.dumps(''))

        self.assertEqual(0, log_event_endpoint.call_count)

        r = self._api.delete_from_collection('tt', 'id=eq.1')

        self.assertEqual(1, log_event_endpoint.call_count)
        self.assertDictEqual({'event_type': 'dropped rows',
                              'message': 'from collection helge_tt where '
                                         'id=eq.1',
                              'user': 'helge'},
                             json.loads(log_event_endpoint.last_request.text))

        expected = {"Message": 'Data from tt deleted'}
        self.check_message(r, expected)

        url = self._base_url + path
        m.delete(url, text="Response Error", status_code=400)

        with self.assertRaises(GeoDBError) as e:
            self._api.delete_from_collection('tt', 'id=eq.1')

        self.assertEqual("Response Error", str(e.exception))

    def test_update_collection(self, m):
        m.get(url=self._base_url + '/helge_tt?limit=10', json={'test': 1})
        path = '/helge_tt?id=eq.1'
        expected_response = 'success'

        m.get(url=self._base_url + "/", text=json.dumps({'definitions': ['helge_tt']}))
        m.patch(self._base_url + path, text=expected_response)
        self.set_global_mocks(m)

        values = {'column': 200, 'id': 1}
        r = self._api.update_collection('tt', values, 'id=eq.1')

        self.assertTrue(r)

        with self.assertRaises(ValueError) as e:
            # noinspection PyTypeChecker
            self._api.update_collection('tt', [1, 2, 3], 'id=eq.1')

        self.assertEqual(str(e.exception), "Format <class 'list'> not supported.")

        m.patch(self._base_url + path, text="Response Error", status_code=400)

        with self.assertRaises(GeoDBError) as e:
            self._api.update_collection('tt', values, 'id=eq.1')

        self.assertEqual('Response Error', str(e.exception))

    # noinspection PyMethodMayBeStatic
    def _make_test_df(self):
        csv = ("\n"
               "id,column1,column2,geometry\n\n"
               "0,1,ì,POINT(10 10)\n\n"
               "1,3,b,POINT(10 10)\n\n")

        for i in range(11000):
            csv += f"{i},{i},b,POINT(10 10)\n\n"
        file = StringIO(csv)

        df = pd.read_csv(file)
        df['geometry'] = df['geometry'].apply(wkt.loads)
        return df
        # return GeoDataFrame(df, crs={'init': 'epsg:4326'}, geometry=df['geometry'])

    def test_insert_into_collection(self, m):
        self.set_global_mocks(m)
        path = '/helge_tt'
        expected_response = 'success'

        m.get(url=self._base_url, text=json.dumps({'definitions': ['tt']}))
        m.post(self._base_url + path, text=expected_response)
        url = f'{self._base_url}/rpc/geodb_log_event'
        log_event_endpoint = m.post(url, text=json.dumps(''))

        self.assertEqual(0, log_event_endpoint.call_count)

        df = self._make_test_df()
        values = GeoDataFrame(df, crs='epsg:4326', geometry=df['geometry'])

        r = self._api.insert_into_collection('tt', values)
        expected = {'Message': '11002 rows inserted into tt'}
        self.check_message(r, expected)
        self.assertEqual(1, log_event_endpoint.call_count)
        self.assertDictEqual({'event_type': 'added rows',
                              'message': '11002 rows inserted into helge_tt',
                              'user': 'helge'},
                             json.loads(log_event_endpoint.last_request.text))

        r = self._api.insert_into_collection('tt', values, upsert=True)
        expected = {'Message': '11002 rows inserted into tt'}
        self.check_message(r, expected)

        with self.assertRaises(ValueError) as e:
            # noinspection PyTypeChecker
            self._api.insert_into_collection('tt', [1, 2, 3])

        self.assertEqual('Error: Format <class \'list\'> not supported.',
                         str(e.exception))

        with self.assertRaises(GeoDBError) as e:
            self._api.insert_into_collection('tt', values, crs=3307)

        self.assertEqual(
            'crs 3307 is not compatible with collection\'s crs 4326',
            str(e.exception))

        values = GeoDataFrame(df, crs='epsg:4326', geometry=df['geometry'])

        values.crs.to_epsg = MagicMock('to_epsg', return_value=None)
        self._api.get_collection_srid = MagicMock('get_collection_srid',
                                                  return_value=None)

        with self.assertRaises(GeoDBError) as e:
            self._api.insert_into_collection('tt', values)

        self.assertEqual('Invalid crs in geopandas data frame. You can pass '
                         'the crs as parameter (crs=[your crs])',
                         str(e.exception))

        exception_msg = '(\'Connection aborted.\', LineTooLong(\'got more ' \
                        'than 65536 bytes when reading header line\'))'
        m.register_uri('POST', self._base_url + path,
                       exc=requests.exceptions.ConnectionError(exception_msg))
        values = GeoDataFrame(df, crs='epsg:4326', geometry=df['geometry'])
        message = self._api.insert_into_collection('tt', values)
        expected = {'Message': '11002 rows inserted into tt'}
        self.check_message(message, expected)

        with self.assertRaises(requests.exceptions.ConnectionError) as e:
            exception_msg = '(\'Connection aborted for some reason\')'
            m.register_uri('POST', self._base_url + path,
                           exc=requests.exceptions.ConnectionError(exception_msg))
            values = GeoDataFrame(df, crs='epsg:4326', geometry=df['geometry'])
            self._api.insert_into_collection('tt', values)


    def test_grant_access_to_collection(self, m):
        self.set_global_mocks(m)
        m.post(self._base_url + "/rpc/geodb_grant_access_to_collection", text="success")

        res = self._api.grant_access_to_collection('test', 'drwho')
        expected = {'Message': 'Access granted on test to drwho'}
        self.check_message(res, expected)

        m.post(self._base_url + "/rpc/geodb_grant_access_to_collection", text="error", status_code=400)

        with self.assertRaises(GeoDBError) as e:
            self._api.grant_access_to_collection('test', 'drwho')
        self.assertEqual("error", str(e.exception))

    # def test_list_my_grants(self, m):
    def test_insert_into_collection_epsg_string(self, m):
        path = '/helge_tt'
        expected_response = 'success'

        m.get(url=self._base_url, text=json.dumps({'definitions': ['tt']}))
        m.post(self._base_url + path, text=expected_response)
        url = f'{self._base_url}/rpc/geodb_log_event'
        m.post(url, text=json.dumps(''))
        self.set_global_mocks(m)

        df = self._make_test_df()
        values = GeoDataFrame(df, crs='epsg:4326', geometry=df['geometry'])

        r = self._api.insert_into_collection('tt', values)

        expected = {'Message': '11002 rows inserted into tt'}
        self.check_message(r, expected)

    def test_list_grants(self, m):
        path = '/rpc/geodb_list_grants'
        response = [{'src': [{'collection': 'test', 'grantee': 'ernie'}]}]

        m.post(self._base_url + path, json=response)
        self.set_global_mocks(m)

        r = self._api.list_my_grants()

        self.assertEqual('test', r.collection[0])
        self.assertEqual('ernie', r.grantee[0])
        self.assertIsInstance(r, pd.DataFrame)

        response = []

        m.post(self._base_url + path, json=response)
        self.set_global_mocks(m)

        r = self._api.list_my_grants()

        self.assertEqual('No Grants', r.Grants[0])
        self.assertIsInstance(r, pd.DataFrame)

        no_json_response = 'vijdasovjidasjo'

        m.post(self._base_url + path, text=no_json_response)
        self.set_global_mocks(m)

        with self.assertRaises(GeoDBError) as e:
            self._api.list_my_grants()

        self.assertIn("Body not in valid JSON format:", str(e.exception))

    @unittest.skip("Not yet implemented")
    def test_register_user_to_geoserver(self, m):
        m.post(self._base_url + '/rpc/geodb_register_user', text="success")
        self.set_global_mocks(m)

        # self._api.register_user_to_geoserver('mama', 'mamaspassword')

    def test_filter_raw(self, m):
        m.get(url=self._base_url + '/helge_test?limit=10', json={'test': 1})
        m.get(url=self._base_url + '/helge_tesdsct?limit=10', json={}, status_code=404)
        m.get(url=self._base_url + "/", text=json.dumps({'definitions': ['helge_test'],
                                                                    'paths': ['/rpc/geodb_get_pg']}))

        expected_result = {'src': []}
        m.post(self._base_url + '/rpc/geodb_get_pg', json=expected_result)

        self.set_global_mocks(m)

        with self.assertRaises(GeoDBError) as e:
            self._api.get_collection_pg('tesdsct', select='min(tt)', group='tt', limit=1, offset=2)

        self.assertEqual("Collection tesdsct does not exist", str(e.exception))

        expected_result = {'src': [{'count': 142, 'D_OD': '2019-03-21'}, {'count': 114, 'D_OD': '2019-02-20'}]}
        m.post(self._base_url + '/rpc/geodb_get_pg', json=expected_result)

        r = self._api.get_collection_pg('test', select='count(D_OD)', group='D_OD', limit=1, offset=2)
        self.assertIsInstance(r, pd.DataFrame)
        self.assertEqual((2, 2), r.shape)

        expected_result = {'src': [{
            'id': 11,
            'created_at': '2020-01-20T14:45:30.763162+00:00',
            'modified_at': None,
            'geometry': '0103000020D20E0000010000001100000046B6F3FDA7151C417D3F355ECE58F740DD2406013C151C410E2DB29DC'
                        '35BF740C74B3709E6141C41F6285C8F1C5EF740BE9F1A2F40141C417F6ABC748562F740894160E583141C417B14A'
                        'E472363F740EC51B81EB0141C415EBA490CE061F7405EBA498CCE141C41E5D022DB1961F7404E621058EA141C41AA'
                        'F1D24D6860F7402FDD248612151C41FED478E9585FF7404A0C022B1E151C4114AE47E1045FF7405839B4C860151C4'
                        '1DBF97E6A2A5DF74021B072E881151C41D122DBF9425CF74093180456A2151C41FED478E9845BF74075931884C3151'
                        'C415839B4C8B45AF7405EBA498CF3151C4191ED7C3FA159F740C3F528DCF1151C41F6285C8F7659F74046B6F3FDA71'
                        '51C417D3F355ECE58F740',
            'RABA_PID': 5983161,
            'RABA_ID': 1100,
            'D_OD': '2019-03-11'}]}
        m.post(self._base_url + '/rpc/geodb_get_pg', json=expected_result)

        r = self._api.get_collection_pg('test', limit=1, offset=2)
        self.assertIsInstance(r, GeoDataFrame)
        self.assertEqual((1, 7), r.shape)
        self.assertIs(True, 'geometry' in r)
        self.assertIs(True, 'id' in r)
        self.assertIs(True, 'created_at' in r)
        self.assertIs(True, 'modified_at' in r)

        m.post(self._base_url + '/rpc/geodb_get_pg', json={'src': []})

        r = self._api.get_collection_pg('test', limit=1, offset=2)
        self.assertIsInstance(r, pd.DataFrame)
        self.assertEqual(len(r), 0)

        self._api._capabilities = dict(paths=[])
        with self.assertRaises(GeoDBError) as e:
            self._api.get_collection_pg('test', limit=1, offset=2)

        self.assertEqual("Stored procedure geodb_get_pg does not exist", str(e.exception))

    def test_init(self, m):
        with self.assertRaises(NotImplementedError) as e:
            GeoDBClient(auth_mode='interactive')

        self.assertEqual("The interactive mode has not been implemented.", str(e.exception))

        # Interactive has been deactivated at this stage due to deployment struggles and it not used in any deployment
        # with self.assertRaises(FileExistsError) as e:
        #     os.environ['GEODB_AUTH0_CONFIG_FILE'] = 'bla.env'
        #     GeoDBClient(auth_mode='interactive')
        #
        # self.assertEqual("Mandatory auth configuration file ipyauth-auth0-demo.env must exist", str(e.exception))

        with self.assertRaises(ValueError) as e:
            GeoDBClient(auth_mode='interacti')

        self.assertEqual("auth_mode can only be 'interactive', 'password', 'client-credentials', or 'openid'!",
                         str(e.exception))

    def test_auth_token(self, m):
        self._api.use_auth_cache = False
        m.post(self._server_test_auth_domain + "/oauth/token", json={"access_token": "A long lived token"})

        access_token = self._api.auth_access_token

        self.assertEqual("A long lived token", access_token)

        self._api._auth_access_token = 'Another token'

        access_token = self._api.auth_access_token

        self.assertEqual("Another token", access_token)

    def test_get_collection_info(self, m):
        self.set_global_mocks(m)

        expected_result = {'required': ['id', 'geometry'],
                           'properties': {'id': {'format': 'integer',
                                                 'type': 'integer',
                                                 'description': 'Note:\nThis is a Primary Key.<pk/>'},
                                          'created_at': {'format': 'timestamp with time zone', 'type': 'string'},
                                          'modified_at': {'format': 'timestamp with time zone', 'type': 'string'},
                                          'geometry': {'format': 'public.geometry(Geometry,3794)', 'type': 'string'},
                                          'raba_pid': {'format': 'double precision', 'type': 'number'}},
                           'type': 'object'}
        m.post(self._base_url + '/rpc/geodb_get_raw', json=expected_result)

        expected_result = {'id': 'integer'}
        m.get(url=self._base_url + "/", text=json.dumps({'definitions': {'helge_test': expected_result},
                                                                    'paths': ['/']}))
        res = self._api.get_collection_info('test')
        self.assertDictEqual(expected_result, res)

        with self.assertRaises(ValueError) as e:
            self._api.get_collection_info('test_not_exist')

        self.assertEqual("Table helge_test_not_exist does not exist.", str(e.exception))

    def test_namespace(self, m):
        self.set_global_mocks(m)

        geodb = GeoDBClient()
        self.assertEqual('helge', geodb.database)

        geodb = GeoDBClient(database='test')
        self.assertEqual('test', geodb.database)

    def test_auth_token_property(self, m):
        self.set_global_mocks(m)

        geodb = GeoDBClient()
        geodb.use_auth_cache = False
        geodb._auth_access_token = "testölasdjdkas"

        self.assertEqual("testölasdjdkas", geodb.auth_access_token)

        geodb._auth_access_token = None
        self.assertEqual("A long lived token", geodb.auth_access_token)

    def test_publish_collection(self, m):
        self.set_global_mocks(m)
        m.post(f'{self._base_url}/rpc/geodb_grant_access_to_collection',
               text='success')

        url = f'{self._base_url}/rpc/geodb_log_event'
        log_event_endpoint = m.post(url, text=json.dumps(''))

        self.assertEqual(0, log_event_endpoint.call_count)

        res = self._api.publish_collection('test')

        self.assertEqual(1, log_event_endpoint.call_count)
        self.assertDictEqual({'event_type': 'published',
                              'message': 'collection helge_test',
                              'user': 'helge'},
                             json.loads(log_event_endpoint.last_request.text))

        expected = {'Message': 'Access granted on helge_test to public.'}
        self.check_message(res, expected)

        m.post(f'{self._base_url}/rpc/geodb_grant_access_to_collection',
               text='error',
               status_code=400)

        with self.assertRaises(GeoDBError) as e:
            self._api.publish_collection('test')

        self.assertEqual('error', str(e.exception))

    def test_unpublish_collection(self, m):
        self.set_global_mocks(m)
        m.post(self._base_url + "/rpc/geodb_revoke_access_from_collection",
               text="success")

        url = f'{self._base_url}/rpc/geodb_log_event'
        log_event_endpoint = m.post(url, text=json.dumps(''))

        self.assertEqual(0, log_event_endpoint.call_count)

        res = self._api.unpublish_collection('test')

        self.assertEqual(1, log_event_endpoint.call_count)
        self.assertDictEqual({'event_type': 'unpublished',
                              'message': 'collection helge_test',
                              'user': 'helge'},
                             json.loads(log_event_endpoint.last_request.text))

        expected = {'Message': 'Access revoked from helge on helge_test'}
        self.check_message(res, expected)

        m.post(self._base_url + "/rpc/geodb_revoke_access_from_collection",
               text="error", status_code=400)

        with self.assertRaises(GeoDBError) as e:
            self._api.unpublish_collection('test')
        self.assertEqual("error", str(e.exception))

    def test_publish_to_geoserver(self, m):
        self.set_global_mocks(m)
        url = self._base_url + "/api/v2/services/xcube_geoserv/databases/" \
                               "geodb_admin/collections"
        m.put(url=url, json={'name': 'land_use'})

        url = f'{self._base_url}/rpc/geodb_log_event'
        log_event_endpoint = m.post(url, text=json.dumps(''))

        self.assertEqual(0, log_event_endpoint.call_count)

        res = self._api.publish_gs(collection="land_use",
                                   database="geodb_admin")

        self.assertEqual(1, log_event_endpoint.call_count)
        self.assertDictEqual({'event_type': 'published to geoserver',
                              'message': 'collection geodb_admin_land_use',
                              'user': 'helge'},
                             json.loads(log_event_endpoint.last_request.text))

        self.assertDictEqual({'name': 'land_use'}, res)

        url = self._base_url + "/api/v2/services/xcube_geoserv/databases/" \
                               "geodb_admin/collections"
        m.put(url=url, text='Error', status_code=400)

        with self.assertRaises(GeoDBError) as e:
            self._api.publish_gs(collection="land_use", database="geodb_admin")

        self.assertEqual("Error", str(e.exception))
        self.assertIsInstance(e.exception, GeoDBError)

    def test_gs_url(self, m):
        geodb = GeoDBClient(server_url='https://test_geodb', server_port=3000, gs_server_url='https://test_geoserv',
                            gs_server_port=4000)

        url = geodb._get_full_url('/test')
        self.assertEqual('https://test_geodb:3000/test', url)

        url = geodb._get_full_url('/services/xcube_geoserv')
        self.assertEqual('https://test_geoserv:4000/services/xcube_geoserv', url)

        geodb._gs_server_port = None
        url = geodb._get_full_url('/services/xcube_geoserv')
        self.assertEqual('https://test_geoserv/services/xcube_geoserv', url)

        geodb._server_port = None
        url = geodb._get_full_url('/test')
        self.assertEqual('https://test_geodb/test', url)

    def test_get_published_gs(self, m):
        self.maxDiff = None
        self.set_global_mocks(m)
        url = self._base_url + "/api/v2/services/xcube_geoserv/databases/geodb_admin/collections"

        server_response = {
            'collection_id': ['land_use'],
            'database': ['geodb_admin'],
            'default_style': [None],
            'geojson_url': [
                'https://test/geoserver/geodb_admin/ows?service=WFS&version=1.0.0'],
            'href': [None],
            'name': ['land_use'],
            'preview_url': [
                'https://test/geoserver/geodb_admin/wms?service=WMS&version=1.1.0'
            ],
            'wfs_url': [
                'https://test/geoserver/geodb_admin/wms?service=WMS&version=1.1.0'
            ]
        }

        m.get(url=url, json=server_response)

        res = self._api.get_published_gs('geodb_admin')
        self.assertIsInstance(res, pd.DataFrame)
        self.assertEqual(1, len(res))

        m.get(url=url, json={})

        res = self._api.get_published_gs('geodb_admin')
        self.assertIsInstance(res, pd.DataFrame)
        self.assertEqual(0, len(res))

    def test_get_all_published_gs(self, m):
        self.maxDiff = None
        self.set_global_mocks(m)
        url = self._base_url + "/api/v2/services/xcube_geoserv/collections"

        server_response = {
            'collection_id': ['land_use'],
            'database': ['None'],
            'default_style': [None],
            'geojson_url': [
                'https://test/geoserver/geodb_admin/ows?service=WFS&version=1.0.0'],
            'href': [None],
            'name': ['land_use'],
            'preview_url': [
                'https://test/geoserver/geodb_admin/wms?service=WMS&version=1.1.0'
            ],
            'wfs_url': [
                'https://test/geoserver/geodb_admin/wms?service=WMS&version=1.1.0'
            ]
        }

        m.get(url=url, json=server_response)

        res = self._api.get_all_published_gs()
        self.assertIsInstance(res, pd.DataFrame)
        self.assertEqual(1, len(res))

        m.get(url=url, json={})

        res = self._api.get_all_published_gs()
        self.assertIsInstance(res, pd.DataFrame)
        self.assertEqual(0, len(res))

    def test_unpublish_from_geoserver(self, m):
        self.set_global_mocks(m)
        url = self._base_url + '/api/v2/services/xcube_geoserv/databases/' \
                               'geodb_admin/collections/land_use'
        m.delete(url=url)

        url = f'{self._base_url}/rpc/geodb_log_event'
        log_event_endpoint = m.post(url, text=json.dumps(''))

        self.assertEqual(0, log_event_endpoint.call_count)

        res = self._api.unpublish_gs(collection='land_use',
                                     database='geodb_admin')

        self.assertEqual(1, log_event_endpoint.call_count)
        self.assertDictEqual({'event_type': 'unpublished from geoserver',
                              'message': 'collection geodb_admin_land_use',
                              'user': 'helge'},
                             json.loads(log_event_endpoint.last_request.text))

        self.assertTrue(res)

        url = self._base_url + '/api/v2/services/xcube_geoserv/databases/' \
                               'geodb_admin/collections/land_use'
        m.delete(url=url, text='Error', status_code=400)

        with self.assertRaises(GeoDBError) as e:
            self._api.unpublish_gs(collection='land_use',
                                   database='geodb_admin')

        self.assertEqual('Error', str(e.exception))
        self.assertIsInstance(e.exception, GeoDBError)

    def check_message(self, message, expected):
        self.assertIsInstance(message, Message)
        self.assertDictEqual(expected, message.to_dict())

    def test_add_property(self, m):
        self.set_global_mocks(m)

        url = f'{self._base_url}/rpc/geodb_add_properties'
        m.post(url, text='Properties added')
        url = f'{self._base_url}/rpc/geodb_log_event'
        log_event_endpoint = m.post(url, text=json.dumps(''))

        self.assertEqual(0, log_event_endpoint.call_count)

        res = self._api.add_property('col', 'prop', 'INT')

        self.assertEqual(1, log_event_endpoint.call_count)
        self.assertDictEqual({'event_type': 'added property',
                              'message': '{name: prop, type: INT} to '
                                         'collection helge_col',
                              'user': 'helge'},
                             json.loads(log_event_endpoint.last_request.text))

        self.check_message(res, {'Message': 'Properties added'})

    def test_drop_property(self, m):
        self.set_global_mocks(m)
        self._api._raise_for_stored_procedure_exists =\
            MagicMock(name='_raise_for_stored_procedure_exists')

        url = f'{self._base_url}/rpc/geodb_drop_properties'
        m.post(url, text=json.dumps(''))
        url = f'{self._base_url}/rpc/geodb_log_event'
        log_event_endpoint = m.post(url, text=json.dumps(''))

        self.assertEqual(0, log_event_endpoint.call_count)

        res = self._api.drop_property('test_col', 'test_prop')
        self._api.drop_property('test_col', 'test_prop2')

        self.assertEqual(2, log_event_endpoint.call_count)
        self.assertDictEqual({'event_type': 'dropped property',
                              'message': 'test_prop2 from '
                                         'collection helge_test_col',
                              'user': 'helge'},
                             json.loads(log_event_endpoint.last_request.text))

        expected = {'Message': 'Properties [\'test_prop\'] dropped from '
                               'helge_test_col'}
        self.check_message(res, expected)

    def test_drop_properties(self, m):
        self.set_global_mocks(m)
        self._api._raise_for_stored_procedure_exists = \
            MagicMock(name='_raise_for_stored_procedure_exists')

        url = f'{self._base_url}/rpc/geodb_log_event'
        log_event_endpoint = m.post(url, text=json.dumps(''))
        self.assertEqual(0, log_event_endpoint.call_count)

        url = self._base_url + "/rpc/geodb_drop_properties"
        m.post(url=url, json={'collection': 'test',
                              'properties': ['raba_id', 'allet']})
        res = self._api.drop_properties('test', ['raba_id', 'allet'])
        self.assertEqual(2, log_event_endpoint.call_count)
        self.assertDictEqual({'event_type': 'dropped property',
                              'message': 'allet from collection helge_test',
                              'user': 'helge'},
                             json.loads(log_event_endpoint.last_request.text))

        expected = {'Message': 'Properties [\'raba_id\', \'allet\'] dropped '
                               'from helge_test'}
        self.check_message(res, expected)

        self._api.raise_it = True
        with self.assertRaises(GeoDBError) as e:
            self._api.drop_properties('test', ['geometry', 'created_at'])

        self.assertIn("Don't delete the following columns", str(e.exception))
        self.assertIn("geometry", str(e.exception))
        self.assertIn("created_at", str(e.exception))
        self._api.raise_it = False

    def test_get_properties(self, m):
        self.set_global_mocks(m)

        url = self._base_url + "/rpc/geodb_get_properties"
        m.post(url=url, json=[{'src': {'name': 'geometry'}}, ])
        res = self._api.get_properties('test')

        self.assertIsInstance(res, pd.DataFrame)

        m.post(url=url, json=[{'src': {}}, ])

        self._api.get_properties('test')

        self.assertIsInstance(res, pd.DataFrame)

    def test_get_my_databases(self, m):
        self.set_global_mocks(m)
        self._api.get_collection = MagicMock(name='get_collection')
        self._api.get_my_databases()
        self._api.get_collection.assert_called_once()

    def test_get_collection_srid(self, m):
        self.set_global_mocks(m)
        url = f"{self._base_url}/rpc/geodb_get_collection_srid"
        m.post(url, json=[{"src": [{"srid": 4326}]}])

        r = self._api.get_collection_srid('test')
        self.assertEqual(4326, r)

        m.post(url, json=[{"src": []}], status_code=400)

        r = self._api.get_collection_srid('test')
        self.assertIsNone(r)

    def test_warn(self, m):
        with self.assertWarns(DeprecationWarning) as e:
            warn('test')

        self.assertEqual("test", str(e.warning))

    @unittest.skip
    def test_setup(self, m):
        geodb = GeoDBClient()
        with self.assertRaises(OperationalError) as e:
            geodb.setup()

        self.assertIn("could not connect to server", str(e.exception))

        # noinspection PyPep8Naming
        class cn:
            @staticmethod
            def commit():
                return True

            # noinspection PyPep8Naming
            class cursor:
                @staticmethod
                def execute(qry):
                    return True

        cn.cursor.execute = MagicMock()
        geodb.setup(conn=cn)
        cn.cursor.execute.assert_called_once()

    def test_df_from_json(self, m):
        # This test tests an impossible situation as `js` cannot be none. However, you never know.
        # noinspection PyTypeChecker
        res = self._api._df_from_json(js=None)
        self.assertIsInstance(res, pd.DataFrame)
        self.assertEqual(0, len(res))

    def test_load_geo(self, m):
        row = {'geometry': TEST_GEOM}
        self._api._convert_geo(row)
        self.assertEqual(Polygon([
            [453952.629, 91124.177],
            [453952.696, 91118.803],
            [453946.938, 91116.326],
            [453945.208, 91114.225],
            [453939.904, 91115.388],
            [453936.114, 91115.388],
            [453935.32, 91120.269],
            [453913.121, 91128.983],
            [453916.212, 91134.782],
            [453917.51, 91130.887],
            [453922.704, 91129.156],
            [453927.194, 91130.75],
            [453932.821, 91129.452],
            [453937.636, 91126.775],
            [453944.994, 91123.529],
            [453950.133, 91123.825],
            [453952.629, 91124.177]]), row['geometry'])

        row = {'geometry': {'type': 'Polygon',
                            'crs': {'type': 'name',
                                    'properties': {'name': 'EPSG:25832'}},
                            'coordinates': [[[52.87, 57.83],
                                             [52.82, 57.53],
                                             [52.68, 57.78],
                                             [52.96, 57.19],
                                             [52.87, 57.83]]]}}
        self._api._convert_geo(row)
        self.assertEqual(Polygon([[52.87, 57.83],
                                  [52.82, 57.53],
                                  [52.68, 57.78],
                                  [52.96, 57.19],
                                  [52.87, 57.83]]),
                         row['geometry'])

    def test_crs(self, m):
        with self.assertRaises(GeoDBError) as e:
            check_crs('epsg:hh')

        self.assertEqual("invalid literal for int() with base 10: 'hh'", str(e.exception))

    def test_refresh_auth_access_token(self, m):
        self.set_global_mocks(m)
        self._api.refresh_auth_access_token()

        self.assertIsNone(self._api._auth_access_token)
        # auth_access_token will retreive new token
        self.assertEqual("A long lived token", self._api.auth_access_token)

    def test_auth_access_token(self, m):
        self.set_global_mocks(m)
        self._api.use_auth_cache = False
        self._api._auth_client_id = None

        with self.assertRaises(GeoDBError) as e:
            r = self._api.auth_access_token

        self.assertEqual("System: Invalid client_credentials configuration.", str(e.exception))

        self._api._auth_mode = 'password'

        with self.assertRaises(GeoDBError) as e:
            r = self._api.auth_access_token

        self.assertEqual("System: Invalid password flow configuration", str(e.exception))

        self._api._auth_client_id = 'ksdjbvdkasj'
        self._api._auth_username = 'ksdjbvdkasj'
        self._api._auth_password = 'ksdjbvdkasj'

        r = self._api.auth_access_token

    def test_get_geodb_sql_version(self, m):
        self.set_global_mocks(m)
        url = f'{self._base_url}/rpc/geodb_get_geodb_sql_version'
        m.get(url, text='"1.1.5-dev"')
        self.assertEqual('1.1.5-dev', self._api.get_geodb_sql_version())

    def test_get_event_log(self, m):
        self.set_global_mocks(m)
        url = f'{self._base_url}/rpc/get_geodb_eventlog'
        get_event_log = m.get(
            url, json=[
                {'events':
                    [{
                        'event_type': 'created',
                        'message': 'collection my_test_collection',
                        'username': 'heisterkamp',
                        'date': '2022-08-19T00:19:03.968185'}]
                }
            ]
        )
        self.assertEqual(0, get_event_log.call_count)
        result = self._api.get_event_log(EventType.CREATED)
        self.assertEqual(1, get_event_log.call_count)
        self.assertEqual('collection my_test_collection',
                         result.iloc[0]['message'])
        self.assertEqual('heisterkamp',
                         result.iloc[0]['username'])

    def test_create_index(self, m):
        self.set_global_mocks(m)
        url = f"{self._base_url}/rpc/geodb_create_index"
        m.post(url, text='')

        m = self._api.create_index('my_collection', 'geometry')

        self.check_message(m, {'Message': 'Created new index on table helge_my_collection and property geometry.'})

    def test_remove_index(self, m):
        self.set_global_mocks(m)
        url = f"{self._base_url}/rpc/geodb_drop_index"
        m.post(url, text='')

        m = self._api.remove_index('my_collection', 'geometry')

        self.check_message(m, {'Message': 'Removed index from table helge_my_collection and property geometry.'})

    def test_show_indexes(self, m):
        self.set_global_mocks(m)
        url = f"{self._base_url}/rpc/geodb_show_indexes"
        m.post(url, json=[
            {'indexname': 'idx_database_my_collection_prop'},
            {'indexname': 'idx_database_my_collection_prop2'}
        ])

        result = self._api.show_indexes('my_collection')
        expected = pd.DataFrame(
            [{'indexname': 'idx_database_my_collection_prop'}, {'indexname': 'idx_database_my_collection_prop2'}])
        self.assertEqual(expected.to_json(), result.to_json())
