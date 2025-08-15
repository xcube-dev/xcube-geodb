import json

import requests_mock

from tests.utils import del_env
from xcube_geodb.core.geodb import GeoDBClient

TEST_GEOM = (
    "0103000020D20E000001000000110000007593188402B51B4"
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
    "93188402B51B41B6F3FDD4423FF640"
)


# noinspection DuplicatedCode
@requests_mock.mock(real_http=False)
class GeoDBClientTestBase:
    def setUp(self) -> None:
        self._api = GeoDBClient(
            dotenv_file="tests/envs/.env_test",
            config_file="tests/.geodb",
            raise_it=True,
        )

        self._server_test_url = self._api._server_url
        self._server_test_port = self._api._server_port
        self._base_url = self._server_test_url
        if self._server_test_port:
            self._base_url += ":" + str(self._server_test_port)
        self._gs_server_url = self._api._gs_server_url
        self._gs_server_port = self._api._gs_server_port

        self._server_test_auth_domain = "https://winchester.deployment"

    def tearDown(self) -> None:
        del_env(dotenv_path="tests/envs/.env_test")

    def set_global_mocks(self, m):
        m.post(
            self._server_test_auth_domain + "/oauth/token",
            json={"access_token": "A long lived token", "expires_in": 12345},
        )

        url = f"{self._base_url}/rpc/geodb_whoami"
        m.get(url, text=json.dumps("helge"))

        url = f"{self._base_url}/rpc/geodb_get_collection_srid"
        m.post(url, json=[{"src": [{"srid": 4326}]}])

        url = f"{self._base_url}/rpc/geodb_log_event"
        m.post(url, text=json.dumps(""))

    def set_auth_change_mocks(self, m):
        m.post(
            self._server_test_auth_domain + "/oauth/token",
            json={
                "access_token": "A long lived token but a different user",
                "expires_in": 12345,
            },
        )

        url = f"{self._base_url}/rpc/geodb_whoami"
        m.get(url, text=json.dumps("pope"))

        self._api._auth_client_id = "fsvsdv"
