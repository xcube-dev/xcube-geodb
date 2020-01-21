import os
from typing import Dict, Optional, Union, Sequence

import geopandas as gpd
import psycopg2
from geopandas import GeoDataFrame
from pandas import DataFrame
from shapely import wkb
import requests
import json
from dotenv import load_dotenv, find_dotenv

from dcfs_geodb.defaults import GEODB_API_DEFAULT_PARAMETERS


class GeoDBError(ValueError):
    pass


class GeoDBClient(object):
    def __init__(self, server_url: Optional[str] = None,
                 server_port: Optional[int] = None,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 anonymous: bool = False,
                 dot_env_file: str = ".env"):
        """

        Args:
            server_url (str): The URL of the PostGrest Rest API
            server_port (str): The port to the PostGrest Rest API
            dot_env_file (str): Name of the dotenv file [.env]
            anonymous (bool): Whether the client connection is anonymous (without credentials) [False]
            client_secret (str): Client secret
            client_id (str): Client ID
        """
        # noinspection SpellCheckingInspection
        self._dotenv_file = find_dotenv(filename=dot_env_file)
        if self._dotenv_file:
            load_dotenv(self._dotenv_file)

        self._set_defaults()
        self._set_from_env()

        self._server_url = server_url or self._server_url
        self._server_port = server_port or self._server_port
        self._auth_client_id = client_id or self._auth_client_id
        self._auth_client_secret = client_secret or self._auth_client_secret
        self._auth_access_token = None

        self._capabilities = None
        self._is_public_client = anonymous
        self._geodb_api_access_token = None

        self._whoami = None

        self._mandatory_properties = ["geometry", "id", "created_at", "modified_at"]

    def get_dataset_info(self, dataset: str):
        capabilities = self.capabilities
        if dataset in capabilities['definitions']:
            return capabilities['definitions'][dataset]
        else:
            raise ValueError(f"Table {dataset} does not exist.")

    @property
    def common_headers(self):
        return {
            'Prefer': 'return=representation',
            'Content-type': 'application/json',
            'Authorization': f"Bearer {self.auth_access_token}"
        }

    @property
    def whoami(self):
        if self._whoami is None:
            self._whoami = self.get(path='/rpc/geodb_whoami').json()

        return self._whoami

    @property
    def capabilities(self):
        if self._capabilities is None:
            self._capabilities = self.get(path='/').json()

        return self._capabilities

    def _refresh_capabilities(self):
        self._capabilities = None

    def post(self, path: str, payload: Union[Dict, Sequence], params: Optional[Dict] = None,
             headers: Optional[Dict] = None) -> requests.models.Response:

        """

        Args:
            headers: Request headers. Allows Overriding common header entries.
            path: API path
            payload: Post body as Dict. Will be dumped to JSON
            params: Request parameters

        Returns:
            A Request object

        Raises:
            PostgrestException: if request fails

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.post(path='/rpc/my_function', payload={'name': 'MyName'})
        """

        common_headers = self.common_headers

        if headers is not None:
            common_headers.update(headers)

        r = None
        try:
            if common_headers['Content-type'] == 'text/csv':
                r = requests.post(self._get_full_url(path=path), data=payload, params=params, headers=common_headers)
            else:
                r = requests.post(self._get_full_url(path=path), json=payload, params=params, headers=common_headers)
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            raise GeoDBError(r.text)

        return r

    def get(self, path: str, params: Optional[Dict] = None, headers: Optional[Dict] = None) -> requests.models.Response:
        """

        Args:
            headers: Request headers. Allows Overriding common header entries.
            path: API path
            params: Request parameters

        Returns:
            A Request object

        Raises:
            PostgrestException: if request fails

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.get(path='/my_table', params={"limit": 1})

        """
        headers = self.common_headers.update(headers) if headers else self.common_headers

        r = None
        try:
            r = requests.get(self._get_full_url(path=path), params=params, headers=headers)
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            raise GeoDBError(r.json()['message'])

        return r

    def delete(self, path: str, params: Optional[Dict] = None, headers: Optional[Dict] = None) \
            -> requests.models.Response:
        """

        Args:
            headers: Request headers. Allows Overriding common header entries.
            path: API path
            params: Request parameters

        Returns:
            A Request object

        Raises:
            PostgrestException: if request fails

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.delete(path='/my_table', params={"limit": 1})

        """

        headers = self.common_headers.update(headers) if headers else self.common_headers
        r = None
        try:
            r = requests.delete(self._get_full_url(path=path), params=params, headers=headers)
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            raise GeoDBError(r.json()['message'])
        return r

    def patch(self, path: str, payload: Union[Dict, Sequence], params: Optional[Dict] = None,
              headers: Optional[Dict] = None) -> requests.models.Response:
        """

        Args:
            headers: Request headers. Allows Overriding common header entries.
            payload:
            path: API path
            params: Request parameters

        Returns:
            A Request object

        Raises:
            GeoDBError: if request fails

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.patch(path='/my_table', params={"limit": 1})

        """

        headers = self.common_headers.update(headers) if headers else self.common_headers
        r = None
        try:
            r = requests.patch(self._get_full_url(path=path), json=payload, params=params, headers=headers)
            r.raise_for_status()
        except requests.HTTPError:
            raise GeoDBError(r.json()['message'])
        return r

    def create_datasets(self, datasets: Sequence[Dict]) -> requests.models.Response:
        """

        Args:
            datasets: A json list of datasets

        Returns:
            requests.models.Response:

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.create_dataset(datasets=['myDataset1', 'myDataset2'])
        """

        self._refresh_capabilities()

        datasets = {"datasets": datasets}
        return self.post(path='/rpc/geodb_create_datasets', payload=datasets)

    def create_dataset(self, dataset: str, properties: Sequence[Dict], crs: str = "4326") -> requests.models.Response:
        """

        Args:
            crs:
            dataset: Dataset to be created
            properties: Property definitions for the dataset

        Returns:
            requests.models.Response:

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.create_dataset(dataset='myDataset')
        """

        dataset = {'name': dataset, 'properties': properties, 'crs': crs}

        self._refresh_capabilities()

        return self.create_datasets([dataset])

    def drop_dataset(self, dataset: str) -> requests.models.Response:
        """

        Args:
            dataset: Dataset to be dropped

        Returns:
            requests.models.Response:

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.drop_dataset(dataset='myDataset')
        """

        self._refresh_capabilities()

        return self.post(path='/rpc/geodb_drop_datasets', payload={'datasets': [dataset]})

    def drop_datasets(self, datasets: Sequence[str]) -> requests.models.Response:
        """

        Args:
            datasets: Datasets to be dropped

        Returns:
            requests.models.Response:

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.drop_datasets(datasets=['myDataset1', 'myDaraset2'])
        """

        self._refresh_capabilities()

        return self.post(path='/rpc/geodb_drop_datasets', payload={'datasets': datasets})

    def grant_access_to_dataset(self, dataset: str, user: str = "public") -> requests.models.Response:
        dataset = f"{self.whoami}_{dataset}"

        return self.post(path='/rpc/geodb_grant_access_to_dataset', payload={'dataset': dataset, 'usr': user})

    def revoke_access_to_dataset(self, dataset: str, user: str = 'public') -> requests.models.Response:
        dataset = f"{self.whoami}_{dataset}"

        return self.post(path='/rpc/geodb_revoke_access_to_dataset', payload={'dataset': dataset, 'usr': user})

    def list_grants(self) -> Sequence:
        r = self.post(path='/rpc/geodb_list_grants', payload={})
        if r.json()[0]['src'] is None:
            return []
        else:
            return r.json()[0]['src']

    def add_property(self, dataset: str, prop: str, typ: str) -> requests.models.Response:
        """
        Add a property to an existing dataset
        Args:
            dataset: Dataset to add a property to
            prop: Property name
            typ: Type of property

        Examples:
            >>> prop = {}
            >>> geodb = GeoDBClient()
            >>> geodb.add_property(dataset='myDataset', prop=prop)
        """

        prop = {'name': prop, 'type': typ}

        return self.add_properties(dataset=dataset, properties=[prop])

    def add_properties(self, dataset: str, properties: Sequence[Dict]) -> requests.models.Response:
        """

        Args:
            dataset:Dataset to add properties to
            properties: Property definitions as json array

        Returns:
            requests.models.Response:

        Examples:
            >>> prop = {}
            >>> geodb = GeoDBClient()
            >>> geodb.add_property(dataset='myDataset', prop=[prop])
        """

        self._refresh_capabilities()

        return self.post(path='/rpc/geodb_add_properties', payload={'dataset': dataset, 'properties': properties})

    def drop_property(self, dataset: str, prop: str) -> requests.models.Response:
        """

        Args:
            dataset: Dataset to drop the property from
            prop: Property to delete

        Returns:
            requests.models.Response:

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.drop_property(dataset='myDataset', prop='myProperty')
        """

        return self.drop_properties(dataset=dataset, properties=[prop])

    def drop_properties(self, dataset: str, properties: Sequence[str]) -> requests.models.Response:
        """

        Args:
            dataset: Dataset to delete properties from
            properties: A json object containing the property definitions

        Returns:
            requests.models.Response:

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.drop_properties(dataset='myDataset', properties=['myProperty1', 'myProperty2'])
        """

        self._refresh_capabilities()

        self._raise_for_mandatory_columns(properties)

        self._raise_for_stored_procedure_exists('geodb_drop_properties')

        return self.post(path='/rpc/geodb_drop_properties', payload={'dataset': dataset, 'properties': properties})

    def _raise_for_mandatory_columns(self, properties: Sequence[str]):
        common_props = list(set(properties) & set(self._mandatory_properties))
        if len(common_props) > 0:
            raise ValueError("Don't delete the following columns: " + str(common_props))

    def get_properties(self, dataset: str) -> DataFrame:
        r = self.post(path='/rpc/geodb_get_properties', payload={'dataset': dataset})

        js = r.json()[0]['src']

        if js:
            return self._df_from_json(js)
        else:
            return DataFrame(columns=["table_name", "column_name", "data_type"])

        # dataset = f"{self.whoami}_{dataset}"
        # return self.capabilities['definitions'][dataset]

    def get_datasets(self) -> DataFrame:
        r = self.post(path='/rpc/geodb_list_datasets', payload={})

        js = r.json()[0]['src']
        if js:
            return self._df_from_json(js)
        else:
            return DataFrame(columns=["table_name"])

    def delete_from_dataset(self, dataset: str, query: str) -> requests.models.Response:
        """

        Args:
            dataset: Dataset to delete from  
            query: Filter which records to delete

        Returns:
            requests.models.Response
        """

        dataset = f"{self.whoami}_{dataset}"

        return self.delete(f'/{dataset}?{query}')

    def update_dataset(self, dataset: str, values: Dict, query: str) -> requests.models.Response:
        """

        Args:
            dataset: Dataset to be updated
            values: Values to update
            query: Filter which values to be updated

        Returns:
            requests.models.Response
        """

        dataset = f"{self.whoami}_{dataset}"

        self._raise_for_dataset_exists(dataset=dataset)

        if isinstance(values, Dict):
            if 'id' in values.keys():
                del values['id']
        else:
            raise ValueError(f'Format {type(values)} not supported.')

        return self.patch(f'/{dataset}?{query}', payload=values)

    # noinspection PyMethodMayBeStatic
    def _gdf_to_csv(self, gpdf: GeoDataFrame, crs: int = None) -> str:
        if crs is None:
            try:
                crs = gpdf.crs["init"].replace("epsg:", "")
            except Exception:
                raise ValueError("Could not guess the dataframe's crs. Please specify.")

        def add_srid(point):
            return f'SRID={str(crs)};' + str(point)

        gpdf['geometry'] = gpdf['geometry'].apply(add_srid)

        return gpdf.to_csv(header=True, index=False).lstrip()

    def insert_into_dataset(self, dataset: str, values: GeoDataFrame, upsert: bool = False, crs: int = None) \
            -> requests.models.Response:
        """

        Args:
            dataset: Dataset to be inserted to
            values: Values to be inserted
            upsert: Whether the insert shall replace existing rows (by PK)
            crs: crs (in the form of an SRID) of the geometries. If not present, thsi methid will attempt to guess it
            from the geodataframe input. Must be in sync with the target dataset in the GeoDatabase.

        Raises:
            ValueError: When crs is not given and cannot be guessed from dataframe

        Returns:
            requests.models.Response
        """

        values.columns = map(str.lower, values.columns)
        dataset = f"{self.whoami}_{dataset}"

        # self._dataset_exists(dataset=dataset)

        if isinstance(values, GeoDataFrame):
            headers = {'Content-type': 'text/csv'}

            if 'id' in values.columns and not upsert:
                values.drop(columns=['id'])

            values = self._gdf_to_csv(values, crs)
        else:
            raise ValueError(f'Format {type(values)} not supported.')

        if upsert:
            headers['Prefer'] = 'resolution=merge-duplicates'

        return self.post(f'/{dataset}', payload=values, headers=headers)

    def filter_by_bbox(self, dataset: str, minx, miny, maxx, maxy, bbox_mode: str = 'contains', bbox_crs: int = 4326,
                       limit: int = 0, offset: int = 0, namespace: Optional[str] = None) \
            -> Union[GeoDataFrame, str]:
        """

        Args:
            namespace:
            dataset: Table to filter
            minx: BBox minx (e.g. lon)
            miny: BBox miny (e.g. lat)
            maxx: BBox maxx
            maxy: BBox maxy
            bbox_mode: Filter mode. Can be 'contains' or 'within' ['contains']
            bbox_crs: Projection code. [4326]
            limit: Limit for paging
            offset: Offset (start) of rows to return. Used in combination with lmt.

        Returns:
            A GeoPandas Dataframe

        Raises:
            ValueError: When either the column ID or geometry or both are missing. Or the result is empty.
        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.filter_by_bbox(table="land_use",minx=452750.0, miny=88909.549, maxx=464000.0, maxy=102486.299, \
                bbox_mode="contains", bbox_crs=3794, lmt=1000, offst=10)
        """

        tab_prefix = namespace or self.whoami

        dataset = f"{tab_prefix}_{dataset}"

        self._raise_for_dataset_exists(dataset=dataset)
        self._raise_for_stored_procedure_exists('geodb_filter_by_bbox')

        headers = {'Accept': 'application/vnd.pgrst.object+json'}

        r = self.post('/rpc/geodb_filter_by_bbox', headers=headers, payload={
            "dataset": dataset,
            "minx": minx,
            "miny": miny,
            "maxx": maxx,
            "maxy": maxy,
            "bbox_mode": bbox_mode,
            "bbox_crs": bbox_crs,
            "limit": limit,
            "offset": offset
        })

        js = r.json()['src']
        if js:
            return self._df_from_json(js)
        else:
            return GeoDataFrame(columns=["Empty Result"])

    def filter(self, dataset: str, query: Optional[str] = None, namespace: Optional[str] = None) \
            -> Union[gpd.GeoDataFrame, DataFrame]:
        """

        Args:
            namespace:
            dataset: The dataset to be filtered
            query: A filter query using PostGrest style

        Returns:
            GeoDataFrame of results

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.filter('land_use', 'id=ge.1000')

        """

        tab_prefix = namespace or self.whoami
        dataset = f"{tab_prefix}_{dataset}"

        self._raise_for_dataset_exists(dataset=dataset)

        if query:
            r = self.get(f"/{dataset}?{query}")
        else:
            r = self.get(f"/{dataset}")

        js = r.json()

        if js:
            return self._df_from_json(js)
        else:
            return DataFrame(columns=["Empty Result"])

    def filter_raw(self, dataset: str, select: str = "*", where: Optional[str] = None,
                   group: Optional[str] = None, order: Optional[str] = None, limit: Optional[int] = None,
                   offset: Optional[int] = None, namespace: Optional[str] = None) -> Union[GeoDataFrame, DataFrame]:

        tab_prefix = namespace or self.whoami

        dataset = f"{tab_prefix}_{dataset}"

        self._raise_for_dataset_exists(dataset=dataset)
        self._raise_for_stored_procedure_exists('geodb_filter_raw')

        headers = {'Accept': 'application/vnd.pgrst.object+json'}

        qry = f'SELECT {select} FROM "{dataset}" '

        if where:
            qry += f'WHERE {where} '

        if group:
            qry += f'GROUP BY {group} '

        if order:
            qry += f'ORDER BY {order} '

        if limit:
            qry += f'LIMIT {limit}  '

        if limit and offset:
            qry += f'OFFSET {offset} '

        print(qry)

        r = self.post("/rpc/geodb_filter_raw", headers=headers, payload={'dataset': dataset, 'qry': qry})
        r.raise_for_status()

        js = r.json()['src']

        if js:
            return self._df_from_json(js)
        else:
            return DataFrame(columns=["Empty Result"])

    @property
    def geoserver_url(self):
        return f"{self._server_url}/geoserver"

    def get_catalog(self) -> object:
        from geoserver.catalog import Catalog

        return Catalog(self.geoserver_url + "/rest/", username=self._auth_client_id, password=self._auth_client_secret)

    def register_user_to_database(self, user_name: str, password: str):
        user = {
            "user_name": user_name,
            "password": password
        }

        return self.post(f'/rpc/geodb_register_user', payload=user)

    def register_user_to_geoserver(self, user_name: str, password: str):
        admin_user = os.environ.get("GEOSERVER_ADMIN_USER")
        admin_pwd = os.environ.get("GEOSERVER_ADMIN_PASSWORD")

        geoserver_url = f"{self.geoserver_url}/rest/security/usergroup/users"

        user = {
            "org.geoserver.rest.security.xml.JaxbUser": {
                "userName": user_name,
                "password": password,
                "enabled": True
            }
        }

        r = requests.post(geoserver_url, json=user, auth=(admin_user, admin_pwd))
        r.raise_for_status()

        from geoserver.catalog import Catalog

        cat = Catalog(self.geoserver_url + "/rest/", username="admin", password="geoserver")
        ws = cat.get_workspace(user_name)
        if not ws:
            cat.create_workspace(user_name)

        geoserver_url = f"{self.geoserver_url}/rest/workspaces/{user_name}/datastores"

        db = {
            "dataStore": {
                "name": user_name + "geodb",
                "connectionParameters": {
                    "entry": [
                        {"@key": "host", "$": "db-dcfs-geodb.cbfjgqxk302m.eu-central-1.rds.amazonaws.com"},
                        {"@key": "port", "$": "5432"},
                        {"@key": "database", "$": "geodb"},
                        {"@key": "user", "$": user_name},
                        {"@key": "passwd", "$": password},
                        {"@key": "dbtype", "$": "postgis"}
                    ]
                }
            }
        }

        r = requests.post(geoserver_url, json=db, auth=(admin_user, admin_pwd))
        r.raise_for_status()

        rule = {
            "org.geoserver.rest.security.xml.JaxbUser": {
                "rule": {
                    "@resource": "helge2.*.a",
                    "text": "GROUP_ADMIN"
                }
            }
        }

        geoserver_url = f"{self.geoserver_url}/rest/security/acl/layers"

        r = requests.post(geoserver_url, json=rule, auth=(admin_user, admin_pwd))
        r.raise_for_status()

    def _df_from_json(self, js: json) -> Union[GeoDataFrame, DataFrame]:
        """
        Converts wkb geometry string to wkt from a PostGrest json result
        Args:
            js: Json string. Will convert geometry to

        Returns:
            GeoDataFrame

        Raises:
            ValueError: When the geometry field is missing

        """
        data = [self._load_geo(d) for d in js]

        gpdf = gpd.GeoDataFrame(data)
        if 'geometry' in gpdf:
            return gpdf.set_geometry('geometry')
        else:
            return DataFrame(gpdf)

    def _get_full_url(self, path: str) -> str:
        """

        Args:
            path: PostGrest API path

        Returns:
            str: Full URL and path
        """
        if self._server_port:
            return f"{self._server_url}:{self._server_port}{path}"
        else:
            return f"{self._server_url}{path}"

    # noinspection PyMethodMayBeStatic
    def _load_geo(self, d: Dict) -> Dict:
        """

        Args:
            d: A row of thePostGrest result

        Returns:
            Dict: A row of thePostGrest result with its geometry converted from wkb to wkt
        """

        if 'geometry' in d:
            d['geometry'] = wkb.loads(d['geometry'], hex=True)
        return d

    def _set_defaults(self):
        self._server_url = GEODB_API_DEFAULT_PARAMETERS.get('server_url')
        self._server_port = GEODB_API_DEFAULT_PARAMETERS.get('server_port')
        self._auth_domain = GEODB_API_DEFAULT_PARAMETERS.get('auth_domain')
        self._auth_aud = GEODB_API_DEFAULT_PARAMETERS.get('auth_aud')
        self._auth_pub_client_id = GEODB_API_DEFAULT_PARAMETERS.get('auth_pub_client_id')
        self._auth_pub_client_secret = GEODB_API_DEFAULT_PARAMETERS.get('auth_pub_client_secret')

    def _set_from_env(self):
        self._server_url = os.getenv('GEODB_API_SERVER_URL') or self._server_url
        self._server_port = os.getenv('GEODB_API_SERVER_PORT') or self._server_port
        self._auth_client_id = os.getenv('GEODB_AUTH_CLIENT_ID') or self._auth_client_id
        self._auth_client_secret = os.getenv('GEODB_AUTH_CLIENT_SECRET') or self._auth_client_secret
        self._auth_pub_client_id = os.getenv('GEODB_AUTH_PUB_CLIENT_ID') or self._auth_pub_client_id
        self._auth_pub_client_secret = os.getenv('GEODB_AUTH_PUB_CLIENT_SECRET') or self._auth_pub_client_secret
        self._auth_domain = os.getenv('GEODB_AUTH_DOMAIN') or self._auth_domain
        self._auth_aud = os.getenv('GEODB_AUTH_AUD') or self._auth_aud

    def _get_geodb_api_access_token(self):
        data = self._get_geodb_api_access_token_data()
        return data['access_token']

    @property
    def auth_access_token(self):
        if self._auth_access_token is not None:
            return self._auth_access_token

        return self._get_geodb_api_access_token()

    def _get_geodb_api_access_token_data(self):
        if self._auth_access_token is not None:
            return
        client_id = self._auth_pub_client_id if self._is_public_client else self._auth_client_id
        client_secret = self._auth_pub_client_secret if self._is_public_client else self._auth_client_secret

        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "audience": self._auth_aud,
            "grant_type": "client_credentials"
        }

        headers = {'content-type': "application/json"}

        r = requests.post(self._auth_domain + "/oauth/token", json=payload, headers=headers)
        r.raise_for_status()

        return r.json()

    # noinspection PyMethodMayBeStatic
    def _validate(self, df: gpd.GeoDataFrame) -> bool:
        """

        Args:
            df: A geopands dataframe to validate columns. Must be "raba_pid", 'raba_id', 'd_od' or 'geometry'

        Returns:
            bool whether validation succeeds
        """
        cols = set([x.lower() for x in df.columns])
        valid_columns = {'id', 'geometry'}

        return len(list(valid_columns - cols)) == 0

    def _raise_for_dataset_exists(self, dataset: str) -> bool:
        """

        Args:
            dataset: A table name to check

        Returns:
            bool whether the table exists

        """
        if dataset in self.capabilities['definitions']:
            return True
        else:
            raise ValueError(f"Dataset {dataset} does not exist")

    def _raise_for_stored_procedure_exists(self, stored_procedure: str) -> bool:
        """

        Args:
            stored_procedure: Name of stored pg procedure

        Returns:
            bool whether the stored procedure exists in DB
        """
        if f"/rpc/{stored_procedure}" in self.capabilities['paths']:
            return True
        else:
            raise ValueError(f"Stored procedure {stored_procedure} does not exist")

    def setup(self):
        host = os.getenv('GEODB_DB_HOST')
        port = os.getenv('GEODB_DB_PORT')
        user = os.getenv('GEODB_DB_USER')
        passwd = os.getenv('GEODB_DB_PASSWD')
        dbname = os.getenv('GEODB_DB_DBNAME')
        conn = psycopg2.connect(host=host, port=port, user=user, password=passwd, dbname=dbname)
        cursor = conn.cursor()

        with open('dcfs_geodb/sql/manage_users.sql') as sql_file:
            sql_create = sql_file.read()
            cursor.execute(sql_create)

        with open('dcfs_geodb/sql/get_by_bbox.sql') as sql_file:
            sql_create = sql_file.read()
            cursor.execute(sql_create)

        with open('dcfs_geodb/sql/manage_properties.sql') as sql_file:
            sql_create = sql_file.read()
            cursor.execute(sql_create)

        with open('dcfs_geodb/sql/manage_table.sql') as sql_file:
            sql_create = sql_file.read()
            cursor.execute(sql_create)

        conn.commit()
        conn.commit()
