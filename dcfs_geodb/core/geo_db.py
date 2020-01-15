import http.client
import os
from typing import Dict, Optional, Union, Sequence

import fastjsonschema
import geopandas as gpd
import psycopg2
from geopandas import GeoDataFrame
from shapely import wkb
import requests
import json
from dotenv import load_dotenv, find_dotenv

from dcfs_geodb.defaults import GEODB_API_DEFAULT_CONNECTION_PARAMETERS, JSON_API_VALIDATIONS_CREATE_DATASET


class GeoDBError(ValueError):
    pass


class GeoDB(object):
    def __init__(self, server_url: Optional[str] = None,
                 server_port: Optional[int] = None,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None):
        """

        Args:
            server_url: The URL of the server
            server_port: The server port
        """
        self._dotenv_file = find_dotenv()
        if self._dotenv_file:
            load_dotenv(self._dotenv_file)

        self._set_defaults()
        self._set_from_env()

        self._server_url = server_url or self._server_url
        self._server_port = server_port or self._server_port
        self._auth_client_id = client_id or self._auth_client_id
        self._auth_client_secret = client_secret or self._auth_client_secret

        self._capabilities = None
        self._geodb_api_access_token = self._get_geodb_api_access_token()

        self._whoami = None

    @property
    def capabilities(self) -> json:
        """
            Get a list of WebAPI endpoints.
            Returns:
                A json object containing all endpoints and capability information about the PostGrest API.
        """

        if self._capabilities is None:
            r = self.get('/')
            r.raise_for_status()

            self._capabilities = r.json()

        return self._capabilities

    def get_dataset_info(self, dataset: str):
        capabilities = self.capabilities
        if dataset in capabilities['definitions']:
            return capabilities['definitions'][dataset]
        else:
            raise ValueError(f"Table {dataset} does not exist.")

    @property
    def common_headers(self):
        if self._get_geodb_api_access_token():
            return {
                'Prefer': 'return=representation',
                'Content-type': 'application/json',
                'Authorization': f"Bearer {self._geodb_api_access_token}"
            }
        else:
            return {
                'Prefer': 'return=representation',
                'Content-type': 'application/json',
            }

    @property
    def whoami(self):
        if self._whoami is None:
            self._whoami = self.get(path='/rpc/geodb_whoami').json()

        return self._whoami

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
            >>> geodb = GeoDB()
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
            raise GeoDBError(r.json()['message'])

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
            >>> geodb = GeoDB()
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
            >>> geodb = GeoDB()
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
            >>> geodb = GeoDB()
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
            >>> geodb = GeoDB()
            >>> geodb.create_dataset(datasets=['myDataset1', 'myDataset2'])
        """
        validate = fastjsonschema.compile(
            JSON_API_VALIDATIONS_CREATE_DATASET['validation'],
            formats=JSON_API_VALIDATIONS_CREATE_DATASET['formats']
        )

        # validate(datasets)

        self._capabilities = None

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
            >>> geodb = GeoDB()
            >>> geodb.create_dataset(dataset='myDataset')
        """

        dataset = {'name': dataset, 'properties': properties, 'crs': crs}

        self._capabilities = None

        return self.create_datasets([dataset])

    def drop_dataset(self, dataset: str) -> requests.models.Response:
        """

        Args:
            dataset: Dataset to be dropped

        Returns:
            requests.models.Response:

        Examples:
            >>> geodb = GeoDB()
            >>> geodb.drop_dataset(dataset='myDataset')
        """

        self._capabilities = None

        return self.post(path='/rpc/geodb_drop_datasets', payload={'datasets': [dataset]})

    def drop_datasets(self, datasets: Sequence[str]) -> requests.models.Response:
        """

        Args:
            datasets: Datasets to be dropped

        Returns:
            requests.models.Response:

        Examples:
            >>> geodb = GeoDB()
            >>> geodb.drop_datasets(datasets=['myDataset1', 'myDaraset2'])
        """

        self._capabilities = None

        return self.post(path='/rpc/geodb_drop_datasets', payload={'datasets': datasets})

    def publish_dataset(self, dataset: str):
        dataset = f"{self.whoami}_{dataset}"

        return self.post(path='/rpc/geodb_publish_dataset', payload={'dataset': dataset})

    def unpublish_dataset(self, dataset: str):
        dataset = f"{self.whoami}_{dataset}"

        return self.post(path='/rpc/geodb_unpublish_dataset', payload={'dataset': dataset})

    def add_property(self, dataset: str, prop: str, typ: str) -> requests.models.Response:
        """
        Add a property to an existing dataset
        Args:
            dataset: Dataset to add a property to
            prop: Property name
            typ: Type of property

        Examples:
            >>> prop = {}
            >>> geodb = GeoDB()
            >>> geodb.add_property(dataset='myDataset', prop=prop)
        """

        prop = {'name': prop, 'type': typ}

        dataset = f"{self.whoami}_{dataset}"

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
            >>> geodb = GeoDB()
            >>> geodb.add_property(dataset='myDataset', prop=[prop])
        """

        dataset = f"{self.whoami}_{dataset}"

        validate = fastjsonschema.compile(
            JSON_API_VALIDATIONS_CREATE_DATASET['validation'],
            formats=JSON_API_VALIDATIONS_CREATE_DATASET['formats']
        )
        # validate(properties)

        return self.post(path='/rpc/geodb_add_properties', payload={'dataset': dataset, 'properties': properties})

    def drop_property(self, dataset: str, prop: str) -> requests.models.Response:
        """

        Args:
            dataset: Dataset to drop the property from
            prop: Property to delete

        Returns:
            requests.models.Response:

        Examples:
            >>> geodb = GeoDB()
            >>> geodb.drop_property(dataset='myDataset', prop='myProperty')
        """

        dataset = f"{self.whoami}_{dataset}"

        return self.drop_properties(dataset=dataset, properties=[prop])

    def drop_properties(self, dataset: str, properties: Union[Sequence[Dict], Sequence[str]]) \
            -> requests.models.Response:
        """

        Args:
            dataset: Dataset to delete properties from
            properties: A json object containing the property definitions

        Returns:
            requests.models.Response:

        Examples:
            >>> geodb = GeoDB()
            >>> geodb.drop_properties(dataset='myDataset', properties=['myProperty1', 'myProperty2'])
        """

        dataset = f"{self.whoami}_{dataset}"

        if not self._stored_procedure_exists('geodb_drop_properties'):
            raise ValueError(f"Stored procedure geodb_drop_properties does not exist")

        if not self._dataset_exists(dataset=dataset):
            raise ValueError(f"Dataset {dataset} does not exist")

        validate = fastjsonschema.compile(
            JSON_API_VALIDATIONS_CREATE_DATASET['validation'],
            formats=JSON_API_VALIDATIONS_CREATE_DATASET['formats']
        )
        # validate(properties)

        return self.post(path='/rpc/geodb_drop_properties', payload={'dataset': dataset, 'properties': properties})

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

        if not self._dataset_exists(dataset=dataset):
            raise ValueError(f"Dataset {dataset} does not exist")

        if isinstance(values, Dict):
            if 'id' in values.keys():
                del values['id']
        else:
            raise ValueError(f'Format {type(values)} not supported.')

        return self.patch(f'/{dataset}?{query}', payload=values)

    def _gdf_to_csv(self, gpdf: GeoDataFrame, crs: int = None) -> str:
        if crs is None:
            try:
                crs = gpdf.crs["init"].replace("epsg:", "")
            except Exception:
                raise ValueError("Could not guess the dataframe's crs. Please specify.")

        add_srid = lambda point: f'SRID={str(crs)};' + str(point)
        # return shapely.wkb.dumps(line, include_srid=True)

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

        dataset = f"{self.whoami}_{dataset}"

        #if not self._dataset_exists(dataset=dataset):
        #    raise ValueError(f"Dataset {dataset} does not exist")

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
            -> gpd.GeoDataFrame:
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
            >>> geodb = GeoDB()
            >>> geodb.filter_by_bbox(table="land_use",minx=452750.0, miny=88909.549, maxx=464000.0, maxy=102486.299, \
                bbox_mode="contains", bbox_crs=3794, lmt=1000, offst=10)
        """

        tab_prefix = namespace or self.whoami

        dataset = f"{tab_prefix}_{dataset}"

        if not self._dataset_exists(dataset=dataset):
            raise ValueError(f"Dataset {dataset} does not exist")

        if not self._stored_procedure_exists('geodb_filter_by_bbox'):
            raise ValueError(f"Stored procedure geodb_filter_by_bbox does not exist")

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
            data = [self._load_geo(d) for d in js]

            gpdf = gpd.GeoDataFrame(data).set_geometry('geometry')
            if not self._validate(gpdf):
                raise ValueError("Geometry or ID field is missing")
            return gpdf
        else:
            raise ValueError("Result is empty")

    def filter(self, dataset: str, query: str, fmt='postgrest', namespace: Optional[str] = None) -> GeoDataFrame:
        """

        Args:
            namespace:
            dataset: The dataset to be filtered
            query: A filter query using PostGrest style

        Returns:
            GeoDataFrame of results

        Examples:
            >>> geodb = GeoDB()
            >>> geodb.filter('land_use', 'id=ge.1000')

        """

        tab_prefix = namespace or self.whoami
        dataset = f"{tab_prefix}_{dataset}"

        if not self._dataset_exists(dataset=dataset):
            raise ValueError(f"Dataset {dataset} does not exist")

        if fmt == "postgrest":
            r = self.get(f"/{dataset}?{query}")
        elif fmt == "raw":
            headers = {'Accept': 'application/vnd.pgrst.object+json'}
            r = self.post("/rpc/geodb_filter_raw", headers=headers, payload={'dataset': dataset, 'qry': query})
        else:
            raise ValueError("Error: Query format not known.")

        js = r.json()

        if js:
            return self._gpdf_from_json(js)
        else:
            raise ValueError("Result is empty")

    def _gpdf_from_json(self, js: json) -> GeoDataFrame:
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
            gpdf = gpdf.set_geometry('geometry')

        if not self._validate(gpdf):
            raise ValueError("Geometry field is missing")

        return gpdf

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
        self._server_url = GEODB_API_DEFAULT_CONNECTION_PARAMETERS.get('server_url') or self._server_url
        self._server_port = GEODB_API_DEFAULT_CONNECTION_PARAMETERS.get('server_port') or self._server_port
        self._auth_domain = GEODB_API_DEFAULT_CONNECTION_PARAMETERS.get('auth_domain') or self._auth_domain
        self._auth_aud = GEODB_API_DEFAULT_CONNECTION_PARAMETERS.get('auth_aud') or self._auth_aud

    def _set_from_env(self):
        self._server_url = os.getenv('GEODB_API_SERVER_URL') or self._server_url
        self._server_port = os.getenv('GEODB_API_SERVER_PORT') or self._server_port
        self._auth_client_id = os.getenv('GEODB_AUTH_CLIENT_ID') or self._auth_client_id
        self._auth_client_secret = os.getenv('GEODB_AUTH_CLIENT_SECRET') or self._auth_client_secret
        self._auth_domain = os.getenv('GEODB_AUTH_DOMAIN') or self._auth_domain
        self._auth_aud = os.getenv('GEODB_AUTH_AUD') or self._auth_aud

    def _get_geodb_api_access_token(self):
        if self._auth_client_id == "ANONYMOUS":
            return None

        conn = http.client.HTTPSConnection(self._auth_domain)
        payload = {
            "client_id": self._auth_client_id,
            "client_secret": self._auth_client_secret,
            "audience": self._auth_aud,
            "grant_type": "client_credentials"
        }

        headers = {'content-type': "application/json"}

        conn.request("POST", "/oauth/token", json.dumps(payload), headers)

        res = conn.getresponse()
        data = json.loads(res.read().decode("utf-8"))
        return data['access_token']

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

    def _dataset_exists(self, dataset: str) -> bool:
        """

        Args:
            dataset: A table name to check

        Returns:
            bool whether the table exists

        """
        return dataset in self.capabilities['definitions']

    def _stored_procedure_exists(self, stored_procedure: str) -> bool:
        """

        Args:
            stored_procedure: Name of stored pg procedure

        Returns:
            bool whether the stored procedure exists in DB
        """
        return f"/rpc/{stored_procedure}" in self.capabilities['paths']

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
