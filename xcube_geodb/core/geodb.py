import logging
import os
from typing import Dict, Optional, Union, Sequence, Tuple

import geopandas as gpd
from geopandas import GeoDataFrame
from pandas import DataFrame
from shapely import wkb
import requests
import json
from dotenv import load_dotenv, find_dotenv

from xcube_geodb.core.collections import Collections
from xcube_geodb.core.message import Message
from xcube_geodb.defaults import GEODB_API_DEFAULT_PARAMETERS

LOGGER = logging.getLogger("geodb.core")
logging.basicConfig(level=logging.INFO)


class GeoDBError(ValueError):
    pass


class GeoDBClient(object):
    minx = 0
    maxx = 1
    miny = 2
    maxy = 3

    def __init__(self,
                 namespace: Optional[str] = None,
                 server_url: Optional[str] = None,
                 server_port: Optional[int] = None,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 access_token: Optional[str] = None,
                 anonymous: bool = False,
                 dotenv_file: str = ".env",
                 auth_mode: str = 'silent'):
        """

        Args:
            namespace (str): defaults to user name at this stage
            server_url (str): The URL of the PostGrest Rest API service
            server_port (str): The port to the PostGrest Rest API service
            dotenv_file (str): Name of the dotenv file [.env] to set client IDs and secrets
            anonymous (bool): Whether the client connection is anonymous (without credentials) [False]
            client_secret (str): Client secret (overrides environment variables)
            client_id (str): Client ID (overrides environment variables)
            auth_mode (str): Authentication modus [silent]. Can be 'silent' and 'interactive'
        """

        self._dotenv_file = dotenv_file
        self._auth_mode = None
        self._namespace = namespace
        self._auth_pub_client_id = "nF1s2D2fs770KLCY16zVk3i5nuqK6Ptx"
        self._auth_pub_client_secret = "WQtMECNYySz-1KTM6acEr_SJlped6QU6qxLyo4ahupLTqlfb4zu2Z27PbrwmEMqU"

        self._server_url = None
        self._server_port = None
        self._auth_client_id = None
        self._auth_client_secret = None
        self._auth_access_token = None
        self._auth0_config_file = None
        self._auth0_config_folder = '.'
        self._auth_domain = "https://edc.eu.auth0.com"
        self._auth_aud = None

        self.refresh_config_from_env(dotenv_file=dotenv_file, use_dotenv=True)

        self._server_url = server_url or self._server_url
        self._server_port = server_port or self._server_port
        self._auth_client_id = client_id or self._auth_client_id
        self._auth_client_secret = client_secret or self._auth_client_secret
        self._auth_access_token = access_token
        self._auth_mode = auth_mode or self._auth_mode

        self._capabilities = None
        self._is_public_client = anonymous

        self._whoami = None
        self._log_level = logging.INFO
        self._ipython_shell = None
        LOGGER.setLevel(level=self._log_level)

        self._mandatory_properties = ["geometry", "id", "created_at", "modified_at"]

        if auth_mode not in ('interactive', 'silent'):
            raise ValueError("auth_mode can only be 'interactive' or 'silent'!")

        if self._auth_mode == "interactive":
            self._auth_login()

    def get_collection_info(self, collection: str) -> Dict:
        """

        Args:
            collection (str): The name of teh collection to inspect

        Returns:
            A dictionary with collection information
        """
        capabilities = self.capabilities
        collection = self.namespace + '_' + collection

        if collection in capabilities['definitions']:
            return capabilities['definitions'][collection]
        else:
            raise ValueError(f"Table {collection} does not exist.")

    def _get_common_headers(self):
        return {
            'Prefer': 'return=representation',
            'Content-type': 'application/json',
            'Authorization': f"Bearer {self.auth_access_token}"
        }

    @property
    def namespace(self) -> str:
        """
        Returns:
            The currentnamespace
        """
        return self._namespace or self.whoami

    @property
    def whoami(self) -> str:
        """

        Returns:
            The current database user
        """
        return self._whoami or self._get(path='/rpc/geodb_whoami').json()

    @property
    def capabilities(self) -> Dict:
        """

        Returns:
            A dictionary of the PostGrest REST API service's capabilities
        """
        return self._capabilities or self._get(path='/').json()

    def _auth_login(self):
        self._auth0_login()

    def _auth0_login(self):
        try:
            from ipyauth import ParamsAuth0, Auth
            import IPython
            from IPython.display import display
        except ImportError:
            raise GeoDBError("You need to install IPython and ipyauth dependencies")

        auth0_config_file = os.environ.get('GEODB_AUTH0_CONFIG_FILE') or 'ipyauth-auth0-demo.env'
        auth0_config_folder = os.environ.get('GEODB_AUTH0_CONFIG_FOLDER') or '.'

        if not os.path.isfile(os.path.join(auth0_config_folder, auth0_config_file)):
            raise FileExistsError("Mandatory auth configuration file ipyauth-auth0-demo.env must exist")

        auth_params = ParamsAuth0(dotenv_file=auth0_config_file, dotenv_folder=auth0_config_folder)
        auth = Auth(params=auth_params)

        self._ipython_shell = IPython.get_ipython()

        if self._ipython_shell is None:
            raise ValueError("You do not seem to be in an interactive ipython session. Interactive login cannot "
                             "be used.")

        self._ipython_shell.push({'__auth__': auth}, interactive=True)
        # noinspection PyTypeChecker
        display(auth)

    def _refresh_capabilities(self):
        self._capabilities = None

    def refresh_config_from_env(self, dotenv_file: str = ".env", use_dotenv: bool = False):
        """

        Args:
            dotenv_file: A dotenv config file
            use_dotenv: Whether to use dotenv. Might be useful if the configuration is set externally.
        """
        if use_dotenv:
            self._dotenv_file = find_dotenv(filename=dotenv_file)
            if self._dotenv_file:
                load_dotenv(self._dotenv_file)
        self._set_from_env()

    def post(self, path: str, payload: Union[Dict, Sequence], params: Optional[Dict] = None,
             headers: Optional[Dict] = None) -> requests.models.Response:

        """

        Args:
            headers: Request headers. Allows Overriding common header entries.
            path: API path
            payload: Post body as Dict. Will be dumped to JSON
            params: Request parameters

        Returns:
            requests.models.Response: A Request object

        Raises:
            GeoDBError: If the database raises an error
            HttpError: If the request fails
        """

        common_headers = self._get_common_headers()

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

    def _get(self, path: str, params: Optional[Dict] = None,
             headers: Optional[Dict] = None) -> requests.models.Response:
        """

        Args:
            headers: Request headers. Allows Overriding common header entries.
            path: API path
            params: Request parameters

        Returns:
            requests.models.Response: A Request object

        Raises:
            GeoDBError: If the database raises an error
            HttpError: If the request fails
        """

        common_headers = self._get_common_headers()
        headers = common_headers.update(headers) if headers else self._get_common_headers()

        r = None
        try:
            r = requests.get(self._get_full_url(path=path), params=params, headers=headers)
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            raise GeoDBError(r.json()['message'])

        return r

    def _delete(self, path: str, params: Optional[Dict] = None, headers: Optional[Dict] = None) \
            -> requests.models.Response:
        """

        Args:
            headers: Request headers. Allows Overriding common header entries.
            path: API path
            params: Request parameters

        Returns:
            requests.models.Response: A Request object

        Raises:
            GeoDBError: If the database raises an error
            HttpError: If the request fails
        """

        common_headers = self._get_common_headers()
        headers = common_headers.update(headers) if headers else self._get_common_headers()

        r = None
        try:
            r = requests.delete(self._get_full_url(path=path), params=params, headers=headers)
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            raise GeoDBError(r.json()['message'])
        return r

    def _patch(self, path: str, payload: Union[Dict, Sequence], params: Optional[Dict] = None,
               headers: Optional[Dict] = None) -> requests.models.Response:
        """

        Args:
            headers: Request headers. Allows Overriding common header entries.
            payload:
            path: API path
            params: Request parameters

        Returns:
            requests.models.Response: A Request object

        Raises:
            GeoDBError: If the database raises an error
            HttpError: If the request fails
        """

        common_headers = self._get_common_headers()
        headers = common_headers.update(headers) if headers else self._get_common_headers()

        r = None
        try:
            r = requests.patch(self._get_full_url(path=path), json=payload, params=params, headers=headers)
            r.raise_for_status()
        except requests.HTTPError:
            raise GeoDBError(r.json()['message'])
        return r

    def create_collections(self, collections: Dict) -> Collections:
        """

        Args:
            collections: A dictionalry of collections

        Returns:
            bool: Success

        Examples:

            >>> geodb = GeoDBClient()
            >>> collections = {'[MyCollection]': {'crs': 1234, 'properties': {'[MyProp1]': 'float', '[MyProp2]': 'date'}}}
            >>> geodb.create_collections(collections)
        """

        self._refresh_capabilities()

        collections = {"collections": collections}
        self.post(path='/rpc/geodb_create_collections', payload=collections)

        self._log(f"Collections {str(collections)} added.", level=logging.DEBUG)
        return Collections(collections['collections'])

    def create_collection(self, collection: str, properties: Dict, crs: int = 4326) -> Collections:
        """

        Args:
            collection: Collection to be created
            properties: Property definitions for the collection
            crs:

        Returns:
            bool: Success

        Examples:
            >>> geodb = GeoDBClient()
            >>> properties = {'[MyProp1]': 'float', '[MyProp2]': 'date'}
            >>> geodb.create_collection(collection='[MyCollection]', crs=3794, properties=properties)
        """

        collection = dict(name=collection, properties=properties, crs=str(crs))

        self._refresh_capabilities()

        return self.create_collections(collection)

    def drop_collection(self, collection: str) -> Message:
        """

        Args:
            collection: Collection to be dropped

        Returns:
            bool: Success

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.drop_collection(collection='[MyCollection]')
        """

        return self.drop_collections([collection])

    def drop_collections(self, collections: Sequence[str]) -> Message:
        """

        Args:
            collections: Collections to be dropped

        Returns:
            Message

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.drop_collections(collections=['[MyCollection1]', '[MyCollection2]'])
        """

        self._refresh_capabilities()

        self.post(path='/rpc/geodb_drop_collections', payload={'collections': collections})

        self._log(f"Collection {str(collections)} deleted", level=logging.DEBUG)
        return Message(f"Collection {str(collections)} deleted")

    def grant_access_to_collection(self, collection: str, user: str = "public") -> Message:
        """

        Args:
            collection: Collection to grant access to
            user: The namespace to grant access to [public]. By default, public access is granted

        Returns:
            bool: Success
        """
        dn = f"{self.namespace}_{collection}"

        self.post(path='/rpc/geodb_grant_access_to_collection', payload={'collection': dn, 'usr': user})

        self._log(message=f"Access granted on {collection} to {user}", level=logging.DEBUG)

        return Message(f"Access granted on {collection} to {user}")

    def revoke_access_from_collection(self, collection: str, user: str = 'public') -> Message:
        """

        Args:
            collection: Collection to grant access to
            user: The user to revoke access from [public].

        Returns:
            bool: Success
        """
        dn = f"{self.namespace}_{collection}"

        self.post(path='/rpc/geodb_revoke_access_to_collection', payload={'collection': dn, 'usr': user})

        self._log(f"Access revoked from {collection} of {user}", level=logging.DEBUG)

        return Message(f"Access revoked from {collection} of {user}")

    def list_grants(self) -> Sequence:
        """

        Returns:
            A list of the current user's collection grants

        """
        r = self.post(path='/rpc/geodb_list_grants', payload={})
        if r.json()[0]['src'] is None:
            return []
        else:
            return r.json()[0]['src']

    def add_property(self, collection: str, prop: str, typ: str) -> Message:
        """
        Add a property to an existing collection
        Args:
            collection: Collection to add a property to
            prop: Property name
            typ: Type of property (Postgres type)

        Returns:
            Success Message

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.add_property(collection='[MyCollection]', name='[MyProperty]', type='[PostgresType]')
        """

        prop = {prop: typ}

        return self.add_properties(collection=collection, properties=prop)

    def add_properties(self, collection: str, properties: Dict) -> Message:
        """

        Args:
            collection: Collection to add properties to
            properties: Property definitions as json array

        Returns:
            bool: Success

        Examples:
            >>> properties = {'[MyName1]': '[PostgresType1]', '[MyName2]': '[PostgresType2]'}
            >>> geodb = GeoDBClient()
            >>> geodb.add_property(collection='[MyCollection]', properties=properties)
        """

        self._refresh_capabilities()

        self.post(path='/rpc/geodb_add_properties', payload={'collection': collection, 'properties': properties})

        self._log(f"Properties added", level=logging.DEBUG)

        return Message(f"Properties added")

    def drop_property(self, collection: str, prop: str) -> Message:
        """

        Args:
            collection: Collection to drop the property from
            prop: Property to delete

        Returns:
            bool: Success

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.drop_property(collection='[MyCollection]', prop='[MyProperty]')
        """

        return self.drop_properties(collection=collection, properties=[prop])

    def drop_properties(self, collection: str, properties: Sequence[str]) -> Message:
        """

        Args:
            collection: Collection to delete properties from
            properties: A json object containing the property definitions

        Returns:
            bool: Success

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.drop_properties(collection='[MyCollection]', properties=['[MyProperty1]', '[MyProperty2]'])
        """

        self._refresh_capabilities()

        self._raise_for_mandatory_columns(properties)

        self._raise_for_stored_procedure_exists('geodb_drop_properties')

        self.post(path='/rpc/geodb_drop_properties', payload={'collection': collection, 'properties': properties})

        self._log(f"Properties {str(properties)} dropped from {collection}", level=logging.DEBUG)

        return Message(f"Properties {str(properties)} dropped from {collection}")

    def _raise_for_mandatory_columns(self, properties: Sequence[str]):
        common_props = list(set(properties) & set(self._mandatory_properties))
        if len(common_props) > 0:
            raise ValueError("Don't delete the following columns: " + str(common_props))

    def get_properties(self, collection: str) -> DataFrame:
        """

        Args:
            collection: Collection to retrieve a list of properties from

        Returns:
            DataFrame: A list of properties

        """
        r = self.post(path='/rpc/geodb_get_properties', payload={'collection': collection})

        js = r.json()[0]['src']

        if js:
            return self._df_from_json(js)
        else:
            return DataFrame(columns=["table_name", "column_name", "data_type"])

    def get_collections(self) -> DataFrame:
        """

        Returns:
            DataFrame: A list of collections the user owns

        """
        r = self.post(path='/rpc/geodb_list_collections', payload={})

        js = r.json()[0]['src']
        if js:
            return self._df_from_json(js)
        else:
            return DataFrame(columns=["table_name"])

    def delete_from_collection(self, collection: str, query: str) -> Message:
        """

        Args:
            collection: Collection to delete from  
            query: Filter which records to delete. Follow the http://postgrest.org/en/v6.0/api.html query convention.

        Returns:
            bool: Success

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.delete_from_collection('[MyCollection]', 'id=eq.1')
        """

        dn = f"{self.whoami}_{collection}"

        self._delete(f'/{dn}?{query}')

        self._log(f"Data from {collection} deleted", level=logging.DEBUG)

        return Message(f"Data from {collection} deleted")

    def update_collection(self, collection: str, values: Dict, query: str) -> Message:
        """

        Args:
            collection: Collection to be updated
            values: Values to update
            query: Filter which values to be updated. Follow the http://postgrest.org/en/v6.0/api.html query convention.

        Returns:
            bool: Success
        """

        dn = f"{self.whoami}_{collection}"

        self._raise_for_collection_exists(collection=dn)

        if isinstance(values, Dict):
            if 'id' in values.keys():
                del values['id']
        else:
            raise ValueError(f'Format {type(values)} not supported.')

        self._patch(f'/{dn}?{query}', payload=values)

        self._log(f"{collection} updated", level=logging.DEBUG)

        return Message(f"{collection} updated")

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

    def insert_into_collection(self, collection: str, values: GeoDataFrame, upsert: bool = False, crs: int = None) \
            -> Message:
        """

        Args:
            collection: Collection to be inserted to
            values: Values to be inserted
            upsert: Whether the insert shall replace existing rows (by PK)
            crs: crs (in the form of an SRID) of the geometries. If not present, tssi method will attempt to guess it
            from the GeoDataFrame input. Must be in sync with the target collection in the GeoDatabase

        Raises:
            ValueError: When crs is not given and cannot be guessed from the GeoDataFrame

        Returns:
            bool: Success
        """

        # self._collection_exists(collection=collection)

        if isinstance(values, GeoDataFrame):
            headers = {'Content-type': 'text/csv'}

            if 'id' in values.columns and not upsert:
                values.drop(columns=['id'])

            values.columns = map(str.lower, values.columns)
            values = self._gdf_to_csv(values, crs)
        else:
            raise ValueError(f'Format {type(values)} not supported.')

        dn = f"{self.namespace}_{collection}"

        if upsert:
            headers['Prefer'] = 'resolution=merge-duplicates'

        self.post(f'/{dn}', payload=values, headers=headers)

        self._log(f"Data inserted into {collection}", level=logging.DEBUG)

        return Message(f"Data inserted into {collection}")

    def get_collection_by_bbox(self, collection: str,
                               bbox: Tuple[float, float, float, float],
                               comparison_mode: str = 'contains',
                               bbox_crs: int = 4326,
                               limit: int = 0,
                               offset: int = 0,
                               where: Optional[str] = None,
                               op: str = 'AND',
                               namespace: Optional[str] = None) -> GeoDataFrame:
        """

        Args:
            collection: Table to get
            bbox (int, int, int, int): minx, maxx, miny, maxy
            comparison_mode: Filter mode. Can be 'contains' or 'within' ['contains']
            bbox_crs: Projection code. [4326]
            op: Operator for where (AND, OR) ['AND']
            where: Additional SQL where statement
            limit: Limit for paging
            offset: Offset (start) of rows to return. Used in combination with lmt.
            namespace: By default the API gets in the user's own namespace. To access
                       collections the user has grant set the namespace accordingly.

        Returns:
            A GeoPandas Dataframe

        Raises:
            HttpError: When the database raises an error

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.get_collection_by_bbox(table="[MyCollection]", bbox=(452750.0, 88909.549, 464000.0, \
                102486.299), comparison_mode="contains", bbox_crs=3794, limit=10, offset=10)
        """

        tab_prefix = namespace or self.whoami

        dn = f"{tab_prefix}_{collection}"

        self._raise_for_collection_exists(collection=dn)
        self._raise_for_stored_procedure_exists('geodb_get_by_bbox')

        headers = {'Accept': 'application/vnd.pgrst.object+json'}

        r = self.post('/rpc/geodb_get_by_bbox', headers=headers, payload={
            "collection": dn,
            "minx": bbox[GeoDBClient.minx],
            "miny": bbox[GeoDBClient.miny],
            "maxx": bbox[GeoDBClient.maxx],
            "maxy": bbox[GeoDBClient.maxy],
            "bbox_mode": comparison_mode,
            "bbox_crs": bbox_crs,
            "limit": limit,
            "where": where,
            "op": op,
            "offset": offset
        })

        js = r.json()['src']
        if js:
            return self._df_from_json(js)
        else:
            return GeoDataFrame(columns=["Empty Result"])

    def get_collection(self, collection: str, query: Optional[str] = None, namespace: Optional[str] = None) \
            -> Union[GeoDataFrame, DataFrame]:
        """

        Args:
            collection: The collection's name
            query: A query. Follow the http://postgrest.org/en/v6.0/api.html query convention.
            namespace: By default the API gets in the user's own namespace. To access
                       collections the user has grant set the namespace accordingly.

        Returns:
            GeoDataFrame or DataFrame: results

        Raises:
            HttpError: When the database raises an error

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.get_collection(collection='[MyCollection]', query='id=ge.1000')

        """

        tab_prefix = namespace or self.whoami
        dn = f"{tab_prefix}_{collection}"

        self._raise_for_collection_exists(collection=dn)

        if query:
            r = self._get(f"/{dn}?{query}")
        else:
            r = self._get(f"/{dn}")

        js = r.json()

        if js:
            return self._df_from_json(js)
        else:
            return DataFrame(columns=["Empty Result"])

    def get_collection_pg(self, collection: str, select: str = "*", where: Optional[str] = None,
                          group: Optional[str] = None, order: Optional[str] = None, limit: Optional[int] = None,
                          offset: Optional[int] = None, namespace: Optional[str] = None) \
            -> Union[GeoDataFrame, DataFrame]:
        """

        Args:
            collection: Collection to query
            select: Properties (columns) to return. Can contain aggregation functions
            where: SQL WHERE statement
            group: SQL GROUP statement
            order: SQL ORDER statement
            limit: Limit for paging
            offset: Offset (start) of rows to return. Used in combination with limit.
            namespace: By default the API gets in the user's own namespace. To access
                       collections the user has grant set the namespace accordingly.

        Returns:
            GeoDataFrame or DataFrame: results

        Raises:
            HttpError: When the database raises an error

        Examples:
            >>> geodb = GeoDBClient()
            >>> df = geodb.get_collection_pg(collection='[MyCollection]', where='raba_id=1410', group='d_od', \
                select='COUNT(d_od) as ct, d_od')
        """
        tab_prefix = namespace or self.namespace

        dn = f"{tab_prefix}_{collection}"

        self._raise_for_collection_exists(collection=dn)
        self._raise_for_stored_procedure_exists('geodb_get_raw')

        headers = {'Accept': 'application/vnd.pgrst.object+json'}

        qry = f'SELECT {select} FROM "{dn}" '

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

        self._log(qry, logging.DEBUG)

        r = self.post("/rpc/geodb_get_raw", headers=headers, payload={'collection': collection, 'qry': qry})
        r.raise_for_status()

        js = r.json()['src']

        if js:
            return self._df_from_json(js)
        else:
            return DataFrame(columns=["Empty Result"])

    @property
    def server_url(self) -> str:
        """

        Returns:
            str: The URL of the GeoDB REST service
        """
        return self._server_url

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
        self._auth_mode = os.getenv('GEODB_AUTH_MODE') or self._auth_mode

    @property
    def auth_access_token(self) -> str:
        """

        Returns:
            The current authentication access_token
        """

        if self._ipython_shell is not None:
            return self._ipython_shell.user_ns['__auth__'].access_token
        elif self._auth_access_token is not None:
            return self._auth_access_token
        else:
            return self._get_geodb_client_credentials_accesss_token()

    def _get_geodb_client_credentials_accesss_token(self):
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

        data = r.json()

        try:
            return data['access_token']
        except KeyError:
            raise ValueError("The authorization request did net return an access token. Please contact helpdesk.")

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

    def _raise_for_collection_exists(self, collection: str) -> bool:
        """

        Args:
            collection: A table name to check

        Returns:
            bool whether the table exists

        """
        if collection in self.capabilities['definitions']:
            return True
        else:
            raise ValueError(f"Collection {collection} does not exist")

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

    # noinspection PyMethodMayBeStatic
    def _log(self, message: str, level: Union[int, str] = logging.INFO):
        LOGGER.log(level, message)

    @property
    def log_level(self):
        return self._log_level

    @log_level.setter
    def log_level(self, level: Union[int, str]):
        """

        Args:
            level: logging level. See https://docs.python.org/2/library/logging.html#levels for levels

        Examples:
            >>> import logging
            >>> geodb = GeoDBClient()
            >>> geodb.log_level = logging.DEBUG
            or
            >>> geodb.log_level = "DEBUG"
        """

        self._log_level = level
        LOGGER.setLevel(level=level)

    # noinspection PyMethodMayBeStatic
    def setup(self):
        """
            Sets up  the datase. Needs DB credentials and the database user requires CREATE TABLE/FUNCTION grants.
        """
        host = os.getenv('GEODB_DB_HOST')
        port = os.getenv('GEODB_DB_PORT')
        user = os.getenv('GEODB_DB_USER')
        passwd = os.getenv('GEODB_DB_PASSWD')
        dbname = os.getenv('GEODB_DB_DBNAME')

        try:
            import psycopg2
        except ImportError:
            raise GeoDBError("You need to install psycopg2 first to run this module.")

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
