import os
from datetime import datetime, timedelta
from typing import Dict, Optional, Union, Sequence, Tuple

import geopandas as gpd
from geopandas import GeoDataFrame
from pandas import DataFrame
from shapely import wkb
import requests
import json
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

from xcube_geodb.core.collections import Collections
from xcube_geodb.core.message import Message
from xcube_geodb.defaults import GEODB_DEFAULTS
from xcube_geodb.version import version


class GeoDBError(ValueError):
    pass


# noinspection PyShadowingNames
class GeoDBClient(object):
    def __init__(self,
                 server_url: Optional[str] = None,
                 server_port: Optional[int] = None,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 access_token: Optional[str] = None,
                 dotenv_file: str = ".env",
                 auth_mode: str = 'silent',
                 auth_aud: Optional[str] = None,
                 config_file: str = str(Path.home()) + '/.geodb',
                 database: Optional[str] = None):
        """

        Args:
            server_url (str): The URL of the PostGrest Rest API service
            server_port (str): The port to the PostGrest Rest API service
            dotenv_file (str): Name of the dotenv file [.env] to set client IDs and secrets
            client_secret (str): Client secret (overrides environment variables)
            client_id (str): Client ID (overrides environment variables)
            auth_mode (str): Authentication mode [silent]. Can be 'silent' and 'interactive'
            auth_aud (str): Authentication audience
            config_file (str): Filename that stores config info for the geodb client
        """

        self._dotenv_file = dotenv_file
        self._database = None
        # Access token is set here or on request

        # defaults
        self._server_url = GEODB_DEFAULTS["server_url"]
        self._server_port = GEODB_DEFAULTS["server_port"]
        self._auth_client_id = GEODB_DEFAULTS["auth_client_id"]
        self._auth_client_secret = GEODB_DEFAULTS["auth_client_secret"]
        self._auth_access_token = GEODB_DEFAULTS["auth_access_token"]
        self._auth0_config_file = GEODB_DEFAULTS["auth0_config_file"]
        self._auth0_config_folder = GEODB_DEFAULTS["auth0_config_folder"]
        self._auth_domain = GEODB_DEFAULTS["auth_domain"]
        self._auth_aud = GEODB_DEFAULTS["auth_aud"]
        self._auth_mode = GEODB_DEFAULTS["auth_mode"]

        # override defaults by .env
        self.refresh_config_from_env(dotenv_file=dotenv_file, use_dotenv=True)

        # override defaults and .env if given in constructor
        self._server_url = server_url or self._server_url
        self._server_port = server_port or self._server_port
        self._auth_client_id = client_id or self._auth_client_id
        self._auth_client_secret = client_secret or self._auth_client_secret
        self._auth_mode = auth_mode or self._auth_mode
        self._auth_aud = auth_aud or self._auth_aud
        self._auth_domain = auth_aud or self._auth_domain
        self._auth_access_token = access_token or self._auth_access_token
        self._database = database

        self._capabilities = None

        self._whoami = None
        self._ipython_shell = None

        self._mandatory_properties = ["geometry", "id", "created_at", "modified_at"]

        self._config_file = config_file

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
        collection = self.database + '_' + collection

        if collection in capabilities['definitions']:
            return capabilities['definitions'][collection]
        else:
            raise ValueError(f"Table {collection} does not exist.")

    def get_my_collections(self, database: Optional[str] = None) -> Sequence:
        """

        Returns:
            An array of collection names
        """
        payload = {'database': database}
        r = self.post(path='/rpc/geodb_get_my_collections', payload=payload)
        js = r.json()[0]['src']
        if js:
            return self._df_from_json(js)
        else:
            return DataFrame(columns=["table_name"])

    def _get_common_headers(self):
        return {
            'Prefer': 'return=representation',
            'Content-type': 'application/json',
            'Authorization': f"Bearer {self.auth_access_token}"
        }

    @property
    def database(self) -> str:
        """
        Returns:
            The current namespace
        """
        return self._database or self.whoami

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

    def logout(self):
        self._auth_access_token = ''
        os.remove(self._config_file)

    def get_my_usage(self, pretty=True) -> Dict:
        """
        Args:
            pretty: Whether to return in human readable form or in bytes

        Returns:
            A dict containing the usage in bytes (int) or as a human readable string
        """
        payload = {'pretty': pretty} if pretty else {}
        r = self.post(path='/rpc/geodb_get_my_usage', payload=payload)
        return r.json()[0]['src'][0]

    def create_collections(self, collections: Dict, database: Optional[str] = None) -> Collections:
        """

        Args:
            collections: A dictionalry of collections
            database:

        Returns:
            bool: Success

        Examples:

            >>> geodb = GeoDBClient()
            >>> colls = {'[MyCollection]': {'crs': 1234, 'properties': \
                {'[MyProp1]': 'float', '[MyProp2]': 'date'}}}
            >>> geodb.create_collections(colls)
        """

        self._refresh_capabilities()
        database = database or self.database
        # self.create_database(database)

        buffer = {}
        for collection in collections:
            buffer[database + '_' + collection] = collections[collection]

        collections = {"collections": buffer}
        self.post(path='/rpc/geodb_create_collections', payload=collections)

        return Collections(collections)

    def create_collection(self,
                          collection: str,
                          properties: Dict,
                          crs: int = 4326,
                          database: Optional[str] = None) -> Collections:
        """

        Args:
            properties: Property definitions for the collection
            collection: Collection to be created
            crs: sfdv
            database: name of database if not default [user name].

        Returns:
            bool: Success

        Examples:
            >>> geodb = GeoDBClient()
            >>> properties = {'[MyProp1]': 'float', '[MyProp2]': 'date'}
            >>> geodb.create_collection(collection='[MyCollection]', crs=3794, properties=properties)
        """
        collections = {
            collection:
                {
                    "properties": properties,
                    "crs": str(crs)
                }
        }

        self._refresh_capabilities()

        return self.create_collections(collections=collections, database=database)

    def drop_collection(self, collection: str, database: Optional[str] = None) -> Message:
        """

        Args:
            collection: Collection to be dropped
            database:

        Returns:
            bool: Success

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.drop_collection(collection='[MyCollection]')
        """

        database = database or self.database
        return self.drop_collections([collection], database)

    def drop_collections(self, collections: Sequence[str], database: Optional[str] = None) -> Message:
        """

        Args:
            database:
            collections: Collections to be dropped

        Returns:
            Message

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.drop_collections(collections=['[MyCollection1]', '[MyCollection2]'])
        """

        self._refresh_capabilities()

        database = database or self.database
        collections = [database + '_' + collection for collection in collections]
        self.post(path='/rpc/geodb_drop_collections', payload={'collections': collections})

        return Message(f"Collection {str(collections)} deleted")

    def grant_access_to_collection(self, collection: str, usr: str, database: str = "public") -> Message:
        """

        Args:
            collection: Collection to grant access to
            usr: User to grant access to
            database: The namespace to grant access to [public]. By default, public access is granted

        Returns:
            bool: Success
        """
        database = database or self.database
        dn = f"{self.database}_{collection}"

        self.post(path='/rpc/geodb_grant_access_to_collection', payload={'collection': dn, 'usr': usr})

        return Message(f"Access granted on {collection} to {database}")

    def rename_collection(self, database: str, collection: str, new_name: str):
        old_dn = f"{database}_{collection}"
        new_dn = f"{database}_{new_name}"

        self.post(path='/rpc/geodb_rename_collection', payload={'collection': old_dn, 'new_name': new_dn})

    def move_collection(self, database: str, collection: str, new_database: str):
        old_dn = f"{database}_{collection}"
        new_dn = f"{new_database}_{collection}"

        self.post(path='/rpc/geodb_rename_collection', payload={'collection': old_dn, 'new_name': new_dn})

    def publish_collection(self, collection: str) -> Message:
        """

        Args:
            collection: Collection to grant access to

        Returns:
            str: Message
        """
        try:
            self.grant_access_to_collection(collection=collection, usr='public', database=self.database)
        except GeoDBError as e:
            return Message(f"Access could not be granted. List grants with geodb.list_grants()" + str(e))

        return Message(f"Access granted on {collection} to {self.database}")

    def unpublish_collection(self, collection: str) -> Message:
        """

        Args:
            collection: Collection to grant access to

        Returns:
            str: Message
        """

        try:
            self.revoke_access_from_collection(collection=collection, usr='public', database=self.database)
        except GeoDBError as e:
            return Message('Collection already unpublished' + str(e))

        return Message(f"Access revoked for {collection} from {self.database}")

    def revoke_access_from_collection(self, collection: str, usr: str, database: str = 'public') -> Message:
        """

        Args:
            collection: Collection to grant access to
            usr: User to revoke access from
            database: The user to revoke access from [public].

        Returns:
            bool: Success
        """
        database = database or self.database
        dn = f"{database}_{collection}"

        self.post(path='/rpc/geodb_revoke_access_to_collection', payload={'collection': dn, 'usr': usr})

        return Message(f"Access revoked from {collection} of {database}")

    def list_my_grants(self) -> DataFrame:
        """

        Returns:
            A list of the current user's collection grants

        """
        r = self.post(path='/rpc/geodb_list_grants', payload={})
        try:
            js = r.json()
            if isinstance(js, list) and len(js) > 0:
                return self._df_from_json(js[0]['src'])
            else:
                return DataFrame(columns=["table_name"])
        except Exception as e:
            raise GeoDBError("Could not read response from GeoDB. " + str(e))

    def add_property(self, collection: str, prop: str, typ: str, database: Optional[str] = None) -> Message:
        """
        Add a property to an existing collection
        Args:
            collection: Collection to add a property to
            prop: Property name
            typ: Type of property (Postgres type)
            database:

        Returns:
            Success Message

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.add_property(collection='[MyCollection]', name='[MyProperty]', type='[PostgresType]')
        """
        prop = {prop: typ}
        return self.add_properties(collection=collection, properties=prop, database=database)

    def add_properties(self, collection: str, properties: Dict, database: Optional[str] = None) -> Message:
        """

        Args:
            collection: Collection to add properties to
            properties: Property definitions as json array
            database:
        Returns:
            bool: Success

        Examples:
            >>> properties = {'[MyName1]': '[PostgresType1]', '[MyName2]': '[PostgresType2]'}
            >>> geodb = GeoDBClient()
            >>> geodb.add_property(collection='[MyCollection]', properties=properties)
        """

        self._refresh_capabilities()

        database = database or self.database
        collection = database + '_' + collection

        self.post(path='/rpc/geodb_add_properties', payload={'collection': collection, 'properties': properties})

        return Message(f"Properties added")

    def drop_property(self, collection: str, prop: str, database: Optional[str] = None) -> Message:
        """

        Args:
            collection: Collection to drop the property from
            prop: Property to delete
            database:

        Returns:
            bool: Success

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.drop_property(collection='[MyCollection]', prop='[MyProperty]')
        """

        return self.drop_properties(collection=collection, properties=[prop], database=database)

    def drop_properties(self, collection: str, properties: Sequence[str], database: Optional[str] = None) -> Message:
        """

        Args:
            collection: Collection to delete properties from
            properties: A json object containing the property definitions
            database:
        Returns:
            bool: Success

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.drop_properties(collection='[MyCollection]', properties=['[MyProperty1]', '[MyProperty2]'])
        """

        self._refresh_capabilities()
        database = database or self.database
        collection = database + '_' + collection

        self._raise_for_mandatory_columns(properties)

        self._raise_for_stored_procedure_exists('geodb_drop_properties')

        self.post(path='/rpc/geodb_drop_properties', payload={'collection': collection, 'properties': properties})

        return Message(f"Properties {str(properties)} dropped from {collection}")

    def _raise_for_mandatory_columns(self, properties: Sequence[str]):
        common_props = list(set(properties) & set(self._mandatory_properties))
        if len(common_props) > 0:
            raise ValueError("Don't delete the following columns: " + str(common_props))

    def get_properties(self, collection: str, database: Optional[str] = None) -> DataFrame:
        """

        Args:
            collection: Collection to retrieve a list of properties from
            database:

        Returns:
            DataFrame: A list of properties

        """
        database = database or self.database
        collection = database + '_' + collection

        r = self.post(path='/rpc/geodb_get_properties', payload={'collection': collection, "version": "0.1.6"})

        js = r.json()[0]['src']

        if js:
            return self._df_from_json(js)
        else:
            return DataFrame(columns=["table_name", "column_name", "data_type"])

    def create_database(self, database: str) -> bool:
        """

        Returns:
            DataFrame: A list of collections the user owns

        """

        self.post(path='/rpc/geodb_create_database', payload={'database': database})

        return True

    def truncate_database(self, database: str) -> Message:
        """

        Returns:
            DataFrame: A list of collections the user owns

        """

        self.post(path='/rpc/geodb_truncate_database', payload={'database': database})

        return Message(f"Database {database} truncated")

    def get_my_databases(self):
        """

        Returns:
            DataFrame: A list of databases the user owns

        """

        return self.get_collection(collection='user_databases', database='geodb', query=f'owner=eq.{self.whoami}')

    def delete_from_collection(self, collection: str, query: str, database: Optional[str] = None) -> Message:
        """

        Args:
            collection: Collection to delete from  
            query: Filter which records to delete. Follow the http://postgrest.org/en/v6.0/api.html query convention.
            database:
        Returns:
            bool: Success

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.delete_from_collection('[MyCollection]', 'id=eq.1')
        """

        database = database or self.database
        dn = database + '_' + collection

        self._delete(f'/{dn}?{query}')

        return Message(f"Data from {collection} deleted")

    def update_collection(self, collection: str, values: Dict, query: str, database: Optional[str] = None) -> Message:
        """

        Args:
            collection: Collection to be updated
            values: Values to update
            query: Filter which values to be updated. Follow the http://postgrest.org/en/v6.0/api.html query convention.
            database:
        Returns:
            bool: Success
        """

        database = database or self.database
        dn = database + '_' + collection

        self._raise_for_collection_exists(collection=dn)

        if isinstance(values, Dict):
            if 'id' in values.keys():
                del values['id']
        else:
            raise ValueError(f'Format {type(values)} not supported.')

        self._patch(f'/{dn}?{query}', payload=values)

        return Message(f"{collection} updated")

    # noinspection PyMethodMayBeStatic
    def _gdf_prepare_geom(self, gpdf: GeoDataFrame, crs: int = None) -> GeoDataFrame:
        if crs is None:
            try:
                if isinstance(gpdf.crs, dict):
                    crs = gpdf.crs["init"].replace("epsg:", "")
                else:
                    import re
                    m = re.search(r'epsg:([0-9]*)', gpdf.crs.srs)
                    crs = m.group(1)
            except Exception:
                raise GeoDBError("Could not guess the dataframe's crs. Please specify.")

        def add_srid(point):
            return f'SRID={str(crs)};' + str(point)

        gpdf2 = gpdf.copy()
        gpdf2['geometry'] = gpdf2['geometry'].apply(add_srid)
        return gpdf2

    def _gdf_to_csv(self, gpdf: GeoDataFrame, crs: int = None) -> str:
        gpdf = self._gdf_prepare_geom(gpdf, crs)
        return gpdf.to_csv(header=True, index=False, encoding="utf-8").lstrip()

    def _gdf_to_json(self, gpdf: GeoDataFrame, crs: int = None) -> Dict:
        gpdf = self._gdf_prepare_geom(gpdf, crs)
        res = gpdf.to_dict('records')
        return res

    def insert_into_collection(self,
                               collection: str,
                               values: GeoDataFrame,
                               upsert: bool = False,
                               crs: int = None,
                               database: Optional[str] = None) \
            -> Message:
        """

        Args:
            database:
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
            # headers = {'Content-type': 'text/csv'}
            values = self._gdf_prepare_geom(values, crs)
            ct = 0
            cont = True
            max_transfer_num_rows = 10000
            total_rows = values.shape[0]

            while cont:
                frm = ct
                to = ct + max_transfer_num_rows - 1
                ngdf = values.loc[frm:to]
                ct += max_transfer_num_rows

                nct = ngdf.shape[0]
                cont = nct > 0
                if not cont:
                    break

                if nct < max_transfer_num_rows:
                    to = frm + nct

                print(f'Processing rows from {frm + 1} to {to}')
                if 'id' in ngdf.columns and not upsert:
                    ngdf.drop(columns=['id'])

                ngdf.columns = map(str.lower, ngdf.columns)
                js = self._gdf_to_json(ngdf)

                database = database or self.database
                dn = database + '_' + collection

                if upsert:
                    headers = {'Prefer': 'resolution=merge-duplicates'}
                else:
                    headers = None

                self.post(f'/{dn}', payload=js, headers=headers)
        else:
            raise ValueError(f'Format {type(values)} not supported.')

        return Message(f"{total_rows} rows inserted into {collection}")

    def get_collection_by_bbox(self, collection: str,
                               bbox: Tuple[float, float, float, float],
                               comparison_mode: str = 'contains',
                               bbox_crs: int = 4326,
                               limit: int = 0,
                               offset: int = 0,
                               where: Optional[str] = "id>-1",
                               op: str = 'AND',
                               database: Optional[str] = None) -> GeoDataFrame:
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
            database: By default the API gets in the user's own namespace. To access
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

        database = database or self.database
        dn = database + '_' + collection

        self._raise_for_collection_exists(collection=dn)
        self._raise_for_stored_procedure_exists('geodb_get_by_bbox')

        headers = {'Accept': 'application/vnd.pgrst.object+json'}

        r = self.post('/rpc/geodb_get_by_bbox', headers=headers, payload={
            "collection": dn,
            "minx": bbox[0],
            "miny": bbox[1],
            "maxx": bbox[2],
            "maxy": bbox[3],
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

    def head_collection(self, collection: str, num_lines: int = 10, database: Optional[str] = None) -> \
            Union[GeoDataFrame, DataFrame]:
        """

        Args:
            collection: The collection's name
            num_lines: The number of line to return
            database: By default the API gets in the user's own database. To access
                       collections the user has grant set the namespace accordingly.

        Returns:
            GeoDataFrame or DataFrame: results

        Raises:
            HttpError: When the database raises an error

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.head_collection(collection='[MyCollection]', num_lines=10)

        """

        return self.get_collection(collection=collection, query=f'limit={num_lines}', database=database)

    def get_collection(self, collection: str, query: Optional[str] = None, database: Optional[str] = None) \
            -> Union[GeoDataFrame, DataFrame]:
        """

        Args:
            collection: The collection's name
            query: A query. Follow the http://postgrest.org/en/v6.0/api.html query convention.
            database: By default the API gets in the user's own namespace. To access
                       collections the user has grant set the namespace accordingly.

        Returns:
            GeoDataFrame or DataFrame: results

        Raises:
            HttpError: When the database raises an error

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.get_collection(collection='[MyCollection]', query='id=ge.1000')

        """

        tab_prefix = database or self.database
        dn = f"{tab_prefix}_{collection}"

        # self._raise_for_collection_exists(collection=dn)

        if query:
            r = self._get(f"/{dn}?{query}")
        else:
            r = self._get(f"/{dn}")

        js = r.json()

        if js:
            return self._df_from_json(js)
        else:
            return DataFrame(columns=["Empty Result"])

    # noinspection PyMethodMayBeStatic
    def _raise_for_injection(self, select: str):
        select = select.lower()
        if "update" in select \
                or "delete" in select \
                or "drop" in select \
                or "create" in select \
                or "function" in select:
            raise GeoDBError("Please don't inject!")

    # noinspection SqlDialectInspection,SqlNoDataSourceInspection,SqlInjection
    def get_collection_pg(self,
                          collection: str,
                          select: str = "*",
                          where: Optional[str] = None,
                          group: Optional[str] = None,
                          order: Optional[str] = None,
                          limit: Optional[int] = None,
                          offset: Optional[int] = None,
                          database: Optional[str] = None) -> Union[GeoDataFrame, DataFrame]:
        """

        Args:
            collection(str): Collection to query
            select(str): Properties (columns) to return. Can contain aggregation functions
            where(Optional[str]): SQL WHERE statement
            group(Optional[str]): SQL GROUP statement
            order(Optional[str]): SQL ORDER statement
            limit(Optional[int]): Limit for paging
            offset(Optional[int]): Offset (start) of rows to return. Used in combination with limit.
            database: By default the API gets in the user's own namespace. To access
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

        tab_prefix = database or self.database
        dn = f"{tab_prefix}_{collection}"

        self._raise_for_collection_exists(collection=dn)
        self._raise_for_stored_procedure_exists('geodb_get_pg')

        headers = {'Accept': 'application/vnd.pgrst.object+json'}

        r = self.post("/rpc/geodb_get_pg", headers=headers, payload={
            'select': select,
            'where': where,
            'group': group,
            'limit': limit,
            'order': order,
            'offset': offset,
            'collection': dn,
        })
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

    def _set_from_env(self):
        self._server_url = os.getenv('GEODB_API_SERVER_URL') or self._server_url
        self._server_port = os.getenv('GEODB_API_SERVER_PORT') or self._server_port
        self._auth_client_id = os.getenv('GEODB_AUTH_CLIENT_ID') or self._auth_client_id
        self._auth_client_secret = os.getenv('GEODB_AUTH_CLIENT_SECRET') or self._auth_client_secret
        self._auth_access_token = os.getenv('GEODB_AUTH_ACCESS_TOKEN') or self._auth_access_token
        self._auth0_config_file = os.getenv('GEODB_AUTH0_CONFIG_FILE') or self._auth0_config_file
        self._auth0_config_folder = os.getenv('GEODB_AUTH0_CONFIG_FOLDER') or self._auth0_config_folder
        self._auth_domain = os.getenv('GEODB_AUTH_DOMAIN') or self._auth_domain
        self._auth_aud = os.getenv('GEODB_AUTH_AUD') or self._auth_aud
        self._auth_mode = os.getenv('GEODB_AUTH_MODE') or self._auth_mode
        self._database = os.getenv('GEODB_DATABASE') or self._database

    @property
    def auth_access_token(self) -> str:
        """

        Returns:
            The current authentication access_token
        """

        if self._ipython_shell is not None:
            token = self._ipython_shell.user_ns['__auth__'].access_token
        elif self._auth_access_token is not None:
            token = self._auth_access_token
        else:
            token = self._get_token_from_file()

        if not token:
            token = self._get_geodb_client_credentials_accesss_token()

        return token

    def _get_token_from_file(self) -> Union[str, bool]:
        if os.path.isfile(self._config_file):
            with open(self._config_file, 'r') as f:
                try:
                    cfg_data = json.load(f)

                    if 'data' not in cfg_data or 'date' not in cfg_data:
                        return False
                    if 'expires_in' not in cfg_data["data"]:
                        return False

                    now = datetime.now()
                    exp = datetime.strptime(cfg_data['date'], '%Y-%m-%d %H:%M:%S.%f') + timedelta(
                        seconds=cfg_data['data']['expires_in'])
                    if now > exp:
                        return False
                    elif 'client' in cfg_data and self._auth_client_id != cfg_data['client']:
                        return False

                    if 'access_token' in cfg_data['data']:
                        return cfg_data['data']['access_token']
                except Exception as e:
                    print(str(e))
                    return False

        return False

    def _get_geodb_client_credentials_accesss_token(self):
        payload = {
            "client_id": self._auth_client_id,
            "client_secret": self._auth_client_secret,
            "audience": self._auth_aud,
            "grant_type": "client_credentials"
        }

        headers = {'content-type': "application/json"}

        r = requests.post(self._auth_domain + "/oauth/token", json=payload, headers=headers)
        r.raise_for_status()

        data = r.json()

        if os.path.isfile(self._config_file):
            with open(self._config_file, 'w') as f:
                cfg_data = {'date': datetime.now(), 'client': self._auth_client_id, 'data': data}
                json.dump(cfg_data, f, sort_keys=True, default=str)

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

        with open(f'dcfs_geodb/sql/geodb--{version}.sql') as sql_file:
            sql_create = sql_file.read()
            cursor.execute(sql_create)

        conn.commit()
