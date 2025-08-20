import json
import os
from json import JSONDecodeError
from pathlib import Path
from typing import Dict, Optional, Union, Sequence, Tuple, List, Any

import geopandas as gpd
import requests
from dotenv import load_dotenv, find_dotenv
from geopandas import GeoDataFrame
from pandas import DataFrame
from shapely import wkb
from shapely.geometry import shape

from xcube_geodb.const import MINX, MINY, MAXX, MAXY
from xcube_geodb.core.db_interface import DbInterface
from xcube_geodb.core.error import GeoDBError
from xcube_geodb.core.message import Message
from xcube_geodb.defaults import GEODB_DEFAULTS
from xcube_geodb.version import version
import warnings
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from xcube_geodb.core.metadata import Metadata, Link, Provider, Asset, ItemAsset


def warn(msg: str):
    warnings.simplefilter("always", DeprecationWarning)  # turn off filter
    warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
    warnings.simplefilter("ignore", DeprecationWarning)  # reset filter


def deprecated_func(msg: Optional[str] = None):
    def decorator(func):
        """This is a decorator which can be used to mark functions
        as deprecated. It will result in a warning being emitted
        when the function is used."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            warnings.simplefilter("always", DeprecationWarning)  # turn off filter
            warnings.warn(
                "Call to deprecated function '{}'. {}".format(
                    func.__name__, msg + "." if msg else ""
                ),
                category=DeprecationWarning,
                stacklevel=2,
            )
            warnings.simplefilter("ignore", DeprecationWarning)  # reset filter
            return func(*args, **kwargs)

        return wrapper

    return decorator


def deprecated_kwarg(
    deprecated_arg: str, new_arg: Optional[str], msg: Optional[str] = None
):
    def decorator(func):
        """This is a decorator which can be used to mark functions
        as deprecated. It will result in a warning being emitted
        when the function is used."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if deprecated_arg in kwargs:
                new_arg_msg = ""
                if new_arg:
                    kwargs[new_arg] = kwargs[deprecated_arg]
                    new_arg_msg = "Use '" + new_arg + "' instead."

                warnings.simplefilter("always", DeprecationWarning)  # turn off filter
                warnings.warn(
                    f"Call to deprecated parameter '{deprecated_arg}' in "
                    f"function '{func.__name__}'. {new_arg_msg} {msg + '.' if msg else ''}",
                    category=DeprecationWarning,
                    stacklevel=2,
                )
                warnings.simplefilter("ignore", DeprecationWarning)  # reset filter
            return func(*args, **kwargs)

        return wrapper

    return decorator


class EventType:
    CREATED = "created"
    DATABASE_CREATED = "created database"
    DROPPED = "dropped"
    DATABASE_DROPPED = "dropped database"
    RENAMED = "renamed"
    COPIED = "copied"
    MOVED = "moved"
    READ = "read"
    PUBLISHED = "published"
    UNPUBLISHED = "unpublished"
    PUBLISHED_GS = "published to geoserver"
    UNPUBLISHED_GS = "unpublished from geoserver"
    ROWS_ADDED = "added rows"
    ROWS_DROPPED = "dropped rows"
    PROPERTY_ADDED = "added property"
    PROPERTY_DROPPED = "dropped property"
    INDEX_CREATED = "created index"
    INDEX_DROPPED = "dropped index"
    GROUP_CREATED = "added group"
    GROUP_DROPPED = "removed group"
    GROUP_ADDED = "added to group"
    GROUP_REMOVED = "removed from group"
    PUBLISHED_GROUP = "published to group"
    UNPUBLISHED_GROUP = "unpublished from group"
    PUBLISHED_DATABASE = "published database to group"
    UNPUBLISHED_DATABASE = "unpublished database from group"


# noinspection PyShadowingNames,PyUnusedLocal
def check_crs(crs):
    """This function is needed in order to ensure xcube_geodb to understand EPSG crs as well as ensure backward
    compatibility. Furthermore, the database only accepts integer as crs."""

    if isinstance(crs, int):
        return crs
    if isinstance(crs, str):
        try:
            crs = int(crs.split(":")[-1])
            return crs
        except ValueError as e:
            raise GeoDBError(str(e))


class GeoDBClient(object):
    """
    Constructing the geoDB client. Depending on the setup it will automatically setup credentials from
    environment variables. The user can also pass credentials into the constructor.

    Args:
        server_url (str): The URL of the PostGrest Rest API service
        server_port (str): The port to the PostGrest Rest API service
        dotenv_file (str): Name of the dotenv file [.env] to set client IDs and secrets
        client_secret (str): Client secret (overrides environment variables)
        client_id (str): Client ID (overrides environment variables)
        auth_mode (str): Authentication mode [silent]. Can be the oauth2 modes 'client-credentials', 'password',
        'interactive' and 'none' for no authentication
        auth_aud (str): Authentication audience
        config_file (str): Filename that stores config info for the geodb client

    Raises:
        GeoDBError: if the auth mode does not exist
        NotImplementedError: on auth mode interactive

    Examples:
        >>> geodb = GeoDBClient(auth_mode='client-credentials', client_id='***', client_secret='***')
        >>> geodb.whoami
        my_user
    """

    version = version

    def __init__(
        self,
        server_url: Optional[str] = None,
        server_port: Optional[int] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        access_token: Optional[str] = None,
        dotenv_file: str = ".env",
        auth_mode: str = None,
        auth_domain: str = None,
        auth_aud: Optional[str] = None,
        config_file: str = str(Path.home()) + "/.geodb",
        database: Optional[str] = None,
        access_token_uri: Optional[str] = None,
        gs_server_url: Optional[str] = None,
        gs_server_port: Optional[int] = None,
        raise_it: bool = True,
    ):
        self._dotenv_file = dotenv_file
        self._database = None
        self._raise_it = raise_it
        self._do_log_read = True

        # Access token is set here or on request

        # defaults
        self._server_url = GEODB_DEFAULTS["server_url"]
        self._server_port = GEODB_DEFAULTS["server_port"]
        self._gs_server_url = None
        self._gs_server_port = None
        self._auth_client_id = GEODB_DEFAULTS["auth_client_id"]
        self._auth_client_secret = GEODB_DEFAULTS["auth_client_secret"]
        self._auth_access_token = GEODB_DEFAULTS["auth_access_token"]
        self._auth_domain = GEODB_DEFAULTS["auth_domain"]
        self._auth_aud = GEODB_DEFAULTS["auth_aud"]
        self._auth_mode = GEODB_DEFAULTS["auth_mode"]
        self._auth_username = GEODB_DEFAULTS["auth_username"]
        self._auth_password = GEODB_DEFAULTS["auth_password"]
        self._auth_access_token_uri = GEODB_DEFAULTS["auth_access_token_uri"]
        # override defaults by .env
        self.refresh_config_from_env(dotenv_file=dotenv_file, use_dotenv=True)

        # override defaults and .env if given in constructor
        self._server_url = server_url or self._server_url
        self._gs_server_url = gs_server_url or self._gs_server_url
        self._gs_server_port = gs_server_port or self._gs_server_port
        self._server_port = server_port or self._server_port
        self._auth_client_id = client_id or self._auth_client_id
        self._auth_client_secret = client_secret or self._auth_client_secret
        self._auth_username = username or self._auth_username
        self._auth_password = password or self._auth_password
        self._auth_mode = auth_mode or self._auth_mode
        self._auth_aud = auth_aud or self._auth_aud
        self._auth_domain = auth_domain or self._auth_domain
        self._auth_access_token = access_token or self._auth_access_token
        self._auth_access_token_uri = access_token_uri or self._auth_access_token_uri

        self._database = database

        # Set geoserver hosts to geodb host if still unset
        self._gs_server_url = self._gs_server_url or self._server_url
        self._gs_server_port = self._gs_server_port or self._server_port

        self._capabilities = None

        self._whoami = None
        self._ipython_shell = None

        self._mandatory_properties = ["geometry", "id", "created_at", "modified_at"]

        self._config_file = config_file

        if self._auth_mode not in (
            "interactive",
            "password",
            "client-credentials",
            "openid",
            "none",
        ):
            raise GeoDBError(
                "auth_mode can only be 'interactive', 'password', "
                "'client-credentials', or 'openid'!"
            )

        if self._auth_mode == "interactive":
            raise NotImplementedError("The interactive mode has not been implemented.")
            # self._auth_login()

        self._db_interface = DbInterface(
            self._server_url,
            self._server_port,
            self._gs_server_url,
            self._gs_server_port,
            self._auth_mode,
            self._auth_client_id,
            self._auth_client_secret,
            self._auth_username,
            self._auth_password,
            self._auth_access_token,
            self._auth_domain,
            self._auth_aud,
            self._auth_access_token_uri,
        )
        from xcube_geodb.core.metadata import MetadataManager

        self._metadata_manager = MetadataManager(self, self._db_interface)

    def _set_from_env(self):
        """
        Load configurations from environment variables. Overrides defaults.

        """
        self._server_url = os.getenv("GEODB_API_SERVER_URL") or self._server_url
        self._server_port = os.getenv("GEODB_API_SERVER_PORT") or self._server_port
        self._gs_server_url = os.getenv("GEOSERVER_SERVER_URL") or self._gs_server_url
        self._gs_server_port = (
            os.getenv("GEOSERVER_SERVER_PORT") or self._gs_server_port
        )
        self._auth_client_id = os.getenv("GEODB_AUTH_CLIENT_ID") or self._auth_client_id
        self._auth_client_secret = (
            os.getenv("GEODB_AUTH_CLIENT_SECRET") or self._auth_client_secret
        )
        self._auth_access_token = (
            os.getenv("GEODB_AUTH_ACCESS_TOKEN") or self._auth_access_token
        )
        self._auth_domain = os.getenv("GEODB_AUTH_DOMAIN") or self._auth_domain
        self._auth_aud = os.getenv("GEODB_AUTH_AUD") or self._auth_aud
        self._auth_mode = os.getenv("GEODB_AUTH_MODE") or self._auth_mode
        self._auth_username = os.getenv("GEODB_AUTH_USERNAME") or self._auth_username
        self._auth_password = os.getenv("GEODB_AUTH_PASSWORD") or self._auth_password
        self._auth_access_token_uri = (
            os.getenv("GEODB_AUTH_ACCESS_TOKEN_URI") or self._auth_access_token_uri
        )
        self._database = os.getenv("GEODB_DATABASE") or self._database

    def get_collection_info(
        self, collection: str, database: Optional[str] = None
    ) -> Dict:
        """

        Args:
            collection (str): The name of the collection to inspect
            database (str): The database the collection resides in [current
            database]

        Returns:
            A dictionary with collection information

        Raises:
            GeoDBError: When the collection does not exist

        Examples:
            >>> geodb = GeoDBClient(auth_mode='client-credentials', client_id='***', client_secret='***')
            >>> geodb.get_collection_info('my_collection')
            {
                'required': ['id', 'geometry'],
                'properties': {
                'id': {
                    'format': 'integer', 'type': 'integer',
                    'description': 'Note:This is a Primary Key.'
                },
                'created_at': {'format': 'timestamp with time zone', 'type': 'string'},
                'modified_at': {'format': 'timestamp with time zone', 'type': 'string'},
                'geometry': {'format': 'public.geometry(Geometry,3794)', 'type': 'string'},
                'my_property1': {'format': 'double precision', 'type': 'number'},
                'my_property2': {'format': 'double precision', 'type': 'number'},
                'type': 'object'
            }
        """
        capabilities = self.capabilities
        database = database or self.database

        collection = database + "_" + collection

        if collection in capabilities["definitions"]:
            return capabilities["definitions"][collection]
        else:
            self._maybe_raise(GeoDBError(f"Table {collection} does not exist."))

    def get_collection_bbox(
        self,
        collection: str,
        database: Optional[str] = None,
        exact: Optional[bool] = False,
    ) -> Union[None, Sequence]:
        """
        Retrieves the bounding box for the collection, i.e. the union of all
        rows' geometries.

        Args:
            collection (str): The name of the collection to return the
             bounding box for.
            database (str): The database the collection resides in. Default:
             current database.
            exact (bool): If the exact bbox shall be computed. Warning: This
             may take much longer. Default: False.

        Returns:
            the bounding box given as tuple xmin, ymin, xmax, ymax or None if
            collection is empty

        Examples:
            >>> geodb = GeoDBClient(auth_mode='client-credentials', \
                                    client_id='***',
                                    client_secret='***')
            >>> geodb.get_collection_bbox('my_collection')
            (-5, 10, 5, 11)

        """
        try:
            from ast import literal_eval

            database = database or self.database
            dn = f"{database}_{collection}"

            if exact:
                r = self._db_interface.post(
                    path="/rpc/geodb_get_collection_bbox", payload={"collection": dn}
                )
            else:
                r = self._db_interface.post(
                    path="/rpc/geodb_estimate_collection_bbox",
                    payload={"collection": dn},
                )

            bbox = (
                r.text.replace("BOX", "")
                .replace(" ", ",")
                .replace("(", "")
                .replace(")", "")
                .replace('"', "")
            )
            if bbox == "null":
                return None
            bbox = literal_eval(bbox)
            return bbox[1], bbox[0], bbox[3], bbox[2]
        except GeoDBError as e:
            self._maybe_raise(e)

    def get_geometry_types(
        self, collection: str, aggregate: bool = True, database: Optional[str] = None
    ) -> List[str]:
        """
        Retrieves the geometry types of the given collection, either as an
        extensive list of the geometry types for each collection entry, or
        as aggregated list of the types that appear in the collection.

        Args:
            collection (str):  The collection to list geometry types for
            aggregate (bool):  If the result shall be an aggregated list, default is set to "True"
            database (str):    The database that contains the collection
        Returns:
            List[str]: A list of the geometry types, either for each
                       collection entry or aggregated.
        Raises:
            GeoDBError: If the database raises an error
        """

        try:
            database = database or self._database
            dn = f"{database}_{collection}"

            payload = {"collection": dn, "aggregate": "TRUE" if aggregate else "FALSE"}
            r = self._db_interface.post(
                path="/rpc/geodb_geometry_types", payload=payload
            )
            types = r.json()[0]["types"]
            return [a["geometrytype"] for a in types]
        except GeoDBError as e:
            self._maybe_raise(e)

    def get_my_collections(
        self, database: Optional[str] = None
    ) -> Union[DataFrame, GeoDataFrame, None]:
        """

        Args:
            database (str): The database to list collections from

        Returns:
            A Dataframe of collection names

        Examples:
            >>> geodb = GeoDBClient(auth_mode='client-credentials', client_id='***', client_secret='***')
            >>> geodb.get_my_collections()
                owner	                        database	                    collection
            0	geodb_9bfgsdfg-453f-445b-a459	geodb_9bfgsdfg-453f-445b-a459	land_use

        """

        try:
            database = database or self._database
            payload = {"database": database}
            r = self._db_interface.post(
                path="/rpc/geodb_get_my_collections", payload=payload
            )
            js = r.json()[0]["src"]
            if js:
                return self._df_from_json(js)
            else:
                return DataFrame(columns=["collection"])
        except GeoDBError as e:
            self._maybe_raise(e)

    @property
    def raise_it(self) -> bool:
        """

        Returns:
            The current error message behaviour
        """
        return self._raise_it

    @raise_it.setter
    def raise_it(self, value: bool):
        self._raise_it = value

    @property
    def database(self) -> str:
        """

        Returns:
            The current database
        """
        return self._database or self.whoami

    @property
    def whoami(self) -> str:
        """

        Returns:
            The current database user
        """
        return self._whoami or self._db_interface.get(path="/rpc/geodb_whoami").json()

    @property
    def capabilities(self) -> Dict:
        """

        Returns:
            A dictionary of the geoDB PostGrest REST API service's capabilities

        """

        if self._capabilities:
            return self._capabilities

        self._capabilities = self._db_interface.get(path="/").json()
        return self._capabilities

    def _refresh_capabilities(self):
        self._capabilities = None

    def get_geodb_sql_version(self) -> str:
        return self._db_interface.get(
            path="/rpc/geodb_get_geodb_sql_version"
        ).text.replace('"', "")

    def refresh_config_from_env(
        self, dotenv_file: str = ".env", use_dotenv: bool = False
    ):
        """
        Refresh the configuration from environment variables. The variables can be preset by a dotenv file.
        Args:
            dotenv_file (str): A dotenv config file
            use_dotenv (bool): Whether to use GEODB_AUTH_CLIENT_ID a dotenv file.

        """
        if use_dotenv:
            self._dotenv_file = find_dotenv(filename=dotenv_file)
            if self._dotenv_file:
                load_dotenv(self._dotenv_file)
        self._set_from_env()

    def _maybe_raise(self, e, return_df=False):
        if self._raise_it:
            raise e
        else:
            if return_df:
                try:
                    msg = json.loads(str(e))["message"]
                except:
                    msg = str(e)
                return DataFrame(
                    data={
                        "Error": [
                            msg,
                        ]
                    }
                )
            return Message(str(e))

    def get_my_usage(self, pretty=True) -> Union[Dict, Message]:
        """
        Get my geoDB data usage.

        Args:
            pretty (bool): Whether to return in human readable form or in bytes

        Returns:
            A dict containing the usage in bytes (int) or as a human readable string

        Example:
            >>> geodb = GeoDBClient()
            >>> geodb.get_my_usage(True)
            {'usage': '6432 kB'}
        """
        payload = {"pretty": pretty} if pretty else {}
        try:
            r = self._db_interface.post(path="/rpc/geodb_get_my_usage", payload=payload)
            return r.json()[0]["src"][0]
        except GeoDBError as e:
            return self._maybe_raise(e)

    def create_collection_if_not_exists(
        self,
        collection: str,
        properties: Dict,
        crs: Union[int, str] = 4326,
        database: Optional[str] = None,
        **kwargs,
    ) -> Union[Dict, Message]:
        """
        Creates a collection only if the collection does not exist already.

        Args:
            collection (str): The name of the collection to be created
            properties (Dict): Properties to be added to the collection
            crs (int, str): projection
            database (str): The database the collection is to be created in [current database]
            kwargs: Placeholder for deprecated parameters

        Returns:
            Collection:  Collection info id operation succeeds
            None: If operation fails

        Examples:
            See create_collection for an example
        """
        exists = self.collection_exists(collection=collection, database=database)
        if not exists:
            try:
                return self.create_collection(
                    collection=collection,
                    properties=properties,
                    crs=crs,
                    database=database,
                    **kwargs,
                )
            except GeoDBError as e:
                return self._maybe_raise(e)

    def create_collections_if_not_exist(
        self, collections: Dict, database: Optional[str] = None, **kwargs
    ) -> Dict:
        """
        Creates collections only if collections do not exist already.

        Args:
            collections (Dict): The name of the collection to be created
            database (str): The database the collection is to be created in [current database]
            kwargs: Placeholder for deprecated parameters

        Returns:
            List of Collections: List of informations about created collections

        Examples:
            See create_collections for examples
        """
        res = dict()
        for collection in collections:
            exists = self.collection_exists(collection=collection, database=database)
            if exists is None:
                res[collection] = collections[collection]

        return self.create_collections(collections=res, database=database)

    def create_collections(
        self, collections: Dict, database: Optional[str] = None, force: bool = False
    ) -> Union[Dict, Message]:
        """
        Create collections from a dictionary
        Args:
            collections (Dict): A dictionary of collections
            database (str): Database to use for creating the collection
            force (bool): If True, remove already existing collections with identical names

        Returns:
            bool: Success

        Examples:
            >>> geodb = GeoDBClient()
            >>> collections = {'[MyCollection]': {'crs': 1234, 'properties': \
                    {'[MyProp1]': 'float', '[MyProp2]': 'date'}}}
            >>> geodb.create_collections(collections)
        """

        for collection in collections:
            if "crs" in collections[collection]:
                collections[collection]["crs"] = check_crs(
                    collections[collection]["crs"]
                )
            if force:
                try:
                    self.drop_collection(collection=collection, database=database)
                except GeoDBError:
                    pass

        database = database or self.database

        if not self.database_exists(database):
            return Message("Database does not exist.")

        buffer = {}
        for collection in collections:
            buffer[database + "_" + collection] = collections[collection]

        collections = {"collections": buffer}
        try:
            self._db_interface.post(
                path="/rpc/geodb_create_collections", payload=collections
            )
            for collection in collections["collections"]:
                self._log_event(EventType.CREATED, f"collection {collection}")
            self._refresh_capabilities()
            return Message(collections)
        except GeoDBError as e:
            return self._maybe_raise(e)

    def create_collection(
        self,
        collection: str,
        properties: Dict,
        crs: Union[int, str] = 4326,
        database: Optional[str] = None,
        force: bool = False,
    ) -> Dict:
        """
        Create collections from a dictionary

        Args:
            collection (str): Name of the collection to be created
            properties (Dict): Property definitions for the collection
            crs (Union[int, str]): The CRS for the collection
            database (str): Database to use for creating the collection
            force (bool): If True, remove already existing collection with identical name

        Returns:
            bool: Success

        Examples:
            >>> geodb = GeoDBClient()
            >>> properties = {'[MyProp1]': 'float', '[MyProp2]': 'date'}
            >>> geodb.create_collection(collection='[MyCollection]', crs=3794, properties=properties)
        """
        crs = check_crs(crs)
        collections = {collection: {"properties": properties, "crs": str(crs)}}

        return self.create_collections(
            collections=collections, database=database, force=force
        )

    def drop_collection(
        self, collection: str, database: Optional[str] = None
    ) -> Message:
        """

        Args:
            collection (str): Name of the collection to be dropped
            database (str): The database the colections resides in [current database]

        Returns:
            bool: Success

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.drop_collection(collection='[MyCollection]')
        """

        database = database or self.database
        return self.drop_collections(
            collections=[collection], database=database, cascade=True
        )

    def drop_collections(
        self,
        collections: Sequence[str],
        cascade: bool = False,
        database: Optional[str] = None,
    ) -> Message:
        """

        Args:
            database (str): The database the colections resides in [current database]
            collections (Sequence[str]): Collections to be dropped
            cascade (bool): Drop in cascade mode. This can be necessary if e.g. sequences have not been
                            deleted properly

        Returns:
            Message

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.drop_collections(collections=['[MyCollection1]', '[MyCollection2]'])
        """

        database = database or self.database
        payload = {
            "database": database,
            "collections": collections,
            "cascade": "TRUE" if cascade else "FALSE",
        }

        try:
            self._db_interface.post(path="/rpc/geodb_drop_collections", payload=payload)
            for collection in collections:
                self._log_event(
                    EventType.DROPPED, f"collection {database}_{collection}"
                )
            self._refresh_capabilities()
            return Message(f"Collections {database}_{str(collections)} deleted")
        except GeoDBError as e:
            return self._maybe_raise(e)

    def grant_access_to_collection(
        self, collection: str, usr: str, database: Optional[str] = None
    ) -> Message:
        """

        Args:
            collection (str): Collection name to grant access to
            usr (str): Username to grant access to
            database (str): The database the collection resides in

        Returns:
            bool: Success

        Raises:
            HttpError: when http request fails

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.grant_access_to_collection('[Collection]', '[User who gets access]')
            Access granted on Collection to User who gets access}
        """
        database = database or self.database
        dn = f"{database}_{collection}"

        try:
            self._db_interface.post(
                path="/rpc/geodb_grant_access_to_collection",
                payload={"collection": dn, "usr": usr},
            )

            return Message(f"Access granted on {collection} to {usr}")
        except GeoDBError as e:
            return self._maybe_raise(e)

    def rename_collection(
        self, collection: str, new_name: str, database: Optional[str] = None
    ):
        """

        Args:
            collection (str): The name of the collection to be renamed
            new_name (str):The new name of the collection
            database (str): The database the collection resides in

        Raises:
            HttpError: When request fails
        """

        database = database or self._database

        old_dn = f"{database}_{collection}"
        new_dn = f"{database}_{new_name}"

        try:
            self._db_interface.post(
                path="/rpc/geodb_rename_collection",
                payload={"collection": old_dn, "new_name": new_dn},
            )
            self._log_event(EventType.RENAMED, f"collection {old_dn} to {new_dn}")
            return Message(f"Collection renamed from {collection} to {new_name}")
        except GeoDBError as e:
            return self._maybe_raise(e)

    def move_collection(
        self, collection: str, new_database: str, database: Optional[str] = None
    ):
        """
        Move a collection from one database to another.

        Args:
            collection (str): The name of the collection to be moved
            new_database (str): The database the collection will be moved to
            database (str): The database the collection resides in

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.move_collection('collection', 'new_database')
        """

        database = database or self._database
        old_dn = f"{database}_{collection}"
        new_dn = f"{new_database}_{collection}"

        try:
            self._db_interface.post(
                path="/rpc/geodb_rename_collection",
                payload={"collection": old_dn, "new_name": new_dn},
            )

            self._log_event(EventType.MOVED, f"collection {old_dn} to {new_dn}")
            return Message(f"Collection moved from {database} to {new_database}")
        except GeoDBError as e:
            return self._maybe_raise(e)

    def copy_collection(
        self,
        collection: str,
        new_collection: str,
        new_database: str,
        database: Optional[str] = None,
    ):
        """

        Args:
            collection (str): The name of the collection to be copied
            new_collection (str): The new name of the collection
            database (str): The database the collection resides in
                            [current database]
            new_database (str): The database the collection will be copied to

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.copy_collection('col', 'col_new', 'target_db',
                                      'source_db')
        """

        database = database or self._database
        from_dn = f"{database}_{collection}"
        to_dn = f"{new_database}_{new_collection}"

        try:
            self._db_interface.post(
                path="/rpc/geodb_copy_collection",
                payload={"old_collection": from_dn, "new_collection": to_dn},
            )

            self._log_event(EventType.COPIED, f"collection {from_dn} to {to_dn}")
            return Message(f"Collection copied from {from_dn} to {to_dn}")
        except GeoDBError as e:
            return self._maybe_raise(e)

    def publish_collection(
        self, collection: str, database: Optional[str] = None
    ) -> Message:
        """
        Publish a collection. The collection will be accessible by all users
        in the geoDB.

        Args:
            database (str): The database the collection resides in
                            [current database]
            collection (str): The name of the collection that will be made
                              public

        Returns:
            Message: Message whether operation succeeded

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.publish_collection('[Collection]')
        """
        try:
            database = database or self.database

            self.grant_access_to_collection(
                collection=collection, usr="public", database=database
            )
            self._log_event(EventType.PUBLISHED, f"collection {database}_{collection}")
            return Message(f"Access granted on {database}_{collection} to public.")
        except GeoDBError as e:
            return self._maybe_raise(e)

    def unpublish_collection(
        self, collection: str, database: Optional[str] = None
    ) -> Message:
        """
        Revoke public access to a collection. The collection will not be
        accessible by all users in the geoDB.

        Args:
            database (str): The database the collection resides in
            [current database]
            collection (str): The name of the collection that will be removed
            from public access

        Returns:
            Message: Message whether operation succeeded

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.unpublish_collection('[Collection]')
        """
        database = database or self.database

        try:
            return self.revoke_access_from_collection(
                collection=collection, usr="public", database=database
            )
        except GeoDBError as e:
            return self._maybe_raise(e)

    def revoke_access_from_collection(
        self, collection: str, usr: str, database: Optional[str] = None
    ) -> Message:
        """
        Revoke access from a collection.

        Args:
            collection (str): Name of the collection
            usr (str): User to revoke access from
            database (str): The database the collection resides in
                            [current database]

        Returns:
            Message: Whether operation has succeeded
        """

        database = database or self.database
        dn = f"{database}_{collection}"

        try:
            self._db_interface.post(
                path="/rpc/geodb_revoke_access_from_collection",
                payload={"collection": dn, "usr": usr},
            )
            self._log_event(EventType.UNPUBLISHED, f"collection {dn}")
            return Message(f"Access revoked from public on {dn}")
        except GeoDBError as e:
            return self._maybe_raise(e)

    def list_my_grants(self) -> Union[DataFrame, Message]:
        """
        List the access grants the current user has granted.

        Returns:
            DataFrame: A list of the current user's access grants

        Raises:
            GeoDBError: If access to geoDB fails
        """
        r = self._db_interface.post(path="/rpc/geodb_list_grants", payload={})
        try:
            js = r.json()
        except JSONDecodeError as e:
            raise GeoDBError("Body not in valid JSON format: " + str(e))
        try:
            if isinstance(js, list) and len(js) > 0 and "src" in js[0] and js[0]["src"]:
                return self._df_from_json(js[0]["src"])
            else:
                return DataFrame(data={"Grants": ["No Grants"]})
        except Exception as e:
            return self._maybe_raise(GeoDBError(str(e)))

    def add_property(
        self, collection: str, prop: str, typ: str, database: Optional[str] = None
    ) -> Message:
        """
        Add a property to an existing collection.

        Args:
            collection (str): The name of the collection to add a property to
            prop (str): Property name
            typ (str): The data type of the property (Postgres type)
            database (str): The database the collection resides in [current
                            database]

        Returns:
            Message: Success message

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.add_property(collection='[MyCollection]', \
                name='[MyProperty]', type='[PostgresType]')
        """
        prop = {prop: typ}

        return self.add_properties(
            collection=collection, properties=prop, database=database
        )

    def add_properties(
        self, collection: str, properties: Dict, database: Optional[str] = None
    ) -> Message:
        """
        Add properties to a collection.

        Args:
            collection (str): The name of the collection to add properties to
            properties (Dict): Property definitions as dictionary
            database (str): The database the collection resides in [current
                            database]
        Returns:
            Message: Whether the operation succeeded

        Examples:
            >>> properties = {'[MyName1]': '[PostgresType1]', \
                              '[MyName2]': '[PostgresType2]'}
            >>> geodb = GeoDBClient()
            >>> geodb.add_property(collection='[MyCollection]', \
                                   properties=properties)
        """

        database = database or self.database
        dn = database + "_" + collection

        try:
            self._db_interface.post(
                path="/rpc/geodb_add_properties",
                payload={"collection": dn, "properties": properties},
            )
            for prop_name in properties:
                prop_type = properties[prop_name]
                self._log_event(
                    EventType.PROPERTY_ADDED,
                    f"{{name: {prop_name}, type: {prop_type}}} to collection {dn}",
                )

            self._refresh_capabilities()
            return Message("Properties added")
        except GeoDBError as e:
            return self._maybe_raise(e)

    def drop_property(
        self, collection: str, prop: str, database: Optional[str] = None, **kwargs
    ) -> Message:
        """
        Drop a property from a collection.

        Args:
            collection (str): The name of the collection to drop the property from
            prop (str): The property to delete
            database (str): The database the collection resides in [current database]

        Returns:
            Message: Whether the operation succeeded

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.drop_property(collection='[MyCollection]', \
                                    prop='[MyProperty]')
        """

        return self.drop_properties(
            collection=collection, properties=[prop], database=database
        )

    def drop_properties(
        self, collection: str, properties: Sequence[str], database: Optional[str] = None
    ) -> Message:
        """
        Drop properties from a collection.

        Args:
            collection (str):  The name of the collection to delete properties
                               from
            properties (List): A list containing the property names
            database (str):    The database the collection resides in [current
                               database]
        Returns:
            Message: Whether the operation succeeded

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.drop_properties(collection='MyCollection', \
                                      properties=['MyProperty1', \
                                                  'MyProperty2'])
        """

        database = database or self.database
        collection = database + "_" + collection

        self._raise_for_mandatory_columns(properties)

        self._raise_for_stored_procedure_exists("geodb_drop_properties")

        try:
            self._db_interface.post(
                path="/rpc/geodb_drop_properties",
                payload={"collection": collection, "properties": properties},
            )
            for prop in properties:
                self._log_event(
                    EventType.PROPERTY_DROPPED, f"{prop} from collection {collection}"
                )

            self._refresh_capabilities()
            return Message(f"Properties {str(properties)} dropped from {collection}")

        except GeoDBError as e:
            return self._maybe_raise(e)

    def _raise_for_mandatory_columns(self, properties: Sequence[str]):
        common_props = list(set(properties) & set(self._mandatory_properties))
        if len(common_props) > 0:
            raise GeoDBError("Don't delete the following columns: " + str(common_props))

    @deprecated_kwarg("namespace", "database")
    def get_properties(
        self, collection: str, database: Optional[str] = None, **kwargs
    ) -> DataFrame:
        """
        Get a list of properties of a collection.

        Args:
            collection (str): The name of the collection to retrieve a list of properties from
            database (str): The database the collection resides in [current database]

        Returns:
            DataFrame: A list of properties

        """
        database = database or self.database
        collection = database + "_" + collection

        try:
            r = self._db_interface.post(
                path="/rpc/geodb_get_properties", payload={"collection": collection}
            )

            js = r.json()[0]["src"]

            if js:
                return self._df_from_json(js)
            else:
                return DataFrame(columns=["collection", "column_name", "data_type"])
        except GeoDBError as e:
            self._maybe_raise(e, return_df=True)

    def create_database(self, database: str) -> Message:
        """
        Create a database.

        Args:
            database (str): The name of the database to be created

        Returns:
            Message: A message about the success or failure of the operation

        """

        try:
            self._db_interface.post(
                path="/rpc/geodb_create_database", payload={"database": database}
            )
            return Message(f"Database {database} created")
        except GeoDBError as e:
            return self._maybe_raise(e)

    def truncate_database(self, database: str, force: bool = False) -> Message:
        """
        Delete the given database, if empty. If the database is not empty, it will not
        be deleted, unless the `force` parameter is True.

        Args:
            database (str): The name of the database to be deleted.
            force (bool): Drop all collections within the database.

        Returns:
            Message: A message that the operation has been successful.
        """

        if not self.database_exists(database):
            raise GeoDBError(
                f"Database {database} does not exist. No action has been taken."
            )

        if database == self.whoami:
            raise GeoDBError(
                f"The default database {database} cannot be dropped. No action has "
                f"been taken."
            )

        if database not in list(self.get_my_databases()["name"]):
            raise GeoDBError(
                f"You can only delete databases you own. You are not the owner of "
                f"database {database}."
            )

        if len(self.get_my_collections(database)) > 0 and not force:
            raise GeoDBError(
                f"The database {database} is not empty, and can therefore not be "
                f"dropped. No action has been taken. "
                f"If you wish to drop the database and all the collections inside, use "
                f"`force=True`. Warning: this action cannot be reverted!"
            )

        # we can safely assume here that either the database is empty, or force == True
        if len(self.get_my_collections(database)) > 0:
            for collection in list(self.get_my_collections(database)["collection"]):
                self.drop_collection(collection, database)

        try:
            self._db_interface.post(
                path="/rpc/geodb_truncate_database", payload={"database": database}
            )
            self._log_event(EventType.DATABASE_DROPPED, database)
            return Message(f"Database {database} truncated")
        except GeoDBError as e:
            return self._maybe_raise(e)

    def get_my_databases(self) -> DataFrame:
        """
        Get a list of databases the current user owns.

        Returns:
            DataFrame: A list of databases the user owns

        """

        self._do_log_read = False

        try:
            return self.get_collection(
                collection="user_databases",
                database="geodb",
                query=f"owner=eq.{self.whoami}",
            )
        except GeoDBError as e:
            return self._maybe_raise(e, return_df=True)

    def database_exists(self, database: str) -> bool:
        """
        Checks whether a database exists.

        Args:
            database (str): The name of the database to be checked

        Returns:
            bool: database exists

        Raises:
            HttpError: If request fails

        """

        self._do_log_read = False
        try:
            res = self.get_collection(
                collection="user_databases",
                database="geodb",
                query=f"name=eq.{database}",
            )
            return len(res) > 0
        except GeoDBError as e:
            self._maybe_raise(e)

    def delete_from_collection(
        self, collection: str, query: str, database: Optional[str] = None
    ) -> Message:
        """
        Delete rows from collection.

        Args:
            collection (str): The name of the collection to delete rows from
            database (str): The name of the database to be checked
            query (str): Filter which records to delete. Follow the
                         https://postgrest.org/en/v6.0/api.html query convention.
        Returns:
            Message: Whether the operation has succeeded

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.delete_from_collection('[MyCollection]', 'id=eq.1')
        """

        database = database or self.database
        dn = database + "_" + collection

        try:
            self._db_interface.delete(f"/{dn}?{query}")
            self._log_event(
                EventType.ROWS_DROPPED, f"from collection {dn} where {query}"
            )
            return Message(f"Data from {collection} deleted")
        except GeoDBError as e:
            return self._maybe_raise(e)

    @deprecated_kwarg("namespace", "database")
    def update_collection(
        self,
        collection: str,
        values: Dict,
        query: str,
        database: Optional[str] = None,
        **kwargs,
    ) -> Message:
        """
        Update data in a collection by a query.

        Args:
            collection (str): The name of the collection to be updated
            database (str): The name of the database to be checked
            values (Dict): Values to update
            query (str): Filter which values to be updated. Follow the http://postgrest.org/en/v6.0/api.html query
            convention.
        Returns:
            Message: Success

        Raises:
            GeoDBError: if the values is not a Dict or request fails
        Example:

        """

        database = database or self.database
        dn = database + "_" + collection

        self._raise_for_collection_exists(collection=collection, database=database)

        if isinstance(values, Dict):
            if "id" in values.keys():
                del values["id"]
        else:
            raise GeoDBError(f"Format {type(values)} not supported.")

        try:
            self._db_interface.patch(f"/{dn}?{query}", payload=values)
            return Message(f"{collection} updated")
        except GeoDBError as e:
            return self._maybe_raise(e)

    # noinspection PyMethodMayBeStatic
    def _gdf_prepare_geom(self, gpdf: GeoDataFrame, crs: int = None) -> DataFrame:
        if crs is None:
            crs = gpdf.crs.to_epsg()

            if crs is None:
                raise GeoDBError(
                    "Invalid crs in geopandas data frame. You can pass the crs as parameter "
                    "(crs=[your crs])"
                )

        def add_srid(point):
            point_str = str(point)
            if "SRID" not in point_str:
                return f"SRID={str(crs)};" + str(point)
            else:
                return str(point)

        gpdf2 = DataFrame(gpdf.copy())
        gpdf2["geometry"] = gpdf2["geometry"].apply(add_srid)
        return gpdf2

    def _gdf_to_json(self, gpdf: GeoDataFrame, crs: int = None) -> Dict:
        gpdf = self._gdf_prepare_geom(gpdf, crs)
        res = gpdf.to_dict("records")
        return res

    def insert_into_collection(
        self,
        collection: str,
        values: GeoDataFrame,
        upsert: bool = False,
        crs: Optional[Union[int, str]] = None,
        database: Optional[str] = None,
        max_transfer_chunk_size: int = 1000,
    ) -> Message:
        """
        Insert data into a collection.

        Args:
            collection (str): Collection to be inserted to
            database (str): The name of the database the collection resides in
                            [current database]
            values (GeoDataFrame): Values to be inserted
            upsert (bool): Whether the insert shall replace existing rows
                           (by PK)
            crs (int, str): crs (in the form of an SRID) of the geometries. If
                            not present, the method will attempt guessing it
                            from the GeoDataFrame input. Must be in sync with
                            the target collection in the GeoDatabase
            max_transfer_chunk_size (int): Maximum number of rows per chunk to
                                           be sent to the geodb.

        Raises:
            ValueError: When crs is not given and cannot be guessed from the
                        GeoDataFrame
            GeoDBError: If the values are not in format Dict

        Returns:
            bool: Success

        Example:

        """
        srid = self.get_collection_srid(collection, database)
        crs = check_crs(crs)
        if crs and srid and srid != crs:
            raise GeoDBError(
                f"crs {crs} is not compatible with collection's crs {srid}"
            )

        crs = crs or srid
        total_rows = 0

        database = database or self.database
        dn = database + "_" + collection

        if isinstance(values, GeoDataFrame):
            # headers = {'Content-type': 'text/csv'}
            # values = self._gdf_prepare_geom(values, crs)
            ct = 0
            cont = True
            total_rows = values.shape[0]

            while cont:
                frm = ct
                to = ct + max_transfer_chunk_size - 1
                ngdf = values.loc[frm:to]
                ct += max_transfer_chunk_size

                nct = ngdf.shape[0]
                cont = nct > 0
                if not cont:
                    break

                if nct < max_transfer_chunk_size:
                    to = frm + nct

                print(f"Processing rows from {frm} to {to}")
                if "id" in ngdf.columns and not upsert:
                    ngdf.drop(columns=["id"])

                ngdf.columns = map(str.lower, ngdf.columns)
                js = self._gdf_to_json(ngdf, crs)

                if upsert:
                    headers = {"Prefer": "resolution=merge-duplicates"}
                else:
                    headers = None

                try:
                    self._db_interface.post(f"/{dn}", payload=js, headers=headers)
                except GeoDBError as e:
                    return self._maybe_raise(e)
                except requests.exceptions.ConnectionError as e:
                    if (
                        "Connection aborted" in str(e)
                        and "LineTooLong('got more than 65536 bytes "
                        "when reading header line')"
                        in str(e)
                    ):
                        # ignore this error - the ingestion has worked.
                        # see https://github.com/dcs4cop/xcube-geodb/issues/60
                        pass
                    else:
                        raise e
        else:
            self._maybe_raise(
                GeoDBError(f"Error: Format {type(values)} not supported.")
            )

        msg = f"{total_rows} rows inserted into "
        self._log_event(EventType.ROWS_ADDED, f"{msg}{dn}")
        return Message(f"{msg}{collection}")

    @staticmethod
    def transform_bbox_crs(
        bbox: Tuple[float, float, float, float],
        from_crs: Union[int, str],
        to_crs: Union[int, str],
        wsg84_order: str = "lat_lon",
    ):
        """
        This function can be used to reproject bboxes particularly with the use of GeoDBClient.get_collection_by_bbox.

        Args:
            bbox: Tuple[float, float, float, float]: bbox to be reprojected, given as MINX, MINY, MAXX, MAXY
            from_crs: Source crs e.g. 3974
            to_crs: Target crs e.g. 4326
            wsg84_order (str): WSG84 (EPSG:4326) is expected to be in Lat Lon format ("lat_lon"). Use "lon_lat" if
                               Lon Lat is used.
        Returns:
            Tuple[float, float, float, float]: The reprojected bounding box

        Examples:
             >>> bbox = GeoDBClient.transform_bbox_crs(bbox=(450000, 100000, 470000, 110000), from_crs=3794, to_crs=4326)
             >>> bbox
             (49.36588643725233, 46.012889756941775, 14.311548793848758, 9.834303086688251)

        """
        from pyproj import Transformer

        from_crs = check_crs(from_crs)
        to_crs = check_crs(to_crs)

        if wsg84_order == "lat_lon" and from_crs == 4326:
            bbox = (bbox[1], bbox[0], bbox[3], bbox[2])

        transformer = Transformer.from_crs(f"EPSG:{from_crs}", f"EPSG:{to_crs}")
        p1 = transformer.transform(bbox[MINX], bbox[MINY])
        p2 = transformer.transform(bbox[MAXX], bbox[MAXY])

        if wsg84_order == "lat_lon" and to_crs == 4326:
            return p1[1], p1[0], p2[1], p2[0]

        return p1[0], p1[1], p2[0], p2[1]

    @deprecated_kwarg("namespace", "database")
    def get_collection_by_bbox(
        self,
        collection: str,
        bbox: Tuple[float, float, float, float],
        comparison_mode: str = "contains",
        bbox_crs: Union[int, str] = 4326,
        limit: int = 0,
        offset: int = 0,
        where: Optional[str] = "id>-1",
        op: str = "AND",
        database: Optional[str] = None,
        wsg84_order="lat_lon",
        **kwargs,
    ) -> Union[GeoDataFrame, DataFrame]:
        """
        Query the database by a bounding box. Please be careful with the bbox crs. The easiest is
        using the same crs as the collection. However, if the bbox crs differs from the collection,
        the geoDB client will attempt to automatially transform the bbox crs according to the collection's crs.
        You can also directly use the method GeoDBClient.transform_bbox_crs yourself before you pass the bbox into
        this method.

        Args:
            collection (str): The name of the collection to be quried
            bbox (Tuple[float, float, float, float]): minx, miny, maxx, maxy
            comparison_mode (str): Filter mode. Can be 'contains' or 'within' ['contains']
            bbox_crs (int, str): Projection code. [4326]
            op (str): Operator for where (AND, OR) ['AND']
            where (str): Additional SQL where statement to further filter the collection
            limit (int): The maximum number of rows to be returned
            offset (int): Offset (start) of rows to return. Used in combination with limit.
            database (str): The name of the database the collection resides in [current database]
            wsg84_order (str): WSG84 (EPSG:4326) is expected to be in Lat Lon format ("lat_lon"). Use "lon_lat" if
            Lon Lat is used.

        Returns:
            A GeoPandas Dataframe

        Raises:
            HttpError: When the database raises an error

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.get_collection_by_bbox(table="[MyCollection]", bbox=(452750.0, 88909.549, 464000.0, \
                102486.299), comparison_mode="contains", bbox_crs=3794, limit=10, offset=10)
        """
        bbox_crs = check_crs(bbox_crs)
        database = database or self.database
        dn = database + "_" + collection

        self._raise_for_collection_exists(collection=collection, database=database)
        self._raise_for_stored_procedure_exists("geodb_get_by_bbox")

        coll_crs = self.get_collection_srid(collection=collection, database=database)

        try:
            if coll_crs is not None and coll_crs != bbox_crs:
                bbox = self.transform_bbox_crs(
                    bbox, bbox_crs, int(coll_crs), wsg84_order=wsg84_order
                )
                bbox_crs = coll_crs

            headers = {"Accept": "application/vnd.pgrst.object+json"}

            payload = {
                "collection": dn,
                "minx": bbox[0],
                "miny": bbox[1],
                "maxx": bbox[2],
                "maxy": bbox[3],
                "comparison_mode": comparison_mode,
                "bbox_crs": bbox_crs,
                "limit": limit,
                "where": where,
                "op": op,
                "offset": offset,
            }
            r = self._db_interface.post(
                "/rpc/geodb_get_by_bbox",
                headers=headers,
                payload=payload,
            )

            self._maybe_log_read(collection, database, str(payload))

            js = r.json()["src"]
            if js:
                srid = self.get_collection_srid(collection, database)
                return self._df_from_json(js, srid)
            else:
                return GeoDataFrame(columns=["Empty Result"])
        except GeoDBError as e:
            return self._maybe_raise(e, return_df=True)

    def count_collection_rows(
        self,
        collection: str,
        database: Optional[str] = None,
        exact_count: Optional[bool] = False,
    ) -> Union[int, Message]:
        """
        Return the number of rows in the given collection. By default, this
        function returns a rough estimate within the order of magnitude of the
        actual number; the exact count can also be retrieved, but this may take
        much longer.
        Note: in some cases, no estimate can be provided. In such cases,
        -1 is returned if exact_count == False.

        Args:
            collection (str):   The name of the collection
            database (str):     The name of the database the collection resides
                                in [current database]
            exact_count (bool): If True, the actual number of rows will be
                                counted. Default value: false.

        Returns:
            the number of rows in the given collection, or -1 if exact_count
            is False and no estimate could be provided.

        Raises:
            GeoDBError: When the database raises an error

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.count_collection_rows('my_collection'], exact_count=True)
        """

        database = database or self.database
        dn = database + "_" + collection

        try:
            if exact_count:
                r = self._db_interface.post(
                    "/rpc/geodb_count_collection", payload={"collection": dn}
                )
            else:
                r = self._db_interface.post(
                    "/rpc/geodb_estimate_collection_count", payload={"collection": dn}
                )
        except GeoDBError as e:
            return self._maybe_raise(e, return_df=False)

        return int(r.text)

    def count_collection_by_bbox(
        self,
        collection: str,
        bbox: Tuple[float, float, float, float],
        comparison_mode: str = "contains",
        bbox_crs: Union[int, str] = 4326,
        where: Optional[str] = "id>-1",
        op: str = "AND",
        database: Optional[str] = None,
        wsg84_order="lat_lon",
    ) -> Union[GeoDataFrame, DataFrame]:
        """
        Query the database by a bounding box and return the count. Please be careful with the bbox crs. The easiest is
        using the same crs as the collection. However, if the bbox crs differs from the collection,
        the geoDB client will attempt to automatially transform the bbox crs according to the collection's crs.
        You can also directly use the method GeoDBClient.transform_bbox_crs yourself before you pass the bbox into
        this method.

        Args:
            collection (str): The name of the collection to be quried
            bbox (Tuple[float, float, float, float]): minx, miny, maxx, maxy
            comparison_mode (str): Filter mode. Can be 'contains' or 'within' ['contains']
            bbox_crs (int, str): Projection code. [4326]
            op (str): Operator for where (AND, OR) ['AND']
            where (str): Additional SQL where statement to further filter the collection
            database (str): The name of the database the collection resides in [current database]
            wsg84_order (str): WSG84 (EPSG:4326) is expected to be in Lat Lon format ("lat_lon"). Use "lon_lat" if
            Lon Lat is used.

        Returns:
            A GeoPandas Dataframe

        Raises:
            HttpError: When the database raises an error

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.count_collection_by_bbox(table="[MyCollection]", bbox=(452750.0, 88909.549, 464000.0, \
                102486.299), comparison_mode="contains", bbox_crs=3794)
        """
        bbox_crs = check_crs(bbox_crs)
        database = database or self.database
        dn = database + "_" + collection

        self._raise_for_collection_exists(collection=collection, database=database)
        self._raise_for_stored_procedure_exists("geodb_get_by_bbox")

        coll_crs = self.get_collection_srid(collection=collection, database=database)

        try:
            if coll_crs is not None and coll_crs != bbox_crs:
                bbox = self.transform_bbox_crs(
                    bbox, bbox_crs, int(coll_crs), wsg84_order=wsg84_order
                )
                bbox_crs = coll_crs
            headers = {"Accept": "application/vnd.pgrst.object+json"}

            r = self._db_interface.post(
                "/rpc/geodb_count_by_bbox",
                headers=headers,
                payload={
                    "collection": dn,
                    "minx": bbox[0],
                    "miny": bbox[1],
                    "maxx": bbox[2],
                    "maxy": bbox[3],
                    "comparison_mode": comparison_mode,
                    "bbox_crs": bbox_crs,
                    "where": where,
                    "op": op,
                },
            )

            js = r.json()["src"]
            if js:
                srid = self.get_collection_srid(collection, database)
                return self._df_from_json(js, srid)
            else:
                return GeoDataFrame(columns=["Empty Result"])
        except GeoDBError as e:
            return self._maybe_raise(e, return_df=True)

    def head_collection(
        self, collection: str, num_lines: int = 10, database: Optional[str] = None
    ) -> Union[GeoDataFrame, DataFrame]:
        """
        Get the first num_lines of a collection.

        Args:
            collection (str): The collection's name
            num_lines (int): The number of line to return
            database (str): The name of the database the collection resides in [current database]

        Returns:
            GeoDataFrame or DataFrame: results

        Raises:
            HttpError: When the database raises an error

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.head_collection(collection='[MyCollection]', num_lines=10)

        """

        return self.get_collection(
            collection=collection, query=f"limit={num_lines}", database=database
        )

    def get_collection(
        self,
        collection: str,
        query: Optional[str] = None,
        database: Optional[str] = None,
        limit: int = None,
        offset: int = 0,
    ) -> Union[GeoDataFrame, DataFrame]:
        """
        Query a collection.

        Args:
            collection (str): The collection's name.
            query (str): A query. Follow the http://postgrest.org/en/v6.0/api.html query convention.
            database (str): The name of the database the collection resides in [current database].
            limit (int): The maximum number of rows to be returned.
            offset (int): Offset (start) of rows to return. Used in combination with limit.

        Returns:
            GeoDataFrame or DataFrame: results

        Raises:
            HttpError: When the database raises an error

        Examples:
            >>> geodb = GeoDBClient()
            >>> geodb.get_collection(collection='[MyCollection]', query='id=ge.1000')

        """

        srid = self.get_collection_srid(collection=collection, database=database)

        tab_prefix = database or self.database
        dn = f"{tab_prefix}_{collection}"

        try:
            actual_query = query if query else ""
            if limit or offset:
                actual_query = f"{query}&" if query else ""
                actual_query = f"{actual_query}limit={limit}&offset={offset}"

            if actual_query:
                r = self._db_interface.get(f"/{dn}?{actual_query}")
            else:
                r = self._db_interface.get(f"/{dn}")

            self._maybe_log_read(collection, database, actual_query)

            js = r.json()

            if js:
                return self._df_from_json(js, srid)
            else:
                return DataFrame(columns=["Empty Result"])
        except GeoDBError as e:
            return self._maybe_raise(e, return_df=True)

    # noinspection SqlDialectInspection,SqlNoDataSourceInspection,SqlInjection
    def get_collection_pg(
        self,
        collection: str,
        select: str = "*",
        where: Optional[str] = None,
        group: Optional[str] = None,
        order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        database: Optional[str] = None,
    ) -> Union[GeoDataFrame, DataFrame]:
        """

        Args:
            collection (str): The name of the collection to query
            select (str): Properties (columns) to return. Can contain aggregation functions
            where (Optional[str]): SQL WHERE statement
            group (Optional[str]): SQL GROUP statement
            order (Optional[str]): SQL ORDER statement
            limit (Optional[int]): Limit for paging
            offset (Optional[int]): Offset (start) of rows to return. Used in combination with limit.
            database (str): The name of the database the collection resides in [current database]

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

        self._raise_for_collection_exists(collection=collection, database=database)
        self._raise_for_stored_procedure_exists("geodb_get_pg")

        headers = {"Accept": "application/vnd.pgrst.object+json"}

        try:
            r = self._db_interface.post(
                "/rpc/geodb_get_pg",
                headers=headers,
                payload={
                    "select": select,
                    "where": where,
                    "group": group,
                    "limit": limit,
                    "order": order,
                    "offset": offset,
                    "collection": dn,
                },
            )
            r.raise_for_status()

            js = r.json()["src"]

            self._maybe_log_read(collection, database)

            if js:
                srid = self.get_collection_srid(collection, database)
                return self._df_from_json(js, srid)
            else:
                return DataFrame(columns=["Empty Result"])
        except GeoDBError as e:
            return self._maybe_raise(e, return_df=True)

    def _maybe_log_read(self, collection, database, query=None):
        if self._do_log_read:
            message = f"read from collection {database}_{collection}"
            if query:
                message += f": {query}"
            self._log_event(EventType.READ, message)
        else:
            self._do_log_read = True

    def create_index(self, collection: str, prop: str, database: str = None) -> Message:
        """
        Creates a new index on the given collection and the given property.
        This may drastically speed up your queries, but adding too many
        indexes on a collection might also hamper its performance. Please use
        with care, and use only if you know what you are doing.

        In case you are doing lots of geographical queries, you'll probably
        want to add an index to your geometry column like this:

            >>> geodb = GeoDBClient()
            >>> geodb.create_index(collection='[MyCollection]',
                                   prop='geometry')

        Note that you may remove your indexes using `remove_index`.

        Args:
            collection (str): The collection's name
            prop (str): The name of the property to add an index to.
                        Use `get_collection_info` to get the list of
                        properties for a collection.
            database (str): The name of the database the collection resides
                            in [current database].
        Returns:
            A message if the index was successfully created.
        Raises:
            GeoDBError if the index already exists.
        """

        database = database or self.database
        dn = f"{database}_{collection}"

        path = "/rpc/geodb_create_index"
        payload = {
            "collection": dn,
            "property": prop,
        }

        self._db_interface.post(path=path, payload=payload)
        self._log_event(EventType.INDEX_CREATED, "table {dn} and property {prop}")
        return Message(f"Created new index on table {dn} and property {prop}.")

    def show_indexes(self, collection: str, database: str = None) -> DataFrame:
        """
        Shows the indexes on the given collection.

        Args:
            collection (str): The collection's name
            database (str): The name of the database the collection resides
                            in [current database].
        Returns:
            A dataframe containing the list of indexes for the given
            collection.
        """

        database = database or self.database
        dn = f"{database}_{collection}"

        path = "/rpc/geodb_show_indexes"
        payload = {
            "collection": dn,
        }

        r = self._db_interface.post(path=path, payload=payload)
        return DataFrame(r.json())

    def remove_index(self, collection: str, prop: str, database: str = None) -> Message:
        """
        Removes the index on the given collection and the given property.

        Args:
            collection (str): The collection's name
            prop (str): The name of the property to add an index to.
            database (str): The name of the database the collection resides
                            in [current database].
        Returns:
            A message if the index was successfully removed.
        """

        database = database or self.database
        dn = f"{database}_{collection}"

        path = "/rpc/geodb_drop_index"
        payload = {
            "collection": dn,
            "property": prop,
        }

        self._log_event(EventType.INDEX_DROPPED, "table {dn} and property {prop}")

        self._db_interface.post(path=path, payload=payload)
        return Message(f"Removed index from table {dn} and property {prop}.")

    @property
    def server_url(self) -> str:
        """
        Get URL of the geoDB server.

        Returns:
            str: The URL of the geoDB REST service
        """
        return self._server_url

    def get_collection_srid(
        self, collection: str, database: Optional[str] = None
    ) -> Optional[Union[str, type(None), Message]]:
        """
        Get the SRID of a collection.

        Args:
            collection (str): The collection's name
            database (str): The name of the database the collection resides in [current database]

        Returns:
            The name of the SRID
        """
        tab_prefix = database or self.database
        dn = f"{tab_prefix}_{collection}"

        try:
            r = self._db_interface.post(
                path="/rpc/geodb_get_collection_srid",
                payload={"collection": dn},
                raise_for_status=False,
            )

            if r.status_code == 200:
                js = r.json()[0]["src"][0]

                if js:
                    return js["srid"]

            return None
        except GeoDBError as e:
            return self._maybe_raise(e)

    def _df_from_json(
        self, js: Dict, srid: Optional[int] = None
    ) -> Union[GeoDataFrame, DataFrame]:
        """
        Converts wkb geometry string to wkt from a PostGREST JSON result.
        Args:
            js (Dict): The geometries to be converted
            srid (Optional[int]):.

        Returns:
            GeoDataFrame, DataFrame

        """
        if js is None:
            return DataFrame()

        data = [self._convert_geo(row) for row in js]

        gpdf = gpd.GeoDataFrame(data)

        if "geometry" in gpdf:
            crs = f"EPSG:{srid}" if srid else None
            gpdf.crs = crs
            return gpdf.set_geometry("geometry")
        else:
            return DataFrame(gpdf)

    # noinspection PyMethodMayBeStatic
    def _convert_geo(self, row: Dict) -> Dict:
        """

        Args:
            row: A row of the PostgREST result

        Returns:
            Dict: The input row of the PostgREST result with its geometry
                  converted to wkt
        """

        if "geometry" in row:
            if type(row["geometry"]) == dict and "coordinates" in row["geometry"]:
                geom = shape(row["geometry"])
                row["geometry"] = geom
            else:
                row["geometry"] = wkb.loads(row["geometry"], hex=True)
        return row

    def publish_gs(self, collection: str, database: Optional[str] = None):
        """
        Publishes collection to a BC geoservice (geoserver instance). Requires
        access registration.

        Args:
            collection (str): Name of the collection
            database (Optional[str]): Name of the database. Defaults to user
                                    database

        Returns:
            Dict

        """
        database = database or self.database

        try:
            if self._db_interface.use_winchester_gs:
                r = self._db_interface.put(
                    path=f"/geodb_geoserver/{database}/collections/",
                    payload={"collection_id": collection},
                )
            else:
                r = self._db_interface.put(
                    path=f"/api/v2/services/xcube_geoserv/databases/"
                    f"{database}/collections",
                    payload={"collection_id": collection},
                )
            self._log_event(
                EventType.PUBLISHED_GS, f"collection {database}_{collection}"
            )
            return r.json()
        except GeoDBError as e:
            return self._maybe_raise(e)

    def get_all_published_gs(
        self, database: Optional[str] = None
    ) -> Union[Sequence, Message]:
        """

        Args:
            database (str): The database to list collections from a database which are published via geoserver

        Returns:
            A Dataframe of collection names

        """

        path = (
            "/api/v2/services/xcube_geoserv/collections" if database is None else None
        )

        try:
            r = self._db_interface.get(path=path)
            js = r.json()
            if js:
                return DataFrame.from_dict(js)
            else:
                return DataFrame(columns=["collection"])
        except GeoDBError as e:
            return self._maybe_raise(e)

    def get_published_gs(
        self, database: Optional[str] = None
    ) -> Union[Sequence, Message]:
        """

        Args:
            database (str): The database to list collections from a database which are published via geoserver

        Returns:
            A Dataframe of collection names

        Examples:
            >>> geodb = GeoDBClient(auth_mode='client-credentials', client_id='***', client_secret='***')
            >>> geodb.get_published_gs()
                owner	                        database	                    collection
            0	geodb_9bfgsdfg-453f-445b-a459	geodb_9bfgsdfg-453f-445b-a459	land_use

        """

        database = database or self._database

        if self._db_interface.use_winchester_gs:
            path = f"/geodb_geoserver/{database}/collections"
        else:
            path = f"/api/v2/services/xcube_geoserv/databases/{database}/collections"

        try:
            r = self._db_interface.get(path=path)
            js = r.json()
            if js:
                return DataFrame.from_dict(js)
            else:
                return DataFrame(columns=["collection"])
        except GeoDBError as e:
            return self._maybe_raise(e)

    def unpublish_gs(self, collection: str, database: str):
        """
        'Unpublishes' collection from a BC geoservice (geoserver instance).
        Requires access registration.

        Args:
            collection (str): Name of the collection
            database (Optional[str]): Name of the database. Defaults to user
                                      database

        Returns:
            Dict

        """

        database = database or self._database

        try:
            if self._db_interface.use_winchester_gs:
                self._db_interface.delete(
                    path=f"/geodb_geoserver/{database}/collections/{collection}"
                )
            else:
                self._db_interface.delete(
                    path=f"/api/v2/services/xcube_geoserv/databases/"
                    f"{database}/collections/{collection}"
                )
            self._log_event(
                EventType.UNPUBLISHED_GS, f"collection {database}_{collection}"
            )
            return Message(
                f"Collection {collection} in database {database} "
                f"deleted from Geoservice"
            )
        except GeoDBError as e:
            return self._maybe_raise(e)

    def create_group(self, group_name: str) -> Message:
        """
        Creates a new group with the given name, and makes the current user
        admin of that group: the current user can now add or remove users to
        and from the group, and all group users can publish or unpublish their
        collections to or from the group. Per default, no collection is
        published to the group.
        This function is restricted to users with a subscription of type
        'manage' (or related).

        Args:
            group_name (str): Name of the group to create.

        Returns:
            A message if the group was successfully created.

        Raises:
            GeoDBError if the group already exists.
        """
        path = "/rpc/geodb_create_role"
        payload = {
            "user_name": self.whoami,
            "user_group": group_name,
        }

        self._db_interface.post(path=path, payload=payload)
        self._log_event(EventType.GROUP_CREATED, group_name)
        return Message(f"Created new group {group_name}.")

    def add_user_to_group(self, user: str, group: str) -> Message:
        """
        Adds the user to the given group.

        Args:
            user (str): Name of the user.
            group (str): Name of the group.

        Returns:
            A message if the user was added to the group.

        Raises:
            GeoDBError if (1) the user does not exist, (2) the group does not
            exist, (3) the current user does not have
            sufficient rights to add the user to the group.
        """

        path = "/rpc/geodb_group_grant"
        payload = {
            "user_name": user,
            "user_group": group,
        }

        self._db_interface.post(path=path, payload=payload)
        self._log_event(EventType.GROUP_ADDED, f"{user}, {group}")
        return Message(f"Added user {user} to {group}")

    def remove_user_from_group(self, user: str, group: str) -> Message:
        """
        Removes the user from the given group.

        Args:
            user (str): Name of the user.
            group (str): Name of the group.

        Returns:
            A message if the user was removed from the group.

        Raises:
            GeoDBError if (1) the user does not exist, (2) the group does not
            exist, (3) the current user does not have
            sufficient rights to remove the user from the group.
        """

        path = "/rpc/geodb_group_revoke"
        payload = {
            "user_name": user,
            "user_group": group,
        }

        self._db_interface.post(path=path, payload=payload)
        self._log_event(EventType.GROUP_REMOVED, f"{user}, {group}")
        return Message(f"Removed user {user} from {group}")

    def publish_collection_to_group(
        self, collection: str, group: str, database: Optional[str] = None
    ) -> Message:
        """
        Publishes the collection to the given group. Group members get read
        and write access to the collection; they
        cannot publish the collection to other users or groups.
        This is only allowed if the current user is the owner of the
        collection.

        Args:
            collection (str): The collection's name
            group (str): Name of the group.
            database (str): The name of the database the collection resides
            in [current database].

        Returns:
            A message if the collection was published to the group.

        Raises:
            GeoDBError if (1) the collection does not exist, (2) the group
            does not exist,
            (3) the current user is not the owner of the collection.
        """

        database = database or self.database
        dn = f"{database}_{collection}"

        if not self._is_owner_of(dn):
            raise GeoDBError(
                f"User {self.whoami} must be owner of collection {dn} to publish."
            )

        path = "/rpc/geodb_group_publish_collection"
        payload = {
            "collection": dn,
            "user_group": group,
        }

        self._db_interface.post(path=path, payload=payload)
        self._log_event(EventType.PUBLISHED_GROUP, f"{collection}, {group}")
        return Message(
            f"Published collection {collection} in database "
            f"{database} to group {group}."
        )

    def unpublish_collection_from_group(
        self, collection: str, group: str, database: Optional[str] = None
    ) -> Message:
        """
        Unpublishes the collection from the given group.

        Args:
            collection (str): The collection's name
            database (str): The name of the database the collection resides
            in [current database].
            group (str): Name of the group.

        Returns:
            A message if the collection was unpublished from the group.

        Raises:
            GeoDBError if (1) the collection does not exist, (2) the group
            does not exist,
            (3) the current user is not the owner of the collection.
        """

        database = database or self.database
        dn = f"{database}_{collection}"

        if not self._is_owner_of(dn):
            raise GeoDBError(
                f"User {self.whoami} must be owner of collection {dn} to unpublish."
            )

        path = "/rpc/geodb_group_unpublish_collection"
        payload = {
            "collection": dn,
            "user_group": group,
        }

        self._db_interface.post(path=path, payload=payload)
        self._log_event(EventType.UNPUBLISHED_GROUP, f"{collection}, {group}")
        return Message(
            f"Unpublished collection {collection} in database "
            f"{database} from group {group}."
        )

    def publish_database_to_group(
        self, group: str, database: Optional[str] = None
    ) -> Message:
        """
        Publishes the database to the given group and enables group users to write into the same database.
        For example, if some user A creates the database `someproject`, users that share a group G with A are
        allowed to create new collections in that database (like `someproject_insitudata`) if A publishes the database
        to the group G. Group users shall not, however, be allowed to read or write any existing collection in the
        database; this needs extra permission by the group admin, using the already existing function
        `publish_collection_to_group`.

        Args:
            database (str): The name of the database to publish. Default: the username.
            group (str): Name of the group.
        """
        database = database or self.database
        dn = f"{database}_dummy"
        if not self._is_owner_of(dn):
            raise GeoDBError(
                f"User {self.whoami} must be owner of database {database} to publish."
            )

        path = "/rpc/geodb_group_publish_database"
        payload = {
            "database": database,
            "user_group": group,
        }

        self._db_interface.post(path=path, payload=payload)
        self._log_event(EventType.PUBLISHED_DATABASE, f"{database}, {group}")
        return Message(f"Published database {database} to group {group}.")

    def unpublish_database_from_group(
        self, group: str, database: Optional[str] = None
    ) -> Message:
        """
        Unpublishes the database from the given group.

        Args:
            database (str): The name of the database to unpublish. Default: the username.
            group (str): Name of the group.
        """
        database = database or self.database
        dn = f"{database}_dummy"
        if not self._is_owner_of(dn):
            raise GeoDBError(
                f"User {self.whoami} must be owner of database {database} to unpublish."
            )

        path = "/rpc/geodb_group_unpublish_database"
        payload = {
            "database": database,
            "user_group": group,
        }

        self._db_interface.post(path=path, payload=payload)
        self._log_event(EventType.UNPUBLISHED_DATABASE, f"{database}, {group}")
        return Message(f"Unpublished database {database} from group {group}.")

    def get_my_groups(self):
        """
        Returns the different group memberships of the current user.

        Returns:
            The different group memberships of the current user.
        """
        path = "/rpc/geodb_get_user_roles"
        names = self._db_interface.post(
            path=path, payload={"user_name": self.whoami}
        ).json()[0]["src"]
        return sorted(
            [name["rolname"] for name in names if not name["rolname"] == self.whoami]
        )

    def get_group_users(self, group: str) -> List:
        """
        Returns the users that are member of the given group.

        Returns:
            the users that are member of the given group.
        """
        path = "/rpc/geodb_get_group_users"
        result = self._db_interface.post(
            path=path, payload={"group_name": group}
        ).json()
        names = result[0]["res"]
        return sorted([name["rolname"] for name in names])

    def get_access_rights(
        self, collection: str, database: Optional[str] = None
    ) -> Dict:
        """
        Returns the access rights on the given collection.

        Returns:
            The access rights on the collection of the current user and all
            groups the user is in.

        Raises:
            GeoDBError if the collection does not exist
        """

        database = database or self.database
        dn = f"{database}_{collection}"

        path = "/rpc/geodb_get_grants"
        result = self._db_interface.post(path=path, payload={"collection": dn}).json()
        df = DataFrame(result[0]["res"])
        df = df.groupby("grantee")["privilege_type"].apply(list)
        return df.to_dict()

    def _is_owner_of(self, dn) -> bool:
        path = "/rpc/geodb_user_allowed"
        payload = {
            "collection": dn,
            "usr": self.whoami,
        }
        r = self._db_interface.post(path=path, payload=payload)
        return int(r.text) > 0

    def get_metadata(
        self, collection: str, database: Optional[str] = None
    ) -> "Metadata":
        """
        Retrieves the metadata for the given collection and database. The metadata
        fields are compliant to the STAC collection specification, v1.1.0.

        :param collection: the collection name to retrieve the metadata for
        :param database: the database the collection is stored in
        :return: the metadata
        """
        database = database or self.database

        path = "/rpc/geodb_get_metadata"
        payload = {"collection": collection, "db": database}

        result = self._db_interface.post(path=path, payload=payload).json()
        return self._metadata_manager.from_json(result, collection, database)

    def set_metadata_field(
        self,
        field: str,
        value: Any,
        collection: str,
        database: Optional[str] = None,
    ) -> None | DataFrame | Message:
        """(str): The collection to set the metadat
        Sets the given metadata field for the given collection. See the STAC
        'collection' specification for details on the possible field values.

        Args:
            field (str): The metadata field to set. Valid fields to set are:
                         title (str), description (str), license (str),
                         keywords (List[str]), providers (List[Provider]),
                         stac_extensions (List[str]), links (List[Link]),
                         summaries (Dict[str, Summary]), assets (List[Asset]),
                         item_assets(Dict[str, ItemAsset]),
                         temporal_extent (TemporalExtent)
            value (Any): The value to set, provided in the type that the field expects
                         (see above documentation for 'field'-parameter for the
                         expected types).
            collection (str): The collection to set the metadata field for
            database (str): The database to set the metadata field for
            >>> GeoDBClient.set_metadata_field("title", "my title", "my_collection")
            >>> GeoDBClient.set_metadata_field("description", "my description", "my_collection")
            >>> GeoDBClient.set_metadata_field("license", "my license", "my_collection")
            >>> GeoDBClient.set_metadata_field("links", [Link.from_json({'href': 'https://link2.bc', 'rel': 'root'})], "my_collection")
            >>> GeoDBClient.set_metadata_field("keywords", ['crops', 'europe', 'rural'], "my_collection")
            >>> GeoDBClient.set_metadata_field("stac_extensions", ['https://stac-extensions.github.io/authentication/v1.1.0/schema.json'], "my_collection")
            >>> GeoDBClient.set_metadata_field("providers", [Provider.from_json({'name': 'provider 1', 'description': 'some provider', 'roles': ['licensor', 'producer']})], "my_collection")
            >>> GeoDBClient.set_metadata_field("summaries", {'columns': ['id', 'geometry'], 'x_range': {'min': '-170', 'max': '170'}, 'y_range': {'min': '-80', 'max': '80'}, 'schema': 'some JSON schema'}, "my_collection")
            >>> GeoDBClient.set_metadata_field("assets", [Asset.from_json({'href': 'https://asset.org', 'title': 'some title', 'roles': ['image', 'ql']})], "my_collection")
            >>> GeoDBClient.set_metadata_field("item_assets", [ItemAsset.from_json({'href': 'https://asset.org', 'type': 'some type'})], "my_collection")
            >>> GeoDBClient.set_metadata_field("temporal_extent", [['2018-01-01T00:00:00Z', None], "my_collection")
        """
        database = database or self.database
        if field == "title" or field == "description" or field == "license":
            jsonified_value = f'"{value}"'
        elif field == "keywords" or field == "stac_extensions":
            value: List[str]
            jsonified_value = [f"{v}" for v in value]
        elif field == "links":
            value: List["Link"]
            jsonified_value = [
                {
                    "href": v.href,
                    "rel": v.rel,
                    "type": v.type,
                    "title": v.title,
                    "method": v.method,
                    "body": v.body,
                    "headers": v.headers,
                }
                for v in value
            ]
        elif field == "providers":
            value: List["Provider"]
            jsonified_value = [
                {
                    "description": v.description,
                    "url": v.url,
                    "name": v.name,
                    "roles": v.roles,
                }
                for v in value
            ]
        elif field == "assets":
            value: List["Asset"]
            jsonified_value = [
                {
                    "description": v.description,
                    "title": v.title,
                    "href": v.href,
                    "type": v.type,
                    "roles": v.roles,
                }
                for v in value
            ]
        elif field == "item_assets":
            value: List["ItemAsset"]
            jsonified_value = [
                {
                    "description": v.description,
                    "title": v.title,
                    "type": v.type,
                    "roles": v.roles,
                }
                for v in value
            ]
        elif field == "summaries" or field == "temporal_extent":
            jsonified_value = value
        else:
            raise ValueError(f"Invalid field: {field}")

        path = "/rpc/geodb_set_metadata_field"
        payload = {
            "field": field,
            "value": jsonified_value,
            "collection": collection,
            "db": database,
        }

        try:
            self._db_interface.post(path=path, payload=payload)
        except GeoDBError as e:
            return self._maybe_raise(e, return_df=True)

    def refresh_auth_access_token(self):
        """
        Refresh the authentication token.

        """
        self._db_interface._auth_access_token = None

    def collection_exists(self, collection: str, database: str) -> bool:
        """
        Checks whether a collection exists

        Args:
            collection (str): The collection's name
            database (str): The name of the database the collection resides in [current database]
        Returns:
             Whether the collection exists
        """
        database = database or self.database

        self._do_log_read = False
        try:
            self.head_collection(collection, database=database)
        except GeoDBError:
            return False

        return True

    def _raise_for_collection_exists(self, collection: str, database: str) -> bool:
        """

        Args:
            collection (str): Name of the collection

        Returns:
            Whether the collection exists

        Raises:
            GeoDBError if the collection does not exist
        """

        collection_exists = self.collection_exists(collection, database=database)
        if collection_exists is True:
            return True
        else:
            raise GeoDBError(f"Collection {collection} does not exist")

    def _raise_for_stored_procedure_exists(self, stored_procedure: str) -> bool:
        """

        Args:
            stored_procedure (str): Name of the stored procedure

        Returns:
            Whether the stored procedure exists

        Raises:
            GeoDBError if the stored procedure does not exist
        """
        if f"/rpc/{stored_procedure}" in self.capabilities["paths"]:
            return True
        else:
            raise GeoDBError(f"Stored procedure {stored_procedure} does not exist")

    def get_event_log(
        self,
        collection: Optional[str] = None,
        database: Optional[str] = None,
        event_type: Optional[EventType] = None,
    ) -> DataFrame:
        """
        Args:
            collection (str):       The name of the collection for which to get
                                    the event log; if None, all collections are
                                    returned
            database (str):         The database of the collection
            event_type (EventType): The type of the events for which to get
                                    the event log; if None, all events are
                                    returned

        Returns:
            Whether the stored procedure exists

        Raises:
            GeoDBError if the stored procedure does not exist
        """
        path = "/rpc/get_geodb_eventlog"
        if event_type or collection or database:
            path = f"{path}?"
        if event_type:
            path = f"{path}event_type={event_type}"

        if collection or database:
            database = database or self.database
            collection = collection or "%"
            dn = f"{database}_{collection}"
            path = f"{path}&collection={dn}"

        try:
            result = self._db_interface.get(path=path).json()[0]
            if result["events"]:
                return DataFrame.from_dict(result["events"])
            else:
                return DataFrame(columns=["event_type", "message", "username", "date"])
        except GeoDBError as e:
            return self._maybe_raise(e)

    def _log_event(self, event_type: str, message: str) -> requests.models.Response:
        event = {"event_type": event_type, "message": message, "user": self.whoami}
        return self._db_interface.post(
            path="/rpc/geodb_log_event",
            payload=event,
            headers={"Prefer": "params=single-object"},
        )
