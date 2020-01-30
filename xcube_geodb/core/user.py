# coding: utf-8
import os
from abc import abstractmethod
from typing import Dict, Optional
from urllib.parse import urljoin
import logging
from xml.etree.ElementTree import XML
import requests
from dotenv import find_dotenv, load_dotenv
from geoserver import settings

from xcube_geodb.defaults import GEOSERVER_DEFAULT_PARAMETERS

LOGGER = logging.getLogger("gsconfig2.catalog")


class GeoserverSession:
    """
    The GeoServer catalog represents all of the information in the GeoServer
    configuration. This includes:
    - Stores of geospatial data
    - Resources, or individual coherent datasets within stores
    - Styles for resources
    - Layers, which combine styles with resources to create a visible map layer
    - LayerGroups, which alias one or more layers for convenience
    - Workspaces, which provide logical grouping of Stores
    - Maps, which provide a set of OWS services with a subset of the server's
        Layers
    - Namespaces, which provide unique identifiers for resources
    """

    def __init__(self, service_url: str = '',
                 username: str = settings.DEFAULT_USERNAME,
                 password: str = settings.DEFAULT_PASSWORD,
                 disable_ssl_certificate_validation: bool = False,
                 dotenv_file: str = '.env'):
        self._service_url = service_url
        self._username = username
        self._password = password
        self._disable_ssl_validation = disable_ssl_certificate_validation
        self._cache = dict()
        self._version = None
        self._session = requests.Session()
        self._session.auth = (self.username, self.password)

        self._dotenv_file = find_dotenv(filename=dotenv_file)
        if self._dotenv_file:
            load_dotenv(self._dotenv_file)

        self._load_from_env()

    def _load_from_env(self):
        self._service_url = os.environ.get("GEOSERVER_API_SERVER_URL") or self._service_url
        self._username = os.environ.get("GEOSERVER_ADMIN_USER") or self._username
        self._password = os.environ.get("GEOSERVER_ADMIN_PASSWORD") or self._password

    @property
    def service_url(self):
        return self._service_url

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    @property
    def disable_ssl_validation(self):
        return self._disable_ssl_validation

    @property
    def session(self):
        return self._session

    @property
    def version(self):
        return self._version

    def about(self):
        """
        :return: About informations of the geoserver as a formatted html.
        """
        about_url = urljoin(self.service_url,
                            "about/version.xml")
        r = self.session.get(about_url)
        if r.status_code == requests.codes.ok:
            return r.text
        raise FailedRequestError("Unable to determine version: {}"
                                 .format(r.text or r.status_code))

    # TODO: Make json
    def gsversion(self):
        """
        :return: The geoserver version.
        """
        if self.version:
            return self.version
        else:
            about_text = self.about()
            dom = XML(about_text)
            resources = dom.findall("resource")
            version = None
            for resource in resources:
                if resource.attrib["name"] == "GeoServer":
                    try:
                        version = resource.find("Version").text
                        break
                    except AttributeError:
                        pass
            if version is None:
                version = "<2.3.x"
            self._version = version
            return version

    def delete(self, path: str, purge=None, recurse: bool = False):
        """
        send a delete request.
        :param recurse: True if underlying objects must be deleted recursively.
        :param purge:

        Args:
            recurse (bool):
            purge:
            config_object:
        """
        delete_url = urljoin(self.service_url, path)

        params = {
            'purge': purge,
            'recurse': recurse,
        }

        headers = {
            "Content-type": "application/json",
            "Accept": "application/json"

        }

        r = self.session.delete(delete_url, params=params, headers=headers)
        r.raise_for_status()

    def reload(self):
        """
        Send a reload request to the GeoServer and clear the cache.
        :return: The response given by the GeoServer.
        """
        reload_url = urljoin(self.service_url, "reload")
        r = self.session.post(reload_url)
        self._cache.clear()
        return r

    def get(self, path: str, params: Optional[Dict] = None):
        """
        Send a reload request to the GeoServer and clear the cache.
        :return: The response given by the GeoServer.
        """
        get_url = urljoin(self.service_url, path)

        r = self.session.get(get_url, params=params)
        r.raise_for_status()

        self._cache.clear()
        return r

    def reset(self):
        """
        Send a reset request to the GeoServer and clear the cache.
        :return: The response given by the server.
        """
        reset_url = urljoin(self.service_url, "reset")
        r = self.session.post(reset_url)
        self._cache.clear()
        return r

    def post(self, path: str, payload: Dict, params: Optional[Dict] = None, content_type: str = "application/json"):
        """

        Args:
            path:
            payload:
            params:
            content_type:

        Returns:

        """
        return self._save(self.session.post, path, payload, params, content_type)

    def put(self, path: str, payload: Dict, params: Optional[Dict] = None, content_type: str = "application/json"):
        """

        Args:
            path:
            payload:
            params:
            content_type:

        Returns:

        """
        return self._save(self.session.put, path, payload, params, content_type)

    def _save(self, method, path: str, payload: Dict, params: Optional[Dict] = None,
              content_type: str = "application/json"):
        """
        saves an object to the REST service
        gets the object's REST location and the data from the object,
        then POSTS the request.
        :return: The response given by the server.

        Args:
            content_type (str):
            obj (GeoserverObject):
        """

        rest_url = urljoin(self.service_url, path)

        headers = {
            "Content-type": content_type,
            "Accept": content_type
        }

        r = method(rest_url, data=payload, params=params, headers=headers)
        r.raise_for_status()

        self._cache.clear()

        return r


class GeoServerObject:
    def __init__(self, session: GeoserverSession = None):
        if session is not None:
            self._session = session
        else:
            self._session = GeoserverSession()

    @abstractmethod
    @property
    def path(self):
        pass


class User(GeoServerObject):
    path = "/rest/usergroup/users"

    def __init__(self,
                 user_name: str = GEOSERVER_DEFAULT_PARAMETERS['user_name'],
                 password: str = GEOSERVER_DEFAULT_PARAMETERS['password'],
                 active: bool = True,
                 session: Optional[GeoserverSession] = None
                 ):
        super().__init__(session=session)

        self._username = user_name
        self._password = password
        self._active = active

    @property
    def user_name(self):
        return self._username

    @classmethod
    def load(cls, user_name: str, session: Optional[GeoserverSession] = None):
        session = session or GeoserverSession()

        r = session.get(urljoin(User.path, user_name))
        user_json = r.json()

        return cls(
            user_name=user_json['userName'],
            password=user_json['password'],
            active=user_json['enabled'],
            session=session
        )

    def save(self):
        pass

    @property
    def name(self):
        return self._username


def _name(named):
    """
    Get the name out of an object.  This varies based on the type of the input:
       * the "name" of a string is itself
       * the "name" of None is itself
       * the "name" of an object with a property named name is that property -
         as long as it's a string
       * otherwise, we raise a ValueError
    """
    if isinstance(named, str) or named is None:
        return named
    elif hasattr(named, 'name') and isinstance(named.name, str):
        return named.name
    else:
        msg = f"Can't interpret object as a name or a configuration object"
        raise ValueError(msg.format(named))


class UploadError(Exception):
    pass


class ConflictingDataError(Exception):
    pass


class AmbiguousRequestError(Exception):
    pass


class FailedRequestError(Exception):
    pass
