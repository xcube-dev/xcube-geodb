# coding: utf-8

from urllib.parse import urljoin
import logging
from xml.etree.ElementTree import XML

import requests

from geoserver import settings


LOGGER = logging.getLogger("gsconfig2.catalog")


class User:
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

    def __init__(self, service_url,
                 username=settings.DEFAULT_USERNAME,
                 password=settings.DEFAULT_PASSWORD,
                 disable_ssl_certificate_validation=False):
        self._service_url = service_url
        self._username = username
        self._password = password
        self._disable_ssl_validation = disable_ssl_certificate_validation
        self._cache = dict()
        self._version = None
        self._session = requests.Session()
        self._session.auth = (self.username, self.password)

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

    def delete(self, config_object, purge=None, recurse=False):
        """
        send a delete request.
        :param recurse: True if underlying objects must be deleted recursively.
        :param purge:
        """
        rest_url = config_object.href
        params = {
            'purge': purge,
            'recurse': recurse,
        }
        headers = {
            "Content-type": "application/xml",
            "Accept": "application/xml"
        }
        r = self.session.delete(rest_url, params=params, headers=headers)
        if r.status_code == requests.codes.ok:
            return r.text
        else:
            msg = "Tried to make a DELETE request to " \
                  + "{} but got a {} status code: \n{}" \
                      .format(rest_url, r.status_code, r.text)
            raise FailedRequestError(msg)

    def reload(self):
        """
        Send a reload request to the GeoServer and clear the cache.
        :return: The response given by the GeoServer.
        """
        reload_url = urljoin(self.service_url, "reload")
        r = self.session.post(reload_url)
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

    def save(self, obj, content_type="application/json"):
        """
        saves an object to the REST service
        gets the object's REST location and the data from the object,
        then POSTS the request.
        :param obj: The object to save.
        :return: The response given by the server.
        """
        rest_url = obj.href
        message = obj.message()
        save_method = obj.save_method
        LOGGER.debug("{} {}".format(save_method, rest_url))
        methods = {
            settings.POST: self.session.post,
            settings.PUT: self.session.put
        }
        headers = {
            "Content-type": content_type,
            "Accept": content_type
        }
        r = methods[save_method](rest_url, data=message, headers=headers)
        self._cache.clear()
        if 400 <= r.status_code < 600:
            raise FailedRequestError(
                "Error code ({}) from GeoServer: {}" \
                    .format(r.status_code, r.text)
            )
        return r

    def get_user(self, name):
        # Make sure workspace is a workspace object and not a string.
        # If the workspace does not exist,
        # continue as if no workspace had been defined.
        pass

    def get_users(self):
        pass

    def create_user(self, name):
        pass


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
        msg = "Can't interpret {} as a name or a configuration object"
        raise ValueError(msg.format(named))


class UploadError(Exception):
    pass


class ConflictingDataError(Exception):
    pass


class AmbiguousRequestError(Exception):
    pass


class FailedRequestError(Exception):
    pass
