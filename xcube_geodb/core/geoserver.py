import os

import requests
from geoserver.catalog import Catalog

from xcube_geodb.core.message import Message


class GeoserverUser:
    def __init__(self, admin_user_name: str = None, admin_pwd: str = None, url: str = None):
        self._admin_user_name = admin_user_name or os.environ.get("GEOSERVER_ADMIN_USER")
        self._admin_pwd = admin_pwd or os.environ.get("GEOSERVER_ADMIN_PASSWORD")
        self._url = url or os.environ.get("GEOSERVER_URL")

    @property
    def admin_user_name(self):
        return self._admin_user_name

    @property
    def url(self):
        return self._url

    def register_user(self, user_name: str, password: str) -> Message:
        """
        Registers a user in the PostGres database. Needs admin privileges.

        Args:
            user_name: User name of the user
            password: Password for the user

        Returns:
            str: Success message
        """
        geoserver_url = f"{self._url}/rest/security/usergroup/users"

        user = {
            "org.geoserver.rest.security.xml.JaxbUser": {
                "userName": user_name,
                "password": password,
                "enabled": True
            }
        }

        r = requests.post(geoserver_url, json=user, auth=(self._admin_user_name, self._admin_pwd))
        r.raise_for_status()

        return Message(f"User {user_name} successfully added")

    def register_user_catalog_to_geoserver(self, cat_name: str) -> Message:
        cat = Catalog(self._url + "/rest/", username=self._admin_user_name, password=self._admin_pwd)
        ws = cat.get_workspace(cat_name)
        if not ws:
            cat.create_workspace(cat_name)

        return Message(f"Catalog {cat_name} successfully added")

    def register_user_datastore_to_geoserver(self, user_name: str, password: str):
        admin_user = os.environ.get("GEOSERVER_ADMIN_USER")
        admin_pwd = os.environ.get("GEOSERVER_ADMIN_PASSWORD")

        geoserver_url = f"{self.geoserver_url}/rest/workspaces/{user_name}/datastores"

        db = {
            "dataStore": {
                "name": user_name + "_geodb",
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

    def register_user_access_to_geoserver(self, user_name: str):
        admin_user = os.environ.get("GEOSERVER_ADMIN_USER")
        admin_pwd = os.environ.get("GEOSERVER_ADMIN_PASSWORD")

        rule = {
            "org.geoserver.rest.security.xml.JaxbUser": {
                "rule": {
                    "@resource": f"{user_name}.*.a",
                    "text": "GROUP_ADMIN"
                }
            }
        }

        geoserver_url = f"{self.geoserver_url}/rest/security/acl/layers"

        r = requests.post(geoserver_url, json=rule, auth=(admin_user, admin_pwd))
        r.raise_for_status()

        return Message(f"User {user_name} successfully added")
