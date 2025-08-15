# The MIT License (MIT)
# Copyright (c) 2025 by the xcube team
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
import json
import urllib
from functools import cached_property
from typing import Union, Dict, Sequence, Optional

import requests

from xcube_geodb.core.error import GeoDBError


class DbInterface:
    def __init__(
        self,
        server_url,
        server_port,
        gs_server_url,
        gs_server_port,
        auth_mode,
        auth_client_id,
        auth_client_secret,
        auth_username,
        auth_password,
        auth_access_token,
        auth_domain,
        auth_aud,
        auth_access_token_uri,
    ):
        self._auth_access_token_uri = auth_access_token_uri
        self._auth_mode = auth_mode
        self._auth_aud = auth_aud
        self._auth_domain = auth_domain
        self._auth_access_token = auth_access_token
        self._auth_password = auth_password
        self._auth_username = auth_username
        self._auth_client_secret = auth_client_secret
        self._auth_client_id = auth_client_id
        self._gs_server_url = gs_server_url
        self._gs_server_port = gs_server_port
        self._server_port = server_port
        self._server_url = server_url

    @property
    def auth_access_token(self) -> str:
        """
        Get the user's access token.

        Returns:
            The current authentication access_token

        Raises:
            GeoDBError on missing ipython shell
        """

        access_token_uri = self._auth_access_token_uri

        return (
            self._auth_access_token
            or self._get_geodb_client_credentials_access_token(
                token_uri=access_token_uri
            )
        )

    def post(
        self,
        path: str,
        payload: Union[Dict, Sequence],
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        raise_for_status: bool = True,
    ) -> requests.models.Response:
        """

        Args:
            headers [Optional[Dict]]: Request headers. Allows Overriding common header entries.
            path (str): API path
            payload (Union[Dict, Sequence]): Post body as Dict or Sequence. Will be dumped to JSON
            params Optional[Dict]: Request parameters
            raise_for_status (bool): raise or not if status is not 200-299 [True]
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
            if common_headers["Content-type"] == "text/csv":
                r = requests.post(
                    self._get_full_url(path=path),
                    data=payload,
                    params=params,
                    headers=common_headers,
                )
            else:
                r = requests.post(
                    self._get_full_url(path=path),
                    json=payload,
                    params=params,
                    headers=common_headers,
                )
            if raise_for_status:
                r.raise_for_status()
        except requests.exceptions.HTTPError:
            raise GeoDBError(r.text)

        return r

    def get(self, path: str, params: Optional[Dict] = None) -> requests.models.Response:
        """

        Args:
            path (str): API path
            params (Optional[Dict]): Request parameters

        Returns:
            requests.models.Response: A Response object

        Raises:
            GeoDBError: If the database raises an error
            HttpError: If the request fails
        """

        r = None
        try:
            r = requests.get(
                self._get_full_url(path=path),
                params=params,
                headers=(self._get_common_headers()),
            )
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            raise GeoDBError(r.content)

        return r

    def delete(
        self, path: str, params: Optional[Dict] = None, headers: Optional[Dict] = None
    ) -> requests.models.Response:
        """

        Args:
            headers (Optional[Dict]): Request headers. Allows Overriding common header entries.
            path (str): API path
            params (Optional[Dict]): Request parameters

        Returns:
            requests.models.Response: A Request object

        Raises:
            GeoDBError: If the database raises an error
            HttpError: If the request fails
        """

        common_headers = self._get_common_headers()
        headers = (
            common_headers.update(headers) if headers else self._get_common_headers()
        )

        r = None
        try:
            r = requests.delete(
                self._get_full_url(path=path), params=params, headers=headers
            )
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            raise GeoDBError(r.text)
        return r

    def patch(
        self,
        path: str,
        payload: Union[Dict, Sequence],
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
    ) -> requests.models.Response:
        """

        Args:
            headers (Optional[Dict]): Request headers. Allows Overriding common header entries.
            payload (Union[Dict, Sequence]): Post body as Dict. Will be dumped to JSON
            path (str): API path
            params (Optional[Dict]): Request parameters

        Returns:
            requests.models.Response: A Request object

        Raises:
            GeoDBError: If the database raises an error
            HttpError: If the request fails
        """

        common_headers = self._get_common_headers()
        headers = (
            common_headers.update(headers) if headers else self._get_common_headers()
        )

        r = None
        try:
            r = requests.patch(
                self._get_full_url(path=path),
                json=payload,
                params=params,
                headers=headers,
            )
            r.raise_for_status()
        except requests.HTTPError:
            raise GeoDBError(r.text)
        return r

    def put(
        self,
        path: str,
        payload: Union[Dict, Sequence],
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
    ) -> requests.models.Response:
        """

        Args:
            headers (Optional[Dict]): Request headers. Allows Overriding common header entries.
            payload (Union[Dict, Sequence]): Post body as Dict. Will be dumped to JSON
            path (str): API path
            params (Optional[Dict]): Request parameters

        Returns:
            requests.models.Response: A Request object

        Raises:
            GeoDBError: If the database raises an error
        """

        common_headers = self._get_common_headers()
        headers = (
            common_headers.update(headers) if headers else self._get_common_headers()
        )

        r = None
        try:
            r = requests.put(
                self._get_full_url(path=path),
                json=payload,
                params=params,
                headers=headers,
            )
            r.raise_for_status()
            return r
        except requests.HTTPError:
            raise GeoDBError(r.text)

    def _get_common_headers(self):
        if self._auth_mode == "none":
            return {
                "Prefer": "return=representation",
                "Content-type": "application/json",
            }
        else:
            return {
                "Prefer": "return=representation",
                "Content-type": "application/json",
                "Authorization": f"Bearer {self.auth_access_token}",
            }

    def _get_full_url(self, path: str) -> str:
        """

        Args:
            path (str): PostgREST API path

        Returns:
            str: Full URL and path
        """

        server_url = self._server_url
        server_port = self._server_port

        if "services/xcube_geoserv" in path:
            server_url = self._gs_server_url
            server_port = self._gs_server_port

        if self.use_winchester_gs and "geodb_geoserver" in path:
            server_url = self._gs_server_url
            server_port = None

        if server_port:
            return f"{server_url}:{server_port}{path}"
        else:
            return f"{server_url}{path}"

    def _get_geodb_client_credentials_access_token(
        self, token_uri: str = "/oauth/token", is_json: bool = True
    ) -> str:
        """
        Get access token from client credentials

        Args:
            token_uri (str): oauth2 token URI
            is_json: whether the request has to be of content type json

        Returns:
             An access token

        Raises:
            GeoDBError, HttpError
        """

        if self._auth_mode == "client-credentials":
            self._raise_for_invalid_client_credentials_cfg()
            payload = {
                "client_id": self._auth_client_id,
                "client_secret": self._auth_client_secret,
                "audience": self._auth_aud,
                "grant_type": "client_credentials",
            }
            headers = {"content-type": "application/json"} if is_json else None
            r = requests.post(
                self._auth_domain + token_uri, json=payload, headers=headers
            )
        elif self._auth_mode == "password":
            self._raise_for_invalid_password_cfg()
            payload = {
                "client_id": self._auth_client_id,
                "username": self._auth_username,
                "password": self._auth_password,
                "grant_type": "password",
            }

            if self._auth_aud:
                payload["audience"] = self._auth_aud
            if self._auth_client_secret:
                payload["client_secret"] = self._auth_client_secret

            headers = {"content-type": "application/x-www-form-urlencoded"}
            r = requests.post(
                self._auth_domain + token_uri, data=payload, headers=headers
            )
        else:
            raise GeoDBError("System Error: auth mode unknown.")

        r.raise_for_status()

        data = r.json()

        try:
            self._auth_access_token = data["access_token"]
            return data["access_token"]
        except KeyError:
            raise GeoDBError(
                "The authorization request did not return an access token."
            )

    def _raise_for_invalid_client_credentials_cfg(self) -> bool:
        """
        Raise when the client-credentials configuration is wrong.

        Returns:
             True on success

        Raises:
            GeoDBError on invalid configuration
        """
        if (
            self._auth_client_id
            and self._auth_client_secret
            and self._auth_aud
            and self._auth_mode == "client-credentials"
        ):
            return True
        else:
            raise GeoDBError("System: Invalid client_credentials configuration.")

    def _raise_for_invalid_password_cfg(self) -> bool:
        """
        Raise when the password configuration is wrong.

        Returns:
             True on success

        Raises:
            GeoDBError on invalid configuration
        """
        if (
            self._auth_username
            and self._auth_password
            and self._auth_client_id
            and self._auth_mode == "password"
        ):
            return True
        else:
            raise GeoDBError("System: Invalid password flow configuration")

    @cached_property
    def use_winchester_gs(self) -> bool:
        try:
            # check if Winchester is the interface to the Geoserver:
            # extract base URL from the authentication URL and retrieve the server's meta
            # information, look for 'winchester' in its list of APIs. Returns "False" if
            # the meta information is structured differently, or does not contain
            # the 'winchester'-API.
            p = urllib.parse.urlparse(self._auth_domain)
            url = f"{p.scheme}://{p.netloc}"
            r = requests.get(url)
            apis = json.loads(r.content.decode())["apis"]
            for api in apis:
                if "winchester" in api["name"]:
                    return True
        except:
            pass
        return False
