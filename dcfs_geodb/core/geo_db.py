from typing import Dict, Optional

import fastjsonschema
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely import wkb
import requests
import json


from dcfs_geodb.config import GEODB_API_DEFAULT_CONNECTION_PARAMETERS, JSON_API_VALIDATIONS_CREATE_DATASET


class GeoDB(object):
    def __init__(self, server_url: Optional[str] = None, server_port: Optional[int] = None):
        self._set_defaults()

        if server_url:
            self._server_url = server_url
        if server_port:
            self._server_port = server_port

        self._capabilities = {'paths': {}}

    def _set_defaults(self):
        if 'server_url' in GEODB_API_DEFAULT_CONNECTION_PARAMETERS:
            self._server_url = GEODB_API_DEFAULT_CONNECTION_PARAMETERS['server_url']
        if 'server_port' in GEODB_API_DEFAULT_CONNECTION_PARAMETERS:
            self._server_port = GEODB_API_DEFAULT_CONNECTION_PARAMETERS['server_port']

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
        return f"/{dataset}" in self._capabilities['paths']

    def _stored_procedure_exists(self, stored_procedure: str) -> bool:
        """

        Args:
            stored_procedure: Name of stored pg procedure

        Returns:
            bool whether the stored procedure exists in DB
        """
        return f"/rpc/{stored_procedure}" in self._capabilities['paths']

    def post(self, path: str, body: Dict, params: Optional[Dict] = None) -> requests.models.Response:
        """

        Args:
            path: API path
            body: Post body as Dict. Will be dumped to JSON
            params: Request parameters

        Returns:
            A Request object
        Examples:
            >>> api = GeoDB()
            >>> api.post(path='/rpc/my_function', body={'name': 'MyName'})
        """
        r = requests.post(self._get_full_url(path=path), json=body, params=params)
        r.raise_for_status()
        return r

    def get(self, path: str, params: Optional[Dict] = None) -> requests.models.Response:
        """

        Args:
            path: API path
            params: Request parameters

        Returns:
            A Request object

        Examples:
            >>> api = GeoDB()
            >>> api.get(path='/my_table', params={"limit": 1})

        """
        r = requests.get(self._get_full_url(path=path), params=params)
        r.raise_for_status()
        return r

    def delete(self, path: str, params: Optional[Dict] = None):
        """

        Args:
            path: API path
            params: Request parameters

        Returns:
            A Request object

        Examples:
            >>> api = GeoDB()
            >>> api.delete(path='/my_table', params={"limit": 1})

        """

        r = requests.delete(self._get_full_url(path=path), params=params)
        r.raise_for_status()
        return r

    def patch(self, path: str, body: Dict, params: Optional[Dict] = None):
        """

        Args:
            path: API path
            params: Request parameters

        Returns:
            A Request object

        Examples:
            >>> api = GeoDB()
            >>> api.patch(path='/my_table', params={"limit": 1})

        """

        r = requests.patch(self._get_full_url(path=path), json=body, params=params)
        r.raise_for_status()
        return r

    def delete_from_dataset(self, dataset: str, query: str):
        self.delete(f'/{dataset}?{query}')

    def update_dataset(self, dataset: str, query: str, values: Dict):
        self.patch(f'/{dataset}?{query}', body=values)

    def insert_into_dataset(self, dataset: str, query: str, values: Dict):
        self.patch(f'/{dataset}?{query}', body=values)

    def filter_by_bbox(self, dataset: str, minx, miny, maxx, maxy, bbox_mode: str = 'contains', bbox_crs: int = 4326,
                       limit: int = 0, offset: int = 0) \
            -> gpd.GeoDataFrame:
        """

        Args:
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
        Examples:
            >>> api = GeoDB()
            >>> api.filter_by_bbox(table="land_use",minx=452750.0, miny=88909.549, maxx=464000.0, maxy=102486.299, bbox_mode="contains", bbox_crs=3794, lmt=1000, offst=10)
        """
        r = self.post('/rpc/get_by_bbox', body={
            "table_name": dataset,
            "minx": minx,
            "miny": miny,
            "maxx": maxx,
            "maxy": maxy,
            "bbox_mode": bbox_mode,
            "bbox_crs": bbox_crs,
            "lmt": limit,
            "offst": offset
            })

        js = r.json()[0]['src']
        if js:
            data = [self._load_geo(d) for d in js]

            gpdf = gpd.GeoDataFrame(data).set_geometry('geometry')
            if not self._validate(gpdf):
                raise ValueError("Geometry field is missing")
            return gpdf
        else:
            raise ValueError("Result is empty")

    def filter(self, dataset: str, query: str) -> GeoDataFrame:
        r = self.get(f"/{dataset}?{query}")
        js = r.json()

        if js:
            return self._gpdf_from_json(js)
        else:
            raise ValueError("Result is empty")

    def _gpdf_from_json(self, js: str):
        data = [self._load_geo(d) for d in js]

        gpdf = gpd.GeoDataFrame(data).set_geometry('geometry')

        if not self._validate(gpdf):
            raise ValueError("Geometry field is missing")

        return gpdf

    def get_capabilities(self) -> json:
        """
            Get a list of WebAPI endpoints.
            Returns:
                A json object containing all endpoints and capability information about the server.
        """
        r = self.get('/')
        return r.json()

    def _get_full_url(self, path: str) -> str:
        if self._server_port:
            return f"{self._server_url}:{self._server_port}{path}"
        else:
                return f"{self._server_url}{path}"

    def _load_geo(self, d):
        if 'geometry' in d:
            d['geometry'] = wkb.loads(d['geometry'], hex=True)
        return d

    def create_dataset(self, name: str, properties: json) -> bool:
        validate = fastjsonschema.compile(
            JSON_API_VALIDATIONS_CREATE_DATASET['validation'],
            formats=JSON_API_VALIDATIONS_CREATE_DATASET['formats']
        )

        validate(properties)
        self.post(path='/rpc/geodb_create_dataset', body={'name': name, 'properties': properties})

        return True

    def drop_dataset(self, name: str, properties: json) -> bool:
        validate = fastjsonschema.compile(
            JSON_API_VALIDATIONS_CREATE_DATASET['validation'],
            formats=JSON_API_VALIDATIONS_CREATE_DATASET['formats']
        )

        validate(properties)
        self.post(path='/rpc/geodb_drop_dataset', body={'name': name, 'properties': properties})

        return True

    def add_properties(self, dataset: str, properties: json) -> bool:
        validate = fastjsonschema.compile(
            JSON_API_VALIDATIONS_CREATE_DATASET['validation'],
            formats=JSON_API_VALIDATIONS_CREATE_DATASET['formats']
        )

        validate(properties)
        self.post(path='/rpc/geodb_add_properties', body={'dataset': dataset, 'properties': properties})

        return True


if __name__ == "__main__":
    api = GeoDB()
    api.filter_by_bbox(dataset="land_use", minx=452750.0, miny=88909.549, maxx=464000.0, maxy=102486.299,
                       bbox_mode="contains", bbox_crs=3794, limit=1000)

    print(api.filter('land_use', 'id=gt.100'))
    print('Finished')

