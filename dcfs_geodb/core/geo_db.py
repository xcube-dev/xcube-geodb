from typing import Dict, Optional, Union, Sequence

import fastjsonschema
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely import wkb
import requests
import json

from dcfs_geodb.config import GEODB_API_DEFAULT_CONNECTION_PARAMETERS, JSON_API_VALIDATIONS_CREATE_DATASET


class GeoDBError(ValueError):
    pass


class GeoDB(object):
    def __init__(self, server_url: Optional[str] = None, server_port: Optional[int] = None):
        """

        Args:
            server_url: The URL of the server
            server_port: The server port
        """
        self._set_defaults()

        if server_url:
            self._server_url = server_url
        if server_port:
            self._server_port = server_port

        self._capabilities = None

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

    @property
    def common_headers(self):
        return {
            'Prefer': 'return=representation',
            'Content-type': 'application/json'
        }

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
            >>> api = GeoDB()
            >>> api.post(path='/rpc/my_function', payload={'name': 'MyName'})
        """

        common_headers = self.common_headers

        if headers is not None:
            common_headers.update(headers)

        r = None
        try:
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
            >>> api = GeoDB()
            >>> api.get(path='/my_table', params={"limit": 1})

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
            >>> api = GeoDB()
            >>> api.delete(path='/my_table', params={"limit": 1})

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
            >>> api = GeoDB()
            >>> api.patch(path='/my_table', params={"limit": 1})

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
        """
        validate = fastjsonschema.compile(
            JSON_API_VALIDATIONS_CREATE_DATASET['validation'],
            formats=JSON_API_VALIDATIONS_CREATE_DATASET['formats']
        )

        # validate(datasets)
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
        """
        datasets = {'name': dataset, 'properties': properties, 'crs': crs}
        return self.create_datasets([datasets])

    def drop_dataset(self, dataset: str) -> requests.models.Response:
        """

        Args:
            dataset: Dataset to be dropped

        Returns:
            requests.models.Response:
        """
        return self.post(path='/rpc/geodb_drop_datasets', payload={'datasets': [dataset]})

    def drop_datasets(self, datasets: Sequence[str]) -> requests.models.Response:
        """

        Args:
            datasets: Datasets to be dropped

        Returns:
            requests.models.Response:
        """
        return self.post(path='/rpc/geodb_drop_datasets', payload={'datasets': datasets})

    def add_property(self, dataset: str, prop: str, typ: str) -> requests.models.Response:
        """

        Args:
            dataset: Dataset to add a property to
            prop: Property name
            typ: Type of property
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
        """
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
        """

        return self.drop_properties(dataset=dataset, properties=[prop])

    def drop_properties(self, dataset: str, properties: Union[Sequence[Dict], Sequence[str]]) \
            -> requests.models.Response:
        """

        Args:
            dataset: Dataset to delete properties from
            properties: A json object containing the property definitions

        Returns:
            requests.models.Response:
        """

        if not self._stored_procedure_exists('geodb_drop_properties'):
            raise ValueError(f"Stored procedure geodb_drop_properties does not exist")

        if not self._dataset_exists(dataset=dataset):
            raise ValueError(f"Dataset {dataset} does not exist")

        validate = fastjsonschema.compile(
            JSON_API_VALIDATIONS_CREATE_DATASET['validation'],
            formats=JSON_API_VALIDATIONS_CREATE_DATASET['formats']
        )
        # validate(properties)

        return self.post(path='/rpc/geodb_drop_properties', payload={'dataset': dataset, 'properties    ': properties})

    def delete_from_dataset(self, dataset: str, query: str) -> requests.models.Response:
        """

        Args:
            dataset: Dataset to delete from  
            query: Filter which records to delete

        Returns:
            requests.models.Response
        """

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

        if not self._dataset_exists(dataset=dataset):
            raise ValueError(f"Dataset {dataset} does not exist")

        return self.patch(f'/{dataset}?{query}', payload=values)

    def insert_into_dataset(self, dataset: str, values: Union[Dict, GeoDataFrame, str], upsert: bool = False) \
            -> requests.models.Response:
        """

        Args:
            dataset: Dataset to be inserted to
            values: Values to be inserted
            upsert: Whether the insert shall replace existing rows (by PK)

        Returns:
            requests.models.Response
        """

        if not self._dataset_exists(dataset=dataset):
            raise ValueError(f"Dataset {dataset} does not exist")

        if isinstance(values, GeoDataFrame):
            headers = {'Content-Type': 'text/csv'}

            def wkb_hexer(line):
                return line.wkb_hex
            values['geometry'] = values['geometry'].apply(wkb_hexer)

            if 'id' in values.columns and not upsert:
                values.drop(columns=['id'])

            values = values.to_csv()
        elif isinstance(values, str):
            headers = {'Content-Type': 'text/csv'}
        elif isinstance(values, Dict):
            headers = {'Content-Type': 'application/json'}
        else:
            raise ValueError(f'Format {type(values)} not supported.')

        if upsert:
            headers['Prefer'] = 'resolution=merge-duplicates'

        return self.post(f'/{dataset}?', payload=values, headers=headers)

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

        Raises:
            ValueError: When either the column ID or geometry or both are missing. Or the result is empty.
        Examples:
            >>> api = GeoDB()
            >>> api.filter_by_bbox(table="land_use",minx=452750.0, miny=88909.549, maxx=464000.0, maxy=102486.299, \
                bbox_mode="contains", bbox_crs=3794, lmt=1000, offst=10)
        """

        if not self._dataset_exists(dataset=dataset):
            raise ValueError(f"Dataset {dataset} does not exist")

        if not self._stored_procedure_exists('get_by_bbox'):
            raise ValueError(f"Stored procedure get_by_bbox does not exist")

        headers = {'Accept': 'application/vnd.pgrst.object+json'}

        r = self.post('/rpc/geodb_get_by_bbox', headers=headers, payload={
            "dataset": dataset,
            "minx": minx,
            "miny": miny,
            "maxx": maxx,
            "maxy": maxy,
            "bbox_mode": bbox_mode,
            "bbox_crs": bbox_crs,
            "lmt": limit,
            "offst": offset
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

    def filter(self, dataset: str, query: str) -> GeoDataFrame:
        """

        Args:
            dataset: The dataset to be filtered
            query: A filter query using PostGrest style

        Returns:
            GeoDataFrame of results

        Examples:
            >>> api = GeoDB()
            >>> api.filter('land_use', 'id=ge.1000')

        """

        if not self._dataset_exists(dataset=dataset):
            raise ValueError(f"Dataset {dataset} does not exist")

        r = self.get(f"/{dataset}?{query}")
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

        gpdf = gpd.GeoDataFrame(data).set_geometry('geometry')

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
        return f"/{dataset}" in self.capabilities['paths']

    def _stored_procedure_exists(self, stored_procedure: str) -> bool:
        """

        Args:
            stored_procedure: Name of stored pg procedure

        Returns:
            bool whether the stored procedure exists in DB
        """
        return f"/rpc/{stored_procedure}" in self.capabilities['paths']
