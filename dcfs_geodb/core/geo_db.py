from typing import Dict, Optional

import geopandas as gpd
from shapely import wkb
import requests
import json


from dcfs_geodb.config import GEODB_API_DEFAULT_CONNECTION_PARAMETERS


class GeoDB(object):
    def __init__(self, server_url: str = None, server_port: int = None):
        self._set_defaults()

        if server_url:
            self._server_url = server_url
        if server_port:
            self._server_port = server_port

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
        valid_columns = {"raba_pid", 'raba_id', 'd_od', 'geometry'}

        return len(list(valid_columns - cols)) == 0

    def _table_exists(self, table: str) -> bool:
        """

        Args:
            table: A table name to check

        Returns:
            bool whether table exists in DB

        """
        return True

    def post(self, path: str, body: Dict, params: Optional[Dict] = None) -> requests.models.Response:
        r = requests.post(self._get_full_url(path=path), json=body, params=params)
        r.raise_for_status()
        return r

    def get(self, path: str, params: Optional[Dict] = None) -> requests.models.Response:
        r = requests.get(self._get_full_url(path=path), params=params)
        r.raise_for_status()
        return r

    def get_by_bbox(self, table: str, minx, miny, maxx, maxy, bbox_mode: str = 'contains', bbox_crs: int = 4326,
                    lmt: int = 0, offst: int = 0) \
            -> gpd.GeoDataFrame:
        """
        Args:
            offst:
            lmt:
            bbox_crs:
            table: name of the table to be queried
            minx: left side of bbox
            miny: bottom side of bbox
            maxx: right side of bbox
            maxy: top side of bbox
            bbox_mode: selection mode. 'within': the geometry A is completely hmhmhm in B,
        Returns:
            A GeopandasDataFrame containing the query result
        """
        r = self.post('/rpc/get_by_bbox', body={
            "table_name": table,
            "minx": minx,
            "miny": miny,
            "maxx": maxx,
            "maxy": maxy,
            "bbox_mode": bbox_mode,
            "bbox_crs": bbox_crs,
            "lmt": lmt,
            "offst": offst
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

    def get_capabilities(self) -> json:
        self.get('/')

    def _get_full_url(self, path: str) -> str:
        return f"{self._server_url}:{self._server_port}{path}"

    def _load_geo(self, d):
        d['geometry'] = wkb.loads(d['geometry'], hex=True)
        return d


if __name__ == "__main__":
    api = GeoDB()
    api.get_by_bbox(table="land_use",minx=452750.0, miny=88909.549, maxx=464000.0, maxy=102486.299,
                    bbox_mode="contains", bbox_crs=3794, lmt=1000)
    print('Finished')

