import json
import unittest

from geopandas import GeoDataFrame
from httpretty import httpretty

from dcfs_geodb.core.geo_db import GeoDB


class GeoDBTest(unittest.TestCase):

    def test_query_by_bbox(self):
        expected_response = []
        url = "http://test:3000/rpc/get_by_bbox"

        httpretty.register_uri(httpretty.POST,
                               url,
                               status=200,
                               body=json.dumps(expected_response).encode("utf-8"))

        api = GeoDB(server_url="http://test", server_port=3000)

        gdf = api.get_by_bbox(dataset='dataset', minx=452750.0, miny=88909.549, maxx=464000.0, maxy=102486.299,
                              bbox_crs=4326)

        print(gdf)

        self.assertIsInstance(gdf, GeoDataFrame)







