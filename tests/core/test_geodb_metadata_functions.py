import unittest

import requests_mock

from tests.core.test_geodb import GeoDBClientTest
from xcube_geodb.core.metadata import Range


@requests_mock.mock(real_http=False)
class GeoDBClientMetadataTest(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        cls.base_test = GeoDBClientTest()
        cls.base_test.setUp()
        cls.default_json = {
            "basic": {
                "collection_name": "3",
                "links": [],
                "temporal_extent": [[None, None]],
                "description": "No description available",
                "license": "proprietary",
            }
        }

    def tearDown(self) -> None:
        self.base_test.tearDown()

    # noinspection PyTypeChecker
    def test_get_metadata(self, m):
        self.base_test.set_global_mocks(m)
        url = f"{self.base_test._base_url}/rpc/geodb_get_metadata"
        json = self.default_json
        json["basic"]["title"] = "Sir Collection"
        json["providers"] = [
            {
                "name": "I am a provider",
                "description": "This is my description",
                "roles": ["licensor", "processor"],
                "url": "https://xcube-dev.github.io/xcube-geodb/",
            },
            {
                "name": "I am another provider",
                "description": "This is my chorizo",
                "roles": ["host"],
            },
        ]
        json["assets"] = [
            {
                "href": "https://my-images.bc/image.png",
                "title": "title of my image",
                "description": "some description",
                "roles": ["thumbnail", "overview"],
            }
        ]
        json["basic"]["temporal_extent"] = [
            ["2019-01-01T00:00:00Z", None],
            ["2019-01-01T00:00:00Z", "2020-01-01T00:00:00Z"],
        ]
        json["basic"]["spatial_extent"] = [
            {"minx": -180, "miny": -90, "maxx": 0, "maxy": 0},
            {"minx": -170, "miny": -80, "maxx": -30, "maxy": -20},
        ]
        json["basic"]["stac_extensions"] = ["stac_ext_1", "stac_ext_2"]
        json["basic"]["keywords"] = ["super", "mega", "awesome"]
        json["links"] = [
            {
                "href": "https://wurst.brot",
                "rel": "item",
                "type": None,
                "title": None,
                "method": "GET",
                "headers": {
                    "header-1": "schnuffel",
                    "header-2": ["schnaffel", "schnoffel"],
                },
            }
        ]
        json["basic"]["summaries"] = {
            "column_names": ["temp", "height", "epoch"],
            "temp_valid_range": Range(-10, 100),
            "epoch_valid_range": Range("barocque", "modern"),
            "schema": "json_schema_as_string",
        }
        json["item_assets"] = {
            "item_asset_0": {
                "title": "some_title",
                "description": "some_description",
                "roles": ["any value allowed", "including this"],
            }
        }
        m.post(
            url,
            json=json,
        )

        metadata = self.base_test._api.get_metadata(
            collection="my_collection", database="database"
        )
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata.id, "3")
        self.assertEqual(metadata.description, "No description available")
        self.assertEqual(metadata.license, "proprietary")
        self.assertEqual(metadata.type, "Collection")
        self.assertEqual(metadata.title, "Sir Collection")
        self.assertEqual(metadata.stac_version, "1.1.0")
        self.assertEqual(metadata.stac_extensions, ["stac_ext_1", "stac_ext_2"])

        self.assertEqual(len(metadata.providers), 2)
        self.assertEqual(metadata.providers[0].name, "I am a provider")
        self.assertEqual(metadata.providers[0].description, "This is my description")
        self.assertEqual(metadata.providers[0].roles, ["licensor", "processor"])
        self.assertEqual(
            metadata.providers[0].url, "https://xcube-dev.github.io/xcube-geodb/"
        )

        self.assertEqual(len(metadata.assets), 1)
        self.assertEqual(metadata.assets[0].href, "https://my-images.bc/image.png")
        self.assertEqual(metadata.assets[0].title, "title of my image")
        self.assertEqual(metadata.assets[0].description, "some description")
        self.assertEqual(metadata.assets[0].roles[0], "thumbnail")
        self.assertEqual(metadata.assets[0].roles[1], "overview")
        self.assertIsNone(metadata.assets[0].type)

        self.assertEqual(
            metadata.temporal_extent,
            [
                ["2019-01-01T00:00:00Z", None],
                ["2019-01-01T00:00:00Z", "2020-01-01T00:00:00Z"],
            ],
        )
        self.assertEqual(
            metadata.spatial_extent,
            [
                [-180, -90, 0, 0],
                [-170, -80, -30, -20],
            ],
        )
        self.assertListEqual(metadata.keywords, ["super", "mega", "awesome"])
        self.assertEqual(len(metadata.links), 1)
        self.assertEqual(metadata.links[0].href, "https://wurst.brot")
        self.assertEqual(metadata.links[0].method, "GET")
        self.assertEqual(metadata.links[0].rel, "item")
        self.assertIsNone(metadata.links[0].title)
        self.assertDictEqual(
            metadata.links[0].headers,
            {
                "header-1": "schnuffel",
                "header-2": ["schnaffel", "schnoffel"],
            },
        )
        self.assertEqual(len(metadata.summaries), 4)
        self.assertEqual(len(metadata.item_assets), 1)

    def test_get_metadata_everything_none(self, m):
        self.base_test.set_global_mocks(m)
        url = f"{self.base_test._base_url}/rpc/geodb_get_metadata"
        m.post(
            url,
            json=self.default_json,
        )

        metadata = self.base_test._api.get_metadata(
            collection="my_collection", database="database"
        )
        self.assertEqual(len(metadata.providers), 0)
        self.assertEqual(len(metadata.assets), 0)
        self.assertEqual(len(metadata.temporal_extent), 1)
        self.assertEqual(len(metadata.temporal_extent[0]), 2)
        self.assertIsNone(metadata.temporal_extent[0][0])
        self.assertIsNone(metadata.temporal_extent[0][1])
        self.assertEqual(metadata.stac_extensions, [])
        self.assertEqual(len(metadata.spatial_extent), 1)
        self.assertListEqual(metadata.spatial_extent[0], [-180, -90, 180, 90])
        self.assertEqual(metadata.title, "")
        self.assertListEqual(metadata.keywords, [])
        self.assertListEqual(metadata.links, [])
        self.assertDictEqual(metadata.summaries, {})
        self.assertDictEqual(metadata.item_assets, {})
