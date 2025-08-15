import unittest

import requests_mock

from tests.core.geodb_test_base import GeoDBClientTestBase
from xcube_geodb.core.error import GeoDBError
from xcube_geodb.core.metadata import (
    Range,
    Link,
    Provider,
    Asset,
    ItemAsset,
)


@requests_mock.mock(real_http=False)
class GeoDBClientMetadataTest(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        cls.base_test = GeoDBClientTestBase()
        cls.base_test.setUp()
        cls.default_json = {
            "basic": {
                "collection_name": "my_test_collection",
                "links": [],
                "temporal_extent": [[None, None]],
                "description": "No description available",
                "license": "proprietary",
            }
        }

    def tearDown(self) -> None:
        self.base_test.tearDown()

    # noinspection PyTypeChecker
    def test_get_metadata(self, m: requests_mock.mocker.Mocker):
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
            collection="my_test_collection", database="database"
        )
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata.id, "my_test_collection")
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

    def test_get_metadata_everything_none(self, m: requests_mock.mocker.Mocker):
        self.base_test.set_global_mocks(m)
        url = f"{self.base_test._base_url}/rpc/geodb_get_metadata"
        m.post(
            url,
            json=self.default_json,
        )
        url = f"{self.base_test._base_url}/rpc/geodb_estimate_collection_bbox"
        m.post(
            url,
            text="BOX(-90 -180 90 180)",
        )
        url = f"{self.base_test._base_url}/rpc/geodb_set_spatial_extent"
        m.post(
            url,
            text="",
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
        self.assertListEqual(metadata.item_assets, [])

    def test_set_metadata_field(self, m: requests_mock.mocker.Mocker):
        self.base_test.set_global_mocks(m)

        json = self.default_json
        json["basic"]["title"] = "Sir Collection"
        url = f"{self.base_test._base_url}/rpc/geodb_get_metadata"
        m.post(
            url,
            json=self.default_json,
        )
        url = f"{self.base_test._base_url}/rpc/geodb_estimate_collection_bbox"
        m.post(
            url,
            text="BOX(-6 9,5 11)",
        )
        url = f"{self.base_test._base_url}/rpc/geodb_set_spatial_extent"
        m.post(url)

        self.assertEqual(
            "Sir Collection",
            self.base_test._api.get_metadata(
                collection="my_collection", database="database"
            ).title,
        )

        url = f"{self.base_test._base_url}/rpc/geodb_set_metadata_field"
        m.post(
            url,
            json={},
        )

        self.base_test._api.set_metadata_field(
            "title", "Lady Collection", collection="my_collection", database="database"
        )

        json["basic"]["title"] = "Lady Collection"
        url = f"{self.base_test._base_url}/rpc/geodb_get_metadata"
        m.post(
            url,
            json=self.default_json,
        )

        self.assertEqual(
            "Lady Collection",
            self.base_test._api.get_metadata(
                collection="my_collection", database="database"
            ).title,
        )

        self.base_test._api.set_metadata_field(
            "keywords",
            ["crops", "europe", "rural"],
            collection="my_collection",
            database="database",
        )
        self.base_test._api.set_metadata_field(
            "links",
            [Link.from_json({"href": "https://link2.bc", "rel": "root"})],
            collection="my_collection",
            database="database",
        )
        self.base_test._api.set_metadata_field(
            "providers",
            [
                Provider.from_json(
                    {
                        "name": "provider 1",
                        "description": "some provider",
                        "roles": ["licensor", "producer"],
                    }
                )
            ],
            collection="my_collection",
            database="database",
        )
        self.base_test._api.set_metadata_field(
            "assets",
            [
                Asset.from_json(
                    {
                        "href": "https://asset.org",
                        "title": "some title",
                        "roles": ["image", "ql"],
                    }
                )
            ],
            collection="my_collection",
            database="database",
        )
        self.base_test._api.set_metadata_field(
            "item_assets",
            [
                ItemAsset.from_json(
                    {
                        "type": "some_type",
                        "title": "some title",
                        "roles": ["image", "ql"],
                    }
                )
            ],
            collection="my_collection",
            database="database",
        )
        self.base_test._api.set_metadata_field(
            "summaries",
            {
                "columns": ["id", "geometry"],
                "x_range": {"min": "-170", "max": "170"},
                "y_range": {"min": "-80", "max": "80"},
                "schema": "some JSON schema",
            },
            collection="my_collection",
            database="database",
        )

        with self.assertRaises(ValueError):
            self.base_test._api.set_metadata_field(
                "ruebennase",
                "ruebennasenhausen",
                collection="my_collection",
                database="database",
            )

        url = f"{self.base_test._base_url}/rpc/geodb_set_metadata_field"
        m.post(
            url,
            exc=GeoDBError("Collection does not exist"),
        )

        with self.assertRaises(GeoDBError):
            self.base_test._api.set_metadata_field(
                "description",
                "Bro Collection",
                collection="no_collection",
                database="database",
            )

    def test_print_metadata(self, m: requests_mock.mocker.Mocker):
        p = Provider.from_json({"name": "some_name", "roles": ["licensor"]})
        self.assertEqual(
            "{name = some_name, description = , url = None, roles = ['licensor']}",
            str(p),
        )
        l = Link.from_json(
            {
                "href": "https://link",
                "rel": "rel",
                "type": "some_type",
                "body": "some request body",
            }
        )
        self.assertEqual(
            "{href = https://link, rel = rel, type = some_type, title = None, method = None, headers = None, body = some request body}",
            str(l),
        )
        a = Asset.from_json(
            {
                "href": "https://link",
                "type": "some_type",
                "title": "some asset title",
                "roles": ["any", "will", "do"],
            }
        )
        self.assertEqual(
            "{href = https://link, description = None, title = some asset title, type = some_type, roles = ['any', 'will', 'do']}",
            str(a),
        )
        ia = ItemAsset.from_json(
            {
                "type": "some_type",
                "description": "description",
                "title": "some item asset title",
                "roles": ["any", "will", "do"],
            }
        )
        self.assertEqual(
            "{description = description, title = some item asset title, type = some_type, roles = ['any', 'will', 'do']}",
            str(ia),
        )

    def test_set_wrong_metadata(self, m: requests_mock.mocker.Mocker):
        with self.assertRaises(ValueError):
            Provider.from_json({"name": "some_name", "roles": ["kaputtnick"]})
