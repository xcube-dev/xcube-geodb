import unittest

import requests_mock
from pystac import Asset, ItemAssetDefinition, Link, Provider

from tests.core.geodb_test_base import GeoDBClientTestBase
from xcube_geodb.core.error import GeoDBError
from xcube_geodb.core.metadata import Range


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
        json["item_assets"] = [
            {
                "title": "some_title",
                "description": "some_description",
                "roles": ["any value allowed", "including this"],
            }
        ]
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
        self.assertIsNone(metadata.assets[0].media_type)

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
        self.assertEqual(metadata.links[0].extra_fields["method"], "GET")
        self.assertEqual(metadata.links[0].rel, "item")
        self.assertIsNone(metadata.links[0].title)
        self.assertDictEqual(
            metadata.links[0].extra_fields["headers"],
            {
                "header-1": "schnuffel",
                "header-2": ["schnaffel", "schnoffel"],
            },
        )
        self.assertEqual(len(metadata.summaries), 4)
        self.assertEqual(len(metadata.item_assets), 1)
        self.assertEqual("some_title", metadata.item_assets[0].title)
        self.assertEqual("some_description", metadata.item_assets[0].description)
        self.assertEqual(
            ["any value allowed", "including this"], metadata.item_assets[0].roles
        )

        self.assertEqual(
            "Sir Collection [my_test_collection]\n"
            + "  Description: No description available\n"
            + "  License: proprietary\n"
            + "  Providers: I am a provider, I am another provider\n"
            + "  Keywords: super, mega, awesome\n"
            + f"  Links: {metadata.links}\n"
            + f"  Assets: {metadata.assets}\n"
            + f"  Item Assets: {metadata.item_assets}\n"
            + "  Spatial extent: [[-180, -90, 0, 0], [-170, -80, -30, -20]]\n"
            + "  Temporal extent: [['2019-01-01T00:00:00Z', None], ['2019-01-01T00:00:00Z', '2020-01-01T00:00:00Z']]",
            str(metadata),
        )

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
            text="BOX(-180 -90,180 90)",
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

        self.assertEqual(
            " [my_collection]\n"
            + "  Description: No description available\n"
            + "  License: proprietary\n"
            + "  Providers: None\n"
            + "  Keywords: None\n"
            + "  Links: []\n"
            + "  Assets: []\n"
            + "  Item Assets: []\n"
            + "  Spatial extent: [[-180, -90, 180, 90]]\n"
            + "  Temporal extent: [[None, None]]",
            str(metadata),
        )

    def test_get_metadata_default_spatial_extent_issue131(self, m: requests_mock.mocker.Mocker):
        self.base_test.set_global_mocks(m)
        url = f"{self.base_test._base_url}/rpc/geodb_get_metadata"
        m.post(
            url,
            json=self.default_json,
        )
        url = f"{self.base_test._base_url}/rpc/geodb_estimate_collection_bbox"
        m.post(
            url,
            text="null",
        )
        url = f"{self.base_test._base_url}/rpc/geodb_set_spatial_extent"
        m.post(
            url,
            text="",
        )

        metadata = self.base_test._api.get_metadata(
            collection="my_collection", database="database"
        )
        self.assertEqual(len(metadata.spatial_extent), 1)
        self.assertListEqual(metadata.spatial_extent[0], [-180, -90, 180, 90])

    def test_get_metadata_preserves_longitude_latitude_order(self, m):
        self.base_test.set_global_mocks(m)
        m.post(
            f"{self.base_test._base_url}/rpc/geodb_get_metadata",
            json={
                "basic": {
                    "description": "Coordinate order probe",
                    "license": "proprietary",
                    "temporal_extent": [[None, None]],
                }
            },
        )
        m.post(
            f"{self.base_test._base_url}/rpc/geodb_estimate_collection_bbox",
            json="BOX(10 53,11 54)",
        )
        endpoint = m.post(
            f"{self.base_test._base_url}/rpc/geodb_set_spatial_extent", text=""
        )

        metadata = self.base_test._api.get_metadata("probe", database="database")

        self.assertEqual([[10, 53, 11, 54]], metadata.spatial_extent)
        self.assertEqual(1, endpoint.call_count)
        self.assertEqual(
            {
                "collection": "probe",
                "db": "database",
                "se": [[10, 53, 11, 54]],
                "srid": 4326,
            },
            endpoint.last_request.json(),
        )

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
            [Link.from_dict({"href": "https://link2.bc", "rel": "root"})],
            collection="my_collection",
            database="database",
        )
        self.base_test._api.set_metadata_field(
            "providers",
            [
                Provider.from_dict(
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
                Asset.from_dict(
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
                ItemAssetDefinition(
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

    def test_provider_roles_are_validated_by_server(self, m: requests_mock.mocker.Mocker):
        self.base_test.set_global_mocks(m)
        endpoint = m.post(
            f"{self.base_test._base_url}/rpc/geodb_set_metadata_field",
            status_code=400,
            json={"code": "22P02", "message": "invalid input value for enum provider_role"},
        )
        provider = Provider.from_dict({"name": "some_name", "roles": ["kaputtnick"]})
        with self.assertRaises(GeoDBError):
            self.base_test._api.set_metadata_field(
                "providers", [provider], "probe", database="database"
            )
        self.assertEqual([provider.to_dict()], endpoint.last_request.json()["value"])

    def test_provider_metadata_roundtrip(self, m: requests_mock.mocker.Mocker):
        self.base_test.set_global_mocks(m)
        expected = [
            {"name": "minimal provider"},
            {"name": "full provider", "description": "Processing center",
             "roles": ["processor"], "url": "https://provider.example"},
        ]
        m.post(
            f"{self.base_test._base_url}/rpc/geodb_get_metadata",
            json={
                "basic": {**self.default_json["basic"], "spatial_extent": [
                    {"minx": 10, "miny": 53, "maxx": 11, "maxy": 54}
                ]},
                "providers": [
                    {**p, "id": i, "collection_name": "probe", "database": "database"}
                    for i, p in enumerate(expected)
                ],
            },
        )
        endpoint = m.post(
            f"{self.base_test._base_url}/rpc/geodb_set_metadata_field", text=""
        )
        metadata = self.base_test._api.get_metadata("probe", database="database")
        self.assertTrue(all(type(p) is Provider for p in metadata.providers))
        self.assertEqual(expected, [p.to_dict() for p in metadata.providers])
        self.base_test._api.set_metadata_field(
            "providers", metadata.providers, "probe", database="database"
        )
        self.assertEqual(
            {"field": "providers", "value": expected,
             "collection": "probe", "db": "database"},
            endpoint.last_request.json(),
        )

    def test_link_metadata_roundtrip(self, m):
        self.base_test.set_global_mocks(m)
        expected = [
            {"href": "../catalog.json", "rel": "root"},
            {
                "href": "https://example.com/search",
                "rel": "item",
                "type": "application/geo+json",
                "title": "Search",
                "method": "POST",
                "headers": {"X-Test": ["first", "second"]},
                "body": {"filter": {"instrument": ["A", "B"]}},
            },
            {
                "href": "https://example.com/parent",
                "rel": "parent",
                "method": None,
                "headers": None,
                "body": None,
            },
        ]
        m.post(
            f"{self.base_test._base_url}/rpc/geodb_get_metadata",
            json={
                "basic": {
                    **self.default_json["basic"],
                    "spatial_extent": [
                        {"minx": 10, "miny": 53, "maxx": 11, "maxy": 54}
                    ],
                },
                "links": [
                    {"type": None, "title": None, **link, "id": i,
                     "collection_name": "probe", "database": "database"}
                    for i, link in enumerate(expected)
                ],
            },
        )
        endpoint = m.post(
            f"{self.base_test._base_url}/rpc/geodb_set_metadata_field", text=""
        )
        metadata = self.base_test._api.get_metadata("probe", database="database")
        self.assertTrue(all(type(link) is Link for link in metadata.links))
        self.assertEqual(expected, [link.to_dict() for link in metadata.links])
        self.assertEqual("application/geo+json", metadata.links[1].media_type)
        self.base_test._api.set_metadata_field(
            "links", metadata.links, "probe", database="database"
        )
        self.assertEqual(
            {"field": "links", "value": expected,
             "collection": "probe", "db": "database"},
            endpoint.last_request.json(),
        )

    def test_asset_metadata_roundtrip(self, m):
        self._check_asset_roundtrip(m, "assets", Asset, [
            {"href": "../data.tif"},
            {
                "href": "https://example.com/data.tif", "title": "Data",
                "description": "Measurements", "type": "image/tiff",
                "roles": ["data", "custom-role"],
            },
            {"href": "https://example.com/empty", "title": "", "description": "", "roles": []},
        ])

    def test_item_asset_metadata_roundtrip(self, m):
        self._check_asset_roundtrip(m, "item_assets", ItemAssetDefinition, [
            {},
            {
                "title": "Data", "description": "Measurements", "type": "image/tiff",
                "roles": ["data", "custom-role"],
            },
            {"title": "", "description": "", "roles": []},
            {"title": None, "description": None, "type": None, "roles": None},
        ])

    def _check_asset_roundtrip(self, m, field, cls, expected):
        self.base_test.set_global_mocks(m)
        m.post(
            f"{self.base_test._base_url}/rpc/geodb_get_metadata",
            json={
                "basic": {
                    **self.default_json["basic"],
                    "spatial_extent": [
                        {"minx": 10, "miny": 53, "maxx": 11, "maxy": 54}
                    ],
                },
                field: [
                    {**({"title": None, "description": None, "type": None, "roles": None}
                        if field == "assets" else {}), **asset, "id": i,
                     "collection_name": "probe", "database": "database"}
                    for i, asset in enumerate(expected)
                ],
            },
        )
        endpoint = m.post(
            f"{self.base_test._base_url}/rpc/geodb_set_metadata_field", text=""
        )
        metadata = self.base_test._api.get_metadata("probe", database="database")
        assets = getattr(metadata, field)
        self.assertTrue(all(type(asset) is cls for asset in assets))
        self.assertEqual(expected, [asset.to_dict() for asset in assets])
        self.assertEqual("image/tiff", assets[1].media_type)
        self.base_test._api.set_metadata_field(field, assets, "probe", database="database")
        self.assertEqual(
            {"field": field, "value": expected, "collection": "probe", "db": "database"},
            endpoint.last_request.json(),
        )
        self.base_test._api.set_metadata_field(field, [], "probe", database="database")
        self.assertEqual([], endpoint.last_request.json()["value"])
