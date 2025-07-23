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
import os
import unittest
from unittest.mock import patch

from tests.sql.test_sql_functions import GeoDBSqlTest
from tests.sql.test_sql_functions import get_app_dir
from xcube_geodb.core.db_interface import DbInterface
from xcube_geodb.core.geodb import GeoDBClient
from xcube_geodb.core.metadata import MetadataManager


class GeoDBSQLMDTest(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        cls.base_test = GeoDBSqlTest()
        cls.base_test.setUp()
        cls._cursor = cls.base_test._cursor
        cls._set_role = cls.base_test._set_role
        cls._conn = cls.base_test._conn

        app_path = get_app_dir()
        fn = os.path.join(app_path, "..", "tests", "sql", "setup-metadata.sql")
        with open(fn) as sql_file:
            cls.base_test._cursor.execute(sql_file.read())

        cls._conn.commit()

    def tearDown(self) -> None:
        self.base_test.tearDown()

    def test_metadata_table_initialisation(self):
        sql = 'SELECT "collection_name" from geodb_collection_metadata.basic;'
        self._cursor.execute(sql)
        self.assertEqual("land_use", self._cursor.fetchone()[0])

        sql = 'SELECT "database" from geodb_collection_metadata.basic;'
        self._cursor.execute(sql)
        self.assertEqual("geodb_user", self._cursor.fetchone()[0])

        sql = 'SELECT "title" from geodb_collection_metadata.basic;'
        self._cursor.execute(sql)
        self.assertEqual("Land Use", self._cursor.fetchone()[0])

        sql = 'SELECT "description" from geodb_collection_metadata.basic;'
        self._cursor.execute(sql)
        self.assertEqual("Sample table", self._cursor.fetchone()[0])

        sql = 'SELECT "license" from geodb_collection_metadata.basic;'
        self._cursor.execute(sql)
        self.assertEqual("proprietary", self._cursor.fetchone()[0])

        sql = 'SELECT "spatial_extent" from geodb_collection_metadata.basic;'
        self._cursor.execute(sql)
        geometries = [
            item.strip("{}") for item in self._cursor.fetchone()[0].split(",") if item
        ]
        sql = f"SELECT ST_ASTEXT('{geometries[0].split(':')[0]}');"
        self._cursor.execute(sql)
        self.assertEqual(
            "POLYGON((-170 -80,-170 80,170 80,-170 -80,-170 -80))",
            self._cursor.fetchone()[0],
        )

        sql = 'SELECT "temporal_extent" from geodb_collection_metadata.basic;'
        self._cursor.execute(sql)
        self.assertEqual([[None, None]], self._cursor.fetchone()[0])

        sql = 'SELECT "stac_extensions" from geodb_collection_metadata.basic;'
        self._cursor.execute(sql)
        self.assertEqual([], self._cursor.fetchone()[0])

        sql = 'SELECT "keywords" from geodb_collection_metadata.basic;'
        self._cursor.execute(sql)
        self.assertEqual(["land", "use"], self._cursor.fetchone()[0])

        sql = 'SELECT "summaries" from geodb_collection_metadata.basic;'
        self._cursor.execute(sql)
        self.assertEqual(
            {
                "columns": ["id", "geometry"],
                "schema": "this is a complex schema stored in a string",
                "x_range": {"max": "170", "min": "-170"},
                "y_range": {"max": "80", "min": "-80"},
            },
            self._cursor.fetchone()[0],
        )

        sql = "SELECT * from geodb_collection_metadata.link;"
        self._cursor.execute(sql)
        result = self._cursor.fetchall()
        self.assertEqual(2, len(result))

        self.assertEqual("some_link", result[0][4])
        self.assertEqual("some_other_link", result[1][4])

        sql = "SELECT * from geodb_collection_metadata.provider;"
        self._cursor.execute(sql)
        result = self._cursor.fetchall()
        self.assertEqual(3, len(result))

        self.assertEqual("some_provider", result[0][0])
        self.assertEqual("some_other_provider", result[1][0])
        self.assertEqual("another_provider", result[2][0])

        self.assertEqual("i am the best provider!", result[0][1])
        self.assertEqual("i am the worst provider!", result[1][1])
        self.assertEqual("i am an ok provider!", result[2][1])

        self.assertEqual("{}", result[0][2])
        self.assertEqual("{}", result[1][2])
        self.assertEqual("{producer,host}", result[2][2])

        sql = "SELECT * from geodb_collection_metadata.asset;"
        self._cursor.execute(sql)
        result = self._cursor.fetchall()
        self.assertEqual(1, len(result))

        self.assertEqual("some_asset", result[0][0])
        self.assertEqual("https://best-assets.bc", result[0][1])
        self.assertEqual([], result[0][5])

        sql = 'SELECT * from geodb_collection_metadata."item_asset";'
        self._cursor.execute(sql)
        result = self._cursor.fetchall()
        self.assertEqual(1, len(result))

        self.assertEqual("some_item_asset", result[0][0])
        self.assertEqual("I have a type", result[0][3])
        self.assertEqual([], result[0][4])

    @patch("xcube_geodb.core.geodb.GeoDBClient")
    @patch("xcube_geodb.core.db_interface.DbInterface")
    def test_get_metadata(self, geodb: GeoDBClient, mockdb: DbInterface):
        sql = "SELECT geodb_get_metadata('land_use', 'geodb_user')"
        self._cursor.execute(sql)
        result = self._cursor.fetchone()[0]
        md = MetadataManager(geodb, mockdb).from_json(result, "land_use", "geodb_user")
        self.assertEqual("land_use", md.id)
        self.assertEqual("Land Use", md.title)
        self.assertEqual("proprietary", md.license)
        self.assertEqual("Sample table", md.description)
        self.assertEqual(
            [[-170, -80, 170, 80], [-169, -79, 169, 79]],
            md.spatial_extent,
        )
        self.assertEqual([[None, None]], md.temporal_extent)
        self.assertEqual(1, len(md.assets))
        self.assertEqual("https://best-assets.bc", md.assets[0].href)
        self.assertEqual(1, len(md.item_assets))
        self.assertEqual("I have a type", md.item_assets[0].type)
        self.assertEqual(3, len(md.providers))
        self.assertEqual("i am the worst provider!", md.providers[1].description)
        self.assertEqual("i am an ok provider!", md.providers[2].description)
        self.assertEqual(["producer", "host"], md.providers[2].roles)

        self.assertEqual(4, len(md.summaries))
        self.assertListEqual(["id", "geometry"], md.summaries["columns"])
        self.assertDictEqual({"min": "-170", "max": "170"}, md.summaries["x_range"])
        self.assertDictEqual({"min": "-80", "max": "80"}, md.summaries["y_range"])
        self.assertEqual(
            "this is a complex schema stored in a string", md.summaries["schema"]
        )
