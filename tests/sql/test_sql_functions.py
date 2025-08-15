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

import datetime
import os
import unittest
import json
import psycopg2

import xcube_geodb.version as version
from tests.sql.geodb_sql_test_base import GeoDBSqlTestBase
from xcube_geodb.core.geodb import EventType


@unittest.skipIf(os.environ.get("SKIP_PSQL_TESTS", "0") == "1", "DB Tests skipped")
class GeoDBSqlBaseTest(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        cls.base_test = GeoDBSqlTestBase()
        cls.base_test.setUp()
        cls._cursor = cls.base_test._cursor
        cls._set_role = cls.base_test.set_role
        cls._conn = cls.base_test._conn

    def tearDown(self) -> None:
        self.base_test.tearDown()

    def test_query_by_bbox(self):
        sql_filter = (
            "SELECT geodb_get_by_bbox('postgres_land_use', 452750.0, 88909.549, 464000.0, "
            "102486.299, 'contains', 3794)"
        )
        self._cursor.execute(sql_filter)

        res = self._cursor.fetchone()

        exp_geo = {
            "type": "Polygon",
            "coordinates": [
                [
                    [453952.629, 91124.177],
                    [453952.696, 91118.803],
                    [453946.938, 91116.326],
                    [453945.208, 91114.225],
                    [453939.904, 91115.388],
                    [453936.114, 91115.388],
                    [453935.32, 91120.269],
                    [453913.121, 91128.983],
                    [453916.212, 91134.782],
                    [453917.51, 91130.887],
                    [453922.704, 91129.156],
                    [453927.194, 91130.75],
                    [453932.821, 91129.452],
                    [453937.636, 91126.775],
                    [453944.994, 91123.529],
                    [453950.133, 91123.825],
                    [453952.629, 91124.177],
                ]
            ],
        }
        self.assertEqual(len(res), 1)

        self.assertEqual(res[0][0]["id"], 1)
        self.assertEqual(res[0][0]["geometry"]["type"], exp_geo["type"])
        self.assertEqual(res[0][0]["geometry"]["coordinates"], exp_geo["coordinates"])

    def column_exists(self, table: str, column: str, data_type: str) -> bool:
        sql = (
            f"\n"
            f"                    SELECT EXISTS\n"
            f"                    (\n"
            f"                        SELECT 1\n"
            f'                        FROM "information_schema".columns\n'
            f"                        WHERE \"table_schema\" = 'public'\n"
            f"                          AND \"table_name\"   = '{table}'\n"
            f"                          AND \"column_name\" = '{column}'\n"
            f"                          AND \"data_type\" = '{data_type}'\n"
            f"                    )\n"
            f"                         ;\n"
            f"            "
        )
        self._cursor.execute(sql)
        return self._cursor.fetchone()[0]

    def table_exists(self, table: str) -> bool:
        # noinspection SqlInjection
        sql = f"""SELECT EXISTS 
                        (
                            SELECT 1 
                            FROM pg_tables
                            WHERE schemaname = 'public'
                            AND tablename = '{table}'
                        );"""

        self._cursor.execute(sql)
        return self._cursor.fetchone()[0]

    def test_manage_table(self):
        user_name = "geodb_user"
        user_table = user_name + "_test"
        self._set_role(user_name)

        props = {"tt": "integer"}
        sql = (
            f"SELECT geodb_create_collection('{user_table}', "
            f"'{json.dumps(props)}', '4326')"
        )
        self._cursor.execute(sql)

        self.assertTrue(self.table_exists(user_table))

        self.assertTrue(self.column_exists(user_table, "id", "integer"))
        self.assertTrue(self.column_exists(user_table, "geometry", "USER-DEFINED"))

        collections = {
            "geodb_user_tt1": {"crs": "4326", "properties": {"tt": "integer"}},
            "geodb_user_tt2": {"crs": "4326", "properties": {"tt": "integer"}},
        }

        sql = f"SELECT geodb_create_collections('{json.dumps(collections)}')"
        self._cursor.execute(sql)

        self.assertTrue(self.table_exists(user_table))

        self.assertTrue(self.column_exists(user_table, "id", "integer"))
        self.assertTrue(self.column_exists(user_table, "geometry", "USER-DEFINED"))

        collection_names = ["test", "tt1", "tt2"]
        sql = f"SELECT geodb_drop_collections('geodb_user', '{json.dumps(collection_names)}')"
        self._cursor.execute(sql)
        self.assertFalse(self.table_exists(user_table))

    def test_manage_properties(self):
        user_name = "geodb_user"
        table = user_name + "_land_use_test"
        self._set_role(user_name)

        props = {"tt": "integer"}
        sql = (
            f"SELECT geodb_create_collection('{table}', '{json.dumps(props)}', '4326')"
        )
        self._cursor.execute(sql)

        cols = {"test_col1": "integer", "test_col2": "integer"}

        sql = (
            f"SELECT public.geodb_add_properties('{table}', '{json.dumps(cols)}'::json)"
        )
        self._cursor.execute(sql)

        self.assertTrue(self.column_exists(table, "test_col1", "integer"))

        cols = ["test_col1", "test_col2"]

        sql = f"SELECT public.geodb_drop_properties('{table}', '{json.dumps(cols)}')"
        self._cursor.execute(sql)
        self.assertFalse(self.column_exists(table, "test_col", "integer"))

    def test_get_my_usage(self):
        user_name = "geodb_user"
        self._set_role(user_name)

        props = {"tt": "integer"}
        sql = f"SELECT geodb_create_collection('geodb_user_test_usage', '{json.dumps(props)}', '4326')"
        self.execute(sql)

        sql = "SELECT public.geodb_get_my_usage()"
        self._cursor.execute(sql)
        self._cursor.fetchone()

        sql = "SELECT current_user"
        self._cursor.execute(sql)
        self._cursor.fetchone()

    def test_create_collection_forbidden(self):
        user_name = "geodb_user_read_only"
        self._set_role(user_name)

        props = {"tt": "integer"}
        sql = f"SELECT geodb_create_collection('geodb_user_test_usage', '{json.dumps(props)}', '4326')"
        with self.assertRaises(psycopg2.errors.InsufficientPrivilege) as e:
            self.execute(sql)
        self.assertIn(
            "permission denied for function geodb_create_collection", str(e.exception)
        )

    def test_create_collections_forbidden(self):
        user_name = "geodb_user_read_only"
        self._set_role(user_name)

        datasets = {
            "geodb_user_tt1": {"crs": "4326", "properties": {"tt": "integer"}},
            "geodb_user_tt2": {"crs": "4326", "properties": {"tt": "integer"}},
        }

        sql = f"SELECT geodb_create_collections('{json.dumps(datasets)}')"
        with self.assertRaises(psycopg2.errors.InsufficientPrivilege) as e:
            self.execute(sql)
        self.assertIn(
            "permission denied for function geodb_create_collection", str(e.exception)
        )

    def test_create_database(self):
        user_name = "geodb_user"
        self._set_role(user_name)

        sql = "SELECT geodb_create_database('test')"
        self._cursor.execute(sql)

        sql = f"SELECT * FROM geodb_user_databases WHERE name='test' AND owner = '{user_name}'"
        self._cursor.execute(sql)
        res = self._cursor.fetchall()

        self.assertEqual(1, len(res))
        res = res[0]
        self.assertEqual("test", res[1])
        self.assertEqual(user_name, res[2])

        # noinspection PyUnresolvedReferences
        with self.assertRaises(psycopg2.errors.RaiseException) as e:
            sql = "SELECT geodb_create_database('test')"
            self._cursor.execute(sql)

        self.assertIn("Database test exists already.", str(e.exception))

    def test_create_database_read_only(self):
        user_name = "geodb_user_read_only"
        self._set_role(user_name)

        with self.assertRaises(psycopg2.errors.InsufficientPrivilege) as e:
            sql = "SELECT geodb_create_database('geodb_user_read_only')"
            self.execute(sql)

        self.assertIn(
            "permission denied for function geodb_create_database", str(e.exception)
        )

    def test_get_geodb_sql_version(self):
        user_name = "geodb_user"
        self._set_role(user_name)

        sql = "SELECT geodb_get_geodb_sql_version()"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()

        self.assertEqual(1, len(res))
        self.assertEqual(version.version, res[0])

    def test_truncate_database(self):
        user_name = "geodb_user"
        self._set_role(user_name)

        sql = f"INSERT INTO geodb_user_databases(name, owner) VALUES('test_truncate', '{user_name}')"
        self._cursor.execute(sql)

        sql = "SELECT geodb_truncate_database('test_truncate')"
        self._cursor.execute(sql)

        sql = f"SELECT * FROM geodb_user_databases WHERE name='test_truncate' AND owner = '{user_name}'"
        self._cursor.execute(sql)
        res = self._cursor.fetchall()

        self.assertEqual(0, len(res))

    def test_grant_access(self):
        user_name = "geodb_user"
        self._set_role(user_name)

        sql = "SELECT geodb_grant_access_to_collection('geodb_user_land_use', 'public')"
        self._cursor.execute(sql)

    def test_geodb_rename_collection(self):
        user_name = "geodb_user"
        self._set_role(user_name)

        sql = "SELECT geodb_rename_collection('geodb_user_land_use', 'geodb_user_land_use2')"
        self._cursor.execute(sql)
        res = self._cursor.fetchall()

        self.assertEqual(1, len(res))
        self.assertEqual("success", res[0][0])

        # noinspection PyUnresolvedReferences
        with self.assertRaises(psycopg2.errors.RaiseException) as e:
            sql = "SELECT geodb_rename_collection('geodb_user_land_use', 'postgres_land_use2')"
            self._cursor.execute(sql)

        self.assertIn(
            "geodb_user has not access to that table or database. ", str(e.exception)
        )

    def test_geodb_count_collection(self):
        user_name = "geodb_user"
        self._set_role(user_name)

        sql = "SELECT geodb_count_collection('geodb_user_land_use')"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()

        self.assertEqual(2, res[0])

    def test_geodb_estimate_collection_count(self):
        user_name = "geodb_user"
        self._set_role(user_name)

        sql = "SELECT geodb_estimate_collection_count('geodb_user_land_use')"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()

        self.assertEqual(-1, res[0])

        self._analyze()

        sql = "SELECT geodb_estimate_collection_count('geodb_user_land_use')"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self.assertEqual(2, res[0])

    def _analyze(self):
        query = "ANALYZE"
        self._cursor.execute(query)

    def test_get_collection_bbox(self):
        user_name = "geodb_user-with-hyphens"
        user_table = user_name + "_test"
        self._set_role(user_name)

        props = {}
        sql = (
            f"SELECT geodb_create_collection('{user_table}', "
            f"'{json.dumps(props)}', '4326')"
        )
        self._cursor.execute(sql)

        sql = (
            f'INSERT INTO "{user_table}" (id, geometry) '
            "VALUES (1, 'POLYGON((-5 10, -5 11, 5 11, 5 10, -5 10))');"
        )
        self._cursor.execute(sql)
        sql = (
            f'INSERT INTO "{user_table}" (id, geometry) '
            "VALUES (2, 'POLYGON((-6 9, -6 10, 3 10, 3 9, -6 9))');"
        )
        self._cursor.execute(sql)

        sql = f"SELECT geodb_get_collection_bbox('{user_table}')"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self.assertEqual("BOX(-6 9,5 11)", res[0])

    def test_get_geometry_types(self):
        user_name = "geodb_user"
        self._set_role(user_name)
        user_table = user_name + "_test"

        props = {}
        sql = (
            f"SELECT geodb_create_collection('{user_table}', "
            f"'{json.dumps(props)}', '4326')"
        )
        self._cursor.execute(sql)

        sql = (
            f'INSERT INTO "{user_table}" (id, geometry) '
            "VALUES (1, 'POLYGON((-5 10, -5 11, 5 11, 5 10, -5 10))');"
        )
        self._cursor.execute(sql)
        sql = (
            f'INSERT INTO "{user_table}" (id, geometry) '
            "VALUES (2, 'POLYGON((-6 9, -6 10, 3 10, 3 9, -6 9))');"
        )
        self._cursor.execute(sql)
        sql = f"INSERT INTO \"{user_table}\" (id, geometry) VALUES (3, 'POINT(-6 9)');"
        self._cursor.execute(sql)

        sql = f"SELECT geodb_geometry_types('{user_table}', 'false')"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self.assertListEqual(
            [
                {"geometrytype": "POLYGON"},
                {"geometrytype": "POLYGON"},
                {"geometrytype": "POINT"},
            ],
            res[0],
        )

        sql = f"SELECT geodb_geometry_types('{user_table}', 'true')"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self.assertListEqual(
            [{"geometrytype": "POINT"}, {"geometrytype": "POLYGON"}], res[0]
        )

    def test_estimate_collection_bbox(self):
        user_name = "geodb_user-with-hyphens"
        user_table = user_name + "_test"
        self._set_role(user_name)

        props = {}
        sql = (
            f"SELECT geodb_create_collection('{user_table}', "
            f"'{json.dumps(props)}', '4326')"
        )
        self._cursor.execute(sql)

        sql = (
            f'INSERT INTO "{user_table}" (id, geometry) '
            "VALUES (1, 'POLYGON((-5 10, -5 11, 5 11, 5 10, -5 10))');"
        )
        self._cursor.execute(sql)
        sql = (
            f'INSERT INTO "{user_table}" (id, geometry) '
            "VALUES (2, 'POLYGON((-6 9, -6 10, 3 10, 3 9, -6 9))');"
        )
        self._cursor.execute(sql)

        sql = f'ANALYZE "{user_table}";'
        self._cursor.execute(sql)

        sql = f"SELECT geodb_estimate_collection_bbox('{user_table}')"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self.assertEqual(
            "BOX(-6.054999828338623 8.989999771118164,"
            "5.054999828338623 11.010000228881836)",
            res[0],
        )

    def test_index_functions_not_permitted(self):
        self._set_role("geodb_user_read_only")
        with self.assertRaises(psycopg2.errors.InsufficientPrivilege) as context:
            self.execute("SELECT geodb_show_indexes('geodb_user_land_use')")

        self.assertTrue(
            "permission denied for function geodb_show_indexes"
            in str(context.exception)
        )

    def test_index_functions(self):
        self.execute("SELECT geodb_show_indexes('geodb_user_land_use')")
        self.assertListEqual([("geodb_user_land_use_pkey",)], self._cursor.fetchall())

        self.execute("SELECT geodb_create_index('geodb_user_land_use', 'geometry')")
        self.execute("SELECT geodb_show_indexes('geodb_user_land_use')")
        self.assertListEqual(
            [("geodb_user_land_use_pkey",), ("idx_geometry_geodb_user_land_use",)],
            self._cursor.fetchall(),
        )

        self.execute("SELECT geodb_drop_index('geodb_user_land_use', 'geometry')")
        self.execute("SELECT geodb_show_indexes('geodb_user_land_use')")
        self.assertListEqual([("geodb_user_land_use_pkey",)], self._cursor.fetchall())

    def test_cant_create_index_twice(self):
        self.execute("SELECT geodb_create_index('geodb_user_land_use', 'geometry')")
        with self.assertRaises(psycopg2.errors.DuplicateTable):
            self.execute("SELECT geodb_create_index('geodb_user_land_use', 'geometry')")
        self._conn.commit()
        self._cursor = self._conn.cursor()
        self.execute("SELECT geodb_drop_index('geodb_user_land_use', 'geometry')")
        self.execute("SELECT geodb_create_index('geodb_user_land_use', 'geometry')")

    def test_geodb_create_role(self):
        user_name = "geodb_user"
        self._set_role(user_name)

        with self.assertRaises(psycopg2.errors.RaiseException):
            self.execute(f"SELECT geodb_create_role('{user_name}', 'some_group')")

        self._conn.commit()
        self._cursor = self._conn.cursor()

        user_name = "geodb_admin"
        self._set_role("postgres")
        self.execute("DROP ROLE IF EXISTS some_group")
        self._cursor = self._conn.cursor()

        self._set_role(user_name)
        self.execute(f"SELECT geodb_create_role('{user_name}', 'some_group')")

    def execute(self, sql):
        self._cursor.execute(sql)
        self._conn.commit()

    # noinspection SpellCheckingInspection
    def test_issue_35_unpublish(self):
        user_name = "geodb_user"
        self._set_role(user_name)

        sql = "SELECT geodb_grant_access_to_collection('geodb_user_land_use', 'public')"
        self._cursor.execute(sql)

        sql = (
            "SELECT geodb_revoke_access_from_collection( "
            "'geodb_user_land_use', 'public')"
        )
        self._cursor.execute(sql)

        self._test_publish_unpublish("alllowercase")
        self._test_publish_unpublish("alllowercase_with_number0")
        self._test_publish_unpublish("ALL_UPPERCASE")
        self._test_publish_unpublish("ALL_UPPERCASE_WITH_NUMBER0")
        self._test_publish_unpublish("partlyUppercase")
        self._test_publish_unpublish("partlyUppercase_with_number1")

    def _test_publish_unpublish(self, name):
        sql = (
            f'CREATE TABLE "{name}" (id SERIAL '
            "PRIMARY KEY, geometry geometry(Geometry, 3794) NOT NULL);"
        )
        self._cursor.execute(sql)

        sql = (
            f'INSERT INTO "{name}" (id, geometry)'
            "VALUES (1, '0103000020D20E000001000000110000007593188402B51B41B6F3FDD4423FF6405839B4C802B51B412B8716D9EC3EF6406F1283C0EBB41B41A8C64B37C53EF640B6F3FDD4E4B41B419A999999A33EF6400E2DB29DCFB41B41EE7C3F35B63EF6407F6ABC74C0B41B41EE7C3F35B63EF6407B14AE47BDB41B41AAF1D24D043FF6408B6CE77B64B41B413F355EBA8F3FF6402B8716D970B41B41986E1283EC3FF640A4703D0A76B41B4179E92631AE3FF6404260E5D08AB41B4123DBF97E923FF6409EEFA7C69CB41B4100000000AC3FF6405839B448B3B41B411D5A643B973FF6408195438BC6B41B41666666666C3FF640D122DBF9E3B41B4139B4C876383FF640E9263188F8B41B41333333333D3FF6407593188402B51B41B6F3FDD4423FF640')"
        )
        self._cursor.execute(sql)

        sql = f"SELECT geodb_grant_access_to_collection('{name}','postgres')"
        self._cursor.execute(sql)
        sql = f"SELECT geodb_revoke_access_from_collection('{name}', 'postgres')"
        self._cursor.execute(sql)

    def test_log_event(self):
        event = {
            "event_type": EventType.CREATED,
            "message": "something something something dark side",
            "user": "thomas",
        }
        sql = f"SELECT geodb_log_event('{json.dumps(event)}'::json)"
        self._cursor.execute(sql)
        sql = 'SELECT * from "geodb_eventlog"'
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self.assertEqual("created", res[0])
        self.assertEqual("something something something dark side", res[1])
        self.assertEqual("thomas", res[2])
        self.assertEqual(datetime.datetime, type(res[3]))

    def test_get_event_log(self):
        event_type_list = [
            event for event in EventType.__dict__.keys() if not event.startswith("__")
        ]
        for t in event_type_list:
            event = {
                "event_type": t,
                "message": f"{t} happened on database_collection",
                "user": "thomas",
            }
            sql = f"SELECT geodb_log_event('{json.dumps(event)}'::json)"
            self._cursor.execute(sql)

        event = {
            "event_type": "ROWS_ADDED",
            "message": "added rows happened on db_col",
            "user": "wahnfried",
        }
        sql = f"SELECT geodb_log_event('{json.dumps(event)}'::json)"
        self._cursor.execute(sql)

        sql = "SELECT get_geodb_eventlog()"
        self._cursor.execute(sql)
        events = self._cursor.fetchall()[0][0]
        self.assertEqual(len(event_type_list) + 1, len(events))

        first_event = events[0]
        self.assertEqual("CREATED", first_event["event_type"])
        self.assertEqual(
            "CREATED happened on database_collection", first_event["message"]
        )
        self.assertEqual("thomas", first_event["username"])

        last_event = events[-1]
        self.assertEqual("ROWS_ADDED", last_event["event_type"])
        self.assertEqual("added rows happened on db_col", last_event["message"])
        self.assertEqual("wahnfried", last_event["username"])

        sql = "SELECT get_geodb_eventlog('PUBLISHED')"
        self._cursor.execute(sql)
        events = self._cursor.fetchall()[0][0]
        self.assertEqual(1, len(events))

        self.assertEqual("PUBLISHED", events[0]["event_type"])
        self.assertEqual(
            "PUBLISHED happened on database_collection", events[0]["message"]
        )
        self.assertEqual("thomas", events[0]["username"])

        sql = "SELECT get_geodb_eventlog('%', 'db_col')"
        self._cursor.execute(sql)
        events = self._cursor.fetchall()[0][0]
        self.assertEqual(1, len(events))

        self.assertEqual("ROWS_ADDED", events[0]["event_type"])
        self.assertEqual("added rows happened on db_col", events[0]["message"])
        self.assertEqual("wahnfried", events[0]["username"])
