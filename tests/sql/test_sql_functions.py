import os
import unittest
import psycopg2
import json
import testing.postgresql


@unittest.skipIf(os.environ.get('SKIP_PSQL_TESTS', False), 'DB Tests skipped')
class GeoDBSqlTest(unittest.TestCase):

    @classmethod
    def setUp(cls) -> None:
        if os.environ.get('SKIP_PSQL_TESTS', False):
            return

        postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)

        cls._postgresql = postgresql()
        conn = psycopg2.connect(**cls._postgresql.dsn())
        cls._cursor = conn.cursor()
        with open('tests/sql/setup.sql') as sql_file:
            cls._cursor.execute(sql_file.read())

        with open('dcfs_geodb/sql/get_by_bbox.sql') as sql_file:
            sql_create = sql_file.read()
            cls._cursor.execute(sql_create)

        with open('dcfs_geodb/sql/manage_table.sql') as sql_file:
            sql_create = sql_file.read()
            cls._cursor.execute(sql_create)

        with open('dcfs_geodb/sql/manage_properties.sql') as sql_file:
            sql_create = sql_file.read()
            cls._cursor.execute(sql_create)

    def tearDown(self) -> None:
        if os.environ.get('SKIP_PSQL_TESTS', False):
            return

        self._postgresql.stop()

    def tearDownModule(self):
        if os.environ.get('SKIP_PSQL_TESTS', False):
            return

        # clear cached database at end of tests
        self._postgresql.clear_cache()

    def test_query_by_bbox(self):
        sql_filter = "SELECT geodb_get_by_bbox('land_use'::VARCHAR(255), 452750.0, 88909.549, 464000.0, " \
                     "102486.299, 'contains', 3794)"
        self._cursor.execute(sql_filter)

        res = self._cursor.fetchone()

        exp_geo = {'type': 'Polygon', 'coordinates': [
            [[453952.629, 91124.177], [453952.696, 91118.803], [453946.938, 91116.326], [453945.208, 91114.225],
             [453939.904, 91115.388], [453936.114, 91115.388], [453935.32, 91120.269], [453913.121, 91128.983],
             [453916.212, 91134.782], [453917.51, 91130.887], [453922.704, 91129.156], [453927.194, 91130.75],
             [453932.821, 91129.452], [453937.636, 91126.775], [453944.994, 91123.529], [453950.133, 91123.825],
             [453952.629, 91124.177]]]}
        self.assertEqual(len(res), 1)

        self.assertEqual(res[0][0]['id'], 1)
        self.assertDictEqual(res[0][0]['geometry'], exp_geo)

    def column_exists(self, table: str, column: str, data_type: str) -> bool:
        sql = f"""
                    SELECT EXISTS
                    (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name   = '{table}'
                          AND column_name = '{column}'
                          AND data_type = '{data_type}'
                    )
                         ;
            """
        self._cursor.execute(sql)
        return self._cursor.fetchone()[0]

    def table_exists(self, table: str) -> bool:
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
        sql = f"SELECT geodb_create_dataset('test', 4326)"
        self._cursor.execute(sql)

        self.assertTrue(self.table_exists('test'))

        self.assertTrue(self.column_exists('test', 'id', 'integer'))
        self.assertTrue(self.column_exists('test', 'geometry', 'USER-DEFINED'))

        sql = f"SELECT geodb_drop_dataset('test')"
        self._cursor.execute(sql)
        self.assertFalse(self.table_exists('test'))

    def test_manage_properties(self):
        # geodb_add_properties

        cols = {'columns': [{'name': 'test_col', 'type': 'integer'}]}

        sql = f"SELECT public.geodb_add_properties('land_use', '{json.dumps(cols)}')"
        self._cursor.execute(sql)

        self.assertTrue(self.column_exists('land_use', 'test_col', 'integer'))

        sql = f"SELECT public.geodb_drop_properties('land_use', '{json.dumps(cols)}')"
        self._cursor.execute(sql)
        self.assertFalse(self.column_exists('land_use', 'test_col', 'integer'))

        sql = f"SELECT public.geodb_add_properties('land_use', '{json.dumps(cols)}')"
        self._cursor.execute(sql)
        sql = f"SELECT public.geodb_drop_property('land_use', 'test_col')"
        self._cursor.execute(sql)
        self.assertFalse(self.column_exists('land_use', 'test_col', 'integer'))
