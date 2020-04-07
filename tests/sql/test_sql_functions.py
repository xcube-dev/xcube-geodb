import os
import unittest
import json


# noinspection SqlNoDataSourceInspection
@unittest.skipIf(os.environ.get('SKIP_PSQL_TESTS', False), 'DB Tests skipped')
class GeoDBSqlTest(unittest.TestCase):

    @classmethod
    def setUp(cls) -> None:
        if os.environ.get('SKIP_PSQL_TESTS', False):
            return

        import psycopg2
        import testing.postgresql
        postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)

        cls._postgresql = postgresql()
        conn = psycopg2.connect(**cls._postgresql.dsn())
        cls._cursor = conn.cursor()
        with open('tests/sql/setup.sql') as sql_file:
            cls._cursor.execute(sql_file.read())

        with open('xcube_geodb/sql/get_by_bbox.sql') as sql_file:
            sql_create = sql_file.read()
            cls._cursor.execute(sql_create)

        with open('xcube_geodb/sql/manage_table.sql') as sql_file:
            sql_create = sql_file.read()
            cls._cursor.execute(sql_create)

        with open('xcube_geodb/sql/manage_properties.sql') as sql_file:
            sql_create = sql_file.read()
            cls._cursor.execute(sql_create)

        with open('xcube_geodb/sql/manage_users.sql') as sql_file:
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
        sql_filter = "SELECT geodb_get_by_bbox('postgres_land_use', 452750.0, 88909.549, 464000.0, " \
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

    def _set_role(self, user_name: str):
        sql = f"SET LOCAL ROLE \"{user_name}\""
        self._cursor.execute(sql)

    def test_manage_table(self):
        user_name = "geodb_9bfgsdfg-453f-445b-a459-osdvjosdvjva"
        user_table = f"{user_name}_test"
        self._set_role(user_name)

        props = {'tt': 'integer'}
        sql = f"SELECT geodb_create_collection('test', '{json.dumps(props)}', '4326')"
        self._cursor.execute(sql)

        self.assertTrue(self.table_exists(user_table))

        self.assertTrue(self.column_exists(user_table, 'id', 'integer'))
        self.assertTrue(self.column_exists(user_table, 'geometry', 'USER-DEFINED'))

        datasets = {'tt1': {'crs': '4326', 'properties': {'tt': 'integer'}},
                    'tt2': {'crs': '4326', 'properties': {'tt': 'integer'}}}

        sql = f"SELECT geodb_create_collections('{json.dumps(datasets)}')"
        self._cursor.execute(sql)

        self.assertTrue(self.table_exists(user_table))

        self.assertTrue(self.column_exists(user_table, 'id', 'integer'))
        self.assertTrue(self.column_exists(user_table, 'geometry', 'USER-DEFINED'))

        datasets = ['test', 'tt1', 'tt2']
        sql = f"SELECT geodb_drop_collections('{json.dumps(datasets)}')"
        self._cursor.execute(sql)
        self.assertFalse(self.table_exists(user_table))

    def test_manage_properties(self):
        user_name = "geodb_9bfgsdfg-453f-445b-a459-osdvjosdvjva"
        table = "land_use"
        user_table = f"{user_name}_{table}"
        self._set_role(user_name)

        props = {'tt': 'integer'}
        sql = f"SELECT geodb_create_collection('land_use', '{json.dumps(props)}', '4326')"
        self._cursor.execute(sql)

        cols = {'test_col1': 'integer', 'test_col2': 'integer'}

        sql = f"SELECT public.geodb_add_properties('{table}', '{json.dumps(cols)}'::json)"
        self._cursor.execute(sql)

        self.assertTrue(self.column_exists(user_table, 'test_col1', 'integer'))

        cols = ['test_col1', 'test_col2']

        sql = f"SELECT public.geodb_drop_properties('{table}', '{json.dumps(cols)}')"
        self._cursor.execute(sql)
        self.assertFalse(self.column_exists(user_table, 'test_col', 'integer'))

    def test_manage_users(self):
        sql = f"SELECT public.geodb_register_user('test', 'test')"
        r = self._cursor.execute(sql)

        sql = f"SELECT public.geodb_user_exists('test')"
        r = self._cursor.execute(sql)

        sql = f"SELECT public.geodb_drop_user('test')"
        r = self._cursor.execute(sql)

        print(r)

    def test_get_my_usage(self):
        user_name = "geodb_9bfgsdfg-453f-445b-a459-osdvjosdvjva"
        self._set_role(user_name)

        props = {'tt': 'integer'}
        sql = f"SELECT geodb_create_collection('test_usage', '{json.dumps(props)}', '4326')"
        self._cursor.execute(sql)

        sql = f"SELECT public.geodb_get_my_usage()"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()

        sql = f"SELECT current_user"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        print(res)
