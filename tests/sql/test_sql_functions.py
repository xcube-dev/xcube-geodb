from time import sleep
import datetime
import os
import signal
import sys
import unittest
import json
import psycopg2
import signal
from time import sleep

from tests.utils import make_install_geodb
import xcube_geodb.version as version
from xcube_geodb.core.geodb import EventType


def get_app_dir():
    import inspect

    # noinspection PyTypeChecker
    version_path = inspect.getfile(version)
    return os.path.dirname(version_path)


@unittest.skipIf(os.environ.get('SKIP_INSTALLATION_TESTS', '0') == '1',
                 'Installation tests skipped')
class TestInstallationProcedure(unittest.TestCase):
    def tearDown(self) -> None:
        app_path = get_app_dir()
        control_fn = os.path.join(app_path, 'sql', 'geodb.control')
        os.remove(control_fn)
        control_fn = os.path.join(app_path, 'sql',
                                  f'geodb--{version.version}.sql')
        os.remove(control_fn)

    def testInstallation(self):
        make_install_geodb()


# noinspection SqlNoDataSourceInspection,SqlResolve
@unittest.skipIf(os.environ.get('SKIP_PSQL_TESTS', '0') == '1',
                 'DB Tests skipped')
# noinspection SqlInjection
class GeoDBSqlTest(unittest.TestCase):
    _postgresql = None
    _cursor = None
    _conn = None

    @classmethod
    def setUp(cls) -> None:
        import psycopg2
        import testing.postgresql

        # for Windows, in case there are multiple installations of Postgres
        # which makes psycopg find initdb like this:
        # c:\path\to\installation-1\initdb.exe\r\nc:\path\to\installation-2\initdb.exe
        # therefore, we split, and use the first installation that has "PostgreSQL" in its path
        # because the PostGIS extension cannot be installed via conda nor pip in a compatible way

        def find_program(name: str) -> str:
            program = testing.postgresql.find_program(name, [])
            return program.split("\r\n")[0]

        if os.name == 'nt':
            path = os.environ["PATH"].split(os.pathsep)
            dirs = [directory for directory in path if "PostgreSQL" in directory]
            dirs.extend([directory for directory in path if not "PostgreSQL" in directory])
            os.environ["PATH"] = os.pathsep.join(dirs)

        initdb = find_program('initdb')
        postgres = find_program('postgres')

        # special windows treatment end

        postgresql = testing.postgresql.PostgresqlFactory(
            cache_initialized_db=False, initdb=initdb, postgres=postgres, port=50777)

        cls._postgresql = postgresql()
        dsn = cls._postgresql.dsn()
        if sys.platform == 'win32':
            dsn['port'] = 50777
            dsn['password'] = 'postgres'
        cls._conn = psycopg2.connect(**dsn)
        cls._cursor = cls._conn.cursor()
        app_path = get_app_dir()
        fn = os.path.join(app_path, 'sql', 'geodb.sql')
        with open(fn) as sql_file:
            sql_content = sql_file.read()
            sql_content = sql_content.replace('VERSION_PLACEHOLDER',
                                              version.version)
            cls._cursor.execute(sql_content)

        fn = os.path.join(app_path, '..', 'tests', 'sql', 'setup.sql')
        with open(fn) as sql_file:
            cls._cursor.execute(sql_file.read())
        cls._conn.commit()

    def tearDown(self) -> None:
        if sys.platform == 'win32':
            self.manual_cleanup()
        else:
            self._postgresql.stop()

    def manual_cleanup(self):
        import shutil

        try:
            self._conn.close()
            dsn = self._postgresql.dsn()
            dsn['port'] = 50777
            dsn['password'] = 'postgres'
            dsn['database'] = 'postgres'
            self._conn = psycopg2.connect(**dsn)
            self._conn.autocommit = True
            self._cursor = self._conn.cursor()
            self._cursor.execute('drop database test;')
            self._cursor.execute('create database test;')
            self._set_role('postgres')
            self._cursor.execute(
                'DROP ROLE IF EXISTS geodb_user ; '
                'DROP ROLE IF EXISTS "geodb_user-with-hyphens" ; '
                'DROP ROLE IF EXISTS test_group ; '
                'DROP ROLE IF EXISTS new_group ; '
                'DROP ROLE IF EXISTS test_admin ; '
                'DROP ROLE IF EXISTS test_noadmin ; '
                'DROP ROLE IF EXISTS test_member ; '
                'DROP ROLE IF EXISTS test_member_2 ; '
                'DROP ROLE IF EXISTS test_nomember ;'
            )
            self._conn.commit()
            self._conn.close()
            self._postgresql.child_process.send_signal(signal.CTRL_BREAK_EVENT)
            killed_at = datetime.datetime.now()
            while self._postgresql.child_process.poll() is None:
                if (datetime.datetime.now() - killed_at).seconds > 10.0:
                    self._postgresql.child_process.kill()
                    raise RuntimeError("*** failed to shutdown postgres (timeout) ***\n")

                sleep(0.1)

        except OSError as e:
            raise e
        shutil.rmtree(self._postgresql.base_dir, ignore_errors=True)

    def tearDownModule(self):
        # clear cached database at end of tests
        self._postgresql.clear_cache()

    def test_query_by_bbox(self):
        sql_filter = "SELECT geodb_get_by_bbox('postgres_land_use', 452750.0, 88909.549, 464000.0, " \
                     "102486.299, 'contains', 3794)"
        self._cursor.execute(sql_filter)

        res = self._cursor.fetchone()

        exp_geo = {'type': 'Polygon',
                   'coordinates': [
                       [[453952.629, 91124.177], [453952.696, 91118.803],
                        [453946.938, 91116.326], [453945.208, 91114.225],
                        [453939.904, 91115.388], [453936.114, 91115.388],
                        [453935.32, 91120.269], [453913.121, 91128.983],
                        [453916.212, 91134.782], [453917.51, 91130.887],
                        [453922.704, 91129.156], [453927.194, 91130.75],
                        [453932.821, 91129.452], [453937.636, 91126.775],
                        [453944.994, 91123.529], [453950.133, 91123.825],
                        [453952.629, 91124.177]]]}
        self.assertEqual(len(res), 1)

        self.assertEqual(res[0][0]['id'], 1)
        self.assertEqual(res[0][0]['geometry']['type'], exp_geo['type'])
        self.assertEqual(res[0][0]['geometry']['coordinates'],
                         exp_geo['coordinates'])

    def column_exists(self, table: str, column: str, data_type: str) -> bool:
        sql = (f'\n'
               f'                    SELECT EXISTS\n'
               f'                    (\n'
               f'                        SELECT 1\n'
               f'                        FROM "information_schema".columns\n'
               f'                        WHERE "table_schema" = \'public\'\n'
               f'                          AND "table_name"   = \'{table}\'\n'
               f'                          AND "column_name" = \'{column}\'\n'
               f'                          AND "data_type" = \'{data_type}\'\n'
               f'                    )\n'
               f'                         ;\n'
               f'            ')
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
        user_name = "geodb_user"
        user_table = user_name + "_test"
        self._set_role(user_name)

        props = {'tt': 'integer'}
        sql = f"SELECT geodb_create_collection('{user_table}', " \
              f"'{json.dumps(props)}', '4326')"
        self._cursor.execute(sql)

        self.assertTrue(self.table_exists(user_table))

        self.assertTrue(self.column_exists(user_table, 'id', 'integer'))
        self.assertTrue(
            self.column_exists(user_table, 'geometry', 'USER-DEFINED'))

        datasets = {
            'geodb_user_tt1': {'crs': '4326', 'properties': {'tt': 'integer'}},
            'geodb_user_tt2': {'crs': '4326', 'properties': {'tt': 'integer'}}}

        sql = f"SELECT geodb_create_collections('{json.dumps(datasets)}')"
        self._cursor.execute(sql)

        self.assertTrue(self.table_exists(user_table))

        self.assertTrue(self.column_exists(user_table, 'id', 'integer'))
        self.assertTrue(
            self.column_exists(user_table, 'geometry', 'USER-DEFINED'))

        datasets = ['geodb_user_test', 'geodb_user_tt1', 'geodb_user_tt2']
        sql = f"SELECT geodb_drop_collections('{json.dumps(datasets)}')"
        self._cursor.execute(sql)
        self.assertFalse(self.table_exists(user_table))

    def test_manage_properties(self):
        user_name = "geodb_user"
        table = user_name + "_land_use_test"
        self._set_role(user_name)

        props = {'tt': 'integer'}
        sql = f"SELECT geodb_create_collection('{table}', '{json.dumps(props)}', '4326')"
        self._cursor.execute(sql)

        cols = {'test_col1': 'integer', 'test_col2': 'integer'}

        sql = f"SELECT public.geodb_add_properties('{table}', '{json.dumps(cols)}'::json)"
        self._cursor.execute(sql)

        self.assertTrue(self.column_exists(table, 'test_col1', 'integer'))

        cols = ['test_col1', 'test_col2']

        sql = f"SELECT public.geodb_drop_properties('{table}', '{json.dumps(cols)}')"
        self._cursor.execute(sql)
        self.assertFalse(self.column_exists(table, 'test_col', 'integer'))

    def test_get_my_usage(self):
        user_name = "geodb_user"
        self._set_role(user_name)

        props = {'tt': 'integer'}
        sql = f"SELECT geodb_create_collection('geodb_user_test_usage', '{json.dumps(props)}', '4326')"
        self._cursor.execute(sql)

        sql = f"SELECT public.geodb_get_my_usage()"
        self._cursor.execute(sql)
        self._cursor.fetchone()

        sql = f"SELECT current_user"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        print(res)

    def test_create_database(self):
        user_name = "geodb_user"
        self._set_role(user_name)

        sql = f"SELECT geodb_create_database('test')"
        self._cursor.execute(sql)

        sql = f"SELECT * FROM geodb_user_databases WHERE name='test' AND owner = '{user_name}'"
        self._cursor.execute(sql)
        res = self._cursor.fetchall()

        self.assertEqual(1, len(res))
        res = res[0]
        self.assertEqual('test', res[1])
        self.assertEqual(user_name, res[2])

        # noinspection PyUnresolvedReferences
        with self.assertRaises(psycopg2.errors.RaiseException) as e:
            sql = f"SELECT geodb_create_database('test')"
            self._cursor.execute(sql)

        self.assertIn('Database test exists already.', str(e.exception))

    def test_get_geodb_sql_version(self):
        user_name = "geodb_user"
        self._set_role(user_name)

        sql = f"SELECT geodb_get_geodb_sql_version()"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()

        self.assertEqual(1, len(res))
        self.assertEqual(version.version, res[0])

    def test_truncate_database(self):
        user_name = "geodb_user"
        self._set_role(user_name)

        sql = f"INSERT INTO geodb_user_databases(name, owner) VALUES('test_truncate', '{user_name}')"
        self._cursor.execute(sql)

        sql = f"SELECT geodb_truncate_database('test_truncate')"
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
        self.assertEqual('success', res[0][0])

        # noinspection PyUnresolvedReferences
        with self.assertRaises(psycopg2.errors.RaiseException) as e:
            sql = "SELECT geodb_rename_collection('geodb_user_land_use', 'postgres_land_use2')"
            self._cursor.execute(sql)

        self.assertIn('geodb_user has not access to that table or database. ',
                      str(e.exception))

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
        query = 'ANALYZE'
        self._cursor.execute(query)

    def test_get_collection_bbox(self):
        user_name = "geodb_user-with-hyphens"
        user_table = user_name + "_test"
        self._set_role(user_name)

        props = {}
        sql = f"SELECT geodb_create_collection('{user_table}', " \
              f"'{json.dumps(props)}', '4326')"
        self._cursor.execute(sql)

        sql = f"INSERT INTO \"{user_table}\" (id, geometry) " \
              "VALUES (1, 'POLYGON((-5 10, -5 11, 5 11, 5 10, -5 10))');"
        self._cursor.execute(sql)
        sql = f"INSERT INTO \"{user_table}\" (id, geometry) " \
              "VALUES (2, 'POLYGON((-6 9, -6 10, 3 10, 3 9, -6 9))');"
        self._cursor.execute(sql)

        sql = f"SELECT geodb_get_collection_bbox('{user_table}')"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self.assertEqual('BOX(-6 9,5 11)', res[0])

    def test_get_geometry_types(self):
        user_name = "geodb_user"
        self._set_role(user_name)
        user_table = user_name + "_test"

        props = {}
        sql = f"SELECT geodb_create_collection('{user_table}', " \
              f"'{json.dumps(props)}', '4326')"
        self._cursor.execute(sql)

        sql = f"INSERT INTO \"{user_table}\" (id, geometry) " \
              "VALUES (1, 'POLYGON((-5 10, -5 11, 5 11, 5 10, -5 10))');"
        self._cursor.execute(sql)
        sql = f"INSERT INTO \"{user_table}\" (id, geometry) " \
              "VALUES (2, 'POLYGON((-6 9, -6 10, 3 10, 3 9, -6 9))');"
        self._cursor.execute(sql)
        sql = f"INSERT INTO \"{user_table}\" (id, geometry) " \
              "VALUES (3, 'POINT(-6 9)');"
        self._cursor.execute(sql)

        sql = f"SELECT geodb_geometry_types('{user_table}', 'false')"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self.assertListEqual([{'geometrytype': 'POLYGON'},
                              {'geometrytype': 'POLYGON'},
                              {'geometrytype': 'POINT'}], res[0])

        sql = f"SELECT geodb_geometry_types('{user_table}', 'true')"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self.assertListEqual([{'geometrytype': 'POINT'},
                              {'geometrytype': 'POLYGON'}], res[0])

    def test_estimate_collection_bbox(self):
        user_name = "geodb_user-with-hyphens"
        user_table = user_name + "_test"
        self._set_role(user_name)

        props = {}
        sql = f"SELECT geodb_create_collection('{user_table}', " \
              f"'{json.dumps(props)}', '4326')"
        self._cursor.execute(sql)

        sql = f"INSERT INTO \"{user_table}\" (id, geometry) " \
              "VALUES (1, 'POLYGON((-5 10, -5 11, 5 11, 5 10, -5 10))');"
        self._cursor.execute(sql)
        sql = f"INSERT INTO \"{user_table}\" (id, geometry) " \
              "VALUES (2, 'POLYGON((-6 9, -6 10, 3 10, 3 9, -6 9))');"
        self._cursor.execute(sql)

        sql = f"ANALYZE \"{user_table}\";"
        self._cursor.execute(sql)

        sql = f"SELECT geodb_estimate_collection_bbox('{user_table}')"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self.assertEqual('BOX(-6.054999828338623 8.989999771118164,'
                         '5.054999828338623 11.010000228881836)', res[0])

    def test_index_functions(self):
        self.execute("SELECT geodb_show_indexes('geodb_user_land_use')")
        self.assertListEqual([('geodb_user_land_use_pkey',)], self._cursor.fetchall())

        self.execute("SELECT geodb_create_index('geodb_user_land_use', 'geometry')")
        self.execute("SELECT geodb_show_indexes('geodb_user_land_use')")
        self.assertListEqual([('geodb_user_land_use_pkey',), ('idx_geometry_geodb_user_land_use',)],
                             self._cursor.fetchall())

        self.execute("SELECT geodb_drop_index('geodb_user_land_use', 'geometry')")
        self.execute("SELECT geodb_show_indexes('geodb_user_land_use')")
        self.assertListEqual([('geodb_user_land_use_pkey',)], self._cursor.fetchall())

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
        self._set_role(user_name)
        self.execute(f"SELECT geodb_create_role('{user_name}', 'some_group')")


    def execute(self, sql):
        self._cursor.execute(sql)
        self._conn.commit()

    # noinspection SpellCheckingInspection
    def test_issue_35_unpublish(self):
        user_name = "geodb_user"
        self._set_role(user_name)

        sql = "SELECT geodb_grant_access_to_collection(" \
              "'geodb_user_land_use', 'public')"
        self._cursor.execute(sql)

        sql = "SELECT geodb_revoke_access_from_collection( " \
              "'geodb_user_land_use', 'public')"
        self._cursor.execute(sql)

        self._test_publish_unpublish('alllowercase')
        self._test_publish_unpublish('alllowercase_with_number0')
        self._test_publish_unpublish('ALL_UPPERCASE')
        self._test_publish_unpublish('ALL_UPPERCASE_WITH_NUMBER0')
        self._test_publish_unpublish('partlyUppercase')
        self._test_publish_unpublish('partlyUppercase_with_number1')

    def _test_publish_unpublish(self, name):
        sql = f'CREATE TABLE "{name}" (id SERIAL ' \
              'PRIMARY KEY, geometry geometry(Geometry, 3794) NOT NULL);'
        self._cursor.execute(sql)

        sql = f'INSERT INTO "{name}" (id, geometry)' \
              'VALUES (1, \'0103000020D20E000001000000110000007593188402B51B41B6F3FDD4423FF6405839B4C802B51B412B8716D9EC3EF6406F1283C0EBB41B41A8C64B37C53EF640B6F3FDD4E4B41B419A999999A33EF6400E2DB29DCFB41B41EE7C3F35B63EF6407F6ABC74C0B41B41EE7C3F35B63EF6407B14AE47BDB41B41AAF1D24D043FF6408B6CE77B64B41B413F355EBA8F3FF6402B8716D970B41B41986E1283EC3FF640A4703D0A76B41B4179E92631AE3FF6404260E5D08AB41B4123DBF97E923FF6409EEFA7C69CB41B4100000000AC3FF6405839B448B3B41B411D5A643B973FF6408195438BC6B41B41666666666C3FF640D122DBF9E3B41B4139B4C876383FF640E9263188F8B41B41333333333D3FF6407593188402B51B41B6F3FDD4423FF640\')';
        self._cursor.execute(sql)

        sql = f'SELECT geodb_grant_access_to_collection(\'{name}\',' \
              f'\'postgres\')'
        self._cursor.execute(sql)
        sql = f'SELECT geodb_revoke_access_from_collection(\'{name}\', ' \
              f'\'postgres\')'
        self._cursor.execute(sql)

    def test_log_event(self):
        event = {
            'event_type': EventType.CREATED,
            'message': 'something something something dark side',
            'user': 'thomas'
        }
        sql = f'SELECT geodb_log_event(\'{json.dumps(event)}\'::json)'
        self._cursor.execute(sql)
        sql = f'SELECT * from "geodb_eventlog"'
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self.assertEqual('created', res[0])
        self.assertEqual('something something something dark side', res[1])
        self.assertEqual('thomas', res[2])
        self.assertEqual(datetime.datetime, type(res[3]))

    def test_get_event_log(self):
        event_type_list = [event for event in EventType.__dict__.keys() if
                  not event.startswith('__')]
        for t in event_type_list:
            event = {
                'event_type': t,
                'message': f'{t} happened on database_collection',
                'user': 'thomas'
            }
            sql = f'SELECT geodb_log_event(\'{json.dumps(event)}\'::json)'
            self._cursor.execute(sql)

        event = {
            'event_type': 'ROWS_ADDED',
            'message': 'added rows happened on db_col',
            'user': 'wahnfried'
        }
        sql = f'SELECT geodb_log_event(\'{json.dumps(event)}\'::json)'
        self._cursor.execute(sql)

        sql = f'SELECT get_geodb_eventlog()'
        self._cursor.execute(sql)
        events = self._cursor.fetchall()[0][0]
        self.assertEqual(len(event_type_list) + 1, len(events))

        first_event = events[0]
        self.assertEqual('CREATED', first_event['event_type'])
        self.assertEqual('CREATED happened on database_collection',
                         first_event['message'])
        self.assertEqual('thomas', first_event['username'])

        last_event = events[-1]
        self.assertEqual('ROWS_ADDED', last_event['event_type'])
        self.assertEqual('added rows happened on db_col',
                         last_event['message'])
        self.assertEqual('wahnfried', last_event['username'])

        sql = f'SELECT get_geodb_eventlog(\'PUBLISHED\')'
        self._cursor.execute(sql)
        events = self._cursor.fetchall()[0][0]
        self.assertEqual(1, len(events))

        self.assertEqual('PUBLISHED', events[0]['event_type'])
        self.assertEqual('PUBLISHED happened on database_collection',
                         events[0]['message'])
        self.assertEqual('thomas', events[0]['username'])

        sql = f'SELECT get_geodb_eventlog(\'%\', \'db_col\')'
        self._cursor.execute(sql)
        events = self._cursor.fetchall()[0][0]
        self.assertEqual(1, len(events))

        self.assertEqual('ROWS_ADDED', events[0]['event_type'])
        self.assertEqual('added rows happened on db_col',
                         events[0]['message'])
        self.assertEqual('wahnfried', events[0]['username'])
