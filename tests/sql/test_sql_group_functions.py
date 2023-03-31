import json
import os
import unittest

import psycopg2

from .test_sql_functions import GeoDBSqlTest
from .test_sql_functions import get_app_dir


class GeoDBSQLGroupTest(unittest.TestCase):

    @classmethod
    def setUp(cls) -> None:
        cls.base_test = GeoDBSqlTest()
        cls.base_test.setUp()
        cls._cursor = cls.base_test._cursor
        cls._set_role = cls.base_test._set_role
        cls._conn = cls.base_test._conn

        app_path = get_app_dir()
        fn = os.path.join(app_path, '..', 'tests', 'sql', 'setup-groups.sql')
        with open(fn) as sql_file:
            cls.base_test._cursor.execute(sql_file.read())

    def tearDown(self) -> None:
        self.base_test.tearDown()


    def test_basic_group_actions(self):
        admin = "test_admin"
        member = "test_member"
        member_2 = "test_member_2"
        nomember = "test_nomember"
        table_name = "test_member_table_for_group"

        self._set_role(admin)

        sql = f"GRANT \"test_group\" TO \"{member}\"; " \
              f"GRANT \"test_group\" TO \"{member_2}\";"
        self._cursor.execute(sql)

        self.create_table_as_user(member, table_name)
        self.access_table_with_user_fail(member_2, table_name)
        self.access_table_with_user_fail(nomember, table_name)
        self.publish_to_group(member, table_name)
        self.access_table_with_user_success(member_2, table_name)
        self.access_table_with_user_fail(nomember, table_name)

        self._set_role(admin)
        sql = f"REVOKE \"test_group\" FROM \"{member_2}\"; "
        self._cursor.execute(sql)
        self.access_table_with_user_fail(member_2, table_name)

        self._set_role(admin)
        sql = f"GRANT \"test_group\" TO \"{member_2}\"; "
        self._cursor.execute(sql)
        self.access_table_with_user_success(member_2, table_name)
        self.unpublish_from_group(member, table_name)
        self.access_table_with_user_fail(member_2, table_name)


        # self.assertTrue(self.table_exists(table_name))
        #
        # self.assertTrue(self.column_exists(table_name, 'id', 'integer'))
        # self.assertTrue(
        #     self.column_exists(table_name, 'geometry', 'USER-DEFINED'))
        #
        # datasets = {
        #     'geodb_user_tt1': {'crs': '4326', 'properties': {'tt': 'integer'}},
        #     'geodb_user_tt2': {'crs': '4326', 'properties': {'tt': 'integer'}}}
        #
        # sql = f"SELECT geodb_create_collections('{json.dumps(datasets)}')"
        # self._cursor.execute(sql)
        #
        # self.assertTrue(self.table_exists(table_name))

    def unpublish_from_group(self, member_user_name, table_name):
        self._set_role(member_user_name)
        sql = f"SELECT geodb_group_unpublish_collection('{table_name}'," \
              f"'test_group')"
        self._cursor.execute(sql)

    def publish_to_group(self, member_user_name, table_name):
        self._set_role(member_user_name)
        sql = f"SELECT geodb_group_publish_collection('{table_name}'," \
              f"'test_group')"
        self._cursor.execute(sql)

    def access_table_with_user_fail(self, user_name, table_name):
        self._set_role(user_name)
        sql = f"SELECT geodb_get_collection_bbox('{table_name}')"
        with self.assertRaises(psycopg2.errors.InsufficientPrivilege):
            self._cursor.execute(sql)
        # necessary so we can keep using the connection after the failed query
        self._conn.rollback()

    def access_table_with_user_success(self, user_name, table_name):
        self._set_role(user_name)
        sql = f"SELECT geodb_get_collection_bbox('{table_name}')"
        self._cursor.execute(sql)

    def create_table_as_user(self, member_user_name, table):
        self._set_role(member_user_name)
        props = {}
        sql = f"SELECT geodb_create_database('test_member')"
        self._cursor.execute(sql)
        sql = f"SELECT geodb_create_collection('{table}', " \
              f"'{json.dumps(props)}', '4326')"
        self._cursor.execute(sql)
        self._conn.commit()