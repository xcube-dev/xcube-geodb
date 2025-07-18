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

import json
import os
import unittest

import pandas as pd
import psycopg2

from tests.sql.test_sql_functions import GeoDBSqlTest
from tests.sql.test_sql_functions import get_app_dir


class GeoDBSQLGroupTest(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        cls.base_test = GeoDBSqlTest()
        cls.base_test.setUp()
        cls._cursor = cls.base_test._cursor
        cls._set_role = cls.base_test._set_role
        cls._conn = cls.base_test._conn

        app_path = get_app_dir()
        fn = os.path.join(app_path, "..", "tests", "sql", "setup-groups.sql")
        with open(fn) as sql_file:
            cls.base_test._cursor.execute(sql_file.read())

        cls._conn.commit()
        cls.admin = "test_admin"
        cls.noadmin = "test_noadmin"
        cls.member = "test_member"
        cls.member_2 = "test_member_2"
        cls.nomember = "test_nomember"
        cls.table_name = "test_member_table_for_group"
        cls.table_name_2 = "test_member_table2_for_group"
        cls.table_name_3 = "test_member_table3_for_group"
        cls.database_name = "test_member"
        cls.test_group = "test_group"

    def tearDown(self) -> None:
        self.base_test.tearDown()

    def test_group_actions(self):
        self.grant_group_to(self.member)
        self.grant_group_to(self.member_2)

        self.create_database_and_table_as_user(self.member)
        self.access_table_with_user_fail(self.member_2)
        self.access_table_with_user_fail(self.nomember)
        self.publish_table_to_group(self.member)

        self.publish_database_to_group(self.member)
        self.create_table_as_user(self.member_2, self.table_name_2)
        self.unpublish_database_from_group(self.member)
        self.create_table_as_user_fails(self.member_2, self.table_name_3)

        self.access_table_with_user_success(self.member_2)
        self.access_table_with_user_fail(self.nomember)

        self.revoke_group_from(self.member_2)
        self.access_table_with_user_fail(self.member_2)

        self.grant_group_to(self.member_2)
        self.access_table_with_user_success(self.member_2)

        self.unpublish_from_group(self.member)
        self.access_table_with_user_fail(self.member_2)

    def test_get_user_roles(self):
        self.grant_group_to(self.member)
        self.execute(f"SELECT geodb_get_user_roles('{self.member}')")
        role_names = self.retrieve_role_names()
        self.assertEqual(2, len(role_names))
        self.assertEqual(role_names[0], self.test_group)
        self.assertEqual(role_names[1], "test_member")

        self.execute(f"SELECT geodb_get_user_roles('{self.member_2}')")
        role_names = self.retrieve_role_names()
        self.assertEqual(1, len(role_names))
        self.assertEqual(role_names[0], "test_member_2")

        self.grant_group_to(self.member_2)
        self.execute(f"SELECT geodb_get_user_roles('{self.member_2}')")
        role_names = self.retrieve_role_names()
        self.assertEqual(2, len(role_names))
        self.assertEqual(role_names[0], self.test_group)
        self.assertEqual(role_names[1], "test_member_2")

        self.execute(f"SELECT geodb_get_user_roles('{self.nomember}')")
        role_names = self.retrieve_role_names()
        self.assertEqual(1, len(role_names))
        self.assertEqual(role_names[0], "test_nomember")

    def test_get_grants(self):
        self.grant_group_to(self.member)
        self.create_database_and_table_as_user(self.member)
        self.publish_table_to_group(self.member)
        self._set_role(self.member)
        self.execute(f"SELECT geodb_get_grants('{self.table_name}')")
        grants = self.retrieve_grants()

        self.assertEqual(2, len(grants))
        self.assertTrue(self.member in grants)
        self.assertTrue(self.test_group in grants)
        self.assertTrue("SELECT" in grants[self.member])
        self.assertTrue("UPDATE" in grants[self.member])

    def test_create_role(self):
        new_group_name = "new_group"
        with self.assertRaises(psycopg2.errors.InvalidParameterValue):
            self._set_role(new_group_name)
        self._conn.commit()
        self._cursor = self._conn.cursor()
        self._set_role(self.admin)
        self.execute(f"SELECT geodb_create_role('{self.admin}', '{new_group_name}')")
        self.grant_group_to(self.member)

    def test_create_role_fails(self):
        self._set_role(self.noadmin)
        with self.assertRaises(psycopg2.errors.RaiseException):
            self.execute(f"SELECT geodb_create_role('{self.noadmin}', 'any_group')")

    def test_get_group_users(self):
        self.execute(f"SELECT geodb_get_group_users('{self.test_group}')")
        users = self.get_group_users()
        self.assertListEqual(["test_admin"], users)

        self.grant_group_to(self.member)
        self.grant_group_to(self.member_2)
        self.execute(f"SELECT geodb_get_group_users('{self.test_group}')")
        users = self.get_group_users()
        self.assertListEqual(["test_admin", self.member, self.member_2], users)

        self.revoke_group_from(self.member_2)
        self.execute(f"SELECT geodb_get_group_users('{self.test_group}')")
        users = self.get_group_users()
        self.assertListEqual(["test_admin", self.member], users)

    def get_group_users(self):
        result = self._cursor.fetchall()
        df = pd.DataFrame(result[0][0])
        users = sorted(df["rolname"].tolist())
        return users

    def retrieve_grants(self):
        result = self._cursor.fetchall()
        df = pd.DataFrame(result[0][0])
        df = df.groupby("grantee")["privilege_type"].apply(list)
        return df.to_dict()

    def retrieve_role_names(self):
        result = self._cursor.fetchone()
        df = pd.DataFrame(result[0])
        role_names = sorted(list(df["rolname"]))
        return role_names

    def execute(self, sql):
        self._cursor.execute(sql)
        self._conn.commit()

    def revoke_group_from(self, user):
        self._set_role(self.admin)
        sql = f"SELECT geodb_group_revoke('{self.test_group}', '{user}');"
        self.execute(sql)

    def grant_group_to(self, user):
        self._set_role(self.admin)
        sql = f"SELECT geodb_group_grant('{self.test_group}', '{user}');"
        self.execute(sql)

    def unpublish_from_group(self, user):
        self._set_role(user)
        sql = (
            f"SELECT geodb_group_unpublish_collection('{self.table_name}',"
            f" '{self.test_group}')"
        )
        self.execute(sql)

    def publish_table_to_group(self, user):
        self._set_role(user)
        sql = (
            f"SELECT geodb_group_publish_collection('{self.table_name}',"
            f"'{self.test_group}')"
        )
        self.execute(sql)

    def publish_database_to_group(self, user):
        self._set_role(user)
        sql = (
            f"SELECT geodb_group_publish_database('{self.database_name}',"
            f"'{self.test_group}')"
        )
        self.execute(sql)

    def unpublish_database_from_group(self, user):
        self._set_role(user)
        sql = (
            f"SELECT geodb_group_unpublish_database('{self.database_name}',"
            f"'{self.test_group}')"
        )
        self.execute(sql)

    def access_table_with_user_fail(self, user):
        self._set_role(user)
        sql = f"SELECT geodb_get_collection_bbox('{self.table_name}')"
        with self.assertRaises(psycopg2.errors.InsufficientPrivilege):
            self.execute(sql)
        # necessary so we can keep using the connection after the failed query
        self._conn.rollback()

    def access_table_with_user_success(self, user):
        self._set_role(user)
        sql = f"SELECT geodb_get_collection_bbox('{self.table_name}')"
        self.execute(sql)

    def create_database_and_table_as_user(self, user):
        self._set_role(user)
        sql = f"SELECT geodb_create_database('{self.database_name}')"
        self.execute(sql)
        self._set_role(user)
        props = {}
        sql = (
            f"SELECT geodb_create_collection('{self.table_name}', "
            f"'{json.dumps(props)}', '4326')"
        )
        self.execute(sql)

    def create_table_as_user(self, user, table_name):
        self._set_role(user)
        props = {}
        sql = (
            f"SELECT geodb_create_collection('{table_name}', "
            f"'{json.dumps(props)}', '4326')"
        )
        self.execute(sql)

    def create_table_as_user_fails(self, user, table_name):
        self._set_role(user)
        props = {}
        sql = (
            f"SELECT geodb_create_collection('{table_name}', "
            f"'{json.dumps(props)}', '4326')"
        )
        with self.assertRaises(psycopg2.errors.RaiseException):
            self.execute(sql)
        # necessary so we can keep using the connection after the failed query
        self._conn.rollback()
