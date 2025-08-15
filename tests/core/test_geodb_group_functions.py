import unittest

import requests_mock

from tests.core.geodb_test_base import GeoDBClientTestBase
from xcube_geodb.core.geodb import GeoDBError
from xcube_geodb.core.message import Message


@requests_mock.mock(real_http=False)
class GeoDBClientGroupsTest(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        cls.test_base = GeoDBClientTestBase()
        cls.test_base.setUp()

    def tearDown(self) -> None:
        self.test_base.tearDown()

    def test_create_group(self, m):
        self.test_base.set_global_mocks(m)
        url = f"{self.test_base._base_url}/rpc/geodb_create_role"
        m.post(url, text="")

        usergroup = "test_group"

        r = self.test_base._api.create_group(usergroup)

        expected = {"Message": f"Created new group {usergroup}."}
        self._check_message(r, expected)

    def test_add_user_to_group(self, m):
        self.test_base.set_global_mocks(m)
        url = f"{self.test_base._base_url}/rpc/geodb_group_grant"
        username = "test_user"
        usergroup = "test_group"
        m.post(url, text="")

        r = self.test_base._api.add_user_to_group(username, usergroup)

        expected = {"Message": f"Added user {username} to {usergroup}"}
        self._check_message(r, expected)

    def test_remove_user_from_group(self, m):
        self.test_base.set_global_mocks(m)
        url = f"{self.test_base._base_url}/rpc/geodb_group_revoke"
        m.post(url, text="")

        username = "test_user"
        usergroup = "test_group"

        r = self.test_base._api.remove_user_from_group(username, usergroup)

        expected = {"Message": f"Removed user {username} from {usergroup}"}
        self._check_message(r, expected)

    def test_get_group_users(self, m):
        self.test_base.set_global_mocks(m)
        url = f"{self.test_base._base_url}/rpc/geodb_get_group_users"
        m.post(
            url,
            json=[
                {
                    "res": [
                        {"rolname": self.test_base._api.whoami},
                        {"rolname": "authenticator"},
                    ]
                }
            ],
        )

        usergroup = "test_group"

        r = self.test_base._api.get_group_users(usergroup)
        self.assertListEqual(["authenticator", self.test_base._api.whoami], r)

    def test_publish_collection_to_group(self, m):
        self.test_base.set_global_mocks(m)
        url = f"{self.test_base._base_url}/rpc/geodb_group_publish_collection"
        m.post(url, text="")
        url = f"{self.test_base._base_url}/rpc/geodb_user_allowed"
        m.post(url, text="1")  # user is owner of database

        collection = "test_col"
        database = "test_db"
        group = "test_group"

        r = self.test_base._api.publish_collection_to_group(collection, group, database)

        expected = {
            "Message": f"Published collection {collection} in "
            f"database {database} to group {group}."
        }
        self._check_message(r, expected)

    def test_publish_database_to_group(self, m):
        self.test_base.set_global_mocks(m)

        url = f"{self.test_base._base_url}/rpc/geodb_group_publish_database"
        m.post(url, text="")
        url = f"{self.test_base._base_url}/rpc/geodb_user_allowed"
        m.post(url, text="1")  # user is owner of database

        database = "test_db"
        group = "test_group"

        r = self.test_base._api.publish_database_to_group(group, database)

        expected = {"Message": f"Published database {database} to group {group}."}
        self._check_message(r, expected)

        url = f"{self.test_base._base_url}/rpc/geodb_user_allowed"
        m.post(url, text="0")  # user is NOT owner of database
        with self.assertRaises(GeoDBError):
            self.test_base._api.publish_database_to_group(group, database)

    def test_unpublish_database_from_group(self, m):
        self.test_base.set_global_mocks(m)

        url = f"{self.test_base._base_url}/rpc/geodb_group_unpublish_database"
        m.post(url, text="")
        url = f"{self.test_base._base_url}/rpc/geodb_user_allowed"
        m.post(url, text="1")  # user is owner of database

        database = "test_db"
        group = "test_group"

        r = self.test_base._api.unpublish_database_from_group(group, database)

        expected = {"Message": f"Unpublished database {database} from group {group}."}
        self._check_message(r, expected)

        url = f"{self.test_base._base_url}/rpc/geodb_user_allowed"
        m.post(url, text="0")  # user is NOT owner of database
        with self.assertRaises(GeoDBError):
            self.test_base._api.unpublish_database_from_group(group, database)

    def test_publish_collection_to_group_fails(self, m):
        self.test_base.set_global_mocks(m)
        url = f"{self.test_base._base_url}/rpc/geodb_group_publish_collection"
        m.post(url, text="")
        url = f"{self.test_base._base_url}/rpc/geodb_user_allowed"
        m.post(url, text="0")  # user is NOT owner of database

        with self.assertRaises(GeoDBError):
            self.test_base._api.publish_collection_to_group(
                "collection", "group", "database"
            )

    def test_unpublish_collection_from_group(self, m):
        self.test_base.set_global_mocks(m)
        url = f"{self.test_base._base_url}/rpc/geodb_group_unpublish_collection"
        m.post(url, text="")
        url = f"{self.test_base._base_url}/rpc/geodb_user_allowed"
        m.post(url, text="1")  # user is owner of database

        collection = "test_col"
        database = "test_db"
        group = "test_group"

        r = self.test_base._api.unpublish_collection_from_group(
            collection, group, database
        )

        expected = {
            "Message": f"Unpublished collection {collection} in "
            f"database {database} from group {group}."
        }
        self._check_message(r, expected)

    def test_get_my_groups(self, m):
        self.test_base.set_global_mocks(m)
        url = f"{self.test_base._base_url}/rpc/geodb_get_user_roles"
        m.post(
            url,
            json=[
                {
                    "src": [
                        {"rolname": self.test_base._api.whoami},
                        {"rolname": "authenticator"},
                    ]
                }
            ],
        )

        roles = self.test_base._api.get_my_groups()
        self.assertListEqual(["authenticator"], roles)

    def test_get_access_rights(self, m):
        self.test_base.set_global_mocks(m)
        url = f"{self.test_base._base_url}/rpc/geodb_get_grants"

        grantee = self.test_base._api.whoami

        m.post(
            url,
            json=[
                {
                    "res": [
                        {"grantee": grantee, "privilege_type": "INSERT"},
                        {"grantee": grantee, "privilege_type": "SELECT"},
                        {"grantee": grantee, "privilege_type": "UPDATE"},
                        {"grantee": grantee, "privilege_type": "DELETE"},
                    ]
                }
            ],
        )

        rights = self.test_base._api.get_access_rights("test_col_test_db")
        self.assertDictEqual(
            {self.test_base._api.whoami: ["INSERT", "SELECT", "UPDATE", "DELETE"]},
            rights,
        )

    def _check_message(self, message, expected):
        self.assertIsInstance(message, Message)
        self.assertDictEqual(expected, message.to_dict())
