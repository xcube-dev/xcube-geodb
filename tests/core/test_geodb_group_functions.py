import unittest

import requests_mock

from tests.core.test_geodb import GeoDBClientTest
from xcube_geodb.core.geodb import GeoDBError


@requests_mock.mock(real_http=False)
class GeoDBClientGroupsTest(unittest.TestCase):

    @classmethod
    def setUp(cls) -> None:
        cls.base_test = GeoDBClientTest()
        cls.base_test.setUp()

    def tearDown(self) -> None:
        self.base_test.tearDown()

    def test_add_user_to_group(self, m):
        self.base_test.set_global_mocks(m)
        url = f'{self.base_test._base_url}/rpc/geodb_group_grant'
        username = 'test_user'
        usergroup = 'test_group'
        m.post(url, text='')

        r = self.base_test._api.add_user_to_group(username, usergroup)

        expected = {'Message': f'Added user {username} to {usergroup}'}
        self.base_test.check_message(r, expected)

    def test_remove_user_from_group(self, m):
        self.base_test.set_global_mocks(m)
        url = f'{self.base_test._base_url}/rpc/geodb_group_revoke'
        m.post(url, text='')

        username = 'test_user'
        usergroup = 'test_group'

        r = self.base_test._api.remove_user_from_group(username, usergroup)

        expected = {'Message': f'Removed user {username} from {usergroup}'}
        self.base_test.check_message(r, expected)

    def test_publish_collection_to_group(self, m):
        self.base_test.set_global_mocks(m)
        url = f'{self.base_test._base_url}/rpc/geodb_group_publish_collection'
        m.post(url, text='')
        url = f'{self.base_test._base_url}/rpc/geodb_user_allowed'
        m.post(url, text='1')  # user is owner of database

        collection = 'test_col'
        database = 'test_db'
        group = 'test_group'

        r = self.base_test._api.publish_collection_to_group(collection, group, database)

        expected = {'Message': f'Published collection {collection} in database {database} to group {group}.'}
        self.base_test.check_message(r, expected)

    def test_publish_collection_to_group_fails(self, m):
        self.base_test.set_global_mocks(m)
        url = f'{self.base_test._base_url}/rpc/geodb_group_publish_collection'
        m.post(url, text='')
        url = f'{self.base_test._base_url}/rpc/geodb_user_allowed'
        m.post(url, text='0')  # user is NOT owner of database

        with self.assertRaises(GeoDBError):
            self.base_test._api.publish_collection_to_group('collection', 'group', 'database')

    def test_unpublish_collection_from_group(self, m):
        self.base_test.set_global_mocks(m)
        url = f'{self.base_test._base_url}/rpc/geodb_group_unpublish_collection'
        m.post(url, text='')
        url = f'{self.base_test._base_url}/rpc/geodb_user_allowed'
        m.post(url, text='1')  # user is owner of database

        collection = 'test_col'
        database = 'test_db'
        group = 'test_group'

        r = self.base_test._api.unpublish_collection_from_group(collection, group, database)

        expected = {'Message': f'Unpublished collection {collection} in database {database} from group {group}.'}
        self.base_test.check_message(r, expected)

    def test_get_user_roles(self, m):
        self.base_test.set_global_mocks(m)
        url = f'{self.base_test._base_url}/rpc/geodb_get_user_roles'
        m.post(url, json=[{'src': [{'rolname': self.base_test._api.whoami}, {'rolname': 'authenticator'}]}])

        roles = self.base_test._api.get_roles()
        self.assertListEqual(['authenticator', self.base_test._api.whoami], roles)

    def test_get_access_rights(self, m):
        self.base_test.set_global_mocks(m)
        url = f'{self.base_test._base_url}/rpc/geodb_get_grants'

        grantee = self.base_test._api.whoami
        m.post(url, json=[([{'grantee': grantee, 'privilege_type': 'INSERT'},
                            {'grantee': grantee, 'privilege_type': 'SELECT'},
                            {'grantee': grantee, 'privilege_type': 'UPDATE'},
                            {'grantee': grantee, 'privilege_type': 'DELETE'}
                            ],)])

        rights = self.base_test._api.get_access_rights('test_col_test_db')
        self.assertDictEqual({self.base_test._api.whoami: ['INSERT', 'SELECT', 'UPDATE', 'DELETE']}, rights)
