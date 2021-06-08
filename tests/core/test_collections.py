import unittest

from xcube_geodb.core.collections import Collections


class TestCollections(unittest.TestCase):
    def test_collections(self):
        coll = Collections({'name': 'test'})
        self.assertDictEqual({'name': 'test'}, coll.config)

        coll = Collections({'name': 'test'})
        coll.config = {'name': 'test2'}
        self.assertDictEqual({'name': 'test2'}, coll.config)


if __name__ == '__main__':
    unittest.main()
