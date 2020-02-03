import unittest

from xcube_geodb.core.collection import Collection


class CollectionTest(unittest.TestCase):
    def test_config(self):
        props = {'prop1': 'integer', 'prop2': 'integer', }
        expected = {'dudes_collection': props}

        coll = Collection('dudes_collection', props)

        self.assertDictEqual(expected, coll.config)

    def test_add(self):
        props_expected = {'prop1': 'integer', 'prop2': 'integer', 'prop3': 'date'}
        props = {'prop1': 'integer', 'prop2': 'integer'}

        expected = {'dudes_collection': props_expected}

        coll = Collection('dudes_collection', props)
        coll.add_props({'prop3': 'date'})

        self.assertDictEqual(expected, coll.config)

    def test_delete(self):
        props_expected = {'prop1': 'integer', 'prop2': 'integer'}
        props = {'prop1': 'integer', 'prop2': 'integer', 'prop3': 'date'}

        expected = {'dudes_collection': props_expected}

        coll = Collection('dudes_collection', props)

        coll.delete_props(['prop3'])

        self.assertDictEqual(expected, coll.config)


if __name__ == '__main__':
    unittest.main()
