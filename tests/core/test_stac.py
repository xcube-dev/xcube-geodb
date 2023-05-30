import unittest

from xcube_geodb.core.stac import StacTransformer
import importlib.resources as resources


class StacTest(unittest.TestCase):

    def test_transform_stac_catalog(self):
        si = StacTransformer()
        catalog_resource = resources \
            .files('tests.core.res.stac') \
            .joinpath('catalog.json')
        with resources.as_file(catalog_resource) as catalog_file:
            collections = si.transform_stac_catalog(
                str(catalog_file.absolute()))
        self.assertEqual(collections[0].name, 'test_collection')
        self.assertEqual(22, len(collections[0].properties))

        self.assertTrue('label:description' in collections[0].properties)
        self.assertTrue('label:counts_building:material_cement_block' in
                        collections[0].properties)

        self.assertEqual(
            collections[0].properties['label:description'],
            'varchar')
        self.assertEqual(
            collections[0].properties[
                'label:counts_building:material_cement_block'
            ], 'integer')

        self.assertEqual((2, 23), collections[0].data.shape)
        self.assertTrue('label:counts_building:material_cement_block' in
                        collections[0].data.columns)
        self.assertEqual(51, collections[0].data.
                         get('label:counts_amenity_place_of_worship')[0])
        self.assertEqual('EPSG:32631', collections[0].data.crs)

    def test_transformation_fails(self):
        si = StacTransformer()
        catalog_resource = resources \
            .files('tests.core.res.stac') \
            .joinpath('catalog_2.json')
        with resources.as_file(catalog_resource) as catalog_file:
            with self.assertRaises(ValueError) as e:
                si.transform_stac_catalog(str(catalog_file.absolute()))
        self.assertEqual('Items with different CRS within a '
                         'collection not supported.', str(e.exception))

    def test_transformation_fails_no_crs(self):
        si = StacTransformer()
        catalog_resource = resources \
            .files('tests.core.res.stac') \
            .joinpath('catalog_3.json')
        with resources.as_file(catalog_resource) as catalog_file:
            with self.assertRaises(ValueError) as e:
                si.transform_stac_catalog(str(catalog_file.absolute()))
        self.assertEqual('Collections without CRS not supported.',
                         str(e.exception))
