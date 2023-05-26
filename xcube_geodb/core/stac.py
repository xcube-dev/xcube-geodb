# The MIT License (MIT)
# Copyright (c) 2021/2022 by the xcube team and contributors
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
from typing import List, Iterable

import geojson
from geopandas import GeoDataFrame
from pystac import Catalog
from pystac.extensions.label import LabelExtension
from pystac.extensions.projection import ProjectionExtension


class Collection:
    """
    Internal class representing a collection.
    """

    def __init__(self, name: str, properties: dict, data: GeoDataFrame):
        self.name = name
        self.properties = properties
        self.data = data


class StacTransformer:
    """
    This class aims to simplify exchange between STAC catalogs and the
    geoDB, offering functions for transformation between STAC catalogs and
    geoDB-ingestible formats.
    """

    def __init__(self):
        self.data = {}
        self.keys = []

    def transform_stac_catalog(self, catalog_path: str) -> List[Collection]:
        """
        Transforms a STAC catalog into the internal format
        xcube_geodb.core.stac.Collection, which can easily be ingested into
        the geoDB.

        Args:
            catalog_path (str): The URL of the PostGrest Rest API service

        Returns:
            A list of xcube_geodb.core.stac.Collection

        """
        root_catalog = Catalog.from_file(catalog_path)
        collections = list(root_catalog.get_collections())
        result = []
        for collection in collections:
            transformed_collection = self._transform_collection(collection)
            result.append(transformed_collection)
        return result

    def _transform_collection(self, collection) \
            -> Collection:
        self.data = {'geometry': []}
        self.keys.clear()
        items = list(collection.get_all_items())
        for item in items:
            self.accumulate_keys(item)
        crs = None
        for item in items:
            label = LabelExtension.ext(item, True)
            for key in self.keys:
                self.fetch_data(label, item, key)
            geom = geojson.loads(str(item.geometry).replace('\'', '"'))
            self.data['geometry'].append(geom)
            extension = ProjectionExtension.ext(item, True)
            if crs and not crs == extension.crs_string :
                raise ValueError('Items with different CRS within a '
                                 'collection not supported.')
            crs = extension.crs_string
        if 'label:classes' in self.data:
            del self.data['label:classes']
        if not crs:
            raise ValueError('Collections without CRS not supported.')
        gdf = GeoDataFrame(self.data, crs=crs)
        properties = self._translate_properties(gdf)
        return Collection(collection.id, properties, gdf)

    def fetch_data(self, label, item, key):
        if key not in self.data:
            self.data[key] = []
        if key == 'geometry':
            return
        index = self._get_label_class_index(key, label.label_classes)
        if label.label_classes and index > -1:
            self.data[key].append(
                label.label_classes[index].properties['classes'])
        elif label.label_overviews and key.startswith('label:counts_'):
            idxs = self._get_label_count_and_overview_index(
                key, label.label_overviews)
            if idxs == (-1, -1):
                self.data[key].append(-1)
            else:
                self.data[key].append(
                    label.label_overviews[idxs[0]].counts[idxs[1]].count)
        elif label.label_overviews and key.startswith(
                'label:statistics_'):
            idxs = self._get_label_stats_and_overview_index(
                key, label.label_overviews)
            if idxs == (-1, -1):
                self.data[key].append(-1)
            else:
                self.data[key].append(
                    label.label_overviews[idxs[0]].counts[idxs[1]].value)
        else:
            self.fetch_regular_data(key, item)

    def fetch_regular_data(self, key, item):
        if key not in item.properties:
            self.data[key].append('')
        elif type(item.properties[key]) == list and \
                len(item.properties[key]) == 1:
            self.data[key].append(item.properties[key])
        else:
            self.data[key].append(str(item.properties[key]))

    def accumulate_keys(self, item):
        for key in item.properties:
            if key.lower() not in self.keys:
                self.keys.append(key.lower())
            elif key.startswith('label:'):
                label = LabelExtension.ext(item)
                self.accumulate_label_keys(label, key)

    def accumulate_label_keys(self, label, key):
        if key == 'label:properties' and key not in self.keys:
            self.keys.append(key.lower())
        elif key == 'label:classes':
            for label_class in label.label_classes:
                if f'label:classes_{label_class.name}' not in self.keys:
                    self.keys.append(
                        f'label:classes_{label_class.name.lower()}')
        elif key == 'label:overviews':
            self.accumulate_label_ov_keys(label)

    def accumulate_label_ov_keys(self, label):
        for label_overview in label.label_overviews:
            for lc in label_overview.counts or []:
                counts_key = f'label:counts_' \
                             f'{label_overview.property_key.lower()}_' \
                             f'{lc.name.lower()}'
                if counts_key not in self.keys:
                    self.keys.append(counts_key)
            for ls in label_overview.statistics or []:
                stats_key = f'label:statistics_' \
                            f'{label_overview.property_key.lower()}_' \
                            f'{ls.name.lower()}'
                if stats_key not in self.keys:
                    self.keys.append(stats_key)

    def _translate_properties(self, gdf):
        properties = {}
        for k in gdf.dtypes.keys():
            if self._translate(gdf.dtypes[k]):
                properties[k] = self._translate(gdf.dtypes[k])
        return properties

    @staticmethod
    def _translate(o):
        if o == 'object':
            return 'varchar'
        elif o == 'int64' or o == 'int32':
            return 'integer'
        elif o == 'float64' or o == 'float32':
            return 'float'
        return None

    @staticmethod
    def _get_label_class_index(key, label_classes):
        for i, lc in enumerate(label_classes):
            if 'label:classes_' + lc.name == key:
                return i
        return -1

    @staticmethod
    def _get_label_count_and_overview_index(key, label_overviews):
        for i, lo in enumerate(label_overviews):
            for j, lc in enumerate(lo.counts):
                if key == 'label:counts_' + lo.property_key.lower() + '_' + lc.name.lower():
                    return i, j
        return -1, -1

    @staticmethod
    def _get_label_stats_and_overview_index(key, label_overviews):
        for i, lo in enumerate(label_overviews):
            for j, ls in enumerate(lo.statistics):
                if key == 'label:statistics_' + lo.property_key.lower() + '_' + ls.name.lower():
                    return i, j
        return -1, -1
