#!/usr/bin/env python
# coding: utf-8

"""
Selecting from a Collection by bbox "get_collection_by_bbox"
=============================================================
"""

from xcube_geodb.core.geodb import GeoDBClient

geodb = GeoDBClient()

gdf = geodb.get_collection_by_bbox(collection="land_use", bbox=(452750.0, 464000.0, 88909.549, 102486.299),
                                   comparison_mode="contains", bbox_crs=3794, limit=200, offset=10)
print(gdf)

gdf.plot(column="raba_pid", figsize=(15, 15))

