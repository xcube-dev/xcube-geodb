#!/usr/bin/env python
# coding: utf-8

"""
Selecting from a Collection using Postgrest Syntax "get_collection"
===================================================================


"""

from xcube_geodb.core.geodb import GeoDBClient

geodb = GeoDBClient()

gdf = geodb.get_collection('land_use', query='raba_id=eq.1410')
print(gdf)

gdf.plot(column="raba_pid", figsize=(15, 15))

