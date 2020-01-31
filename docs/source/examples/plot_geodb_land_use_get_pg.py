#!/usr/bin/env python
# coding: utf-8

"""
Selecting from a Collection using PostgreSQl Syntax "get_collection_pg"
=======================================================================


"""

from xcube_geodb.core.geodb import GeoDBClient

geodb = GeoDBClient()

gdf = geodb.get_collection_pg(collection='land_use', where='raba_id=1410')

print(gdf.head())

gdf.plot(column="raba_pid")

