#!/usr/bin/env python
# coding: utf-8

"""
"This" is my example-script
===========================

This example doesn't do much, it just makes a simple plot
"""


from xcube_geodb.core.geodb import GeoDBClient

geodb = GeoDBClient()
print(geodb.whoami)

gdf = geodb.get_collection('land_use')
print(gdf)

gdf.plot(column="raba_pid", figsize=(15,15))


