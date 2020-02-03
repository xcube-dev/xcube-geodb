[![Documentation Status](https://readthedocs.org/projects/xcube-geodb/badge/?version=latest)](https://xcube-geodb.readthedocs.io/en/latest/?badge=latest)
[![Build Status](https://travis-ci.com/dcs4cop/xcube-geodb.svg?branch=master)](https://travis-ci.com/dcs4cop/xcube-geodb)

# xcube geodb

The xcube GeoDB is a geo-spatial database plugin for [xcube](https://github.com/dcs4cop/xcube).
geodb reads features from e.g. Shapefiles in the form of a [GeoDataFrame](http://geopandas.org/) and processes
them in a custom PostgreSQL database provided by [Brockmann Consult](https://www.brockmann-consult.de) and the Euro Data Cube (EDC) consortium.
 The processed data can be accessed and queried via a REST API with full querying capabilities.   


Please refer to our [documentation](https://xcube-geodb.readthedocs.io) for further information.

## Technologies used:

- [xcube](https://github.com/dcs4cop/xcube)
- [xarray](http://xarray.pydata.org/en/stable/)
- [ProgreSQL](https://www.postgresql.org/)
- [Postgrest](http://postgrest.org/en/v6.0/)
- [AWS RDS](https://aws.amazon.com/de/rds/)
- [docker](https://www.docker.com/)
