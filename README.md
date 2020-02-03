# xcube geodb

xcube geodb is a geospatial database plugin for [xcube](https://github.com/dcs4cop/xcube).
geodb takes features from e.g. shapefiles in teh form of a [GeoDataFrame](http://geopandas.org/) and processes
them in a custom PostgreSQL database provided by [Brockmann Consult](https://www.brockmann-consult.de) and the ECD consortium.
 The processed data can be accessed and queried via a Rest API with full querying capabilities.   


Please refer to our [documentation](https://xcube-geodb.readthedocs.io) for further information.

## Technologies used:

- [xcube](https://github.com/dcs4cop/xcube)
- [xarray](http://xarray.pydata.org/en/stable/)
- [ProgreSQL](https://www.postgresql.org/)
- [Postgrest](http://postgrest.org/en/v6.0/)
- [AWS RDS](https://aws.amazon.com/de/rds/)
- [docker](https://www.docker.com/)
