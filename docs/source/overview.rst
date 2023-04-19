========
Overview
========

xcube geoDB is a geo-spatial database within the [xcube](https://github.com/dcs4cop/xcube) ecosystem.
xcube geoDB reads features from e.g. Shapefiles in the form of a [GeoDataFrame](http://geopandas.org/) and processes
them in a custom PostgreSQL database provided by [Brockmann Consult GmbH](https://www.brockmann-consult.de) and the Euro
Data Cube (EDC) consortium.
The processed data can be accessed and queried via a REST API with full querying capabilities.

================
Technologies used:
================

- [PostgreSQL](https://www.postgresql.org/)
- [Postgrest](http://postgrest.org/en/v6.0/)
- [AWS RDS](https://aws.amazon.com/de/rds/)
- [docker](https://www.docker.com/)