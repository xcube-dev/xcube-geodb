[![Documentation Status](https://readthedocs.org/projects/xcube-geodb/badge/?version=latest)](https://xcube-geodb.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/xcube-dev/xcube-geodb/branch/main/graph/badge.svg)](https://codecov.io/gh/dcs4cop/xcube-geodb)

# xcube geoDB

xcube geoDB is a geo-spatial database within the [xcube](https://github.com/dcs4cop/xcube) ecosystem.
xcube geoDB reads features from e.g. Shapefiles in the form of a [GeoDataFrame](http://geopandas.org/) 
and processes them in a custom PostgreSQL database
provided by [Brockmann Consult](https://www.brockmann-consult.de) and the
Euro Data Cube (EDC) consortium. The processed data can be accessed and
queried via a REST API with full querying capabilities.

Please refer to our [documentation](https://xcube-geodb.readthedocs.io) for
further information.

## Technologies used:

- [xcube](https://github.com/dcs4cop/xcube)
- [xarray](http://xarray.pydata.org/en/stable/)
- [PostgreSQL](https://www.postgresql.org/)
- [Postgrest](http://postgrest.org/en/v6.0/)
- [AWS RDS](https://aws.amazon.com/de/rds/)
- [docker](https://www.docker.com/)

## Deployment Process

1. Local Unittest
2. Create stage/dev release (tag only. Tag name must include the expression '
   dev' plus a number, e.g. 0.1.9.dev1)
3. The stage release will be deployed using GitHub Actions:
   1. A docker image will be created and pushed to quay.io
   2. The new version of the image will be deployed to the cluster using a 
      [helm chart](https://github.com/dcs4cop/xcube-k8s/geodb)
4. The stage release will be tested:
   1. An automatic test is run similar to a unit test
   2. Users will test the new features and fixes
5. Create release
6. The release will be deployed using GitHub Actions:
   1. unittest
   2. the new version of the xcube geodb image will be created and pushed to 
      quay.io
   3. the xcube-geodb helm chart will be deployed using the new version of 
      the docker image 
 
