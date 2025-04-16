[![Documentation Status](https://github.com/xcube-dev/xcube-geodb/actions/workflows/pages/pages-build-deployment/badge.svg)](https://github.com/xcube-dev/xcube-geodb/actions/workflows/pages/pages-build-deployment)
[![Unittest xcube-geodb](https://github.com/xcube-dev/xcube-geodb/actions/workflows/workflow.yml/badge.svg)](https://github.com/xcube-dev/xcube-geodb/actions/workflows/workflow.yml)
[![codecov](https://codecov.io/gh/xcube-dev/xcube-geodb/graph/badge.svg?token=67zPacCxuz)](https://codecov.io/gh/xcube-dev/xcube-geodb)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v1.json)](https://github.com/astral-sh/ruff)
![GitHub Release](https://img.shields.io/github/v/release/xcube-dev/xcube-geodb)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/xcube_geodb/badges/version.svg)](https://anaconda.org/conda-forge/xcube_geodb)
![Conda Downloads](https://img.shields.io/conda/d/conda-forge/xcube_geodb)
![GitHub License](https://img.shields.io/github/license/xcube-dev/xcube-geodb)

# xcube geoDB

xcube geoDB is a geo-spatial database within the [xcube](https://github.com/dcs4cop/xcube) ecosystem.
xcube geoDB reads features from e.g. Shapefiles in the form of a [GeoDataFrame](http://geopandas.org/)
and processes them in a custom PostgreSQL database
provided by [Brockmann Consult](https://www.brockmann-consult.de) and the
Euro Data Cube (EDC) consortium. The processed data can be accessed and
queried via a REST API with full querying capabilities.

Please refer to our [documentation](https://xcube-dev.github.io/xcube-geodb) for
further information.

## Technologies used:

- [xcube](https://github.com/dcs4cop/xcube)
- [xarray](http://xarray.pydata.org/en/stable/)
- [PostgreSQL](https://www.postgresql.org/)
- [PostgREST](http://postgrest.org/en/v6.0/)
- [AWS RDS](https://aws.amazon.com/de/rds/)
- [docker](https://www.docker.com/)
