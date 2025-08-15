## v1.0.11 (in development)

### New Features

### Fixes

- removed and updated some values in `defaults.py` [#127]

## v1.0.10

### New Features

- the geoDB client correctly refreshes the capabilities of the PostGREST server
- geoDB now logs collection read events, if deliberately triggered by the user
- it is now possible to create users that only have permission to read [#117]

### Fixes

- database truncation now works as name suggests [#009]
- cleaned up documentation [#110]
- fixed caching of capabilities [#108]

## v1.0.9

### New Features

- formatted codebase [#100]
- geoDB now allows to share a database with an entire group. [#98]
- geodb_admin is allowed to create user groups on registration. [#96]

### Fixes

- fixed wrong message on unpublishing collections [#89]
- updated default value for `auth_aud` [#80]
- fixed wrong usage of `auth_domain` [#103]

## v1.0.8

### New Features

### Fixes

- Fixed issue where the client threw errors if Winchester was used with Geoserver. [#92]

## v1.0.7

### New Features

- added function that allows users to retrieve the geometry types from their
  collections
- made client support PostGIS > 3.0.0
- made client compatible with Winchester authentication module

### Fixes

- Fixed issue where geoDB was not installable using conda. [#87]

## v1.0.6

### New Features

- added functions that allow to create/show/remove indexes on collections
- added functions that allow users to work in user groups
    - those new functions are: `create_group`, `add_user_to_group`,
      `remove_user_from_group`, `publish_collection_to_group`,
      `unpublish_collection_from_group`, `get_my_groups`, `get_group_users`, and
      `get_access_rights`.
    - see the respective
      [notebook](https://xcube-geodb.readthedocs.io/en/latest/notebooks/geodb_group_management.html)
      for usage examples

### Fixes

- removed not working parameter from internal function `_get`
- fixed function `count_collection_rows`

## v1.0.5

### New Features

- allowed to use `get_collection_bbox` in an alternative, faster way
- added function `count_collection_rows` that allows to count or (faster) to
  estimate the count of collection rows

### Fixes

- removed `jupyterlab` from dependencies, as it is not being used.
- fixed rare exception in `insert_into_collection`
- updated tests to run on with latest psycopg2 versions
- fixed: function `get_collection_bbox` raises exception for empty collections
- fixed broken function `get_geodb_sql_version`

## v1.0.4

### New Features

- Added function `get_geodb_sql_version` that shows the version of the deployed
  SQL, which can differ from the Python client version.
- Added logging: the database now maintains a table that reflects
  changes to the collections. The logs can be read using the new function
  `get_event_log`.

### Fixes

## v1.0.3

### New Features

- Added a new auth mode `none`. The aim of this mode is enabling users
  installing xcube geoDB infrastructure without oauth2 for e.g. testing
  purposes.
- A new xcube geoDB docker backend image has been added. This image can be used
  to set
  up a postgis database with a pre-installed xcube geoDB database extension
- Added a new `count_by_bbox` filter function
- Allowed to provide limit and offset in function `get_collection`
- Added the new function `get_collection_bbox`, that returns the bounding box
  of all geometries within a collection.

### Fixes

- Added Python pip requirements to have control over installed sphinx version on
  readthedocs
- Improved heading in notebooks
- Removed auto cp of notebooks in Makefile.
- Ensured that xcube geoDB client works with Keycloak
- Reduced substantially the number of times the token is received from auth
  service
- Fixed bug that caused unpublishing of collections with upper case letters
  in name to fail

## v1.0.2

### Fixes

- Fixed the behaviour of `create_collection` when `clear` is set to `True` and
  the collection to be cleared does not exist.
  In that situation, xcube geoDB was throwing the wrong error and would not
  proceed to creating the collection.

## v1.0.1

### Fixes

- The default value for `raise_it` is now set to true. Better value for using
  the client as the original behaviour was
  raising the error

## v1.0.0

### Fixes

- Fixed version naming issue in the extension's Makefile
- Fixed create database. Does not allow users anymore to write collections
  to other users' databases.
  Closes [#164](https://github.com/bcdev/service-eurodatacube/issues/164),
  [#165](https://github.com/bcdev/service-eurodatacube/issues/165),
  [#166](https://github.com/bcdev/service-eurodatacube/issues/166)
- Fixed that admin users have access to users' collections
- Collections are now always reported as 'collection' and not 'table_name'

### New Features

- EPSG code strings e.g. 'epsg:4326' or 'EPSG:4326' are now accepted as CRS
  definition by `geodb.create_collection*` functions as well as
  `geodb.insert_into_collection`, `geodb.transform_bbox_crs`,
  `geodb.get_collection_by_bbox`.
  Before only integers were accepted. This ensures alignment with xcube
  software.
- The geoDB client has now two new methods (`publish_gs`, `unpublish_gs`) that
  allow publishing collections to a WMS service (geoserver)
- The geoDB client has now a new method (`get_published_gs`) that allows to list
  published collections to a WMS service (geoserver)
- Added a notebook that describes how to use the WMS self service
- Ensured that the rest URL of the geoserver for publishing to WMS is
  configurable
- Messages are handled by class Message allowing to nicer print them in Jupyter
  notebooks. The user can also choose whether an error will be raised or
  just printed.
- Added method `geodb.get_all_published_gs` to get all gs published collections

## v0.1.16

### Fixes

- Corrected transform_bbox_crs method. Uses now minx, miny, maxx, maxy notation
  for the bbox

### New Features

- Added a parameter to transform_bbox_crs bbox allowing to use Lon Lat notation
  for EPSG 4326 when transforming the bbox's CRS to EPSG 4326.

## v0.1.15

### Fixes

- Fixed default value auth_mode from `client_credentials`
  to `client-credentials`

## v0.1.14

### Fixes

- Fixed issue that get_my_collections() would not use the parameter `database`
  correctly
- Fixed that collection_exists fails on newly created collections due to
  postgres/postgrest caching delays

## v0.1.13

- In this version the default auth mode is `client-credentials` instead
  of `password`

## v0.1.12

### New Features

- Made the number of rows during inserting data into a collection configurable
- Added a version method to the client
- The postgis extension is now created when executing geodb--*.sql
- Added the jupyter lab GitHub extension to the docker image
- Added a method list_users, register_users
- Added a deprecation warning if '3.120.53.215.nip.io' is used as
  server address.
- version is now a static property of xcube geoDB client
- `get_collection_from_bbox` will now attempt to transform the bbox crs if teh
  crses differ between the
  bbox and the collection
- Added the method `transform_bbox_crs` to manage the crs of bboxes
- Added a cascade option to `GeoDBClient.drop_collections`

### Fixes

- Fixed revoke access SEL
  function [fixing](https://gitext.sinergise.com/dcfs/common/-/issues/221)
- Fixed odd function options for copy collection
- The xcube geoDB setup procedure can now accept a connection or database connection
  parameter
- The xcube geoDB SQL setup does not fail anymore when items exist already
- Renamed method post to _post to be in line with all other rest methods
- Ensured that renaming collections throws an error when the rename-to-database
  does not exist or the user does not have access to teh new database
- Get collection info has now a `database` option
- `GeoDBClient.get_my_collections `

## v0.1.11

### New Features

- Added a copy/move/rename collection operations
- Added a new authentication auth flow (password)
- Added a method create_collection_if_not_exists

### Fixes

- Deactivated interactive authentication mode.

## v0.1.10

### Fixes:

- insert_into_collection does now honour the crs parameter correctly

## v0.1.9

### Fixes:

- fixed publish collection message
- list_grants has now a database column
- `geodb.get_my_collections(database="some_db")` does now work
- create_collection is now creating a database when the database parameter is
  given and the database not exists
- get_collection functions is now retaining the crs/srid
- a new operation get_collection_srid was added
- corrected message when access to a collection is granted
- publish and unpublish collection has now a database parameter
- fixed grant access function raising a sequence not found error
- grant access now uses the correct database when specified in the parameters
- Added decorators to allow deprecation notices on functions and parameters

## v0.1.8

- Added support for creating and deleting databases
- Listing tables now omits prefixes
- A new head_collection function has been introduced
- Removed the function get_collections. Use get_my_collections instead
- When inserting new data, rows are streamed to avoid issues with larger
  datasets
- Fixed error message when attempting to publish a collection

## v0.1.6

- When publishing collections geoDB shifts the data now to the database 'public'
- Functions were added to list collections a user has access to
- The Dockerfile uses now the xcube base image

## v0.1.5

- Updated the exploring Jupyter Notebook and
- Assured that someone can use the client from a custom environment (i.e.
  outside EDC)
- Bug fixes

## v0.1.4

- Bug fixes

## v0.1.3

- Bug fixes

## v0.1.2

- Added a where option to get_by_bbox to allow further querying
- Added an op option to get_by_bbox to allow choosing between and or when
  using the where option

## v0.1.1

- Fixed SQL errors when the username contains hyphens

## v0.1.0

This version comprises:

- The Core Python GeoDBClient
- Sphinx docs
- Example notebooks
- A conda build recipe
- A Dockerfile
