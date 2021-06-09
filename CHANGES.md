## v1.0.0 (in development)

### Fixes

### New Features
- EPSG code strings e.g. 'epsg:4326' or 'EPSG:4326' are now accepted as CRS definition by `geodb.create_collection*` 
  functions as well as `geodb.insert_into_collection`, `geodb.transform_bbox_crs`, `geodb.get_collection_by_bbox`. 
  Before only integers were accepted. This ensures alignment with xcube software. 

## v0.1.16

### Fixes
- Corrected transform_bbox_crs method. Uses now minx, miny, maxx, maxy notation for the bbox

### New Features

- Added a parameter to transform_bbox_crs bbox allowing to use Lon Lat notation for EPSG 4326 when
  transforming the bbox's CRS to EPSG 4326.


## v0.1.15

### Fixes

- Fixed default value auth_mode from `client_credentials` to `client-credentials`

## v0.1.14

### Fixes

- Fixed issue that get_my_collections() would not use the parameter `database` correctly
- Fixed that collection_exists fails on newly created collections due to postgres/postgrest caching delays

## v0.1.13

- In this version the default auth mode is `client-credentials` instead of `password`

## v0.1.12

### New Features

- Made the number of rows during inserting data into a collection configurable
- Added a version method to the client
- The postgis extension is now created when executing geodb--*.sql
- Added the jupyter lab GitHub extension to the docker image 
- Added a method list_users, register_users
- Added a deprecation warning if '3.120.53.215.nip.io' is used as 
  server address.
- version is now a static property of the geoDB client
- `get_collection_from_bbox` will now attempt to transform the bbix crs if teh crses differ between the 
  bbox and the collection
- Added the method `transform_bbox_crs` to manage the crs of bboxes
- Added a cascade option to `GeoDBClient.drop_collections`

### Fixes

- Fixed revoke access SEL function [fixing](https://gitext.sinergise.com/dcfs/common/-/issues/221)
- Fixed odd function options for copy collection
- The geodb setup procedure can now accept a connection or database connection parameter
- The geodb SQL setup does not fail anymore when items exist already
- Renamed method post to _post to be in line with all other rest methods
- Ensured that renaming collections throws an error when the rename-to-database does not exist or the user does not have access to teh new database
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
- create_collection is now creating a database when the database parameter is given and the database not exists
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
- When inserting new data, rows are streamed to avoid issues with larger datasets 
- Fixed error message when attempting to publish a collection


## v0.1.6

- When publishing collections geoDB shifts the data now to the database 'public'
- Functions were added to list collections a user has access to
- The Dockerfile uses now the xcube base image

## v0.1.5

- Updated the exploring Jupyter Notebook and
- Assured that somone can use the client from a custom environment (i.e. outside EDC)
- Bug fixes

## v0.1.4

- Bug fixes

## v0.1.3

- Bug fixes

## v0.1.2

- Added a where option to get_by_bbox to allow further querying
- Added an op option to get_by_bbox to allow choseing bewteen and and or when using the where
  option

## v0.1.1

- Fixed SQL errors when the user name contains hyphens

## v0.1.0

This version comprises:

- The Core Python GeoDBClient
- Sphinx docs
- Example notebooks
- A conda build recipe
- A Dockerfile
