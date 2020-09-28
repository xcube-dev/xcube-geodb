## v0.1.9

### Fixes:

- get_collection functions is now retaining teh crs/srid
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
