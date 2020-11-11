# Testplan for the geoDBclient

## Authentication

__Operations__:

- Get access token from auth provider (i.e. auth0)

__Test__:

- Configure .env using client credentials only and auth method client-credentials
- Configure .env using client credentials as well as user name and password and auth method password
- Configure .env using method interactive

## Manage collections

__Operations__:

1. Create a collection
2. Insert into a collection
3. Update a collection
4. Delete from a collection
5. Drop a collection
6. Add properties to a collection
7. Remove properties from a collection

__Test__:

The aim of this plan is to test the manage collectinos functionality (i.e. CRUD collection operations)

- Install the geodbClient
- Configure your credentials in .env
- Start a jupyterlab and use the notebook notebooks/geodb_manage_collections.ipynb
- Execute the notebook and play

## Share collections

The aim of this plan is to test the share collections functionality

__Operations__:

- Share a collection with another user
- Revoke access from a collection 
- Publish a collection (short hand for sharing a collectoin to the user PUBLIC)
- Unpublish a collection (short hand for revoking access from collection from the user PUBLIC)

__Test__:

- Install the geodbClient
- Configure your credentials in .env
- Start a jupyterlab and use the notebook notebooks/geodb_manage_collections.ipynb
- Execute the notebook and play

## Explore collections

The aim of this plan is to test querying collections.

__Operations__:

- Query my usage
- Return the collections I have access to
- Get a whole collection and plot it
- Get a subset of a collection using postgrest syntax
- Query a collection by bbox
- Query a collection using postgres syntax

__Plan__:

- Install the geodbClient
- Configure your credentials in .env
- Start a jupyterlab and use the notebook notebooks/geodb_explore_collections.ipynb
- Execute the notebook and play
