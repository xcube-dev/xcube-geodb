# Testplan for the geoDBclient and webAPI

## Manage collections

__Operations__:

- Create a collection
- Insert into a collection
- Update a collection
- Delete from a collection
- Drop a collection
- Add properties to a collection
- Remove properties from a collection

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
