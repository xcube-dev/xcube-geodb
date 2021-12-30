# Installation

In this chapter we will describe how to install the geoDB infrastructure. The infrastructure consists of four main
components:

1. A [Python client/API](https://github.com/dcs4cop/xcube-geodb) accessing the database through the PostGrest RestAPI (this version)
2. A [PosGIS database](https://postgis.net/) (Version 14)
3. A geoDB extension to be installed into the PostGIS database (this version)
4. A [PostGrest RestAPI](https://postgrest.org/en/stable/) (Version 7)

## 1. Installing the geoDB Client/API

The aim of the chapter is to describe the installation of the geoDB client which serves as a wrapper to accessing an
existing geoDB service. You can omit steps 2.-4. entirely if you have gained access to such a service 
(e.g. by buying geoDB access through the [eurodatacube](https://eurodatacube.com/) service).

### Installation Using the Package Manager conda/mamba

The xcube geodb client is preferably installed into a conda environment. Ensure that you use Python 3 (>=3.6). 
The geoDB client has not been tested with Python 2 and will very likely not work. 

You install the client using conda 

```bash
$ conda install -c conda-forge xcube_geodb
$ conda activate xcube_geodb
```

The client comes with a jupyter lab pre-installed. To launch the lab type:

```bash
$ conda activate xcube_geodb
$ jupyter lab
```

We have described the installation using the standard package manager`conda`. 
However, we strongly encourage you to consider using [`mamba`](https://github.com/mamba-org/mamba) instead, 
particularly if you combine the geoDB with [xcube](https://github.com/dcs4cop/xcube). 

### Installation from Sources

We discourage you to install from source. However, for developers installing from source is the only way.
To do so clone the repository using `git`:
    
```bash
$ git clone https://github.com/dcs4cop/xcube-geodb.git
$ cd xcube-geodb
```    

You need to create the conda environment to install all dependencies: 

```bash
$ conda env create
```

Once the environment has been created you can activate the environment and install the geoDB client:

```bash
$ conda activate xcube-geodb
$ python setup.py develop
```

## 2. Installation of the Database Backend

This section describes how to set up the geoDB PostGIS database. The geoDB POstGIS database consists of three 
software components:

- A PostGreSQl database (version 14)
- A PostGIS extension (version 14)
- The geoDB extension (this version)

The easiest way is to use docker. We maintain a docker image that includes all these three components hosted on
quay.io.

```bash
docker run -p 5432:5432 -e POSTGRES_PASSWORD=mypassword quay.io/bcdev/xcube-geodb-backend
```

For more information about the docker image refer to [the PostGIS docker image](https://registry.hub.docker.com/r/postgis/postgis/).

Another option is to install the geoDB extension into an existing PostGIS instance. Prerequisite is, though, that 
you have full access to the database. If so, you can use a Makefile in our geoDB client repository. To do so clone
the repository and move into the sql directory of teh xcube_geodb package:

```bash
$ git clone https://github.com/dcs4cop/xcube-geodb.git
$ cd xcube-geodb/xcube_geodb/sql
```

You will find a `Makefile`  in this directory. Before you can install the geodb extension you need to ensure that
the xcube geodb version is set properly. You use the software `make` for that. Ensure that make is installed.

```bash
export GEODB_VERSION=<version>
make release_version
```

You will find two new files in your directory:

- `geodb.control` and
- `geodb--<version>.sql`

It is important that those exist and that the version matches with the geoDB software version. The extension might 
otherwise not install. You find the current version in xcube_geodb/version.py.

Once the above-mentioned files exist, run `make install`. This will install all necessary files into your PostGIS
directory structure.

Lastly, open a PostGreSQL console or a database GUI of your choice and enter the following SQL command:

```postgresql
CREATE EXTENSION geodb;
```

## 3. Installation of the Postgrest RestAPI

One of the main objectives of the geoDB is to offer an easy way for users to gain access to a PostGIS database
via a RestAPI. The geoDB takes advantage of teh existing PostGreSQL restAPI [`Postgrest`](https://postgrest.org/en/stable/releases/v7.0.1.html)
version 7.0.1 (we aim to upgrade to version 9 as it integrates better with POstGIS in the future).

To configure a postgrest instance please refer to the [postgrest configuration docs](https://postgrest.org/en/stable/configuration.html).

In our setup we used the following steps:

- Installation of the postgrest service

```dotenv
db-uri = "postgres://user:mapassword@localhost:5432/geodb"
db-schema = "public, geodb_user_info"
db-anon-role = "anonymous"
```

### Some Comments about Authorization/Authentication 

The geoDB infrastructure was developed to run on the eurodatacube infrastructure. Hence, it was never meant to run
outside an authorization flow i.e. oauth2 bearer tokens. There are various ways to configure authorization flows 
with postgrest. However, the client only allows oauth2 flows.

The geoDB client can be run without any authentication and authorization 
since version `1.0.3`. However, as the client is meant to run using oauth2 flows, you have to explicitly switch
off authorization using the client's constructor:

```python
import xcube_geodb.core.geodb

geodb = xcube_geodb.core.geodb.GeoDBClient(auth_mode=None)
```

In the following, we give an example, how to configure postgrest for proper authorization. In this example
we use auth0 as the authentication provider. This should also in principle work with other providers like Keycloak.  

__Step 1__: Create an API in Auth0

Ensure that you name the API (this will be the audience in the client configuration) and make sure that
`Add Permissions in the Access Token` is enabled.

__Step 2__: Create an API in Auth0

You need to create a 'Machine to Machine' application in Auth0 in order for this example to work. Other providers 
call this 'client'. Select the API created above and note down the client_id and client_secret. 

__Step 3__: Configure the client 

The AUth0 application is used by the geoDB client. It connects to Auth0 and asks for a so-called bearer token. 
IN our example the geoDB client uses a client_id/client_secret pair and sends it to the authentication provider. 
The provider returns the `bearer token`. The token contains information about the client as given in the example below:

```json
{
  "iss": "an issuer",
  "aud": [
    "an audience"
  ],
  "scope": "scoped that are authorized",
  "gty": "client-credentials",
  "email": "email of the user",
  "permissions": [
    "read:collections"
  ],
  "https://geodb.brockmann-consult.de/dbrole": "the posgresql role",
  "exp": "expiry date"
}
```

The geoDB client will use that token every time it connects to the postgrest service. The postgrest service will test, 
whether the token (and hence the user/client) is authorized to access the DB. The token also contains client/user 
information like the PostGreSQL role the client/user is assigned to. 

The client can be configured using dotenv for your convenience. Add a .env file in your working directory. Add the
following entries of you use client credentials:

```dotenv
GEODB_AUTH_CLIENT_ID = "the auth0 client id"
GEODB_AUTH_CLIENT_SECRET = "the auth0 client secret"
GEODB_AUTH_MODE = "client-credentials"
GEODB_AUTH_AUD = "the auth0 audience (The name of your API)"
GEODB_AUTH_DOMAIN="The auth0 domain (Look in yout auth0 application under 'Endpoints/OAuth Token URL')"
GEODB_API_SERVER_URL = "The postgrest API server URL"
GEODB_API_SERVER_PORT = "The postgrest API server port"
```

__Side remark__: If you would like to use a username and password flow you will use the following entries:

```dotenv
GEODB_AUTH_USERNAME = "the auth0 username"
GEODB_AUTH_PASSWORD = "the auth0 password"
GEODB_AUTH_MODE = "password"
GEODB_AUTH_AUD = "the auth0 audience"
GEODB_AUTH_DOMAIN="The auth0 domain"
GEODB_API_SERVER_URL = "The postgrest API server URL"
GEODB_API_SERVER_PORT = "The postgrest API server port"
```

Please be aware that the username/password flow is discouraged. However, Auth0 has a strict limit on the
number of applications (100). Hence, it might be necessary to use the username/password flow in Auth0 if you
have a large number of users. Otherwise, you can switch to Keycloak. Please refer to teh Auth0 docs how to set up
that flow.

__Step 4__: Configure the postgrest Service

The postgrest service needs a key to check the signature of the token. This is done using the `jwt-secret`
in the postgrest configuration file using asymmetric encryption (tested with Auth0) 
(see [postgrest docs chapter 'JWT from Auth0'](https://postgrest.org/en/stable/auth.html#client-auth))

```dotenv
db-uri = "postgres://user:mapassword@localhost:5432/geodb"
db-schema = "public, geodb_user_info"
db-anon-role = "anonymous"
jwt-secret = ""{\"alg\":\"RS256\",\"e\":\"AQAB\",\"key_ops\":[\"verify\"],\"kty\":\"RSA\",\"n\":\"aav7svBqEXAw-5D29LO...\"}""
```

The entry in section "n" is provided by Auth0 as a so-called 'public key' of the application you have configured in 
Auth0.

