# Installation

In this chapter we will describe how to install the xcube geoDB infrastructure.
The infrastructure consists of four main
components:

1. A [Python client/API](https://github.com/dcs4cop/xcube-geodb) accessing
   the database through the PostGrest RestAPI (this version)
2. A [PostGIS database](https://postgis.net/) (Version 14)
3. An xcube geoDB extension to be installed into the PostGIS database (this
   version)
4. A [PostGrest RestAPI](https://postgrest.org/en/stable/) (Version 7)

## 1. Installing xcube geoDB Client/API

The aim of the chapter is to describe the installation of the xcube geoDB
client which serves as a wrapper to accessing an existing xcube geoDB 
service. You can omit steps 2.-4. entirely if you have gained access to such 
a service (e.g. by buying xcube geoDB access through the [eurodatacube]
(https://eurodatacube.com/) service).

### Installation Using the Package Manager conda/mamba

The xcube geoDB client is preferably installed into a conda environment.
Ensure that you use Python 3 (>=3.6). The xcube geoDB client has not been
tested with Python 2 and will very likely not work.

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

We have described the installation using the standard package manager `conda`.
However, we strongly encourage you to consider using
[`mamba`](https://github.com/mamba-org/mamba) instead, particularly if you
combine the xcube geoDB with [xcube](https://github.com/dcs4cop/xcube).

### Installation from Sources

We discourage you to install from source. However, if you are a developer,
use the following steps.
Clone the repository using `git`:

```bash
$ git clone https://github.com/dcs4cop/xcube-geodb.git
$ cd xcube-geodb
```    

You need to create the conda environment to install all dependencies:

```bash
$ conda env create
```

Once the environment has been created you can activate the environment and
install the xcube geoDB client:

```bash
$ conda activate xcube-geodb
$ python setup.py develop
```

## 2. Installation of the Database Backend

This section describes how to set up the xcube geoDB PostGIS database. The
xcube geoDB PostGIS database consists of three
software components:

- A PostGreSQl database (version 14)
- A PostGIS extension (version 14)
- The xcube geoDB extension (this version)

The easiest way is to use docker. We maintain a docker image that includes
all these three components hosted on quay.io.

```bash
docker run -p 5432:5432 -e POSTGRES_PASSWORD=mypassword quay.io/bcdev/xcube-geodb-backend
```

For more information about the docker image refer to
[the PostGIS docker image](https://registry.hub.docker.com/r/postgis/postgis/).

Another option is to install the xcube geoDB extension into an existing PostGIS
instance. Prerequisite is, though, that you have full access to your
database. If so, you can use a Makefile in our xcube geoDB client repository.
To do so clone the repository and move into the sql directory of the
xcube_geodb package:

```bash
$ git clone https://github.com/dcs4cop/xcube-geodb.git
$ cd xcube-geodb/xcube_geodb/sql
```

You will find a `Makefile` in this directory. Before you can install the
xcube geoDB extension you need to ensure that
the xcube geoDB version is set properly in the filename of the SQL file.

```bash
export GEODB_VERSION=<version>
make release_version
```

After the execution of the above make command, you will find two new files
in your directory:

- `geodb.control` and
- `geodb--<version>.sql`

It is essential that those exist and that the version in the SQL file name
matches the xcube geoDB software version you attempt to install. The
extension will not install otherwise.

Once the above-mentioned files exist, run `make install`. This will install
all necessary files into your PostGIS directory structure.

Lastly, open a PostGreSQL console or a database GUI of your choice as
super-user and enter the following SQL command:

```postgresql
CREATE EXTENSION geodb;
```

## 3. Installation of the Postgrest RestAPI

One of the main objectives of xcube geoDB is to offer an easy way for users
to gain access to a PostGIS database via a RestAPI. xcube geoDB takes
advantage of the existing PostGreSQL restAPI
[`Postgrest`](https://postgrest.org/en/stable/releases/v7.0.1.html) version
7.0.1 (we aim to upgrade to version 9 as it integrates better with PostGIS
in the future).

To configure a postgrest instance please refer to the
[postgrest configuration docs](https://postgrest.org/en/stable/configuration.html).
We will give an example in the next chapter where we talk about
authorization and authentication.

## 4. Authorization/Authentication

The xcube geoDB infrastructure was developed to run on the eurodatacube
infrastructure. Hence, it was never meant to run outside a different
authorization flow other than oauth2 machine to machine or password flows.
Therefore, we provide an example, how to configure postgrest for proper
authorization with Auth0 using the client_credentials flow. This
configuration should also in principle work with other providers like Keycloak.

__Step 1__: Create an API in Auth0

Ensure that you name the API (this will be the audience in the client
configuration) and make sure that `Add Permissions in the Access Token` is
enabled.

__Step 2__: Create an Application in Auth0

You need to create a 'Machine to Machine' application in Auth0 in order for this
example to work. Other providers call this 'client'. Select the API created 
above and note down the client_id and client_secret Auth0 will provide after 
the creation of the application.

__Step 3__: Configure the client for the `client_credentials` auth flow

The Auth0 application is used by xcube geoDB client when connecting to the Auth0
token end points. In our example the xcube geoDB client uses a  
client_id/client_secret pair and sends it to the authentication provider. 
The provider returns the `bearer token`. The token contains information 
about the client as given in the example below (this example might be 
incomplete):

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

The xcube geoDB client will use that token every time it connects to the 
postgrest service. The postgrest service will test, whether the token (and 
hence the user/client) is authorized to access the PostGIS instance. The 
token also contains client/user information like the PostGreSQL role the 
client/user is assigned to.

The client can be configured using dotenv for your convenience. Add a .env file
in your working directory. Add the following entries of you use client 
credentials:

```dotenv
GEODB_AUTH_CLIENT_ID = "the auth0 client id"
GEODB_AUTH_CLIENT_SECRET = "the auth0 client secret"
GEODB_AUTH_MODE = "client-credentials"
GEODB_AUTH_AUD = "the auth0 audience (The name of your API)"
GEODB_AUTH_DOMAIN="The auth0 domain (Look in yout auth0 application under 'Endpoints/OAuth Token URL')"
GEODB_API_SERVER_URL = "The postgrest API server URL"
GEODB_API_SERVER_PORT = "The postgrest API server port"
```

__Step 3 (alternative)__: Configure the client for the `password` auth flow

The configuration for the password flow is very similar to the 
client_credentials flow. You need to create a single machine to machine 
application in Auth0 instead of an application per user. Use the id and 
secret in your `.env` file. The main difference in the configuration is that 
you need users with a username and password. You can add those in Auth0 as 
many as you need. You need to use a `username-password` connection. The 
username and password can also be configured as environment variable. This 
is meant to be used in JupyterLabs to provide the user's credentials 
automatically in the user's notebook.

```dotenv
GEODB_AUTH_CLIENT_ID = "the auth0 password flow client id"
GEODB_AUTH_CLIENT_SECRET = "the auth0 password flow client secret"
GEODB_AUTH_MODE = "password"
GEODB_AUTH_AUD = "the auth0 audience (The name of your API)"
GEODB_AUTH_DOMAIN="The auth0 domain (Look in yout auth0 application under 'Endpoints/OAuth Token URL')"
GEODB_AUTH_USERNAME = "the auth0 username"
GEODB_AUTH_PASSWORD = "the auth0 password"
GEODB_API_SERVER_URL = "The postgrest API server URL"
GEODB_API_SERVER_PORT = "The postgrest API server port"
```

Please be aware that the username/password flow is discouraged for security
reasons. However, Auth0 has a strict limit on the number of applications 
(100). Hence, it might be necessary to use the username/password flow in 
Auth0 if you have a large number of users. Please refer to the Auth0 docs 
how to set up that flow.

__Step 4__: Configure the postgrest Service

The postgrest service needs a key to check the signature of the token. This is
done using the `jwt-secret` in the postgrest configuration file using 
asymmetric encryption (tested with Auth0) (
see [postgrest docs chapter 'JWT from Auth0'](https://postgrest.org/en/stable/auth.html#client-auth)).

```dotenv
db-uri = "postgres://user:mapassword@localhost:5432/geodb"
db-schema = "public, geodb_user_info"
db-anon-role = "anonymous"
jwt-secret = ""{\"alg\":\"RS256\",\"e\":\"AQAB\",\"key_ops\":[\"verify\"],\"kty\":\"RSA\",\"n\":\"aav7svBqEXAw-5D29LO...\"}""
```

The entry in section "n" is provided by Auth0 as a so-called 'public key' of the
application you have configured in Auth0.

## 5. Installation of the geoserver

The xcube geoDB Python client provides a wrapper around publishing xcube geoDB 
collections as an e.g. WMS service to a Geoserver instance. In order to  
access such a server, xcube geoDB client needs access to a Geoserver 
instance using the credentials of a generic Geoserver user. The current 
xcube geoDB setup uses the docker image of the Geoserver version 2.19.1
(image: terrestris/geoserver:2.19.1).

When installing this docker image we ran into CORS issues and a wrong redirect
to a http not https URL after login. The redirect and CORS issues have been 
resolved by the following settings in the Kubernetes setup:

```yaml
 geoserver:
   geoserverCsrfWhitelist: xcube-geodb.brockmann-consult.de
   proxyBaseUrl: https://xcube-geodb.brockmann-consult.de/geoserver
```

These values are imputed as environment variables into the Geoserver container
and should also be configurable in the `web.xml` in 
`/usr/local/tomcat/webapps/geoserver/WEB-INF`.

In addition, a vectortile plugin has been added to the geoserver image by
building a custom docker image hosted on quay.io (current version: `quay.
io/bcdev/xcube-geoserv:1.0.3`) build using this
[Dockerfile](https://github.com/bc-org/k8s-configs/blob/main/xcube-geodb/docker/geoserver/Dockerfile).

For any more detailed information about installation, please refer to this
[Dockerfile](https://github.com/terrestris/docker-geoserver/blob/master/Dockerfile)
or the original
[Installation instructions](https://docs.geoserver.org/stable/en/user/installation/index.html).

Please be aware that the admin credentials should be changed after installation.
Otherwise, any user with even the most mediocre intelligence will be able to 
log on as admin.
