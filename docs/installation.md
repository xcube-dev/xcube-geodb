# Installation

In this chapter we describe 1)  how to install the Python client, which is the
user interface to the xcube geoDB, and 2) how to set up the xcube geoDB
infrastructure. Steps 2.-4. are only relevant if you wish to install your own
instance of xcube geoDB. The usual case is that you have acquired access to the
xcube geoDB via 3rd-party services, such as
[EuroDataCube](https://eurodatacube.com/) or [EO-TDL](https://www.eotdl.com/).

If any problems occur regarding the installation, please create a new issue
in the xcube geoDB
[issue tracker](https://github.com/xcube-dev/xcube-geodb/issues).

## 1. Installing the xcube geoDB user client

This chapter describes the installation of the xcube geoDB client, which
is the user interface and serves as a wrapper to accessing an existing
xcube geoDB service.

### Installation using the conda/mamba

The xcube geoDB client is preferably installed into a conda environment.
Ensure that you use Python 3 (>=3.6). The xcube geoDB client has not been
tested with Python 2.

The client is installed using conda (replace `conda` with `mamba` below if you
are using mamba):

```bash
$ conda install -c conda-forge xcube_geodb
$ conda activate xcube_geodb
```

We strongly recommend using
[`mamba`](https://github.com/mamba-org/mamba) instead of `conda`.

### Installation from source

There are frequent `conda` releases of the xcube geoDB, so usually it is not
necessary to install from sources. However, if you want to try the cutting-
edge version, use the following steps.
Clone the repository using `git`:

```bash
$ git clone https://github.com/xcube-dev/xcube-geodb.git
$ cd xcube-geodb
```    

You need to create the conda environment to install all dependencies
(replace `conda` with `mamba` below if you are using mamba):

```bash
$ conda env create
```

Once the environment has been created, activate the environment and
install the xcube geoDB client (replace `conda` with `mamba` below if are
using mamba):

```bash
$ conda activate xcube-geodb
$ python setup.py develop
```

## 2. Installation of the Database Backend

This section describes how to set up the xcube geoDB database. The
xcube geoDB core database consists of three software components:

- an installation of a PostgreSQL database (version >= v13.19)
- the PostGIS extension (version 3.x.x), installed in the database
- the xcube geoDB extension, installed in the database

The easiest way is to install the xcube geoDB extension into an existing PostGIS
instance. There are ready-to-use PostGIS docker images, see
[PostGIS docker image](https://registry.hub.docker.com/r/postgis/postgis/).

To install the xcube geoDB extension, open a PostGreSQL console or a database
GUI of your choice as superuser. Then copy and paste the contents of the geoDB
[SQL script](https://github.com/xcube-dev/xcube-geodb/blob/main/xcube_geodb/sql/geodb.sql),
replacing `VERSION_PLACEHOLDER` with the version provided in
[version.py](https://github.com/xcube-dev/xcube-geodb/blob/main/xcube_geodb/version.py).

Then choose a password for the `authenticator` user, and apply it:

`ALTER ROLE authenticator WITH PASSWORD '<password>';`

## 3. Installation of the Postgrest RestAPI

xcube geoDB offers access to the database via a RestAPI. xcube geoDB takes
advantage of the wonderful Postgres REST API
[PostgREST](https://github.com/PostgREST/postgrest). There are ready-to-use
docker images that provide PostGREST, e.g. at
https://hub.docker.com/r/postgrest/postgrest. PostGREST does not have many
version requirements on the Postgres database; however, PostGREST ≥ v12.2.0
supports only those versions of Postgres that are officially supported
(i.e., ≥ 13).

To configure a PostgREST instance please refer to the
[PostgREST configuration documentation](https://postgrest.org/en/stable/configuration.html).
The configuration is straightforward; the only part of the configuration that
is not is the authorization/authentication part, which is covered in detail
in the next chapter.

## 4. Authorization/Authentication

The xcube geoDB infrastructure was developed to run on the
[EuroDataCube](https://eurodatacube.com/) infrastructure.

Therefore, we provide an example on how to configure PostgREST for proper
authorization with an OAuth2-enabled Identity and Access Management (IAM)
system (like Keycloak) using the `client_credentials` flow. The example uses
Keycloak values and wording, but can be transferred to any OAuth2-enabled IAM.

__Step 1__: Create an API in your IAM system

Create a new API (Keycloak: Realm) in your IAM system. The users registered in
this API can be enabled to access to your geoDB instance. In Keycloak, be sure
to create a new Keystore provider of type `rsa` within the realm, that uses the
algorithm `RS256`, and has the highest priority of all existing other
keystores. Generate public/private Keypair, e.g. like this:

```bash
$ ssh-keygen -t rsa -b 4096 -m PEM -f winchester.key
$ openssl rsa -in winchester.key -pubout -outform PEM -out winchester.key.pub
```

Turn the public key into a JWK, e.g. using this
[website](https://8gwifi.org/jwkconvertfunctions.jsp), and set the private key
as the key of the Keystore provider you just configured:

![realm_key_provider.png](realm_key_provider.png)

__Step 2__: Create an Application in your IAM API

In Keycloak, create a client, choose Client Protocol `openid-connect`, and
Access Type `confidential`. Enter a redirect URI that matches the URL of your
service that provides the users with the access tokens. For the existing
instances of xcube geoDB, this is the dedicated service `winchester`. In the
simplest case, you need to set up a service that forwards the user
login requests to your Keycloak instance, and returns the access token after
successful login. That service must authenticate at Keycloak with the
`client_id` and `client_secret` that can be retrieved from Keycloak.
Furthermore, configure a mapper that maps the user role to the token claim
that has been configured as `PGRST_ROLE_CLAIM_KEY` in your PostGREST
configuration in section 3. The user role might be some user attribute. For
the `winchester` client, this is done like this:

![winchester_mapper.png](winchester_mapper.png)

Consequently, the PostGREST configuration item `PGRST_ROLE_CLAIM_KEY` is
set to `'."https://geodb.brockmann-consult.de/dbrole"'`. Every user in the
Realm has an attribute called `xcube_geodb_role`.

__Step 3__: Use the client to authenticate

The client can be configured using dotenv for your convenience. Add a .env file
in your working directory. Add the following entries of you use client
credentials:

```dotenv
GEODB_AUTH_CLIENT_ID = "the username as stored in Keycloak"
GEODB_AUTH_CLIENT_SECRET = "the password as stored in Keycloak"
GEODB_AUTH_MODE = "client-credentials"
GEODB_AUTH_DOMAIN= "the URL of your login service"
GEODB_API_SERVER_URL = "The postgrest API server URL"
GEODB_API_SERVER_PORT = "The postgrest API server port"
```

__Step 4__: Configure the PostgREST Service

Point the PostgREST service to your database server by setting `db-uri`.
From the sample below, replace `password` with the password you set in
section 2; and replace `localhost:5432` with the host and port of your
database server.
Also, the PostgREST service needs a key to check the signature of the token. This is
done using the `jwt-secret` in the PostGREST configuration. You can use
symmetric encryption and store the key passphrase in `jwt-secret`, or use
asymmetric encryption. Keycloak only supports asymmetric encryption. In that
case, use the public part of the Keypair you generated in step 1 as value for
`jwt-secret`. See also the
[PostgREST documentation, section "JWT from Auth0"](https://postgrest.org/en/stable/auth.html#client-auth)):

```dotenv
db-uri = "postgres://authenticator:password@localhost:5432/geodb"
db-schema = "public, geodb_user_info"
db-anon-role = "anonymous"
jwt-secret = ""{\"alg\":\"RS256\",\"e\":\"AQAB\",\"key_ops\":[\"verify\"],\"kty\":\"RSA\",\"n\":\"aav7svBqEXAw-5D29LO...\"}""
```

## 5. Installation of the geoserver

Data collections stored in the xcube geoDB can be published to a GeoServer
instance, which can offer OGC WMS and WCS access to the data. In order to  
access such a server, the login server described section 4, step 2,
needs access to a GeoServer instance using the credentials of a generic
GeoServer user. The current xcube geoDB setup uses a custom docker image of
based on the image `terrestris/geoserver:2.19.1`, with the `vectortiles`
extension installed. It is published at
[quay.io](https://quay.io/repository/bcdev/xcube-geoserv).

**Careful**: When installing this docker image, you may run into CORS issues,
and a wrong redirect to a http not https URL after login. Those issues have
been resolved by adding the following settings to the GeoServer configuration:

```yaml
 geoserver:
   geoserverCsrfWhitelist: xcube-geodb.brockmann-consult.de
   proxyBaseUrl: https://xcube-geodb.brockmann-consult.de/geoserver
```

These values are also configurable in the `web.xml` in
`${GEOSERVER_DIR}/geoserver/WEB-INF`.

For any more detailed information about installation, please refer to the
[Dockerfile](https://github.com/terrestris/docker-geoserver/blob/master/Dockerfile)
or the original
[Installation instructions](https://docs.geoserver.org/stable/en/user/installation/index.html).

Please be aware that the admin credentials should be changed after installation.