# Using alpine for security reasons. Debian images tend to have SECURITY SCAN issues in quay.io
# An image usoing this docker file has been pushed to quay.io/bcdev/xcube-geodb-backend:GEODB_VERSION
FROM postgis/postgis:14-3.2-alpine

ARG GEODB_VERSION=1.0.6

WORKDIR /tmp

RUN apk update && apk upgrade && apk add make

# Adding installation files
ADD ./ .

# Installation of the geodb extension into the postgres structure. The final installation happens in 20_init_geodb.sh
RUN make release_version && make install

# Initialise geodb extension in PG database in entrypoint
COPY init_geodb.sh /docker-entrypoint-initdb.d/20_init_geodb.sh
