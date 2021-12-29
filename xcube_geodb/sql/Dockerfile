FROM postgis/postgis:14-3.1

ARG GEODB_VERSION=1.0.3

WORKDIR /tmp

RUN apt-get update -y && apt-get install build-essential -y

ADD ./ .
RUN make install

COPY init_geodb.sh /docker-entrypoint-initdb.d/20_init_geodb.sh

