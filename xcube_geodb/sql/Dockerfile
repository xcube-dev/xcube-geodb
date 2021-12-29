FROM postgis/postgis:14-3.1

ARG GEODB_VERSION=1.0.2

WORKDIR /tmp

RUN apt-get update -y && apt-get install wget build-essential -y

ADD ./ .
RUN mv geodb.sql geodb--${GEODB_VERSION}.sql
RUN make install

COPY init_geodb.sh /docker-entrypoint-initdb.d/20_init_geodb.sh

