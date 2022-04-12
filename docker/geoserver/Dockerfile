# An image usoing this docker file has been pushed to quay.io/bcdev/xcube-geodb-backend:GEODB_VERSION
# This image is automatically built using q quay build trigger
FROM terrestris/geoserver:2.19.1

WORKDIR /usr/local/tomcat/webapps/geoserver/WEB-INF/lib

RUN wget https://altushost-swe.dl.sourceforge.net/project/geoserver/GeoServer/2.19.1/extensions/geoserver-2.19.1-vectortiles-plugin.zip

RUN unzip geoserver-2.19.1-vectortiles-plugin.zip

