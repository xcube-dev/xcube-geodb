# An image on basis of this docker file should be pushed to quay.io/bcdev/xcube-geoserv:<version>
# The version may diverge from geodb version as geoserver is expected to seldom change.
# After pushing a new version of this docker image to quay.io, adapt tag in k8s-configs/xcube-geodb/helm/charts/values-*.yaml
#
# Example for version 1.0.3:
#   sudo docker build -t xcube-geoserv:1.0.3 .
#   sudo docker tag xcube-geoserv:1.0.3 quay.io/bcdev/xcube-geoserv:1.0.3
#   sudo docker login quay.io
#     username: bcdev
#     password: <pw>
#   sudo docker push quay.io/bcdev/xcube-geoserv:1.0.3
FROM terrestris/geoserver:2.19.1

WORKDIR /usr/local/tomcat/webapps/geoserver/WEB-INF/lib

RUN wget https://altushost-swe.dl.sourceforge.net/project/geoserver/GeoServer/2.19.1/extensions/geoserver-2.19.1-vectortiles-plugin.zip

RUN unzip geoserver-2.19.1-vectortiles-plugin.zip

