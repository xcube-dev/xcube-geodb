# Docker builds for the PostGIS/geoDB Backend Docker Image

The backend image is automatically built on push to master as well as release. The image is built during
the default GitHub workflow in step `build-docker-image`.  

This image is not yet used. Currently teh geodb is installed on a AWS RDS PostgreSQL instance. This image can be used
when the geoDB is installed as a stand-alone instance in an e.g. Kubenertes cluster. It can also be run using docker
like so:

```bash
docker run -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword  quay.io/bcdev/xcube-geoserv 
```

This would start a geoDB/PostGIS instance accessible through port 5432 and user `postgres:mysecretpassword`.