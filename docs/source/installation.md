
# Installation

In this chapter we will describe how to install the geoDB Python client. The client accesses 
a [postgrest](https://postgrest.org) instance as an web API to a PostGreSQL instance. 
  
## Installing the geodb package using conda

Into existing conda environment (>= Python 3.6)

```bash
$ conda install -c conda-forge xcube_geodb
```

Into new conda environment
   
```bash
$ conda create -c conda-forge -n xcube-geodb python3
$ conda install -c conda-forge xcube_geodb
```

## Installation from sources

First
    
```bash
$ git clone https://github.com/dcs4cop/xcube-geodb.git
$ cd xcube-geodb
$ conda env create
```    

Then

```bash
$ conda activate xcube-geodb
$ python setup.py develop
```

Update

```bash
$ conda activate xcube-geodb
$ git pull --force
$ python setup.py develop
```
    
Running unit tests

```bash
$ pytest
```

## Installation of the backend

__Step 1__: Set up a PostGreSQL instance

It is up to you how you set up the PostGreSQL backend. We have used AWS RDS. The only prerequisites are the following:

- Version: Tested with PostGreSQL 10
- The Postgrest service needs access to the PostGreSQL service

If you use docker or orchestration tools like Kubernetes it is recommended to use docker images with PostGIS 
pre-installed.


__Step 2__: Install POSTGIS

If you need to install PostGIS yourself please use these [installations instructions](https://postgis.net/install/). 

__Step 3__: Set up the GeoDB extension

The geoDB can be setup like a PostGreSQL extension. To use the geoDB as extension you need to have full access to 
the PostGreSQL database. Move into the 

If you do not have access to the PostGreSQL service, you can use the content of the file ... and execute it as superuser 
in PG. If you do not have superuser access you cannot install the geoDB backend.





