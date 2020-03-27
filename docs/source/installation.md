
# Installation

In this chapter we will describe how to install the geoDB Python client. The client accesses 
a [postgrest]() instance as an web API to a PostGreSQL instance. We will in principle describe 
how to setup the backend.   
  
## Installing the geodb package using conda

Into existing conda environment (>= Python 3.6)

```bash
$ conda install -c conda-forge xcube-geodb
```

Into new conda environment
   
```bash
$ conda create -c conda-forge -n xcube-geodb python3
$ conda install -c conda-forge xcube-geodb
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

- __Step 1__: Setup a PostGreSQL instance
- __Step 2__: Setup tables and roles 
- __Step 3__: Setup a PostgREST instance.
- __Step __: Setup a PostGreSQL instance.


