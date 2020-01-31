
# Installation

## Installation using conda

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
    
Run tests

```bash
$ pytest
```

with coverage

```bash
$ pytest --cov=xcube
```

with [coverage report](https://pytest-cov.readthedocs.io/en/latest/reporting.html) in HTML

```bash
$ pytest --cov-report html --cov=xcube
```
