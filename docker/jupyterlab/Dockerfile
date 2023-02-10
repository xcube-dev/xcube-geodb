# Image from https://hub.docker.com (syntax: repo/image:version)
FROM jupyter/datascience-notebook:latest

# Person responsible
LABEL maintainer="thomas.storm@brockmann-consult.de"
LABEL name=xcube_geodb

USER $NB_UID

RUN conda install -n base -c conda-forge mamba pip

WORKDIR /tmp
ADD environment.yml /tmp/environment.yml
RUN mamba env update -n base
RUN mamba install -n base -y  -c conda-forge jupyterlab-git jupyterlab-drawio jupyterlab_code_formatter jupyterlab-spellchecker
RUN source activate base && pip install jupyterlab-geojson

ADD --chown=1000:100 . ./xcube-geodb
WORKDIR /tmp/xcube-geodb

RUN python setup.py install

WORKDIR $HOME
