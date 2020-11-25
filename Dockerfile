# Image from https://hub.docker.com (syntax: repo/image:version)
FROM jupyter/datascience-notebook

# Person responsible
LABEL maintainer="helge.dzierzon@brockmann-consult.de"
LABEL name=xcube_geodb
ENV XCUBE_GEODB_VERSION=0.1.11.dev1

LABEL version=${XCUBE_GEODB_VERSION}

USER $NB_UID

RUN conda install -n base -c conda-forge mamba pip
RUN mamba install -y -c conda-forge xcube

WORKDIR /tmp
ADD environment.yml /tmp/environment.yml
RUN mamba env update -n base
RUN pip install ipyauth IPython

ADD --chown=1000:100 . ./xcube-geodb
WORKDIR /tmp/xcube-geodb

RUN python setup.py install

WORKDIR $HOME
