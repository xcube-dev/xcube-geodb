# Image from https://hub.docker.com (syntax: repo/image:version)
FROM quay.io/bcdev/datascience-notebook:latest

# Person responsible
LABEL maintainer="helge.dzierzon@brockmann-consult.de"
LABEL name=xcube_geodb
ENV XCUBE_GEODB_VERSION=0.1.11.dev1

LABEL version=${XCUBE_GEODB_VERSION}

USER $NB_UID

RUN conda install -n base -c conda-forge mamba pip
RUN mamba install -y -c conda-forge xcube
RUN pip install ipyauth IPython
RUN jupyter labextension install ipyauth
RUN jupyter labextension install @jupyterlab/github
RUN pip install jupyterlab_github
RUN jupyter serverextension enable --sys-prefix jupyterlab_github

WORKDIR /tmp
ADD environment.yml /tmp/environment.yml
RUN mamba env update -n base


ADD --chown=1000:100 . ./xcube-geodb
WORKDIR /tmp/xcube-geodb

RUN python setup.py install

WORKDIR $HOME
