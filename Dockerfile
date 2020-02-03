# Image from https://hub.docker.com (syntax: repo/image:version)
FROM continuumio/miniconda3:latest

# Person responsible
LABEL maintainer="helge.dzierzon@brockmann-consult.de"
LABEL name=xcube_geodb
ENV XCUBE_VERSION=0.3.0
ENV XCUBE_GEODB_VERSION=0.1.0

LABEL version=${XCUBE_GEODB_VERSION}

# Ensure usage of bash (simplifies source activate calls)
SHELL ["/bin/bash", "-c"]

# Update system and install dependencies
RUN apt-get -y update && apt-get -y upgrade && apt-get -y install curl vim

RUN groupadd -g 1000 xcube
RUN useradd -u 1000 -g 1000 -ms /bin/bash xcube
RUN mkdir /workspace && chown xcube.xcube /workspace
RUN chown -R xcube.xcube /opt/conda

USER xcube

RUN conda create -n xcube -c conda-forge xcube=${XCUBE_VERSION}

RUN git clone https://github.com/dcs4cop/xcube-geodb /workspace/xcube-geodb
WORKDIR /workspace/xcube-geodb

RUN source activate xcube && conda env update xcube -f environment.yml
RUN source activate xcube && python setup.py develop

WORKDIR /workspace

RUN conda init

RUN echo "conda activate xcube" >> ~/.bashrc

ENTRYPOINT ["/bin/bash", "-c"]
