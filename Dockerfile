ARG XCUBE_DOCKER_BASE_VERSION=0.4.2

FROM quay.io/bcdev/xcube-python-base:${XCUBE_DOCKER_BASE_VERSION}

ARG XCUBE_VERSION=0.4.0.dev0
ARG XCUBE_GEN_VERSION=1.0.1
ARG XCUBE_USER_NAME=xcube

LABEL maintainer="helge.dzierzon@brockmann-consult.de"
LABEL name="xcube geoDB"
LABEL xcube_version=${XCUBE_VERSION}
LABEL xcube_gen_version=${XCUBE_GEN_VERSION}

# Update system and install dependencies
USER root
RUN apt-get -y update && apt-get -y upgrade && apt-get -y install curl vim

USER ${XCUBE_USER_NAME}
RUN mkdir /home/xcube/geodb
WORKDIR /home/xcube/geodb
ADD --chown=1000:1000 environment.yml environment.yml
RUN conda env update -n xcube

ADD --chown=1000:1000 ./ .
RUN source activate xcube && python setup.py install

WORKDIR /workspace

RUN conda init

RUN echo "conda activate xcube" >> ~/.bashrc

ENTRYPOINT ["/bin/bash", "-c"]
