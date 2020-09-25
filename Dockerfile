# Image from https://hub.docker.com (syntax: repo/image:version)
FROM quay.io/bcdev/xcube-jupyterlab:0.5.1

# Person responsible
LABEL maintainer="helge.dzierzon@brockmann-consult.de"
LABEL name=xcube_geodb
ENV XCUBE_VERSION=0.5.1
ENV XCUBE_GEODB_VERSION=0.1.9

LABEL version=${XCUBE_GEODB_VERSION}

# Ensure usage of bash (simplifies source activate calls)
SHELL ["/bin/bash", "-c"]

USER xcube

ADD --chown=1000:1000 . /workspace/xcube-geodb
WORKDIR /workspace/xcube-geodb

RUN source activate xcube && mamba env update xcube -f environment.yml
RUN source activate xcube && python setup.py install

WORKDIR /workspace

CMD ["/bin/bash", "-c", "source activate xcube && jupyter lab --ip 0.0.0.0"]
