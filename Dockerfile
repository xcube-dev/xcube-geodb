# Image from https://hub.docker.com (syntax: repo/image:version)
FROM continuumio/miniconda3:latest

# Person responsible
LABEL maintainer="helge.dzierzon@brockmann-consult.de"
LABEL name=xcube_geodb
ENV XCUBE_VERSION=0.5.1
ENV XCUBE_USER_NAME=xcube
ENV XCUBE_GEODB_VERSION=0.1.9

LABEL version=${XCUBE_GEODB_VERSION}

# Ensure usage of bash (simplifies source activate calls)
SHELL ["/bin/bash", "-c"]
RUN groupadd -g 1000 ${XCUBE_USER_NAME}
RUN useradd -u 1000 -g 1000 -ms /bin/bash ${XCUBE_USER_NAME}
RUN mkdir /workspace && chown ${XCUBE_USER_NAME}.${XCUBE_USER_NAME} /workspace
RUN chown -R ${XCUBE_USER_NAME}.${XCUBE_USER_NAME} /opt/conda

USER ${XCUBE_USER_NAME}

ADD --chown=1000:1000 ./environment.yml /tmp/environment.yml
WORKDIR /tmp
RUN source activate base && conda update -n base conda && conda init
RUN source activate base && conda install -n base -c conda-forge mamba pip
RUN mamba env create

ADD --chown=1000:1000 . /workspace/xcube-geodb
WORKDIR /workspace/xcube-geodb

RUN source activate xcube_geodb && python setup.py install
RUN source activate xcube_geodb && mamba install -y -c conda-forge ipykernel
RUN source activate xcube_geodb && ipython kernel install --user --name=xcube-geodb

WORKDIR /workspace

CMD ["/bin/bash", "-c", "source activate xcube_geodb && jupyter lab --ip 0.0.0.0"]