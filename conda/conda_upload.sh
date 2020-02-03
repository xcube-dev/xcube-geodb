# Only need to change these two variables
export PKG_NAME=xcube_geodb
export USER=bc-dev


if [[ "$TRAVIS_OS_NAME" == "linux" ]]; then
  conda config --set anaconda_upload no

  CONDA_PACKAGE=$(conda build -c conda-forge recipe --output)

  echo anaconda -t ${CONDA_UPLOAD_TOKEN} upload  -u ${USER} ${CONDA_PACKAGE} --force

  anaconda -t ${CONDA_UPLOAD_TOKEN} upload  -u ${USER} ${CONDA_PACKAGE} --force
fi
