#!/usr/bin/env bash
# (c) 2017 - 2023 Tuplex team
# downloads and builds a debug version of Python against which Tuplex can be build then

PYTHON_VERSION=3.11.7
# check from where script is invoked
CWD="$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

DEST_DIR=`readlink -f $CWD/..`"/build/python${PYTHON_VERSION}"

echo ">>> Building debug version of python ${PYTHON_VERSION} in ${DEST_DIR}"
mkdir -p $DEST_DIR

CPU_CORES=$(getconf _NPROCESSORS_ONLN)

## fetch python version to build dir
## build Python according to https://pythonextensionpatterns.readthedocs.io/en/latest/debugging/debug_python.html
## with counts enabled to make sure it works and results are produced.
#cd $DEST_DIR \
#&& echo ">>> Downloading Python ${PYTHON_VERSION}" \
#&& curl -O https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz \
#&& echo ">>> uncompressing archive" \
#&& tar xf Python-${PYTHON_VERSION}.tgz \
#&& cd Python-${PYTHON_VERSION} \
#&& echo ">>> configuring python debug build" \
#&& ./configure CFLAGS='-DCOUNT_ALLOCS' --with-pydebug --without-pymalloc --prefix=$DEST_DIR \
#&& make -j${CPU_CORES} && make install && cd $DEST_DIR && rm -rf Python-${PYTHON_VERSION}{,.tgz} \
#&& echo ">>> Installed Python ${PYTHON_VERSION} in $DEST_DIR, add $DEST_DIR/bin to PATH to make debug python version visible"

# build Tuplex now with this python debug version
PYTHON=$DEST_DIR/bin/python3
REPOSITORY_ROOT_DIR=$DEST_DIR/../..
echo ">>> install wheel package" && $PYTHON -m pip install wheel cloudpickle numpy nbformat pytest delocate
echo ">>> Build Tuplex with debug Python version" && cd $REPOSITORY_ROOT_DIR && $PYTHON setup.py bdist_wheel \
&& cd dist && delocate-wheel -w fixed_wheels tuplex*.whl -v && cd fixed_wheels