#!/usr/bin/env bash
# (c) 2024 L.Spiegelberg
# collects script invocations required to produce graphs for flight experiments

# Contains a pure C++ version with a perfect sparse struct type to check how fast it is compared to python baseline.

set -e pipefail

# get shell script location
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $BASE_DIR

BUILD_DIR=$BASE_DIR/build
ALT_BUILD_DIR=$BASE_DIR/alt-build

PYTHON=python3
DESIRED_PYTHON_VERSION=3.11


echo "-- Tuplex benchmarking -- "

# Check python3 version
PYTHON3_VERSION=$($PYTHON --version | cut -f2 -d ' ')
PYTHON3_MAJMIN=${PYTHON3_VERSION%.*}

if [ "$PYTHON3_MAJMIN" != "$DESIRED_PYTHON_VERSION" ]; then
  echo ">>> Found Python ${PYTHON3_VERSION}, but need at least ${DESIRED_PYTHON_VERSION}, abort."
  exit 1
else
  echo ">>> Using Python ${PYTHON3_VERSION}."
fi

PYTHON3_EXECUTABLE=$($PYTHON -c "import sys;print(sys.executable)")

# Install benchmarking requirements
$PYTHON -m pip install -r requirements.txt

echo "-- Using python3 executable ${PYTHON3_EXECUTABLE}"

echo "-> Compiling Tuplex (w. yyjson) to $BUILD_DIR"
mkdir -p $BUILD_DIR
echo "-> Compiling Tuplex (w. cjson) to $ALT_BUILD_DIR"
mkdir -p $ALT_BUILD_DIR
TUPLEX_DIR=$BUILD_DIR/../../../../../tuplex

# How many cores? Use all for build.
if [[ "$OSTYPE" =~ ^linux ]]; then
    N_PROCESSORS=`nproc`
elif [[ "$OSTYPE" =~ ^darwin ]]; then
    N_PROCESSORS=`sysctl -n hw.physicalcpu`
fi

echo "-- Using ${N_PROCESSORS} to build Tuplex."

# Unix
if [[ "$OSTYPE" =~ ^linux ]]; then
    LLVM_DIR=/opt/llvm-16.0.6
    echo ">>> Building w. yyjson"
    cd $BUILD_DIR && cmake -DCMAKE_BUILD_TYPE=Release -DPython3_EXECUTABLE=$PYTHON3_EXECUTABLE -DUSE_YYJSON_INSTEAD=ON -DSKIP_AWS_TESTS=ON -DBUILD_WITH_ORC=ON -DAWS_S3_TEST_BUCKET='tuplex-test' -DLLVM_ROOT_DIR=$LLVM_DIR $TUPLEX_DIR && make -j${N_PROCESSORS} tuplex && make -j${N_PROCESSORS} tuplex-worker && cd ..

    #echo ">>> Building w. cjson"
    #cd $ALT_BUILD_DIR && cmake -DCMAKE_BUILD_TYPE=Release -DUSE_YYJSON_INSTEAD=OFF -DSKIP_AWS_TESTS=ON -DBUILD_WITH_ORC=ON -DAWS_S3_TEST_BUCKET='tuplex-test' -DLLVM_ROOT_DIR=$LLVM_DIR $TUPLEX_DIR && make -j${N_PROCESSORS} tuplex_github && cd ..
fi

# mac os
if [[ "$OSTYPE" =~ ^darwin ]]; then
    LLVM_DIR=/usr/local/Cellar/llvm/16.0.3
    cd $BUILD_DIR && cmake -DPYTHON3_VERSION=3.11 -DCMAKE_BUILD_TYPE=Release -DSKIP_AWS_TESTS=ON -DBUILD_WITH_ORC=ON -DAWS_S3_TEST_BUCKET='tuplex-test' -DLLVM_ROOT_DIR=$LLVM_DIR $TUPLEX_DIR && make -j$(nproc) tuplex tuplex-worker && cd ..
fi

echo "-- Built Tuplex."

# Create wheel from compiled tuplex version.
echo ">>> Creating whl file from compiled Tuplex python folder."
${PYTHON} ${BUILD_DIR}/dist/python/setup.py bdist_wheel

WHL_FILE=$(ls ${BUILD_DIR}/dist/python/dist/*.whl)
echo ">>> Installing tuplex (force) from wheel ${WHL_FILE}"
${PYTHON} -m pip install --upgrade --force-reinstall ${WHL_FILE}

# start benchmarking, first single run + validate results.
${PYTHON} runtuplex-new.py --help

INPUT_PATTERN='/hot/data/github_daily/*.json'
RESULT_DIR=.local/test
OUTPUT_PATH=${RESULT_DIR}/output
mkdir -p ${RESULT_DIR}

${PYTHON} runtuplex-new.py --mode tuplex --input-pattern "${INPUT_PATTERN}" --output-path ${OUTPUT_PATH} \
                           --tuplex-worker-path $BUILD_DIR/dist/bin/tuplex-worker \
                           --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/log.txt \
                           --result-path ${RESULT_DIR}/result.ndjson


# TODO: other frameworks (Ray (?), PySpark (?), Pandas (?), lithops (?))