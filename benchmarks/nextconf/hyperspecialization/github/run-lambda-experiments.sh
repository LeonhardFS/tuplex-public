#!/usr/bin/env bash
# (c) 2024 L.Spiegelberg
# Script to run lambda experiments, requires working awscli and AWS keys.

# more detailed debugging
set -euxo pipefail

# get shell script location
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $BASE_DIR

BUILD_DIR=$BASE_DIR/build
ALT_BUILD_DIR=$BASE_DIR/alt-build

PYTHON=python3
DESIRED_PYTHON_VERSION=3.11


echo "-- Tuplex on Lambda benchmarking -- "

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
    cd $BUILD_DIR && cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_WITH_CEREAL=ON -DPython3_EXECUTABLE=$PYTHON3_EXECUTABLE -DUSE_YYJSON_INSTEAD=ON -DSKIP_AWS_TESTS=ON -DBUILD_WITH_ORC=ON -DAWS_S3_TEST_BUCKET='tuplex-test' -DLLVM_ROOT_DIR=$LLVM_DIR $TUPLEX_DIR && make -j${N_PROCESSORS} tuplex && make -j${N_PROCESSORS} tuplex-worker && cd ..
fi

# mac os
if [[ "$OSTYPE" =~ ^darwin ]]; then
    LLVM_DIR=/usr/local/Cellar/llvm/16.0.3
    echo "ERROR: need to update cmake here."
    exit 1
    #cd $BUILD_DIR && cmake -DPYTHON3_VERSION=3.11 -DCMAKE_BUILD_TYPE=Release -DSKIP_AWS_TESTS=ON -DBUILD_WITH_ORC=ON -DAWS_S3_TEST_BUCKET='tuplex-test' -DLLVM_ROOT_DIR=$LLVM_DIR $TUPLEX_DIR && make -j$(nproc) tuplex tuplex-worker && cd ..
fi

echo "-- Built Tuplex."

# Create wheel from compiled tuplex version.
echo ">>> Creating whl file from compiled Tuplex python folder."
${PYTHON} ${BUILD_DIR}/dist/python/setup.py bdist_wheel

WHL_FILE=$(ls ${BUILD_DIR}/dist/python/dist/*.whl)
echo ">>> Installing tuplex (force) from wheel ${WHL_FILE}"
${PYTHON} -m pip install --upgrade --force-reinstall ${WHL_FILE}

# There's an issue with an old cloudpickle version, remove and install newer one
${PYTHON} -m pip uninstall -y cloudpickle && ${PYTHON} -m pip install "cloudpickle>=3.0"

# Before running benchmarks, update Lambda. This will also kill existing containers.
echo ">>> Building Tuplex Lambda and deploying it."
ADD_ZIP_ARGS="--no-libc --with-upx" $TUPLEX_DIR/../scripts/create_lambda_zip.sh
${PYTHON} $TUPLEX_DIR/../deploy.py
echo "-- Tuplex deployed to AWS account."

# start benchmarking, first single run + validate results.
${PYTHON} runtuplex-new.py --help

RESULT_DIR=./lambda-exp/github/
OUTPUT_PATH=${RESULT_DIR}/output
mkdir -p ${RESULT_DIR}

LAMBDA_PARALLELISM=100
LAMBDA_SIZE=1536

run_benchmarks() {
  run=$1
  LAMBDA_ARGS="--lambda --lambda-parallelism ${LAMBDA_PARALLELISM} --lambda-size ${LAMBDA_SIZE}"


  echo ">>> Running tuplex on LAMBDA with interpreter only"
  mode=python
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --mode python --no-hyper --input-pattern "${INPUT_PATTERN}" --output-path ${RESULT_DIR}/output/${mode} \
                             --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                             --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson

  echo ">>> Running tuplex on LAMBDA with no hyper, no sparse structs"
  mode=tuplex-global-structs
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --mode tuplex --no-hyper --input-pattern "${INPUT_PATTERN}" --output-path ${RESULT_DIR}/output/${mode} \
                             --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                             --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson

  echo ">>> Running tuplex on LAMBDA with no hyper, generic dicts"
  mode=tuplex-global-generic-dicts
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --mode tuplex --no-hyper --generic-dicts --input-pattern "${INPUT_PATTERN}" --output-path ${RESULT_DIR}/output/${mode} \
                            --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                            --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson

  echo ">>> Running tuplex on LAMBDA with hyper, generic dicts"
  mode=tuplex-hyper-generic-dicts
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --mode tuplex --no-hyper --generic-dicts --input-pattern "${INPUT_PATTERN}" --output-path ${RESULT_DIR}/output/${mode} \
                            --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                            --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson

  echo ">>> Running tuplex on LAMBDA with no hyper, sparse structs"
  mode=tuplex-global-sparse-structs
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --mode tuplex --no-hyper --sparse-structs --input-pattern "${INPUT_PATTERN}" --output-path ${RESULT_DIR}/output/${mode} \
                            --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                            --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson

  echo ">>> Running tuplex on LAMBDA with hyper, sparse structs"
  mode=tuplex-hyper-sparse-structs
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --mode tuplex --sparse-structs --input-pattern "${INPUT_PATTERN}" --output-path ${RESULT_DIR}/output/${mode} \
                            --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                            --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson

}

# Run all benchmarks once (run 0 is validation run)
run_benchmarks 0

## Validate results
#echo ">>> Validating python baseline vs. tuplex with no hyper, no sparse structs"
#python3 validate.py "${RESULT_DIR}/output/python" "${RESULT_DIR}/output/tuplex-global-structs"
#
#echo ">>> Validating python baseline vs. tuplex with no hyper, generic dicts"
#python3 validate.py "${RESULT_DIR}/output/python" "${RESULT_DIR}/output/tuplex-global-generic-dicts"
#
#echo ">>> Validating python baseline vs. tuplex with hyper, generic dicts"
#python3 validate.py "${RESULT_DIR}/output/python" "${RESULT_DIR}/output/tuplex-hyper-generic-dicts"
#
#echo ">>> Validating python baseline vs. tuplex with no hyper, sparse structs"
#python3 validate.py "${RESULT_DIR}/output/python" "${RESULT_DIR}/output/tuplex-global-sparse-structs"
#
#echo ">>> Validating python baseline vs. tuplex with hyper, sparse structs"
#python3 validate.py "${RESULT_DIR}/output/python" "${RESULT_DIR}/output/tuplex-hyper-sparse-structs"

# Actual benchmark now.

# Run a couple runs here.
NUM_RUNS=${NUM_RUNS:-5}

for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "-- RUN ${r}/${NUM_RUNS}"

  run_benchmarks $r
done

## Once this is done, run plotting scripts to generate all sorts of paper plots.
#
## First, combine results into single ndjson file
#./combine-results.py ${RESULT_DIR}/results/ ${RESULT_DIR}/combined.ndjson
#
## Second, run plots
#./make-plots.py ${RESULT_DIR}/combined.ndjson ${RESULT_DIR}/plots/
#
## TODO: other frameworks (Ray (?), PySpark (?), Pandas (?), lithops (?))