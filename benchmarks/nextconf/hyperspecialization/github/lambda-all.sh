#!/usr/bin/env bash

# More detailed debugging
set -euxo pipefail

PYTHON=python3

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