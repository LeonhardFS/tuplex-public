#!/usr/bin/env bash
# (c) L. Spiegelberg 2024
# Flights benchmark w. hyper vs. no hyper

# More detailed debugging
set -euxo pipefail

PYTHON=python3.11

INPUT_PATTERN="s3://tuplex-public/data/flights_all/*.csv"
RESULT_DIR=./lambda-exp/flights/
# Root output path.
OUTPUT_PATH="s3://tuplex-leonhard/experiments/flights"
mkdir -p ${RESULT_DIR}


# How many files are there? -> use aws s3 ls to determine.
# should be 410.
NUM_FILES=$(aws s3 ls s3://tuplex-public/data/flights_all/ | wc -l)
echo "There are $NUM_FILES files."

LAMBDA_PARALLELISM=410 # set to NUM_FILES in case this fails.
LAMBDA_SIZE=1536

validate_benchmarks() {
  # Compare with local reference if exists.
  LOCAL_RESULT_DIR="./local-exp/flights"
  # Check if dir exists, if not - skip validation.
  if [ -d "$LOCAL_RESULT_DIR/output/python" ]; then
    echo ">>> Found local result directory, validating files."

    # Copy S3 output files to local storage to compare with local reference. (Skip if not there)
    echo ">>> Copying S3 results to local folder for validation."
    aws s3 cp --recursive "${OUTPUT_PATH}/" $RESULT_DIR

#    echo ">>> Validating python baseline vs. tuplex with no hyper, no sparse structs"
#    ${PYTHON} validate.py "${LOCAL_RESULT_DIR}/output/python" "${RESULT_DIR}/output/tuplex-global-structs"
#
#    echo ">>> Validating python baseline vs. tuplex with no hyper, generic dicts"
#    ${PYTHON} validate.py "${LOCAL_RESULT_DIR}/output/python" "${RESULT_DIR}/output/tuplex-global-generic-dicts"
#
#    echo ">>> Validating python baseline vs. tuplex with hyper, generic dicts"
#    ${PYTHON} validate.py "${LOCAL_RESULT_DIR}/output/python" "${RESULT_DIR}/output/tuplex-hyper-generic-dicts"
#
#    echo ">>> Validating python baseline vs. tuplex with no hyper, sparse structs"
#    ${PYTHON} validate.py "${LOCAL_RESULT_DIR}/output/python" "${RESULT_DIR}/output/tuplex-global-sparse-structs"
#
#    echo ">>> Validating python baseline vs. tuplex with hyper, sparse structs"
#    ${PYTHON} validate.py "${LOCAL_RESULT_DIR}/output/python" "${RESULT_DIR}/output/tuplex-hyper-sparse-structs"
  else
    echo ">>> Skipping validation, no local dir found. Run run-tuplex-experiments.sh to create local dir."
  fi
}

run_benchmarks() {
  run=$1
  LAMBDA_ARGS="--lambda --lambda-parallelism ${LAMBDA_PARALLELISM} --lambda-size ${LAMBDA_SIZE}"

  # Delete results from S3 (this may take a while).
  echo ">>> Deleting existing results from S3:"
  aws s3 rm --recursive --dryrun "${OUTPUT_PATH}/"
  aws s3 rm --recursive "${OUTPUT_PATH}/"

  echo ">>> Running tuplex on LAMBDA with interpreter only"
  mode=python
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --mode python --no-hyper --input-pattern "${INPUT_PATTERN}" --output-path ${OUTPUT_PATH}/output/${mode} \
                             --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                             --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson

  echo ">>> Running tuplex on LAMBDA hyper with constant-folding"
  mode=tuplex-hyper-cf
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --mode tuplex --input-pattern "${INPUT_PATTERN}" --output-path ${OUTPUT_PATH}/output/${mode} \
                             --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                             --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson

  echo ">>> Running tuplex on LAMBDA global with constant-folding"
  mode=tuplex-global-cf
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --mode tuplex --no-hyper --input-pattern "${INPUT_PATTERN}" --output-path ${OUTPUT_PATH}/output/${mode} \
                             --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                             --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson

  echo ">>> Running tuplex on LAMBDA hyper without constant-folding"
  mode=tuplex-hyper
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --mode tuplex --no-cf --input-pattern "${INPUT_PATTERN}" --output-path ${OUTPUT_PATH}/output/${mode} \
                             --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                             --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson

  echo ">>> Running tuplex on LAMBDA global without constant-folding"
  mode=tuplex-global
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --mode tuplex --no-hyper --no-cf --input-pattern "${INPUT_PATTERN}" --output-path ${OUTPUT_PATH}/output/${mode} \
                             --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                             --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson
}

# Run all benchmarks once (run 0 is validation run)
run_benchmarks 0

#validate_benchmarks

# Run a few benchmarks.
NUM_RUNS=${NUM_RUNS:-1}
echo "Using ${NUM_RUNS}"
for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "-- RUN ${r}/${NUM_RUNS}"

  run_benchmarks $r
done

# Create plots necessary.

# First, combine results into single csv file
#./lambda-combine-results.py ${RESULT_DIR}/results/ ${RESULT_DIR}/combined.csv

## Second, run plots
#./make-plots.py ${RESULT_DIR}/combined.ndjson ${RESULT_DIR}/plots/