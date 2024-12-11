#!/usr/bin/env bash

# More detailed debugging
set -euxo pipefail

PYTHON=python3.11

INPUT_PATTERN="s3://tuplex-public/data/github_daily/*.json"
RESULT_DIR=./lambda-exp/github-push-events/

  # Compare with local reference if exists.
  LOCAL_RESULT_DIR="./local-exp/github-push-events"

# Root output path.
OUTPUT_PATH="s3://tuplex-leonhard/experiments/github"
mkdir -p ${RESULT_DIR}

LAMBDA_PARALLELISM=100
LAMBDA_SIZE=1536

echo "Running push event query (Github)"

validate_benchmarks() {

  # Check if dir exists, if not - skip validation.
  if [ -d "$LOCAL_RESULT_DIR/output/python" ]; then
    echo ">>> Found local result directory, validating files."

      # Copy S3 output files to local storage to compare with local reference. (Skip if not there)
    echo ">>> Copying S3 results to local folder for validation."
    aws s3 cp --recursive "${OUTPUT_PATH}/" $RESULT_DIR

    echo ">>> Validating python baseline vs. tuplex with no hyper, no sparse structs"
    ${PYTHON} validate.py "${LOCAL_RESULT_DIR}/output/python" "${RESULT_DIR}/output/tuplex-global-structs"

    echo ">>> Validating python baseline vs. tuplex with no hyper, generic dicts"
    ${PYTHON} validate.py "${LOCAL_RESULT_DIR}/output/python" "${RESULT_DIR}/output/tuplex-global-generic-dicts"

    echo ">>> Validating python baseline vs. tuplex with hyper, generic dicts"
    ${PYTHON} validate.py "${LOCAL_RESULT_DIR}/output/python" "${RESULT_DIR}/output/tuplex-hyper-generic-dicts"

    echo ">>> Validating python baseline vs. tuplex with no hyper, sparse structs"
    ${PYTHON} validate.py "${LOCAL_RESULT_DIR}/output/python" "${RESULT_DIR}/output/tuplex-global-sparse-structs"

    echo ">>> Validating python baseline vs. tuplex with hyper, sparse structs"
    ${PYTHON} validate.py "${LOCAL_RESULT_DIR}/output/python" "${RESULT_DIR}/output/tuplex-hyper-sparse-structs"
  else
    echo ">>> Skipping validation, no local dir found. Run run-tuplex-experiments.sh to create local dir."
  fi
}

run_benchmarks() {
  run=$1
  LAMBDA_ARGS="--lambda --lambda-parallelism ${LAMBDA_PARALLELISM} --lambda-size ${LAMBDA_SIZE}"

  # Delete results from S3 (this may take a while)
  echo ">>> Deleting existing results from S3:"
  aws s3 rm --recursive --dryrun "${OUTPUT_PATH}/"
  aws s3 rm --recursive "${OUTPUT_PATH}/"

  echo ">>> Running tuplex on LAMBDA with interpreter only"
  mode=python
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --query push --mode python --no-hyper --input-pattern "${INPUT_PATTERN}" --output-path ${OUTPUT_PATH}/output/${mode} \
                             --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                             --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson

  echo ">>> Running tuplex on LAMBDA with no hyper, no sparse structs"
  mode=tuplex-global-structs
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --query push --mode tuplex --no-hyper --input-pattern "${INPUT_PATTERN}" --output-path ${OUTPUT_PATH}/output/${mode} \
                             --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                             --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson

  echo ">>> Running tuplex on LAMBDA with no hyper, generic dicts"
  mode=tuplex-global-generic-dicts
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --query push --mode tuplex --no-hyper --generic-dicts --input-pattern "${INPUT_PATTERN}" --output-path ${OUTPUT_PATH}/output/${mode} \
                            --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                            --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson

  echo ">>> Running tuplex on LAMBDA with hyper, generic dicts"
  mode=tuplex-hyper-generic-dicts
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --query push --mode tuplex --generic-dicts --input-pattern "${INPUT_PATTERN}" --output-path ${OUTPUT_PATH}/output/${mode} \
                            --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                            --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson

  echo ">>> Running tuplex on LAMBDA with no hyper, sparse structs"
  mode=tuplex-global-sparse-structs
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --query push --mode tuplex --no-hyper --sparse-structs --input-pattern "${INPUT_PATTERN}" --output-path ${OUTPUT_PATH}/output/${mode} \
                            --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                            --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson

  echo ">>> Running tuplex on LAMBDA with hyper, sparse structs"
  mode=tuplex-hyper-sparse-structs
  ${PYTHON} runtuplex-new.py ${LAMBDA_ARGS} --query push --mode tuplex --sparse-structs --input-pattern "${INPUT_PATTERN}" --output-path ${OUTPUT_PATH}/output/${mode} \
                            --scratch-dir ${RESULT_DIR}/scratch --log-path ${RESULT_DIR}/results/${mode}/log-run-${run}.txt \
                            --result-path ${RESULT_DIR}/results/${mode}/log-run-${run}.ndjson

}

# Run all benchmarks once (run 0 is validation run)
run_benchmarks 0

validate_benchmarks

# Run a few benchmarks.
NUM_RUNS=${NUM_RUNS:-5}

for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "-- RUN ${r}/${NUM_RUNS}"

  run_benchmarks $r
done

# Create plots necessary.

# First, combine results into single csv file
./lambda-combine-results.py ${RESULT_DIR}/results/ ${RESULT_DIR}/combined.csv
