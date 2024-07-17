#!/usr/bin/env bash
# (c) 2024 L.Spiegelberg
# collects script invocations required to produce graphs for flight experiments

# Contains a pure C++ version with a perfect sparse struct type to check how fast it is compared to python baseline.

set -e pipefail

# get shell script location
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $BASE_DIR

BUILD_DIR=$BASE_DIR/build

echo $BUILD_DIR
mkdir -p $BUILD_DIR
TUPLEX_DIR=$BUILD_DIR/../../../../../tuplex

# Unix
if [[ "$OSTYPE" =~ ^linux ]]; then
    LLVM_DIR=/opt/llvm-16.0.6
    cd $BUILD_DIR && cmake -DCMAKE_BUILD_TYPE=Release -DSKIP_AWS_TESTS=ON -DBUILD_WITH_ORC=ON -DAWS_S3_TEST_BUCKET='tuplex-test' -DLLVM_ROOT_DIR=$LLVM_DIR $TUPLEX_DIR && make -j$(nproc) tuplex_github && cd ..
fi

# mac os
if [[ "$OSTYPE" =~ ^darwin ]]; then
    LLVM_DIR=/usr/local/Cellar/llvm/16.0.3
    cd $BUILD_DIR && cmake -DPYTHON3_VERSION=3.11 -DCMAKE_BUILD_TYPE=Release -DSKIP_AWS_TESTS=ON -DBUILD_WITH_ORC=ON -DAWS_S3_TEST_BUCKET='tuplex-test' -DLLVM_ROOT_DIR=$LLVM_DIR $TUPLEX_DIR && make -j$(nproc) tuplex_github && cd ..
fi

PROG=./build/dist/bin/tuplex_github
INPUT_PATTERN='/hot/data/github_daily/*.json'
${PROG} --help

# helper function
run_benchmarks() {

  echo ">>> Running Tuplex C++ (best sparse struct)"
  ${PROG} -m "best" --input-pattern "${INPUT_PATTERN}" --output-path "./local-exp/tuplex-c++/github/best/output" --result-path "./local-exp/tuplex-c++/github/best_results.csv"

}

# Run python baseline experiment once (to compare)
python3 runtuplex-new.py --mode python --input-pattern "/hot/data/github_daily/*.json" --output-path "./local-exp/python-baseline/github/output" --scratch-dir "./local-exp/scratch" --result-path "./local-exp/python-baseline/github/results.ndjson"

# run all benchmarks once
run_benchmarks

## Validating results
echo ">>> Validating python baseline vs. C++ (best)"
python3 validate.py "./local-exp/python-baseline/github/output" "./local-exp/tuplex-c++/github/best/output"

echo "validation succeeded!"


# run a couple runs here
NUM_RUNS=5

for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "-- RUN ${r}/${NUM_RUNS}"

  run_benchmarks
done
