#!/usr/bin/env bash

echo "Running both Github and flights on Lambda"
set -euxo pipefail

export NUM_RUNS=5

cd flights
./lambda-all.sh
cd ..

cd github
./lambda-all.sh
cd ..
