#!/usr/bin/env bash
set -euxo pipefail
aws s3 rm --recursive  s3://tuplex-leonhard/wheels/
aws s3 cp wheelhouse/*.whl s3://tuplex-leonhard/wheels/
