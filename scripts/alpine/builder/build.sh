#!/bin/bash

# Alpine wheel builder script
# This script will be executed when the container starts

set -e

echo "Alpine wheel builder container started"

# Use Python 3.11 from the tuplex/musl image
PYTHON="/opt/_internal/cpython-3.11.13/bin/python3.11"
PIP="/opt/_internal/cpython-3.11.13/bin/pip"

echo "Using Python: $PYTHON"
echo "Python version: $($PYTHON --version)"

# Check if the project root is mounted
if [ ! -f "/code/setup.py" ]; then
    echo "Error: /code/setup.py not found. Make sure the project root is mounted."
    exit 1
fi

# Check if wheelhouse directory is mounted
if [ ! -d "/wheelhouse" ]; then
    echo "Error: /wheelhouse directory not found. Make sure the wheelhouse is mounted."
    exit 1
fi

echo "Installing required dependencies..."
$PIP install setuptools wheel cloudpickle numpy attrs dill pluggy py pygments six wcwidth astor prompt_toolkit jedi PyYAML psutil pymongo iso8601

export LLVM_ROOT_DIR="/opt/llvm-16.0.6"
export LLVM_CONFIG="/opt/llvm-16.0.6/bin/llvm-config"
export PATH="/opt/llvm-16.0.6/bin:$PATH"


echo "Building wheel for tuplex..."

# Change to the tuplex source directory
cd /code

export CMAKE_ARGS="
   -DCMAKE_BUILD_TYPE=RelWithDebInfo \
   -DBUILD_WITH_AWS=OFF \
   -DBUILD_WITH_ORC=OFF \
   -DBUILD_SHARED_LIBS=ON \
   -DSKIP_AWS_TESTS=ON \
   -DBUILD_FOR_CI=ON \
   -DBUILD_TESTING=OFF \
   -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON \
   -DCMAKE_CXX_FLAGS=\"-O3 -DNDEBUG\" \
   -DCMAKE_C_FLAGS=\"-O3 -DNDEBUG\" \
   -DLLVM_ROOT=/opt/llvm-16.0.6 \
   -DLLVM_ROOT_DIR=/opt/llvm-16.0.6 \
   -DLLVM_CONFIG=/opt/llvm-16.0.6/bin/llvm-config \
   -DPython3_EXECUTABLE=/opt/_internal/cpython-3.11.13/bin/python3.11 \
   -DPython3_ROOT_DIR=/opt/_internal/cpython-3.11.13 \
   -DPython3_LIBRARY=/opt/_internal/cpython-3.11.13/lib/libpython3.11.so \
   -DPython3_INCLUDE_DIR=/opt/python/cp311-cp311/include/python3.11 \
   -DPYTHON3_VERSION=3.11 \
   -DCMAKE_POLICY_VERSION_MINIMUM=3.5
"



# Build the wheel using setup.py
echo "Building wheel with setup.py..."
CMAKE_ARGS="$CMAKE_ARGS" $PYTHON setup.py bdist_wheel

# Find the built wheel
WHEEL_FILE=$(find dist/ -name "*.whl" | head -1)

if [ -z "$WHEEL_FILE" ]; then
    echo "Error: No wheel file found in dist/ directory"
    exit 1
fi

echo "Built wheel: $WHEEL_FILE"

# Use auditwheel to repair/delocate the wheel
echo "Repairing wheel with auditwheel..."
$PIP install auditwheel
auditwheel repair "$WHEEL_FILE" -w /wheelhouse

# Find the repaired wheel
REPAIRED_WHEEL=$(find /wheelhouse -name "*.whl" | head -1)

if [ -z "$REPAIRED_WHEEL" ]; then
    echo "Error: No repaired wheel found in /wheelhouse"
    exit 1
fi

echo "Successfully built and repaired wheel: $REPAIRED_WHEEL"
echo "Wheel stored in: /wheelhouse"

