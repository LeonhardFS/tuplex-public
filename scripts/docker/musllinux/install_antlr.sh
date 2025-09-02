#!/bin/sh
# (c) Tuplex team 2017-2023
# Installs ANTLR4 dependencies required to build tuplex on Alpine Linux.

# Variables needed incl. defaults.
PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}
ANTLR4_VERSION=${ANTLR4_VERSION:-4.13.1}
CC=${CC:-gcc}
CXX=${CXX:-g++}

# Get CPU count for parallel builds
CPU_COUNT=$(( 1 * $( grep '^processor[[:space:]]*:' /proc/cpuinfo | wc -l ) ))

# Start script.
set -euxo pipefail

# need to run this with root privileges
if [ "$(id -u)" -ne 0 ]; then
  echo "Please run this script with root privileges"
  exit 1
fi

echo ">> Installing ANTLR4 ${ANTLR4_VERSION}"

# Create download directory
DOWNLOAD_DIR=${WORKDIR}/tuplex-downloads
mkdir -p ${DOWNLOAD_DIR}/antlr && cd ${DOWNLOAD_DIR}/antlr \
&& curl -O https://www.antlr.org/download/antlr-${ANTLR4_VERSION}-complete.jar \
&& cp antlr-${ANTLR4_VERSION}-complete.jar ${PREFIX}/lib/ \
&& curl -O https://www.antlr.org/download/antlr4-cpp-runtime-${ANTLR4_VERSION}-source.zip \
&& unzip antlr4-cpp-runtime-${ANTLR4_VERSION}-source.zip -d antlr4-cpp-runtime \
&& rm antlr4-cpp-runtime-${ANTLR4_VERSION}-source.zip \
&& cd antlr4-cpp-runtime \
&& mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PREFIX} .. \
&& make -j ${CPU_COUNT}&& make install

echo ">> ANTLR4 ${ANTLR4_VERSION} installation completed successfully"
