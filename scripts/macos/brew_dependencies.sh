#!/usr/bin/env bash
# This script installs all required dependencies via brew
# for instructions on how to install brew, visit https://brew.sh/

# AWS SDK CPP is excluded, because we want to build it with special parameters/configs.
# Same is true for antlr4-runtime cpp.
# If you want to install them, uncomment the following line:
# brew install antlr4-cpp-runtime aws-sdk-cpp

# brew doesn't provide llvm@16 bottle anymore for big sur, but python3.8 only works with big sur tags. use llvm@15 instead
brew install openjdk@11 cmake coreutils protobuf zstd zlib libmagic llvm pcre2 gflags yaml-cpp celero wget boost googletest libdwarf libelf

# link (when e.g. used from restoring cache)
brew link --overwrite cmake coreutils protobuf zstd zlib libmagic llvm pcre2 gflags yaml-cpp celero wget boost googletest libdwarf libelf abseil
