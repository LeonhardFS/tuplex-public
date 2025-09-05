#!/usr/bin/env bash
# This script installs all required dependencies via brew
# for instructions on how to install brew, visit https://brew.sh/

set -euxo pipefail

# Uninstall conflicting cmake first if it exists from a different tap
echo "Checking for conflicting cmake installations..."
brew uninstall cmake --ignore-dependencies || true

# brew doesn't provide llvm@16 bottle anymore for big sur, but python3.8 only works with big sur tags. use llvm@15 instead
echo "Installing dependencies..."
brew install openjdk@11 cmake coreutils protobuf zstd zlib libmagic llvm@15 pcre2 gflags yaml-cpp celero wget boost googletest libdwarf libelf

# link (when e.g. used from restoring cache)
echo "Linking packages..."
brew link --overwrite cmake coreutils protobuf zstd zlib libmagic llvm@15 pcre2 gflags yaml-cpp celero wget boost googletest libdwarf libelf abseil

# Verify protobuf installation
echo "Verifying protobuf installation..."
protoc --version || echo "Warning: protoc not found in PATH"
find /opt/homebrew /usr/local -name "protobuf*" -type d 2>/dev/null | head -5

echo "Done!"