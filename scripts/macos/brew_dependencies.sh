#!/usr/bin/env bash
# This script installs all required dependencies via brew
# for instructions on how to install brew, visit https://brew.sh/

set -euxo pipefail

# Function to verify a command exists and is working
verify_command() {
    local cmd="$1"
    local expected_output="$2"
    echo "Verifying $cmd..."
    
    if command -v "$cmd" >/dev/null 2>&1; then
        if [ -n "$expected_output" ]; then
            local actual_output
            actual_output=$($cmd --version 2>/dev/null || echo "version check failed")
            echo "✅ $cmd found: $actual_output"
        else
            echo "✅ $cmd found"
        fi
        return 0
    else
        echo "❌ $cmd not found in PATH"
        return 1
    fi
}

# Function to verify library exists
verify_library() {
    local lib_name="$1"
    local search_paths="$2"
    echo "Verifying $lib_name library..."
    
    for path in $search_paths; do
        if find "$path" -name "*${lib_name}*" -type f 2>/dev/null | grep -q .; then
            echo "✅ $lib_name found in $path"
            find "$path" -name "*${lib_name}*" -type f 2>/dev/null | head -3
            return 0
        fi
    done
    
    echo "❌ $lib_name not found in any of: $search_paths"
    return 1
}

# Function to verify cmake package
verify_cmake_package() {
    local package_name="$1"
    echo "Verifying CMake package: $package_name"
    
    if find /opt/homebrew/lib/cmake /usr/local/lib/cmake -name "*${package_name}*" 2>/dev/null | grep -q .; then
        echo "✅ CMake package $package_name found"
        find /opt/homebrew/lib/cmake /usr/local/lib/cmake -name "*${package_name}*" 2>/dev/null | head -3
        return 0
    else
        echo "❌ CMake package $package_name not found"
        return 1
    fi
}

echo "=== Starting macOS dependency installation ==="
echo "Current PATH: $PATH"
echo "Current architecture: $(uname -m)"
echo "macOS version: $(sw_vers -productVersion)"

# Update brew first
echo "Updating brew..."
brew update || echo "Warning: brew update failed"

# Uninstall conflicting cmake first if it exists from a different tap
echo "Checking for conflicting cmake installations..."
brew uninstall cmake --ignore-dependencies || true

# Install dependencies with better error handling
echo "Installing dependencies..."
DEPENDENCIES="openjdk@11 cmake coreutils protobuf zstd zlib libmagic llvm@15 pcre2 gflags yaml-cpp celero wget boost googletest libdwarf libelf"

for dep in $DEPENDENCIES; do
    echo "Installing $dep..."
    if brew install "$dep"; then
        echo "✅ $dep installed successfully"
    else
        echo "❌ Failed to install $dep"
        echo "Attempting to reinstall $dep..."
        brew reinstall -f "$dep" || {
            echo "❌ Failed to reinstall $dep, continuing..."
        }
    fi
done

# Link packages with better error handling
echo "Linking packages..."
LINK_PACKAGES="cmake coreutils protobuf zstd zlib libmagic llvm@15 pcre2 gflags yaml-cpp celero wget boost googletest libdwarf libelf abseil"

for pkg in $LINK_PACKAGES; do
    echo "Linking $pkg..."
    brew link --overwrite "$pkg" || echo "Warning: Failed to link $pkg"
done

# Force reinstall protobuf as it often has issues
echo "Force reinstalling protobuf..."
brew reinstall -f protobuf || {
    echo "❌ Failed to reinstall protobuf"
    exit 1
}

# Update PATH to include brew binaries
echo "Updating PATH..."
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
echo "Updated PATH: $PATH"

# Comprehensive verification
echo "=== Verifying installations ==="

# Verify essential commands
verify_command "protoc" "libprotoc"
verify_command "cmake" "cmake version"
verify_command "java" "openjdk"
verify_command "pkg-config" "pkg-config"

# Verify protobuf specifically
echo "=== Protobuf verification ==="
if verify_command "protoc" "libprotoc"; then
    echo "Protobuf version: $(protoc --version)"
    echo "Protobuf include path: $(pkg-config --variable=includedir protobuf 2>/dev/null || echo 'not found')"
    echo "Protobuf lib path: $(pkg-config --variable=libdir protobuf 2>/dev/null || echo 'not found')"
else
    echo "❌ Protobuf verification failed"
    echo "Searching for protobuf installations..."
    find /opt/homebrew /usr/local -name "*protobuf*" 2>/dev/null | head -10
    exit 1
fi

# Verify libraries
verify_library "protobuf" "/opt/homebrew/lib /usr/local/lib"
verify_library "boost" "/opt/homebrew/lib /usr/local/lib"
verify_library "llvm" "/opt/homebrew/lib /usr/local/lib"

# Verify CMake packages
verify_cmake_package "protobuf"
verify_cmake_package "boost"
verify_cmake_package "llvm"

# Test protobuf compilation
echo "=== Testing protobuf compilation ==="
cat > /tmp/test.proto << 'EOF'
syntax = "proto3";
package test;
message TestMessage {
  string name = 1;
  int32 value = 2;
}
EOF

if protoc --cpp_out=/tmp /tmp/test.proto; then
    echo "✅ Protobuf compilation test passed"
    ls -la /tmp/test.pb.*
else
    echo "❌ Protobuf compilation test failed"
    exit 1
fi

# Final environment summary
echo "=== Environment Summary ==="
echo "PATH: $PATH"
echo "PKG_CONFIG_PATH: ${PKG_CONFIG_PATH:-not set}"
echo "CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH:-not set}"
echo "Protobuf locations:"
find /opt/homebrew /usr/local -name "*protobuf*" 2>/dev/null | head -5

echo "=== Installation completed successfully! ==="