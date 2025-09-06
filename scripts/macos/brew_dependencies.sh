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
echo "Current working directory: $(pwd)"
echo "User: $(whoami)"
echo "Home directory: $HOME"

# Update brew first
echo "Updating brew..."
brew update || echo "Warning: brew update failed"

# Uninstall conflicting cmake first if it exists from a different tap
echo "Checking for conflicting cmake installations..."
brew uninstall cmake --ignore-dependencies || true

# Install dependencies with better error handling
echo "Installing dependencies..."

# Install core dependencies first
CORE_DEPENDENCIES="openjdk@11 cmake coreutils zstd zlib libmagic pcre2 gflags yaml-cpp celero wget googletest libdwarf libelf"

for dep in $CORE_DEPENDENCIES; do
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

# Install boost separately with special handling
echo "Installing boost..."
if brew install boost; then
    echo "✅ boost installed successfully"
else
    echo "❌ Failed to install boost"
    echo "Attempting to reinstall boost..."
    brew reinstall -f boost || {
        echo "❌ Failed to reinstall boost, trying alternative approach..."
        # Try installing boost with specific options
        brew install boost --build-from-source || {
            echo "❌ Failed to install boost from source, continuing..."
        }
    }
fi

# Install LLVM separately (can be problematic)
echo "Installing llvm@15..."
if brew install llvm@15; then
    echo "✅ llvm@15 installed successfully"
else
    echo "❌ Failed to install llvm@15"
    echo "Attempting to reinstall llvm@15..."
    brew reinstall -f llvm@15 || {
        echo "❌ Failed to reinstall llvm@15, continuing..."
    }
fi

# Install protobuf last (often has issues)
echo "Installing protobuf..."
if brew install protobuf; then
    echo "✅ protobuf installed successfully"
else
    echo "❌ Failed to install protobuf"
    echo "Attempting to reinstall protobuf..."
    brew reinstall -f protobuf || {
        echo "❌ Failed to reinstall protobuf, continuing..."
    }
fi

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

# Special boost verification
echo "=== Boost verification ==="
if verify_library "boost" "/opt/homebrew/lib /usr/local/lib"; then
    echo "Boost library found"
    # Check for specific boost libraries
    for lib in libboost_system libboost_filesystem libboost_thread libboost_iostreams; do
        if find /opt/homebrew/lib /usr/local/lib -name "${lib}*" 2>/dev/null | grep -q .; then
            echo "✅ $lib found"
        else
            echo "⚠️  $lib not found"
        fi
    done
else
    echo "❌ Boost verification failed"
    echo "Searching for boost installations..."
    find /opt/homebrew /usr/local -name "*boost*" 2>/dev/null | head -10
fi

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

# Test boost compilation
echo "=== Testing boost compilation ==="
cat > /tmp/test_boost.cpp << 'EOF'
#include <boost/version.hpp>
#include <boost/filesystem.hpp>
#include <boost/thread.hpp>
#include <boost/iostreams.hpp>
#include <iostream>

int main() {
    std::cout << "Boost version: " << BOOST_VERSION << std::endl;
    std::cout << "Boost filesystem test: " << boost::filesystem::current_path() << std::endl;
    std::cout << "Boost test successful" << std::endl;
    return 0;
}
EOF

if g++ -std=c++17 -I/opt/homebrew/include -I/usr/local/include -L/opt/homebrew/lib -L/usr/local/lib -lboost_system -lboost_filesystem -lboost_thread -lboost_iostreams /tmp/test_boost.cpp -o /tmp/test_boost && /tmp/test_boost; then
    echo "✅ Boost compilation test passed"
    rm -f /tmp/test_boost.cpp /tmp/test_boost
else
    echo "❌ Boost compilation test failed"
    echo "Trying with different boost libraries..."
    # Try with just system library
    if g++ -std=c++17 -I/opt/homebrew/include -I/usr/local/include -L/opt/homebrew/lib -L/usr/local/lib -lboost_system /tmp/test_boost.cpp -o /tmp/test_boost && /tmp/test_boost; then
        echo "✅ Boost compilation test passed (system library only)"
        rm -f /tmp/test_boost.cpp /tmp/test_boost
    else
        echo "❌ Boost compilation test failed completely"
        rm -f /tmp/test_boost.cpp /tmp/test_boost
        exit 1
    fi
fi

# Final environment summary
echo "=== Environment Summary ==="
echo "PATH: $PATH"
echo "PKG_CONFIG_PATH: ${PKG_CONFIG_PATH:-not set}"
echo "CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH:-not set}"
echo "Protobuf locations:"
find /opt/homebrew /usr/local -name "*protobuf*" 2>/dev/null | head -5

echo "=== Installation completed successfully! ==="