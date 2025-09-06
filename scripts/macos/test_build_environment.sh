#!/usr/bin/env bash
# Comprehensive test script for macOS build environment
# This script verifies that all dependencies are properly installed and accessible

set -euxo pipefail

echo "=== macOS Build Environment Test ==="
echo "Timestamp: $(date)"
echo "Architecture: $(uname -m)"
echo "macOS version: $(sw_vers -productVersion)"
echo "Current user: $(whoami)"
echo "Working directory: $(pwd)"

# Function to test command availability and version
test_command() {
    local cmd="$1"
    local min_version="$2"
    local version_cmd="$3"
    
    echo "Testing $cmd..."
    
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "❌ $cmd not found in PATH"
        return 1
    fi
    
    local version_output
    if version_output=$($version_cmd 2>&1); then
        echo "✅ $cmd found: $version_output"
        
        if [ -n "$min_version" ]; then
            # Simple version comparison (works for most tools)
            if [[ "$version_output" =~ $min_version ]]; then
                echo "✅ $cmd version meets requirements"
            else
                echo "⚠️  $cmd version may not meet requirements (expected: $min_version, got: $version_output)"
            fi
        fi
        return 0
    else
        echo "❌ $cmd found but version check failed: $version_output"
        return 1
    fi
}

# Function to test library availability
test_library() {
    local lib_name="$1"
    local search_paths="$2"
    local pkg_config_name="$3"
    
    echo "Testing $lib_name library..."
    
    # Test via pkg-config if available
    if [ -n "$pkg_config_name" ] && command -v pkg-config >/dev/null 2>&1; then
        if pkg-config --exists "$pkg_config_name" 2>/dev/null; then
            echo "✅ $lib_name found via pkg-config"
            echo "  Version: $(pkg-config --modversion "$pkg_config_name")"
            echo "  Cflags: $(pkg-config --cflags "$pkg_config_name")"
            echo "  Libs: $(pkg-config --libs "$pkg_config_name")"
            return 0
        fi
    fi
    
    # Test via file search
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

# Function to test CMake package
test_cmake_package() {
    local package_name="$1"
    local cmake_name="$2"
    
    echo "Testing CMake package: $package_name"
    
    # Create a temporary CMakeLists.txt to test package finding
    cat > /tmp/test_cmake_${package_name}.cmake << EOF
cmake_minimum_required(VERSION 3.16)
project(test_${package_name})
find_package(${cmake_name} REQUIRED)
message(STATUS "Found ${cmake_name}: \${${cmake_name}_VERSION}")
EOF
    
    if cmake -P /tmp/test_cmake_${package_name}.cmake 2>/dev/null; then
        echo "✅ CMake package $package_name found"
        rm -f /tmp/test_cmake_${package_name}.cmake
        return 0
    else
        echo "❌ CMake package $package_name not found"
        rm -f /tmp/test_cmake_${package_name}.cmake
        return 1
    fi
}

# Function to test compilation
test_compilation() {
    local test_name="$1"
    local source_code="$2"
    local compile_cmd="$3"
    
    echo "Testing $test_name compilation..."
    
    # Create temporary source file
    local temp_file="/tmp/test_${test_name}.cpp"
    echo "$source_code" > "$temp_file"
    
    if eval "$compile_cmd" 2>/dev/null; then
        echo "✅ $test_name compilation successful"
        rm -f "$temp_file" "/tmp/test_${test_name}"
        return 0
    else
        echo "❌ $test_name compilation failed"
        rm -f "$temp_file" "/tmp/test_${test_name}"
        return 1
    fi
}

echo "=== Testing Essential Commands ==="

# Test basic commands
test_command "brew" "" "brew --version"
test_command "cmake" "3.16" "cmake --version"
test_command "protoc" "libprotoc" "protoc --version"
test_command "java" "openjdk" "java -version"
test_command "pkg-config" "" "pkg-config --version"
test_command "python3" "3.9" "python3 --version"
test_command "pip3" "" "pip3 --version"

echo "=== Testing Libraries ==="

# Test protobuf
test_library "protobuf" "/opt/homebrew/lib /usr/local/lib" "protobuf"

# Test boost
test_library "boost" "/opt/homebrew/lib /usr/local/lib" "boost"

# Test LLVM
test_library "llvm" "/opt/homebrew/lib /usr/local/lib" "llvm"

# Test other libraries
test_library "zstd" "/opt/homebrew/lib /usr/local/lib" "libzstd"
test_library "pcre2" "/opt/homebrew/lib /usr/local/lib" "libpcre2-8"

echo "=== Testing CMake Packages ==="

# Test CMake package finding
test_cmake_package "protobuf" "Protobuf"
test_cmake_package "boost" "Boost"
test_cmake_package "llvm" "LLVM"

echo "=== Testing Compilation ==="

# Test protobuf compilation
test_compilation "protobuf" \
"#include <google/protobuf/message.h>
#include <iostream>
int main() {
    std::cout << \"Protobuf test successful\" << std::endl;
    return 0;
}" \
"g++ -std=c++17 -I/opt/homebrew/include -I/usr/local/include -L/opt/homebrew/lib -L/usr/local/lib -lprotobuf $temp_file -o /tmp/test_protobuf"

# Test boost compilation
test_compilation "boost" \
"#include <boost/version.hpp>
#include <iostream>
int main() {
    std::cout << \"Boost version: \" << BOOST_VERSION << std::endl;
    return 0;
}" \
"g++ -std=c++17 -I/opt/homebrew/include -I/usr/local/include -L/opt/homebrew/lib -L/usr/local/lib $temp_file -o /tmp/test_boost"

echo "=== Testing Environment Variables ==="

# Test important environment variables
echo "PATH: $PATH"
echo "PKG_CONFIG_PATH: ${PKG_CONFIG_PATH:-not set}"
echo "CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH:-not set}"
echo "LD_LIBRARY_PATH: ${LD_LIBRARY_PATH:-not set}"

# Test brew paths
echo "Brew prefix: $(brew --prefix 2>/dev/null || echo 'not found')"
echo "Brew cellar: $(brew --cellar 2>/dev/null || echo 'not found')"

echo "=== Testing Python Environment ==="

# Test Python packages
python3 -c "
import sys
print(f'Python version: {sys.version}')
print(f'Python executable: {sys.executable}')

# Test required packages
required_packages = ['cloudpickle', 'numpy']
for pkg in required_packages:
    try:
        module = __import__(pkg)
        print(f'✅ {pkg}: {module.__version__}')
    except ImportError as e:
        print(f'❌ {pkg}: {e}')
"

echo "=== Testing CMake Configuration ==="

# Test CMake configuration with our project
if [ -f "tuplex/CMakeLists.txt" ]; then
    echo "Testing CMake configuration..."
    
    # Create a test build directory
    mkdir -p /tmp/cmake_test_build
    cd /tmp/cmake_test_build
    
    # Test basic CMake configuration
    if cmake -DCMAKE_BUILD_TYPE=Debug -DBUILD_WITH_AWS=ON -DBUILD_WITH_ORC=ON /Users/runner/work/tuplex/tuplex/tuplex 2>/dev/null; then
        echo "✅ CMake configuration successful"
    else
        echo "❌ CMake configuration failed"
        echo "CMake error output:"
        cmake -DCMAKE_BUILD_TYPE=Debug -DBUILD_WITH_AWS=ON -DBUILD_WITH_ORC=ON /Users/runner/work/tuplex/tuplex/tuplex 2>&1 | head -20
    fi
    
    cd - >/dev/null
    rm -rf /tmp/cmake_test_build
else
    echo "⚠️  tuplex/CMakeLists.txt not found, skipping CMake test"
fi

echo "=== Final Summary ==="
echo "Build environment test completed at $(date)"

# Count successes and failures
echo "Test results summary:"
echo "✅ Successful tests: $(grep -c '✅' <<< "$(cat)")"
echo "❌ Failed tests: $(grep -c '❌' <<< "$(cat)")"
echo "⚠️  Warnings: $(grep -c '⚠️' <<< "$(cat)")"
