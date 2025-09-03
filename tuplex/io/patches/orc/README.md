# ORC Patches

This directory contains patches for the Apache ORC library to fix compatibility issues.

## 0001-fix-protobuf-int64-compatibility.patch

**Purpose**: Fixes protobuf compatibility issues when building ORC with newer protobuf versions (3.0+)

**Problem**: Newer protobuf versions removed the deprecated `google::protobuf::int64` type, causing compilation errors in ORC.

**Solution**: Replaces all occurrences of `google::protobuf::int64` with the standard `int64_t` type and adds `#include <cstdint>` to the necessary header files.

**Files Modified**:
- `c++/src/io/InputStream.hh` - Added `#include <cstdint>` and updated method signature
- `c++/src/io/InputStream.cc` - Updated implementation to use `int64_t`
- `c++/src/io/OutputStream.hh` - Added `#include <cstdint>` and updated method signature  
- `c++/src/io/OutputStream.cc` - Updated implementation to use `int64_t`

**Applied**: Automatically applied during CMake build via `PATCH_COMMAND` in the ORC ExternalProject_Add configuration.

**Compatibility**: This patch allows ORC to compile successfully with protobuf 3.0+ while maintaining backward compatibility.
