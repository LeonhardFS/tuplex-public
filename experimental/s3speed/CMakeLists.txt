
CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)
project(s3speed)

# enable c++14
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(SRCS main.cc md5.cc)

# set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -march=native -Wall")

# find zlib
find_package(ZLIB REQUIRED)

# Check if local AWS SDK is available.
find_package(AWSSDK COMPONENTS core s3 s3-crt)
if(AWSSDK_FOUND)
    Message(STATUS "Found local AWS SDK with AWSSDK_LINK_LIBRARIES: ${AWSSDK_LINK_LIBRARIES}")
    set(AWS_LINK_LIBRARIES ${AWSSDK_LINK_LIBRARIES})
else()
    # Build AWS SDK so version is locked.
    include("cmake/awssdk.cmake")
endif()

# linking
add_executable(s3speed ${SRCS})

# enable export symbols in debug for backtrace
set_target_properties(s3speed PROPERTIES ENABLE_EXPORTS 1)

target_link_libraries(s3speed PRIVATE ${AWS_LINK_LIBRARIES})
target_compile_features(s3speed PRIVATE "cxx_std_17")
