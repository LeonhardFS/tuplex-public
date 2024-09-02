# Integration tests.

CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++17
SET(CMAKE_CXX_STANDARD 17)

include(GoogleTest)
FILE(GLOB SRCS *.cc)
ADD_EXECUTABLE(testinteg ${SRCS})
target_include_directories(testinteg PUBLIC "../../worker/include")
# find_package(pybind11 REQUIRED)

find_package(absl REQUIRED)

TARGET_LINK_LIBRARIES(testinteg
        libutils
        libio
        libcodegen
        libcore
        libcpythonadapter
        ${GTest_LIBRARIES}
        ${AWSSDK_LINK_LIBRARIES}
        ${Python3_LIBRARIES}
        pybind11::embed
        absl::absl_check
        absl::absl_log
        absl::debugging
        absl::failure_signal_handler
)

message(STATUS "Building tests for worker ${WORKER_NAME}")
# mark worker & runtime as dependency so they get build
add_dependencies(testinteg ${WORKER_NAME} runtime)

gtest_add_tests(TARGET testinteg TEST_PREFIX "")