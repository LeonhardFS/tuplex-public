CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++17
SET(CMAKE_CXX_STANDARD 17)

include(GoogleTest)

ADD_EXECUTABLE(testworker main.cc BasicInvocation.cc nextconf/FlightsQueryTest.cc nextconf/GithubQueryTest.cc nextconf/ServiceRequestsQueryTest.cc nextconf/QueriesTest.cc nextconf/SparseGithubTest.cc
        nextconf/JsonSparseParse.cc)
target_include_directories(testworker PUBLIC "../../worker/include")
# find_package(pybind11 REQUIRED)

find_package(absl REQUIRED)

TARGET_LINK_LIBRARIES(testworker
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
add_dependencies(testworker ${WORKER_NAME} runtime)

gtest_add_tests(TARGET testworker TEST_PREFIX "")