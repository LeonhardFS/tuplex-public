# (c) 2017 Leonhard Spiegelberg
cmake_minimum_required(VERSION 3.16 FATAL_ERROR)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
message(STATUS "Using language versions C++${CMAKE_CXX_STANDARD}")

# include simdjson, tutorial here https://github.com/simdjson/cmake_demo_single_file/blob/master/CMakeLists.txt
include(FetchContent)
FetchContent_Declare(
        simdjson
        GIT_REPOSITORY https://github.com/simdjson/simdjson.git
        GIT_SHALLOW TRUE
        GIT_TAG tags/v3.8.0
)

# FetchContent_MakeAvailable(simdjson)

if(NOT simdjson_POPULATED)
    FetchContent_Populate(simdjson)
    # Disable threads for simdjson
    set(SIMDJSON_ENABLE_THREADS OFF CACHE INTERNAL "Disable dimsjon threads")
    add_subdirectory(${simdjson_SOURCE_DIR} ${simdjson_BINARY_DIR})
endif()


FetchContent_Declare(lyra
        GIT_REPOSITORY https://github.com/bfgroup/Lyra
        GIT_TAG 1.6.1
)
FetchContent_MakeAvailable(lyra)

set(SRCS main.cc)

# linking
add_executable(tuplex_github ${SRCS})
target_link_libraries(tuplex_github simdjson lyra ${AWSSDK_LINK_LIBRARIES})