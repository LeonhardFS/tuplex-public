CMAKE_MINIMUM_REQUIRED(VERSION 3.19 FATAL_ERROR)

# enable c++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

## external libraries

# LLVM
# list to reduce size of shared object. Compared to linking against all LLVM components, this saves about ~10MB.
# from https://github.com/llvm-mirror/llvm/blob/master/cmake/modules/LLVM-Config.cmake#L218?
# for minimum JIT these components are recommended:
#    core
#    executionengine
#    native
#    object
#    orcjit
#    runtimedyld
#    support
# this may make it easier but increases size of shared object tremendously
set(LLVM_REQUIRED_COMPONENTS core orcjit nativecodegen native scalaropts objcarcopts passes)

IF(BREW_FOUND)
    IF(APPLE)

        # there might be multiple LLVM versions installed.
        # check which version there is
        # if not sys.stdin.isatty():
        #    data = sys.stdin.readlines()
        if(LLVM_ROOT_DIR)
            message(STATUS "Given LLVM root directory: ${LLVM_ROOT_DIR}")
            # check if llvm-config is available in directory
            if(NOT EXISTS "${LLVM_ROOT_DIR}/bin/llvm-config")
                message(FATAL_ERROR "could not find llvm-config in ${LLVM_ROOT_DIR}/bin/llvm-config")
            endif()
            set(BREW_LLVM_NOTFOUND 0)
        else()
            # could use brew prefix here, but let's leave it like this
            EXECUTE_PROCESS(COMMAND bash "-c" "brew info llvm | grep Cellar | cut -d ' ' -f 1" OUTPUT_VARIABLE LLVM_ROOT_DIR RESULT_VARIABLE BREW_LLVM_NOTFOUND OUTPUT_STRIP_TRAILING_WHITESPACE)
        endif()

        IF(NOT BREW_LLVM_NOTFOUND EQUAL "0")
            MESSAGE(WARNING "did not find LLVM, you might install it via `brew install llvm`")
        ELSE()
            # check version, needs to be within 5 and 9 incl.
            # i.e. execute something like /usr/local/opt/llvm/bin/llvm-config --version
            EXECUTE_PROCESS(COMMAND ${LLVM_ROOT_DIR}/bin/llvm-config --version OUTPUT_VARIABLE LLVM_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)

            # check if empty, if it is parse again using brew info json
            IF("${LLVM_VERSION}" STREQUAL "")
                EXECUTE_PROCESS(COMMAND bash "-c" "brew info --json=v1 llvm | python3 -c 'import sys,json; x=json.load(sys.stdin); print(x[0][\"versions\"][\"stable\"])'" OUTPUT_VARIABLE LLVM_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)
                execute_process(COMMAND bash "-c" "brew info llvm | grep Cellar | cut -d ' ' -f 1" OUTPUT_VARIABLE LLVM_ROOT_DIR RESULT_VARIABLE BREW_RET OUTPUT_STRIP_TRAILING_WHITESPACE)
            ENDIF()

            message(STATUS "Found LLVM ${LLVM_VERSION}")
        ENDIF()

    ELSEIF(UNIX)
        # ...
    ENDIF()
ENDIF()

# for brewed llvm, add to cmakemodulepath
IF(NOT "${LLVM_ROOT_DIR}" STREQUAL "")
    message(STATUS "Detected LLVM root dir: ${LLVM_ROOT_DIR}")
    # make cmake find in config mode the right LLVMConfig.cmake file which is located here
    set(LLVM_DIR "${LLVM_ROOT_DIR}/lib/cmake/llvm")
    FIND_PACKAGE(LLVM 9.0 REQUIRED COMPONENTS ${LLVM_REQUIRED_COMPONENTS})
ELSE()
    FIND_PACKAGE(LLVM 9.0 REQUIRED COMPONENTS ${LLVM_REQUIRED_COMPONENTS})
ENDIF()

MESSAGE(STATUS "Found LLVM ${LLVM_VERSION_STRING}")
if(LLVM_DIR)
    message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
endif()
MESSAGE(STATUS "Found LLVM include dirs at: " ${LLVM_INCLUDE_DIRS})
MESSAGE(STATUS "LLVM library dir: ${LLVM_LIBRARY_DIRS}")
message(STATUS "Requiring following LLVM components: ${LLVM_REQUIRED_COMPONENTS}")
llvm_map_components_to_libnames(LLVM_LIBRARIES ${LLVM_REQUIRED_COMPONENTS})
set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LLVM_LIBRARY_DIRS})

include_directories(${LLVM_INCLUDE_DIRS})
add_definitions(${LLVM_DEFINITIONS})

# special case on Unix, make use rtti gets the right flag for the build
if (NOT LLVM_ENABLE_RTTI)
        message(WARNING
                "This build configuration is not supported and will likely not work."
                "You should recompile LLVM with RTTI enabled.")
endif()

# BOOST libs
include_directories(${Boost_INCLUDE_DIR})

# ANTLR4 Runtime (installed e.g. via brew install antlr4-cpp-runtime)
find_package(ANTLR4Runtime REQUIRED)

# Source code & linking
file(GLOB_RECURSE SOURCES src/*.cc)

# antlr target
add_definitions(-DANTLR4CPP_STATIC)
set(ANTLR4_WITH_STATIC_CRT OFF)
include(ExternalAntlr4Cpp)
include_directories(${ANTLR4_INCLUDE_DIRS})
set(ANTLR_EXECUTABLE ${CMAKE_CURRENT_SOURCE_DIR}/tools/antlr-4.13.1-complete.jar)
find_package(ANTLR ${ANTLR4Runtime_VERSION})

# if package fails, try to download proper antlr4 tool
if(NOT ANTLR_FOUND)
    set(ANTLR_TOOL_URL "https://www.antlr.org/download/antlr-${ANTLR4Runtime_VERSION}-complete.jar")
    message(STATUS "Downloading compatible ANTLR tool from ${ANTLR_TOOL_URL}")
    file(DOWNLOAD ${ANTLR_TOOL_URL} ${CMAKE_CURRENT_SOURCE_DIR}/tools/antlr-${ANTLR4Runtime_VERSION}-complete.jar SHOW_PROGRESS)
    set(ANTLR_EXECUTABLE ${CMAKE_CURRENT_SOURCE_DIR}/tools/antlr-${ANTLR4Runtime_VERSION}-complete.jar)
    # run again, this time in required mode
    find_package(ANTLR ${ANTLR4Runtime_VERSION} REQUIRED)
endif()


# check that antlr and antlr target version are compatible -> if not, abort.
message(STATUS "Antlr4 runtime version ${ANTLR4Runtime_VERSION}")
message(STATUS "Antlr4 version ${ANTLR4_VERSION}")

if(NOT ANTLR4Runtime_VERSION VERSION_EQUAL ANTLR4_VERSION)
    message(FATAL_ERROR "Antlr versions not compatible, runtime is ${ANTLR4Runtime_VERSION} but antlr tool is ${ANTLR4_VERSION}")
endif()

antlr_target(Python3Grammar ${CMAKE_CURRENT_SOURCE_DIR}/grammar/Python3.g4 OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/managed PACKAGE antlr4 LISTENER VISITOR)

# enable rtti and exceptions
ucm_add_flags("-fexceptions -frtti")

add_library(libcodegen ${LIBRARY_MODE}
        ${CMAKE_CURRENT_BINARY_DIR} ${SOURCES} ${ANTLR_Python3Grammar_CXX_OUTPUTS})
set_target_properties(libcodegen PROPERTIES PREFIX ""
        LINK_FLAGS "${LLVM_LDFLAGS}")

# use precompiled headers for this target to decrease the cereal template instantiation cost
if(USE_PRECOMPILED_HEADERS)
    file(GLOB_RECURSE VISITORS_HEADERS include/visitors/*.h)
    file(GLOB_RECURSE AST_HEADERS include/ast/*.h)
    file(GLOB_RECURSE SYMBOLS_HEADERS include/symbols/*.h)
    set(CODEGEN_HEADERS ${VISITORS_HEADERS} ${AST_HEADERS} ${SYMBOLS_HEADERS})
    message(STATUS "headers to precompile: ${CODEGEN_HEADERS}")
    target_precompile_headers(libcodegen PUBLIC ${CODEGEN_HEADERS})
endif()
# Specify here the include directories exported
# by this library
target_include_directories(libcodegen PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        ${LLVM_INCLUDE_DIRS}
        ${CMAKE_CURRENT_SOURCE_DIR}/managed
        ${Boost_INCLUDE_DIR}
        ${ANTLR4Runtime_INCLUDE_DIR}
        ${PCRE2_INCLUDE_DIRS}
        )

ASSERT_VAR(LLVM_LIBRARIES)

# Declare the library
target_link_libraries(libcodegen
        PUBLIC
        libutils
        PRIVATE
        ${FFI_LIBRARIES}
        ${PCRE2_LIBRARIES}
        ${ANTLR4Runtime_LIB}
        ${AWSSDK_LINK_LIBRARIES}
        ${PCRE2_LIBRARIES}
        ${LLVM_LIBRARIES}
        ${ZLIB_LIBRARIES}
        ${CURSES_LIBRARIES}
        ${LLVM_LIBRARIES}
        )