//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TESTUTILS_H
#define TUPLEX_TESTUTILS_H

#include "gtest/gtest.h"
#ifndef GTEST_IS_THREADSAFE
#error "need threadsafe version of google test"
#else
#if (GTEST_IS_THREADSAFE != 1)
#error "need threadsafe version of google test"
#endif
#endif


#include <Row.h>
#include <UDF.h>
#include <Executor.h>
#include <ContextOptions.h>
#include <spdlog/sinks/ostream_sink.h>
#include <LocalEngine.h>
#include <physical/CodeDefs.h>
#include <RuntimeInterface.h>
#include <ClosureEnvironment.h>
#include <Environment.h>

#ifdef BUILD_WITH_AWS
#include <ee/aws/AWSLambdaBackend.h>
#endif

#include <boost/filesystem/operations.hpp>

// helper functions to faciliate test writing
extern tuplex::Row execRow(const tuplex::Row& input, tuplex::UDF udf=tuplex::UDF("lambda x: x"));

extern std::unique_ptr<tuplex::Executor> testExecutor();

inline tuplex::ContextOptions testOptions() {
    using namespace tuplex;
    ContextOptions co = ContextOptions::defaults();
    co.set("tuplex.executorCount", "4");
    co.set("tuplex.partitionSize", "512KB");
    co.set("tuplex.executorMemory", "8MB");
    co.set("tuplex.useLLVMOptimizer", "true");
    co.set("tuplex.allowUndefinedBehavior", "false");
    co.set("tuplex.webui.enable", "false");
#ifdef BUILD_FOR_CI
    co.set("tuplex.aws.httpThreadCount", "0");
#else
    co.set("tuplex.aws.httpThreadCount", "1");
#endif
    return co;
}

inline tuplex::ContextOptions microTestOptions() {
    using namespace tuplex;
    ContextOptions co = ContextOptions::defaults();
    co.set("tuplex.executorCount", "4");
    co.set("tuplex.partitionSize", "256B");
    co.set("tuplex.executorMemory", "4MB");
    co.set("tuplex.useLLVMOptimizer", "true");
//    co.set("tuplex.useLLVMOptimizer", "false");
    co.set("tuplex.allowUndefinedBehavior", "false");
    co.set("tuplex.webui.enable", "false");
    co.set("tuplex.optimizer.mergeExceptionsInOrder", "true"); // force exception resolution for single stages to occur in order

    // disable schema pushdown
    co.set("tuplex.csv.selectionPushdown", "true");

#ifdef BUILD_FOR_CI
    co.set("tuplex.aws.httpThreadCount", "0");
#else
    co.set("tuplex.aws.httpThreadCount", "1");
#endif
    return co;
}

#ifdef BUILD_WITH_AWS
inline tuplex::ContextOptions microLambdaOptions() {
    auto co = microTestOptions();
    co.set("tuplex.backend", "lambda");

    // scratch dir
    co.set("tuplex.aws.scratchDir", std::string("s3://") + S3_TEST_BUCKET + "/.tuplex-cache");

#ifdef BUILD_FOR_CI
    co.set("tuplex.aws.httpThreadCount", "1");
#else
    co.set("tuplex.aws.httpThreadCount", "4");
#endif

    return co;
}
#endif

inline void printRows(const std::vector<tuplex::Row>& v) {
    for(auto r : v)
        std::cout<<"type: "<<r.getRowType().desc()<<" content: "<<r.toPythonString()<<std::endl;
}

inline void compareStrArrays(std::vector<std::string> arr_A, std::vector<std::string> arr_B, bool ignore_order) {
    if(ignore_order) {
        std::sort(arr_A.begin(), arr_A.end());
        std::sort(arr_B.begin(), arr_B.end());
    }

    for (int i = 0; i < std::min(arr_A.size(), arr_B.size()); ++i) {
        EXPECT_EQ(arr_A[i], arr_B[i]);
    }
    ASSERT_EQ(arr_A.size(), arr_B.size());
}

// helper class to not have to write always the python interpreter startup stuff
// need for these tests a running python interpreter, so spin it up
class PyTest : public ::testing::Test {
protected:
    PyThreadState *saveState;
    std::stringstream logStream;

    void SetUp() override {

        // reset global static variables, i.e. whether to use UDF compilation or not!
        tuplex::UDF::enableCompilation();

        // init logger to write to both stream as well as stdout
        // ==> searching the stream can be used to validate err Messages
        Logger::init({std::make_shared<spdlog::sinks::ansicolor_stdout_sink_mt>(),
                std::make_shared<spdlog::sinks::ostream_sink_mt>(logStream)});

        python::initInterpreter();
        // release GIL
        python::unlockGIL();
    }

    void TearDown() override {
        python::lockGIL();
        // important to get GIL for this
        python::closeInterpreter();

        // release runtime memory
        tuplex::runtime::releaseRunTimeMemory();

        // remove all loggers ==> note: this crashed because of multiple threads not being done yet...
        // call only AFTER all threads/threadpool is terminated from Context/LocalBackend/LocalEngine...
        Logger::instance().reset();

        tuplex::UDF::enableCompilation(); // reset

        // check whether exceptions work, LLVM9 has a bug which screws up C++ exception handling in ORCv2 APIs
        try {
            throw std::exception();
        } catch(...) {
            std::cout << "test done." << std::endl;
        }
    }

    inline void remove_temp_files() {
        tuplex::Timer timer;
        // remove the temporary files
        // get path from default context options
        auto temp_cache_path = tuplex::ContextOptions::defaults().SCRATCH_DIR();
        boost::filesystem::remove_all(temp_cache_path.toPath().c_str());

        std::cout<<"removed temp files in "<<timer.time()<<"s"<<std::endl;
    }

    virtual ~PyTest() {
        remove_temp_files();
    }
};



/// a simple wrapper class to call closeInterpreter on destruction
class PyInterpreterGuard {
public:
    PyInterpreterGuard() {
        python::initInterpreter();
        printf("*** CALLED initInterpreter ***\n");
    }
    ~PyInterpreterGuard() {
        printf("*** CALLED closeInterpreter ***\n");
        python::closeInterpreter();
    }
};



// other test macros (https://stackoverflow.com/questions/1460703/comparison-of-arrays-in-google-test)
template<typename T> void EXPECT_IN_VECTOR(const std::vector<T>& v, const T& x) {
    auto it = std::find(v.begin(), v.end(), x);
    ASSERT_TRUE(it != v.end());
}


// often, we need to test for a couple recurring test cases.
inline std::vector<tuplex::Field> primitive_test_values(bool numeric_types=true,
                                                 bool strings=true,
                                                 bool empty_compound_types=true) {
    using namespace tuplex;
    using namespace std;

    vector<Field> v;

    if(strings) {
        // a couple string test cases
        v.push_back(Field(""));
        v.push_back(Field("hello world!"));

        // test string with all printable chars...
        // -> this is helpful when testing for formatting overlap cases...
        std::string printable_chars;
        for(int i = 0; i < 256; ++i) {
            char buf[2] = {'\0', '\0'};
            buf[0] = i;
            if(isprint(i))
                printable_chars += std::string(buf);
        }
        v.push_back(Field(printable_chars));
    }

    if(numeric_types) {
        vector<Field> values{Field((int64_t)-42), Field((int64_t)0), Field((int64_t)42),
        Field(-3.1415965), Field(0.0), Field(3.1415965), Field(INFINITY), Field(-INFINITY), Field(NAN),
                Field(true), Field(false)};

        std::copy(values.begin(), values.end(), std::back_inserter(v));
    }

    if(empty_compound_types) {
        v.push_back(Field::null());
        v.push_back(Field::empty_tuple());
        v.push_back(Field::empty_list());
        v.push_back(Field::empty_dict());
    }

    return v;
}

inline std::vector<tuplex::Field> compound_test_values(bool tuples=true, bool dicts=true, bool lists=true) {
    using namespace tuplex;
    using namespace std;

    vector<Field> v;

    // get the primitives & then compose compounds out of them!
    auto prims = primitive_test_values();

    // 1.) tuples
    if(tuples) {
        v.push_back(Field::empty_tuple());

        // tuple of single element
        for(const auto& f : prims) {
            v.push_back(Field(Tuple(f)));
        }
        // tuple of combo of two elements
        for(const auto& a : prims) {
            for(const auto& b : prims) {
                v.push_back(Field(Tuple(a, b)));
            }
        }

        // one large tuple of all types
        v.push_back(Field(Tuple::from_vector(prims)));

        // nested tuples...
        // --> basically perform fold
        v.push_back((Field(Tuple(prims.front(), Tuple(prims.front())))));

        // nest (a(b(...)
        Field nested = prims[0];
        for(int i = 1; i < prims.size(); ++i)
            nested = Field(Tuple(prims[i], nested));
        v.push_back(nested);
    }

    // 2.) dicts
    // @TODO:


    // compound of compounds??
    // i.e. mixing tuplex/dicts/primitives/...

    return v;
}









#endif //TUPLEX_TESTUTILS_H