//
// Created by leonhards on 9/30/24.
//

#include <gtest/gtest.h>

#include "S3File.h"
#include "Pipe.h"
#include "jit/RuntimeInterface.h"
#include <ContextOptions.h>
#include <Timer.h>

#include "helper.h"
#include "ee/aws/LambdaWorkerApp.h"
#include <ee/worker/WorkerBackend.h>
#include <ee/aws/AWSLambdaBackend.h>

// tuplex files
#include <UDF.h>
#include <Context.h>

// AWS Lambda files
#include <aws/lambda/model/CreateFunctionRequest.h>
#include <aws/lambda/model/DeleteFunctionRequest.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <aws/lambda/model/ListFunctionsRequest.h>
#include <aws/lambda/LambdaClient.h>

using namespace tuplex;

// change these keys here to carry out actual remote testing.


class LambdaTest : public ::testing::Test {
protected:
    std::string testName;
    std::string bucketName;

    // Per Test setup
    void SetUp() override {
        testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
    }

    void TearDown() override {

    }

    static void SetUpTestSuite() {
        using namespace tuplex;
        using namespace std;

        if(!is_docker_installed())
            GTEST_SKIP() << "Docker not found, can not initialize test suite.";

        // init AWS SDK
        cout<<"Initializing AWS SDK"<<endl;
        initAWSSDK();

        cout<<"Initializing interpreter and releasing GIL"<<endl;
        python::initInterpreter();
        python::unlockGIL();
    }

    static void TearDownTestSuite() {
        using namespace std;
        using namespace tuplex;

        cout<<"Shutting down interpreter"<<endl;
        python::lockGIL();
        python::closeInterpreter();

        cout<<"Shutting down AWSSDK"<<endl;
        shutdownAWS();
    }

    Context create_lambda_context() {
        ContextOptions co = ContextOptions::defaults();
        co.set("tuplex.backend", "lambda");

        // enable requester pays
        co.set("tuplex.aws.requesterPay", "true");
        // scratch dir
        // co.set("tuplex.aws.scratchDir", std::string("s3://") + S3_TEST_BUCKET + "/.tuplex-cache");
        // co.set("tuplex.aws.scratchDir", std::string("s3://tuplex-test-") + tuplex::getUserName() + "/.tuplex-cache");
        co.set("tuplex.aws.scratchDir", "s3://tuplex-test/.tuplex-cache");
        co.set("tuplex.aws.httpThreadCount", "4");

        return std::move(Context(co));
    }

};

TEST_F(LambdaTest, SimpleRemoteTest) {
    using namespace std;
    using namespace tuplex;

    auto ctx = create_lambda_context();

    auto v = ctx.parallelize({Row(1), Row(2), Row(3)}).map(UDF("lambda x: x + 1")).collectAsVector();
    ASSERT_EQ(v.size(), 3);
    vector<int> ref{2, 3, 4};
    for(unsigned i = 0; i < v.size(); ++i) {
        EXPECT_EQ(v[i].getInt(0), ref[i]);
    }
}