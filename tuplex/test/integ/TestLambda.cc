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

    std::string s3PathForTest() {
        return "s3://tuplex-test/tests/integration/" + testName;
    }

    static void SetUpTestSuite() {
        using namespace tuplex;
        using namespace std;

        if(!is_docker_installed())
            GTEST_SKIP() << "Docker not found, can not initialize test suite.";

        // init AWS SDK
        cout<<"Initializing AWS SDK."<<endl;
        initAWSSDK();

        // Add S3 filesystem (the actual one).
        cout<<"Initializing S3 file system."<<endl;
        VirtualFileSystem::addS3FileSystem();

        cout<<"Initializing interpreter and releasing GIL."<<endl;
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

    Context create_lambda_context(const std::unordered_map<std::string, std::string>& conf_override={},
                                  const std::unordered_map<std::string, std::string>& env_override={}) {
        ContextOptions co = ContextOptions::defaults();
        co.set("tuplex.backend", "lambda");

        // enable requester pays
        co.set("tuplex.aws.requesterPay", "true");
        // scratch dir
        // co.set("tuplex.aws.scratchDir", std::string("s3://") + S3_TEST_BUCKET + "/.tuplex-cache");
        // co.set("tuplex.aws.scratchDir", std::string("s3://tuplex-test-") + tuplex::getUserName() + "/.tuplex-cache");
        co.set("tuplex.aws.scratchDir", "s3://tuplex-test/.tuplex-cache");
        co.set("tuplex.aws.httpThreadCount", std::to_string(410));
        co.set("tuplex.aws.maxConcurrency", std::to_string(410));
        // co.set("tuplex.aws.lambdaMemory", "10000");
        // co.set("tuplex.aws.lambdaThreads", "3");

        // Overwrite settings with conf_override.
        for(const auto& kv : conf_override) {
            co.set(kv.first, kv.second);
        }

        auto ctx = Context(co);
        auto wb = dynamic_cast<AwsLambdaBackend*>(ctx.backend());
        assert(wb);
        wb->setEnvironment(env_override);
        return std::move(ctx);
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

TEST_F(LambdaTest, GithubPipeline) {
    using namespace std;
    using namespace tuplex;

    string input_pattern = "../resources/hyperspecialization/github_daily/*.json.sample";
    string output_path = "./" + testName + "/output";

    auto s3_root = s3PathForTest();
    cout<<"Storing test data in "<<s3_root<<"."<<endl;

    // Test github pipeline with small sample files.
    // Step 1: Upload all files into bucket.
    // test file, write some stuff to it.

    auto files_to_upload = glob(input_pattern);
    cout<<"Uploading "<<pluralize(files_to_upload.size(), "file")<<" to S3."<<endl;
    for(const auto& path : files_to_upload) {
        auto target_uri = s3_root + "/data/" + URI(path).base_name();
        VirtualFileSystem::copy(path, target_uri);
    }

    input_pattern = s3_root + "/data/" + "*.json.sample";
    output_path = s3_root + "/output";

    cout<<"-- Input pattern: "<<input_pattern<<endl;
    cout<<"-- Output dest: "<<output_path<<endl;

    cout<<"Creating Lambda context."<<endl;
    auto ctx = create_lambda_context();

    cout<<"Starting Github (mini) pipeline."<<endl;
    github_pipeline(ctx, input_pattern, output_path);

    cout<<"Checking result."<<endl;
    // glob output files (should be equal amount, as 1 request per file)
    auto output_uris = VirtualFileSystem::fromURI(input_pattern).glob(output_path + "/*.csv");

    cout<<"Found "<<pluralize(output_uris.size(), "output file")<<" in local S3 file system."<<endl;

    // there must be one file now (because of the request).
    EXPECT_EQ(output_uris.size(), files_to_upload.size());

    // Step 5: Check csv counts to make sure these are correct.
    auto total_row_count = csv_row_count_for_pattern(output_path + "/*.csv");
    EXPECT_EQ(total_row_count, 378);
}

TEST_F(LambdaTest, GithubPipelineSelfInvoke) {
    using namespace std;
    using namespace tuplex;

    auto s3_root = s3PathForTest();
    cout<<"Storing test data in "<<s3_root<<"."<<endl;

    string local_input_pattern = "../resources/hyperspecialization/github_daily/2012*.json.sample";
    string local_output_path = "./" + testName + "/output";
    auto s3_input_pattern = s3_root + "/data/" + "2012*.json.sample";
    auto s3_output_path = s3_root + "/output";


    // Test github pipeline with small sample files.
    // Step 1: Upload all files into bucket.
    // test file, write some stuff to it.

    // auto files_to_upload = glob(local_input_pattern);
    // cout<<"Uploading "<<pluralize(files_to_upload.size(), "file")<<" to S3."<<endl;
    // for(const auto& path : files_to_upload) {
    //     auto target_uri = s3_root + "/data/" + URI(path).base_name();
    //     VirtualFileSystem::copy(path, target_uri);
    // }

    cout<<"-- Input pattern: "<<s3_input_pattern<<endl;
    cout<<"-- Output dest: "<<s3_output_path<<endl;

    auto output_pattern = s3_output_path + "/*.csv";
    auto output_uris = VirtualFileSystem::fromURI(output_pattern).glob(output_pattern);
    if(!output_uris.empty()) {
        cout<<"Removing existing files from S3:"<<endl;
        cout<<"Found "<<pluralize(output_uris.size(), "old output uri")<<" to be removed to run test."<<endl;
        auto ret = s3RemoveObjects(VirtualFileSystem::getS3FileSystemImpl()->client(), output_uris, &cerr);
        ASSERT_TRUE(ret);

        // check invariant again.
        output_uris = VirtualFileSystem::fromURI(output_pattern).glob(output_pattern);
        ASSERT_TRUE(output_uris.empty());
    }

    cout<<"Creating Lambda context."<<endl;
    std::unordered_map<std::string, std::string> conf;
    conf["tuplex.aws.lambdaInvocationStrategy"] = "tree";
    conf["tuplex.experimental.minimumSizeToSpecialize"] = "0"; // disable minimum size.

    // the object code interchange fails with segfaults when using the libc preloader...
    conf["tuplex.experimental.interchangeWithObjectFiles"] = "true";

    conf["tuplex.experimental.interchangeWithObjectFiles"] = "false";

    // enable hyper specialization
    conf["tuplex.experimental.hyperspecialization"] = "true";

    // disable opportune compilation
    conf["tuplex.experimental.opportuneCompilation"] = "false";

    // concurrency limit:
    conf["tuplex.aws.maxConcurrency"] = "400"; // use 10 as maximum parallelism.

    auto ctx = create_lambda_context(conf);

    cout<<"Starting Github (mini) pipeline."<<endl;
    github_pipeline(ctx, s3_input_pattern, s3_output_path);

    cout<<"Checking result."<<endl;
    // glob output files (should be equal amount, as 1 request per file)
    output_uris = VirtualFileSystem::fromURI(s3_input_pattern).glob(s3_output_path + "/*.csv");

    cout<<"Found "<<pluralize(output_uris.size(), "output file")<<" in local S3 file system."<<endl;

    // Step 5: Check csv counts to make sure these are correct.
    auto total_row_count = csv_row_count_for_pattern(s3_output_path + "/*.csv");

//    // only 2011.
//    EXPECT_EQ(total_row_count, 30);

    // only 2012.
    EXPECT_EQ(total_row_count, 53);

    // EXPECT_EQ(total_row_count, 378);
}

TEST_F(LambdaTest, GithubPipelineSelfInvokeDaily) {
    using namespace std;
    using namespace tuplex;

    string input_pattern = "s3://tuplex-public/data/github_daily/*.json";
    string output_path = "./" + testName + "/output";

    auto s3_root = s3PathForTest();
    cout<<"Storing output test data in "<<s3_root<<"."<<endl;
    output_path = s3_root + "/output";

    cout<<"-- Input pattern: "<<input_pattern<<endl;
    cout<<"-- Output dest: "<<output_path<<endl;

    // Step 1: Check that files exist (input pattern), and remove output files.
    auto input_uris = VirtualFileSystem::fromURI(input_pattern).glob(input_pattern);
    ASSERT_EQ(input_uris.size(), 11);

    auto output_pattern = output_path + "/*.csv";
    auto output_uris = VirtualFileSystem::fromURI(output_pattern).glob(output_pattern);
    if(!output_uris.empty()) {
        cout<<"Removing existing files from S3:"<<endl;
        cout<<"Found "<<pluralize(output_uris.size(), "old output uri")<<" to be removed to run test."<<endl;
        auto ret = s3RemoveObjects(VirtualFileSystem::getS3FileSystemImpl()->client(), output_uris, &cerr);
        ASSERT_TRUE(ret);
    }

    cout<<"Creating Lambda context."<<endl;
    std::unordered_map<std::string, std::string> conf;
    conf["tuplex.aws.lambdaInvocationStrategy"] = "tree";
    conf["tuplex.aws.maxConcurrency"] = "100"; // use 100 as maximum parallelism.
    conf["tuplex.experimental.minimumSizeToSpecialize"] = "0"; // disable minimum size.

    // the object code interchange fails with segfaults when using the libc preloader...
    conf["tuplex.experimental.interchangeWithObjectFiles"] = "true";

    conf["tuplex.experimental.interchangeWithObjectFiles"] = "false";

    // enable hyper specialization
    conf["tuplex.experimental.hyperspecialization"] = "true";

    // deactivate compiled resolver for now.
    conf["tuplex.resolveWithInterpreterOnly"] = "True";

    auto ctx = create_lambda_context(conf);

    cout<<"Starting Github (daily) pipeline."<<endl;
    github_pipeline(ctx, input_pattern, output_path);

    cout<<"Checking result."<<endl;
    // glob output files (should be equal amount, as 1 request per file)
    output_uris = VirtualFileSystem::fromURI(input_pattern).glob(output_path + "/*.csv");

    cout<<"Found "<<pluralize(output_uris.size(), "output file")<<" in local S3 file system."<<endl;

    // Step 5: Check csv counts to make sure these are correct.
    auto total_row_count = csv_row_count_for_pattern(output_path + "/*.csv");
    EXPECT_EQ(total_row_count, 294195);
}

TEST_F(LambdaTest, RecursiveLambdaRequestBySize) {
    using namespace std;
    using namespace tuplex;

    string input_pattern = "s3://tuplex-public/data/github_daily/*.json";
    vector<tuple<URI,size_t>> uri_infos;
    VirtualFileSystem::walkPattern(URI(input_pattern), [&](void *userData, const tuplex::URI &uri, size_t size) {
        uri_infos.push_back(make_tuple(uri, size));
        return true;
    });

    cout<<"Found "<<pluralize(uri_infos.size(), "uri")<<" to split up into requests."<<endl;

    auto minimum_chunk_size = memStringToSize("16MB");
    auto maximum_chunk_size = memStringToSize("512MB");
    auto& handler = Logger::instance().defaultLogger();
    auto requests = create_specializing_recursive_requests(uri_infos, minimum_chunk_size, maximum_chunk_size, handler);

    cout<<"Got "<<pluralize(requests.size(), "request")<<endl;
}


namespace tuplex {
    size_t total_request_count(const std::vector<AwsLambdaRequest>& requests) {
        size_t count = requests.size();

        for(const auto& request : requests)
            if(request.body.stage().invocationcount_size() != 0)
                count += request.body.stage().invocationcount(0);

        return count;
    }
}

TEST_F(LambdaTest, FindSuitableMaxChunkSize) {
    using namespace std;
    using namespace tuplex;

    // daily.
    // string input_pattern = "s3://tuplex-public/data/github_daily/*.json";
    // test quantity (parametrize test over this)
    // auto desired_parallelism = 400; // --> ~97MB.

    // monthly
    string input_pattern = "s3://tuplex-public/data/github_monthly/*.json";

    // test quantity (parametrize test over this)
    auto desired_parallelism = 5000; // --> ~5.24GB.

    vector<tuple<URI,size_t>> uri_infos;
    VirtualFileSystem::walkPattern(URI(input_pattern), [&](void *userData, const tuplex::URI &uri, size_t size) {
        uri_infos.push_back(make_tuple(uri, size));
        return true;
    });

    cout<<"Found "<<pluralize(uri_infos.size(), "uri")<<" to split up into requests."<<endl;

    auto minimum_chunk_size = memStringToSize("16MB");
    std::vector<size_t> sizes;
    for(auto uri_info : uri_infos)
        sizes.emplace_back(std::get<1>(uri_info));

    auto c_max = find_max_chunk_size(sizes, minimum_chunk_size, desired_parallelism);

    // check:
    auto& handler = Logger::instance().defaultLogger();
    auto requests = create_specializing_recursive_requests(uri_infos, minimum_chunk_size, c_max, handler);

    cout<<"Found suitable maximum chunk size "<<sizeToMemString(c_max)<<"\n."
        <<"Splitting into tree will yield "<<requests.size()<<" direct requests.\n"
        <<"Desired: "<<desired_parallelism<<" Actual max parallel requests: "<< total_request_count(requests)<<endl;

}