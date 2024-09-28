//
// Created by leonhards on 9/9/24.
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

static const std::string LOCAL_TEST_BUCKET_NAME="local-bucket";

// set to true if test suite should start stack. For debugging may be easier
// to start the stack manually, and then
static const bool START_DOCKER_COMPOSE_STACK=false;

namespace tuplex {
    bool start_local_lambda_stack(const std::string& yaml_path) {
        using namespace std;

        // docker compose -f <path> up -d

        stringstream ss;
        ss<<"docker compose -f"<<yaml_path<<" up -d "; // detached mode.

        Pipe p(ss.str());
        p.pipe();
        if(p.retval() != 0) {
            cerr<<"Failed starting docker compose stack: "<<p.stderr()<<endl;
            return false;
        } else {
            cout<<"Started local docker compose stack for "<<yaml_path<<"."<<endl;
#ifndef NDEBUG
            cout<<p.stdout()<<endl;
#endif
            return true;
        }
    }

    bool wait_for_stack(const std::string& yaml_path, std::unordered_set<std::string> names, int max_tries=10, double sleep_delay=0.2) {
        // TODO: Need to wait for ALL services to come online.
        // --> explicit wait.

        using namespace std;

        // docker compose ps
        stringstream ss;
        ss<<"docker compose -f "<<yaml_path<<" ps --format=json";

        bool not_up_yet = true;
        int try_count = 0;
        while(not_up_yet && try_count < max_tries) {
            cout<<"Check whether docker compose stack is up and running, try "<<(try_count+1)<<"/"<<max_tries<<"..."<<endl;
            Pipe p(ss.str());
            p.pipe();
            if(p.retval() != 0) {
                throw std::runtime_error("Failed to list docker compose containers: " + p.stderr());
            } else {

                unordered_set<string> services_up;

                string json_str = p.stdout();
                std::vector<std::string> names;
                std::string line;
                std::stringstream input; input<<json_str;
                while(std::getline(input, line)) {
                    auto j = nlohmann::json::parse(line);
                    if(j.is_array() && j.empty())
                        continue;

                    // there are two different formats:
                    // 1. everything in an array
                    // 2. ndjson
                    if(j.is_array()) {
                        for(auto el : j) {
                            services_up.insert(el["Name"].get<string>());
                        }
                    } else {
                        services_up.insert(j["Name"].get<string>());
                    }
                }

                // Check if all services are up, if not: print out
                set<string> intersect;
                set_intersection(services_up.begin(), services_up.end(), names.begin(), names.end(),
                                 std::inserter(intersect, intersect.begin()));

                if(intersect.size() == names.size())
                    not_up_yet = false;
                else {
                    cout<<"--> Requested services: "<<vector<string>{names.begin(), names.end()}<<endl;
                    cout<<"    Currently running: "<<vector<string>{intersect.begin(), intersect.end()}<<endl;
                    not_up_yet = true;
                }
            }
            try_count++;

            if(not_up_yet)
                std::this_thread::sleep_for(std::chrono::duration_cast<std::chrono::steady_clock::duration>(std::chrono::duration<double>(sleep_delay)));
        }

        if(!not_up_yet)
            cout<<"Docker stack up and running."<<endl;
        else {
            cerr<<"Failed to connect to docker stack after "<<max_tries<<" retries."<<endl;
        }

        return !not_up_yet;
    }

    bool stop_local_lambda_stack(const std::string& yaml_path) {
        using namespace std;

        // docker compose -f <path> stop
        stringstream ss;
        ss<<"docker compose -f"<<yaml_path<<" stop"; // detached mode.

        Pipe p(ss.str());
        p.pipe();
        if(p.retval() != 0) {
            cerr<<"Failed stopping docker compose stack: "<<p.stderr()<<endl;
            return false;
        } else {
            cout<<"Stopped local docker compose stack for "<<yaml_path<<"."<<endl;
#ifndef NDEBUG
            cout<<p.stdout()<<endl;
#endif
            return true;
        }
        return true;
    }
}



class LocalStackFixture {
public:
    static std::string yaml_path;
    static std::string lambda_endpoint;
protected:
    inline Context create_lambda_context(const std::unordered_map<std::string, std::string>& conf_override={},
                                         const std::unordered_map<std::string, std::string>& env_override={}) {

        // Create a mini test context with Lambda backend for Tuplex.
        auto co = ContextOptions::defaults();
        co.set("tuplex.backend", "lambda");
        co.set("tuplex.aws.scratchDir", LOCAL_TEST_BUCKET_NAME + "/scratch");
        co.set("tuplex.aws.endpoint", lambda_endpoint);
        co.set("tuplex.aws.name", "tplxlam"); // <-- need to set this, default is different. Purposefully test with other name.

        // Overwrite settings with conf_override.
        for(const auto& kv : conf_override) {
            co.set(kv.first, kv.second);
        }

        Context ctx(co);

        // Adjust S3 endpoint in Lambda by sending over environment as in local S3 tests.
        assert(ctx.backend());
        auto wb = dynamic_cast<AwsLambdaBackend*>(ctx.backend());
        assert(wb);

        std::unordered_map<std::string, std::string> env;
        {
            // overwrite with minio S3 variables.
            // cf. https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-endpoints.html#endpoints-service-specific-table for table
            // and https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-envvars.html.
            auto [local_credentials, local_config] = local_s3_credentials();

            env["AWS_SECRET_ACCESS_KEY"] = local_credentials.GetAWSSecretKey();
            env["AWS_ACCESS_KEY_ID"] = local_credentials.GetAWSAccessKeyId();
            // The docker network uses internally "minio" as hostname for the S3 service.
            env["AWS_ENDPOINT_URL_S3"] = "http://minio:9000";
            // For the lambda, the endpoint within docker is called "".
            env["AWS_ENDPOINT_URL_LAMBDA"] = "http://rest:8090";
        }

        for(const auto& kv : env_override)
            env[kv.first] = kv.second;

        wb->setEnvironment(env);

        return std::move(ctx);
    }


    static bool create_test_bucket(const std::string& name, bool exists_ok=true) {
        using namespace tuplex;
        using namespace std;
        Aws::S3::Model::CreateBucketRequest request;
        request.SetBucket(name);

        auto impl = VirtualFileSystem::getS3FileSystemImpl();
        if(!impl) {
            cerr<<"No S3 filesystem registered to tuplex yet."<<endl;
        }
        auto outcome = impl->client().CreateBucket(request);

        if(outcome.IsSuccess()) {
            return true;
        } else {
            // If bucket already exists, all good (given exists_ok flag is set).
            if(exists_ok && outcome.GetError().GetErrorType() == Aws::S3::S3Errors::BUCKET_ALREADY_OWNED_BY_YOU || outcome.GetError().GetErrorType() == Aws::S3::S3Errors::BUCKET_ALREADY_EXISTS)
                return true;
            cerr<<"Failed creating bucket "<<name<<". Details: " + std::string(outcome.GetError().GetMessage().c_str())<<endl;
            return false;
        }
    }

    static void SetUpTestSuite() {
        using namespace tuplex;
        using namespace std;

        if(!is_docker_installed())
            GTEST_SKIP() << "Docker not found, can not initialize test suite.";

         // Stop existing running containers.
        if(START_DOCKER_COMPOSE_STACK) {
             vector<string> containers_to_stop{"docker-rest-1", "docker-lambda-1", "minio"};
             for(const auto& name: containers_to_stop)
                 stop_container(name);
        }

        // init AWS SDK
        cout<<"Initializing AWS SDK"<<endl;
        // Increase log-level here to get more detailed output about individual requests.
        // Else, leave at WARN.
        // initAWSSDK(Aws::Utils::Logging::LogLevel::Trace);
        initAWSSDK(Aws::Utils::Logging::LogLevel::Warn);

        // Docker-compose yaml file.
        if(!fileExists(yaml_path)) {
            cerr<<"Could not find docker compose stack "<<URI(yaml_path).toPath()<<"."<<endl;
            GTEST_SKIP()<<"Could not start docker stack for local lambda testing.";
            return;
        }

        // File-watch, check status and rebuild if necessary (todo).
        if(START_DOCKER_COMPOSE_STACK) {
             cout<<"Starting local lambda stack"<<endl;
             start_local_lambda_stack(yaml_path);
        }

        auto MAX_DOCKER_STACK_CONNECT_RETRIES=10;
        if(!wait_for_stack(yaml_path, {"docker-rest-1", "docker-lambda-1-1", "docker-lambda-2-1", "minio"}, MAX_DOCKER_STACK_CONNECT_RETRIES))
            GTEST_SKIP()<<"Docker stack not up running after "<<MAX_DOCKER_STACK_CONNECT_RETRIES<<" retries";

        // Add minio S3 Filesystem to tuplex.
        Aws::Auth::AWSCredentials credentials;
        Aws::Client::ClientConfiguration config;
        std::tie(credentials, config) = local_s3_credentials();
        NetworkSettings ns;
        ns.endpointOverride = config.endpointOverride.c_str();
        ns.verifySSL = false;
        ns.useVirtualAddressing = false;
        ns.signPayloads = false;
        VirtualFileSystem::addS3FileSystem(credentials.GetAWSAccessKeyId().c_str(), credentials.GetAWSSecretKey().c_str(), "", "", ns);


        // create test bucket (because everything is in-memory so far)
        auto rc = create_test_bucket(LOCAL_TEST_BUCKET_NAME);
        ASSERT_TRUE(rc);

        cout<<"Initializing interpreter and releasing GIL"<<endl;
        python::initInterpreter();
        python::unlockGIL();
    }

    static void TearDownTestSuite() {
        using namespace std;
        using namespace tuplex;

        if(python::isInterpreterRunning()) {
            cout<<"Shutting down interpreter."<<endl;
            python::lockGIL();
            python::closeInterpreter();
        }

        if(START_DOCKER_COMPOSE_STACK) {
             cout<<"Stopping local lambda stack."<<endl;
             stop_local_lambda_stack(yaml_path);
        }

        cout<<"Shutting down AWSSDK."<<endl;
        shutdownAWS();
    }
};

std::string LocalStackFixture::yaml_path = "../resources/docker/docker-compose.yml";
std::string LocalStackFixture::lambda_endpoint = "http://localhost:" + std::to_string(8090);

bool py3majmin_match(const std::string& v1, const std::string& v2) {
    auto p1 = splitToArray(v1, '.');
    auto p2 = splitToArray(v2, '.');

    return p1[0] == p2[0] && p1[1] == p2[1];
}


// Google test
class LambdaLocalTest : public ::testing::Test, public LocalStackFixture {
protected:
    std::string testName;

    // Per Test setup
    void SetUp() override {
        // Check if docker is installed and S3 minio is up and running. If not, skip test.
        if (!is_docker_installed())
            GTEST_SKIP() << "Docker not found.";

        testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) +
                   std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
    }

    void TearDown() override {

    }

    static void SetUpTestSuite() {
        LocalStackFixture::SetUpTestSuite();
    }

    static void TearDownTestSuite() {
        LocalStackFixture::TearDownTestSuite();
    }
};

TEST_F(LambdaLocalTest, ConnectionTestInvoke) {
    using namespace std;
    using namespace tuplex;

    // Note: If you're using pyenv, make sure the embedded python is build using
    //       PYTHON_CONFIGURE_OPTS="--enable-shared" pyenv install --force 3.11.6

    // Basic Lambda invoke to check whether docker stack is launched, and that Lambda responds.

    cout<<"Connecting via AWS Lambda client..."<<endl;

    // Test with AWS Lambda client.
    Aws::Auth::AWSCredentials credentials;
    Aws::Client::ClientConfiguration s3_config;
    std::tie(credentials, s3_config) = local_s3_credentials();

    Aws::Client::ClientConfiguration config;
    // Overwrite lambda endpoint.
    NetworkSettings ns;
    ns.endpointOverride = lambda_endpoint;
    ns.signPayloads = false;
    ns.useVirtualAddressing = false;
    applyNetworkSettings(ns, config);

    // Need to use long timeouts, Lambdas may take a while.
    config.connectTimeoutMs = 30 * 1000.0; // 30s connect timeout.
    config.requestTimeoutMs = 60 * 1000.0; // 60s timeout.

    std::shared_ptr<Aws::Lambda::LambdaClient> client = make_shared<Aws::Lambda::LambdaClient>(credentials, config);

    // local Lambda test endpoint only supports InvokeRequests, i.e.
    // import boto3
    // boto3.set_stream_logger(name='botocore')
    // lam = boto3.client('lambda',
    //                    endpoint_url='http://localhost:8080',
    //                    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    //                    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')
    // lam.invoke(FunctionName='function', InvocationType='RequestResponse', LogType='Tail', Payload=b'')
    // is a minimal python snippet on how to connect.
    // For more requests, need to implement additional Lambda REST endpoints.

    Aws::Lambda::Model::InvokeRequest invoke_req;
    invoke_req.SetFunctionName("tplxlam");
    invoke_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
    // logtype to extract log data??
    invoke_req.SetLogType(Aws::Lambda::Model::LogType::Tail);
    std::string json_buf;

    // Send basic Environment request message.
    ::messages::InvocationRequest req;
    req.set_type(::messages::MessageType::MT_ENVIRONMENTINFO);
    google::protobuf::util::MessageToJsonString(req, &json_buf);

    invoke_req.SetBody(stringToAWSStream(json_buf));
    invoke_req.SetContentType("application/javascript");
    auto outcome = client->Invoke(invoke_req);
    if (!outcome.IsSuccess()) {
        std::stringstream ss;
         ss << "error: "<<outcome.GetError().GetExceptionName().c_str() << ", "
           << outcome.GetError().GetMessage().c_str();

         cerr<<ss.str()<<endl;
    } else {
        // Get result, and display environment:

        // write response
        auto &result = outcome.GetResult();
        auto statusCode = result.GetStatusCode();
        std::string version = result.GetExecutedVersion().c_str();

        // parse payload
        stringstream ss;
        auto &stream = const_cast<Aws::Lambda::Model::InvokeResult &>(result).GetPayload();
        ss << stream.rdbuf();
        string data = ss.str();
        ::messages::InvocationResponse response;
        auto status = google::protobuf::util::JsonStringToMessage(data, &response);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(statusCode, 200); // should be 200 for ok.
        ASSERT_EQ(response.resources_size(), 2); // 1 resource for encoded JSON, 1 resource for LOG.
        // Note: order does not matter.
        auto env_resource = response.resources(0);
        auto log_resource = response.resources(1);

        ASSERT_EQ(env_resource.type(), static_cast<uint32_t>(ResourceType::ENVIRONMENT_JSON));
        ASSERT_EQ(log_resource.type(), static_cast<uint32_t>(ResourceType::LOG));

        // Log:
        cout<<"Log of Lambda invocation:\n"
            <<decompress_string(log_resource.payload());

        auto j = nlohmann::json::parse(env_resource.payload());
        cout<<"Environment information message:\n"<<j.dump(2)<<endl;

        auto this_environment = codegen::compileEnvironmentAsJson();

        // Add python specific information.
        this_environment["python"] = PY_VERSION;
        std::string version_string = "unknown";
        python::lockGIL();
        python::cloudpickleVersion(version_string);
        python::unlockGIL();
        this_environment["cloudpickleVersion"] = version_string;

#ifdef BUILD_WITH_CEREAL
        this_environment["astSerializationFormat"] = "cereal";
#else
        this_environment["astSerializationFormat"] = "json";
#endif

        // Check that serialization format (cereal/JSON) is identical.
        // Check that python version matches (?)
        // Check that LLVM version matches (?)
        vector<string> keys_to_check{"astSerializationFormat", "cloudpickleVersion", "llvmVersion"};
        for(const auto& key : keys_to_check)
            EXPECT_EQ(j[key], this_environment[key]);
        EXPECT_TRUE(py3majmin_match(j["python"], this_environment["python"]));
    }
    EXPECT_TRUE(outcome.IsSuccess());
}

TEST_F(LambdaLocalTest, ListFunctions) {
    using namespace std;
    using namespace tuplex;

    // Basic Lambda invoke to check whether docker stack is launched, and that Lambda responds.

    cout<<"Connecting via AWS Lambda client..."<<endl;

    // Test with AWS Lambda client.
    Aws::Auth::AWSCredentials credentials;
    Aws::Client::ClientConfiguration config;
    std::tie(credentials, config) = local_s3_credentials();

    // Overwrite lambda endpoint.
    config.endpointOverride = lambda_endpoint;

    std::shared_ptr<Aws::Lambda::LambdaClient> client = make_shared<Aws::Lambda::LambdaClient>(credentials, config);

    Aws::Lambda::Model::ListFunctionsRequest list_req;
    auto outcome = client->ListFunctions(list_req);
    if (!outcome.IsSuccess()) {
        std::stringstream ss;
        ss << outcome.GetError().GetExceptionName().c_str() << ", "
           << outcome.GetError().GetMessage().c_str();
    } else {
        // check whether function is contained
        auto funcs = outcome.GetResult().GetFunctions();

        EXPECT_EQ(funcs.size(), 1);

        // search for the function of interest
        for (const auto &f: funcs) {
            cout<<"Found function: "<<f.GetFunctionName().c_str()<<endl;
            EXPECT_EQ(f.GetFunctionName(), "tplxlam"); // <-- mock should reflect default name.
        }
    }
    EXPECT_TRUE(outcome.IsSuccess());
}

TEST_F(LambdaLocalTest, CompactListFunctions) {
    using namespace std;
    using namespace tuplex;

    // Test with AWS Lambda client.
    Aws::Auth::AWSCredentials credentials;
    Aws::Client::ClientConfiguration config;
    config.endpointOverride = "http://localhost:" + std::to_string(8090);
    config.enableEndpointDiscovery = false;
    // need to disable signing https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html.
    config.verifySSL = false;
    config.connectTimeoutMs = 1500; // 1.5s timeout (local machine)
    std::shared_ptr<Aws::Lambda::LambdaClient> client = make_shared<Aws::Lambda::LambdaClient>(credentials, config);

    Aws::Lambda::Model::ListFunctionsRequest list_req;
    auto outcome = client->ListFunctions(list_req);
    std::stringstream ss;
    if (!outcome.IsSuccess()) {
        ss << outcome.GetError().GetExceptionName().c_str() << ", "
           << outcome.GetError().GetMessage().c_str();
    } else {
        // check whether function is contained
        auto funcs = outcome.GetResult().GetFunctions();
        std::vector<std::string> v;
        for (const auto &f: funcs) {
            v.push_back(f.GetFunctionName().c_str());
        }
        ss<<v;
    }
    cout<<"List Result: "<<ss.str()<<endl;
    ASSERT_FALSE(ss.str().empty());
}

TEST_F(LambdaLocalTest, SimpleEndToEndTest) {
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

TEST_F(LambdaLocalTest, MiniGithub) {
    using namespace std;
    using namespace tuplex;

    string input_pattern = "../resources/hyperspecialization/github_daily/*.json.sample";
    string output_path = "./" + testName + "/output";

    // Test github pipeline with small sample files.
    // Step 1: Upload all files into bucket.
    // test file, write some stuff to it.

    auto files_to_upload = glob(input_pattern);
    cout<<"Uploading "<<pluralize(files_to_upload.size(), "file")<<" to S3."<<endl;
    for(const auto& path : files_to_upload) {
        auto target_uri = "s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/" + URI(path).base_name();
        VirtualFileSystem::copy(path, target_uri);
    }

    input_pattern = "s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/" + "*.json.sample";
    output_path = "s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/" + "output";

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

TEST_F(LambdaLocalTest, GithubSplitTestWithSelfInvoke) {
    using namespace std;
    using namespace tuplex;

    string input_pattern = "../resources/hyperspecialization/github_daily/2020-10-15.json.sample";
    uint64_t input_file_size = 0;
    VirtualFileSystem::fromURI(input_pattern).file_size(input_pattern, input_file_size);

    // upload to S3.
    cout<<"Upload file to S3."<<endl;
    auto target_uri = "s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/" + URI(input_pattern).base_name();
    VirtualFileSystem::copy(input_pattern, target_uri);
    input_pattern = target_uri;
    auto output_path = "s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/" + "output";
    cout<<"Stored file as: "<<input_pattern<<endl;

    // Get size and split into parts.
    int n_parts = 5;

    cout<<"Found file with size "<<input_file_size<<"B, splitting into "<<pluralize(n_parts, "part")<<endl;
    auto parts = splitIntoEqualParts(n_parts, {URI(input_pattern)}, {input_file_size}, 1024);
    for(const auto& thread_parts : parts) {
        for(auto part : thread_parts)
            cout<<"part "<<part.partNo<<": "<<part.uri<<":"<<part.rangeStart<<"-"<<part.rangeEnd<<endl;
    }

    // Now perform github incl. specialization:
    cout<<"Creating Lambda context."<<endl;
    std::unordered_map<std::string, std::string> conf;
    conf["tuplex.aws.lambdaInvocationStrategy"] = "tree";
    conf["tuplex.aws.maxConcurrency"] = "10"; // use 10 as maximum parallelism.
    conf["tuplex.experimental.minimumSizeToSpecialize"] = "0"; // disable minimum size.

    // use this (may file under macOS).
    conf["tuplex.experimental.interchangeWithObjectFiles"] = "true";

    auto ctx = create_lambda_context(conf);

    cout<<"Starting Github (mini) pipeline."<<endl;
    github_pipeline(ctx, input_pattern, output_path);

    cout<<"Checking whether result matches."<<endl;
    auto total_row_count = csv_row_count_for_pattern(output_path + "/*.csv");
    EXPECT_EQ(total_row_count, 378);
}

TEST_F(LambdaLocalTest, S3Connectivity) {
    using namespace tuplex;

    auto credentials = std::get<0>(local_s3_credentials());
    // valid s3 endpoint is "http://localhost:9000" but "http://minio:9000" fails.
    EXPECT_TRUE(check_s3_connection("http://localhost:9000", credentials.GetAWSAccessKeyId().c_str(),
                                    credentials.GetAWSSecretKey().c_str(), credentials.GetSessionToken().c_str()));

    EXPECT_FALSE(check_s3_connection("http://minio:9000", credentials.GetAWSAccessKeyId().c_str(),
                                    credentials.GetAWSSecretKey().c_str(), credentials.GetSessionToken().c_str()));
}


class ParametrizedLambdaLocalTest : public LocalStackFixture, public ::testing::TestWithParam<std::unordered_map<std::string,std::string>> {
public:
    std::string testName;

    // Per Test setup
    void SetUp() override {
        // Check if docker is installed and S3 minio is up and running. If not, skip test.
        if (!is_docker_installed())
            GTEST_SKIP() << "Docker not found.";

        testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) +
                   std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
    }

    void TearDown() override {

    }
protected:
    static void SetUpTestSuite() {
        LocalStackFixture::SetUpTestSuite();
    }

    static void TearDownTestSuite() {
        LocalStackFixture::TearDownTestSuite();
    }
};

TEST_P(ParametrizedLambdaLocalTest, GithubSplitTestWithSelfInvokeWithAppDebug) {
    // This test allows to debug/step through a LambdaWorkerApp instance.
    // It is similar to the end-to-end test GithubSplitTestWithSelfInvoke.
    using namespace std;
    using namespace tuplex;

    auto conf_override = GetParam();

    string input_pattern = "../resources/hyperspecialization/github_daily/2020-10-15.json.sample";
    uint64_t input_file_size = 0;
    VirtualFileSystem::fromURI(input_pattern).file_size(input_pattern, input_file_size);

    // upload to S3.
    cout<<"Upload file to S3."<<endl;
    auto target_uri = "s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/" + URI(input_pattern).base_name();
    VirtualFileSystem::copy(input_pattern, target_uri);
    input_pattern = target_uri;
    auto output_path = "s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/" + "output";
    cout<<"Stored file as: "<<input_pattern<<endl;

    // Get size and split into parts.
    int n_parts = 5;

    cout<<"Found file with size "<<input_file_size<<"B, splitting into "<<pluralize(n_parts, "part")<<endl;
    auto parts = splitIntoEqualParts(n_parts, {URI(input_pattern)}, {input_file_size}, 1024);
    for(const auto& thread_parts : parts) {
        for(auto part : thread_parts)
            cout<<"part "<<part.partNo<<": "<<part.uri<<":"<<part.rangeStart<<"-"<<part.rangeEnd<<endl;
    }

    // Now perform github incl. specialization:
    cout<<"Creating Lambda context."<<endl;
    std::unordered_map<std::string, std::string> conf;
    conf["tuplex.aws.lambdaInvocationStrategy"] = "tree";
    conf["tuplex.aws.maxConcurrency"] = "10"; // use 10 as maximum parallelism.
    conf["tuplex.experimental.minimumSizeToSpecialize"] = "0"; // disable minimum size.
    conf["tuplex.experimental.opportuneCompilation"] = "false"; // disable, is buggy.


    // override conf with parameters.
    conf["tuplex.useInterpreterOnly"] = "false";
    conf["tuplex.experimental.interchangeWithObjectFiles"] = "false";
    conf["tuplex.experimental.hyperspecialization"] = "false";

    for(const auto& kv : conf_override)
        conf[kv.first] = kv.second;

    // Test option: check with python mode only.
    //conf["tuplex.useInterpreterOnly"] = "true";

    // Test option: enable shipping code (at least for self-invoked Lambdas).
    //conf["tuplex.experimental.interchangeWithObjectFiles"] = "true";

    // Test option: hyper on/off.
    //conf["tuplex.experimental.hyperspecialization"] = "true";


    auto ctx = create_lambda_context(conf);

    cout<<"Changing backend to emit requests only."<<endl;
    ASSERT_TRUE(ctx.backend());
    auto wb = static_cast<AwsLambdaBackend*>(ctx.backend());
    wb->setRequestMode(true);

    cout<<"Starting Github (mini) pipeline."<<endl;
    github_pipeline(ctx, input_pattern, output_path);

    auto requests = wb->pendingRequests(true);

    EXPECT_EQ(requests.size(), 1);
    ASSERT_FALSE(requests.empty());

    auto request = requests.front();

    // Overwrite lambda endpoint.
    // "AWS_ENDPOINT_URL_LAMBDA" -> "http://localhost:8090"
    request.mutable_env()->at("AWS_ENDPOINT_URL_LAMBDA") = "http://localhost:" + std::to_string(8090); // lambda/rest
    request.mutable_env()->at("AWS_ENDPOINT_URL_S3") = "http://minio:9000;http://localhost:9000"; // minio/localhost. Need to use two here to make test work.
    string json_str;
    auto status = google::protobuf::util::MessageToJsonString(request, &json_str);
    EXPECT_TRUE(status.ok());

    // Create LambdaWorkerApp and invoke (locally) to Lambda backend - this allows to easily debug.
    auto app = std::make_shared<LambdaWorkerApp>();
    app->WorkerApp::globalInit(false);

    app->setFunctionName("tplxplam");

    auto rc = app->processJSONMessage(json_str);
    EXPECT_EQ(rc, WORKER_OK);

    auto ret = app->response();

    app->WorkerApp::shutdown();

    // Check output row count in S3:
    auto total_row_count = csv_row_count_for_pattern(output_path + "/*.csv");
    EXPECT_EQ(total_row_count, 24); // 24 output rows for 2020 file. (387 across all years).

    // Check that response ret has information about recursively invoked containers.
    EXPECT_EQ(ret.invokedresponses_size(), 3);
    EXPECT_EQ(ret.invokedrequests_size(), 3);

    // Print out invoked request info.
    int request_no = 0;
    for(auto info : ret.invokedrequests()) {
        cout<<"Request "<<request_no++<<":\n";
        std::string info_str;
        google::protobuf::util::MessageToJsonString(info, &info_str);
        // pretty print
        auto j = nlohmann::json::parse(info_str);
        cout<<j.dump(2)<<endl;
    }

    // Retrieve output uris and check their names.
    std::vector<URI> output_uris;
    std::copy(ret.outputuris().begin(), ret.outputuris().end(), std::back_inserter(output_uris));
    EXPECT_EQ(output_uris.size(), 1);
    for(auto sub_ret : ret.invokedresponses()) {
        std::copy(sub_ret.outputuris().begin(), sub_ret.outputuris().end(), std::back_inserter(output_uris));
    }
    EXPECT_EQ(output_uris.size(), 4); // 4 requests invoked.
    // check names:
    std::sort(output_uris.begin(), output_uris.end(), [](const URI& a, const URI& b) {
        return a.toString() < b.toString();
    });
    for(unsigned i = 0; i < output_uris.size(); ++i) {
        EXPECT_EQ(output_uris[i].toString(), output_path +"/part" + std::to_string(i) + ".csv");
    }

    // -> compute stats from there.

    // @TODO: pass compiled code to recursive lambdas.
}

INSTANTIATE_TEST_SUITE_P(Blub, ParametrizedLambdaLocalTest, ::testing::Values(std::unordered_map<std::string, std::string>{},
                                                                              std::unordered_map<std::string, std::string>{std::make_pair("tuplex.experimental.interchangeWithObjectFiles", "true")},
                                                                              std::unordered_map<std::string, std::string>{std::make_pair("tuplex.experimental.hyperspecialization", "true")},
                                                                              std::unordered_map<std::string, std::string>{std::make_pair("tuplex.experimental.hyperspecialization", "true"),
                                                                                                                           std::make_pair("tuplex.experimental.interchangeWithObjectFiles", "true")},
                                                                              std::unordered_map<std::string, std::string>{std::make_pair("tuplex.useInterpreterOnly", "true")}));

// Notes: https://guihao-liang.github.io/2020/04/12/aws-s3-retry