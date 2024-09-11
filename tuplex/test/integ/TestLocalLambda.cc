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
#include "ee/worker/WorkerBackend.h"

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
                auto j = nlohmann::json::array();
                while(std::getline(input, line)) {
                    auto j_line = nlohmann::json::parse(line);
                    if(j_line.is_array() && j_line.empty())
                        continue;
                    j = j_line;

                    for(auto el : j_line) {
                        services_up.insert(el["Name"].get<string>());
                    }
                }

                if(j.empty()) {
                    not_up_yet = true;
                } else {

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



class LambdaLocalTest : public ::testing::Test {
public:
    static std::string yaml_path;
    static std::string lambda_endpoint;
protected:
    std::string testName;
    std::string bucketName;

    // Per Test setup
    void SetUp() override {
        // Check if docker is installed and S3 minio is up and running. If not, skip test.
        if(!is_docker_installed())
            GTEST_SKIP() << "Docker not found.";

        testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
    }

    void TearDown() override {

    }


    static bool create_test_bucket(const std::string& name) {
        using namespace tuplex;
        using namespace std;
        Aws::S3::Model::CreateBucketRequest request;
        request.SetBucket(name);

        auto impl =VirtualFileSystem::getS3FileSystemImpl();
        if(!impl) {
            cerr<<"No S3 filesystem registered to tuplex yet."<<endl;
        }
        auto outcome = impl->client().CreateBucket(request);

        if(outcome.IsSuccess()) {
            return true;
        } else {
            cerr<<"Failed creating bucket "<<name<<". Details: " + std::string(outcome.GetError().GetMessage().c_str());
            return false;
        }
    }

    static void SetUpTestSuite() {
        using namespace tuplex;
        using namespace std;

        if(!is_docker_installed())
            GTEST_SKIP() << "Docker not found, can not initialize test suite.";

        // init AWS SDK
        cout<<"Initializing AWS SDK"<<endl;
        initAWSSDK(Aws::Utils::Logging::LogLevel::Trace);

        // Docker-compose yaml file.
        if(!fileExists(yaml_path)) {
            cerr<<"Could not find docker compose stack "<<URI(yaml_path).toPath()<<"."<<endl;
            GTEST_SKIP()<<"Could not start docker stack for local lambda testing.";
            return;
        }

        // File-watch, check status and rebuild if necessary (todo).
        cout<<"Starting local lambda stack"<<endl;
        start_local_lambda_stack(yaml_path);

        auto MAX_DOCKER_STACK_CONNECT_RETRIES=10;
        if(!wait_for_stack(yaml_path, {"docker-rest-1", "docker-lambda-1", "minio"}, MAX_DOCKER_STACK_CONNECT_RETRIES))
            GTEST_SKIP()<<"Docker stack not up running after "<<MAX_DOCKER_STACK_CONNECT_RETRIES<<" retries";

        cout<<"Starting local MinIO test."<<endl;
        auto data_location = minio_data_location();

        cout<<"MinIO data location: "<<data_location<<endl;

        cout<<"Starting MinIO docker container"<<endl;
        auto rc = start_local_s3_server();
        ASSERT_TRUE(rc);

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
        rc = create_test_bucket(LOCAL_TEST_BUCKET_NAME);
        ASSERT_TRUE(rc);

        cout<<"Initializing interpreter and releasing GIL"<<endl;
        python::initInterpreter();
        python::unlockGIL();
    }

    static void TearDownTestSuite() {
        using namespace std;
        using namespace tuplex;

        cout<<"Shutting down interpreter."<<endl;
        python::lockGIL();
        python::closeInterpreter();

        cout<<"Stopping local lambda stack."<<endl;
        stop_local_lambda_stack(yaml_path);

        cout<<"Shutting down AWSSDK."<<endl;
        shutdownAWS();
    }
};

std::string LambdaLocalTest::yaml_path = "../resources/docker/docker-compose.yml";
std::string LambdaLocalTest::lambda_endpoint = "http://localhost:" + std::to_string(8090);

TEST_F(LambdaLocalTest, ConnectionTestInvoke) {
    using namespace std;
    using namespace tuplex;

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