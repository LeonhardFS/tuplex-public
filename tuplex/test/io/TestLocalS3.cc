//
// Created by leonhards on 8/26/24.
//

#include <gtest/gtest.h>

#include "FileSystemUtils.h"
#include "S3File.h"
#include "Pipe.h"
#include <ContextOptions.h>
#include <Timer.h>

#include <AWSCommon.h>
#include <VirtualFileSystem.h>
#include <S3Cache.h>
#include <filesystem>
#include <aws/core/auth/AWSCredentials.h>

#include <nlohmann/json.hpp>

//#include <reproc>

bool is_docker_installed() {
    using namespace tuplex;
    using namespace std;

    // Check by running `docker --version`, whether locally docker is available.
    // If not, skip tests.
    Pipe p("docker --version");
    p.pipe();
    if(p.retval() == 0) {
        auto p_stdout = p.stdout();
        trim(p_stdout);
        cout<<"Found "<<p_stdout<<"."<<endl;
        return true;
    }
    return false;
}

std::string minio_data_location() {
    using namespace tuplex;

    // create location if needed
    auto absolute_path = URI("../resources/data").toPath();
    if(!dirExists(absolute_path.c_str())) {
        std::filesystem::create_directories(absolute_path);
    }
    return absolute_path;
}

// dummy values used for testing
static const std::string MINIO_ACCESS_KEY="AKIAIOSFODNN7EXAMPLE";
static const std::string MINIO_SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
static const std::string MINIO_DOCKER_CONTAINER_NAME="tuplex-local-s3-minio";
static const int MINIO_S3_ENDPOINT_PORT=9000;
static const int MINIO_S3_CONSOLE_PORT=9001;

std::tuple<Aws::Auth::AWSCredentials, Aws::Client::ClientConfiguration> local_s3_credentials(const std::string& access_key=MINIO_ACCESS_KEY, const std::string& secret_key=MINIO_SECRET_KEY, int port=MINIO_S3_ENDPOINT_PORT) {
    Aws::Auth::AWSCredentials credentials(access_key.c_str(), secret_key.c_str(), "");
    Aws::Client::ClientConfiguration config;
    config.endpointOverride = "http://localhost:" + std::to_string(port);
    config.enableEndpointDiscovery = false;
    config.verifySSL = false;
    config.connectTimeoutMs = 1500; // 1.5s timeout (local machine)
    return std::make_tuple(credentials, config);
}

bool start_local_s3_server() {
    using namespace std;
    stringstream ss;
    // General form is docker run [OPTIONS] IMAGE [COMMAND] [ARG...]
    ss<<"docker run -p 9000:"<<MINIO_S3_ENDPOINT_PORT<<" -p 9001:"<<MINIO_S3_CONSOLE_PORT
      <<" -e \"MINIO_ROOT_USER="<<MINIO_ACCESS_KEY<<"\""
      <<" -e \"MINIO_ROOT_PASSWORD="<<MINIO_SECRET_KEY<<"\""
      <<" --name \""<<MINIO_DOCKER_CONTAINER_NAME<<"\""
      <<" -d " // detached mode.
      <<" quay.io/minio/minio server /data --console-address \":"<<MINIO_S3_CONSOLE_PORT<<"\"";

    Pipe p(ss.str());
    p.pipe();
    if(p.retval() != 0) {
        cerr<<"Failed starting local s3 server: "<<p.stderr()<<endl;
        return false;
    } else {
        cout<<"Started local s3 server."<<endl;
#ifndef NDEBUG
        cout<<p.stdout()<<endl;
#endif
        return true;
    }
}

bool stop_container(const std::string& container_name) {
    using namespace std;
    stringstream ss;
    ss<<"docker stop "<<container_name;
    Pipe p(ss.str());
    p.pipe();
    if(p.retval() != 0) {
        cerr<<"Failed stopping container "<<container_name<<": "<<p.stderr()<<endl;
        return false;
    } else {
        cout<<"Stopped container "<<container_name<<"."<<endl;
#ifndef NDEBUG
        cout<<p.stdout()<<endl;
#endif
        return true;
    }
}

bool remove_container(const std::string& container_name) {
    using namespace std;
    stringstream ss;
    ss<<"docker rm "<<container_name;
    Pipe p(ss.str());
    p.pipe();
    if(p.retval() != 0) {
        cerr<<"Failed removing container "<<container_name<<": "<<p.stderr()<<endl;
        return false;
    } else {
        cout<<"Removed container "<<container_name<<"."<<endl;
#ifndef NDEBUG
        cout<<p.stdout()<<endl;
#endif
        return true;
    }
}

std::vector<std::string> list_containers(bool only_active=false) {
    using namespace std;
    stringstream ss;
    ss<<"docker ps ";
    if(!only_active)
        ss<<" -a ";
    ss<<" --format=json";

    Pipe p(ss.str());
    p.pipe();
    if(p.retval() != 0) {
        throw std::runtime_error("Failed to list docker containers: " + p.stderr());
    } else {
        auto json_str = p.stdout();

        auto j = nlohmann::json::parse(json_str);

        // TODO: parse the JSON here, it's ndjson.
        return {};
    }
}

bool stop_local_s3_server() {
    using namespace std;

    if(!stop_container(MINIO_DOCKER_CONTAINER_NAME))
        return false;
    if(!remove_container(MINIO_DOCKER_CONTAINER_NAME))
        return false;

    return true;
}

class S3LocalTests : public ::testing::Test {
protected:
    std::string testName;


    // Per Test setup
    void SetUp() override {
        // Check if docker is installed and S3 minio is up and running. If not, skip test.
        if(!is_docker_installed())
            GTEST_SKIP() << "Docker not found.";
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

        cout<<"Starting local MinIO test."<<endl;
        auto data_location = minio_data_location();

        cout<<"MinIO data location: "<<data_location<<endl;

        cout<<"Starting MinIO docker container"<<endl;
        auto rc = start_local_s3_server();
        ASSERT_TRUE(rc);

//        // init S3 file system
//        auto cred = AWSCredentials::get();
//        NetworkSettings ns;
//        initAWS(cred, ns, true);
//        VirtualFileSystem::addS3FileSystem(cred.access_key, cred.secret_key, cred.session_token, cred.default_region, ns, false, true);
//        testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
    }

    static void TearDownTestSuite() {
        using namespace std;
        using namespace tuplex;

        cout<<"Stopping local MinIO server";
        stop_local_s3_server();

        cout<<"Shutting down AWSSDK"<<endl;
        shutdownAWS();
    }
};



TEST_F(S3LocalTests, BasicConnectWithListBucket) {
    using namespace std;
    using namespace tuplex;

    // test with aws s3 client
    Aws::Auth::AWSCredentials credentials;
    Aws::Client::ClientConfiguration config;
    std::tie(credentials, config) = local_s3_credentials();

    std::shared_ptr<Aws::S3::S3Client> client = make_shared<Aws::S3::S3Client>(credentials, config);

    auto outcome = client->ListBuckets();
    EXPECT_TRUE(outcome.IsSuccess());

    if(outcome.IsSuccess()) {
        cout<<"S3 request success:"<<endl;
        auto buckets = outcome.GetResult().GetBuckets();
        for(auto entry : buckets) {
            cout<<"-- Found bucket: s3://" + std::string(entry.GetName().c_str())<<endl;
        }
    } else {
        cerr<<"Failed listing buckets. Details: " + std::string(outcome.GetError().GetMessage().c_str());
    }
}
