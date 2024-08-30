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
#include <aws/s3/model/CreateBucketRequest.h>
#include <nlohmann/json.hpp>
#include <boost/interprocess/streams/bufferstream.hpp>

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
    // need to disable signing https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html.
    config.verifySSL = false;
    config.connectTimeoutMs = 1500; // 1.5s timeout (local machine)
    return std::make_tuple(credentials, config);
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
        string json_str = p.stdout();
        std::vector<std::string> names;
        std::string line;
        std::stringstream input; input<<json_str;
        while(std::getline(input, line)) {
            auto j = nlohmann::json::parse(line);

            names.push_back(j["Names"].get<std::string>());
        }

        return names;
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

bool start_local_s3_server() {
    using namespace std;

    // check if container with the name already exists, if so stop & remove.
    auto container_names = list_containers();
    if(std::find(container_names.begin(), container_names.end(), MINIO_DOCKER_CONTAINER_NAME) != container_names.end()) {
        cout<<"Found existing oontainer named "<<MINIO_DOCKER_CONTAINER_NAME<<", stopping and removing container."<<endl;
        stop_container(MINIO_DOCKER_CONTAINER_NAME);
        remove_container(MINIO_DOCKER_CONTAINER_NAME);
    }

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



bool stop_local_s3_server() {
    using namespace std;

    if(!stop_container(MINIO_DOCKER_CONTAINER_NAME))
        return false;
    if(!remove_container(MINIO_DOCKER_CONTAINER_NAME))
        return false;

    return true;
}

static const std::string LOCAL_TEST_BUCKET_NAME="local-bucket";

class S3LocalTests : public ::testing::Test {
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
        initAWSSDK();

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
    }

    static void TearDownTestSuite() {
        using namespace std;
        using namespace tuplex;

        cout<<"Stopping local MinIO server"<<endl;
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

        EXPECT_EQ(buckets.size(), 1); // only local test bucket should have been created so far.
    } else {
        cerr<<"Failed listing buckets. Details: " + std::string(outcome.GetError().GetMessage().c_str());
        ASSERT_TRUE(false);
    }
}

TEST_F(S3LocalTests, BasicPut) {
    using namespace std;
    using namespace tuplex;

    string buffer= "test123!";

    auto& s3_client = VirtualFileSystem::getS3FileSystemImpl()->client();

    Aws::S3::Model::PutObjectRequest putObjectRequest;
    putObjectRequest.SetBucket(LOCAL_TEST_BUCKET_NAME);
    putObjectRequest.SetKey("test-file-123.txt");

    putObjectRequest.SetContentLength(buffer.size() + 1); // ok
    // Amazon specific headers (?)
    //putObjectRequest.SetRequestPayer(Aws::S3::Model::RequestPayer::NOT_SET); // <-- breaks minio

    auto stream = std::shared_ptr<Aws::IOStream>(new boost::interprocess::bufferstream((char*)buffer.c_str(), buffer.size() + 1));
    putObjectRequest.SetBody(stream);
    auto outcome = s3_client.PutObject(putObjectRequest);
    EXPECT_TRUE(outcome.IsSuccess());

    if(outcome.IsSuccess()) {
        cout<<"S3 put request success:"<<endl;
    } else {
        cerr << "PutObject error: " <<
                  outcome.GetError().GetExceptionName() << " " <<
                  outcome.GetError().GetMessage() << std::endl;
        cerr<<"Failed putting object. Details: " + std::string(outcome.GetError().GetMessage().c_str());
    }
}

TEST_F(S3LocalTests, BasicFileWriteAndRead) {
    using namespace tuplex;
    using namespace std;

    // simple test to write a file, and read it back using Tuplex VFS infrastructure.
    URI uri("s3://" + LOCAL_TEST_BUCKET_NAME + "/simple-file.txt");
    string test_data = "Hello world!";

    auto vfs = VirtualFileSystem::fromURI(uri);

    auto vf = vfs.open_file(uri, VirtualFileMode::VFS_WRITE | VirtualFileMode::VFS_TEXTMODE);
    ASSERT_TRUE(vf);
    vf->write(test_data.c_str(), test_data.size() + 1);
    vf->close();

    // read back
    vf = vfs.open_file(uri, VirtualFileMode::VFS_READ | VirtualFileMode::VFS_TEXTMODE);
    ASSERT_TRUE(vf);
    auto file_size = vf->size();
    EXPECT_EQ(file_size, test_data.size() + 1);
    char *buf = new char[file_size];
    size_t bytes_read = 0;
    vf->read(buf, file_size, &bytes_read);
    EXPECT_EQ(bytes_read, file_size);

    EXPECT_EQ(buf, test_data);
    delete [] buf;
}

TEST_F(S3LocalTests, FileUploadLargerThanInternal) {
    // tests S3 writing capabilities
    using namespace tuplex;

    EXPECT_GE(S3File::DEFAULT_INTERNAL_BUFFER_SIZE(), 0);

    auto internal_buf_size = S3File::DEFAULT_INTERNAL_BUFFER_SIZE();

    // write S3 file that's larger than internal size
    auto test_buf_size = 2 * internal_buf_size;
    auto test_buf = new uint8_t[test_buf_size];
    memset(test_buf, 42, test_buf_size);

    auto s3_test_path = "s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/larger_than_internal_buf.bin";

    auto vfs = VirtualFileSystem::fromURI(URI("s3://"));

    // write parts...
    auto file = vfs.open_file(s3_test_path, VirtualFileMode::VFS_OVERWRITE);
    ASSERT_TRUE(file);
    file->write(test_buf, test_buf_size);
    file->close();

    // check file was written correctly
    uint64_t file_size;
    vfs.file_size(s3_test_path, file_size);
    EXPECT_EQ(file_size, test_buf_size);
}

TEST_F(S3LocalTests, MimickError) {
    // tests S3 writing capabilities
    using namespace tuplex;

    EXPECT_GE(S3File::DEFAULT_INTERNAL_BUFFER_SIZE(), 0);

    auto internal_buf_size = S3File::DEFAULT_INTERNAL_BUFFER_SIZE();

    // write S3 file that's larger than internal size
    size_t test_buf_size = 1.2 * internal_buf_size;
    auto test_buf = new uint8_t[test_buf_size];
    memset(test_buf, 42, test_buf_size);

    auto s3_test_path = "s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/mimick.bin";

    auto vfs = VirtualFileSystem::fromURI(URI("s3://"));

    // write parts...
    auto file = vfs.open_file(s3_test_path, VirtualFileMode::VFS_OVERWRITE);
    ASSERT_TRUE(file);
    int64_t test_num = 20;
    file->write(&test_num, 8);
    file->write(&test_num, 8);
    file->write(test_buf, test_buf_size);
    file->close();

    // check file was written correctly
    uint64_t file_size;
    vfs.file_size(s3_test_path, file_size);
    EXPECT_EQ(file_size, test_buf_size + 2 * 8);
}

TEST_F(S3LocalTests, FileUploadMultiparts) {
    // tests S3 writing capabilities
    using namespace tuplex;

    EXPECT_GE(S3File::DEFAULT_INTERNAL_BUFFER_SIZE(), 0);

    auto internal_buf_size = S3File::DEFAULT_INTERNAL_BUFFER_SIZE();

    // write S3 file that's larger than internal size
    size_t test_buf_size = 2 * internal_buf_size;
    auto test_buf = new uint8_t[test_buf_size];
    memset(test_buf, 42, test_buf_size);

    auto s3_test_path = "s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/multiparts.bin";

    auto vfs = VirtualFileSystem::fromURI(URI("s3://"));

    // write parts...
    auto file = vfs.open_file(s3_test_path, VirtualFileMode::VFS_OVERWRITE);
    ASSERT_TRUE(file);

    // test some edge cases when writing

    size_t part_size = 0.75 * internal_buf_size;
    file->write(test_buf, 0);
    file->write(test_buf, part_size);
    file->write(test_buf + part_size, internal_buf_size);
    file->write(test_buf + part_size + internal_buf_size, test_buf_size - (part_size + internal_buf_size));
    file->close();

    // check file was written correctly
    uint64_t file_size;
    vfs.file_size(s3_test_path, file_size);
    EXPECT_EQ(file_size, test_buf_size);
}

TEST_F(S3LocalTests, FileSeek) {
    using namespace tuplex;

    // test file, write some stuff to it.
    URI file_uri("s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/test.bin");
    auto file = VirtualFileSystem::open_file(file_uri, VirtualFileMode::VFS_OVERWRITE);

    // write 5 numbers
    for(int i = 0; i < 5; ++i) {
        int64_t x = i;
        file->write(&x, sizeof(int64_t));
    }
    file->close();
    file = nullptr;

    // 1. Basic Read of S3 file.
    // perform a couple checks
    file = VirtualFileSystem::open_file(file_uri, VirtualFileMode::VFS_READ);
    ASSERT_TRUE(file);
    EXPECT_EQ(file->size(), 5 * sizeof(int64_t));
    // read contents
    int64_t *buf = new int64_t[10];
    memset(buf, 0, sizeof(int64_t) * 10);
    size_t nbytes_read = 0;
    file->readOnly(buf, 5 * sizeof(int64_t), &nbytes_read);
    EXPECT_EQ(nbytes_read, 5 * sizeof(int64_t));
    for(int i = 0; i < 10; ++i) {
        if(i < 5)
            EXPECT_EQ(buf[i], i);
        else
            EXPECT_EQ(buf[i], 0);
    }
    file->close();

    // now perform file seeking!
    file = VirtualFileSystem::open_file(file_uri, VirtualFileMode::VFS_READ);
    ASSERT_TRUE(file);
    // seek forward 8 bytes!
    file->seek(sizeof(int64_t));

    // now read in bytes
    memset(buf, 0, sizeof(int64_t) * 5);
    file->readOnly(buf, 5 * sizeof(int64_t), &nbytes_read);
    EXPECT_EQ(nbytes_read, 4 * sizeof(int64_t));
    for(int i = 1; i < 10; ++i) {
        if(i < 5)
            EXPECT_EQ(buf[i - 1], i);
        else
            EXPECT_EQ(buf[i - 1], 0);
    }
}