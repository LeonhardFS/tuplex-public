//
// Created by leonhards on 8/26/24.
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

using namespace tuplex;

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

namespace tuplex {
    void github_pipeline(Context& ctx, const std::string& input_pattern, const std::string& output_path) {
        using namespace std;
        // start pipeline incl. output
        auto repo_id_code = "def extract_repo_id(row):\n"
                            "    if 2012 <= row['year'] <= 2014:\n"
                            "        \n"
                            "        if row['type'] == 'FollowEvent':\n"
                            "            return row['payload']['target']['id']\n"
                            "        \n"
                            "        if row['type'] == 'GistEvent':\n"
                            "            return row['payload']['id']\n"
                            "        \n"
                            "        repo = row.get('repository')\n"
                            "        \n"
                            "        if repo is None:\n"
                            "            return None\n"
                            "        return repo.get('id')\n"
                            "    else:\n"
                            "        repo =  row.get('repo')\n"
                            "        if repo:\n"
                            "            return repo.get('id')\n"
                            "        else:\n"
                            "            return None\n";

        ctx.json(input_pattern, true, true)
                .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
                .withColumn("repo_id", UDF(repo_id_code))
                .filter(UDF("lambda x: x['type'] == 'ForkEvent'")) // <-- this is challenging to push down.
                .withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
                .withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
                .selectColumns(vector<string>{"type", "repo_id", "year", "number_of_commits"})
                .tocsv(output_path);
    }

    ContextOptions microIntegOptions() {
        auto co = ContextOptions::defaults();

        co.set("tuplex.backend", "worker");
        co.set("tuplex.experimental.opportuneCompilation", "false");
        return co;
    }
}



TEST_F(S3LocalTests, TestGithubPipeline) {
    using namespace tuplex;
    using namespace std;

    // Test github pipeline with small sample files.
    // Step 1: Upload all files into bucket.
    // test file, write some stuff to it.

    auto files_to_upoad = glob("../resources/hyperspecialization/github_daily/*.json.sample");
    cout<<"Found "<<pluralize(files_to_upoad.size(), "test file")<<" to upload to local S3 storage."<<endl;

    Timer timer;
    cout<<"Uploading files..."<<endl;
    for(const auto&path : files_to_upoad) {
        auto target_uri = "s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/" + URI(path).base_name();
        VirtualFileSystem::copy(path, target_uri);
    }
    cout<<"Upload took "<<std::fixed<<std::setprecision(1)<<timer.time()<<"s."<<endl;

    // Step 2: Check files are existing:
    string input_pattern = "s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/" + "*.json.sample";
    auto uris = VirtualFileSystem::fromURI(input_pattern).glob(input_pattern);
    EXPECT_EQ(uris.size(), files_to_upoad.size());

    auto output_path = "s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/" + "output";

    // Step 3: Create Tuplex context according to settings
    auto co = microIntegOptions();
    Context ctx(co);
    runtime::init(co.RUNTIME_LIBRARY().toPath());

    cout<<"Saving files to "<<output_path<<endl;

    // Step 4: execute pipeline (inprocess with default config)
    github_pipeline(ctx, input_pattern, output_path);

    // glob output files (should be equal amount, as 1 request per file)
    auto output_uris = VirtualFileSystem::fromURI(input_pattern).glob(output_path + "/*.csv");

    cout<<"Found "<<pluralize(output_uris.size(), "output file")<<" in local S3 file system."<<endl;

    EXPECT_EQ(output_uris.size(), files_to_upoad.size());

    // Step 5: Check csv counts to make sure these are correct.
    auto total_row_count = csv_row_count_for_pattern(output_path + "/*.csv");
    EXPECT_EQ(total_row_count, 378);
}

TEST_F(S3LocalTests, TestGithubPipelineObjectCompileAndProcess) {
    // This test is similar to TestGithubPipeline, however here the requests are only collected.
    using namespace tuplex;
    using namespace std;

    // Test github pipeline with small sample files.
    // Step 1: Upload all files into bucket.
    // test file, write some stuff to it.

    auto files_to_upoad = glob("../resources/hyperspecialization/github_daily/*.json.sample");
    for(const auto&path : files_to_upoad) {
        auto target_uri = "s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/" + URI(path).base_name();
        VirtualFileSystem::copy(path, target_uri);
    }

    string input_pattern = "s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/" + "*.json.sample";
    auto output_path = "s3://" + LOCAL_TEST_BUCKET_NAME + "/" + testName + "/" + "output";

    // Step 2: Create Tuplex context according to settings
    auto co = microIntegOptions();
    Context ctx(co);
    runtime::init(co.RUNTIME_LIBRARY().toPath());

    // Step 3: Configure backend to only emit requests
    ASSERT_TRUE(ctx.backend());
    auto wb = static_cast<WorkerBackend*>(ctx.backend());
    wb->setRequestMode(true);

    // Step 4: execute pipeline (inprocess with default config)
    github_pipeline(ctx, input_pattern, output_path);

    // Receive pending requests & clear.
    auto requests = wb->pendingRequests(true);

    EXPECT_EQ(requests.size(), files_to_upoad.size());
    ASSERT_FALSE(requests.empty());

    // Compute sizes for each request to see how large they are.
    for(unsigned i = 0; i < requests.size(); ++i) {
        std::string json_str;
        google::protobuf::util::MessageToJsonString(requests[i], &json_str);
        auto bin_str = requests[i].SerializeAsString();
        auto base64_str = encodeAWSBase64(bin_str);
        cout<<"-- request "<<(i + 1)<<":  json: "<<sizeToMemString(json_str.size())<<" bin: "<<sizeToMemString(bin_str.size())<<" base64: "<<sizeToMemString(base64_str.size())<<endl;
    }

    // change mode now of request to only compile and return code as object.
    auto request = requests[0];
    request.set_requestmode(REQUEST_MODE_COMPILE_AND_RETURN_OBJECT_CODE | REQUEST_MODE_COMPILE_ONLY);

    // Issue request to worker (via helper function) and retrieve response.
    // Doesn't matter here whether process or not. Only get the (object) code.
    auto response = process_request_with_worker(co.EXPERIMENTAL_WORKER_PATH(), co.SCRATCH_DIR().toPath(), request);

    // check result code is ok.
    cout<<"Status of request: "<<response.status()<<endl;

    // print stuff
    {
        std::string json_str;
        google::protobuf::util::MessageToJsonString(response, &json_str);
        cout<<"Response\n:"<<json_str<<endl;
    }

    // Now issue request again, but this time with object code provided and skip compilation.
    // @TODO: Need to update types as well?
    request.set_requestmode(REQUEST_MODE_SKIP_COMPILE);
    auto object_code_fast_path = find_resources_by_type(response, ResourceType::OBJECT_CODE_NORMAL_CASE).front().payload();
    request.mutable_stage()->mutable_fastpath()->set_code(object_code_fast_path);
    request.mutable_stage()->mutable_fastpath()->set_codeformat(::messages::CodeFormat::OBJECT_CODE);

    if(!find_resources_by_type(response, ResourceType::OBJECT_CODE_GENERAL_CASE).empty()) {
        auto object_code_slow_path = find_resources_by_type(response, ResourceType::OBJECT_CODE_GENERAL_CASE).front().payload();
        request.mutable_stage()->mutable_slowpath()->set_code(object_code_slow_path);
        request.mutable_stage()->mutable_slowpath()->set_codeformat(::messages::CodeFormat::OBJECT_CODE);
    }

    // Issue request to worker again with object code this time.
    // Because here S3 is configured locally, use WorkerApp in this process.
    response = process_request_with_worker(co.EXPERIMENTAL_WORKER_PATH(), co.SCRATCH_DIR().toPath(), request, false);

    // check result code is ok.
    cout<<"Status of request: "<<response.status()<<endl;nvo

    // glob output files (should be equal amount, as 1 request per file)
    auto output_uris = VirtualFileSystem::fromURI(input_pattern).glob(output_path + "/*.csv");

    cout<<"Found "<<pluralize(output_uris.size(), "output file")<<" in local S3 file system."<<endl;

    // there must be one file now (because of the request).
    EXPECT_EQ(output_uris.size(), 1);

//
//    EXPECT_EQ(output_uris.size(), files_to_upoad.size());
//
//    // Step 5: Check csv counts to make sure these are correct.
//    auto total_row_count = csv_row_count_for_pattern(output_path + "/*.csv");
//    EXPECT_EQ(total_row_count, 378);
}