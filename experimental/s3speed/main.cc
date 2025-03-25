#include <iostream>
#include <fstream>
#include <memory>
#include <string>

#include "lyra.hpp"
#include "md5.h"

#include <chrono>

// snippet-start:[s3-crt.cpp.bucket_operations.list_create_delete]
#include <iostream>
#include <fstream>
#include <aws/core/Aws.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/logging/CRTLogSystem.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/CreateBucketRequest.h>
#include <aws/s3-crt/model/BucketLocationConstraint.h>
#include <aws/s3-crt/model/DeleteBucketRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/DeleteObjectRequest.h>
#include <aws/s3-crt/model/HeadObjectRequest.h>
#include <aws/core/utils/UUID.h>

#include <aws/core/utils/threading/Executor.h>
#include <aws/transfer/TransferManager.h>
#include <aws/transfer/TransferHandle.h>
#include <aws/s3/S3Client.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/core/utils/StringUtils.h>

#include <aws/s3/S3Client.h>

class Timer {
private:
    std::chrono::high_resolution_clock::time_point _start;
public:

    Timer() {
        reset();
    }

    // nanoseconds since 1970
    static int64_t currentTimestamp();

    /*!
     * returns time since start of the timer in seconds
     * @return time in seconds
     */
    double time() {
        auto stop = std::chrono::high_resolution_clock::now();

        double duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - _start).count() / 1000000000.0;
        return duration;
    }

    void reset() {
        _start = std::chrono::high_resolution_clock::now();
    }
};

using namespace std;

// use md5 from:
// http://www.zedwood.com/article/cpp-md5-function

static const char ALLOCATION_TAG[] = "s3-crt-demo";

// Get the Amazon S3 object from the bucket.
bool GetObject(const Aws::S3Crt::S3CrtClient& s3CrtClient, const Aws::String& bucketName, const Aws::String& objectKey) {

    std::cout << "Getting object: \"" << objectKey << "\" from bucket: \"" << bucketName << "\" ..." << std::endl;
    Aws::S3Crt::Model::GetObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(objectKey);
    request.SetRequestPayer(Aws::S3Crt::Model::RequestPayer::requester);
    Aws::S3Crt::Model::GetObjectOutcome outcome = s3CrtClient.GetObject(request);

    if (outcome.IsSuccess()) {
       //Uncomment this line if you wish to have the contents of the file displayed. Not recommended for large files
       // because it takes a while.
       // std::cout << "Object content: " << outcome.GetResult().GetBody().rdbuf() << std::endl << std::endl;
       std::cout<<"GOT OBJECT"<<std::endl;
        return true;
    }
    else {
        std::cout << "GetObject error:\n" << outcome.GetError() << std::endl << std::endl;

        return false;
    }
}

std::string s3Bucket(const std::string& uri) {
    // validate & throw exception if not valid s3 path
    if(uri.substr(0, 5) != "s3://")
        throw std::runtime_error("S3 path " + uri + " must start with s3:// " + "started with ");
    auto idx = uri.substr(5).find_first_of('/');
    return uri.substr(5, idx);
}

std::string s3Key(const std::string& uri) {
    // validate & throw exception if not valid s3 path
    if(uri.substr(0, 5) != "s3://")
        throw std::runtime_error("S3 path " + uri + " must start with s3:// " + "started with ");
    auto idx = uri.substr(5).find_first_of('/');
    if(std::string::npos == idx)
        return "";
    return uri.substr(5 + idx + 1);
}

size_t s3_size_of_uri(Aws::S3Crt::S3CrtClient& client, const std::string& bucket, const std::string& key) {
    Aws::S3Crt::Model::HeadObjectRequest req;
    req.WithBucket(bucket);
    req.WithKey(key);
    auto outcome = client.HeadObject(req);
    auto result = outcome.GetResultWithOwnership();
    return result.GetContentLength();
}

class MyUnderlyingStream : public Aws::IOStream
{
public:
    using Base = Aws::IOStream;
    // Provide a customer-controlled streambuf to hold data from the bucket.
    explicit MyUnderlyingStream(std::streambuf* buf)
            : Base(buf)
    {}

    ~MyUnderlyingStream() override = default;
};

void download_to_buffer(uint8_t* buffer, size_t buffer_size, const std::string& bucket, const std::string& key) {
    auto s3_client = Aws::MakeShared<Aws::S3::S3Client>("S3Client");
    auto executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("executor", 25);
    Aws::Transfer::TransferManagerConfiguration transfer_config(executor.get());
    transfer_config.s3Client = s3_client;

    // The local variable 'streamBuffer' is captured by reference in a lambda.
    // It must persist until all downloading by the 'transfer_manager' is complete.
    Aws::Utils::Stream::PreallocatedStreamBuf streamBuffer(buffer, buffer_size);

    auto transfer_manager = Aws::Transfer::TransferManager::Create(transfer_config);

    auto downloadHandle = transfer_manager->DownloadFile(bucket,
                                                         key,
                                                         [&]() { //Define a lambda expression for the callback method parameter to stream back the data.
                                                             return Aws::New<MyUnderlyingStream>("TestTag", &streamBuffer);
                                                         });
    downloadHandle->WaitUntilFinished();// Block calling thread until download is complete.
    auto downStat = downloadHandle->GetStatus();
    if (downStat != Aws::Transfer::TransferStatus::COMPLETED)
    {
        auto err = downloadHandle->GetLastError();
        std::cout << "File download failed:  " << err.GetMessage() << std::endl;
    }
    std::cout << "File download to memory finished."  << std::endl;
}

int main(int argc, char* argv[]) {
   std::string file_uri = "s3://tuplex-public/data/github_daily/2020-10-15.json";
   unsigned int num_tries = 1; // number of tries using AWS S3 client
   bool show_help = false;

   // construct CLI
   auto cli = lyra::cli();
   cli.add_argument(lyra::help(show_help));
   cli.add_argument(lyra::opt(num_tries, "num_tries").name("-n").name("--num-tries").help("how often to run S3 access."));
   cli.add_argument(lyra::opt(file_uri, "file-uri").name("-i").name("--input-uri").help("S3 URI from where to download test file from."));

   auto result = cli.parse({argc, argv});
   if(!result) {
       cerr<<"Error parsing command line: "<<result.errorMessage()<<std::endl;
       return 1;
   }

   if(show_help) {
       cout<<cli<<endl;
       return 0;
   }

   Aws::SDKOptions options;
   //Turn on logging.
   options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
   // Override the default log level for AWS common runtime libraries to see multipart upload entries in the log file.
   options.loggingOptions.crt_logger_create_fn = []() {
       return Aws::MakeShared<Aws::Utils::Logging::DefaultCRTLogSystem>(ALLOCATION_TAG, Aws::Utils::Logging::LogLevel::Debug);
   };

   // Uncomment the following code to override default global client bootstrap for AWS common runtime libraries.
   // options.ioOptions.clientBootstrap_create_fn = []() {
   //     Aws::Crt::Io::EventLoopGroup eventLoopGroup(0 /* cpuGroup */, 18 /* threadCount */);
   //     Aws::Crt::Io::DefaultHostResolver defaultHostResolver(eventLoopGroup, 8 /* maxHosts */, 300 /* maxTTL */);
   //     auto clientBootstrap = Aws::MakeShared<Aws::Crt::Io::ClientBootstrap>(ALLOCATION_TAG, eventLoopGroup, defaultHostResolver);
   //     clientBootstrap->EnableBlockingShutdown();
   //     return clientBootstrap;
   // };

   // Uncomment the following code to override default global TLS connection options for AWS common runtime libraries.
   // options.ioOptions.tlsConnectionOptions_create_fn = []() {
   //     Aws::Crt::Io::TlsContextOptions tlsCtxOptions = Aws::Crt::Io::TlsContextOptions::InitDefaultClient();
   //     Aws::Crt::Io::TlsContext tlsContext(tlsCtxOptions, Aws::Crt::Io::TlsMode::CLIENT);
   //     return Aws::MakeShared<Aws::Crt::Io::TlsConnectionOptions>(ALLOCATION_TAG, tlsContext.NewConnectionOptions());
   // };

   uint8_t* DATA_BUFFER = nullptr;

   Aws::InitAPI(options);
   {

       // // TODO: Add a large file to your executable folder, and update file_name to the name of that file.
       // //    File "ny.json" (1940 census data; https://www.archives.gov/developer/1940-census#accessportiondataset)
       // //    is an example data file large enough to demonstrate multipart upload.
       // // Download "ny.json" from https://nara-1940-census.s3.us-east-2.amazonaws.com/metadata/json/ny.json
       // Aws::String file_name = "ny.json";

       //TODO: Set to your account AWS Region.
       Aws::String region = Aws::Region::US_EAST_1;

       //The object_key is the unique identifier for the object in the bucket.
       Aws::String object_key = s3Key(file_uri);

       // Create a globally unique name for the new bucket.
       // Format: "my-bucket-" + lowercase UUID.
       Aws::String uuid = Aws::Utils::UUID::RandomUUID();
       Aws::String bucket_name = s3Bucket(file_uri);

       const double throughput_target_gbps = 5;
       const uint64_t part_size = 8 * 1024 * 1024; // 8 MB.

       Aws::S3Crt::ClientConfiguration config;
       config.region = region;
       config.throughputTargetGbps = throughput_target_gbps;
       config.partSize = part_size;

       Aws::S3Crt::S3CrtClient s3_crt_client(config);

       //Use BucketLocationConstraintMapper to get the BucketLocationConstraint enum from the region string.
       //https://sdk.amazonaws.com/cpp/api/0.14.3/namespace_aws_1_1_s3_1_1_model_1_1_bucket_location_constraint_mapper.html#a50d4503d3f481022f969eff1085cfbb0
       Aws::S3Crt::Model::BucketLocationConstraint locConstraint = Aws::S3Crt::Model::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(region);


       // run access
       num_tries = std::max(1u, std::min(num_tries, 25u));
       cout<<"Starting benchmark with "<<num_tries<<" tries::"<<endl;
       for(unsigned i = 0; i < num_tries; ++i) {
          cout<<"Run "<<(i+1)<<"/"<<num_tries<<endl;

          Timer timer;

          cout<<"Downloading "<<file_uri<<"..."<<endl;

          // 1. send HEAD request out to get size.
          size_t uri_size = s3_size_of_uri(s3_crt_client, bucket_name, object_key);
          cout<<timer.time()<<" HEAD object request success, size of content: "<<uri_size<<"B"<<endl;

          // 2. Allocate buffer of content size to write output
          if(DATA_BUFFER) {
              delete [] DATA_BUFFER;
              DATA_BUFFER = nullptr;
          }
          DATA_BUFFER = new uint8_t[uri_size];
          cout<<timer.time()<<" Buffer alloc succeeded."<<endl;

          // 3. Download to buffer via transfer manager.
          download_to_buffer(DATA_BUFFER, uri_size, bucket_name, object_key);
          cout<<timer.time()<<" Download done."<<endl;

          // 4. compute md5 from buffer
          MD5 md5;
          auto data = string("Hello world");
          md5.update(DATA_BUFFER, uri_size);
          md5.finalize();
          cout<<timer.time()<<" MD5 digest: "<<md5.hexdigest()<<endl;

          // Use S3 crt.
          cout<<" took "<<timer.time()<<"s "<<endl;

       }
   }
   Aws::ShutdownAPI(options);


  return 0;
}
