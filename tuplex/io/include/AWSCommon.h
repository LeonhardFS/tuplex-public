//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifdef BUILD_WITH_AWS
#ifndef TUPLEX_AWSCOMMON_H
#define TUPLEX_AWSCOMMON_H

#include <string>
#include <unordered_map>
#include <cstdlib>
#include <vector>

#include <Utils.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include "nlohmann/json.hpp"

namespace tuplex {

    struct AWSCredentials {
        std::string access_key;
        std::string secret_key;
        std::string session_token; // required for temp credentials like the ones used in Lambda
        std::string default_region;

        static AWSCredentials get();
    };

    // use base64 library from https://github.com/ReneNyffenegger/cpp-base64

    inline std::shared_ptr<Aws::IOStream> stringToAWSStream(const std::string &str, const std::string &tag = "tuplex") {
        auto input = Aws::MakeShared<Aws::StringStream>(tag.c_str());

        *input << str.c_str();
        input->flush();
        return input;
    }

    /*!
     * update clientConfig with given Network settings.
     * @param ns network settings
     * @param config AWS clientConfig
     */
    extern void applyNetworkSettings(const NetworkSettings& ns, Aws::Client::ClientConfiguration& config);

    /*!
    calls Aws::InitAPI()
    */
    extern bool initAWSSDK(Aws::Utils::Logging::LogLevel log_level=Aws::Utils::Logging::LogLevel::Warn);

    /*!
     * initializes AWS SDK globally (lazy) and add S3 FileSystem.
     * @return true if initializing, else false
     */
    extern bool initAWS(const AWSCredentials& credentials=AWSCredentials::get(), const NetworkSettings& ns=NetworkSettings(), bool requesterPay=false);

    /*!
     * shuts down AWS SDK (freeing resources).
     */
    extern void shutdownAWS();

    /*!
     * validates zone string.
     * @param zone
     * @return true/false.
     */
    extern bool isValidAWSZone(const std::string& zone);


    inline std::string decodeAWSBase64(const std::string& log) {
        std::stringstream ss;
        // Decode the result header to see requested log information
        auto byteLogResult = Aws::Utils::HashingUtils::Base64Decode(log.c_str());
        for (unsigned i = 0; i < byteLogResult.GetLength(); i++)
            ss << byteLogResult.GetItem(i);
        auto logTail =  ss.str();
        return logTail;
    }

    inline std::string encodeAWSBase64(const std::string& data) {
        // TODO: Use simdutf https://github.com/simdutf/simdutf?tab=readme-ov-file#base64 in the future.
        return Aws::Utils::HashingUtils::Base64Encode(Aws::Utils::ByteBuffer(
                reinterpret_cast<const unsigned char *>(data.c_str()), data.size())).c_str();
    }

    inline std::shared_ptr<Aws::S3::S3Client> s3_client_from_credentials_and_config(const Aws::Auth::AWSCredentials& credentials=Aws::Auth::AWSCredentials(),
                                                                                    const Aws::Client::ClientConfiguration& config=Aws::Client::ClientConfiguration()) {
#if AWS_SDK_VERSION_MINOR <= 9
        // AWS SDK sometimes has changes to constructors, fix that here.
        return std::make_shared<Aws::S3::S3Client>(credentials, config);
#else
        // No endpoint provider.
         return std::make_shared<Aws::S3::S3Client>(credentials, nullptr, config);
#endif
    }


    inline std::ostream& operator<<(std::ostream &os, const Aws::Client::ClientConfiguration& config) {
        nlohmann::json j;
        j["userAgent"] = config.userAgent;
        j["scheme"] = config.scheme == Aws::Http::Scheme::HTTP ? "http" : "https";
        j["useDualStack"] = config.useDualStack;
        // j["useFIPS"] = config.
        j["maxConnections"] = config.maxConnections;
        j["httpRequestTimeoutMs"] = config.httpRequestTimeoutMs;
        j["requestTimeoutMs"] = config.requestTimeoutMs;
        j["connectTimeoutMs"] = config.connectTimeoutMs;
        j["enableTcpKeepAlive"] = config.enableTcpKeepAlive;
        j["tcpKeepAliveIntervalMs"] = config.tcpKeepAliveIntervalMs;
        j["lowSpeedLimit"] = config.lowSpeedLimit;
        j["retryStrategy"] = !config.retryStrategy ? "null" : "<unknown>";
        j["endpointOverride"] = config.endpointOverride;
        //  j["allowSystemProxy"] = config.allowSystemProxy;
        j["proxyScheme"] = config.proxyScheme == Aws::Http::Scheme::HTTP ? "http" : "https";
        j["proxyHost"] = config.proxyHost;
        j["proxyPort"] = config.proxyPort;
        j["proxyUserName"] = config.proxyUserName;
        j["proxyPassword"] = config.proxyPassword;
        j["proxySSLCertPath"] = config.proxySSLCertPath;
        j["proxySSLCertType"] = config.proxySSLCertType;
        j["proxySSLKeyPath"] = config.proxySSLKeyPath;
        j["proxySSLKeyType"] = config.proxySSLKeyType;
        j["proxySSLKeyPassword"] = config.proxySSLKeyPassword;
        // j["nonProxyHosts"] = std::vector<std::string>{config.nonProxyHosts;
        j["verifySSL"] = config.verifySSL;
        j["caPath"] = config.caPath;
        j["caFile"] = config.caFile;
        // j["proxyCaFile"] = config.proxyCaFile;
        os << j.dump(2); // print as JSON
        return os;
    }

    static const std::string AWS_LAMBDA_ENDPOINT_KEY = "AWS_ENDPOINT_URL_LAMBDA";

    // cf. src/aws-cpp-sdk-core/source/client/DefaultRetryStrategy.cpp
    // A lot of straightforward codes get retried for no reason, i.e. Curl code 6: Couldn't resolve host name
    // Handle these better here.
    class ModifiedRetryStrategy : public Aws::Client::DefaultRetryStrategy {
    public:

        bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attemptedRetries) const override;

    };
}

// Amazon frequently changes the parameters of lambda functions,
// this here are a couple constants to update them
// current state: https://aws.amazon.com/about-aws/whats-new/2020/12/aws-lambda-supports-10gb-memory-6-vcpu-cores-lambda-functions/#:~:text=Events-,AWS%20Lambda%20now%20supports%20up%20to%2010%20GB%20of%20memory,vCPU%20cores%20for%20Lambda%20Functions&text=AWS%20Lambda%20customers%20can%20now,previous%20limit%20of%203%2C008%20MB.
// cf. https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html
// also check https://www.fourtheorem.com/blog/lambda-10gb for quotas on vCPU, i.e. how much is provisioned.
// https://docs.aws.amazon.com/lambda/latest/dg/configuration-function-common.html

// i.e. at 1,769 MB, a function has the equivalent of one vCPU (one vCPU-second of credits per second).
#define AWS_MINIMUM_LAMBDA_MEMORY_MB 128ul
// how much memory a function for Tuplex should have
#define AWS_MINIMUM_TUPLEX_MEMORY_REQUIREMENT_MB 384ul

// 10240MB, i.e. 10GB
#define AWS_MAXIMUM_LAMBDA_MEMORY_MB 10240ul
// 15 min
#define AWS_MAXIMUM_LAMBDA_TIMEOUT 900ul
// minumum Tuplex requirement (5s?)
#define AWS_MINIMUM_TUPLEX_TIMEOUT_REQUIREMENT 5ul

// note(Leonhard): I think the number of available cores/threads is dependent on the memory size
#define AWS_MAXIMUM_LAMBDA_THREADS 6

// may change, the minimum AWS limit for unreserved concurrency functions. Also a limit how much can get executed...
#define AWS_MINIMUM_UNRESERVED_CONCURRENCY 100

// the 64MB increase limit seems to have been changed now...

#endif //TUPLEX_AWSCOMMON_H
#endif
