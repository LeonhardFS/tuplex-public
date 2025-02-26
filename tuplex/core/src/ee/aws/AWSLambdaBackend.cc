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

#include <ee/aws/AWSLambdaBackend.h>
#include <ee/local/LocalBackend.h>

#include <aws/lambda/model/CreateFunctionRequest.h>
#include <aws/lambda/model/DeleteFunctionRequest.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <aws/lambda/model/ListFunctionsRequest.h>
#include <aws/lambda/model/UpdateFunctionConfigurationRequest.h>
#include <aws/lambda/model/UpdateFunctionConfigurationResult.h>
#include <aws/lambda/model/GetFunctionConcurrencyRequest.h>
#include <aws/lambda/model/GetFunctionConcurrencyResult.h>
#include <aws/lambda/model/PutFunctionConcurrencyRequest.h>
#include <aws/lambda/model/PutFunctionConcurrencyResult.h>
#include <aws/lambda/model/GetAccountSettingsRequest.h>
#include <aws/lambda/model/GetAccountSettingsResult.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include <JSONUtils.h>

// only exists in newer SDKs...
// #include <aws/lambda/model/Architecture.h>

// protobuf header
#include <utils/Messages.h>

#include <third_party/base64/base64.h>

#include <google/protobuf/util/json_util.h>
#include <iomanip>

#include <utility>
#include "ee/worker/WorkerBackend.h"

namespace tuplex {

    static size_t compute_total_requests(const std::vector<size_t>& sizes,
                                         size_t minimum_chunk_size,
                                         size_t maximum_chunk_size) {

        // this code here is adapted from AWSLambdaBackend.cc
        // to condense it more, maybe this formulat works. Yet it is somehow off.
        //            size_t p = 0;
        //            for(auto s : sizes) {
        //                // smaller equal then c_min + c_max: 1 request.
        //                if(s <= c_min + c_max)
        //                    p++;
        //                else {
        //                    // integer rounded down
        //                    p += s / c_max;
        //                }
        //            }


        auto n_requests = 0;

        // Go through uris and create initial requests (no TrafoStage fill in yet).
        for(auto uri_size : sizes) {
            // single part?
            if(uri_size <= maximum_chunk_size + minimum_chunk_size) { // Note the add here.
                n_requests++;
            } else {
                // split up. There will be at least two parts. One maximum chunk size, the other at least minimum chunk size.
                // First chunk part will be executed by the worker itself, the others will be self-invoke.
                std::vector<FilePart> parts;
                int64_t remaining_bytes = uri_size;
                size_t current_offset = 0;
                while(remaining_bytes > maximum_chunk_size + minimum_chunk_size) {
                    n_requests++;
                    current_offset += maximum_chunk_size;
                    remaining_bytes -= maximum_chunk_size;
                }
                // add last part, whatever is remaining.
                n_requests++;
            }
        }

        return n_requests;
    }


    size_t find_max_chunk_size(std::vector<size_t> sizes, size_t c_min, size_t parallelism) {
        using namespace std;

        // c_min -> minimum chunk size
        // c_max -> maximum chunk size
        // this is a basic heuristic approach (there's probably an optimization problem/formula for this)

        // check invariant
        for(auto s : sizes)
            assert(s >= c_min);

        // minimum parallelism needed. Else, no solution.
        assert(parallelism >= sizes.size());

        size_t max_size = 0;
        for(auto s: sizes)
            max_size = std::max(s, max_size);

        // Trivial solution: If sizes.size() == parallelism, it is max file size.
        if(sizes.size() == parallelism)
            return max_size;

        // upper/lower bounds for c_max.
        // lower bound is c_min.
        // upper bound can be calculated by max_size.
        size_t c_max_lower = c_min;
        size_t c_max_upper = max_size;

        // check:
        if(c_max_lower >= c_max_upper) {
#ifndef NDEBUG
            cout<<"ill-defined bounds, no solution. Returning c_min."<<endl;
#endif
            return c_min;
        }

        size_t c_max = c_min; // start value.

        // iterations (basically binary search here)
        size_t max_iterations = 40; // 40 steps should be sufficient to solve most scenarios. Could also set time limit.
        for(int i = 0; i < max_iterations; ++i) {
            // Calculate current parallelism p (direct, no request creation yet).
            auto p = compute_total_requests(sizes, c_min, c_max);

            // Current iteration:
#ifndef NDEBUG
            cout<<"i="<<i<<"\tp_target: "<<parallelism<<" p: "<<p<<" c_max: "<<c_max<<" ("<<sizeToMemString(c_max)<<")"<<endl;
#endif
            // adjust variables for finding.
            // smaller c_max -> higher p
            // larger c_max -> lower p
            if(p == parallelism)
                return c_max; // solution found

            if(p > parallelism) {
                c_max_lower = c_max;
                // need larger c_max, less parallelism.
                c_max = (c_max_lower + c_max_upper) / 2;
            } else {
                c_max_upper = c_max;
                // need smaller c_max, more parallelism.
                c_max = (c_max_lower + c_max_upper + 1) / 2; // bias towards max.
            }
        }

        return c_max;
    }

    AwsLambdaBackend::~AwsLambdaBackend() {
        // stop http requests
        if (_client)
            _client->DisableRequestProcessing();

        // delete scratch dir?
        if (_deleteScratchDirOnShutdown) {
            auto vfs = VirtualFileSystem::fromURI(_scratchDir);
            vfs.remove(_scratchDir); // TODO: could optimize this by keeping track of temp files and issuing on shutdown
            // a single multiobject delete request...
        }
    }

    std::shared_ptr<Aws::Lambda::LambdaClient> AwsLambdaBackend::makeClient() {
        // Note: should have only a SINGLE context with Lambda backend...

        // init Lambda client
        Aws::Client::ClientConfiguration clientConfig;

        clientConfig.requestTimeoutMs = _options.AWS_REQUEST_TIMEOUT() * 1000; // conv seconds to ms
        clientConfig.connectTimeoutMs = _options.AWS_CONNECT_TIMEOUT() * 1000; // connection timeout

        // tune client, according to https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/client-config.html
        // note: max connections should not exceed max concurrency if it is below 100, else aws lambda
        // will return toomanyrequestsexception
        clientConfig.maxConnections = std::max(32ul, _options.AWS_MAX_CONCURRENCY());

        // to avoid thread exhaust of system, use pool thread executor with 8 threads
        clientConfig.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(_tag.c_str(),
                                                                                             _options.AWS_NUM_HTTP_THREADS());
        if (_options.AWS_REGION().empty())
            clientConfig.region = _credentials.default_region.c_str();
        else
            clientConfig.region = _options.AWS_REGION().c_str(); // hard-coded here

        auto ns = _options.AWS_NETWORK_SETTINGS();
        if(!_options.AWS_LAMBDA_ENDPOINT().empty()) {
            ns.endpointOverride = _options.AWS_LAMBDA_ENDPOINT();
        } else {
            // verify zone
            if (!isValidAWSZone(clientConfig.region.c_str())) {
                logger().warn("Specified AWS zone '" + std::string(clientConfig.region.c_str()) +
                              "' is not a valid AWS zone. Defaulting to " + _credentials.default_region + " zone.");
                clientConfig.region = _credentials.default_region.c_str();
            }
        }

        //clientConfig.userAgent = "tuplex"; // should be perhaps set as well.
        applyNetworkSettings(ns, clientConfig);

        // change aws settings here
        Aws::Auth::AWSCredentials cred(_credentials.access_key.c_str(),
                                       _credentials.secret_key.c_str(),
                                       _credentials.session_token.c_str());
        auto client = Aws::MakeShared<Aws::Lambda::LambdaClient>(_tag.c_str(), cred, clientConfig);

        Aws::Lambda::Model::ListFunctionsRequest list_req;
        const Aws::Lambda::Model::FunctionConfiguration *fc = nullptr; // holds lambda conf
        Aws::String fc_json_str;
        auto outcome = client->ListFunctions(list_req);
        if (!outcome.IsSuccess()) {
            std::stringstream ss;
            ss << outcome.GetError().GetExceptionName().c_str() << ", "
               << outcome.GetError().GetMessage().c_str();

            throw std::runtime_error("LAMBDA failed to list functions, details: " + ss.str());
        } else {
            // check whether function is contained
            auto funcs = outcome.GetResult().GetFunctions();

            // search for the function of interest
            for (const auto &f: funcs) {
                if (f.GetFunctionName().c_str() == _functionName) {
                    fc_json_str = f.Jsonize().View().WriteCompact();
                    fc = new Aws::Lambda::Model::FunctionConfiguration(Aws::Utils::Json::JsonValue(fc_json_str));
                    break;
                }
            }

            if (!fc)
                throw std::runtime_error("could not find lambda function '" + _functionName + "'");
            else {
                logger().info(
                        "Found AWS Lambda function " + _functionName + " (" + std::to_string(fc->GetMemorySize()) +
                        "MB)");
            }
        }

        // check architecture of function (only newer AWS SDKs support this...)
        {
            using namespace Aws::Utils::Json;
            using namespace Aws::Utils;
            auto fc_json = Aws::Utils::Json::JsonValue(fc_json_str);
            if (fc_json.View().ValueExists("Architectures")) {
                Array<JsonView> architecturesJsonList = fc_json.View().GetArray("Architectures");
                std::vector<std::string> architectures;
                for (unsigned architecturesIndex = 0;
                     architecturesIndex < architecturesJsonList.GetLength(); ++architecturesIndex)
                    architectures.push_back(std::string(architecturesJsonList[architecturesIndex].AsString().c_str()));
                // there should be one architecture
                if (architectures.size() != 1) {
                    logger().warn(
                            "AWS Lambda changed specification, update how to deal with mulit-architecture functions");
                    if (!architectures.empty())
                        _functionArchitecture = architectures.front();
                } else {
                    _functionArchitecture = architectures.front();
                }
            } else {
                _functionArchitecture = "x86_64";
            }
        }

        logger().info("Using Lambda running on " + _functionArchitecture + ".");
#ifdef BUILD_WITH_CEREAL
        logger().info("Lambda client uses Cereal AST serialization format.");
#else
        logger().info("Lambda client uses JSON AST serialization format.");
#endif

        // could also check account limits in case:
        // https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html#Lambda.Client.get_function_configuration

        checkAndUpdateFunctionConcurrency(client, _options.AWS_MAX_CONCURRENCY(), _functionName,
                                          false); // no provisioned concurrency!

        // limit concurrency + mem of function manually (TODO: uncomment for faster speed!), if it doesn't fit options
        // i.e. aws lambda put-function-concurrency --function-name tplxlam --reserved-concurrent-executions $MAX_CONCURRENCY
        //      aws lambda update-function-configuration --function-name tplxlam --memory-size $MEM_SIZE --timeout 60
        // update required?
        bool needToUpdateConfig = false;
        Aws::Lambda::Model::UpdateFunctionConfigurationRequest update_req;
        update_req.SetFunctionName(_functionName.c_str());
        if (fc->GetTimeout() != _lambdaTimeOut) {
            update_req.SetTimeout(_lambdaTimeOut);
            needToUpdateConfig = true;
        }

        if (fc->GetMemorySize() != _lambdaSizeInMB) {
            update_req.SetMemorySize(_lambdaSizeInMB);
            needToUpdateConfig = true;
        }
        if (needToUpdateConfig) {
            logger().info("updating Lambda settings to timeout: " + std::to_string(_lambdaTimeOut) + "s memory: " +
                          std::to_string(_lambdaSizeInMB) + " MB");
            auto outcome = client->UpdateFunctionConfiguration(update_req);
            if (!outcome.IsSuccess()) {
                std::stringstream ss;
                ss << outcome.GetError().GetExceptionName().c_str()
                   << outcome.GetError().GetMessage().c_str();

                throw std::runtime_error("LAMBDA failed update configuration, details: " + ss.str());
            }
            logger().info("Updated Lambda configuration successfully.");
        }

        delete fc;

        return client;
    }

    void AwsLambdaBackend::checkAndUpdateFunctionConcurrency(const std::shared_ptr<Aws::Lambda::LambdaClient> &client,
                                                             size_t concurrency,
                                                             const std::string &functionName,
                                                             bool provisioned) {
        if (provisioned) {
            throw std::runtime_error("Provisioned concurrency not yet supported...");
        } else {
            // check concurrency & adjust (may fail with account limits...)
            Aws::Lambda::Model::GetFunctionConcurrencyRequest req;
            req.SetFunctionName(functionName.c_str());
            auto outcome = client->GetFunctionConcurrency(req);
            if (!outcome.IsSuccess()) {
                logger().error("Failed to retrieve function concurrency");
            } else {
                auto &result = outcome.GetResult();
                _functionConcurrency = result.GetReservedConcurrentExecutions();

                // different than what is desired?
                if (_functionConcurrency != concurrency) {
                    auto concurrency_description = std::to_string(_functionConcurrency);
                    if (0 == _functionConcurrency)
                        concurrency_description = "(no reserved concurrency)";
                    logger().info("Adjusting reserved concurrency from " + concurrency_description + " to " +
                                  std::to_string(concurrency));

                    // update function
                    Aws::Lambda::Model::PutFunctionConcurrencyRequest u_req;
                    u_req.SetFunctionName(functionName.c_str());
                    u_req.SetReservedConcurrentExecutions(concurrency);

                    auto outcome = client->PutFunctionConcurrency(u_req);
                    if (!outcome.IsSuccess()) {
                        // check error
                        auto &error = outcome.GetError();
                        auto statusCode = static_cast<int>(error.GetResponseCode());
                        auto errorMessage = std::string(error.GetMessage().c_str());
                        auto errorName = std::string(error.GetExceptionName().c_str());

                        // check if it was invalid param
                        if (error.GetErrorType() == Aws::Lambda::LambdaErrors::INVALID_PARAMETER_VALUE) {
                            // get max allowed concurrency from account & adjust
                            //Aws::Lambda::Model::
                            Aws::Lambda::Model::GetAccountSettingsRequest a_req;
                            auto outcome = client->GetAccountSettings(a_req);
                            if (!outcome.IsSuccess()) {
                                auto &error = outcome.GetError();
                                logger().error(
                                        "Failed to retrieve account settings, can not adjust invalid concurrency configuration. Details: " +
                                        std::string(error.GetMessage().c_str()));
                                return;
                            } else {
                                // check what the limit is, adjust accordingly
                                auto &result = outcome.GetResult();
                                auto unreserved_capacity = result.GetAccountLimit().GetUnreservedConcurrentExecutions();

                                // unreserved must be at least 100
                                if (unreserved_capacity > AWS_MINIMUM_UNRESERVED_CONCURRENCY)
                                    unreserved_capacity -= AWS_MINIMUM_UNRESERVED_CONCURRENCY;
                                else {
                                    throw std::runtime_error(
                                            "No account capacity remaining, can't assign any concurrency to LAMBDA function " +
                                            _functionName);
                                }

                                auto max_account_concurrency = result.GetAccountLimit().GetConcurrentExecutions();
                                auto concurrency_to_assign = unreserved_capacity + _functionConcurrency;
                                std::stringstream ss;
                                ss << """Account has overall concurrency limit of " << max_account_concurrency
                                   << ", remaining capacity of " << unreserved_capacity
                                   << ": Can assign maximum of " << concurrency_to_assign << " to function "
                                   << _functionName << ".";
                                logger().info(ss.str());

                                // assign concurrency to assign
                                Aws::Lambda::Model::PutFunctionConcurrencyRequest u_req;
                                u_req.SetFunctionName(functionName.c_str());
                                u_req.SetReservedConcurrentExecutions(concurrency_to_assign);
                                auto outcome = client->PutFunctionConcurrency(u_req);
                                if (!outcome.IsSuccess()) {
                                    auto &error = outcome.GetError();
                                    auto statusCode = static_cast<int>(error.GetResponseCode());
                                    auto errorMessage = std::string(error.GetMessage().c_str());
                                    auto errorName = std::string(error.GetExceptionName().c_str());
                                    throw std::runtime_error(
                                            "internal error assigning maximum, remaining capacity to Lambda runner.");
                                } else {
                                    logger().info("Used maximum available concurrency of "
                                                  + std::to_string(concurrency_to_assign) + " for Lambda runner.");
                                    _functionConcurrency = concurrency_to_assign;
                                }
                            }
                        }

                        logger().error(
                                "Failed to update concurrency with error " + errorName + ", details: " + errorMessage);
                    } else {
                        auto &result = outcome.GetResult();
                        _functionConcurrency = result.GetReservedConcurrentExecutions();
                        logger().info("Function concurrency adjusted to " + std::to_string(_functionConcurrency));
                    }
                }
            }
        }

        if (0 == _functionConcurrency) {
            // issue message & set dummy limit of 100
            logger().info("Function is treated as unreserved concurrency, thus AWS Limit of " +
                          std::to_string(AWS_MINIMUM_UNRESERVED_CONCURRENCY) + " applies.");
            _functionConcurrency = AWS_MINIMUM_UNRESERVED_CONCURRENCY;
        }
    }

    void AwsLambdaBackend::invokeAsync(const AwsLambdaRequest &req) {
        assert(_service);

        // invoke using callbacks!
        _service->invokeAsync(req, [this](const AwsLambdaRequest &req, const AwsLambdaResponse &resp) {
                                  onLambdaSuccess(req, resp);
                              },
                              [this](const AwsLambdaRequest &req, LambdaStatusCode code,
                                     const std::string &msg) { onLambdaFailure(req, code, msg); },
                              [this](const AwsLambdaRequest &req, LambdaStatusCode code, const std::string &msg,
                                     bool b) { onLambdaRetry(req, code, msg, b); });

        // add to local tracking (thread-safe)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _invokedRequests[uuidToString(req.id)] = req;
        }
    }

    void AwsLambdaBackend::onLambdaFailure(const AwsLambdaRequest &req, LambdaStatusCode err_code,
                                           const std::string &err_msg) {

        // TODO: what about cost?

        if(!_service)
            return;

        // Save request for easier debugging/re-execution.
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _failedRequests.push_back(std::make_tuple(err_code, err_msg, req));
        }

        // abort all requests
        _service->abortAllRequests(false);

        // print failure as last message!
        logger().error(err_msg);
    }

    void AwsLambdaBackend::onLambdaRetry(const AwsLambdaRequest &req, LambdaStatusCode retry_code,
                                         const std::string &retry_msg, bool decreasesRetryCount) {

        auto retries_left_str = req.retriesLeft == 1 ? "1 retry left" : std::to_string(req.retriesLeft) + " retries left";

        if(decreasesRetryCount) {
            logger().info("LAMBDA retrying task (" + retries_left_str + "), details: " + retry_msg);
        }

        // do not display silent retries unless in debug mode
        else {
            logger().debug("LAMBDA retrying task (" + retries_left_str + "), because of " + retry_msg);
        }
    }


    struct ResponseInfo {
        int statusCode;
        bool reused;
        std::string id;
        double taskExecutionTime;
        size_t outputRowCount;
        size_t exceptionRowCount;
        size_t selfActualInvokedCount;
        size_t selfActualFailureCount;
        size_t selfInvokeCount;

        ResponseInfo() : statusCode(200), reused(false), taskExecutionTime(0), outputRowCount(0), exceptionRowCount(0), selfInvokeCount(0), selfActualFailureCount(0), selfActualInvokedCount(0) {}

        static ResponseInfo from_protobuf(const messages::InvocationResponse& response) {
            ResponseInfo r;
            r.taskExecutionTime = response.taskexecutiontime();
            r.reused = response.container().reused();
            r.outputRowCount = response.numrowswritten();
            r.exceptionRowCount = response.numexceptions();
            r.id = response.container().uuid();

            // If self-invoke, add up self-invoked container responses.
            for(const auto& self_response : response.invokedresponses()) {
                r.outputRowCount += self_response.numrowswritten();
                r.exceptionRowCount += self_response.numexceptions();
                r.selfActualFailureCount += self_response.status() != messages::InvocationResponse_Status::InvocationResponse_Status_SUCCESS;
            }
            r.selfActualInvokedCount = response.invokedresponses_size();
            r.selfInvokeCount = response.invokedrequests_size();
            return r;
        }
    };

    void AwsLambdaBackend::onLambdaSuccess(const AwsLambdaRequest &req, const AwsLambdaResponse &resp) {

        // message
        std::stringstream  ss;

        auto statusCode = 200; // should be that?

        // parse:
        auto r = ResponseInfo::from_protobuf(resp.response);

        // Get cost before to print out delta.
        auto cost_before = lambdaCost();

        // Add this request to cost.
        addBilling(resp.info.awsTimings.billedDurationInMs * resp.info.awsTimings.memorySizeInMb, 1);

        // Recursive invocations? Add them too.
        if(resp.response.invokedrequests_size() > 0) {
            for(const auto& info : resp.response.invokedrequests()) {
                addBilling(static_cast<size_t>(info.timings().billeddurationinms()) * static_cast<size_t>(info.timings().memorysizeinmb()), 1);
            }
        }

        // this here should go into the success callback!
        ss << "LAMBDA task done in " << r.taskExecutionTime << "s ";
        std::string container_status = r.reused ? "reused" : "new";
        ss << "[" << statusCode << ", " << pluralize(r.outputRowCount, "row")
           << ", " << pluralize(r.exceptionRowCount, "exception") << ", "
           << container_status << ", id: " << r.id << "] ";
        // compute cost and print out
        ss << "Cost so far: $";
        double price = lambdaCost();
        if (price < 0.01)
            ss.precision(4);
        if (price < 0.0001)
            ss.precision(6);
        ss << std::fixed << price;

        // print delta.
        ss<<" (+ $"<<(price - cost_before)<<")";

        // Check if self-invoke, then print out stats from there:
        if(r.selfInvokeCount > 0) {
            ss<<" -- "<<r.selfInvokeCount<<" to recursively invoke, "<<r.selfActualInvokedCount<<" actually invoked, "<<r.selfActualFailureCount<<" thereof failed.";


            // additional debug output for failures:
            if(r.selfActualFailureCount > 0) {
                // Get error messages of failed Lambdas.
                for(const auto& self_response : resp.response.invokedresponses()) {
                    if(self_response.status() != messages::InvocationResponse_Status::InvocationResponse_Status_SUCCESS) {
                        ss<<"\n -- Self-invoked LAMBDA failed with: "<<self_response.errormessage();
                    }
                }
            }
        }

        logger().info(ss.str());

        // lock & save response!
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _tasks.push_back(resp);

#ifndef NDEBUG
            // print response as JSON for easy debugging (selected fields).
            std::string response_as_string;
            google::protobuf::util::MessageToJsonString(resp.response, &response_as_string);
            std::stringstream ss;
            auto j = nlohmann::json::parse(response_as_string);
            ss<<"Got response for task:\n"<<j.dump(2)<<std::endl;
            logger().debug(ss.str());
#endif
        }
    }

//    std::set<std::string> AwsLambdaBackend::performWarmup(const std::vector<int> &countsToInvoke,
//                                                          size_t timeOutInMs,
//                                                          size_t baseDelayInMs) {
//
//        //            size_t numWarmingRequests = 50;
//        std::set<std::string> containerIds;
//        logger().info("Warming up containers...");
//        // do a single synced request (else reuse will occur!)
//        // Tuplex request
//        messages::InvocationRequest req;
//        req.set_type(messages::MessageType::MT_WARMUP);
//
//        // specific warmup message contents
//        auto wm = std::make_unique<messages::WarmupMessage>();
//        wm->set_timeoutinms(timeOutInMs);
//        wm->set_basedelayinms(baseDelayInMs);
//        for (auto count: countsToInvoke)
//            wm->add_invocationcount(count);
//        req.set_allocated_warmup(wm.release());
//
//        // construct req object
//        Aws::Lambda::Model::InvokeRequest invoke_req;
//        invoke_req.SetFunctionName(_functionName.c_str());
//        invoke_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
//        // logtype to extract log data??
//        //req.SetLogtype(Aws::Lambda::Model::LogType::None);
//        invoke_req.SetLogType(Aws::Lambda::Model::LogType::Tail);
//        std::string json_buf;
//        google::protobuf::util::MessageToJsonString(req, &json_buf);
//        invoke_req.SetBody(stringToAWSStream(json_buf));
//        invoke_req.SetContentType("application/javascript");
//
//        // perform synced (!) invoke.
//        auto outcome = _client->Invoke(invoke_req);
//        if (outcome.IsSuccess()) {
//
//            // write response
//            auto &result = outcome.GetResult();
//            auto statusCode = result.GetStatusCode();
//            std::string version = result.GetExecutedVersion().c_str();
//            auto response = parsePayload(result);
//
//            auto log = result.GetLogResult();
//
//            if (response.status() == messages::InvocationResponse_Status_SUCCESS) {
//                // extract info
//                auto info = RequestInfo::parseFromLog(log.c_str());
//                info.fillInFromResponse(response);
//                std::stringstream ss;
//                auto &task = response;
//                if (task.type() == messages::MessageType::MT_WARMUP) {
//                    containerIds.insert(task.container().uuid());
//                    for (auto info: task.invokedcontainers())
//                        containerIds.insert(info.uuid());
//                }
//                ss << "Warmup request took " << response.taskexecutiontime() << " s, " << "initialized "
//                   << containerIds.size();
//                logger().info(ss.str());
//            } else {
//                logger().info("Message returned was weird.");
//            }
//
//            logger().info("Warming succeeded.");
//        } else {
//            // failed
//            logger().error("Warming request failed.");
//
//            auto &error = outcome.GetError();
//            auto statusCode = static_cast<int>(error.GetResponseCode());
//            std::string exceptionName = outcome.GetError().GetExceptionName().c_str();
//            std::string errorMessage = outcome.GetError().GetMessage().c_str();
//            // rate limit? => reissue request
//            if (statusCode == static_cast<int>(Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS) || // i.e. 429
//                statusCode == static_cast<int>(Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR)) {  // i.e. 500
//            } else {
//            }
//        }
////            for(unsigned i = 0; i < numWarmingRequests; ++i) {
////
////                // Tuplex request
////                messages::InvocationRequest req;
////                req.set_type(messages::MessageType::MT_WARMUP);
////
////                // specific warmup message contents
////                auto wm = std::make_unique<messages::WarmupMessage>();
////                wm->set_timeoutinms(timeOutInMs);
////                wm->set_invocationcount(numLambdasToInvoke);
////                req.set_allocated_warmup(wm.release());
////
////                invokeAsync(req);
////            }
////            waitForRequests();
//        logger().info("warmup done");
//
//        return containerIds;
//    }

    std::string remove_non_digits(const std::string &s) {
        std::string res;

        std::copy_if(s.begin(), s.end(), std::back_inserter(res), [](char c) {
            return '0' <= c && c <= '9';
        });
        return res;
    }

    std::string nextJobDumpPath(const std::string &root_path) {
        // create root path if not exists
        auto vfs = VirtualFileSystem::fromURI(root_path);
        vfs.create_dir(root_path);

        // check whether job_0001.json etc. exists
        auto uris = vfs.globAll(URI(root_path).join_path("job_*.json").toString());

        if (uris.empty())
            return URI(root_path).join_path("job_0000.json").toString();
        unsigned max_job_no = 0;
        for (auto uri: uris) {
            auto name = uri.base_name();
            unsigned num = std::stoi(remove_non_digits(name));
            max_job_no = std::max(num, max_job_no);
        }
        return URI(root_path).join_path("job_" + fixedLength(max_job_no + 1, 4) + ".json").toString();
    }


    void AwsLambdaBackend::fill_with_stage(std::vector<AwsLambdaRequest> &requests, TransformStage *tstage, size_t numThreads,
                                           size_t max_retries) {
        for (unsigned i = 0; i < requests.size(); ++i) {
            auto &req = requests[i];
            req.retriesLeft = max_retries;
            fill_with_transform_stage(req.body, tstage);
            fill_env(req.body);
        }
    }


    void AwsLambdaBackend::fill_output_uris(std::vector<AwsLambdaRequest> &requests, const TransformStage* tstage, int num_digits, int numThreads) {

        // quick check that num_digits >= ilog10c(requests.size()) + 1
        assert(num_digits >= ilog10c(requests.size()) + 1);

        uint32_t part_offset = 0;
        for (unsigned i = 0; i < requests.size(); ++i) {
            auto &req = requests[i];

            req.body.set_partnooffset(part_offset);
            // for different parts need different spill folders.
            auto worker_spill_uri = this->spillURI() + "/lam" + fixedLength(part_offset, num_digits);
            fill_with_worker_config(req.body, worker_spill_uri, numThreads);

            // Output URI.
            auto lambda_output_uri = generate_output_base_uri(tstage, part_offset, num_digits, 0, 0);
            req.body.set_baseoutputuri(lambda_output_uri.toString());
            req.body.set_baseisfinaloutput(true); // <-- is final, the recursive will modify it.

            // print out info
            {
                std::stringstream ss;
                ss << "Request " << i << ": part_offset=" << part_offset << " self-invoke count: "
                   << recursive_invocation_count(req.body) << " , producing parts from " << lambda_output_uri;
                logger().debug(ss.str());
            }

            part_offset += 1 + recursive_invocation_count(req.body);
        }
    }

    void AwsLambdaBackend::execute(PhysicalStage *stage) {
        using namespace std;

        _startTimestamp = current_utc_timestamp();

        reset();

        if (_options.USE_WEBUI()) {
            // add job to WebUI
            _historyServer.reset();
            _historyServer = HistoryServerConnector::registerNewJob(_historyConn,
                                                                    "AWS Lambda backend", stage->plan(), _options);
            if (_historyServer) {
                logger().info("track job under " + _historyServer->trackURL());
                _historyServer->sendStatus(JobStatus::STARTED);
            }
        }

        // Notes:
        // ==> could use the warm up events for sampling & speed detection
        // ==> helps to plan the query more efficiently!

        // perform warmup phase if desired (only for first stage?)
//        if(_options.AWS_LAMBDA_SELF_INVOCATION()) {
//            // issue a couple self-invoke requests...
//            Timer timer;
//
//            // warmup in multiple steps (for a maximum time...)
//            std::set<std::string> containerIds;
////            for(int i = 0; i < 10; ++i) {
//                logger().info("Performing warmup.");
//                auto before_count = containerIds.size();
//                auto ids = performWarmup({20, 10, 4}); // 800 total invocations??
//                for(auto id : ids)
//                    containerIds.insert(id);
//                auto after_count = containerIds.size();
//                logger().info("Warmup gave " + std::to_string(after_count - before_count) + " new IDs (" + std::to_string(after_count) + " total)");
////            }
//
//            logger().info("Warmup yielded " + pluralize(containerIds.size(), "container id"));
//            logger().info("Warmup took: " + std::to_string(timer.time()));
//
//            reset();
//            exit(0);
//        }

        auto tstage = dynamic_cast<TransformStage *>(stage);
        if (!tstage)
            throw std::runtime_error("only transform stage from AWS Lambda backend yet supported");

        vector<tuple<std::string, size_t>> uri_infos;

        // decode data from stage
        // -> i.e. could be memory or file, so far only files are supported!
        if (tstage->inputMode() != EndPointMode::FILE) {
            // the data needs to get somehow transferred from the local driver to the cloud
            // Either it could be passed directly via the request OR via S3.

            // For now, use S3 for simplicity...
            auto s3tmp_uri = scratchDir(hintsFromTransformStage(tstage));
            if (s3tmp_uri == URI::INVALID) {
                throw std::runtime_error("could not find/create AWS Lambda scratch dir.");

            }
            // need to transfer the Tuplex partitions to S3
            // -> which format?
            switch (tstage->inputMode()) {
                case EndPointMode::MEMORY: {
                    // simply save to S3!
                    // @TODO: larger/smaller files?
                    Timer timer;
                    int partNo = 0;
                    auto num_partitions = tstage->inputPartitions().size();
                    auto num_digits = ilog10c(num_partitions) + 1;
                    size_t total_uploaded = 0;
                    for (auto p: tstage->inputPartitions()) {
                        // lock each and write to S3!
                        // save input URI and size!
                        auto part_uri = s3tmp_uri.join_path("input_part_" + fixedLength(partNo, num_digits) + ".mem");
                        auto vfs = VirtualFileSystem::fromURI(part_uri);
                        auto vf = vfs.open_file(part_uri, VirtualFileMode::VFS_OVERWRITE);

                        // @TODO: setMIMEtype?

                        if (!vf)
                            throw std::runtime_error("could not open file " + part_uri.toString());
                        auto buf = p->lockRaw();
                        auto buf_size = p->bytesWritten();
                        if (!buf_size)
                            buf_size = p->size();
                        vf->write(buf, buf_size);
                        logger().info("Uploading " + sizeToMemString(buf_size) + " to AWS Lambda cache dir");
                        vf->close();
                        p->unlock();
                        p->invalidate();
                        total_uploaded += buf_size;
                        uri_infos.push_back(make_tuple(part_uri.toString(), buf_size));
                        partNo++;
                    }

                    logger().info("Upload done, " + sizeToMemString(total_uploaded) +
                                  " in total transferred to " + s3tmp_uri.toString() + ", took " +
                                  std::to_string(timer.time()) + "s");
                    break;
                }
                default: {
                    throw std::runtime_error("unsupported endpoint inputmode in AWS Lambda Backend, not supported yet");
                }
            }
        } else {
            // simply decode uris from input partitions...
            uri_infos = decodeFileURIs(tstage->inputPartitions());
        }

        std::string optimizedBitcode = "";
        // optimize at client @TODO: optimize for target triple?
        if (_options.USE_LLVM_OPTIMIZER()) {
            Timer timer;
            bool optimized_ir = false;
            // optimize in parallel???
            if (!tstage->fastPathCode().empty()) {

                assert(tstage->fastPathCodeFormat() == codegen::CodeFormat::LLVM_IR_BITCODE);
                llvm::LLVMContext ctx;
                LLVMOptimizer opt;
                auto mod = codegen::bitCodeToModule(ctx, tstage->fastPathCode());
                opt.optimizeModule(*mod);
                optimizedBitcode = codegen::moduleToBitCodeString(*mod);
                optimized_ir = true;
            } else if (!tstage->slowPathCode().empty()) {
                // todo...
            }
            if (optimized_ir)
                logger().info("client-side LLVM IR optimization of took " + std::to_string(timer.time()) + "s");
        } else {
            optimizedBitcode = tstage->fastPathCode();
        }

        if (stage->outputMode() == EndPointMode::MEMORY) {
            // check whether scratch dir exists.
            auto scratch = scratchDir(hintsFromTransformStage(tstage));
            if (scratch == URI::INVALID) {
                throw std::runtime_error(
                        "temporaty AWS Lambda scratch dir required to write output, please specify via tuplex.aws.scratchDir key");
                return;
            }
        }

        // Worker config variables
        size_t numThreads = 1;
        // check what setting is given for threads
        if (_options.AWS_LAMBDA_THREAD_COUNT() == "auto") {
            numThreads = core::ceilToMultiple(_options.AWS_LAMBDA_MEMORY(), 1792ul) /
                         1792ul; // 1792MB is one vCPU. Use the 200+ for rounding.
            logger().debug("Given Lambda size of " + std::to_string(_options.AWS_LAMBDA_MEMORY()) + "MB, use " +
                           pluralize(numThreads, "thread"));
        } else {
            numThreads = std::stoi(_options.AWS_LAMBDA_THREAD_COUNT());
        }
        logger().info("Lambda executors each use " + pluralize(numThreads, "thread"));
        auto spillURI = _options.AWS_SCRATCH_DIR() + "/spill_folder";
        auto buf_spill_size = compute_buf_spill_size(numThreads);

        logger().info("Setting buffer size for each thread to " + sizeToMemString(buf_spill_size));

        Timer timer;

        // create requests depending on execution strategy
        vector<AwsLambdaRequest> requests;
        switch (stringToAwsExecutionStrategy(_options.AWS_LAMBDA_INVOCATION_STRATEGY())) {
            case AwsLambdaExecutionStrategy::DIRECT: {
                requests = createSingleFileRequests(tstage, optimizedBitcode, numThreads, uri_infos, spillURI,
                                                    buf_spill_size);
                break;
            }
            case AwsLambdaExecutionStrategy::TREE: {
                logger().info("Using tree invocation strategy (specialize per file and then distribute).");

                // configure
                RequestConfig conf;

                // check how large input data is in total
                std::vector<size_t> sizes;
                for (auto info: uri_infos)
                    sizes.emplace_back(std::get<1>(info));
                size_t total_size = std::reduce(sizes.begin(), sizes.end());

                size_t max_lambda_parallelism = _options.AWS_MAX_CONCURRENCY();
                size_t max_parallelism = numThreads * max_lambda_parallelism;
                {
                    std::stringstream ss;
                    ss<<"Found "<<sizeToMemString(total_size)
                      <<" to process in total, with maximum parallelism of "<<max_parallelism
                      <<" ("<<pluralize(max_lambda_parallelism, "lambda")
                      <<", "<<pluralize(max_parallelism, "thread")<<").";
                    logger().info(ss.str());
                }

                // Minimum AWS input size per-lambda.
                // Needs to be smallest file for solver to work.
                size_t minimum_chunk_size = _options.EXPERIMENTAL_AWS_MINIMUM_INPUT_SIZE_PER_LAMBDA();
                size_t maximum_chunk_size = 0;
                for(const auto& s : sizes) {
                    minimum_chunk_size = std::min(s, minimum_chunk_size);
                    maximum_chunk_size = std::max(s, maximum_chunk_size);
                }

                size_t chunk_size = maximum_chunk_size; // per default, set chunk size to largest file (i.e., no split).
                // if lambda_parallelism > #files, find suitable chunking size.
                if(max_lambda_parallelism >= sizes.size()) {
                    chunk_size = find_max_chunk_size(sizes, minimum_chunk_size, max_lambda_parallelism);
                    {
                        std::stringstream ss;
                        auto n_requests = compute_total_requests(sizes, minimum_chunk_size, chunk_size);
                        ss<<"Distributing "<<sizeToMemString(total_size)<<" over "<<pluralize(n_requests, "request")<<" (max lambda parallelism: "<<max_lambda_parallelism<<")";
                        logger().info(ss.str());
                    }

                    // Create first number of requests by distributing data.
                    auto L = logger();
                    requests = create_specializing_recursive_requests(uri_infos, minimum_chunk_size, chunk_size, L);

                    // Fill in invocation details (code path etc.)
                    // This should also generate proper output/offset output uris.
                    size_t max_retries = 5;
                    logger().info("Generating requests with num threads=" + std::to_string(numThreads) + ", max retries=" + std::to_string(max_retries) + ".");
                    fill_with_stage(requests, tstage, numThreads, max_retries);

                    // Same true for specialization unit (should be each file).
                    // Because this here is tree invocation, fill in specialization unit as full files.
                    logger().info("Specializing on each file (" + pluralize(requests.size(), "request") + ").");
                    assert(requests.size() == uri_infos.size());

                    // compute total requests.
                    size_t n_requests = 0;
                    for (const auto &req: requests)
                        n_requests += req.body.inputuris_size();
                    auto num_digits = ilog10c(n_requests) + 1;

                    // set specialization units (per file).
                    for (unsigned i = 0; i < requests.size(); ++i) {
                        auto &req = requests[i];

                        auto sampling_mode = tstage->samplingMode();
                        fill_specialization_unit(*requests[i].body.mutable_stage()->mutable_specializationunit(),
                                                 {uri_infos[i]}, sampling_mode);

                        // add (self) invocation count.
                        int num_self_invocations = (int) requests[i].body.inputuris_size() - 1;
                        assert(num_self_invocations >= 0);
                        req.body.mutable_stage()->add_invocationcount(num_self_invocations);

                        logger().info("Specializing " + std::get<0>(uri_infos[i]) + " and performing " +
                                      pluralize(num_self_invocations, "self invocation") + " then.");
                    }

                    // set output uris, spill uris etc.
                    fill_output_uris(requests, tstage, num_digits, numThreads);

                    // @TODO: if file too small, no need to hyperspecialize. If even smaller, no need to compile -> simply use python interpreter.

                    // Check that max concurrency works with requests being processed. I.e., each request incl. all recursive invocations must be <= MAX_CONCURRENCY.
                    validate_requests_can_be_served_with_concurrency_limit(requests, _options.AWS_MAX_CONCURRENCY());

                } else {
                    // Need to distribute files across few parallel workers (likely partial results with timeouts.
                    auto L = logger();
                    requests = create_specializing_requests_with_multiple_files(tstage, numThreads, uri_infos, L);

                    auto num_digits = ilog10c(requests.size()) + 1;
                    fill_output_uris(requests, tstage, num_digits, numThreads);
                }

                break;
            }
            default:
                logger().error("Unknown execution strategy");
                throw std::runtime_error("unknown invocation strategy '" + _options.AWS_LAMBDA_INVOCATION_STRATEGY() + "', abort");
                break;
        }

        // Start request invocation.
        if (!requests.empty()) {
            logger().info("Invoking " + pluralize(requests.size(), "request") + " ...");

#ifndef NDEBUG
            logger().debug("Emitting request files for easier debugging...");
            for(unsigned i = 0; i < requests.size(); ++i) {
                std::string json_buf;
                google::protobuf::util::MessageToJsonString(requests[i].body, &json_buf);
                stringToFile(URI("request_" + std::to_string(i) + ".json"), json_buf);
            }
            logger().debug("Debug files written (" + pluralize(requests.size(), "file") + ").");
#endif

            if(_emitRequestsOnly) {
                logger().info("Skipping actual LAMBDA invocation, storing " + pluralize(requests.size(), "request") + " as pending.");
                _pendingRequests.clear();
                for(const auto& aws_req : requests)
                    _pendingRequests.push_back(aws_req.body);
                return; // <-- stop further processing, early abort.
            } else {
                perform_requests(requests, _options.AWS_MAX_CONCURRENCY());
            }
            logger().info("LAMBDA requesting took " + std::to_string(timer.time()) + "s");
        } else {
            logger().warn("No requests generated, skipping stage.");
        }

        // TODO: check signals, allow abort...

        // wait till everything finished computing
        assert(_service);
        _service->waitForRequests();

        // Now check whether processing succeeded or not.

        auto processed_input_uris = thread_safe_get_input_uris();
        auto processed_output_uris = thread_safe_get_output_uris();

        logger().info("LAMBDA backend transformed " + pluralize(processed_input_uris.size(), "input path") + " -> " + pluralize(processed_output_uris.size(), "output path"));
        // logger().info("Expected input paths: " + std::to_string());

        gatherStatistics();

        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> ecounts;

        // collect exception counts from stage
        {
            std::lock_guard<std::mutex> lock(_mutex);

            // aggregate stats over responses
            for (const auto &task: _tasks) {
                // @TODO: response needs to contain exception info incl. traceback (?)
                for (const auto &keyval: task.response.exceptioncounts()) {
                    // decode from message
                    // auto key = std::get<0>(keyval.first) << 32 | std::get<1>(keyval.first);
                    // auto key = std::make_tuple(exceptionOperatorID, exceptionCode);
                    int64_t opID = keyval.first >> 32;
                    int64_t ecCode = keyval.first & 0xFFFFFFFF;
                    auto key = std::make_tuple(opID, i64ToEC(ecCode));
                    ecounts[key] += keyval.second;

                    std::cout << "opID=" << opID << " ecCode=" << ecCode << " #: " << keyval.second << std::endl;
                }
            }
        }

        // check here whether all files where successfully processed or not!
        // -> reissue requests for missing files...!

        // save request end! --> i.e. synchronization points!
        _endTimestamp = current_utc_timestamp();

        // check if any remote files are required to be downloaded
        if(!_remoteToLocalURIMapping.empty()) {
            Timer fetchTimer;
            logger().info("Downloading remote files to local machine.");

            for(auto kv : _remoteToLocalURIMapping) {
                logger().info("remote -> local: " + kv.first.toPath() + " -> " + kv.second.toPath());
            }

            for(auto task : _tasks) {
                // check output uris and their mapping
                for(auto output_uri : task.response.outputuris()) {
                    logger().info("output uri: " + output_uri);

                    // fetch file
                    auto it = _remoteToLocalURIMapping.find(output_uri);
                    if(it == _remoteToLocalURIMapping.end()) {
                        logger().warn("could not find output uri " + output_uri + " in remote -> local mapping, skipping.");
                    } else {
                        // get basename of remote uri and map to local (join with parent)
                        auto remote_basename = URI(it->first).base_name();
                        auto local_parent = it->second.parent();
                        auto target_path = local_parent.join(remote_basename).toPath();
                        logger().info("storing " + it->first.toPath() + " to " + target_path);
                        VirtualFileSystem::s3DownloadFile(it->first, target_path);
                    }
                }
            }
            logger().info("Fetching files from remote took " + std::to_string(fetchTimer.time()) + "s");
        }

        auto path = nextJobDumpPath("job");
        dumpAsJSON(path);
        logger().info("dumped job info as JSON to " + path);

        // For easier debugging purposes, dump logs of Lambdas to folder.
        // They're also contained in json, but this here may be easier.
        auto log_path = strReplaceAll(path, ".json", "") + "/logs";
        log_path = strReplaceAll(log_path, "file://", "");
        if(!dirExists(log_path)) {
            std::filesystem::create_directories(log_path);
        }

        {
            std::lock_guard<std::mutex> lock(_mutex);

            int lam_no = 0;
            for (const auto &task: _tasks) {
                if(task.response.type() == messages::MessageType::MT_TRANSFORM) {
//                    // Get lambda name from spill uri.
//                    auto key = task.response.container().uuid();
//
//                    // Check if key was found
//                    auto req = _invokedRequests.at(key);
//                    auto spill_uri = req.body.settings().spillrooturi();
//                    auto lam_no_str = spill_uri.substr(spill_uri.rfind("/lam") + 4);
//                    auto lam_no = std::stoi(lam_no_str);

                    // log of this container.
                    for (const auto &r: task.response.resources()) {
                        if (r.type() == static_cast<uint32_t>(ResourceType::LOG)) {
                            auto log = decompress_string(r.payload());
                            stringToFile(URI(log_path).join_path("lam" + std::to_string(lam_no) + ".txt"), log);
                            lam_no++;
                            break;
                        }
                    }

                    for (unsigned i = 0; i < task.response.invokedresponses_size(); ++i) {
                        // the lam no here won't match. This is not perfect.
                        // TODO: return lam no/identifier in map?
                        auto self_invoke_response = task.response.invokedresponses(i);
                        for (const auto &r: self_invoke_response.resources()) {
                            if (r.type() == static_cast<uint32_t>(ResourceType::LOG)) {
                                auto log = decompress_string(r.payload());
                                stringToFile(URI(log_path).join_path("lam" + std::to_string(lam_no) + ".txt"), log);
                                lam_no++;
                                break;
                            }
                        }
                    }
                }
            }
        }
        logger().info("Saved " + pluralize(glob(log_path + "/*.txt").size(), "log") + " to " + log_path);


        // failed requests? If so dump them to file under failed_requests.
        if(!_failedRequests.empty()) {
            std::string failed_request_path = "failed_requests";
            logger().error("Found " + pluralize(_failedRequests.size(), "failed requests") + ", saving to disk to " + failed_request_path + ".");
            if(!dirExists(failed_request_path.c_str())) {
                std::filesystem::create_directories(failed_request_path);
            }
            int pos = 0;
            for(auto t : _failedRequests) {
                stringToFile(URI(failed_request_path).join("log" + std::to_string(pos) + ".txt"), "Lambda request failed with code " +
                        lambda_status_to_string(std::get<0>(t)) + "\n\n" + std::get<1>(t));
                // save request (as raw protobuf)
                auto failed_req = std::get<2>(t);
                auto bin_proto = failed_req.body.SerializeAsString();
                std::string json_proto;
                google::protobuf::util::MessageToJsonString(failed_req.body, &json_proto);
                // Save as json and proto.
                stringToFile(URI(failed_request_path).join("request" + std::to_string(pos) + ".bin"), bin_proto);
                stringToFile(URI(failed_request_path).join("request" + std::to_string(pos) + ".json"), json_proto);

                pos++;
            }
            logger().info("Saved failed requests to disk.");
        }

        {
            std::stringstream ss;
            ss << "LAMBDA compute took " << timer.time() << "s";
            double cost = lambdaCost();
            if (cost < 0.01)
                ss << ", cost < $0.01";
            else
                ss << std::fixed << std::setprecision(2) << ", cost: $" << cost <<"";
            logger().info(ss.str());
        }

        // generate here csv with information per file
        {
            auto csv_info = csvPerFileInfo();
            logger().info("\nper-file information:\n" + csv_info + "\n");
        }

        // @TODO: results sets etc.
        switch (tstage->outputMode()) {
            case EndPointMode::FILE: {
                tstage->setFileResult(ecounts);
                break;
            }

            case EndPointMode::MEMORY: {
                // fetch from outputs, alloc partitions and set
                // tstage->setMemoryResult()
                Timer timer;
                vector<URI> output_uris;
                for (const auto &task: _tasks) {
                    for (const auto &uri: task.response.outputuris())
                        output_uris.push_back(uri);
                }
                // sort after part no @TODO
                std::sort(output_uris.begin(), output_uris.end(), [](const URI &a, const URI &b) {
                    return a.toString() < b.toString();
                });

                // download and store each part in one partition (TODO: resize etc.)
                vector<Partition *> output_partitions;
                int partNo = 0;
                int num_digits = ilog10c(output_uris.size()) + 1;
                vector<URI> local_paths;
                for (const auto &uri: output_uris) {
                    // download to local scratch dir
                    auto local_path = _options.SCRATCH_DIR().join_path("aws-part" + fixedLength(partNo, num_digits));
                    VirtualFileSystem::copy(uri.toString(), local_path);
                    local_paths.push_back(local_path);
                    partNo++;
                }
                logger().info(
                        "fetching results from " + scratchDir().toString() + " took " + std::to_string(timer.time()) +
                        "s");

                // convert to partitions
                timer.reset();
                for (auto path: local_paths) {
                    auto vf = VirtualFileSystem::fromURI(path).open_file(path, VirtualFileMode::VFS_READ);
                    if (!vf) {
                        throw std::runtime_error("could not read locally cached file " + path.toString());
                    }

                    auto file_size = vf->size();
                    size_t bytesRead = 0;

                    // alloc new driver partition
                    Partition *partition = _driver->allocWritablePartition(file_size, tstage->outputSchema(),
                                                                           tstage->outputDataSetID(),
                                                                           stage->context().id());
                    auto ptr = partition->lockWrite();
                    int64_t bytesWritten = file_size;
                    int64_t numRows = 0;
                    vf->read(&bytesWritten, sizeof(int64_t), &bytesRead);
                    vf->read(&numRows, sizeof(int64_t), &bytesRead);
                    vf->read(ptr, file_size, &bytesRead);
                    partition->unlockWrite();
                    partition->setNumRows(numRows);
                    partition->setBytesWritten(bytesWritten);
                    vf->close();

                    logger().debug("read " + sizeToMemString(bytesRead) + " to a single partition");
                    output_partitions.push_back(partition);

                    // remove local file @TODO: could be done later to be more efficient, faster...
                    VirtualFileSystem::remove(path);
                }
                logger().info("Loading S3 results into driver took " + std::to_string(timer.time()) + "s");
                tstage->setMemoryResult(output_partitions);

                break;
            }

            default: {
                throw std::runtime_error("other end points then memory/file via S3 not yet implemented");
            }
        }


        // if webui, send job end (single stage right now only supported)
        if (_historyServer)
            _historyServer->sendStatus(JobStatus::FINISHED);
    }

    void AwsLambdaBackend::perform_requests(const std::vector<AwsLambdaRequest> &requests, size_t concurrency_limit) {
        validate_requests_can_be_served_with_concurrency_limit(requests, concurrency_limit);

        if(!_blockRequests) {
            // Check that total request count can be served with limit, else need to perform throttled invocation (not yet implemented).
            size_t total_request_count = 0;
            for(const auto& req : requests) {
                total_request_count += 1 + recursive_invocation_count(req.body);
            }

//            if(total_request_count > concurrency_limit) {
//                throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " Got in total " + pluralize(total_request_count, "request") + ", but Lambda is configured with " + std::to_string(concurrency_limit) + " concurrency limit. Need to implement throttled invocation.");
//            }

            // is rate limiting necessary (because of max concurrency?), if so limit.
            if(requests.size() > concurrency_limit) {
                for(const auto& req: requests) {
                    // simple wait, no fancy filling in requests on capacity.
                    while(_service->activeMaximumConcurrency() >= concurrency_limit) {
                        std::this_thread::sleep_for(std::chrono::duration<double, std::milli>(100.0));
                    }
                    invokeAsync(req);
                }
            } else {
                // limit using function themselves.
                for (const auto &req: requests)
                    invokeAsync(req);
            }
        } else {
            // invoke requests one by one.
            // the validation check ensures this works with the concurrency limit.
            for(const auto& req : requests) {
                invokeAsync(req);
                _service->waitForRequests();
            }
        }
    }

    std::vector<URI> AwsLambdaBackend::thread_safe_get_input_uris() {
        std::lock_guard<std::mutex> lock(_mutex);
        std::vector<URI> uris;
        for (const auto &task: _tasks) {
            for(const auto& uri : task.response.inputuris())
                uris.push_back(uri);
        }

        std::sort(uris.begin(), uris.end(),[](const URI& a, const URI& b) {
            return a.toString() < b.toString();
        });
        return uris;
    }

    std::vector<URI> AwsLambdaBackend::thread_safe_get_output_uris() {
        std::lock_guard<std::mutex> lock(_mutex);
        std::vector<URI> uris;
        for (const auto &task: _tasks) {
            for(const auto& uri : task.response.outputuris())
                uris.push_back(uri);
            // self-invoke?
            if(task.response.invokedrequests_size() > 0) {
                for(const auto& self_invoke_response : task.response.invokedresponses()) {
                    for(const auto& uri : self_invoke_response.outputuris())
                        uris.push_back(uri);
                }
            }
        }

        std::sort(uris.begin(), uris.end(),[](const URI& a, const URI& b) {
            return a.toString() < b.toString();
        });

        return uris;
    }

    // recurse depth:
    // 2^n?
    // 3^n?

    std::vector<messages::InvocationRequest>
    AwsLambdaBackend::createSelfInvokingRequests(const TransformStage *tstage, const std::string &bitCode,
                                                 const size_t numThreads,
                                                 const std::vector<std::tuple<std::string, std::size_t>> &uri_infos,
                                                 const std::string &spillURI, const size_t buf_spill_size) {

        // how many files are there? What's the total size?
        std::vector<messages::InvocationRequest> requests;

        size_t total_size = 0;
        for (auto info: uri_infos) {
            total_size += std::get<1>(info);
        }
        logger().info("Creating self-invoking requests for " + pluralize(uri_infos.size(), "file") + " - " +
                      sizeToMemString(total_size));

        // create a request which invokes Lambdas recursively?
        // for now simply let one Lambda invoke all the others
        std::vector<size_t> recursive_invocations;

        // // Strategy I:
        // // one lambda per file?
        // if(uri_infos.size() > 2)
        //     recursive_invocations.push_back(uri_infos.size() - 1);

        // Strategy II: use concurrency setting!
        //recursive_invocations.push_back(_functionConcurrency - 1);

        // just use  2 -> 2 -> 2
        recursive_invocations = std::vector<size_t>{1, 1,
                                                    1}; // 2 * 2 * 2 -> 8 invocations. still need to figure out the part naming.

        // test, use 5 x 4 --> spawns 512 Lambdas.
        recursive_invocations = std::vector<size_t>{4, 4, 4, 4, 4}; // 512 Lambdas?

        recursive_invocations = std::vector<size_t>{200, 4};

        // when concurrency is < 200, use simple invocation strategy.
        // AWS EMR compatible setting.

        // always should split into MORE parts than function concurrency in order
        // to max out everything...
        recursive_invocations = std::vector<size_t>{2 * _functionConcurrency - 1};

        // transform to request
        messages::InvocationRequest req;
        fill_env(req);
        req.set_type(messages::MessageType::MT_TRANSFORM);
        auto pb_stage = tstage->to_protobuf();
        for (auto count: recursive_invocations)
            pb_stage->add_invocationcount(count);

        req.set_allocated_stage(pb_stage.release());

        // add request for this
        for (auto info: uri_infos) {
            auto inputURI = std::get<0>(info);
            auto inputSize = std::get<1>(info);
            req.add_inputuris(inputURI);
            req.add_inputsizes(inputSize);
        }

        // worker config
        auto ws = std::make_unique<messages::WorkerSettings>();
        throw std::runtime_error("ERROR: need unique spill URIS");
        config_worker(ws.get(), _options, numThreads, spillURI, buf_spill_size);
        req.set_allocated_settings(ws.release());

        // partNo offset
        req.set_partnooffset(0); // single request!

        // output uri of job? => final one? parts?
        // => create temporary if output is local! i.e. to memory etc.
        int taskNo = 0;
        int num_digits = 5;
        if (tstage->outputMode() == EndPointMode::MEMORY) {
            // create temp file in scratch dir!
            req.set_baseoutputuri(scratchDir(hintsFromTransformStage(tstage)).join_path(
                    "output.part" + fixedLength(taskNo, num_digits)).toString());
        } else if (tstage->outputMode() == EndPointMode::FILE) {
            // create output URI based on taskNo
            auto uri = outputURI(tstage->outputPathUDF(), tstage->outputURI(), taskNo, tstage->outputFormat());
            req.set_baseoutputuri(uri.toPath());
        } else if (tstage->outputMode() == EndPointMode::HASHTABLE) {
            throw std::runtime_error("join, aggregate not yet supported in lambda backend");
        } else throw std::runtime_error("unknown output endpoint in lambda backend");
        requests.push_back(req);

        logger().info("Created " + pluralize(requests.size(), "LAMBDA request") + +".");
        return requests;
    }

    static AwsLambdaRequest create_tree_based_hyperspecialization_request(const TransformStage* tstage,
                                                                          const std::string& bitCode,
                                                                          const URI& uri, size_t uri_size,
                                                                          size_t rangeStart, size_t rangeEnd,
                                                                          const ContextOptions& options,
                                                                          std::unordered_map<URI,URI>& remoteToLocalURIMapping
                                                                          ) {
//        assert(tstage);
//
//        messages::InvocationRequest req;
//        req.set_type(messages::MessageType::MT_TRANSFORM);
//        auto pb_stage = tstage->to_protobuf();
//
//        // auto rangeStart = cur_size;
//        // auto rangeEnd = std::min(cur_size + splitSize, uri_size);
//
//        // clamp rangeEnd
//        rangeEnd = std::min(rangeEnd, uri_size);
//
//        pb_stage->set_bitcode(bitCode);
//
//        req.set_allocated_stage(pb_stage.release());
//
//        // add request for this
//        auto inputURI = uri.toString();
//        auto inputSize = uri_size;
//        inputURI += ":" + std::to_string(rangeStart) + "-" + std::to_string(rangeEnd);
//        req.add_inputuris(inputURI);
//        req.add_inputsizes(inputSize);
//
//        // worker config
//        auto ws = std::make_unique<messages::WorkerSettings>();
//        auto worker_spill_uri = spillURI + "/lam" + fixedLength(i, num_digits) + "/" + fixedLength(part_no, num_digits_part);
//        config_worker(ws.get(), options, numThreads, worker_spill_uri, buf_spill_size);
//        req.set_allocated_settings(ws.release());
//        req.set_verboselogging(options.AWS_VERBOSE_LOGGING());
//
//        // output uri of job? => final one? parts?
//        // => create temporary if output is local! i.e. to memory etc.
//        int taskNo = i;
//        if (tstage->outputMode() == EndPointMode::MEMORY) {
//            // create temp file in scratch dir!
//            req.set_baseoutputuri(scratchDir(hintsFromTransformStage(tstage)).join_path(
//                    "output.part" + fixedLength(taskNo, num_digits) + "_" +
//                    fixedLength(part_no, num_digits_part)).toString());
//        } else if (tstage->outputMode() == EndPointMode::FILE) {
//            // create output URI based on taskNo
//            auto uri = outputURI(tstage->outputPathUDF(), tstage->outputURI(), taskNo, tstage->outputFormat());
//            auto remote_output_uri = uri.toPath();
//
//            // is it a local file URI as target? if so, generate dummy uri under spill folder for this job & add mapping
//            if(uri.isLocal()) {
//                auto tmp_uri = scratchDir(hintsFromTransformStage(tstage)).join_path(
//                        "output.part" + fixedLength(taskNo, num_digits) + "_" +
//                        fixedLength(part_no, num_digits_part)).toString();
//
//                remote_output_uri = tmp_uri;
//
//                // add extension to mapping
//                auto output_fmt = tstage->outputFormat();
//                auto ext = defaultFileExtension(output_fmt);
//                tmp_uri += "." + ext; // done in worker app, fix in the future.
//                remoteToLocalURIMapping[tmp_uri] = uri;
//            }
//
//            req.set_baseoutputuri(remote_output_uri);
//        } else if (tstage->outputMode() == EndPointMode::HASHTABLE) {
//            // there's two options now, either this is an end-stage (i.e., unique/aggregateByKey/...)
//            // or an intermediate stage where a temp hash-table is required.
//            // in any case, because compute is done on Lambda materialize hash-table as temp file.
//            auto temp_uri = tempStageURI(tstage->number());
//            req.set_baseoutputuri(temp_uri.toString());
//        } else throw std::runtime_error("unknown output endpoint in lambda backend");

        AwsLambdaRequest aws_req;
//        aws_req.body = req;
        aws_req.retriesLeft = 5;

        return aws_req;
    }

    std::vector<AwsLambdaRequest> create_specializing_recursive_requests(const std::vector<std::tuple<URI, std::size_t>> &uri_infos,
                                                                         size_t minimum_chunk_size,
                                                                         size_t maximum_chunk_size,
                                                                         MessageHandler& logger,
                                                                         std::function<std::string(int n, int n_digits)> generate_output_base_uri) {

        assert(minimum_chunk_size < maximum_chunk_size);
        // at least 1KB.
        assert(minimum_chunk_size > 1024);
        assert(maximum_chunk_size > 1024);
        std::vector<AwsLambdaRequest> requests;

        // How many URIs are there?
        {
            std::stringstream ss;
            ss<<"Found "<<pluralize(uri_infos.size(), "specialization unit")<<" to use.";
            logger.info(ss.str());
        }

        // Step 1: Figure out how much data needs to get processed.
        // -> From this, infer part size for max-parallelism.
        size_t total_size_in_bytes = 0;
        for(auto uri_info : uri_infos) {
            total_size_in_bytes += std::get<1>(uri_info);
        }
        {
            std::stringstream ss;
            ss<<"Found "<<sizeToMemString(total_size_in_bytes)
            <<" to process, each LAMBDA will receive at most ~"<<sizeToMemString(maximum_chunk_size);
            logger.info(ss.str());
        }

        // Go through uris and create initial requests (no TrafoStage fill in yet).
        for(auto uri_info : uri_infos) {
            AwsLambdaRequest req;

            // Split URI into equal chunks to parallelize them.
            // A chunk must be at least minimum chunk size.
            auto uri = std::get<0>(uri_info);
            auto uri_size = std::get<1>(uri_info);

            // single part?
            if(uri_size <= maximum_chunk_size + minimum_chunk_size) { // Note the add here.
                req.body.add_inputuris(uri.toString());
                req.body.add_inputsizes(uri_size);

                // no split up necessary, simply process full data.
                req.body.mutable_stage()->add_invocationcount(0);

                {
                    std::stringstream ss;
                    ss<<uri<<" ("<<sizeToMemString(uri_size)<<") not split";
                    logger.info(ss.str());
                }

            } else {
                // split up. There will be at least two parts. One maximum chunk size, the other at least minimum chunk size.
                // First chunk part will be executed by the worker itself, the others will be self-invoke.
                std::vector<FilePart> parts;
                int64_t remaining_bytes = uri_size;
                size_t current_offset = 0;
                while(remaining_bytes > maximum_chunk_size + minimum_chunk_size) {
                    FilePart part;
                    part.size = uri_size;
                    part.rangeStart = current_offset;
                    part.rangeEnd = current_offset + maximum_chunk_size;
                    part.uri = uri;
                    parts.push_back(part);

                    current_offset += maximum_chunk_size;
                    remaining_bytes -= maximum_chunk_size;
                }
                // add last part, whatever is remaining.
                FilePart last_part;
                last_part.size = uri_size;
                last_part.rangeStart = current_offset;
                last_part.rangeEnd = uri_size;
                last_part.uri = uri;
                parts.push_back(last_part);

                // go over parts and add them individually.
                for(const auto& part: parts) {
                    auto inputURI = encodeRangeURI(part.uri, part.rangeStart, part.rangeEnd);
                    auto inputSize = part.size;
                    req.body.add_inputuris(inputURI);
                    req.body.add_inputsizes(inputSize);
                }

                {
                    std::stringstream ss;
                    auto smallest_part_size = std::min(maximum_chunk_size, last_part.part_size());
                    auto largest_part_size = std::max(maximum_chunk_size, last_part.part_size());
                    ss<<uri<<" ("<<sizeToMemString(uri_size)<<") split into "<<pluralize(parts.size(), "part")<<" (smallest: "<<sizeToMemString(smallest_part_size)<<", largest: "<<sizeToMemString(largest_part_size)<<")";
                    logger.info(ss.str());
                }

                assert(parts.size() >= 2);
                req.body.mutable_stage()->add_invocationcount(parts.size() - 1);
            }

            requests.push_back(req);
        }

        // Step 2: generate output uris (consecutive)
        int n_output_parts = 0;
        // Count how many output parts there will be.
        for(const auto& req : requests) {
            n_output_parts++; // +1 for the request itself.
            // Are there self-invocations? For each invocation count +1.
            // Only 1 level supported yet.
            n_output_parts += recursive_invocation_count(req.body);
        }
        int n_digits = ilog10c(n_output_parts) + 1;
        logger.info("Recursive tree invocation will invoke in total " + pluralize(n_output_parts, "lambda") + " producing " + pluralize(n_output_parts, "part") + ".");

        // Go over requests again & set part offsets.
        int task_no = 0;
        for(auto& req : requests) {
            req.body.set_partnooffset(task_no);
            auto lambda_output_uri = generate_output_base_uri(task_no, n_digits);
            req.body.set_baseoutputuri(lambda_output_uri);
            req.body.set_baseisfinaloutput(true); // <-- is final, the recursive will modify it.
            task_no += 1 + recursive_invocation_count(req.body);
        }

        logger.info("Recursive Lambda invocation will produce " + pluralize(task_no, "output file") + ".");




        // TODO:
        //             // for different parts need different spill folders.
        //            auto worker_spill_uri = spillURI() + "/lam" + fixedLength(partno, num_digits);
        //            fill_with_worker_config(req.body, worker_spill_uri, numThreads);

        // TODO:
        //             // @TODO: specialization unit.
        //            fill_specialization_unit(*req.body.mutable_stage()->mutable_specializationunit(),
        //                                     {uri_info}, sampling_mode);


        return requests;
    }

    std::vector<AwsLambdaRequest>
    AwsLambdaBackend::createSpecializingSelfInvokeRequests(const TransformStage *tstage,
                                                           const std::string &bitCode,
                                                           const size_t numThreads,
                                                           const std::vector<std::tuple<std::string, std::size_t>> &uri_infos,
                                                           const RequestConfig &conf) {

        std::vector<AwsLambdaRequest> requests;
        std::vector<URI> request_uris;

        // How many URIs are there?
        {
            std::stringstream ss;
            ss<<"Found "<<pluralize(uri_infos.size(), "specialization unit")<<" to use.";
            logger().info(ss.str());
        }

        // Generate for each URI a specialization unit (and request).
        int partno = 0;
        int max_retries = 5; // @TODO: should be option.
        auto num_digits = ilog10c(uri_infos.size()) + 1;

        assert(tstage);
        auto sampling_mode = tstage->samplingMode();


        // Step 1: Figure out how much data needs to get processed.
        // -> From this, infer part size for max-parallelism.
        size_t total_size_in_bytes = 0;
        for(auto uri_info : uri_infos) {
            total_size_in_bytes += std::get<1>(uri_info);
        }
        size_t per_lambda_total_size = total_size_in_bytes / _options.AWS_MAX_CONCURRENCY();
        {
            std::stringstream ss;
            ss<<"Found "<<sizeToMemString(total_size_in_bytes)
            <<" to process, with maximum parallelism of "<<_options.AWS_MAX_CONCURRENCY()
            <<" each LAMBDA will receive ~"<<sizeToMemString(per_lambda_total_size);
            logger().info(ss.str());
        }

        // TODO: figure this here out.
        int num_self_invoke = 20; // 3 parts.

        // @TODO: improve this.
        auto base_output_uri = tstage->outputURI();

        // Produce S3 output uris as follows for each request invoking n Lambdas overall:
        // first request:
        // <base_uri>/part0.csv
        // ...
        // <base_uri>/part{n}.csv

        int current_part_offset = 0;

        for(auto uri_info : uri_infos) {
            AwsLambdaRequest req;
            req.retriesLeft = max_retries;
            req.body.set_partnooffset(partno); // offset for parts.

            fill_with_transform_stage(req.body, tstage);
            fill_env(req.body);

            // for different parts need different spill folders.
            auto worker_spill_uri = spillURI() + "/lam" + fixedLength(partno, num_digits); //+ fixedLength(i, num_digits) + "/" + fixedLength(part_no, num_digits_part);
            fill_with_worker_config(req.body, worker_spill_uri, numThreads);

            // @TODO: specialization unit.
            fill_specialization_unit(*req.body.mutable_stage()->mutable_specializationunit(),
                                     {uri_info}, sampling_mode);

            // Split URI into equal chunks to parallelize then.
            // Basically the Lambda worker will do self-invoke with the number of parts.
            // Can limit then.
            int n_parts = num_self_invoke + 1; // Let Lambda itself work on data as well.
            // Split into parts.
            auto split_parts = splitIntoEqualParts(n_parts, {std::get<0>(uri_info)}, {std::get<1>(uri_info)}, 1024);
            std::vector<FilePart> parts;
            for(const auto& thread_parts : split_parts) {
                for(auto part : thread_parts) {
                    part.partNo = parts.size();
                    parts.push_back(part);
                }
            }

            // go over parts and add them individually.
            for(const auto& part : parts) {
                auto inputURI = encodeRangeURI(part.uri, part.rangeStart, part.rangeEnd);
                auto inputSize = part.size;
                req.body.add_inputuris(inputURI);
                req.body.add_inputsizes(inputSize);
            }

            // no output yet, will be done in NEXT loop.

            // set self invocation, i.e. recurse.
            req.body.mutable_stage()->add_invocationcount(num_self_invoke);
            requests.push_back(req);
        }


        // Step 2: generate output uris (consecutive)
        int n_output_parts = 0;
        // Count how many output parts there will be.
        for(const auto& req : requests) {
            n_output_parts++; // +1 for the request itself.
            // Are there self-invocations? For each invocation count +1.
            // Only 1 level supported yet.
            n_output_parts += recursive_invocation_count(req.body);
        }
        int n_digits = ilog10c(n_output_parts) + 1;
        logger().info("Recursive tree invocation will invoke in total " + pluralize(n_output_parts, "lambda") + " producing " + pluralize(n_output_parts, "part") + ".");

        // Go over requests again & set part offsets.
        int task_no = 0;
        for(auto& req : requests) {
            req.body.set_partnooffset(task_no);
            auto lambda_output_uri = generate_output_base_uri(tstage, task_no, n_digits, 0, 0);
            req.body.set_baseoutputuri(lambda_output_uri.toString());
            req.body.set_baseisfinaloutput(true); // <-- is final, the recursive will modify it.
            task_no += 1 + recursive_invocation_count(req.body);
        }

        logger().info("Recursive Lambda invocation will produce " + pluralize(task_no, "output file") + ".");
        return requests;
    }

    void AwsLambdaBackend::fill_specialization_unit(messages::SpecializationUnit& m,
                                                    const std::vector<std::tuple<URI,size_t>>& uri_infos,
                                                    const SamplingMode& sampling_mode) {
        for(auto entry : uri_infos) {
            m.add_inputuris(std::get<0>(entry).toString());
            m.add_inputsizes(std::get<1>(entry));
        }
        m.set_samplemode(static_cast<uint64_t>(sampling_mode));
    }

    void AwsLambdaBackend::fill_with_worker_config(messages::InvocationRequest& req,
                                                   const std::string& worker_spill_uri,
                                                   int numThreads) const {
        // worker config
        auto ws = std::make_unique<messages::WorkerSettings>();
        config_worker(ws.get(), _options, numThreads, worker_spill_uri, compute_buf_spill_size(numThreads));
        req.set_allocated_settings(ws.release());
        req.set_verboselogging(_options.AWS_VERBOSE_LOGGING());
    }

    void AwsLambdaBackend::fill_with_transform_stage(messages::InvocationRequest& req, const TransformStage* tstage) const {
        req.set_type(messages::MessageType::MT_TRANSFORM);
        auto pb_stage = tstage->to_protobuf();
        req.set_allocated_stage(pb_stage.release());
    }

    std::vector<AwsLambdaRequest>
    AwsLambdaBackend::createSingleFileRequests(const TransformStage *tstage,
                                               const std::string &bitCode,
                                               const size_t numThreads,
                                               const std::vector<std::tuple<std::string, std::size_t>> &uri_infos,
                                               const std::string &spillURI,
                                               const size_t buf_spill_size) {

        std::vector<AwsLambdaRequest> requests;

        size_t splitSize = _options.INPUT_SPLIT_SIZE();
        size_t total_size = 0;
        for (auto info: uri_infos) {
            total_size += std::get<1>(info);
        }

        logger().info("Found " + pluralize(uri_infos.size(), "uri")
                      + " with total size = " + sizeToMemString(total_size) + " (split size=" +
                      sizeToMemString(splitSize) + ")");

        // new: part based
        // Note: for now, super simple: 1 request per file (this is inefficient, but whatever)
        // @TODO: more sophisticated splitting of workload!
        Timer timer;
        int num_digits = ilog10c(uri_infos.size()) + 1;
        for (int i = 0; i < uri_infos.size(); ++i) {
            auto info = uri_infos[i];

            // the smaller
            auto uri_size = std::get<1>(info);
            auto num_parts_per_file = uri_size / splitSize;

            auto num_digits_part = ilog10c(num_parts_per_file + 1);
            int part_no = 0;
            size_t cur_size = 0;
            while (cur_size < uri_size) {

                auto rangeStart = cur_size;
                auto rangeEnd = std::min(cur_size + splitSize, uri_size);

                messages::InvocationRequest req;

                auto worker_spill_uri = spillURI + "/lam" + fixedLength(i, num_digits) + "/" + fixedLength(part_no, num_digits_part);
                fill_with_transform_stage(req, tstage);

                // add other core data to the request.
                fill_env(req);
                fill_with_worker_config(req, worker_spill_uri, numThreads);

                // add request for this
                auto inputURI = std::get<0>(info);
                auto inputSize = std::get<1>(info);
                inputURI += ":" + std::to_string(rangeStart) + "-" + std::to_string(rangeEnd);
                req.add_inputuris(inputURI);
                req.add_inputsizes(inputSize);

                // output uri of job? => final one? parts?
                // => create temporary if output is local! i.e. to memory etc.
                int taskNo = i;

                auto remote_output_uri = generate_output_base_uri(tstage, taskNo, num_digits, part_no, num_digits_part);
                req.set_baseoutputuri(remote_output_uri.toString());
                req.set_baseisfinaloutput(true); // Only single file.
                AwsLambdaRequest aws_req;
                aws_req.body = req;
                aws_req.retriesLeft = 5;
                requests.push_back(aws_req);

                part_no++;
                cur_size += splitSize;
            }
        }

        logger().info("Created " + std::to_string(requests.size()) + " LAMBDA requests.");

        return requests;
    }

    URI AwsLambdaBackend::generate_output_base_uri(const TransformStage* tstage, int taskNo, int num_digits, int part_no, int num_digits_part) {

        // Adjust amount of digits to print, default to regular printing if not set.
        if(num_digits <= 0 || ilog10c(taskNo) + 1 > num_digits)
            num_digits = ilog10c(taskNo) + 1;
        if(part_no >= 0 && (num_digits_part <= 0 || ilog10c(part_no) + 1 > num_digits_part))
            num_digits_part = ilog10c(part_no) + 1;

        if (tstage->outputMode() == EndPointMode::MEMORY) {
            // create temp file in scratch dir!
            return scratchDir(hintsFromTransformStage(tstage)).join_path(
                    "output.part" + fixedLength(taskNo, num_digits) + "_" +
                    fixedLength(part_no, num_digits_part));
        } else if (tstage->outputMode() == EndPointMode::FILE) {
            // create output URI based on taskNo
            auto uri = outputURI(tstage->outputPathUDF(), tstage->outputURI(), taskNo, tstage->outputFormat());
            auto remote_output_uri = uri.toPath();

            // is it a local file URI as target? if so, generate dummy uri under spill folder for this job & add mapping
            if(uri.isLocal()) {
                auto part_str ="output.part" + fixedLength(taskNo, num_digits);
                if(part_no >= 0)
                    part_str += "_" + fixedLength(part_no, num_digits_part);

                auto tmp_uri = scratchDir(hintsFromTransformStage(tstage)).join_path(part_str).toString();

                remote_output_uri = tmp_uri;

                // Add extension to mapping.
                auto output_fmt = tstage->outputFormat();
                auto ext = defaultFileExtension(output_fmt);
                tmp_uri += "." + ext; // done in worker app, fix in the future.
                _remoteToLocalURIMapping[tmp_uri] = uri;
            }
            return remote_output_uri;

        } else if (tstage->outputMode() == EndPointMode::HASHTABLE) {
            // there's two options now, either this is an end-stage (i.e., unique/aggregateByKey/...)
            // or an intermediate stage where a temp hash-table is required.
            // in any case, because compute is done on Lambda materialize hash-table as temp file.
            auto temp_uri = tempStageURI(tstage->number());
            return temp_uri.toString();
        } else throw std::runtime_error("unknown output endpoint in lambda backend");
    }

    AwsLambdaBackend::AwsLambdaBackend(Context &context,
                                       const AWSCredentials &credentials,
                                       const std::string &functionName) : IRequestBackend(context),
                                                                          _credentials(credentials),
                                                                          _functionName(functionName),
                                                                          _options(context.getOptions()),
                                                                          _logger(Logger::instance().logger(
                                                                                  "aws-lambda")), _tag("tuplex"),
                                                                          _client(nullptr),
                                                                          _startTimestamp(0),
                                                                          _endTimestamp(0) {


        _deleteScratchDirOnShutdown = false;
        _scratchDir = URI::INVALID;

        // // check that scratch dir is s3 path!
        // if(options.SCRATCH_DIR().prefix() != "s3://") // @TODO: check further it's a dir...
        //     throw std::runtime_error("need to provide as scratch dir an s3 path to Lambda backend");

        initAWS(credentials, _options.AWS_NETWORK_SETTINGS(), _options.AWS_REQUESTER_PAY());

        // several options are NOT supported currently in AWS Lambda Backend, hence
        // force them to what works
        if (_options.OPT_GENERATE_PARSER()) {
            logger().warn(
                    "using generated CSV parser not yet supported in AWS Lambda backend, defaulting back to original parser");
            _options.set("tuplex.optimizer.generateParser", "false");
        }
//        if(_options.OPT_NULLVALUE_OPTIMIZATION()) {
//            logger().warn("null value optimization not yet available for AWS Lambda backend, deactivating.");
//            _options.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
//        }

        _driver.reset(new Executor(_options.DRIVER_MEMORY(),
                                   _options.PARTITION_SIZE(),
                                   _options.RUNTIME_MEMORY(),
                                   _options.RUNTIME_MEMORY_DEFAULT_BLOCK_SIZE(),
                                   _options.SCRATCH_DIR(), "aws-local-driver"));

        _lambdaSizeInMB = _options.AWS_LAMBDA_MEMORY();
        _lambdaTimeOut = _options.AWS_LAMBDA_TIMEOUT();

        logger().info("Execution over lambda with " + std::to_string(_lambdaSizeInMB) + "MB");

        // Lambda supports 1MB increments. Hence, no adjustment to 64MB granularity anymore necessary as in prior to Dec 2020.
        _lambdaSizeInMB = std::min(std::max(AWS_MINIMUM_LAMBDA_MEMORY_MB, _lambdaSizeInMB),
                                   AWS_MAXIMUM_LAMBDA_MEMORY_MB);
        logger().info("Adjusted lambda size to " + std::to_string(_lambdaSizeInMB) + "MB");

        if (_lambdaTimeOut < 10 || _lambdaTimeOut > 15 * 60) {
            _lambdaTimeOut = std::min(std::max(AWS_MINIMUM_TUPLEX_TIMEOUT_REQUIREMENT, _lambdaTimeOut),
                                      AWS_MAXIMUM_LAMBDA_TIMEOUT); // min 5s, max 15min
            logger().info("Adjusted lambda timeout to " + std::to_string(_lambdaTimeOut));
        }

        // init lambda client (Note: must be called AFTER aws init!)
        _client = makeClient();

        // init invocation service
        _service.reset(new AwsLambdaInvocationService(_client, functionName));

        if (_options.USE_WEBUI()) {

            TUPLEX_TRACE("initializing REST/Curl interface");
            // init rest interface if required (check if already done by AWS!)
            RESTInterface::init();
            TUPLEX_TRACE("creating history server connector");
            _historyConn = HistoryServerConnector::connect(_options.WEBUI_HOST(),
                                                           _options.WEBUI_PORT(),
                                                           _options.WEBUI_DATABASE_HOST(),
                                                           _options.WEBUI_DATABASE_PORT());
            TUPLEX_TRACE("connection established");
        }
    }

    static void
    printBreakdowns(const std::map<std::string, RollingStats<double>> &breakdownTimings, std::stringstream &ss) {
        ss << "{";

        size_t prefix_offset = 3;
        std::string found_prefixes[2] = {"", ""};
        std::string prefixes[2] = {"process_mem_", "process_file_"};
        std::map<std::string, RollingStats<double>> m[2];

        bool first_breakdown = true;
        auto print_breakdown = [&](const std::pair<std::string, RollingStats<double>> &keyval) {
            if (first_breakdown) {
                first_breakdown = false;
            } else {
                ss << ", ";
            }
            ss << "\"" << keyval.first << "\": { \"mean\": " << keyval.second.mean() << ", \"std\": "
               << keyval.second.std() << "}";
        };

        for (const auto &keyval: breakdownTimings) {
            auto is_prefix = [prefix_offset](const std::string &a, const std::string &b) {
                // check if a is a prefix of b
                if (a.size() + prefix_offset <= b.size()) {
                    auto res = std::mismatch(a.begin(), a.end(), b.begin() + prefix_offset);
                    return res.first == a.end();
                }
                return false;
            };
            if (is_prefix(prefixes[0], keyval.first) || is_prefix(prefixes[1], keyval.first)) {
                auto prefix_idx = is_prefix(prefixes[0], keyval.first) ? 0 : 1;
                found_prefixes[prefix_idx] = keyval.first.substr(0, prefixes[prefix_idx].length() + prefix_offset);
                auto suffix = keyval.first.substr(found_prefixes[prefix_idx].length());
                m[prefix_idx][suffix] = keyval.second;
            } else {
                print_breakdown(keyval);
            }
        }

        for (int i = 0; i < 2; i++) {
            if (found_prefixes[i].empty()) continue;
            ss << ", ";
            ss << "\"" << found_prefixes[i] << "\": {";
            first_breakdown = true;
            for (const auto &keyval: m[i]) {
                print_breakdown(keyval);
            }
            ss << "}";
        }

        ss << "}\n";
    }

    struct PerFileInfo {
        std::string requestId;
        size_t in_normal;
        size_t in_general;
        size_t in_fallback;
        size_t in_unresolved;
        size_t out_normal;
        size_t out_unresolved;

        // for convenience
        std::string log;

        inline std::string to_csv() const {
            std::stringstream ss;
            ss<<requestId<<","
              <<in_normal<<","
              <<in_general<<","
              <<in_fallback<<","
              <<in_unresolved<<","
              <<out_normal<<","
              <<out_unresolved;
            return ss.str();
        }

        inline std::string header() const {
            return "requestId,in_normal,in_general,in_fallback,in_unresolved,out_normal,out_unresolved";
        }
    };

    std::string AwsLambdaBackend::csvPerFileInfo() {
        using namespace std;

        // settings etc.
        bool hyper_mode_str = (_options.USE_EXPERIMENTAL_HYPERSPECIALIZATION() ? "true" : "false");

        std::unordered_map<std::string, PerFileInfo> uri_map;

        // 1. tasks
        {
            std::lock_guard<std::mutex> lock(_mutex);

            int task_counter = 0;
            for (const auto &task: _tasks) {
                ContainerInfo info = task.response.container();

                std::string log = "";
                // log
                for (const auto &r: task.response.resources()) {
                    if (r.type() == static_cast<uint32_t>(ResourceType::LOG)) {
                        log = decompress_string(r.payload());
                        break;
                    }
                }

               // invoked input uris
                for (unsigned i = 0; i < task.response.inputuris_size(); ++i) {
                    auto input_uri = task.response.inputuris(i);

                    // clean from part
                    URI uri;
                    size_t rangeStart = 0, rangeEnd = 0;
                    decodeRangeURI(input_uri, uri, rangeStart, rangeEnd);

                    auto it = uri_map.find(uri.toPath());
                    if(it != uri_map.end())
                        uri_map[uri.toPath()] = PerFileInfo();

                    auto& f_info = uri_map[uri.toPath()];
                    f_info.requestId = info.requestId;
                    f_info.log = log;

                }

                task_counter++;
            }
        }


        // 2. requests & responses?
        {
            std::lock_guard<std::mutex> lock(_mutex);
            for (unsigned i = 0; i < _tasks.size(); ++i) {
                auto &info = _tasks[i].info;
                // info has information as well

                // find in map the info with requestid
                for(auto& kv : uri_map) {
                    if(kv.second.requestId == info.awsRequestId) {
                        // save now in normal etc.
                        kv.second.in_normal = info.in_normal;
                        kv.second.in_general = info.in_general;
                        kv.second.in_fallback = info.in_fallback;
                        kv.second.in_unresolved = info.in_unresolved;
                        kv.second.out_normal = info.out_normal;
                        kv.second.out_unresolved = info.out_unresolved;
                    }
                }
            }
        }

        // create version (sort after first entry!)
        std::vector<std::tuple<std::string, std::string>> entries;
        std::string header;
        std::stringstream ss;
        bool first_time = true;
        for(const auto& kv : uri_map) {
            if(first_time) {
                first_time = false;
                header = "uri," + kv.second.header();
            }
            entries.push_back(std::make_tuple(kv.first, kv.second.to_csv()));

            // write to local dir for convenience
            auto log_path = "./logs/" + URI(kv.first).base_name()  + ".log.txt";
            stringToFile(log_path, kv.second.log);
        }

        std::sort(entries.begin(), entries.end(), [](const std::tuple<std::string, std::string>& rhs, const std::tuple<std::string, std::string>& lhs) {
           return std::get<0>(rhs) < std::get<0>(lhs);
        });

        ss<<header<<"\n";
        for(auto entry : entries)
            ss<<std::get<0>(entry)<<","<<std::get<1>(entry)<<"\n";

        return ss.str();
    }

    void AwsLambdaBackend::dumpAsJSON(const std::string &json_path) {
        using namespace std;
        stringstream ss;

        // Extract triplets from internal storage.
        vector<TaskTriplet> requests_responses;
        {
            std::lock_guard<std::mutex> lock(_mutex);
            for (const auto &task: _tasks) {
                messages::InvocationRequest req;
                req.set_type(messages::MessageType::MT_UNKNOWN); // Unknown means missing here.

                // Lookup based on dict, if not check for failed requests.
                auto requestId = task.info.requestId;
                if(_invokedRequests.find(requestId) != _invokedRequests.end()) {
                    req = _invokedRequests.at(requestId).body;
                } else {
                    logger().debug("Did not find request for " + requestId + ", checking failed requests.");
                    for(const auto& failed_request_info : _failedRequests) {
                        if(uuidToString(get<2>(failed_request_info).id) == requestId) {
                            req = get<2>(failed_request_info).body;
                            break;
                        }
                    }
                    if(req.type() == messages::MessageType::MT_UNKNOWN) {
                        logger().error("Could not find request in successful and failed requests with id " + requestId +", storing dummy.");
                    }
                }
                requests_responses.push_back({task.info, req, task.response});
            }
        }

        IRequestBackend::dumpAsJSON(ss, _startTimestamp, _endTimestamp, _options.USE_EXPERIMENTAL_HYPERSPECIALIZATION(),_info.cost,
                                    numRequests(), _total_mbms, costPerGBSecond(), _info.total_input_normal_path, _info.total_input_general_path, _info.total_input_fallback_path, _info.total_input_unresolved,
                                    _info.total_output_rows, _info.total_output_exceptions, requests_responses);

//        ss << "{";
//
//        // 0. general info
//        ss << "\"stageStartTimestamp\":" << _startTimestamp << ",";
//        ss << "\"stageEndTimestamp\":" << _endTimestamp << ",";
//
//        // settings etc.
//        ss << "\"hyper_mode\":" << (_options.USE_EXPERIMENTAL_HYPERSPECIALIZATION() ? "true" : "false") << ",";
//        ss << "\"cost\":" << _info.cost << ",";
//
//        // detailed billing info to breakdown better.
//        ss <<"\"billing\":{";
//        ss<<"\"requestCount\":"<<numRequests()<<",";
//        ss<<"\"mbms\":"<<_total_mbms<<",";
//        ss<<"\"costPerGBSecond\":"<<costPerGBSecond();
//        ss<<"},";
//
//        ss << "\"input_paths_taken\":{"
//           << "\"normal\":" << _info.total_input_normal_path << ","
//           << "\"general\":" << _info.total_input_general_path << ","
//           << "\"fallback\":" << _info.total_input_fallback_path << ","
//           << "\"unresolved\":" << _info.total_input_unresolved
//           << "},";
//        ss << "\"output_paths_taken\":{"
//           << "\"normal\":" << _info.total_output_rows << ","
//           << "\"unresolved\":" << _info.total_output_exceptions
//           << "},";
//
//
//        // 1. tasks
//        ss << "\"tasks\":[";
//        {
//            std::lock_guard<std::mutex> lock(_mutex);
//
//            int task_counter = 0;
//            for (const auto &task: _tasks) {
//                ContainerInfo info = task.response.container();
//                ss << "{\"container\":" << info.asJSON() << ",\"invoked_containers\":[";
//                for (unsigned i = 0; i < task.response.invokedresponses_size(); ++i) {
//                    auto invoked_response = task.response.invokedresponses(i);
//                    ContainerInfo info = invoked_response.container();
//                    ss << info.asJSON();
//                    if (i != task.response.invokedresponses_size() - 1)
//                        ss << ",";
//                }
//                ss << "]";
//
//                // log
//                for (const auto &r: task.response.resources()) {
//                    if (r.type() == static_cast<uint32_t>(ResourceType::LOG)) {
//                        auto log = decompress_string(r.payload());
//                        ss << ",\"log\":" << escape_for_json(log) << "";
//                        break;
//                    }
//                }
//
//                ss << ",\"invoked_requests\":[";
//                RequestInfo r_info;
//                for (int i = 0; i < task.response.invokedrequests_size(); ++i) {
//                    r_info = task.response.invokedrequests(i);
//                    r_info.fillInFromResponse(task.response.invokedresponses(i));
//                    ss << r_info.asJSON();
//                    if (i != task.response.invokedrequests_size() - 1)
//                        ss << ",";
//                }
//                ss << "]";
//
//                // invoked input uris
//                ss << ",\"input_uris\":[";
//                for (unsigned i = 0; i < task.response.inputuris_size(); ++i) {
//                    ss << "\"" << task.response.inputuris(i) << "\"";
//                    if (i != task.response.inputuris_size() - 1)
//                        ss << ",";
//                }
//
//                ss << "]";
//
//                // end container.
//                ss << "}";
//                if (task_counter != _tasks.size() - 1)
//                    ss << ",";
//                task_counter++;
//            }
//        }
//        ss << "],";
//
//
//        // 2. requests & responses?
//        // 1. tasks
//        ss << "\"requests\":[";
//        {
//            std::lock_guard<std::mutex> lock(_mutex);
//            for (unsigned i = 0; i < _tasks.size(); ++i) {
//                auto &info = _tasks[i].info;
//                ss << info.asJSON();
//                if (i != _tasks.size() - 1)
//                    ss << ",";
//            }
//        }
//        ss << "]";
//
//        // Dump all responses (full data for analysis).
//        ss << ",\"responses\":[";
//        {
//            std::lock_guard<std::mutex> lock(_mutex);
//            for (unsigned i = 0; i < _tasks.size(); ++i) {
//                std::string response_as_str;
//                google::protobuf::util::MessageToJsonString(_tasks[i].response, &response_as_str);
//                ss << response_as_str;
//                if (i != _tasks.size() - 1)
//                    ss << ",";
//            }
//        }
//        ss << "]";
//
//        ss << "}";

        stringToFile(json_path, ss.str());
    }

    void AwsLambdaBackend::gatherStatistics() {
        std::stringstream ss;

        _info.cost = lambdaCost();

        {
            std::lock_guard<std::mutex> lock(_mutex);

            RollingStats<double> awsInitTime;
            RollingStats<double> taskExecutionTime;
            std::map<std::string, RollingStats<size_t>> s3Stats;
            std::map<std::string, RollingStats<double>> breakdownTimings;
            std::set<std::string> containerIDs;
            size_t numReused = 0;
            size_t numNew = 0;

            size_t total_num_output_rows = 0;
            size_t total_num_exceptions = 0;
            size_t total_bytes_written = 0;

            // paths rows took
            size_t total_normal_path = 0;
            size_t total_general_path = 0;
            size_t total_interpreter_path = 0;
            size_t total_unresolved = 0;

            // sanity checks, output paths -> these should be unique.
            std::vector<std::string> written_output_paths;

            // aggregate stats over responses
            for (const auto &task: _tasks) {
                total_num_output_rows += task.response.numrowswritten();
                total_num_exceptions += task.response.numexceptions();
                total_bytes_written += task.response.numbyteswritten();

                total_normal_path += task.response.rowstats().normal();
                total_general_path += task.response.rowstats().general();
                total_interpreter_path += task.response.rowstats().interpreter();
                total_unresolved += task.response.rowstats().unresolved();


                std::copy(task.response.outputuris().begin(), task.response.outputuris().end(), std::back_inserter(written_output_paths));

                awsInitTime.update(task.response.awsinittime());
                taskExecutionTime.update(task.response.taskexecutiontime());
                for (const auto &keyval: task.response.s3stats()) {
                    auto key = keyval.first;
                    auto val = keyval.second;

                    auto it = s3Stats.find(key);
                    if (it == s3Stats.end())
                        s3Stats[key] = RollingStats<size_t>();
                    s3Stats[key].update(val);
                }
                for (const auto &keyval: task.response.breakdowntimes()) {
                    auto key = keyval.first;
                    auto val = keyval.second;

                    auto it = breakdownTimings.find(key);
                    if (it == breakdownTimings.end())
                        breakdownTimings[key] = RollingStats<double>();
                    breakdownTimings[key].update(val);
                }

                containerIDs.insert(task.response.container().uuid());
                numReused += task.response.container().reused();
                numNew += !task.response.container().reused();

                // Add recursive invocation statistics:
                if(task.response.invokedresponses_size() > 0) {
                    for(const auto& self_invoke_response: task.response.invokedresponses()) {
                        containerIDs.insert(self_invoke_response.container().uuid());
                        numReused += self_invoke_response.container().reused();
                        numNew += !task.response.container().reused();

                        total_num_output_rows += self_invoke_response.numrowswritten();
                        total_num_exceptions += self_invoke_response.numexceptions();

                        std::copy(self_invoke_response.outputuris().begin(), self_invoke_response.outputuris().end(), std::back_inserter(written_output_paths));
                    }
                }
            }

            // print stage stats
            logger().info("LAMBDA Stage output: " + pluralize(total_num_output_rows, "row") + ", "
                          + pluralize(total_num_exceptions, "exception"));
            std::string general_info = _options.RESOLVE_WITH_INTERPRETER_ONLY() ? "(deactivated)" : std::to_string(
                    total_general_path);
            logger().info("LAMBDA paths input rows took: normal: " + std::to_string(total_normal_path)
                          + " general: " + general_info + " interpreter: "
                          + std::to_string(total_interpreter_path));

            std::unordered_set<URI> unique_written_output_paths(written_output_paths.begin(), written_output_paths.end());
            logger().info("LAMBDA wrote " + pluralize(written_output_paths.size(), "output uri") + " (" + pluralize(unique_written_output_paths.size(), "unique uri") + ")");

            // If non unique URIs, check what went wrong by explicitly listing debug information
            if(written_output_paths.size() != unique_written_output_paths.size()) {
                // go over request and print out for each specializaition unit which output uris were created.
                std::stringstream ss;
                ss<<"Output uris do not seem unique:\n"<<std::endl;

                for (const auto &task: _tasks) {
                    std::vector<std::string> output_paths;
                    for(auto uri : task.response.outputuris())
                        output_paths.push_back(uri);
                    if(task.response.invokedresponses_size() > 0) {
                        for(const auto& self_invoke_response: task.response.invokedresponses()) {
                            for(auto uri : self_invoke_response.outputuris())
                                output_paths.push_back(uri);
                        }
                    }

                    ss << "Task " << task.info.requestId << ":\n";
                    for(auto path : output_paths) {
                        ss<<"  -- "<<path<<std::endl;
                    }
                }

                logger().error(ss.str());
            }


            // save to internal
            _info.total_input_normal_path = total_normal_path;
            _info.total_input_general_path = total_general_path;
            _info.total_input_fallback_path = total_interpreter_path;
            _info.total_input_unresolved = total_unresolved; // <-- this doesn't really make sense, unresolved as input?? -> only from previous caching... | this is a check more or less.

            _info.total_output_rows = total_num_output_rows;
            _info.total_output_exceptions = total_num_exceptions;

//            // print exception summary if any occurred
//            if(total_num_exceptions > 0) {
//                printErrorTreeHelper
//            }

            // compute cost of s3 + Lambda
            ss << "Lambda #containers used: " << containerIDs.size() << " reused: " << numReused
               << " newly initialized: " << numNew << "\n";
            ss << "Lambda init time: " << awsInitTime.mean() << " +- " << awsInitTime.std() << " min: "
               << awsInitTime.min() << " max: " << awsInitTime.max() << "\n";
            ss << "Lambda execution time: " << taskExecutionTime.mean() << " +- " << taskExecutionTime.std() << " min: "
               << taskExecutionTime.min() << " max: " << taskExecutionTime.max() << "\n";
            // compute S3 cost => this is more complicated, i.e. postpone
            // for(auto keyval : s3Stats) {
            //     ss<<"s3: "<<keyval.first<<" sum: "<<keyval.second.mean()<<"\n";
            // }

            // breakdown timings
            ss << "\n----- BREAKDOWN TIMINGS -----\n";
            printBreakdowns(breakdownTimings, ss);
        }

        ss << "Lambda cost: $" << lambdaCost();

        logger().info("LAMBDA statistics: \n" + ss.str());
    }

    URI AwsLambdaBackend::scratchDir(const std::vector<URI> &hints) {
        // is URI valid? return
        if (_scratchDir != URI::INVALID)
            return _scratchDir;

        // fetch dir from options
        auto ctx_scratch_dir = _options.AWS_SCRATCH_DIR();
        if (!ctx_scratch_dir.empty()) {
            _scratchDir = URI(ctx_scratch_dir);
            if (_scratchDir.prefix() != "s3://") // force S3
                _scratchDir = URI("s3://" + ctx_scratch_dir);
            _deleteScratchDirOnShutdown = false; // if given externally, do not delete per default
            return _scratchDir;
        }

        auto cache_folder = ".tuplex-cache";

        // check hints
        for (const auto &hint: hints) {
            if (hint.prefix() != "s3://") {
                logger().warn("AWS scratch dir hint given, but is no S3 URI: " + hint.toString());
                continue;
            }

            // check whether a file exists, if so skip, else valid dir found!
            auto dir = hint.join_path(cache_folder);
            if (!dir.exists()) {
                _scratchDir = dir;
                _deleteScratchDirOnShutdown = true;
                logger().info("Using " + dir.toString() +
                              " as temporary AWS S3 scratch dir, will be deleted on tuplex context shutdown.");
                return _scratchDir;
            }
        }

        // invalid, no aws scratch dir available
        logger().error(
                "requesting AWS S3 scratch dir, but none configured. Please set a AWS S3 scratch dir for the context by setting the config key tuplex.aws.scratchDir to a valid S3 URI");
        return URI::INVALID;
    }

    std::vector<URI> AwsLambdaBackend::hintsFromTransformStage(const TransformStage *stage) {
        std::vector<URI> hints;

        // take input and output folder as hints
        // prefer output folder hint over input folder hint
        if (stage->outputMode() == EndPointMode::FILE) {
            auto uri = stage->outputURI();
            if (uri.prefix() == "s3://")
                hints.push_back(uri);
        }

        if (stage->inputMode() == EndPointMode::FILE) {
            // TODO
            // get S3 uris, etc.
        }

        return hints;
    }

    void AwsLambdaBackend::reset() {
        _tasks.clear();
        _failedRequests.clear();
        _invokedRequests.clear();

        // reset path mapping
        _remoteToLocalURIMapping.clear();

        // make sure to reset service and cost calculations there!
        if(_service)
            _service->reset();

        // reset billing
        resetBilling();

        _info.reset();

        // other reset? @TODO.
    }


    void AwsLambdaBackend::validate_requests_can_be_served_with_concurrency_limit(const std::vector<AwsLambdaRequest>& requests,
                                                                                  size_t concurrency_limit) {
        size_t min_concurrency_needed = 1;
        for(const auto& req : requests) {
            size_t invocations = 1 + recursive_invocation_count(req.body);
            min_concurrency_needed = std::max(invocations, min_concurrency_needed);
        }

        if(min_concurrency_needed > concurrency_limit)
            throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " Recursive requests need at least " + std::to_string(min_concurrency_needed) + " concurrency, but Lambda is configured with " + std::to_string(concurrency_limit) + " concurrency limit.");
    }

    AwsLambdaRequest& find_request_with_least_bytes_to_process(std::vector<AwsLambdaRequest>& requests) {
        size_t minimum_total_size = std::numeric_limits<size_t>::max();
        int smallest_element_idx = 0;

        assert(!requests.empty());

        int pos = 0;
        for(const auto& req: requests) {

            // compute size of request
            size_t req_total_size = 0;
            for(unsigned i = 0; i < req.body.inputuris_size(); ++i) {
                // range URI?
                std::string uri = req.body.inputuris(i);
                URI baseURI;
                size_t range_start=0, range_end=0;
                decodeRangeURI(uri, baseURI, range_start, range_end);
                if(uri != baseURI.toString() && !(range_start == 0 && range_end == 0)) {
                    req_total_size += range_end - range_start;
                } else
                    req_total_size += req.body.inputsizes(i);
            }

            // recursive requests.
            if(recursive_invocation_count(req.body) != 0)
                throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " need to implement");

            if(req_total_size < minimum_total_size)
                smallest_element_idx = pos;
            minimum_total_size = std::min(req_total_size, minimum_total_size);
            pos++;
        }
        assert(smallest_element_idx >= 0);
        return requests[smallest_element_idx];
    }


//    std::vector<AwsLambdaRequest> AwsLambdaBackend::create_specializing_requests_with_multiple_files(
//            const TransformStage* tstage,
//            int numThreads,
//            const std::vector<std::tuple<std::string, size_t>>& uri_infos, MessageHandler& logger,
//            std::function<std::string(int n, int n_digits)> generate_output_base_uri) {
//
//        // no self-invocation because too few parallelism.
//        // But issue directly requests, some may have multiple files. Still specialize PER file.
//        size_t total_size_in_bytes_to_process = 0;
//        for(const auto& info : uri_infos)
//            total_size_in_bytes_to_process += std::get<1>(info);
//
//        size_t avg_size_in_bytes_to_process = total_size_in_bytes_to_process / uri_infos.size();
//        {
//            std::stringstream ss;
//            ss<<pluralize(uri_infos.size(), "input file")<<" ("<<sizeToMemString(total_size_in_bytes_to_process)<<") to distribute (avg size per file: "<<sizeToMemString(avg_size_in_bytes_to_process)<<").";
//            logger.debug(ss.str());
//        }
//
//        auto sampling_mode = tstage->samplingMode();
//
//        // later: need to sort requests
//        std::vector<AwsLambdaRequest> requests;
//
//        // sort files after size (heuristic).
//        auto uri_infos_sorted_by_size = uri_infos;
//        std::sort(uri_infos_sorted_by_size.begin(), uri_infos_sorted_by_size.end(),
//                  [](const std::tuple<std::string, size_t>& lhs, const std::tuple<std::string, size_t>& rhs) {
//            return std::get<1>(lhs) >= std::get<1>(rhs);
//        });
//
//        // this approach ensures greedy file selection.
//        size_t max_parallelism =  _options.AWS_MAX_CONCURRENCY();
//
//        // Doesn't preserve order of input files.
//        for(const auto& info : uri_infos_sorted_by_size) {
//            URI uri = std::get<0>(info);
//            size_t uri_size = std::get<1>(info);
//
//            if(requests.size() < max_parallelism) {
//                AwsLambdaRequest req;
//
//                req.body.add_inputuris(uri.toString());
//                req.body.add_inputsizes(uri_size);
//
//                // no split up necessary, simply process full data.
//                req.body.mutable_stage()->add_invocationcount(0);
//                requests.emplace_back(req);
//            } else {
//                // add to file with smallest total bytes sum.
//                auto& req = find_request_with_least_bytes_to_process(requests);
//
//                // add this file to request. Note: could split up file as well in the future!
//                req.body.add_inputuris(uri.toString());
//                req.body.add_inputsizes(uri_size);
//            }
//        }
//
//        // Step 2: generate output uris (consecutive)
//        int n_output_parts = 0;
//        // Count how many output parts there will be.
//        for(const auto& req : requests) {
//            n_output_parts++; // +1 for the request itself.
//            // Are there self-invocations? For each invocation count +1.
//            // Only 1 level supported yet.
//            n_output_parts += recursive_invocation_count(req.body);
//        }
//        int n_digits = ilog10c(n_output_parts) + 1;
//        logger.info("Recursive tree invocation will invoke in total " + pluralize(n_output_parts, "lambda") + " producing " + pluralize(n_output_parts, "part") + ".");
//
//        // Go over requests again & set part offsets.
//        int task_no = 0;
//        auto num_digits = ilog10c(requests.size()) + 1;
//        for(auto& req : requests) {
//            req.body.set_partnooffset(task_no);
//            auto lambda_output_uri = generate_output_base_uri(task_no, n_digits);
//            req.body.set_baseoutputuri(lambda_output_uri);
//            req.body.set_baseisfinaloutput(true); // <-- is final, the recursive will modify it.
//            task_no += 1 + recursive_invocation_count(req.body);
//
//
//            // fill with trafo.
//            fill_with_transform_stage(req.body, tstage);
//            fill_env(req.body);
//
//            // for different parts need different spill folders.
//            auto worker_spill_uri = spillURI() + "/lam" + fixedLength(task_no, num_digits);
//            fill_with_worker_config(req.body, worker_spill_uri, numThreads);
//
//            // Check whether specialization should occur. If so, add specialization units for each file exceeding threshold.
//            if(_options.USE_EXPERIMENTAL_HYPERSPECIALIZATION()) {
//                req.body.mutable_stage()->mutable_specializationunit()->set_samplemode(static_cast<uint64_t>(sampling_mode));
//
//                for(unsigned i = 0; i < req.body.inputuris_size(); ++i) {
//                    auto su = req.body.mutable_stage()->mutable_specializationunit();
//                    // Other param re specialization unit size NOT YET SUPPORTED.
//                    if(req.body.inputsizes(i) > _options.EXPERIMENTAL_MINIMUM_SIZE_TO_SPECIALIZE()) {
//                        su->add_inputuris(req.body.inputuris(i));
//                        su->add_inputsizes(req.body.inputuris_size());
//                    }
//                }
//            }
//        }
//
//        logger.info("Recursive Lambda invocation will produce " + pluralize(task_no, "output file") + ".");
//
//        return requests;
//    }

    std::vector<AwsLambdaRequest> AwsLambdaBackend::create_specializing_requests_with_multiple_files(
            const TransformStage* tstage,
            int numThreads,
            const std::vector<std::tuple<std::string, size_t>>& uri_infos, MessageHandler& logger,
            std::function<std::string(int n, int n_digits)> generate_output_base_uri) {

        // no self-invocation because too few parallelism.
        // But issue directly requests, some may have multiple files. Still specialize PER file.
        size_t total_size_in_bytes_to_process = 0;
        for(const auto& info : uri_infos)
            total_size_in_bytes_to_process += std::get<1>(info);

        size_t avg_size_in_bytes_to_process = total_size_in_bytes_to_process / uri_infos.size();
        {
            std::stringstream ss;
            ss<<pluralize(uri_infos.size(), "input file")<<" ("<<sizeToMemString(total_size_in_bytes_to_process)<<") to distribute (avg size per file: "<<sizeToMemString(avg_size_in_bytes_to_process)<<").";
            logger.debug(ss.str());
        }

        auto sampling_mode = tstage->samplingMode();

        // later: need to sort requests
        std::vector<AwsLambdaRequest> requests;

        // this approach ensures greedy file selection.
        size_t max_parallelism =  _options.AWS_MAX_CONCURRENCY();

        // For each file assume one request.
        for(const auto& info : uri_infos) {
            URI uri = std::get<0>(info);
            size_t uri_size = std::get<1>(info);

            AwsLambdaRequest req;

            req.body.add_inputuris(uri.toString());
            req.body.add_inputsizes(uri_size);

            // no split up necessary, simply process full data.
            req.body.mutable_stage()->add_invocationcount(0);
            requests.emplace_back(req);
        }

        // Step 2: generate output uris (consecutive)
        int n_output_parts = 0;
        // Count how many output parts there will be.
        for(const auto& req : requests) {
            n_output_parts++; // +1 for the request itself.
            // Are there self-invocations? For each invocation count +1.
            // Only 1 level supported yet.
            n_output_parts += recursive_invocation_count(req.body);
        }
        int n_digits = ilog10c(n_output_parts) + 1;
        logger.info("Recursive tree invocation will invoke in total " + pluralize(n_output_parts, "lambda") + " producing " + pluralize(n_output_parts, "part") + ".");

        // Go over requests again & set part offsets.
        int task_no = 0;
        auto num_digits = ilog10c(requests.size()) + 1;
        for(auto& req : requests) {
            req.body.set_partnooffset(task_no);
            auto lambda_output_uri = generate_output_base_uri(task_no, n_digits);
            req.body.set_baseoutputuri(lambda_output_uri);
            req.body.set_baseisfinaloutput(true); // <-- is final, the recursive will modify it.
            task_no += 1 + recursive_invocation_count(req.body);


            // fill with trafo.
            fill_with_transform_stage(req.body, tstage);
            fill_env(req.body);

            // for different parts need different spill folders.
            auto worker_spill_uri = spillURI() + "/lam" + fixedLength(task_no, num_digits);
            fill_with_worker_config(req.body, worker_spill_uri, numThreads);

            // Check whether specialization should occur. If so, add specialization units for each file exceeding threshold.
            if(_options.USE_EXPERIMENTAL_HYPERSPECIALIZATION()) {
                req.body.mutable_stage()->mutable_specializationunit()->set_samplemode(static_cast<uint64_t>(sampling_mode));

                for(unsigned i = 0; i < req.body.inputuris_size(); ++i) {
                    auto su = req.body.mutable_stage()->mutable_specializationunit();
                    // Other param re specialization unit size NOT YET SUPPORTED.
                    if(req.body.inputsizes(i) > _options.EXPERIMENTAL_MINIMUM_SIZE_TO_SPECIALIZE()) {
                        su->add_inputuris(req.body.inputuris(i));
                        su->add_inputsizes(req.body.inputuris_size());
                    }
                }
            }
        }

        // Go over requests again & set part offsets.
        task_no = 0;
        for(auto& req : requests) {
            req.body.set_partnooffset(task_no);
            auto lambda_output_uri = generate_output_base_uri(task_no, n_digits);
            req.body.set_baseoutputuri(lambda_output_uri);
            req.body.set_baseisfinaloutput(true); // <-- is final, the recursive will modify it.
            task_no += 1 + recursive_invocation_count(req.body);
        }

        logger.info("Recursive Lambda invocation will produce " + pluralize(task_no, "output file") + ".");


        logger.info("Lambda invocation will produce " + pluralize(task_no, "output file") + ".");



        return requests;
    }

}
#endif