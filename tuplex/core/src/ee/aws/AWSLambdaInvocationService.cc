//
// Created by leonhards on 4/13/23.
//

#ifdef BUILD_WITH_AWS

#include <ee/aws/AWSLambdaInvocationService.h>
#include <google/protobuf/util/json_util.h>

namespace tuplex {

    // helper class to provide backend in callback
    class AwsLambdaBackendCallerContext : public Aws::Client::AsyncCallerContext {
    private:
        AwsLambdaInvocationService *_service;
        std::chrono::high_resolution_clock::time_point _ts;
        uint64_t _tsUTC; //! utc start of this request

        // original request, restore basically from data
        AwsLambdaRequest _original_request;
        std::string _payload;

        // callbacks to trigger when async invocation of lambda finishes
        // note that when these functions are called, the mutex of the service MUST NOT BE LOCKED.

        // called before the next retry is issued.
        // original_request, retry_error_code, retry_message, willDecreaseRetryCount
        std::function<void(const AwsLambdaRequest&, LambdaStatusCode, const std::string&, bool)> _onRetry;

        // called when Lambda returns normally
        // original_request, response_received
        std::function<void(const AwsLambdaRequest&, const AwsLambdaResponse&)> _onSuccess;

        // called when retries are exhausted and Lambda fails for good.
        // original_request, final_error_code, error_message
        std::function<void(const AwsLambdaRequest&, LambdaStatusCode, const std::string&)> _onFailure;
    public:
        AwsLambdaBackendCallerContext() = delete;

        AwsLambdaBackendCallerContext(AwsLambdaInvocationService *service,
                                     const AwsLambdaRequest& req,
                                      std::function<void(const AwsLambdaRequest&, const AwsLambdaResponse&)> onSuccess=[](const AwsLambdaRequest& req, const AwsLambdaResponse& resp) {},
                                      std::function<void(const AwsLambdaRequest&, LambdaStatusCode, const std::string&)> onFailure=[](const AwsLambdaRequest& req,
                                                                                                                                      LambdaStatusCode err_code,
                                                                                                                                      const std::string& err_msg) {},
                                      std::function<void(const AwsLambdaRequest&, LambdaStatusCode, const std::string&, bool)> onRetry=[](const AwsLambdaRequest& req,
                                                                                                                                          LambdaStatusCode retry_code,
                                                                                                                                          const std::string& retry_reason,
                                                                                                                                          bool willDecreaseRetryCount) {})
                : _service(service),
                  _ts(std::chrono::high_resolution_clock::now()),
                  _tsUTC(current_utc_timestamp()),
                  _original_request(req),
                  _onSuccess(onSuccess),
                  _onFailure(onFailure),
                  _onRetry(onRetry){
        }

        AwsLambdaInvocationService *getService() const { return _service; }

        std::string payload() const { return _original_request.body.SerializeAsString(); }

        uint64_t utc_start() const { return _tsUTC; }

        double time() const {
            auto stop = std::chrono::high_resolution_clock::now();
            double duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - _ts).count() / 1000000000.0;
            return duration;
        }

        AwsLambdaRequest original_request() const { return _original_request; }
        uniqueid_t getTaskID() const { return _original_request.id; }

        // helper functions triggering the callback
        void success(const AwsLambdaResponse& response) const {
            if(_onSuccess)
                _onSuccess(_original_request, response);
        }

        void retry(const LambdaStatusCode& retry_code, const std::string& retry_message, bool willDecreaseRetryCount) const {
            if(_onRetry)
                _onRetry(_original_request, retry_code, retry_message, willDecreaseRetryCount);
        }

        void fail(const LambdaStatusCode& err_code, const std::string& err_message) const {
            if(_onFailure)
                _onFailure(_original_request, err_code, err_message);
        }
    };

    messages::InvocationResponse AwsParseRequestPayload(const Aws::Lambda::Model::InvokeResult &result) {
        using namespace std;
        stringstream ss;
        auto &stream = const_cast<Aws::Lambda::Model::InvokeResult &>(result).GetPayload();
        ss << stream.rdbuf();
        string data = ss.str();
        messages::InvocationResponse response;
        google::protobuf::util::JsonStringToMessage(data, &response);
        return response;
    }

    void AwsLambdaInvocationService::reset() {
        abortAllRequests(false);

        std::lock_guard<std::mutex> lock(_mutex);

        _numPendingRequests = 0;
        _numRequests = 0;

        // reset cost counters as well!
        _mbms = 0;
    }

    size_t AwsLambdaInvocationService::abortAllRequests(bool print) {
        // abort all active requests
        int numPending = std::max((int) _numPendingRequests.load(), 0);
        if(print) {
            if (numPending > 0)
                logger().info("Aborting " + pluralize(numPending, " pending request"));
            else
                logger().info("Aborting.");
        }

        _numPendingRequests = 0;
        _client->DisableRequestProcessing();
        if(print)
            logger().info("Shutdown remote execution.");
        _client->EnableRequestProcessing();

        return numPending;
    }

    bool AwsLambdaInvocationService::invokeAsync(const AwsLambdaRequest &req,
                                                 std::function<void(const AwsLambdaRequest &,
                                                                    const AwsLambdaResponse &)> onSuccess,
                                                 std::function<void(const AwsLambdaRequest &, LambdaStatusCode,
                                                                    const std::string &)> onFailure,
                                                 std::function<void(const AwsLambdaRequest &, LambdaStatusCode,
                                                                    const std::string &, bool)> onRetry) {

        // prepare request

        // add to pending list & invoke
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _numPendingRequests++;
            _numRequests++;

            // TOOD: change this.
            // _pendingRequests.push_back(req);
        }

        // invoke now
        auto taskID = req.id;
        // Note: If message too large, will receive RequestEntityTooLargeException. => need to deal with that!
        // async limit is 128K, requestresponse is 6MB.

        // https://www.stackery.io/blog/RequestEntityTooLargeException-aws-lambda-message-invocation-limits/
        // Note: ObjectExpiration date for temp objects and can also use multiobject delete request
        // https://aws.amazon.com/blogs/aws/amazon-s3-multi-object-deletion/

        // construct req object
        Aws::Lambda::Model::InvokeRequest invoke_req;
        invoke_req.SetFunctionName(_functionName.c_str());
        // note: may redesign lambda backend to work async, however then response only yields status code
        // i.e., everything regarding state needs to be managed explicitly...
        invoke_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
        // logtype to extract log data??
        //req.SetLogtype(Aws::Lambda::Model::LogType::None);
        invoke_req.SetLogType(Aws::Lambda::Model::LogType::Tail);
        // qualifier to specify a version to be invoked... ==> critical for multiple users!

        // this doesn't work, use json instead...
        // invoke_req.SetBody(stringToAWSStream(req.SerializeAsString()));
        // invoke_req.SetContentType("application/x-protobuf");

        std::string json_buf;
        google::protobuf::util::MessageToJsonString(req.body, &json_buf);
        invoke_req.SetBody(stringToAWSStream(json_buf));
        invoke_req.SetContentType("application/javascript");

        // send to client
        _client->InvokeAsync(invoke_req, AwsLambdaInvocationService::asyncLambdaCallback,
                             Aws::MakeShared<AwsLambdaBackendCallerContext>("LAMBDA", this, req, onSuccess, onFailure, onRetry));

        return true;
    }


    LambdaStatusCode AwsLambdaInvocationService::specialEventHappened(const std::string& log, const messages::InvocationResponse &response, std::string* event_message) {
        // search in log for timeout info
        // i.e., string should look something like this: "Task timed out after 15.02 seconds"
        std::string timeout_info = extractTimeoutStr(log);
        trim(timeout_info);

        if(!timeout_info.empty()) {
            if(event_message)
                *event_message = timeout_info;

            return LambdaStatusCode::ERROR_TIMEOUT;
        }

        // what error type is it?
        // could be time out, or Runtime exit error
        bool exited_with_error = log.find("Error: Runtime exited with error:") != std::string::npos;
        if(exited_with_error) {
            std::string exit_code_str = extractExitCodeStr(log);
            std::stringstream ss;
            ss<<"LAMBDA task failed with exit code "<<exit_code_str;
            if(event_message)
                *event_message = ss.str();
            return LambdaStatusCode::ERROR_WORKER_DOWN_WITH_EXIT_CODE;
        }

        // lambda may be shutdown b.c. of previous bad signal, check for string here.
        // in this case, simply ignore - and reissue query.
        auto needleI = "Previous invocation recevied unrecoverable signal, shutting down this Lambda container via exit(1)."; // <-- do not correct typo here, this here is correct
        auto needleII = "Previous invocation received unrecoverable signal, shutting down this Lambda container via exit(1)."; // <-- corrected typo for updated LAMBDA
        bool previous_failure_and_worker_shutdown = log.find(needleI) != std::string::npos || log.find(needleII) != std::string::npos;

        if(previous_failure_and_worker_shutdown)
            return LambdaStatusCode::ERROR_WORKER_DOWN_WITH_UNRECOVERABLE_SIGNAL;

        return LambdaStatusCode::OK;
    }

    void AwsLambdaInvocationService::asyncLambdaCallback(const Aws::Lambda::LambdaClient *client,
                                               const Aws::Lambda::Model::InvokeRequest &aws_req,
                                               const Aws::Lambda::Model::InvokeOutcome &aws_outcome,
                                               const std::shared_ptr<const Aws::Client::AsyncCallerContext> &ctx) {
        using namespace std;
        stringstream ss;

        // get timestamp
        auto tsEnd = current_utc_timestamp();

        auto lctx = dynamic_cast<const AwsLambdaBackendCallerContext *>(ctx.get());
        assert(lctx);

        auto tsStart = lctx->utc_start();
        auto service = lctx->getService(); assert(service);

        // Note: lambda needs to be explicitly configured for async invocation
        // -> https://docs.aws.amazon.com/lambda/latest/dg/lambda-dg.pdf, unhandled

        // recreate the original message
//        messages::InvocationRequest invoke_req;
//        invoke_req.ParseFromString(lctx->payload());
//        AwsLambdaRequest req;
         auto  req = lctx->original_request();

        int statusCode = 0;
        std::string log;
        if (!aws_outcome.IsSuccess()) {
            auto &error = aws_outcome.GetError();
            statusCode = static_cast<int>(error.GetResponseCode());

            // rate limit? => reissue request
            if (statusCode == static_cast<int>(Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS) || // i.e. 429
                statusCode == static_cast<int>(Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR)) {  // i.e. 500

                // this is a retry that doesn't change the retry count
                auto retry_message = "LAMBDA task failed (" + req.input_desc() + ") with [" + std::to_string(statusCode) +
                                       "], invoking again.";

                lctx->retry(LambdaStatusCode::ERROR_RATE_LIMIT, retry_message, false);

                // invoke again, do not change retry count - as this was a rate limit.
                service->invokeAsync(req);
                service->_numPendingRequests.fetch_add(-1, std::memory_order_release);
                return;
            } else {

                // this is a true failure, report as such.
                ss << "LAMBDA task failed (" + req.input_desc() + ") with [" << statusCode << "]"
                   << aws_outcome.GetError().GetExceptionName().c_str()
                   << aws_outcome.GetError().GetMessage().c_str();
                lctx->fail(LambdaStatusCode::ERROR_UNKNOWN, ss.str());
                service->_numPendingRequests.fetch_add(-1);
                return;
            }
        } else {
            // write response
            auto &result = aws_outcome.GetResult();
            statusCode = result.GetStatusCode();
            string version = result.GetExecutedVersion().c_str();
            auto response = AwsParseRequestPayload(result);
            string function_error = result.GetFunctionError().c_str();
            log = result.GetLogResult();
            auto decoded_log = decodeAWSBase64(log);

            // extract info
            auto info = RequestInfo::parseFromLog(log);
            info.fillInFromResponse(response);
            // update with timestamp info
            info.tsRequestStart = tsStart;
            info.tsRequestEnd = tsEnd;
            info.containerId = response.container().uuid();

            // update cost info
            lctx->getService()->addCost(info.billedDurationInMs, info.memorySizeInMb);

            if (response.status() == messages::InvocationResponse_Status_SUCCESS) {

                // Check if special exit codes apply or not
                // -> i.e. crash, timeout.
                LambdaStatusCode status = LambdaStatusCode::OK;
                std::string msg;
                if((status = specialEventHappened(decoded_log, response, &msg)) != LambdaStatusCode::OK) {
                    // was it a timeout failure or a restore failure?
                    if(status == LambdaStatusCode::ERROR_TIMEOUT ) {
                        // For timeout failures, check if retries are left.
                        if(req.retriesLeft > 0) {
                            // call callback
                            lctx->retry(LambdaStatusCode::ERROR_TIMEOUT, "Lambda timed out after " +msg + " s", true);

                            // modify req and invoke again
                            req.retriesLeft--;
                            req.retryErrors.push_back(LambdaStatusCode::ERROR_TIMEOUT);

                            service->invokeAsync(req);
                            service->_numPendingRequests.fetch_add(-1, std::memory_order_release);
                            return;
                        } else {
                            // no retry left? -> done.
                            lctx->fail(LambdaStatusCode::ERROR_RETRIES_EXHAUSTED, "Lambda timed out after " + msg + " s, no more retries left.");
                        }

                    } else {
                        // just re-invoke, it's a dummy invocation - do not change counts etc.
                        lctx->retry(LambdaStatusCode::ERROR_TIMEOUT, "Lambda was reset server-side, invoke again.", false);

                        service->invokeAsync(req);
                        service->_numPendingRequests.fetch_add(-1, std::memory_order_release);
                        return;
                    }
                } else {

                    // did request fail on Lambda?
                    if (info.returnCode != 0) {
                        // stop execution
                        service->_numPendingRequests.fetch_add(-1, std::memory_order_release);

                        // in dev mode, print out details which file caused the failure!
                        std::stringstream err_stream;
                        err_stream << "LAMBDA failure for uri";
                        if (req.body.inputuris_size() > 1)
                            err_stream << "s";
                        for (const auto &uri: req.body.inputuris())
                            err_stream << " " << uri.c_str();
                        auto err_message = err_stream.str();

                        lctx->fail(LambdaStatusCode::ERROR_TASK, err_message);

                        // this here also should go into the backend b.c. it's managing what should happen
                        // // abort the other requests (save the $)
                        // backend->abortRequestsAndFailWith(info.returnCode, info.errorMessage);
                        return;
                    } else {

                        // worked, call callback!
                        AwsLambdaResponse full_response;
                        full_response.info = info;
                        full_response.response = response;
                        lctx->success(full_response);
                    }
                }
            } else {
                // TODO: maybe still track the response info (e.g. reused, cost, etc.)
                ss << "Lambda task failed (" + req.input_desc() + ") [" << statusCode << "], details: "
                   << response.errormessage();
                ss << " RequestId: " << info.requestId;
                if (!function_error.empty())
                    ss << " Function Error: " << function_error;
                // print out log:
                ss << "\nLog:\n" << decodeAWSBase64(log);

                lctx->fail(LambdaStatusCode::ERROR_UNKNOWN, ss.str());
                // decrease wait counter
                service->_numPendingRequests.fetch_add(-1);
                return;
            }
        }

//        // log out message
//        service->logger().info(ss.str());

        // // debug: print out log
        // service->logger().debug(decodeAWSBase64(log));

        // decrease wait counter
        service->_numPendingRequests.fetch_add(-1);
    }

    void AwsLambdaInvocationService::waitForRequests(std::chrono::duration<double, std::milli> sleep_interval) {
        // wait for requests to be finished & check periodically PyErrCheckSignals for Ctrl+C

        logger().info("There are currently " + pluralize(_numPendingRequests.load(), "request") + " active.");

        int64_t pendingTasks = 0;
        while ((pendingTasks = _numPendingRequests.load(std::memory_order_acquire)) > 0) {
            // sleep
            std::stringstream ss;
            ss<<"Currently there are "<<pluralize(pendingTasks, "request")<<", sleeping for "<<sleep_interval.count()<<"ms, checking for signals + pending requests."<<std::endl;
            logger().debug(ss.str());
            std::this_thread::sleep_for(sleep_interval);

            logger().debug("Checking for python signals, interpreter active: " + boolToString(python::isInterpreterRunning()));
            python::lockGIL();
            if (PyErr_CheckSignals() != 0) {
                logger().info("Recevied python signal, setting pending requests to 0.");
                // stop requests & cleanup @TODO: cleanup on S3 with requests...
                if (_client)
                    _client->DisableRequestProcessing();
                _numPendingRequests.store(0); //, std::memory_order_acq_rel);
            }

            python::unlockGIL();
        }
    }
}

#endif