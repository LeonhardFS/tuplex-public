//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 12/2/2021                                                                //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#ifdef BUILD_WITH_AWS

#include <ee/aws/LambdaWorkerApp.h>

#include <StringUtils.h>

// HACK
#include <physical/codegen/StagePlanner.h>
// planning

// AWS specific includes
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/platform/Environment.h>

#include <aws/lambda/model/InvokeRequest.h>
#include <aws/lambda/model/ListFunctionsRequest.h>
#include <aws/lambda/model/UpdateFunctionConfigurationRequest.h>
#include <aws/lambda/model/UpdateFunctionConfigurationResult.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/FormattedLogSystem.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/platform/Environment.h>

#include <AWSCommon.h>
#include <aws/lambda/LambdaClient.h>

#include <cassert>

namespace tuplex {

    // Lambda specific configuration
    const std::string LambdaWorkerApp::caFile = "/etc/pki/tls/certs/ca-bundle.crt";
    const std::string LambdaWorkerApp::tuplexRuntimePath = "lib/tuplex_runtime.so";
    const bool LambdaWorkerApp::verifySSL = true;

    struct SelfInvocationContext {
        std::atomic_int32_t numPendingRequests;
        mutable std::mutex mutex;
        std::vector<ContainerInfo> containers;
        std::string tag;
        std::string functionName;
        size_t timeOutInMs;
        size_t baseDelayInMs;
        std::chrono::high_resolution_clock::time_point tstart; // start point of context
        std::shared_ptr<Aws::Lambda::LambdaClient> client;

        static void lambdaCallback(const Aws::Lambda::LambdaClient* client,
                                        const Aws::Lambda::Model::InvokeRequest& req,
                                        const Aws::Lambda::Model::InvokeOutcome& outcome,
                                        const std::shared_ptr<const Aws::Client::AsyncCallerContext>& ctx);

        SelfInvocationContext() : numPendingRequests(0), tstart(std::chrono::high_resolution_clock::now()) {}

        inline double timeSinceStartInSeconds() {
            auto stop = std::chrono::high_resolution_clock::now();
            double duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - tstart).count() / 1000000000.0;
            return duration;
        }

        /*!
         * checks whether container with uuid is contained already or not
         */
        inline bool contains(const std::string& uuid) const {
            std::unique_lock<std::mutex> lock(mutex);
            auto it = std::find_if(containers.cbegin(), containers.cend(), [&uuid](const ContainerInfo& info) {
                return info.uuid == uuid;
            });
            return it != containers.cend();
        }

        class CallbackContext : public Aws::Client::AsyncCallerContext {
        private:
            SelfInvocationContext* _ctx;
            uint32_t _no;
            messages::WarmupMessage _wm;
        public:
            CallbackContext() = delete;
            CallbackContext(SelfInvocationContext* ctx, uint32_t invocationNo, messages::WarmupMessage wm) : _ctx(ctx), _no(invocationNo), _wm(wm) {}

            SelfInvocationContext* ctx() const { return _ctx; }
            uint32_t no() const { return _no; }
            const messages::WarmupMessage& message() const { return _wm; }
        };
    };

    void SelfInvocationContext::lambdaCallback(const Aws::Lambda::LambdaClient* client,
                                               const Aws::Lambda::Model::InvokeRequest& req,
                                               const Aws::Lambda::Model::InvokeOutcome& outcome,
                                               const std::shared_ptr<const Aws::Client::AsyncCallerContext>& ctx) {
        using namespace std;

        auto callback_ctx = dynamic_cast<const SelfInvocationContext::CallbackContext*>(ctx.get());
        assert(callback_ctx);
        auto self_ctx = callback_ctx->ctx();
        assert(self_ctx);

        double timeout = self_ctx->timeOutInMs / 1000.0;

        MessageHandler& logger = Logger::instance().logger("lambda-warmup");

        int statusCode = 0;

        // lock & add container ID if successful outcome!
        if(!outcome.IsSuccess()) {
            auto &error = outcome.GetError();
            statusCode = static_cast<int>(error.GetResponseCode());

            // rate limit? => reissue request
            if(statusCode == static_cast<int>(Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS) || // i.e. 429
               statusCode == static_cast<int>(Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR)) {
                // should retry...

                logger.info("should retry request... (nyimpl)");

            } else {
                logger.error("Self-Invoke request errored with code " + std::to_string(statusCode) + " details: " + std::string(error.GetMessage().c_str()));
                if(self_ctx->timeSinceStartInSeconds() < timeout) {
                    // timeout reached...
                    logger.error("timeout reached");
                    // failure??
                }
            }
        } else {
            // write response
            auto& result = outcome.GetResult();
            statusCode = result.GetStatusCode();
            std::string version = result.GetExecutedVersion().c_str();

            // parse payload
            stringstream ss;
            auto& stream = const_cast<Aws::Lambda::Model::InvokeResult&>(result).GetPayload();
            ss<<stream.rdbuf();
            string data = ss.str();
            messages::InvocationResponse response;
            google::protobuf::util::JsonStringToMessage(data, &response);

            // logger.info("got answer from self-invocation request");

            if(response.status() == messages::InvocationResponse_Status_SUCCESS) {

                // check if container is already part of containers or not, if reused and part of it -> reinvoke!
                if(response.container().reused() && self_ctx->contains(response.container().uuid())) {

                    // check whether to re-invoke or to leave
                    if(self_ctx->timeSinceStartInSeconds() < timeout) {
                        // logger.info("container reused, invoke again.");
                        // invoke again (do not change count)

                        // Tuplex request
                        messages::InvocationRequest req;
                        req.set_type(messages::MessageType::MT_WARMUP);

                        const auto& original_message = callback_ctx->message();
                        vector<size_t> remaining_counts;
                        for(unsigned i = 1; i < original_message.invocationcount_size(); ++i)
                            remaining_counts.push_back(original_message.invocationcount(i));

                        // specific warmup message contents
                        auto wm = std::make_unique<messages::WarmupMessage>();
                        wm->set_timeoutinms(self_ctx->timeOutInMs); // remaining time?
                        wm->set_basedelayinms(self_ctx->baseDelayInMs);
                        for(auto count : remaining_counts)
                            wm->add_invocationcount(count);
                        req.set_allocated_warmup(wm.release());

                        messages::WarmupMessage message;
                        message.set_timeoutinms(self_ctx->timeOutInMs);
                        message.set_basedelayinms(self_ctx->baseDelayInMs);
                        for(auto count : remaining_counts)
                            message.add_invocationcount(count);

                        // construct invocation request
                        Aws::Lambda::Model::InvokeRequest invoke_req;
                        invoke_req.SetFunctionName(self_ctx->functionName.c_str());
                        // note: may redesign lambda backend to work async, however then response only yields status code
                        // i.e., everything regarding state needs to be managed explicitly...
                        invoke_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
                        // logtype to extract log data??
                        //req.SetLogtype(Aws::Lambda::Model::LogType::None);
                        std::string json_buf;
                        google::protobuf::util::MessageToJsonString(req, &json_buf);
                        invoke_req.SetBody(stringToAWSStream(json_buf));
                        invoke_req.SetContentType("application/javascript");

                        self_ctx->client->InvokeAsync(invoke_req,
                                                      SelfInvocationContext::lambdaCallback,
                                                      Aws::MakeShared<SelfInvocationContext::CallbackContext>(self_ctx->tag.c_str(),
                                                                                                              self_ctx,
                                                                                                              callback_ctx->no(),
                                                                                                              message));
                    } else {
                        // atomic decref
                        self_ctx->numPendingRequests.fetch_add(-1, std::memory_order_release);
                        logger.info("warmup request timed out.");
                    }
                } else {
                    if(!response.container().reused())
                        logger.info("New container " + std::string(response.container().uuid().c_str()) + " started.");
                    else {
                        logger.info("Found already running container " + std::string(response.container().uuid().c_str()) + ".");
                    }
                    std::unique_lock<std::mutex> lock(self_ctx->mutex);
                    // add the container info of the invoker itself!
                    const_cast<SelfInvocationContext*>(self_ctx)->containers.emplace_back(response.container());
//                    // and all IDs that that container invoked
//                    for(auto info : response.invokedcontainers()) {
//                        self_ctx->containers.emplace_back(info);
//                    }
                    self_ctx->numPendingRequests.fetch_add(-1, std::memory_order_release);
                }
            } else {
                // failed...
                logger.error("invoke failed, wrong code returned.");
            }
        }
    }

    // helper function to self-invoke quickly (creates new client!)
    std::vector<ContainerInfo> selfInvoke(const std::string& functionName,
                                        size_t count,
                                        const std::vector<size_t>& recursive_counts,
                                        size_t timeOutInMs,
                                        size_t baseDelayInMs,
                                        const AWSCredentials& credentials,
                                        const NetworkSettings& ns,
                                        std::string tag) {

        MessageHandler& logger = Logger::instance().logger("lambda-warmup");

        if(0 == count)
            return {};

        std::vector<std::string> containerIds;
        Timer timer;

        // init Lambda client
        Aws::Client::ClientConfiguration clientConfig;

        size_t lambdaToLambdaTimeOutInMs = 200;

        clientConfig.requestTimeoutMs = lambdaToLambdaTimeOutInMs; // conv seconds to ms
        clientConfig.connectTimeoutMs = lambdaToLambdaTimeOutInMs; // connection timeout

        // tune client, according to https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/client-config.html
        // note: max connections should not exceed max concurrency if it is below 100, else aws lambda
        // will return toomanyrequestsexception
        clientConfig.maxConnections = count;

        // to avoid thread exhaust of system, use pool thread executor with 8 threads
        clientConfig.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(tag.c_str(), count);
        clientConfig.region = credentials.default_region.c_str();

        //clientConfig.userAgent = "tuplex"; // should be perhaps set as well.
        applyNetworkSettings(ns, clientConfig);

        // change aws settings here
        Aws::Auth::AWSCredentials cred(credentials.access_key.c_str(),
                                       credentials.secret_key.c_str(),
                                       credentials.session_token.c_str());

        SelfInvocationContext ctx;
        ctx.client = Aws::MakeShared<Aws::Lambda::LambdaClient>(tag.c_str(), cred, clientConfig);
        ctx.tag = tag;
        ctx.timeOutInMs = timeOutInMs;
        ctx.baseDelayInMs = baseDelayInMs;
        ctx.functionName = functionName;

        double timeout = (double)timeOutInMs / 1000.0;

        // async callback & invocation
        for(unsigned i = 0; i < count; ++i) {

            // Tuplex request
            messages::InvocationRequest req;
            req.set_type(messages::MessageType::MT_WARMUP);

            // specific warmup message contents
            auto wm = std::make_unique<messages::WarmupMessage>();
            auto invoked_timeout = baseDelayInMs > timeOutInMs ? baseDelayInMs : timeOutInMs - baseDelayInMs;
            wm->set_timeoutinms(invoked_timeout);
            wm->set_basedelayinms(baseDelayInMs);
            for(auto count : recursive_counts)
                wm->add_invocationcount(count);
            req.set_allocated_warmup(wm.release());

            messages::WarmupMessage message;
            message.set_timeoutinms(invoked_timeout);
            message.set_basedelayinms(baseDelayInMs);
            for(auto count : recursive_counts)
                message.add_invocationcount(count);

            // construct invocation request
            Aws::Lambda::Model::InvokeRequest invoke_req;
            invoke_req.SetFunctionName(functionName.c_str());
            // note: may redesign lambda backend to work async, however then response only yields status code
            // i.e., everything regarding state needs to be managed explicitly...
            invoke_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
            // logtype to extract log data??
            //req.SetLogtype(Aws::Lambda::Model::LogType::None);
            std::string json_buf;
            google::protobuf::util::MessageToJsonString(req, &json_buf);
            invoke_req.SetBody(stringToAWSStream(json_buf));
            invoke_req.SetContentType("application/javascript");

            if(ctx.timeSinceStartInSeconds() < timeout) {
                // invoke if time is larger
                ctx.numPendingRequests.fetch_add(1, std::memory_order_release);
                ctx.client->InvokeAsync(invoke_req,
                                        SelfInvocationContext::lambdaCallback,
                                        Aws::MakeShared<SelfInvocationContext::CallbackContext>(tag.c_str(), &ctx, i, message));
            }
        }

        // wait till pending is 0 or timeout (done in individual tasks)
        while(ctx.numPendingRequests > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }

        logger.info("warmup done, result are " + pluralize(ctx.containers.size(), "container"));

        // how long did it take?
        return ctx.containers;
    }

    int LambdaWorkerApp::globalInit(bool skip) {

        // skip if already initialized
        if(_globallyInitialized)
            return WORKER_OK;

        if(skip) {
            _globallyInitialized=true;
            return WORKER_OK;
        }

        // Lambda specific initialization
        Timer timer;
        Aws::InitAPI(_aws_options);

        // if desired (i.e. environment variable TUPLEX_ENABLE_FULL_AWS_LOGGING is set), turn here logging on
        if(checkIfOptionIsSetInEnv("TUPLEX_ENABLE_FULL_AWS_LOGGING")) {
            auto log_level = Aws::Utils::Logging::LogLevel::Trace;
            // log_level = Aws::Utils::Logging::LogLevel::Info;
            auto log_system = Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("tuplex", log_level);
            Aws::Utils::Logging::InitializeAWSLogging(log_system);
        }

        // get AWS credentials from Lambda environment...
        // Note that to run on Lambda this requires a session token!
        // e.g., https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime
        std::string access_key = Aws::Environment::GetEnv("AWS_ACCESS_KEY_ID").c_str();
        std::string secret_key = Aws::Environment::GetEnv("AWS_SECRET_ACCESS_KEY").c_str();
        std::string session_token = Aws::Environment::GetEnv("AWS_SESSION_TOKEN").c_str();

        // get region from AWS_REGION env
        auto region = Aws::Environment::GetEnv("AWS_REGION");
        auto functionName = Aws::Environment::GetEnv("AWS_LAMBDA_FUNCTION_NAME");

        // Print AWS variables cf. https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html
        {

            auto size_in_mb = Aws::Environment::GetEnv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE");
            auto version = Aws::Environment::GetEnv("AWS_LAMBDA_FUNCTION_VERSION");
            auto init_type = Aws::Environment::GetEnv("AWS_LAMBDA_INITIALIZATION_TYPE");
            std::stringstream ss;
            ss<<"START Lambda (name: "<<functionName<<", region: "<<region<<", version: "<<version<<", "<<size_in_mb<<"MB, type: "<<init_type<<")";
            logger().info(ss.str());
        }

        _functionName = functionName.c_str();

        _credentials.access_key = access_key;
        _credentials.secret_key = secret_key;
        _credentials.session_token = session_token;
        _credentials.default_region = region;

        update_network_settings();

        VirtualFileSystem::addS3FileSystem(access_key, secret_key, session_token, region.c_str(), _networkSettings,
                                           true, true);

        runtime::init(tuplexRuntimePath);
        _compiler = std::make_shared<JITCompiler>();

        // add exception hierarchy to typesystem
        // in order to decode properly, need to add exceptions to type system.
        // do this by initializing a dummy symbol table
        auto sym_table = SymbolTable::createFromEnvironment(nullptr);
        if(sym_table) {
           logger().info("Initialized type system with builtin types.");
        }

        // init python & set explicitly python home for Lambda
        std::string task_root = std::getenv("LAMBDA_TASK_ROOT");
        python::python_home_setup(task_root);
        logger().debug("Set PYTHONHOME=" + task_root);
        // init interpreter AND release GIL!!!
        python::initInterpreter();
        python::unlockGIL();
        metrics.global_init_time = timer.time();

        // fetch container info and set timeout based on that incl. security buffer!
        auto this_container = getThisContainerInfo();
        if(this_container.msRemaining < AWS_LAMBDA_SAFETY_DURATION_IN_MS) {
            logger().error("timeout for LAMBDA set to low. Please set higher.");
            return WORKER_ERROR_GLOBAL_INIT;
        }
        std::chrono::milliseconds timeout(this_container.msRemaining - AWS_LAMBDA_SAFETY_DURATION_IN_MS);
        setLambdaTimeout(timeout);
        _globallyInitialized = true;

#ifdef BUILD_WITH_CEREAL
        logger().info("Lambda is using Cereal AST serialization.");
#else
        logger().info("Lambda is using JSON AST serialization.");
#endif

        return WORKER_OK;
    }

    void LambdaWorkerApp::update_network_settings(const std::unordered_map<std::string, std::string> &env) {
        _networkSettings = NetworkSettings();
        _networkSettings.verifySSL = verifySSL;
        _networkSettings.caFile = caFile;

        if(env.find(AWS_LAMBDA_ENDPOINT_KEY) != env.end()) {

            // Overwrite lambda endpoint.
            auto endpoint = env.at(AWS_LAMBDA_ENDPOINT_KEY);

            // Amazon endpoints end with .amazonaws.com.
            // For a list of available endpoints, cf. https://docs.aws.amazon.com/general/latest/gr/lambda.html
            // For non-Aws endpoints (i.e., local rest) disable SSL.
            if(endpoint.find(".amazonaws.com") == std::string::npos) {
                NetworkSettings ns;
                ns.verifySSL = false;
                ns.useVirtualAddressing = false;
                ns.signPayloads = false;
                _networkSettings = ns;
            }
            _networkSettings.endpointOverride = endpoint;
            logger().info("Updated Lambda endpoint to " + endpoint + ".");
        }
    }

    int LambdaWorkerApp::processMessage(const tuplex::messages::InvocationRequest& req) {
        using namespace std;

        // reset results
        resetResult();
        _response.set_originalrequestid(req.id());

        _messageType = req.type();

        // check message type
        if(req.type() == messages::MessageType::MT_WARMUP) {
            logger().info("Received warmup message");
            size_t selfInvokeCount = 0;
            vector<size_t> recursive_counts;
            size_t timeOutInMs = 100;
            size_t baseDelayInMs = 75;
            if(req.has_warmup()) {
                for(unsigned i = 0; i < req.warmup().invocationcount_size(); ++i) {
                    if(0 == i)
                        selfInvokeCount = req.warmup().invocationcount(i);
                    else
                        recursive_counts.push_back(req.warmup().invocationcount(i));
                }

                timeOutInMs = req.warmup().timeoutinms();
                baseDelayInMs = req.warmup().basedelayinms();
            }

            // use self invocation
            if(selfInvokeCount > 0) {
                logger().info("invoking " + pluralize(selfInvokeCount, "other lambda") + " (timeout: " + std::to_string(timeOutInMs) + "ms)");
                Timer timer;
                auto ret = selfInvoke(_functionName,
                                                selfInvokeCount,
                                                recursive_counts,
                                                timeOutInMs,
                                                baseDelayInMs,
                                                _credentials,
                                                _networkSettings);

                _invokedContainers = normalizeInvokedContainers(ret);

                // wait till delay for this func is reached
                double delayForThis = static_cast<double>(recursive_counts.size() * baseDelayInMs) / 1000.0;
                while(timer.time() < delayForThis)
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));

                logger().info("warmup done.");
            }

            return WORKER_OK;
        } else if(req.type() == messages::MessageType::MT_TRANSFORM) {
            // extract settings from req & init multi-threading!
            _settings = settingsFromMessage(req);

            bool purePythonMode = req.has_settings() && req.settings().has_useinterpreteronly() && req.settings().useinterpreteronly();
            auto numThreads = purePythonMode ? 1 : _settings.numThreads;

            if(purePythonMode)
                logger().info("Processing in pure python/fallback only mode");

            if(!_threadEnvs)
                initThreadEnvironments(numThreads);

            // validate only S3 uris are given (in debug mode)
#ifdef NDEBUG
            bool invalid_uri_found = false;
        for(const auto& str_path : req.inputuris()) {
	    URI path(str_path);
            // check paths are S3 paths
            if(path.prefix() != "s3://") {
                logger().error("InvalidPath: input path must be s3:// path, is " + path.toPath());
                invalid_uri_found = true;
            }
        }
        if(invalid_uri_found)
            return WORKER_ERROR_INVALID_URI;
#endif

            logger().info("Invoking WorkerApp fallback");
            // can reuse here infrastructure from WorkerApp!
            auto rc = WorkerApp::processMessage(req);
            if(rc == WORKER_OK) {
                // add to output
                // ok, add output_uri and parts to request success output
                for(const auto& in_uri : req.inputuris())
                    _input_uris.push_back(in_uri);
                // output uris from worker app
                for(const auto& uri : output_uris())
                    _output_uris.push_back(uri.toString());

                // @TODO: return small results as part of the request.

                // _response should already hold information about counts etc.
                // --> i.e. fill_response_with_state fills response with data about this invocation.
                // --> fill_response_with_self_invocation_state with data about self-invocation state.
                fill_with_result(_response);
            }

            return rc;
        } else {
            return WORKER_ERROR_UNKNOWN_MESSAGE;
        }

        // TODO notes for Lambda:
        // 1. scale-out should work (via self-invocation!)
        // 2. Joins (i.e. allow flight query to work)
        // 3. self-specialization (for flights should work) --> requires range optimization + detection on files.
        // ==> need other optimizations as well -.-

        return WORKER_OK;
    }

    void LambdaWorkerApp::fill_with_result(messages::InvocationResponse& response) {

        // Reset fields (if modified before)
        response.clear_inputuris();
        response.clear_outputuris();
        response.clear_invokedrequests();

        for(const auto& r_info : _requests) {
            auto element = response.add_invokedrequests();
            r_info.fill(element);
        }

        // add which outputs from which inputs this query produced
        for(const auto& uri : _input_uris)
            response.add_inputuris(uri);
        for(const auto& uri : _output_uris)
            response.add_outputuris(uri);
    }

//    void LambdaWorkerApp::prepareResponseFromSelfInvocations() {
//
//        std::vector<ContainerInfo> successful_containers;
//        std::vector<RequestInfo> requests; // which requests to send along message
//        std::vector<std::string> output_uris;
//        std::vector<std::string> input_uris;
//
//        // go over all invocations
//        {
//            std::unique_lock<std::mutex> lock(_invokeRequestMutex);
//            unsigned n = _invokeRequests.size();
//
//            for(unsigned i = 0; i < n; ++i) {
//                auto& req = _invokeRequests[i];
//                if(req.response.success()) {
//                    successful_containers.push_back(req.response.container);
//
//                    // all other invoked containers
//                    std::copy(req.response.invoked_containers.begin(), req.response.invoked_containers.end(), std::back_inserter(successful_containers));
//
//                    std::copy(req.response.output_uris.begin(), req.response.output_uris.end(), std::back_inserter(output_uris));
//                    std::copy(req.response.input_uris.begin(), req.response.input_uris.end(), std::back_inserter(input_uris));
//                }
//            }
//
//            // copy ALL requests to output message for proper debugging/cost metrics.
//            for(unsigned i = 0; i < n; ++i) {
//                auto& req = _invokeRequests[i];
//                requests.push_back(req.response.invoke_desc);
//                std::copy(req.response.invoked_requests.begin(), req.response.invoked_requests.end(), std::back_inserter(requests));
//            }
//        }
//
//        std::sort(output_uris.begin(), output_uris.end());
//        std::sort(input_uris.begin(), input_uris.end());
//
//        // TODO: merge input uris?
//
//        // fetch invoked containers etc.
//        _invokedContainers = normalizeInvokedContainers(successful_containers);
//        _requests = requests;
//        _output_uris = output_uris;
//        _input_uris = input_uris;
//    }

//    void LambdaWorkerApp::invokeLambda(double timeout, const std::vector<FilePart>& parts,
//                                  const URI& base_output_uri,
//                                  uint32_t partNoOffset,
//                                  const tuplex::messages::InvocationRequest& original_message,
//                                  size_t max_retries,
//                                  const std::vector<size_t>& invocation_counts) {
//
//        std::string tag = "tuplex-lambda";
//
//        // skip if empty
//        if(parts.empty())
//            return;
//
//        // create protobuf message
//        tuplex::messages::InvocationRequest req = original_message;
//        req.mutable_inputsizes()->Clear();
//        req.mutable_inputuris()->Clear();
//
//        for(const auto& part : parts) {
//            req.add_inputuris(encodeRangeURI(part.uri, part.rangeStart, part.rangeEnd));
//            assert(part.size != 0);
//            req.add_inputsizes(part.size);
//        }
//
//        req.set_baseoutputuri(base_output_uri.toString());
//        req.set_partnooffset(partNoOffset);
//
//        auto transform_message = req.mutable_stage();
//        transform_message->mutable_invocationcount()->Clear();
//        for(auto count : invocation_counts)
//            transform_message->add_invocationcount(count);
//
//        // changed protobuf message
//        // init client
//        if(!_lambdaClient) {
//            logger().error("internal error, need to initialize client first before invoking lambdas");
//            return;
//        }
//
//        std::string json_buf;
//        google::protobuf::util::MessageToJsonString(req, &json_buf);
//
//        // now create request (thread-safe)
//        SelfInvokeRequest invoke_req;
//        invoke_req.max_retries = max_retries;
//        invoke_req.retries = 0;
//        invoke_req.payload = json_buf;
//        auto requestNo = addRequest(invoke_req);
//
//        // invoke lambda
//        // construct invocation request
//        Aws::Lambda::Model::InvokeRequest lambda_req;
//        lambda_req.SetFunctionName(_functionName.c_str());
//        // note: may redesign lambda backend to work async, however then response only yields status code
//        // i.e., everything regarding state needs to be managed explicitly...
//        lambda_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
//        // logtype to extract log data??
//        //req.SetLogtype(Aws::Lambda::Model::LogType::None);
//        lambda_req.SetLogType(Aws::Lambda::Model::LogType::Tail);
//        lambda_req.SetBody(stringToAWSStream(invoke_req.payload));
//        lambda_req.SetContentType("application/javascript");
//
//        // invoke if time left permits
//        if(timeLeftOnLambda()) {
//            // update req start timestamp
//            auto& req = _invokeRequests[requestNo];
//            req.tsStart = current_utc_timestamp();
//            _lambdaClient->InvokeAsync(lambda_req,
//                                       lambdaCallback,
//                                       Aws::MakeShared<LambdaRequestContext>(tag.c_str(), this, requestNo));
//        }
//    }

    bool LambdaWorkerApp::aboutToTimeout() {
        std::lock_guard<std::mutex> lock(_timeoutMutex);
        if(!_timeoutFunctor)
            return false;

        auto time_left_in_s = _timeoutFunctor();
        logger().info("Only " + std::to_string(time_left_in_s) + "s left.");
        return time_left_in_s < AWS_LAMBDA_TIMEOUT_IN_S;
    }

    void LambdaWorkerApp::lambdaOnSuccess(SelfInvokeRequest &request, const messages::InvocationResponse &response,
                                          const RequestInfo& desc) {
        // Lambda succeeded, now deal with response.
        std::stringstream ss;

        // check what the return code is...
        if(!desc.errorMessage.empty()) {
            ss<<"LAMBDA ["<<(int)response.status()<<"] Error: "<<desc.returnCode<<" "<<desc.errorMessage;
        } else {
            ss<<"LAMBDA ["<<(int)response.status()<<"] succeeded, took "<<desc.awsTimings.durationInMs<<"ms, billed: "<<desc.awsTimings.billedDurationInMs<<"ms";
        }

        logger().info(ss.str());

        // save result, i.e. containerInfo of invoked container etc.
        request.response.returnCode = (int)response.status();
        request.response.container = response.container();
        request.response.invoke_desc = desc;
//        for(auto c : response.invokedcontainers())
//            request.response.invoked_containers.push_back(c);
        for(auto r : response.invokedrequests())
            request.response.invoked_requests.push_back(r);
        for(auto out_uri : response.outputuris())
            request.response.output_uris.push_back(out_uri);
        for(auto in_uri : response.inputuris())
            request.response.input_uris.push_back(in_uri);
    }

    void LambdaWorkerApp::lambdaCallback(const Aws::Lambda::LambdaClient *client,
                                         const Aws::Lambda::Model::InvokeRequest &req,
                                         const Aws::Lambda::Model::InvokeOutcome &outcome,
                                         const std::shared_ptr<const Aws::Client::AsyncCallerContext> &ctx) {
        // cast & invoke app
        using namespace std;

        // get timestamp (request length)
        auto tsEnd = current_utc_timestamp();

        auto callback_ctx = dynamic_cast<const LambdaRequestContext*>(ctx.get());
        assert(callback_ctx);

        assert(callback_ctx->app);
        MessageHandler& logger = Logger::instance().logger("lambda-warmup");

        int statusCode = 0;
        auto& self_req = callback_ctx->app->_invokeRequests[callback_ctx->requestIdx];

        // lock & add container ID if successful outcome!
        if(!outcome.IsSuccess()) {
            auto &error = outcome.GetError();
            statusCode = static_cast<int>(error.GetResponseCode());

            std::string error_message = error.GetMessage();
            std::string error_name = error.GetExceptionName();

            // invoke failure callback
            callback_ctx->app->lambdaOnFailure(self_req, statusCode, error_name, error_message);
        } else {
            // write response
            auto &result = outcome.GetResult();
            statusCode = result.GetStatusCode();
            std::string version = result.GetExecutedVersion().c_str();

            // parse payload
            stringstream ss;
            auto &stream = const_cast<Aws::Lambda::Model::InvokeResult &>(result).GetPayload();
            ss << stream.rdbuf();
            string data = ss.str();
            messages::InvocationResponse response;
            google::protobuf::util::JsonStringToMessage(data, &response);

            //callback_ctx->app->logger().info("extracting log...");
            auto log = result.GetLogResult();
            //callback_ctx->app->logger().info("Got log, size: " + std::to_string(log.size()));
            auto desc = RequestInfo::parseFromLog(log);

            // update timestamp info in desc
            desc.tsRequestStart = self_req.tsStart;
            desc.tsRequestEnd = tsEnd;
            desc.containerId = response.container().uuid();

            // invoke from app callback function
            // fetch right request
            callback_ctx->app->lambdaOnSuccess(self_req, response, desc);
        }

        // dec counter
        callback_ctx->app->decRequests();
    }

    bool is_failed_s3_access_request(int status_code, const std::string& error_message) {
        // find two needles, if both are contained in the message it's an s3 failure.

        // status code should be 1 (or error_unknown)
        if(status_code != static_cast<int>(LambdaStatusCode::ERROR_UNKNOWN))
            return false;

        auto needleI = "The AWS Access Key Id you provided does not exist in our records.";
        auto needleI_found = error_message.find(needleI) != std::string::npos;
        auto needleII = "S3 Filesystem error";
        auto needleII_found = error_message.find(needleII) != std::string::npos;

        return needleI_found & needleII_found;


        // a failure message looks like this:
        // ------------------------------------
        // [1]: Lambda task failed (s3://tuplex-public/data/github_daily/2020-10-15.json:12354199453-12445448390) [200], details: Got signal 6, traceback:
        //Stack trace (most recent call last):
        //#24   Object "", at 0xffffffffffffffff, in
        //#23   Object "", at 0xe92006, in
        //#22   Object "/usr/lib64/libc-2.26.so", at 0x7fbfcc60d139, in __libc_start_main
        //#21   Object "", at 0xcf7586, in
        //#20   Object "", at 0xfda47d, in
        //#19   Object "", at 0xe9b9d0, in
        //#18   Object "", at 0xe9b6ee, in
        //#17   Object "", at 0xe9b238, in
        //#16   Object "", at 0xf8ba9f, in
        //#15   Object "", at 0x141e95c, in
        //#14   Object "", at 0xfa1913, in
        //#13   Object "", at 0x14216dd, in
        //#12   Object "", at 0x141aa36, in
        //#11   Object "", at 0xfaa107, in
        //#10   Object "", at 0x1227cf8, in
        //#9    Object "", at 0x1227537, in
        //#8    Object "", at 0xa28606, in
        //#7    Object "/usr/lib64/libgcc_s-7-20180712.so.1", at 0x7fbfcc9a8bbd, in _Unwind_Resume
        //#6    Object "/usr/lib64/libgcc_s-7-20180712.so.1", at 0x7fbfcc9a839c, in
        //#5    Object "/usr/lib64/libstdc++.so.6.0.24", at 0x7fbfccf7e7a3, in __gxx_personality_v0
        //#4    Object "/usr/lib64/libstdc++.so.6.0.24", at 0x7fbfccf7de7e, in
        //#3    Object "/usr/lib64/libstdc++.so.6.0.24", at 0x7fbfccf7ee85, in
        //#2    Object "/usr/lib64/libstdc++.so.6.0.24", at 0x7fbfccf810ee, in __gnu_cxx::__verbose_terminate_handler()
        //#1    Object "/usr/lib64/libc-2.26.so", at 0x7fbfcc621147, in abort
        //#0    Object "/usr/lib64/libc-2.26.so", at 0x7fbfcc61fca0, in raise
        // RequestId: 22ae7c3d-d2ef-43d4-9535-3507088b912e
        //Log:
        //_folder/lam090/original_request.json':
        //buf pos: 1495937	buf size: 5242980	buf length: 1495937	file pos: 0	part no: 0
        //details: The AWS Access Key Id you provided does not exist in our records. - this may be the result of accessing a public bucket with requester pay mode. Set tuplex.aws.requesterPay to true when initializing the context. Also make sure the object in the public repo has a proper ACL set. I.e., to make it publicly available use `aws s3api put-object-acl --bucket <bucket> --key <path> --acl public-read --request-payer requester`{
        //"caFile": "/etc/pki/tls/certs/ca-bundle.crt",
        //"caPath": "",
        //"connectTimeoutMs": 10000,
        //"enableTcpKeepAlive": true,
        //"endpointOverride": "",
        //"httpRequestTimeoutMs": 0,
        //"lowSpeedLimit": 1,
        //"maxConnections": 25,
        //"proxyHost": "",
        //"proxyPassword": "",
        //"proxyPort": 0,
        //"proxySSLCertPath": "",
        //"proxySSLCertType": "",
        //"proxySSLKeyPassword": "",
        //"proxySSLKeyPath": "",
        //"proxySSLKeyType": "",
        //"proxyScheme": "http",
        //"proxyUserName": "",
        //"requestTimeoutMs": 60000,
        //"retryStrategy": "<unknown>",
        //"scheme": "https",
        //"tcpKeepAliveIntervalMs": 30000,
        //"useDualStack": false,
        //"userAgent": "",
        //"verifySSL": true
        //}
        //requestPayer: true isAmazon: true
        //terminate called after throwing an instance of 'tuplex::s3exception'
        //what():  /code/tuplex/io/src/S3File.cc:119 S3 Filesystem error for uri='s3://tuplex-leonhard/scratch/github-exp/spill_folder/lam090/original_request.json':
        //buf pos: 1495937	buf size: 5242980	buf length: 1495937	file pos: 0	part no: 0
        //details: The AWS Access Key Id you provided does not exist in our records. - this may be the result of accessing a public bucket with requester pay mode. Set tuplex.aws.requesterPay to true when initializing the context. Also make sure the object in the public repo has a proper ACL set. I.e., to make it publicly available use `aws s3api put-object-acl --bucket <bucket> --key <path> --acl public-read --request-payer requester`{
        //"caFile": "/etc/pki/tls/certs/ca-bundle.crt",
        //"caPath": "",
        //"connectTimeoutMs": 10000,
        //"enableTcpKeepAlive": true,
        //"endpointOverride": "",
        //"httpRequestTimeoutMs": 0,
        //"lowSpeedLimit": 1,
        //"maxConnections": 25,
        //"proxyHost": "",
        //"proxyPassword": "",
        //"proxyPort": 0,
        //"proxySSLCertPath": "",
        //"proxySSLCertType": "",
        //"proxySSLKeyPassword": "",
        //"proxySSLKeyPath": "",
        //"proxySSLKeyType": "",
        //"proxyScheme": "http",
        //"proxyUserName": "",
        //"requestTimeoutMs": 60000,
        //"retryStrategy": "<unknown>",
        //"scheme": "https",
        //"tcpKeepAliveIntervalMs": 30000,
        //"useDualStack": false,
        //"userAgent": "",
        //"verifySSL": true
        //}
        //requestPayer: true isAmazon: true
        //Stack trace (most recent call last):
        //#24   Object "", at 0xffffffffffffffff, in
        //#23   Object "", at 0xe92006, in
        //#22   Object "/usr/lib64/libc-2.26.so", at 0x7fbfcc60d139, in __libc_start_main
        //#21   Object "", at 0xcf7586, in
        //#20   Object "", at 0xfda47d, in
        //#19   Object "", at 0xe9b9d0, in
        //#18   Object "", at 0xe9b6ee, in
        //#17   Object "", at 0xe9b238, in
        //#16   Object "", at 0xf8ba9f, in
        //#15   Object "", at 0x141e95c, in
        //#14   Object "", at 0xfa1913, in
        //#13   Object "", at 0x14216dd, in
        //#12   Object "", at 0x141aa36, in
        //#11   Object "", at 0xfaa107, in
        //#10   Object "", at 0x1227cf8, in
        //#9    Object "", at 0x1227537, in
        //#8    Object "", at 0xa28606, in
        //#7    Object "/usr/lib64/libgcc_s-7-20180712.so.1", at 0x7fbfcc9a8bbd, in _Unwind_Resume
        //#6    Object "/usr/lib64/libgcc_s-7-20180712.so.1", at 0x7fbfcc9a839c, in
        //#5    Object "/usr/lib64/libstdc++.so.6.0.24", at 0x7fbfccf7e7a3, in __gxx_personality_v0
        //#4    Object "/usr/lib64/libstdc++.so.6.0.24", at 0x7fbfccf7de7e, in
        //#3    Object "/usr/lib64/libstdc++.so.6.0.24", at 0x7fbfccf7ee85, in
        //#2    Object "/usr/lib64/libstdc++.so.6.0.24", at 0x7fbfccf810ee, in __gnu_cxx::__verbose_terminate_handler()
        //#1    Object "/usr/lib64/libc-2.26.so", at 0x7fbfcc621147, in abort
        //#0    Object "/usr/lib64/libc-2.26.so", at 0x7fbfcc61fca0, in raise
        //Aborted (Signal sent by tkill() 8 993)
        //END RequestId: 22ae7c3d-d2ef-43d4-9535-3507088b912e
        //REPORT RequestId: 22ae7c3d-d2ef-43d4-9535-3507088b912e	Duration: 1273.24 ms	Billed Duration: 1274 ms	Memory Size: 1536 MB	Max Memory Used: 207 MB
    }

    void LambdaWorkerApp::lambdaOnFailure(SelfInvokeRequest &request, int statusCode, const std::string &errorName,
                                          const std::string &errorMessage) {

        static const std::string tag = "TUPLEX_LAMBDA";

        // On AWS Lambda, S3 happen because of session token refresh (I assume, not 100% sure).
        // Retry them.
        if(is_failed_s3_access_request(statusCode, errorMessage)) {

            // Log should be contained, extract time.
            auto lines = splitToLines(errorMessage);
            lines.back();

            // emit shorter message (simply stating the S3 access error).
            std::stringstream ss;
            ss<<"LAMBDA request failed with S3 filesystem access key error, retrying.";
            logger().info(ss.str());
        } else {
            // emit full log.
            // Log failure:
            std::stringstream ss;
            ss<<"LAMBDA request failed with code ["<<statusCode<<"]: "<<errorName<<": "<<errorMessage;
            logger().info(ss.str());
        }

        // rate limit? => reissue request
        if(statusCode == static_cast<int>(Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS) || // i.e. 429
           statusCode == static_cast<int>(Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR) ||
           statusCode == static_cast<int>(Aws::Http::HttpResponseCode::SERVICE_UNAVAILABLE) || // 503
           is_failed_s3_access_request(statusCode, errorMessage) // S3 credentials not ok, expired etc.
           ) {

            // (silent retry)
            if(request.retries < request.max_retries) {
                // retry
                _outstandingRequests++;
                request.retries++;

                // exponential sleep
                // AWS recommends the following:
                // Do some asynchronous operation.
                //
                //retries = 0
                //
                //DO
                //    wait for (2^retries * 100) milliseconds
                //
                //    status = Get the result of the asynchronous operation.
                //
                //    IF status = SUCCESS
                //        retry = false
                //    ELSE IF status = NOT_READY
                //        retry = true
                //    ELSE IF status = THROTTLED
                //        retry = true
                //    ELSE
                //        Some other error occurred, so stop calling the API.
                //        retry = false
                //    END IF
                //
                //    retries = retries + 1
                //
                //WHILE (retry AND (retries < MAX_RETRIES))
                // source: https://docs.aws.amazon.com/general/latest/gr/api-retries.html
                std::this_thread::sleep_for(std::chrono::milliseconds((0x1 << request.retries) * 100));

                // construct invocation request
                Aws::Lambda::Model::InvokeRequest lambda_req;
                lambda_req.SetFunctionName(_functionName.c_str());
                // note: may redesign lambda backend to work async, however then response only yields status code
                // i.e., everything regarding state needs to be managed explicitly...
                lambda_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
                // logtype to extract log data??
                //req.SetLogtype(Aws::Lambda::Model::LogType::None);
                lambda_req.SetLogType(Aws::Lambda::Model::LogType::Tail);
                lambda_req.SetBody(stringToAWSStream(request.payload));
                lambda_req.SetContentType("application/javascript");

                if(timeLeftOnLambda()) {
                    // update timestamp
                    request.tsStart = current_utc_timestamp();
                    _lambdaClient->InvokeAsync(lambda_req,
                                               lambdaCallback,
                                               Aws::MakeShared<LambdaRequestContext>(tag.c_str(), this, request.requestIdx));
                }
            } else {

                if(is_failed_s3_access_request(statusCode, errorMessage)) {
                    std::stringstream ss;
                    ss<<"Lambda request with S3 problems failed, exceeded "<<request.retries<<" retries.";
                    request.response.returnCode = statusCode; // indicate failure.
                    logger().warn(ss.str());
                } else {
                    std::stringstream ss;
                    ss<<"Self-invoke request "<<request.requestIdx<<" failed with HTTP="<<statusCode;
                    ss<<"exceeded "<<request.retries<<" retries.";
                    request.response.returnCode = statusCode; // indicate failure.
                    logger().warn(ss.str());
                }
            }
        } else {
            std::stringstream ss;
            ss<<"Self-invoke request "<<request.requestIdx<<" failed with HTTP="<<statusCode;
            if(!errorName.empty())
                ss<<" "<<errorName;
            if(!errorMessage.empty())
                ss<<" "<<errorMessage;
            logger().error(ss.str());
        }
    }

    std::shared_ptr<Aws::Lambda::LambdaClient> LambdaWorkerApp::createLambdaClient(Aws::Client::ClientConfiguration config,
                                                                                   double timeout,
                                                                                   size_t max_connections) const {
        logger().info("Creating Lambda client (for self-invocation).");

        size_t lambdaToLambdaTimeOutInMs = 800; // 200 should be sufficient, yet sometimes lambdas break with broken pipe
        std::string tag = "tuplex-lambda";

        config.requestTimeoutMs = static_cast<int>(timeout * 1000.0); // conv seconds to ms
        config.connectTimeoutMs = lambdaToLambdaTimeOutInMs; // connection timeout
        config.tcpKeepAliveIntervalMs = 15; // lower this

        // tune client, according to https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/client-config.html
        // note: max connections should not exceed max concurrency if it is below 100, else aws lambda
        // will return toomanyrequestsexception
        config.maxConnections = max_connections;

        logger().info(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " Creating thread executor Pool for Lambda client.");

        // to avoid thread exhaust of system, use pool thread executor with 8 threads
        config.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(tag.c_str(), max_connections);
        config.region = _credentials.default_region.c_str();

        // Can't have empty region, set to us-east-1 else.
        if(config.region.empty()) {
            logger().warn("Given region is " + escape_to_python_str(config.region.c_str()) + ", setting to us-east-1.");
            config.region = "us-east-1";
        }

        //clientConfig.userAgent = "tuplex"; // should be perhaps set as well.
        applyNetworkSettings(_networkSettings, config);

        // change aws settings here
        Aws::Auth::AWSCredentials cred(_credentials.access_key.c_str(),
                                       _credentials.secret_key.c_str(),
                                       _credentials.session_token.c_str());

        logger().info("Lambda config done, now creating object.");

        // std::stringstream ss;
        // ss<<"AWS client config:\n"<<config<<std::endl;
        // logger().info(ss.str());

        return Aws::MakeShared<Aws::Lambda::LambdaClient>(tag.c_str(), cred, config);
    }

    std::vector<std::string> list_functions(const std::shared_ptr<Aws::Lambda::LambdaClient>& client,
                                            std::ostream* os=nullptr) {
        Aws::Lambda::Model::ListFunctionsRequest list_req;
        auto outcome = client->ListFunctions(list_req);
        if (!outcome.IsSuccess()) {
            std::stringstream ss;
            ss << outcome.GetError().GetExceptionName().c_str() << ", "
            << outcome.GetError().GetMessage().c_str();
            if(os)
                *os << ss.str();
            return {};
        } else {
            // check whether function is contained
            auto funcs = outcome.GetResult().GetFunctions();
            // search for the function of interest
            std::vector<std::string> v;
            for (const auto &f: funcs) {
                v.push_back(f.GetFunctionName().c_str());
            }
            return v;
        }
    }

//    URI get_lambda_output_uri(const messages::InvocationRequest& req) {
//        static
//        // get output uri for THIS lambda
//        std::string base_output_uri = req.baseoutputuri();
//        URI output_uri;
//        FileFormat out_format = proto_toFileFormat(req.stage().outputformat());
//        auto file_ext = defaultFileExtension(out_format);
//        uint32_t partno = 0;
//        if(req.has_partnooffset()) {
//            partno = req.partnooffset();
//            output_uri = URI(base_output_uri).join("part" + std::to_string(partno));
//        } else {
//            output_uri = URI(base_output_uri).join(part_counter++);
//        }
//
//        return output_uri;
//    }


    URI create_output_uri_from_first_part_uri(const URI& first_part_uri, int first_part_offset, int offset) {
        // first_part_uri is an uri expected to have been created through generate_output_base_uri in AWSLambdaBackend.
        // this means URIs will have the form s3://.../....part<first_part_offset>.<ext>
        // Extract via regex the stored part number, if this fails -> create manually!

        // regex: /s3:\/\/.*\/.*part(\d+)\..*/gm
        std::regex r_exp("s3:\\/\\/.*\\/.*part(\\d+)\\..*");
        std::smatch matches;
        std::string str = first_part_uri.toString();
        std::string str_part_no;
        if(std::regex_search(str, matches, r_exp) && matches.size() == 2) {
                std::ssub_match sub_match = matches[1];
                str_part_no = sub_match.str();
        } else
          throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " could not extract part number from " + first_part_uri.toString());

        auto extracted_part_no = std::stoi(str_part_no);
        if(extracted_part_no != first_part_offset)
            throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " extracted part no does not match expected part no");
        auto str_part = "part" + str_part_no;

        auto n_digits = str_part_no.size();

        // Replace now part with new part
        return strReplaceAll(first_part_uri.toString(), str_part, "part" + fixedLength(first_part_offset + offset, n_digits));
    }

    URI create_spill_uri_from_first_part_uri(const URI& first_part_uri, int first_part_offset, int offset) {
        // first_part_uri is an uri expected to have been created through generate_output_base_uri in AWSLambdaBackend.
        // this means URIs will have the form s3://.../lam{number}
        // Extract via regex the stored part number, if this fails -> create manually!

        std::regex r_exp("s3:\\/\\/.*\\/lam(\\d+)");
        std::smatch matches;
        std::string str = first_part_uri.toString();
        std::string str_part_no;
        if(std::regex_search(str, matches, r_exp) && matches.size() == 2) {
            std::ssub_match sub_match = matches[1];
            str_part_no = sub_match.str();
        } else
            throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " could not extract spill number from " + first_part_uri.toString());

        auto extracted_part_no = std::stoi(str_part_no);
        if(extracted_part_no != first_part_offset)
            throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " extracted part no does not match expected part no");
        auto n_digits = str_part_no.size();

        // find from right last one
        auto s = first_part_uri.toString();
        s = s.substr(0, s.rfind(str_part_no));
        return s + fixedLength(first_part_offset + offset, n_digits);
    }

    int LambdaWorkerApp::invokeRecursivelyAsync(int num_to_invoke,
                                                   const std::string& lambda_endpoint,
                                                   const messages::InvocationRequest& req_template,
                                                   const std::vector<std::vector<FilePart>>& parts) {
        if(parts.empty()) {
            logger().info("No parts given, skip.");
            return WORKER_OK;
        }

        // quick check, else need to redistribute.
        if(parts.size() != num_to_invoke) {
            logger().error("number of parts does not match number of workers to invoke. TODO: redistribute.");
            return WORKER_ERROR_EXCEPTION;
        }

        logger().info("For now recursively invoking " + pluralize(num_to_invoke, "request") + ".");

        // adjust settings based on request
        if(!lambda_endpoint.empty()) {
            logger().info("Updating network settings.");
            update_network_settings({std::make_pair(AWS_LAMBDA_ENDPOINT_KEY, lambda_endpoint)});
        }

        logger().info("Creating Lambda invoker for recursive requests.");

        // std::shared_ptr<Aws::Lambda::LambdaClient> client = std::make_shared<Aws::Lambda::LambdaClient>(credentials, config);
        double timeout = 180.0;
        auto client = createLambdaClient(timeout, num_to_invoke);

        // Make sure functionName is not empty.
        if(_functionName.empty()) {
            std::string err_msg = "FunctionName not set for Lambda worker, can't start invocation service.";
            _response.set_status(messages::InvocationResponse_Status::InvocationResponse_Status_ERROR);
            _response.set_errormessage(err_msg);
            logger().error(err_msg);
            return WORKER_ERROR_LAMBDA_CLIENT;
        }

        reset_lambda_invocation_service(client);
        logger().info("Lambda client created.");
        // Default Lambda role created by deploy script does not have lambda:ListFunctions policy enabled.
        // Therefore, this code will result in an AccessDeniedException. Commented out for now, not really
        // a purpose to list functions again here.
        // logger().info("Lambda client created, listing functions:");
        // {
        //     std::stringstream ss;
        //     auto v = list_functions(client, &ss);
        //     ss<<"\nFound functions: "<<v<<std::endl;
        //     logger().info(ss.str());
        // }

        // For each part, invoke new message.
        if(req_template.type() != messages::MessageType::MT_TRANSFORM) {
            logger().error("Unknown message type of template, can't self-invoke.");
            return WORKER_ERROR_EXCEPTION;
        }

        // Template must have a part offset assigned (i.e., client controls where to invoke what).
        if(!req_template.has_partnooffset()) {
            throw std::runtime_error("Template request in invokeRecursivelyAsync must have a partnooffset assigned.");
        }

        // Check that there are no further recursive invocations.
        if(req_template.stage().invocationcount_size() != 0)
            throw std::runtime_error("Recursive invocations found, for now only single-level supported.");

        // For each file part, create new message & invoke recursively.
        for(unsigned i = 0; i < parts.size(); ++i) {

            auto part = parts[i];

            AwsLambdaRequest aws_req;
            aws_req.body = req_template;
            aws_req.body.clear_inputuris();
            aws_req.body.clear_inputsizes();
            aws_req.body.clear_baseoutputuri();
            aws_req.body.clear_partnooffset();

            for(const auto& p : part) {
                aws_req.body.add_inputuris(encodeRangeURI(p.uri, p.rangeStart, p.rangeEnd));
                aws_req.body.add_inputsizes(p.size);
            }

            auto output_uri = create_output_uri_from_first_part_uri(req_template.baseoutputuri(), req_template.partnooffset(), i + 1);
            aws_req.body.set_baseoutputuri(output_uri.toString());
            aws_req.body.set_baseisfinaloutput(true);
            {
                std::stringstream ss;
                ss<<"Created request "<<(i + 1)<<"/"<<parts.size()<<" with base output uri: "<<output_uri.toString();
                logger().info(ss.str());
            }

            // Reset also spill uri (no conflicts allowed there as well).
            auto spill_root_uri = create_spill_uri_from_first_part_uri(req_template.settings().spillrooturi(), req_template.partnooffset(), i + 1);
            logger().info("Request using in case spill uri: " + spill_root_uri.toString());
            aws_req.body.mutable_settings()->set_spillrooturi(spill_root_uri.toString());

            // Add to invoker (this will immediately start the request).
            _lambdaInvoker->invokeAsync(aws_req, [this](const AwsLambdaRequest &req, const AwsLambdaResponse &resp) { thread_safe_lambda_success_handler(req, resp); },
                                        [this](const AwsLambdaRequest& request, LambdaStatusCode error_code, const std::string& error_message) {
                                            thread_safe_lambda_failure_handler(request, error_code, error_message);
            },
            [this](const AwsLambdaRequest& request, LambdaStatusCode retry_code, const std::string& retry_reason, bool will_decrease_retry_count) {
                std::stringstream ss;
                ss << "LAMBDA request failed, retrying with [" << (int)retry_code << "]: " << retry_reason<<" will decrease count: "<<will_decrease_retry_count;
                logger().info(ss.str());
            }
            );
        }

        // requests are now queued up. Now wait for them using waitForInvoker() function.

        return WORKER_OK;
    }

    void LambdaWorkerApp::thread_safe_lambda_failure_handler(const tuplex::AwsLambdaRequest &request,
                                                             tuplex::LambdaStatusCode error_code,
                                                             const std::string &error_message) {
        std::stringstream ss;
        ss << "LAMBDA request failed [" << (int)error_code << "]: " << error_message;
        logger().info(ss.str());

        // add as failure to response (so client can decide what to do, i.e. debug or reissue).
        // Extract RequestInfo and Container Info.
        messages::RequestInfo request_info; // dummy.

        // fill response as error message.
        messages::InvocationResponse response;
        response.set_status(messages::InvocationResponse_Status::InvocationResponse_Status_ERROR);
        response.set_errormessage(ss.str());

        // Add (mutex protected) to _response.
        {
            std::lock_guard<std::mutex> lock(_thread_safe_response_mutex);
            _thread_safe_response.add_invokedrequests()->CopyFrom(request_info);
            _thread_safe_response.add_invokedresponses()->CopyFrom(response);

            // Add request as resource for better debugging.
            // Could remove this code in the future.
            auto resource_msg = _thread_safe_response.add_resources();
            assert(resource_msg);
            resource_msg->set_type((uint32_t)ResourceType::BAD_REQUEST);
            nlohmann::json j;
            j["id"] = request.id;
            j["retriesLeft"] = request.retriesLeft;
            j["base64Request"] = encodeAWSBase64(request.body.SerializeAsString());
            j["errorCode"] = error_code;
            j["errorMessage"] = error_message;
            resource_msg->set_payload(j.dump());
        }
    }

    void LambdaWorkerApp::thread_safe_lambda_success_handler(const tuplex::AwsLambdaRequest &request,
                                                             const tuplex::AwsLambdaResponse &response) {
        {
            std::stringstream ss;
            ss << "LAMBDA request done (rc=" << response.info.returnCode << ", duration=" << response.info.awsTimings.durationInMs
               << "ms" << ").";

            // Additional debug info.
            std::vector<std::string> output_uris;
            std::copy(response.response.outputuris().begin(), response.response.outputuris().end(), std::back_inserter(output_uris));
            if(request.body.inputsizes_size() > 0)
                ss<<"\n-- input uri: "<<request.body.inputuris(0);
            ss<<"\n-- output uris: "<<output_uris;

            logger().info(ss.str());
        }

        // Extract RequestInfo and Container Info.
        messages::RequestInfo request_info;
        response.info.fill(&request_info);

        // Add (mutex protected) to _response.
        {
            std::lock_guard<std::mutex> lock(_thread_safe_response_mutex);
            _thread_safe_response.add_invokedrequests()->CopyFrom(request_info);
            _thread_safe_response.add_invokedresponses()->CopyFrom(response.response);
        }
    }

    int LambdaWorkerApp::waitForInvoker() const {
        if(!_lambdaInvoker) {
            std::string err_msg = "No valid lambda invoker, can not wait for <nullptr>.";
            const_cast<LambdaWorkerApp*>(this)->_response.set_errormessage(err_msg);
            logger().error(err_msg);
            return WORKER_ERROR_LAMBDA_CLIENT;
        }

        logger().info("Waiting for requests to finish...");
        _lambdaInvoker->waitForRequests();
        logger().info(""
                      "Recursive invoke done, GBs: " + std::to_string(_lambdaInvoker->usedGBSeconds())
        + " n_requests: " + std::to_string(_lambdaInvoker->numRequests()));

        // check results now.

        return WORKER_OK;
    }

    void LambdaWorkerApp::fill_response_with_self_invocation_state(messages::InvocationResponse& response) const {
        {
            // Get from thread (bad hack with constness)
            std::lock_guard<std::mutex> lock(const_cast<LambdaWorkerApp*>(this)->_thread_safe_response_mutex);
            response.mutable_invokedrequests()->CopyFrom(_thread_safe_response.invokedrequests());
            response.mutable_invokedresponses()->CopyFrom(_thread_safe_response.invokedresponses());
        }
    }

    std::vector<ContainerInfo> normalizeInvokedContainers(const std::vector<ContainerInfo>& containers) {

        auto& logger = Logger::instance().defaultLogger();

        // clean containers
        std::unordered_map<std::string, ContainerInfo> uniqueContainers;

        bool internal_error = false;

        for(auto info : containers) {
            auto it = uniqueContainers.find(info.uuid);
            if(it == uniqueContainers.end())
                uniqueContainers[info.uuid] = info;
            else {
                // update if more recent (only for reused, new should be unique!)
                if(it->second.reused && it->second.msRemaining >= info.msRemaining) {
                    it->second = info;
                }

                if(!it->second.reused)
                    internal_error = true;
            }
        }

        if(internal_error)
            logger.error("internal error, 2x new with unique ID?");

        std::vector<ContainerInfo> ret;
        ret.reserve(uniqueContainers.size());
        for(auto keyval : uniqueContainers) {
            ret.push_back(keyval.second);
        }
        return ret;
    }


    bool checkIfOptionIsSetInEnv(const std::string& option_name) {
        auto var = getenv(option_name.c_str());
        if(!var)
            return false;
        if(var) {
            std::string value = var;
            for(auto& c : value)
                c = std::tolower(c);
            // compare to true, on, ...
            if(value == "true" || value == "on" || value == "yes")
                return true;
            else if(value == "false" || value == "off" || value == "no")
                return false;
            else {
                Logger::instance().defaultLogger().error("Found option " + option_name + " in environment, but with invalid value");
                return false;
            }
        }

        return false;
    }
}

#endif
