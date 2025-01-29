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

#ifndef TUPLEX_AWSLAMBDABACKEND_H
#define TUPLEX_AWSLAMBDABACKEND_H

#include "../IRequestBackend.h"
#include <vector>
#include <physical/execution/TransformStage.h>
#include <physical/execution/HashJoinStage.h>
#include <physical/execution/AggregateStage.h>
#include <physical/codegen/BlockBasedTaskBuilder.h>
#include <physical/execution/IExceptionableTask.h>
#include <numeric>
#include <physical/execution/TransformTask.h>
#include <physical/execution/ResolveTask.h>

#include <utils/Messages.h>

#include "AWSCommon.h"
#include "ContainerInfo.h"
#include "../RequestInfo.h"
#include "AWSLambdaInvocationService.h"
#include <aws/core/Aws.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/lambda/model/CreateFunctionRequest.h>
#include <aws/lambda/model/DeleteFunctionRequest.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <aws/lambda/model/ListFunctionsRequest.h>
#include <aws/lambda/LambdaClient.h>
#include <regex>

namespace tuplex {

    class LogicalPlan;
    class PhysicalPlan;
    class PhysicalStage;


    enum class AwsLambdaExecutionStrategy {
        UNKNOWN=0,
        DIRECT,
        TREE
    };

    inline AwsLambdaExecutionStrategy stringToAwsExecutionStrategy(const std::string& str) {
        std::string name = str;
        trim(name);
        for(auto& c : name)
             c = std::tolower(c);

        if("direct" == name)
            return AwsLambdaExecutionStrategy::DIRECT;
        if("tree" == name)
            return AwsLambdaExecutionStrategy::TREE;

        throw std::runtime_error("Unknown Lambda execution strategy " + str);

        return AwsLambdaExecutionStrategy::UNKNOWN;
    }



    class AwsLambdaBackend : public IRequestBackend {
    public:
        AwsLambdaBackend() = delete;
        ~AwsLambdaBackend() override;

        AwsLambdaBackend(Context& context, const AWSCredentials& credentials, const std::string& functionName);

        Executor* driver() override { return _driver.get(); }
        void execute(PhysicalStage* stage) override;

    private:
        AWSCredentials _credentials;
        std::string _functionName; // name of the AWS lambda func.
        std::string _functionArchitecture; // x86_64 | arm64
        ContextOptions _options;
        std::unique_ptr<Executor> _driver;

        MessageHandler& _logger;

        // AWS Lambda interface
        std::string _tag;
        std::shared_ptr<Aws::Lambda::LambdaClient> _client;
        std::unique_ptr<AwsLambdaInvocationService> _service; // <-- use this to invoke lambdas

        // fetch values via options
        size_t _lambdaSizeInMB;
        size_t _lambdaTimeOut;

        uint64_t _startTimestamp;
        uint64_t _endTimestamp;

        JobInfo _info;

        std::vector<std::tuple<LambdaStatusCode, std::string, AwsLambdaRequest>> _failedRequests;
        std::unordered_map<std::string, AwsLambdaRequest> _invokedRequests; // for easier lookup.

        // mapping of remote to local paths for result collection.
        std::unordered_map<URI, URI> _remoteToLocalURIMapping;

        // web ui
        HistoryServerConnection _historyConn;
        std::shared_ptr<HistoryServerConnector> _historyServer;

        void reset();
        void checkAndUpdateFunctionConcurrency(const std::shared_ptr<Aws::Lambda::LambdaClient>& client,
                                               size_t concurrency,
                                               const std::string& functionName,
                                               bool provisioned=false);
        size_t _functionConcurrency;

        std::vector<AwsLambdaRequest> createSingleFileRequests(const TransformStage* tstage,
                                                                          const std::string& bitCode,
                                                                          const size_t numThreads,
                                                                          const std::vector<std::tuple<
                                                                          std::string, std::size_t>>& uri_infos,
                                                                          const std::string& spillURI, const size_t buf_spill_size);

        struct RequestConfig {
            std::string spillURI;
            size_t buf_spill_size;
            size_t minimum_size_to_specialize; //! don't invoke hyperspecialization for small files
            size_t specialization_unit_size; //! size on which to split files into individual specialization units,
                                             //! i.e. if this is 1G and a file is 2G then two specializsaiton units are used.
            size_t maximum_lambda_process_size; //! fine-grained block size what to process with a Lambda function

            RequestConfig():buf_spill_size(0), minimum_size_to_specialize(0), specialization_unit_size(0), maximum_lambda_process_size(0) {}

            inline bool valid() const {
                if(0 == buf_spill_size || 0 == minimum_size_to_specialize || 0 == specialization_unit_size || 0 == maximum_lambda_process_size)
                    return false;

                return true;
            }
        };

        std::vector<AwsLambdaRequest> createSpecializingSelfInvokeRequests(const TransformStage* tstage,
                                                                           const std::string& bitCode,
                                                                           const size_t numThreads,
                                                                           const std::vector<std::tuple<
                                                                                   std::string, std::size_t>>& uri_infos,
                                                                                   const RequestConfig& conf);

        std::vector<messages::InvocationRequest> createSelfInvokingRequests(const TransformStage* tstage,
                                                                          const std::string& bitCode,
                                                                          const size_t numThreads,
                                                                          const std::vector<std::tuple<
                                                                                  std::string, std::size_t>>& uri_infos,
                                                                          const std::string& spillURI, const size_t buf_spill_size);

        // Lambda request callbacks
        void onLambdaSuccess(const AwsLambdaRequest& req, const AwsLambdaResponse& resp);
        void onLambdaFailure(const AwsLambdaRequest& req, LambdaStatusCode err_code, const std::string& err_msg);
        void onLambdaRetry(const AwsLambdaRequest& req, LambdaStatusCode retry_code, const std::string& retry_msg, bool decreasesRetryCount);

        URI _scratchDir;
        bool _deleteScratchDirOnShutdown;

        /*!
         * returns a scratch dir. If none is stored/found, abort
         * @param hints one or more directories (typically buckets) where a temporary cache region could be stored.
         * @return URI or URI::INVALID
         */
        URI scratchDir(const std::vector<URI>& hints=std::vector<URI>{});

        // // store for tasks done
        std::mutex _mutex;
        std::vector<AwsLambdaResponse> _tasks;
        std::shared_ptr<Aws::Lambda::LambdaClient> makeClient();

        void invokeAsync(const AwsLambdaRequest& req);

        std::vector<URI> hintsFromTransformStage(const TransformStage* stage);

        inline MessageHandler logger() const { return _logger; }

        // void abortRequestsAndFailWith(int returnCode, const std::string& errorMessage);

        // std::set<std::string> performWarmup(const std::vector<int>& countsToInvoke, size_t timeOutInMs=4000, size_t baseDelayInMs=75);

        /*!
         * print extended lambda statistics out
         */
        void gatherStatistics();


        std::vector<URI> thread_safe_get_input_uris();
        std::vector<URI> thread_safe_get_output_uris();

        /*!
         * outputs all the stats etc. as json file for analysis
         * @param json_path
         */
        void dumpAsJSON(const std::string& json_path);

        std::string csvPerFileInfo();


        std::atomic<size_t> _total_mbms;
        std::atomic<size_t> _total_requests;

        inline size_t getMBMs() const { return _total_mbms; }

        inline void resetBilling() {
            _total_mbms = 0;
            _total_requests = 0;
        }

        /*!
         * add incurred costs to calculations.
         * @param mbms
         * @param n_requests
         */
        inline void addBilling(size_t mbms, size_t n_requests=1) {
            _total_mbms += mbms;
            _total_requests += n_requests;
        }

        inline double usedGBSeconds() const {
            // 1ms = 0.001s
            // 1mb = 0.001Gb
            return (double)getMBMs() / 1000000.0;

            // old: Before Dec 2020, now costs changed.
            // return (double)getMB100Ms() / 10000.0;
        }
        inline size_t numRequests() const { return _total_requests; }

        inline double costPerGBSecond() const {
            // depends on ARM or x86 architecture
            // values from https://aws.amazon.com/lambda/pricing/ (Nov 2021)
            double cost_per_gb_second_x86 = 0.0000166667;
            double cost_per_gb_second_arm = 0.0000133334;

            double cost_per_gb_second = cost_per_gb_second_x86;

            // check architecture, else assume x86 cost
            if(_functionArchitecture == "arm64")
                cost_per_gb_second = cost_per_gb_second_arm;
            return cost_per_gb_second;
        }

        inline double lambdaCost() const {
            double cost_per_request = 0.0000002;

            auto cost_per_gb_second = costPerGBSecond();

            auto usedGBSeconds = this->usedGBSeconds();
            auto numRequests = this->numRequests();

            return usedGBSeconds * cost_per_gb_second + (double)numRequests * cost_per_request;
        }

        /*!
         * generate a baseURI for a temporary file.
         * @param stageNo
         * @return URI
         */
        inline URI tempStageURI(int stageNo) const {
            return URI(_options.AWS_SCRATCH_DIR() + "/temporary_stage_output/" + "stage_" + std::to_string(stageNo));
        }

        void fill_output_uris(std::vector<AwsLambdaRequest> &requests, const TransformStage* tstage, int num_digits, int numThreads);

        inline void fill_with_transform_stage(messages::InvocationRequest& req, const TransformStage* tstage) const;
        inline void fill_with_worker_config(messages::InvocationRequest& req,
                    const std::string& worker_spill_uri,
                    int numThreads) const;

        inline size_t compute_buf_spill_size(int numThreads) const {
            // at least 1 thread.
            numThreads = std::max(numThreads, 1);

            // perhaps also use:  - 64 * numThreads ==> smarter buffer scaling necessary.
            size_t buf_spill_size = (_options.AWS_LAMBDA_MEMORY() - 256) / numThreads * 1000 * 1024;

            // limit to 128mb each
            if (buf_spill_size > 128 * 1000 * 1024)
                buf_spill_size = 128 * 1000 * 1024;

            return buf_spill_size;
        }

        inline std::string spillURI() const {
            auto scratchDir = _options.AWS_SCRATCH_DIR();
            if(scratchDir.empty())
                scratchDir = _options.SCRATCH_DIR().toPath();
            return scratchDir + "/spill_folder";
        }

        inline void fill_specialization_unit(messages::SpecializationUnit& m,
                                             const std::vector<std::tuple<URI,size_t>>& uri_infos,
                                             const SamplingMode& sampling_mode);

        // this may internally modify the remote mapping.
        URI generate_output_base_uri(const TransformStage* tstage, int taskNo, int num_digits=-1, int part_no=-1, int num_digits_part=-1);

        void validate_requests_can_be_served_with_concurrency_limit(const std::vector<AwsLambdaRequest>& requests,
                                                               size_t concurrency_limit);


        void perform_requests(const std::vector<AwsLambdaRequest>& requests, size_t concurrency_limit);

        void fill_with_stage(std::vector<AwsLambdaRequest> &requests, TransformStage *tstage, size_t numThreads,
                             size_t max_retries);

        std::vector<AwsLambdaRequest>
        create_specializing_requests_with_multiple_files(const TransformStage* tstage,
                                                         int numThreads,
                                                         const std::vector<std::tuple<std::string, size_t>>& uri_infos, MessageHandler& logger,
                                                         std::function<std::string(int n, int n_digits)> generate_output_base_uri=[](int n, int n_digits) {
                                                             return "part" + fixedLength(n, n_digits); });
    };


    // Helper functions (helpful when splitting up requests into recursive ones)
    // this here splits by size.
    extern std::vector<AwsLambdaRequest> create_specializing_recursive_requests(const std::vector<std::tuple<URI, std::size_t>> &uri_infos,
                                                                         size_t minimum_chunk_size,
                                                                         size_t maximum_chunk_size,
                                                                         MessageHandler& logger,
                                                                         std::function<std::string(int n, int n_digits)> generate_output_base_uri=[](int n, int n_digits) {
                                                                             return "part" + fixedLength(n, n_digits); });

    // Helper when not URIs but strings.
    inline std::vector<AwsLambdaRequest> create_specializing_recursive_requests(const std::vector<std::tuple<std::string, std::size_t>> &uri_infos,
                                                                                size_t minimum_chunk_size,
                                                                                size_t maximum_chunk_size,
                                                                                MessageHandler& logger,
                                                                                std::function<std::string(int n, int n_digits)> generate_output_base_uri=[](int n, int n_digits) {
                                                                                    return "part" + fixedLength(n, n_digits); }) {
        using namespace std;
        vector<tuple<URI, size_t>> v;
        for(const auto& t : uri_infos)
            v.emplace_back(make_tuple(URI(get<0>(t)), get<1>(t)));
        return create_specializing_recursive_requests(v, minimum_chunk_size, maximum_chunk_size, logger, generate_output_base_uri);
    }


    // Helper functions to distribute workload between lambdas.

    /*!
     * binary search based solver to find suitable maximum chunk size given input parameters.
     * @param sizes vector of file sizes
     * @param c_min minimum chunk size.
     * @param parallelism desired parallelism. Ideally, maximum chunk size c_max will be calculated that per-file specialization will result in parallelism requests.
     * @return c_max, maximum chunk size.
     */
    extern size_t find_max_chunk_size(std::vector<size_t> sizes, size_t c_min, size_t parallelism);

}

#endif //TUPLEX_AWSLAMBDABACKEND_H

#endif