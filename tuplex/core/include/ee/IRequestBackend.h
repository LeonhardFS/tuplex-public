//
// Created by Leonhard Spiegelberg on 9/17/24.
//

#ifndef TUPLEX_IREQUESTBACKEND_H
#define TUPLEX_IREQUESTBACKEND_H

#include "IBackend.h"

// Protobufs.
#include <utils/Messages.h>
#include "RequestInfo.h"

namespace tuplex {
    class IRequestBackend : public IBackend {
    public:
        IRequestBackend() = delete;
        IRequestBackend(Context& context) : IBackend(context), _emitRequestsOnly(false), _blockRequests(false) {}

        /*!
         * sets additional environment keys to be sent as part of request.
         * @param env key=value environment key pairs.
         */
        virtual void setEnvironment(const std::unordered_map<std::string, std::string>& env) {
            _environmentOverwrite = env;
        }

        // Helpers for testing.

        /*!
         * if true, requests are only created but not posted.
         * @param emitOnly
         */
        void setRequestMode(bool emitOnly) {
            _emitRequestsOnly = emitOnly;
        }

        /*!
         * if true, requests will be carried out synchronously. One by one.
         * @param block
         */
        void setBlockingRequests(bool block) {
            _blockRequests = block;
        }

        inline std::vector<messages::InvocationRequest> pendingRequests(bool clear=true) {
            if(clear) {
                auto v = _pendingRequests;
                _pendingRequests.clear();
                return v;
            }
            return _pendingRequests;
        }

        virtual ~IRequestBackend() {}
    protected:
        std::vector<messages::InvocationRequest> _pendingRequests;
        bool _emitRequestsOnly;
        bool _blockRequests;
        std::unordered_map<std::string, std::string> _environmentOverwrite;

        /*! add environment keys, global configs */
        inline void fill_env(messages::InvocationRequest &request) {
            if(!_environmentOverwrite.empty()) {
                for(const auto& key_value : _environmentOverwrite) {
                    (*request.mutable_env())[key_value.first] = key_value.second;
                }
            }
        }

        /*!
         * Helper function to print out (Lambda and Worker compatible) as JSON.
         * @param ss where to print data
         * @param startTimestamp
         * @param endTimestamp
         * @param use_hyper
         * @param cost
         * @param n_requests
         * @param total_mbmbs
         * @param cost_per_gb_second
         * @param total_input_normal_path
         * @param total_general_path
         * @param total_input_fallback_path
         * @param total_unresolved_path
         * @param total_output_rows
         * @param total_output_exceptions
         * @param requests_responses vector of triplets: First is the client-side request info (i.e. time from client), then the request and its response.
         */
        static void dumpAsJSON(std::stringstream& ss, uint64_t startTimestamp,
                               uint64_t endTimestamp,
                               bool use_hyper,
                               double cost,
                               size_t n_requests,
                               size_t total_mbmbs,
                               double cost_per_gb_second,
                               size_t total_input_normal_path,
                               size_t total_general_path,
                               size_t total_input_fallback_path,
                               size_t total_unresolved_path,
                               size_t total_output_rows,
                               size_t total_output_exceptions,
                               const std::vector<TaskTriplet>& requests_responses
        );
    };
}
#endif //TUPLEX_IREQUESTBACKEND_H
