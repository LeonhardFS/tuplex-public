//
// Created by Leonhard Spiegelberg on 9/17/24.
//

#ifndef TUPLEX_IREQUESTBACKEND_H
#define TUPLEX_IREQUESTBACKEND_H

#include "IBackend.h"

// Protobufs.
#include <utils/Messages.h>

namespace tuplex {
    class IRequestBackend : public IBackend {
    public:

        IRequestBackend(const Context& context) : IBackend(context), _emitRequestsOnly(false) {}

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
        std::unordered_map<std::string, std::string> _environmentOverwrite;

        /*! add environment keys, global configs */
        inline void fill_env(messages::InvocationRequest &request) {
            if(!_environmentOverwrite.empty()) {
                for(const auto& key_value : _environmentOverwrite) {
                    (*request.mutable_env())[key_value.first] = key_value.second;
                }
            }
        }
    };
}
#endif //TUPLEX_IREQUESTBACKEND_H
