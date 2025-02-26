//
// Created by Leonhard Spiegelberg on 11/16/21.
//

#ifndef TUPLEX_NETWORK_H
#define TUPLEX_NETWORK_H

#include <string>

namespace tuplex {

    // helper struct to store various network related settings to apply to CURL etc.
    struct NetworkSettings {
        std::string caFile;
        std::string caPath;
        std::string endpointOverride;
        bool verifySSL;
        bool signPayloads; // use v4 payload signing policy.
        bool useVirtualAddressing; // whether to use http{s}://<bucket>.endpoint.com or not. When using AWS services, this should be true.
        NetworkSettings() : verifySSL(false), signPayloads(true), useVirtualAddressing(true) {}
    };
}

#endif //TUPLEX_NETWORK_H
