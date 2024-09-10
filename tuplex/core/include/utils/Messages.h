//
// Created by Leonhard Spiegelberg on 7/8/22.
//

#ifndef TUPLEX_MESSAGES_H
#define TUPLEX_MESSAGES_H

// collection of all protobuf messages + definitions
#include <managed/proto/Lambda.pb.h>

namespace tuplex {
    enum class ResourceType : uint32_t {
        UNKNOWN = 0,
        LOG = 1,
        OBJECT_CODE_NORMAL_CASE=2,
        OBJECT_CODE_GENERAL_CASE=3,
        ENVIRONMENT_JSON=4,
    };

    // request modes:
    // Compile object code and return it.
    static const uint64_t REQUEST_MODE_COMPILE_AND_RETURN_OBJECT_CODE=0x1;
    // Only compile, do not execute compiled code.
    static const uint64_t REQUEST_MODE_COMPILE_ONLY=0x2;
    // In this mode, use provided object code.
    static const uint64_t REQUEST_MODE_SKIP_COMPILE=0x4;


    inline std::vector<messages::Resource> find_resources_by_type(const messages::InvocationResponse& response, ResourceType type) {
        std::vector<messages::Resource> v;
        for(auto r : response.resources()) {
            if(r.type() == static_cast<uint32_t>(type))
                v.push_back(r);
        }
        return v;
    }
}

#endif //TUPLEX_MESSAGES_H
