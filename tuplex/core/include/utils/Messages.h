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
        OBJECT_CODE_GENERAL_CASE=3
    };

    // request modes:
    static const uint64_t REQUEST_MODE_COMPILE_AND_RETURN_OBJECT_CODE=0x1;
    static const uint64_t REQUEST_MODE_COMPILE_ONLY=0x2;
}

#endif //TUPLEX_MESSAGES_H
