//
// Created by Leonhard Spiegelberg on 7/8/22.
//

#ifndef TUPLEX_MESSAGES_H
#define TUPLEX_MESSAGES_H

#ifdef BUILD_WITH_AWS
// collection of all protobuf messages + definitions
#include <managed/proto/Lambda.pb.h>
#endif

namespace tuplex {
    enum class ResourceType : uint32_t {
        UNKNOWN = 0,
        LOG = 1
    };
}

#endif //TUPLEX_MESSAGES_H
