//
// Created by Leonhard Spiegelberg on 1/7/25.
//

#include <CustomArchive.h>
#include <TypeSystem.h>

namespace tuplex {
    void BinaryOutputArchive::saveType(int hash) {

        // Track hashes and their string encoding.
        if(typeMap.find(hash) == typeMap.end()) {
            typeMap[hash] = python::Type::fromHash(hash).encode();
        }

        std::cout<<"Saving hash: "<<hash<<std::endl;
        auto& ar = *this;
        ar(hash);
    }
}