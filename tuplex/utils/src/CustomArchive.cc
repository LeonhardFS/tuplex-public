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
            typeHashes.emplace_back(hash);
        }

        std::cout<<"Saving hash: "<<hash<<std::endl;
        auto& ar = *this;
        ar(hash);
    }


    std::vector<std::pair<int, std::string>> BinaryOutputArchive::typeClosure() const {
        // Compute which types correspond to which hash (in the encoded message):



        return {};
    }

    int BinaryInputArchive::loadTypeHash(int encoded_hash) {
        // This is more tricky. Basically, with lookup map transform/check hash vs. TypeSystem.

        std::cout<<"Loading hash: "<<encoded_hash<<std::endl;

        return encoded_hash;
    }
}