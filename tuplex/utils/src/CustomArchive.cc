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

        auto& ar = *this;
        ar(hash);
    }


    std::vector<std::pair<int, std::string>> BinaryOutputArchive::typeClosure() const {
        // Compute which types correspond to which hash (in the encoded message):
        // Go in order of registered types.
        // This should minimize parsing.
        std::vector<std::pair<int, std::string>> v;
        for(auto hash : typeHashes)
            v.push_back(std::make_pair(hash, typeMap.at(hash)));
        return v;
    }

    int BinaryInputArchive::loadTypeHash(int encoded_hash) {
        // This is more tricky. Basically, with lookup map transform/check hash vs. TypeSystem.
        // std::cout<<"Loading hash: "<<encoded_hash<<std::endl;

        // debugging:
        if(this_type_system_to_encoded_hash_mapping.find(encoded_hash) == this_type_system_to_encoded_hash_mapping.end()) {
#ifndef NDEBUG
            std::cerr<<"Could not find hash when loading type hash in deserialization."<<std::endl;
#endif
        }

        // return mapped version.
        return this_type_system_to_encoded_hash_mapping.at(encoded_hash);
    }

    void BinaryInputArchive::init_type_map(const std::vector<std::pair<int, std::string>> &type_map) {
        if(type_map.empty())
            return;

        // Go through type map, for each encoded hash check if Type exists.
        for(const auto& p : type_map) {
            // can hash desc quickly to check if it already exists...
            auto decoded_type = python::Type::decode(p.second);

            this_type_system_to_encoded_hash_mapping[p.first] = decoded_type.hash();
        }
    }

    std::string serialize_type_closure(const std::vector<std::pair<int, std::string>>& v) {
        size_t memory_size = sizeof(int64_t);

        for(const auto& s: v)
            memory_size += sizeof(int) + sizeof(int) + s.second.size();

        std::string buffer;
        buffer.resize(memory_size);

        auto ptr = (uint8_t*)buffer.data();
        *(int64_t*)ptr = v.size();
        ptr += sizeof(int64_t);
        for(const auto& s: v) {
            *(int*)ptr = s.first;
            ptr += sizeof(int);
            *(int*)ptr = s.second.size();
            ptr += sizeof(int);
            memcpy(ptr, s.second.data(), s.second.size());
            ptr += s.second.size();
        }

        return buffer;
    }

    std::vector<std::pair<int, std::string>> deserialize_type_closure(const uint8_t* ptr, size_t* bytes_read) {
        auto original_ptr = ptr;

        auto n_entries = *((int64_t*)ptr);
        ptr += sizeof(int64_t);

        std::vector<std::pair<int, std::string>> v;

        // decode hashes + string representations.
        for(unsigned i = 0; i < n_entries; ++i) {
            auto hash = *(int*)ptr;
            ptr += sizeof(int);
            auto size = *(int*)ptr;
            ptr += sizeof(int);

            std::string s(size, '\0');
            memcpy(s.data(), ptr, size);
            ptr += size;

            v.emplace_back(std::make_pair(hash, s));
        }

        if(bytes_read)
            *bytes_read = ptr - original_ptr;

        return v;
    }
}