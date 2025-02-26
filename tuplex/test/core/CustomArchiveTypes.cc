//
// Created by Leonhard Spiegelberg on 1/7/25.
//

#include "gtest/gtest.h"

#ifdef BUILD_WITH_CEREAL
#include <CustomArchive.h>
#include <Context.h>

// Test to check more efficient encoding.
TEST(TypeSys, EfficientTypeSerialization) {
    using namespace tuplex;
    using namespace std;

    auto t1 = python::Type::makeOptionType(python::Type::I64);

    cout<<"Type to encode: "<<t1.desc()<<endl;

    // Encode:
    std::ostringstream oss(std::stringstream::binary);
    {
        // cereal::BinaryOutputArchive ar(oss);
        BinaryOutputArchive ar(oss);
        ar(t1);
        // ar going out of scope flushes everything
    }
    auto bytes_str = oss.str();

    cout<<"Serialized data length: "<<bytes_str.size()<<" B"<<endl;

    // Decode:
    python::Type t1_decoded;
    {
        std::istringstream iss(bytes_str);
        BinaryInputArchive ar(iss);

        ar(t1_decoded);
    }

    cout<<"Decoded Type: "<<t1_decoded.desc()<<endl;

    EXPECT_EQ(t1.desc(), t1_decoded.desc());
}

TEST(TypeSys, EncodeManyTypes) {
    using namespace tuplex;
    using namespace std;

    // use look up table to speed up type encoding/decoding.
    // Load file to string vector
    auto data = fileToString("../resources/schemas.txt");
    auto lines = splitToLines(data);

    std::unordered_map<int, int> unique_types_map;
    std::vector<python::Type> types_to_encode;
    for (const auto &line: lines) {
        auto t = python::decodeType(line);
        types_to_encode.emplace_back(t);
    }


    // Start testing:
    // --> Encode simply all types.
    // Encode:
    std::ostringstream oss(std::stringstream::binary);

    std::string serialized_type_closure;

    {
        // cereal::BinaryOutputArchive ar(oss);
        BinaryOutputArchive ar(oss);
        for(const auto& t : types_to_encode)
            ar(t);
        // ar going out of scope flushes everything

        serialized_type_closure = serialize_type_closure(ar.typeClosure());
    }
    auto bytes_str = oss.str();

    cout<<"Serialized data length: "<<bytes_str.size()<<" B"<<endl;
    cout<<"Type system closure serialized length: "<<serialized_type_closure.size()<<" B"<<endl;
    ASSERT_NE(bytes_str.size(), 0);

    // concat both strings together (util).
    auto full_data = string_mem_cat(serialized_type_closure, bytes_str);
    EXPECT_EQ(full_data.size(), serialized_type_closure.size() + bytes_str.size());
    cout<<"Fully serialized with prepended type closure: "<<full_data.size()<<" B"<<endl;


    // Deserialize now, first decode type map.
    {
        std::vector<python::Type> decoded_types;

        size_t bytes_read_for_type_map = 0;
        auto type_map = deserialize_type_closure(reinterpret_cast<const uint8_t *>(full_data.data()), &bytes_read_for_type_map);

        {
            std::istringstream iss(full_data.substr(bytes_read_for_type_map));
            BinaryInputArchive ar(iss, type_map);

            for(unsigned i = 0; i < types_to_encode.size(); ++i) {
                python::Type t;
                ar(t);
                decoded_types.push_back(t);
            }
        }

        EXPECT_EQ(decoded_types.size(), types_to_encode.size());
        for(unsigned i = 0; i < types_to_encode.size(); ++i)
            EXPECT_EQ(types_to_encode[i].desc(), decoded_types[i].desc());
    }
}

#endif


TEST(TypeSys, CompactEncodeDecode) {
   using namespace tuplex;

   // new helper function for compact encoding/decoding of types (manual parse).
   // @TODO:

   // Basically encode with tag | content for each type.


    using namespace tuplex;
    using namespace std;

    // use look up table to speed up type encoding/decoding.
    // Load file to string vector
    auto data = fileToString("../resources/schemas.txt");
    auto lines = splitToLines(data);

    std::unordered_map<int, int> unique_types_map;
    std::vector<python::Type> types_to_encode;
    for (const auto &line: lines) {
        auto t = python::decodeType(line);
        types_to_encode.emplace_back(t);
    }


    // TEST:
    std::vector<std::string> v_encoded_full;
    std::vector<std::string> v_encoded_compact;

    size_t total_size_old = 0;
    size_t total_size_compact = 0;

    for(unsigned i = 0; i < types_to_encode.size(); ++i) {
        auto t = types_to_encode[i];
        cout<<"type #"<<i<<"\t"<<"compact: "<<compact_type_encode(t).size()<<" B\t"<<t.desc().size()<<" B"<<endl;

        // Check encode/decode cycle
        auto encoded = compact_type_encode(t);
        auto decoded = compact_type_decode(encoded);

        EXPECT_EQ(decoded.desc(), t.desc());

        v_encoded_full.push_back(t.desc());
        v_encoded_compact.push_back(encoded);


        total_size_compact += v_encoded_compact.back().size();
        total_size_old += v_encoded_full.back().size();
    }


    // timed test:
    Timer timer;
    std::vector<python::Type> v_decoded_full;
    for(auto enc : v_encoded_full) {
        v_decoded_full.push_back(python::Type::decode(enc));
    }
    cout<<"Took "<<timer.time()<<"s to decode all (non-compact)."<<endl;
    timer.reset();
    std::vector<python::Type> v_decoded_compact;
    for(auto enc : v_encoded_compact) {
        v_decoded_compact.push_back(compact_type_decode(enc));
    }
    cout<<"Took "<<timer.time()<<"s to decode all (compact)."<<endl;
    cout<<"Total size (full):\t"<<total_size_old<<" B"<<endl;
    cout<<"Total size (compact):\t"<<total_size_compact<<" B"<<endl;
    auto pct = 100.0 * (double)total_size_compact / (double)total_size_old;
    auto factor = (double)total_size_old / (double)total_size_compact;
    std::cout.precision(2);
    cout<<"compact is % of full: "<<pct<<" ("<<factor<<"x compression)"<<endl;
}