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

#endif