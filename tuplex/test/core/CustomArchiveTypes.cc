//
// Created by Leonhard Spiegelberg on 1/7/25.
//

#include "gtest/gtest.h"

#ifdef BUILD_WITH_CEREAL
#include <utils/CustomArchive.h>
#include <Context.h>

// Test to check more efficient encoding.
TEST(TypeSys, EfficientTypeSerialization) {
    using namespace tuplex;
    using namespace std;

    auto t1 = python::Type::makeOptionType(python::Type::I64);

    std::ostringstream oss(std::stringstream::binary);
    {
        // cereal::BinaryOutputArchive ar(oss);
        BinaryOutputArchive ar(oss);
        ar(t1);
        // ar going out of scope flushes everything
    }
    auto bytes_str = oss.str();

    cout<<"Length: "<<bytes_str.size()<<endl;
}


#endif