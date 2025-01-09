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

namespace tuplex {

    std::string i32tobuf(int i) {
        char buf[4];
        *(int*)buf = i;
        return buf;
    }


    // Custom binary stream for types to store data efficiently.
    // Also helps to decode.
    class BinaryOutputStream {
    public:
        BinaryOutputStream() {}

        std::string str() const {
            return _stream.str();
        }

        BinaryOutputStream& operator << (int i) {
            _stream.write(reinterpret_cast<const char*>(&i), sizeof(int));
            return *this;
        }

        BinaryOutputStream& operator << (std::string s) {
            int size = size; // cast.
            _stream.write(reinterpret_cast<const char*>(&size), sizeof(int));
            _stream.write(s.data(), s.size());
            return *this;
        }

        BinaryOutputStream& operator << (const char c) {
            _stream<<c;
            return *this;
        }

        BinaryOutputStream& operator << (bool b) {
            // use 1 byte, could use a bit as well...
            if(b)
                _stream<<"T";
            else
                _stream<<"F";
            return *this;
        }

    private:
        std::ostringstream _stream;
    };


    void type_encode(BinaryOutputStream& stream, const python::Type& t) {
        // binary stream??

        // some frequent python types get their own symbol
        if(t == python::Type::NULLVALUE) {
            stream<<'N';
            return;
        }
        if(t == python::Type::BOOLEAN) {
            stream<<'B';
            return;
        }
        if(t == python::Type::I64) {
            stream<<'I';
            return;
        }
        if(t == python::Type::STRING) {
            stream<<'S';
            return;
        }
        if(t == python::Type::F64) {
            stream<<'F';
            return;
        }

        // simple encoding with hash
        if(t.hash() < 16) {
           stream<<'H'; // H for hash.
           stream<<t.hash();
           return;
        }

        // primitive? need to encode name, that's it.
        if(t.isAbstractPrimitiveType()) {
            stream<<'P'; // P for primitive
            stream<<t.desc();
            return;
        }

        // Option
        if(t.isOptionType()) {
            stream<<'O';
            type_encode(stream, t.getReturnType());
            return;
        }

        // List
        if(t.isListType()) {
            stream<<'L';
            type_encode(stream, t.getReturnType());
            return;
        }

        // encode each other type using desc() as fallback with U tag.
        stream<<'U';
        stream<<t.desc();
    }

    // TODO: reuse output buffer.
    std::string compact_type_encode(const python::Type& t) {
        BinaryOutputStream os;
        type_encode(os, t);
        return os.str();
    }
}

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
    for(unsigned i = 0; i < types_to_encode.size(); ++i) {
        auto t = types_to_encode[i];
        cout<<"type #"<<i<<"\t"<<"compact: "<<compact_type_encode(t).size()<<" B\t"<<t.desc().size()<<" B"<<endl;
    }
}