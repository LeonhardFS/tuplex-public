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
            int size = s.size(); // cast.
            _stream.write(reinterpret_cast<const char*>(&size), sizeof(int));
            _stream.write(s.data(), size);
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

    class BinaryInputStream {
    public:
        BinaryInputStream(const std::string& data) : _stream(data) {}

        std::string str() const {
            return _stream.str();
        }

        BinaryInputStream& operator >> (int& i) {
            _stream.read(reinterpret_cast<char*>(&i), sizeof(int));
            return *this;
        }

        BinaryInputStream& operator >> (std::string& s) {
            int size = 0; // cast.
            _stream.read(reinterpret_cast<char*>(&size), sizeof(int));
            s = std::string(size, '\0');
            _stream.read(s.data(), s.size());
            return *this;
        }

        BinaryInputStream& operator >> (char& c) {
            _stream>>c;
            return *this;
        }

        BinaryInputStream& operator << (bool& b) {
            // use 1 byte, could use a bit as well...
            char c;
            _stream>>c;
            b = c == 'T';
            return *this;
        }

    private:
        std::istringstream _stream;
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
        if(t == python::Type::PYOBJECT) {
            stream<<'P';
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
            stream<<'*'; // * for primitive
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

        // Row
        if(t.isRowType()) {
            stream<<'R';
            // how many names?
            // which entries?
            auto names = t.get_column_names();
            stream<<static_cast<int>(names.size());
            for(auto name: names)
                stream<<name;
            auto types = t.get_column_types();
            stream<<static_cast<int>(types.size());
            for(auto col_type: types)
                type_encode(stream, col_type);
            return;
        }

        // Struct/SparseStruct
        if(t.isStructuredDictionaryType() || t.isSparseStructuredDictionaryType()) {
            stream<<(t.isSparseStructuredDictionaryType() ? '<' : '{');
            auto entries = t.get_struct_pairs();
            stream<<static_cast<int>(entries.size());
            for(const auto& entry: entries) {
                type_encode(stream, entry.keyType);
                type_encode(stream, entry.valueType);
                stream<<entry.key;
                stream<<static_cast<char>(entry.presence);
            }
            return;
        }

        // Dict (code must come AFTER struct dict/sparse dict)
        if(t.isDictionaryType()) {
            stream<<'D';
            type_encode(stream, t.keyType());
            type_encode(stream, t.valueType());
            return;
        }

        // constant
        if(t.isConstantValued()) {
            stream<<'=';
            type_encode(stream, t.underlying());
            stream<<t.constant();
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



    // FAST decode function
    python::Type type_decode(BinaryInputStream& is) {
        char c;
        is>>c;

        switch(c) {
            // @TODO: use lower-case letters for options!
            case 'N':
                return python::Type::NULLVALUE;
            case 'B':
                return python::Type::BOOLEAN;
            case 'I':
                return python::Type::I64;
            case 'S':
                return python::Type::STRING;
            case 'F':
                return python::Type::F64;
            case 'P':
                return python::Type::PYOBJECT;

            // compound types
            case 'H': {
                int hash;
                is >> hash;
                return python::Type::fromHash(hash);
            }

            case '*': {
                std::string name;
                is >> name;
                return python::TypeFactory::instance().createOrGetPrimitiveType(name);
            }


            case 'O':
                return python::Type::makeOptionType(type_decode(is));
            case 'L':
                return python::Type::makeListType(type_decode(is));

            case 'R': {
                int n_names;
                int n_types;
                std::vector<std::string> names;
                std::vector<python::Type> types;
                is >> n_names;
                for(unsigned i = 0; i < n_names; ++i) {
                    std::string name;
                    is >> name;
                    names.push_back(name);
                }
                is >> n_types;
                for(unsigned i = 0; i < n_types; ++i) {
                    types.push_back(type_decode(is));
                }
                return python::Type::makeRowType(types, names);
            }

            case '<':
            case '{': {
                auto is_sparse = c == '<';
                std::vector<python::StructEntry> entries;

                int n_entries;
                is >> n_entries;
                for(unsigned i = 0; i < n_entries; ++i) {
                    python::StructEntry entry;
                    entry.keyType = type_decode(is);
                    entry.valueType = type_decode(is);
                    is >> entry.key;
                    char presence;
                    is >> presence;
                    entry.presence = static_cast<python::StructPresence>(presence);
                    entries.push_back(entry);
                }

                return python::Type::makeStructuredDictType(entries, is_sparse);
            }

            case 'D':
                return python::Type::makeDictionaryType(type_decode(is), type_decode(is));

            case '=': {
                python::Type underlying = type_decode(is);
                std::string constant;
                is >> constant;
                return python::Type::makeConstantValuedType(underlying, constant);
            }

            case 'U': {
                std::string desc;
                is >> desc;
                return python::Type::decode(desc);
            }

            default:
                throw std::runtime_error("DECODE ERROR IN compact type decode for " + is.str());
        }
    }


    python::Type compact_type_decode(const std::string& s) {
        BinaryInputStream is(s);
        return type_decode(is);
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

        // Check encode/decode cycle
        auto encoded = compact_type_encode(t);
        auto decoded = compact_type_decode(encoded);

        EXPECT_EQ(decoded.desc(), t.desc());

    }
}