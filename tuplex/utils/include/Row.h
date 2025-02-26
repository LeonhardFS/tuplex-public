//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_ROW_H
#define TUPLEX_ROW_H

#include <Serializer.h>
#include <Field.h>
#include <ExceptionCodes.h>
#include <TypeHelper.h>

#ifdef BUILD_WITH_CEREAL
#include "cereal/access.hpp"
#include "cereal/types/memory.hpp"
#include "cereal/types/polymorphic.hpp"
#include "cereal/types/base_class.hpp"
#include "cereal/types/vector.hpp"
#include "cereal/types/utility.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/common.hpp"
#include "cereal/archives/binary.hpp"
#endif

namespace tuplex {
    /*!
     * expensive wrapper for a single column::
     * For the actual computation, serialized versions are used.
     * This is merely a result type to be passed to the frontend.
     */
    class Row {
    private:

        // row is internally a vector of strings
        // right now only basic types supported.
        // serialize i.e. each element as string
        // ==> optimize later.

        Schema                      _schema;
        std::vector<Field>          _values;
        size_t                      _serializedLength;

        // get length from values
        Serializer getSerializer() const;
        size_t getSerializedLength() const;

        python::Type row_type_from_values() const;

        // For faster size compute, have handy map ready to lookup types.
        static const std::unordered_map<size_t, size_t> precomputed_size_map;
    public:
        Row() : _serializedLength(0) {}

        Row(const Row& other) : _schema(other._schema), _values(other._values), _serializedLength(other._serializedLength) {}

        Row& operator = (const Row& other) {
            _schema = other._schema;
            _values = other._values;
            _serializedLength = other._serializedLength;
            return *this;
        }

        Row(Row&& other) : _schema(other._schema), _serializedLength(other._serializedLength), _values(std::move(other._values)) {
            other._values = {};
            other._serializedLength = 0;
            other._schema = Schema::UNKNOWN;
        }


        // new constructor using variadic templates
        template<typename... Targs> Row(Targs... Fargs) {
            vec_build(_values, Fargs...);
            _schema = Schema(Schema::MemoryLayout::ROW, row_type_from_values());
            _serializedLength = getSerializedLength();
        }

        inline size_t   getNumColumns() const { return _values.size(); }
        inline Field    get(const int col) const {
            assert(!_values.empty());
            assert(0 <= col && col < _values.size());
            return _values[col];
        }

        inline void set(const unsigned col, const Field& f) {
#ifndef NDEBUG
            if(col >= _values.size())
                throw std::runtime_error("invalid column index in get specified");
#endif
            _values[col] = f;

            // need to update type of row!
            auto old_type = getRowType();
            auto types = old_type.isTupleType() ? old_type.parameters() : old_type.get_column_types();
            if(types[col] != f.getType()) {
                types[col] = f.getType();
                auto new_type = old_type.isTupleType() ? python::Type::makeTupleType(types) : python::Type::makeRowType(types, old_type.get_column_names());
                _schema = Schema(_schema.getMemoryLayout(), new_type);
            }

            // update length, may change!
            _serializedLength = getSerializedLength();
        }


        bool            getBoolean(const int col) const;
        int64_t         getInt(const int col) const;
        double          getDouble(const int col) const;
        std::string     getString(const int col) const;
        Tuple           getTuple(const int col) const;
        List            getList(const int col) const;
        Schema          getSchema() const;

        Tuple getAsTuple() const;

        /*!
         * creates a row representation for easier output from internal memory storage format
         * @param schema Schema to use in order to deserialize from the memory ptr
         * @param ptr pointer to an address on where to start deserialization
         * @param size safe size of the memory region starting at ptr
         * @return Row with contents if deserialization succeeds. Empty row else.
         */
        static Row fromMemory(const Schema& schema, const void *ptr, size_t size);
        static Row fromMemory(Deserializer& ds, const void *ptr, size_t size);


        /*!
         * creates a row representation from memory where exceptions are stored. Because the exception type partially defines
         * the storage format, the normal fromMemory doesn't work always
         */
        static Row fromExceptionMemory(const Schema& schema, ExceptionCode ec, const void *ptr, size_t size);
        static Row fromExceptionMemory(Deserializer& ds, ExceptionCode ec, const void *ptr, size_t size);

        static size_t exceptionRowSize(Deserializer& ds, ExceptionCode ec, const void *ptr, size_t size);

        /*!
         * length contents would require in internal memory storage format
         * @return length in bytes
         */
        size_t serializedLength() const { return _serializedLength; }

        /*!
         * returns type of corresponding column
         * @param col column index
         * @return type of column col
         */
        python::Type getType(const int col) const;

        /*!
         * returns type of the row as tuple type
         * @return python tuple type corresponding to the type build from field types
         */
        python::Type getRowType() const;

        /*!
         * serializes row contents to memory starting at buffer. performs capacity check before serialization
         * @param buffer memory address where to serialize to
         * @param capacity secured capacity after memory address
         * @return serialized length of the row's contents
         */
        size_t serializeToMemory(uint8_t* buffer, const size_t capacity) const;

        /*!
         * creates valid python source representation of values as tuple.
         * @return string with pythonic data representation
         */
        std::string toPythonString() const;

        /*!
         * creates a representation as JSON string. If no columns are given, then the first level is represented as array
         * @return Json string in the form o [...] if no columns are given, or {col: key, col:key, ...} if columns are given
         */
        std::string toJsonString(const std::vector<std::string>& columns={}) const;

        /*!
         * returns for each column a string representing its contents. Can be used for display.
         * @return vector of strings of the row contents.
         */
        inline std::vector<std::string> getAsStrings() const {
            std::vector<std::string> out;
            out.reserve(_values.size());
            for(const auto& el: _values)
                out.push_back(el.desc());
            return out;
        }

        friend bool operator == (const Row& lhs, const Row& rhs);
        friend bool operator < (const Row& lhs, const Row& rhs);

        static Row from_vector(const std::vector<Field>& fields) {
            Row row;
            row._values = fields;
            row._schema = Schema(Schema::MemoryLayout::ROW, row.row_type_from_values());
            row._serializedLength = row.getSerializedLength();
            return row;
        }

        inline std::vector<Field> to_vector() const {
            return _values;
        }

        Row upcastedRow(const python::Type& targetType) const;

        // creates new Row with RowType
        Row with_columns(const std::vector<std::string>& column_names) const;

#ifdef BUILD_WITH_CEREAL
        // cereal serialization function
        template<class Archive> void serialize(Archive &ar) {
            ar(_schema, _values, _serializedLength);
        }
#endif
    };

    // used for tests
    extern bool operator == (const Row& lhs, const Row& rhs);
    extern bool operator < (const Row& lhs, const Row& rhs);

    struct ExceptionSample {
        std::string first_row_traceback;
        std::vector<Row> rows;
    };

    /*!
    * prints a nicely formatted table of rows
    * @param os
    * @param header
    * @param rows
    * @param quoteStrings whether to display with single quotes ' or not.
    */
    void printTable(std::ostream& os, const std::vector<std::string>& header,
                    const std::vector<Row>& rows, bool quoteStrings=true);

    /*!
     * detect (each col independent) majority for each column and form row type
     * @param rows sample, if empty unknown is returned
     * @param threshold normal-case threshold
     * @param independent_columns whether to treat each column independently or use joint maximization
     * @param use_nvo if active Option[T] types are speculated on to be either None, T or Option[T] depending on threshold
     * @param t_policy to create a bigger majority case, types may be unified. This controls which policy to apply when unifying types.
     * @return majority type
     */
    extern python::Type detectMajorityRowType(const std::vector<Row>& rows,
                                              double threshold,
                                              bool independent_columns=true,
                                              bool use_nvo=true,
                                              const TypeUnificationPolicy& t_policy=TypeUnificationPolicy::defaultPolicy());

    extern python::Type detectMajorityRowType(const std::vector<python::Type>& row_types,
                                       double threshold,
                                       bool independent_columns=true,
                                       bool use_nvo=true,
                                       const TypeUnificationPolicy& t_policy=TypeUnificationPolicy::defaultPolicy());

    template<typename T> bool vec_set_eq(const std::vector<T>& lhs, const std::vector<T>& rhs) {
        std::set<T> L(lhs.begin(), lhs.end());
        std::set<T> R(rhs.begin(), rhs.end());

        auto lsize = L.size();
        auto rsize = R.size();

        if(lsize != rsize)
            return false;

        // merge sets
        for(auto el : rhs)
            L.insert(el);
        return L.size() == lsize;
    }

    inline void reorder_row(Row& row,
                     const std::vector<std::string>& row_column_names,
                     const std::vector<std::string>& dest_column_names) {

        assert(row_column_names.size() == dest_column_names.size());
        assert(vec_set_eq(row_column_names, dest_column_names)); // expensive check

        // for each name, figure out where it has to be moved to!
        std::unordered_map<unsigned, unsigned> map;
        std::vector<Field> fields(row_column_names.size());
        for(unsigned i = 0; i < row_column_names.size(); ++i) {
            map[i] = indexInVector(row_column_names[i], dest_column_names);
        }
        for(unsigned i = 0; i < row_column_names.size(); ++i)
            fields[map[i]] = row.get(i);
        row = Row::from_vector(fields);
    }

    template<typename T> inline std::vector<T> vec_set_difference(const std::vector<T>& lhs, const std::vector<T>& rhs) {
        std::unordered_set<T> S_lhs(lhs.begin(), lhs.end());
        std::unordered_set<T> S_rhs(rhs.begin(), rhs.end());

        std::vector<T> v;
        for(const auto& el : S_lhs) {
            if(S_rhs.find(el) == S_rhs.end())
                v.push_back(el);
        }
        return v;
    }

    inline void reorder_and_fill_missing_will_null(Row& row, const std::vector<std::string>& row_column_names, const std::vector<std::string>& dest_column_names) {
        // check now which names are missing
        std::vector<std::string> missing_dest = vec_set_difference(dest_column_names, row_column_names);
        auto missing_origin = vec_set_difference(row_column_names, dest_column_names);

        // append to dest missing origing, and put them at the end.
        auto expanded_dest_column_names = dest_column_names;
        if(!missing_origin.empty())
            std::copy(missing_origin.begin(), missing_origin.end(), std::back_inserter(expanded_dest_column_names));

        if(!missing_dest.empty()) {
            auto fields = row.to_vector();
            auto expanded_names = row_column_names;
            for(const auto& name : missing_dest) {
                fields.push_back(Field::null());
                expanded_names.push_back(name);
            }

            // checks:
            assert(expanded_names.size() == expanded_dest_column_names.size());
            assert(fields.size() == expanded_names.size());

            row = Row::from_vector(fields); // this sets the new type as well.
            reorder_row(row, expanded_names, expanded_dest_column_names);

            if(PARAM_USE_ROW_TYPE)
                row = row.with_columns(expanded_dest_column_names);


            // // do not reorder, simply fill. This also projects out in case.
            // std::vector<Field> dest;
            // for(const auto& name : dest_column_names) {
            //     auto idx = indexInVector(name, expanded_names);
            //     if(idx >= 0)
            //         dest.push_back(fields[idx]);
            //     else
            //         dest.push_back(Field::null());
            // }
            //
            // row = Row::from_vector(dest);
            return;
        }

        assert(row_column_names.size() == expanded_dest_column_names.size());
        assert(row.getNumColumns() == row_column_names.size());
        reorder_row(row, row_column_names, expanded_dest_column_names);

        if(PARAM_USE_ROW_TYPE)
            row = row.with_columns(expanded_dest_column_names);
    }

    inline std::vector<std::string> reorder_and_expand_column_names(const std::vector<std::string>& row_column_names,
                                                                    const std::vector<std::string>& dest_column_names) {
        // check now which names are missing
        std::vector<std::string> missing_dest = vec_set_difference(dest_column_names, row_column_names);
        auto missing_origin = vec_set_difference(row_column_names, dest_column_names);

        // append to dest missing origing, and put them at the end.
        auto expanded_dest_column_names = dest_column_names;
        if(!missing_origin.empty())
            std::copy(missing_origin.begin(), missing_origin.end(), std::back_inserter(expanded_dest_column_names));

        return expanded_dest_column_names;
    }

}
#endif //TUPLEX_ROW_H