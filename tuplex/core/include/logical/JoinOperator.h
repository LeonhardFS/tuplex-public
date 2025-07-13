//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_JOINOPERATOR_H
#define TUPLEX_JOINOPERATOR_H

#include "LogicalOperator.h"
#include "LogicalOperatorType.h"
#include "ExceptionOperator.h"

namespace tuplex {


    enum class JoinType {
        INNER,
        LEFT,
        RIGHT
    };

    class JoinOperator : public LogicalOperator {
    public:
        // required by cereal
        JoinOperator() = default;

        JoinOperator(const std::shared_ptr<LogicalOperator> &left,
                           const std::shared_ptr<LogicalOperator> &right,
                           option<std::string> leftColumn,
                           option<std::string> rightColumn, const JoinType& jt,
                           const std::string& leftPrefix, const std::string& leftSuffix,
                           const std::string& rightPrefix, const std::string& rightSuffix);

        virtual ~JoinOperator() {}

        virtual std::string name() const override { return "join"; }

        bool columnBasedJoin() const {
            assert(!((_leftColumn.has_value() && !_rightColumn.has_value()) ||
            (!_leftColumn.has_value() && _rightColumn.has_value())));

            return _leftColumn.has_value() && _rightColumn.has_value();
        }

        bool good() const override;

        std::vector<Row> getSample(size_t num) const override;

        bool isActionable() override;

        bool isDataSource() override;

        std::shared_ptr<LogicalOperator> clone(bool cloneParents) const override;

        Schema getInputSchema() const override;

        std::vector<std::string> columns() const override { return _columns; }

        // whether to build left or right (build on smaller relation)
        bool buildRight() const {

#warning "Query optimizer bug here: force build on right side for left join"
            if(joinType() == JoinType::LEFT)
                return true;

            return left()->cost() >= right()->cost();
        }

        // overwrite cost (should be estimated better, for now simply multiply)

    private:
        option<std::string> _leftColumn;  // column within left dataset
        option<std::string> _rightColumn;
        std::string _keyColumn;
        JoinType _joinType;

        std::string _leftPrefix;
        std::string _leftSuffix;
        std::string _rightPrefix;
        std::string _rightSuffix;

        // Keep to allow for linearization of stages and partial retyping.
        int _leftKeyIndex;
        int _rightKeyIndex;
        int _leftColumnCount;
        int _rightColumnCount;

        void updateIndicesAndCounts();
    public:
        LogicalOperatorType type() const override;

        std::shared_ptr<LogicalOperator> left() const  { assert(parents().size() == 2); return parents().front(); }
        std::shared_ptr<LogicalOperator> right() const { assert(parents().size() == 2); return parents()[1]; }

        int64_t leftKeyIndex() const;
        int64_t rightKeyIndex() const;
        /*!
         * where is the key stored in the final output?
         * @return index of the join key in the final schema
         */
        int64_t outputKeyIndex() const;

        option<std::string> leftColumn() const { return _leftColumn; }  // column within left dataset
        option<std::string> rightColumn() const { return _rightColumn; }
        JoinType joinType() const { return _joinType; }
        std::string leftPrefix () const { return _leftPrefix; }
        std::string leftSuffix() const { return _leftSuffix; }
        std::string rightPrefix() const { return _rightPrefix; }
        std::string rightSuffix() const { return _rightSuffix; }
        std::string keyColumn() const { return _keyColumn; }


        /*!
         * return columns in the bucket if a hash join is used.
         * @return vector of columns within bucket (key col excluded!)
         */
        std::vector<std::string> bucketColumns() const {
            std::vector<std::string> cols;
            if(buildRight()) {
                for(int i = 0; i < right()->columns().size(); ++i) {
                    if(i != rightKeyIndex())
                        cols.emplace_back(right()->columns()[i]);
                }
            } else {
                for(int i = 0; i < left()->columns().size(); ++i) {
                    if(i != leftKeyIndex())
                        cols.emplace_back(left()->columns()[i]);
                }
            }
            return cols;
        }

        /*!
         * return python Type for operator
         * @return
         */
        python::Type keyType() const {
            assert(right());
            assert(left());
            auto right_col_types = right()->getOutputSchema().getColumnTypes();
            auto left_col_types = left()->getOutputSchema().getColumnTypes();

           auto rk = right_col_types.at(rightKeyIndex());
           auto lk = left_col_types.at(leftKeyIndex());
           if(rk == lk)
              return rk;
           if(python::canUpcastType(rk, lk))
               return lk;
           if(python::canUpcastType(lk, rk))
               return rk;
           throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " incompatible key types " + rk.desc() +
           " [right] and " + lk.desc() + " [left] found.");
        }

        /*!
         * return bucket Type (depending where join is built) if hash was used
         * @return
         */
        python::Type bucketType() const {
            assert(left());
            assert(right());

            // fetch columns from schema
            std::vector<python::Type> types;
            auto rt = right()->getOutputSchema().getColumnTypes();
            auto lt = left()->getOutputSchema().getColumnTypes();

            if(buildRight()) {
                for(int i = 0; i < rt.size(); ++i) {
                    if(i != rightKeyIndex())
                        types.emplace_back(rt[i]);
                }
            } else {
                for(int i = 0; i < lt.size(); ++i) {
                    if(i != leftKeyIndex())
                        types.emplace_back(lt[i]);
                }
            }
            return python::Type::makeTupleType(types);
        }

        void partialRetype(const Schema& schema, const std::vector<std::string>& columns);

        /*!
         * restrict join on columns, i.e. use a rewrite map for that
         * @param rewriteMap
         */
        virtual void projectionPushdown();

#ifdef BUILD_WITH_CEREAL
        // cereal serialization functions
        template<class Archive> void save(Archive &ar) const {
            ar(::cereal::base_class<LogicalOperator>(this), _leftColumn, _rightColumn, _joinType, _leftPrefix, _leftSuffix, _rightPrefix, _rightSuffix);
        }
        template<class Archive> void load(Archive &ar) {
            ar(::cereal::base_class<LogicalOperator>(this), _leftColumn, _rightColumn, _joinType, _leftPrefix, _leftSuffix, _rightPrefix, _rightSuffix);
        }
#endif

    private:
        // column within right dataset

        // join mode is inner join for now only

        std::vector<std::string> _columns;

        void inferSchema();
    };

    /*!
     * computes left and right column mapping map.
     * I.e., (k, v) will yield index of column k present as column v in the combined type. v will have values 0, ..., n-1 where n = #entries in combinedJoinType (see below).
     * @param leftType
     * @param leftKeyIndex
     * @param rightType
     * @param rightKeyIndex
     * @param joinType
     * @return
     */
    inline std::tuple<std::unordered_map<int, int>, std::unordered_map<int, int>> combinedColumnMapping(int leftColumnCount,
                                                                                                        int leftKeyIndex,
                                                                                                        int rightColumnCount,
                                                                                                        int rightKeyIndex) {

        std::unordered_map<int, int> leftColumnMapping;
        std::unordered_map<int, int> rightColumnMapping;

        int combined_output_pos = 0;

        // combined schema from row type
        std::vector<python::Type> combinedTypes;
        for(int i = 0; i < leftColumnCount; ++i) {
            if(i != leftKeyIndex)
                leftColumnMapping[i] = combined_output_pos++;
        }

        // The key column.
        leftColumnMapping[leftKeyIndex] = combined_output_pos;
        rightColumnMapping[rightKeyIndex] = combined_output_pos;
        combined_output_pos++;

        for(int i = 0; i < rightColumnCount; ++i) {
            if(i != rightKeyIndex)
                rightColumnMapping[i] = combined_output_pos++;
        }

        return std::make_tuple(leftColumnMapping, rightColumnMapping);
    }

    template<class K, class V> std::unordered_map<V, K> swap_map(const std::unordered_map<K, V>& in) {
        std::unordered_map<V, K> m;
        for (const auto& p: in)
            m[p.second] = p.first;
        return m;
    }

    inline python::Type combinedJoinType(const python::Type& leftType,
                                         int leftKeyIndex,
                                         const python::Type& rightType,
                                         int rightKeyIndex,
                                         JoinType joinType) {
        std::vector<python::Type> left_types = leftType.isRowType() ? leftType.get_column_types() : leftType.parameters();
        std::vector<python::Type> right_types = rightType.isRowType() ? rightType.get_column_types() : rightType.parameters();

        std::vector<std::string> left_names;
        std::vector<std::string> right_names;
        std::vector<std::string> combined_names;
        if (leftType.isRowType())
            left_names = leftType.get_column_names();
        if (rightType.isRowType())
            right_names = rightType.get_column_names();
        if (left_names.empty())
            for (unsigned i = 0; i < left_types.size(); ++i)
                left_names.emplace_back("_L" + std::to_string(i));
        if (right_names.empty())
            for (unsigned i = 0; i < right_types.size(); ++i)
                right_names.emplace_back("_R" + std::to_string(i));

        // combined schema from row type
        std::vector<python::Type> combinedTypes;
        for(int i = 0; i < left_types.size(); ++i) {
            if(i != leftKeyIndex) {
                combinedTypes.push_back(left_types[i]);
                combined_names.push_back(left_names[i]);
            }
        }

        // fetch more restrictive type b.c. it's an inner join...
        auto leftKeyType = left_types[leftKeyIndex];
        auto rightKeyType = right_types[rightKeyIndex];

        auto leftName = left_names[leftKeyIndex];
        auto rightName = right_names[rightKeyIndex];

        // if one is option type and the other is not, take
        switch(joinType) {
            case JoinType::LEFT: {
                // always the left result
                combinedTypes.push_back(leftKeyType);
                combined_names.push_back(leftName);
                break;
            }
            case JoinType::RIGHT: {
                // always the right result
                combinedTypes.push_back(rightKeyType);
                combined_names.push_back(rightName);
                break;
            }
            case JoinType::INNER: {
                // more interesting case:
                // same type => doesn't matter
                combined_names.push_back(leftName);

                if(leftKeyType == rightKeyType)
                    combinedTypes.push_back(rightKeyType);
                else {
                    // there are a couple cases. Some should be handled separately, i.e. those resulting in
                    // empty datasets!
                    if(leftKeyType == python::Type::NULLVALUE && !rightKeyType.isOptionType())
                        throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " empty datset, should be handled somewhere up the chain!");
                    else if(!leftKeyType.isOptionType() && rightKeyType == python::Type::NULLVALUE)
                        throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " empty datset, should be handled somewhere up the chain!");
                    else if(leftKeyType == python::Type::NULLVALUE && rightKeyType.isOptionType())
                        combinedTypes.push_back(python::Type::NULLVALUE);
                    else if(leftKeyType.isOptionType() && rightKeyType == python::Type::NULLVALUE)
                        combinedTypes.push_back(python::Type::NULLVALUE);
                    else if(leftKeyType.isOptionType() && !rightKeyType.isOptionType())
                        combinedTypes.push_back(rightKeyType);
                    else if(!leftKeyType.isOptionType() && rightKeyType.isOptionType())
                        combinedTypes.push_back(leftKeyType);
                    else throw std::runtime_error("unknown combination encountered");
                }
                break;
            }
        }



        for(int i = 0; i < right_types.size(); ++i) {
            auto t = right_types[i];
            if(i != rightKeyIndex) {
                combined_names.push_back(right_names[i]);
                // important to make option type (nullable in left join)
                switch(joinType) {
                    case JoinType::LEFT: {
                        combinedTypes.push_back(python::Type::makeOptionType(t));
                        break;
                    }
                    case JoinType::INNER: {
                        combinedTypes.push_back(t);
                        break;
                    }
                    default: {
                        throw std::runtime_error("join type not implemented");
                    }
                }
            }
        }

        if (leftType.isRowType() && rightType.isRowType()) {
         return python::Type::makeRowType(combinedTypes, combined_names);
        }
        return python::Type::makeTupleType(combinedTypes);
    }
}

#ifdef BUILD_WITH_CEREAL
CEREAL_REGISTER_TYPE(tuplex::JoinOperator);
#endif

#endif //TUPLEX_JOINOPERATOR_H