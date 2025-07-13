//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/JoinOperator.h>
#include <algorithm>
#include <CSVUtils.h>

namespace tuplex {

    JoinOperator::JoinOperator(const std::shared_ptr<LogicalOperator>& left, const std::shared_ptr<LogicalOperator>& right,
                               tuplex::option<std::string> leftColumn, tuplex::option<std::string> rightColumn,
                               const tuplex::JoinType &jt, const std::string &leftPrefix, const std::string &leftSuffix,
                               const std::string &rightPrefix, const std::string &rightSuffix) : LogicalOperator(
            {left, right}),
                                                                                                 _leftColumn(
                                                                                                         leftColumn),
                                                                                                 _rightColumn(
                                                                                                         rightColumn),
                                                                                                 _joinType(jt),
                                                                                                 _leftPrefix(
                                                                                                         leftPrefix),
                                                                                                 _leftSuffix(
                                                                                                         leftSuffix),
                                                                                                 _rightPrefix(
                                                                                                         rightPrefix),
                                                                                                 _rightSuffix(
                                                                                                         rightSuffix) {

        // inner join:
        // schema is to be combined using columns etc.
        _leftColumnCount = -1;
        _rightColumnCount = -1;
        _leftKeyIndex = -1;
        _rightKeyIndex = -1;

        updateIndicesAndCounts();

        // Only by default perform typing iff both parents are set. Else, need to manually update.
        if (left && right)
            inferSchema();
    }

    void JoinOperator::updateIndicesAndCounts() {
        if (!columnBasedJoin()) {
            _leftKeyIndex = 0;
            _rightKeyIndex = 0;
            return;
        }

        if (left()) {
            if (columnBasedJoin())
                _leftKeyIndex = indexInVector(_leftColumn.value(), left()->columns());
            _leftColumnCount = left()->getOutputSchema().getColumnCount();
        }
        if (right()) {
            if (columnBasedJoin())
                _rightKeyIndex = indexInVector(_rightColumn.value(), right()->columns());
            _rightColumnCount = right()->getOutputSchema().getColumnCount();
        }
    }

    int64_t JoinOperator::leftKeyIndex() const {
        return _leftKeyIndex;
    }

    int64_t JoinOperator::rightKeyIndex() const {
        return _rightKeyIndex;
    }

    int64_t JoinOperator::outputKeyIndex() const {
        // easy, simply last idx of left columns
        if(columnBasedJoin()) {
            assert(left() && right());
            return left()->columns().size() - 1; // -1 for index
        } else throw std::runtime_error("not yet supported!");
    }

    void JoinOperator::partialRetype(const Schema& schema, const std::vector<std::string>& columns) {
        if (!columnBasedJoin())
            throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " non column based join not yet implemented.");

        // two modes:
        // either column based OR (K, V), (K, W) based
        if ((_leftColumn.has_value() && !_rightColumn.has_value()) ||
            (!_leftColumn.has_value() && _rightColumn.has_value()))
            throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " join mode is either column name based or tuple based with types (K, V), (K, W).");


        // Get left and right type from schema.
        auto t = combinedColumnMapping(_leftColumnCount, _leftKeyIndex, _rightColumnCount, _rightKeyIndex);
        auto left_map = std::get<0>(t);
        auto right_map = std::get<1>(t);

        auto col_types = schema.getColumnTypes();
        std::vector<python::Type> left_col_types(left_map.size());
        std::vector<python::Type> right_col_types(right_map.size());
        for (const auto& p: left_map)
            left_col_types[p.first] = col_types[p.second];
        for (const auto& p: right_map)
            right_col_types[p.first] = col_types[p.second];
        auto left_type = python::Type::makeTupleType(left_col_types);
        auto right_type = python::Type::makeTupleType(right_col_types);

        // Overwrite now types with partial output types!
        if (left()) {
            assert(left()->getOutputSchema().getColumnCount() == left_col_types.size());
            left_type = python::Type::makeTupleType(left()->getOutputSchema().getColumnTypes());
        }
        if (right()) {
            assert(right()->getOutputSchema().getColumnCount() == right_col_types.size());
            right_type = python::Type::makeTupleType(right()->getOutputSchema().getColumnTypes());
        }

        // combine types
        auto combinedRowType = combinedJoinType(left_type,
                leftKeyIndex(),
                right_type,
                rightKeyIndex(),
                joinType());

        // create schema
        setOutputSchema(Schema(Schema::MemoryLayout::ROW, combinedRowType));
        _columns = columns;
    }

    void JoinOperator::inferSchema() {
        using namespace std;

        // two modes:
        // either column based OR (K, V), (K, W) based
        if ((_leftColumn.has_value() && !_rightColumn.has_value()) ||
            (!_leftColumn.has_value() && _rightColumn.has_value()))
            throw std::runtime_error("join mode is either column name based or tuple based with types (K, V), (K, W).");

        if (columnBasedJoin()) {
            // column based => get key type of column!

            // important to get here the columns of the result dataset! // ==> no, projection pushdown???
            //            auto leftColumns = left()->getDataSet()->columns();
            //            auto rightColumns = right()->getDataSet()->columns();

            auto leftColumns = left()->columns();
            auto rightColumns = right()->columns();

            auto leftIndex = indexInVector(_leftColumn.value(), leftColumns);
            auto rightIndex = indexInVector(_rightColumn.value(), rightColumns);

            if (leftIndex < 0)
                throw std::runtime_error("column '" + _leftColumn.value() + "' not found in left dataset for join.");
            if (rightIndex < 0)
                throw std::runtime_error("column '" + _rightColumn.value() + "' not found in right dataset for join.");

            // indexing asserts...
            assert(leftIndex >= 0 && rightIndex >= 0);
            assert(leftIndex < left()->getOutputSchema().getColumnCount());
            assert(rightIndex < right()->getOutputSchema().getColumnCount());
            assert(leftColumns.size() == left()->getOutputSchema().getColumnCount());
            assert(rightColumns.size() == right()->getOutputSchema().getColumnCount());

            auto left_col_types = left()->getOutputSchema().getRowType().isRowType() ? left()->getOutputSchema().getRowType().get_column_types() : left()->getOutputSchema().getRowType().parameters();
            auto right_col_types = right()->getOutputSchema().getRowType().isRowType() ? right()->getOutputSchema().getRowType().get_column_types() : right()->getOutputSchema().getRowType().parameters();

            auto leftType = left_col_types[leftIndex];
            auto rightType = right_col_types[rightIndex];

            // make sure key types are the same, else abort
            // @TODO: could replace logically with empty result set b.c. python objects
            if (!((leftType == rightType) ||
                  (leftType.isOptionType() && leftType.getReturnType() == rightType) ||
                  (rightType.isOptionType() && rightType.getReturnType() == leftType) ||
                  (leftType.isOptionType() && rightType == python::Type::NULLVALUE) ||
                  (rightType.isOptionType() && leftType == python::Type::NULLVALUE))) {
                throw std::runtime_error(
                        "can't perform join, left column '" + _leftColumn.value() + "'type " + leftType.desc()
                        + " is not the same as right column '" + _rightColumn.value() + "'type " + rightType.desc());
            }

            // same types, hence extract values
            // what order of columns?
            // ==> first all the left columns EXCEPT the join column, then the join column,
            //     then the right columns EXCEPT the join column?
            //     what about the column name?
            // @TODO: different options here...


            // Check whether there are conflicting column names. If so, issue warning!
            // sanity check, see whether column names exist multiple times!
            if(leftPrefix().empty() && rightPrefix().empty() && leftSuffix().empty() && rightSuffix().empty()) {
                vector<string> overlappingColumns;
                set_intersection(leftColumns.begin(), leftColumns.end(), rightColumns.begin(), rightColumns.end(),
                                 std::back_inserter(overlappingColumns));
                sort(overlappingColumns.begin(), overlappingColumns.end());

                if ((overlappingColumns.size() == 1 &&
                     (_leftColumn.value() != _rightColumn.value() || overlappingColumns.front() != _leftColumn.value())) ||
                    overlappingColumns.size() > 1) {
                    stringstream ss;
                    ss << "Found columns " << csvToHeader(overlappingColumns)
                       << " in both left and right dataset for join operation, consider prefixing or suffixing them, because when indexing with names the first matching column name will be used.";
                    Logger::instance().defaultLogger().warn(ss.str());
                }
            }


            // @TODO: probably need at some point a rename function...
            // ==> this can be a simple map...
            // Note: need to update this in WebUI! => i.e. selectColumns, rename etc. can be all realized using a simple map.
            // why reinvent the wheel?


            // @TODO: use here combinedTypes

            // Note: if a left or right join is involved, propagate types to Nullables!
            // construct columns
            // general type of join is
            // | ... all cols from left side... | keycol | ... all cols from right side ... |
            vector<string> columns;
            int joinColIdx = 0;
            for (int i = 0; i < leftColumns.size(); ++i) {
                if (_leftColumn.value() != leftColumns[i]) {
                    columns.push_back(_leftPrefix + leftColumns[i] + _leftSuffix);
                } else
                    joinColIdx = i;
            }

            // the join column (reuse name from left!)
            // ==> it never gets nulled!
            // @TODO: add alias...
            _keyColumn = _leftPrefix + leftColumns[joinColIdx] + _leftSuffix;
            columns.push_back(_keyColumn);

            for (int i = 0; i < rightColumns.size(); ++i) {
                if (_rightColumn.value() != rightColumns[i]) {
                    columns.push_back(_rightPrefix + rightColumns[i] + _rightSuffix);
                }
            }

            // combine types
            auto combinedRowType = combinedJoinType(left()->getOutputSchema().getRowType(),
                    leftKeyIndex(),
                    right()->getOutputSchema().getRowType(),
                    rightKeyIndex(),
                    joinType());

            // create schema
            setOutputSchema(Schema(Schema::MemoryLayout::ROW, combinedRowType));
            _columns = columns;
        } else {
            // tuple based
            // easier: nothing to worry about.
            throw std::runtime_error("not implemented yet");
        }
    }

    LogicalOperatorType JoinOperator::type() const {
        return LogicalOperatorType::JOIN;
    }

    bool JoinOperator::good() const {
        return true;
    }

    std::vector<Row> JoinOperator::getSample(size_t num) const {
        // @TODO: fix this later!!!
        Logger::instance().defaultLogger().warn("getSample for join not yet implemented, returning empty vector");

        // @TODO: better handling of C++ exceptions with C-extension.
        //throw std::runtime_error("getSample for join not yet implemented");

        // for now, empty sample...
        return std::vector<Row>();
    }

    bool JoinOperator::isActionable() {
        return false;
    }

    bool JoinOperator::isDataSource() {
        return false;
    }

    Schema JoinOperator::getInputSchema() const {
        throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " input schema makes no sense for join operator, because there are two input schemas!");
        return Schema();
    }

    void JoinOperator::projectionPushdown() {
        updateIndicesAndCounts();

        // need to rewrite keys etc. here...
        inferSchema();
    }

    std::shared_ptr<LogicalOperator> JoinOperator::clone(bool cloneParents) const {
        JoinOperator* copy = nullptr;
        if (cloneParents)
            copy = new JoinOperator(left() ? left()->clone() : nullptr, right() ? right()->clone() : nullptr,
                _leftColumn, _rightColumn, _joinType, _leftPrefix, _leftSuffix, _rightPrefix, _rightSuffix);
        else {
            // Set all fields.
            copy = new JoinOperator();
            copy->setParents(std::vector<std::shared_ptr<LogicalOperator>>({nullptr, nullptr}));
            copy->_leftColumn = _leftColumn;
            copy->_rightColumn = _rightColumn;
            copy->_joinType = _joinType;
            copy->_leftPrefix = _leftPrefix;
            copy->_leftSuffix = _leftSuffix;
            copy->_rightPrefix = _rightPrefix;
            copy->_rightSuffix = _rightSuffix;
            copy->_columns = _columns;
            copy->setOutputSchema(getOutputSchema());
        }
        copy->_keyColumn = keyColumn();
        copy->_leftKeyIndex = _leftKeyIndex;
        copy->_rightKeyIndex = _rightKeyIndex;
        copy->_leftColumnCount = _leftColumnCount;
        copy->_rightColumnCount = _rightColumnCount;
        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        assert(checkBasicEqualityOfOperators(*copy, *this, true));
        return std::shared_ptr<LogicalOperator>(copy);
    }
}