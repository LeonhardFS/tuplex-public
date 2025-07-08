//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/execution/AggregateStage.h>
#include <logical/AggregateOperator.h>


namespace tuplex {

    AggregateStage::AggregateStage(tuplex::PhysicalPlan *plan, tuplex::IBackend *backend, tuplex::PhysicalStage *parent,
                                   const tuplex::AggregateType &at, int64_t stage_number, int64_t outputDataSetID)
            : PhysicalStage::PhysicalStage(plan,
                                           backend,
                                           stage_number,
                                           {parent}),
              _aggType(at),
              _outputDataSetID(outputDataSetID) {

    }

    std::shared_ptr<LogicalOperator> AggregateOperator::clone(bool cloneParents) const {
        // copy manually everything else over
        auto copy = new AggregateOperator();
        if(cloneParents)
            copy->setParent(parent()->clone());
        // members
        copy->_aggType = aggType();
        copy->_combiner = _combiner;
        copy->_aggregator = _aggregator;
        copy->_initialValue = _initialValue;
        copy->_keys = _keys;
        // only invoke for aggType() == AggregateType::AGG_BYKEY
        if(aggType() == AggregateType::AGG_BYKEY) {
            copy->_keyColsInParent = keyColsInParent();
        }

        copy->_keyType = keyType();
        copy->_aggregateOutputType = _aggregateOutputType;

//        // important to use here input column names, i.e. stored in base class UDFOperator!
//        auto copy = new AggregateOperator(cloneParents ? parent()->clone() : nullptr, aggType(),
//                                          _combiner, _aggregator, _initialValue, _keys);

        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return std::shared_ptr<LogicalOperator>(copy);
    }

    void hintTwoParamUDF(UDF& udf, const python::Type& a, const python::Type& b) {
        auto params = udf.getInputParameters();

        // there should be two params:
        if(params.size() != 2) {
            throw std::runtime_error("function " +
                                     (udf.isPythonLambda() ? udf.getCode() : udf.pythonFunctionName())
                                     + " has incompatible signature to be passed to aggregate function. "
                                       "Function has to have two parameters aggregate,row.");
        }

        // add type hints
        if(!udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({a, b})))) {
            // could not hint
            std::stringstream ss;
            ss << "Could not hint input schema " << std::get<0>(params[0]) << "=" << a.desc()
               << ", " <<
               std::get<0>(params[1]) << "=" << b.desc()
               << " to combiner UDF" << std::endl;
            throw std::runtime_error(ss.str());
        }
    }

    void retypeTwoParamUDF(UDF& udf, const python::Type& a, const python::Type& b) {
        auto params = udf.getInputParameters();

        // there should be two params:
        if(params.size() != 2) {
            throw std::runtime_error("function " +
                                     (udf.isPythonLambda() ? udf.getCode() : udf.pythonFunctionName())
                                     + " has incompatible signature to be passed to aggregate function. "
                                       "Function has to have two parameters aggregate,row.");
        }

        // retype using provided types
        auto new_row_type = python::Type::makeTupleType({a, b});
        udf.removeTypes(false);
        if(!udf.retype(new_row_type)) {
            // could not hint
            std::stringstream ss;
            ss << "Could not retype combiner UDF with " << std::get<0>(params[0]) << "=" << a.desc()
               << ", " <<
               std::get<0>(params[1]) << "=" << b.desc()
               << std::endl;
            throw std::runtime_error(ss.str());
        }
    }

    bool AggregateOperator::inferAndCheckTypes() {

        auto& logger = Logger::instance().defaultLogger();

        try {
            // aggregate type unique? -> simply take parent schema
            if(AggregateType::AGG_UNIQUE == aggType()) {
                setOutputSchema(parent()->getOutputSchema()); // inherit schema from parent
                // TODO: Can use Row construct here as well.
                _keyType = getOutputSchema().getRowType();
                _aggregateOutputType = python::Type::EMPTYTUPLE;
                return true;
            }

            // one empty? --> abort.
            if(_combiner.empty() || _aggregator.empty() || _initialValue == Row()) {
#ifndef NDEBUG
                if(AggregateType::AGG_UNIQUE != _aggType)
                    throw std::runtime_error("params invalid for aggregateType != unique");
#endif
                _combiner = UDF("", "");
                _aggregator = UDF("", "");
                _initialValue = Row();
                return true;
            }

            //  rewrite dict access in UDFs
            if(_aggregator.isCompiled()) {
                assert(_aggregator.getInputParameters().size() == 2);
                auto rowParam = std::get<0>(_aggregator.getInputParameters()[1]);

                // rewrite second param!
                _aggregator.rewriteDictAccessInAST(parent()->columns(), rowParam);
            }

            // check functions for compatibility:
            // i.e. do inference first, then deduce types.
            auto aggregateType = _initialValue.getRowType();

            // unpack one level if single param!
            if(aggregateType.parameters().size() == 1)
                aggregateType = aggregateType.parameters().front();

            _aggregateOutputType = aggregateType;

            // hint first the aggregator. It's output type an aggregateType need to be compatible.
            // if not - fail.
            // hint the aggregator function. It does have to have two input params too.
            auto rowtype = parent()->getOutputSchema().getRowType();
            if (rowtype.isRowType())
                rowtype = rowtype.get_columns_as_tuple_type();
            if(rowtype.parameters().size() == 1) // unpack one level
                rowtype = rowtype.parameters().front();
            hintTwoParamUDF(_aggregator, aggregateType, rowtype);
            logger.debug("aggregator output-schema is: " + _aggregator.getOutputSchema().getRowType().desc());

            if(Schema::UNKNOWN == _aggregator.getOutputSchema()) {
                throw std::runtime_error("failed to type aggregator function.");
            }

            // are they compatible?
            auto t_policy = TypeUnificationPolicy::defaultPolicy();
            t_policy.allowAutoUpcastOfNumbers = true;
            t_policy.unifyMissingDictKeys = true;
            auto aggregator_output_type = _aggregator.getOutputSchema().getRowType();
            if(aggregator_output_type.parameters().size() == 1)
               aggregator_output_type = aggregator_output_type.parameters().front();
            auto uni_type = unifyTypes(aggregateOutputType(), aggregator_output_type, t_policy);
            if(python::Type::UNKNOWN == uni_type) {
                logger.error("type of initial aggregate " + aggregateOutputType().desc() +
                " and output type of aggregator udf " + aggregator_output_type.desc() + " incompatible.");
                return false;
            }

            // different? update!
            if(aggregateOutputType() != uni_type) {
                logger.debug("updating aggregate type from " + aggregateType.desc()
                + " to " + uni_type.desc() + " due to output of aggregator udf.");
                aggregateType = uni_type;
                _aggregateOutputType = uni_type;

                // update aggregator func as well
                _aggregator.removeTypes();
                auto new_row_type = python::Type::makeTupleType({aggregateType, rowtype});
                if(!_aggregator.retype(new_row_type)) {
                    logger.error("could not infer type for aggregator udf with updated aggregate type " + aggregateType.desc());
                    return false;
                }
            }

            // type combiner now (with potentially updated output type)
            hintTwoParamUDF(_combiner, aggregateType, aggregateType);
            logger.debug("combiner output-schema is: " + _combiner.getOutputSchema().getRowType().desc());


            // check whether everything is compatible.
            // i.e. find out what the super type of everything is
            auto ctype = _combiner.getOutputSchema().getRowType();
            auto atype = _aggregator.getOutputSchema().getRowType();
            auto itype = _initialValue.getRowType();

            // @TODO: upcasting checks from tplx197...

            auto final_type = unifyTypes(ctype, unifyTypes(atype, itype, t_policy), t_policy);
            if(final_type == python::Type::UNKNOWN)
                throw std::runtime_error("incompatible types in aggregate operator");

            // aggregate by key needs to keep the key columns
            if(AggregateType::AGG_BYKEY == aggType()) {
                auto parent_row_type = parent()->getOutputSchema().getRowType();
                auto parent_row_types = parent_row_type.isRowType() ? parent_row_type.get_column_types() : parent_row_type.parameters();
                std::vector<python::Type> final_row_type;
                for(const auto &idx : keyColsInParent()) final_row_type.push_back(parent_row_types[idx]);
                // TODO(rahuly): should this be a recursive flatten?
                for(const auto &t : final_type.parameters()) final_row_type.push_back(t); // flatten the aggregate type
                final_type = python::Type::makeTupleType(final_row_type);
            }

            logger.debug("aggregate operator yields: " + final_type.desc());
            setOutputSchema(Schema(Schema::MemoryLayout::ROW, final_type));
            return true;
        } catch(std::exception& e) {
            logger.error("exception while inferring types in aggregate: " + std::string(e.what()));
            return false;
        }
    }

    bool AggregateOperator::retype(const RetypeConfiguration &conf) {

        auto& logger = Logger::instance().defaultLogger();

        logger.info("retyping aggregate operator with new input row type=" + conf.row_type.desc());

        // unique? no retype necessary, simply take over new row type.
        if(AggregateType::AGG_UNIQUE == aggType()) {
            setOutputSchema(Schema(Schema::MemoryLayout::ROW, conf.row_type)); // inherit schema from parent
            return true;
        }

        // others require retyping of aggregator etc.
        // -> means extracting part of row type to feed in the case of bykey
        try {
            // update key type from parent
            _keyType = keyTypeFromParent(); // <-- redundant? really need to store this?

            //  rewrite dict access in UDFs
            if(_aggregator.isCompiled()) {
                assert(_aggregator.getInputParameters().size() == 2);
                auto rowParam = std::get<0>(_aggregator.getInputParameters()[1]);

                // rewrite second param!
                _aggregator.rewriteDictAccessInAST(conf.columns, rowParam);
            }

            // check functions for compatibility:
            // i.e. do inference first, then deduce types.
            auto aggregateType = _initialValue.getRowType();

            // unpack one level if single param!
            if(aggregateType.parameters().size() == 1)
                aggregateType = aggregateType.parameters().front();

            _aggregateOutputType = aggregateType;

            // hint first the aggregator. It's output type an aggregateType need to be compatible.
            // if not - fail.
            // hint the aggregator function. It does have to have two input params too.
            auto rowtype = conf.row_type;
            if (rowtype.isRowType())
                rowtype = rowtype.get_columns_as_tuple_type();

            if(rowtype.parameters().size() == 1) // unpack one level
                rowtype = rowtype.parameters().front();
            retypeTwoParamUDF(_aggregator, aggregateType, rowtype);
            logger.debug("aggregator output-schema is: " + _aggregator.getOutputSchema().getRowType().desc());

            if(Schema::UNKNOWN == _aggregator.getOutputSchema()) {
                throw std::runtime_error("failed to type aggregator function.");
            }

            // are they compatible?
            auto t_policy = TypeUnificationPolicy::defaultPolicy();
            t_policy.allowAutoUpcastOfNumbers = true;
            t_policy.unifyMissingDictKeys = true;
            auto aggregator_output_type = _aggregator.getOutputSchema().getRowType();
            if(aggregator_output_type.parameters().size() == 1)
                aggregator_output_type = aggregator_output_type.parameters().front();
            auto uni_type = unifyTypes(aggregateOutputType(), aggregator_output_type, t_policy);
            if(python::Type::UNKNOWN == uni_type) {
                logger.error("type of initial aggregate " + aggregateOutputType().desc() +
                             " and output type of aggregator udf " + aggregator_output_type.desc() + " incompatible.");
                return false;
            }

            // different? update!
            if(aggregateOutputType() != uni_type) {
                logger.debug("updating aggregate type from " + aggregateType.desc()
                             + " to " + uni_type.desc() + " due to output of aggregator udf.");
                aggregateType = uni_type;
                _aggregateOutputType = uni_type;

                _aggregator.removeTypes();
                auto new_row_type = python::Type::makeTupleType({aggregateType, conf.row_type});
                if(!_aggregator.retype(new_row_type)) {
                    logger.error("could not retype aggregator udf with updated aggregate type " + aggregateType.desc());
                    return false;
                }
            }

            // type combiner now (with potentially updated output type)
            retypeTwoParamUDF(_combiner, aggregateType, aggregateType);
            logger.debug("combiner output-schema is: " + _combiner.getOutputSchema().getRowType().desc());

            // how


            // check whether everything is compatible.
            // i.e. find out what the super type of everything is
            auto ctype = _combiner.getOutputSchema().getRowType();
            auto atype = _aggregator.getOutputSchema().getRowType();
            auto itype = _initialValue.getRowType();

            // @TODO: upcasting checks from tplx197...

            auto final_type = unifyTypes(ctype, unifyTypes(atype, itype, t_policy), t_policy);
            if(final_type == python::Type::UNKNOWN)
                throw std::runtime_error("incompatible types in aggregate operator");

            // aggregate by key needs to keep the key columns.
            if(AggregateType::AGG_BYKEY == aggType()) {
                auto parent_row_types = conf.row_type.isRowType() ? conf.row_type.get_column_types() : conf.row_type.parameters();
                std::vector<python::Type> final_row_type;
                for(const auto &idx : keyColsInParent()) final_row_type.push_back(parent_row_types[idx]);
                // TODO(rahuly): should this be a recursive flatten?
                for(const auto &t : final_type.parameters()) final_row_type.push_back(t); // flatten the aggregate type
                final_type = python::Type::makeTupleType(final_row_type);
            }

            logger.debug("aggregate operator yields: " + final_type.desc());
            setOutputSchema(Schema(Schema::MemoryLayout::ROW, final_type));
            return true;
        } catch(std::exception& e) {
            logger.error("exception while retyping types in aggregate: " + std::string(e.what()));
            return false;
        }

        return false;
    }

    python::Type AggregateOperator::keyTypeFromParent() const {
        std::vector<python::Type> keyTypes;

        auto parent_row_type = parent()->getOutputSchema().getRowType();
        auto col_types_of_parent = parent_row_type.isRowType() ? parent_row_type.get_column_types() : parent_row_type.parameters();

        for(auto idx : _keyColsInParent) {
            keyTypes.push_back(col_types_of_parent[idx]);
        }
        auto keyType = python::Type::makeTupleType(keyTypes);
        if(keyType.parameters().size() == 1) keyType = keyType.parameters().front();
        return keyType;
    }

    std::vector<Row> AggregateOperator::aggByKeySample(size_t num) const {
        auto rows = parent()->getSample(num);

        // Hash rows by key.
        std::unordered_map<Row, Row> map;
        auto keys_in_parent = keyColsInParent();
        std::vector<Field> key_fields(keys_in_parent.size());

        auto agg_pickled_code = _aggregator.getPickledCode();
        auto combine_pickled_code = _combiner.getPickledCode();

        python::lockGIL();

        // deserialize functions.
        auto agg_func = python::deserializePickledFunction(python::getMainModule(), agg_pickled_code.c_str(), agg_pickled_code.length());
        auto combine_func = python::deserializePickledFunction(python::getMainModule(), combine_pickled_code.c_str(), combine_pickled_code.length());
        auto output_columns = columns();

        for (const auto &row : rows) {
            auto fields = row.to_vector();
            for (unsigned int i = 0; i < keys_in_parent.size(); i++)
                key_fields[i] = fields[keys_in_parent[i]];
            auto key_row = Row::from_vector(key_fields);

            // Check if key_row is contained, if not init with initial aggregate.
            auto it = map.find(key_row);
            PyObject* py_agg_value = nullptr;
            if (it == map.end()) {
                py_agg_value = python::rowToPython(_initialValue, true);
            } else {
                py_agg_value = python::rowToPython(it->second, true);
            }
            auto py_row = python::rowToPython(row, true);

            PyObject* py_args = PyTuple_New(2);
            PyTuple_SetItem(py_args, 0, py_agg_value);
            PyTuple_SetItem(py_args, 1, py_row);

            // // debug: print object.
            // Py_XINCREF(py_args);
            // std::cout<<"ROW:  "<<python::PyString_AsString(py_args)<<std::endl;

            // Run aggregator function.
            auto pcr = python::callFunctionEx(agg_func, py_args);

            if (pcr.exceptionCode != ExceptionCode::SUCCESS) {
                // fail...
            } else {
                auto res = python::pythonToRowWithDictUnwrap(pcr.res, output_columns);
                map[key_row] = res;
            }
        }

        // Of each result, combine at least once with neutral element.
        for (auto& kv: map) {
            auto py_agg_value = python::rowToPython(kv.second, true);
            auto py_agg_initial_value = python::rowToPython(_initialValue, true);

            PyObject* py_args = PyTuple_New(2);
            PyTuple_SetItem(py_args, 0, py_agg_value);
            PyTuple_SetItem(py_args, 1, py_agg_initial_value);

            auto pcr = python::callFunctionEx(combine_func, py_args);
            if (pcr.exceptionCode != ExceptionCode::SUCCESS) {
                // fail...
            } else {
                kv.second = python::pythonToRowWithDictUnwrap(pcr.res, output_columns);
            }
        }

        python::unlockGIL();

        // mash rows together.
        std::vector<Row> result;
        for (const auto& kv: map) {
            auto fields = kv.first.to_vector();
            auto fields_agg = kv.second.to_vector();
            std::copy(fields_agg.begin(), fields_agg.end(), std::back_inserter(fields));

            result.push_back(Row::from_vector(fields).with_columns(columns()));
        }
        return result;
    }

    std::vector<Row> AggregateOperator::aggUniqueSample(size_t num) const {
        auto rows = parent()->getSample(num);
        // Hash rows by key.
        std::unordered_map<Row, unsigned> map;
        for (const auto& row : rows)
            map[row]++;

        std::vector<Row> result;
        for (const auto& kv: map)
            result.push_back(kv.first);
        return result;
    }

}