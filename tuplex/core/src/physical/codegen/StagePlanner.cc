//
// Created by Leonhard Spiegelberg on 2/27/22.
//

#include <physical/codegen/StagePlanner.h>
#include <physical/codegen/AggregateFunctions.h>
#include <logical/CacheOperator.h>
#include <logical/JoinOperator.h>
#include <logical/FileInputOperator.h>
#include <JSONUtils.h>
#include <CSVUtils.h>
#include <Utils.h>
#include <logical/AggregateOperator.h>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <iostream>
#include <iterator>
#include <string_view>
#include <vector>
#include <physical/codegen/StageBuilder.h>
#include <graphviz/GraphVizBuilder.h>
#include "logical/LogicalOptimizer.h"
#include <visitors/ApplyVisitor.h>
#include <unordered_map>
#include <tracing/TraceVisitor.h>

#define VERBOSE_BUILD

namespace tuplex {
    namespace codegen {

        // check type.
        void checkRowType(const python::Type& rowType) {
            assert(rowType.isExceptionType() || rowType.isTupleType());
            if(rowType.isTupleType())
                for(auto t : rowType.parameters())
                    assert(t != python::Type::UNKNOWN);
        }

        template<typename T> std::vector<T> vec_prepend(const T& t, const std::vector<T>& v) {
            using namespace std;
            vector<T> res; res.reserve(v.size() + 1);
            res.push_back(t);
            for(const auto& el : v)
                res.push_back(el);
            return res;
        }

        std::vector<std::shared_ptr<LogicalOperator>> StagePlanner::filterReordering(const std::vector<Row>& sample) {
            // todo

            return vec_prepend(_inputNode, _operators);
        }


        python::Type StagePlanner::get_specialized_row_type(const std::shared_ptr<LogicalOperator>& inputNode, const DetectionStats& ds) const {
            assert(inputNode);

            auto& logger = Logger::instance().logger("specializing stage optimizer");

            logger.debug("output schema of input node is: " + inputNode->getOutputSchema().getRowType().desc());

            // check if inputNode is FileInput -> i.e. projection scenario!
            if(inputNode->type() == LogicalOperatorType::FILEINPUT) {
                auto fop = std::dynamic_pointer_cast<FileInputOperator>(inputNode);
                return ds.specialize_row_type(inputNode->getOutputSchema().getRowType());

//                auto pushed_down_output_row_type = fop->getOutputSchema().getRowType();
//                std::vector<python::Type> col_types = pushed_down_output_row_type.parameters();
//                auto num_cols_to_serialize = fop->outputColumnCount();
//                if(num_cols_to_serialize != ds.constant_row.getNumColumns()) {
//                    throw std::runtime_error("internal error in constant-folding optimization, original number of columns not matching detection columns");
//                } else {
//
//                    logger.debug("file input operator output type: " + fop->getOutputSchema().getRowType().desc());
//                    col_types = std::vector<python::Type>(num_cols_to_serialize, python::Type::NULLVALUE); // init as dummy nulls
//                    auto input_col_types = fop->getOutputSchema().getRowType().parameters();
//                    unsigned pos = 0;
//                    for(unsigned i = 0; i < cols_to_serialize.size(); ++i) {
//                        // to be serialized or not?
//                        if(cols_to_serialize[i]) {
//                            assert(pos < input_col_types.size());
//                            col_types[i] = input_col_types[pos++];
//                        }
//                    }
//                }
//                auto reprojected_output_row_type = python::Type::makeTupleType(col_types);
//                auto specialized_row_type = ds.specialize_row_type(reprojected_output_row_type);
//
//                // create projected version
//                col_types.clear();
//                for(unsigned i = 0; i < cols_to_serialize.size(); ++i) {
//                    // to be serialized or not?
//                    if(cols_to_serialize[i]) {
//                        col_types.push_back(specialized_row_type.parameters()[i]);
//                    }
//                }
//
//                auto projected_specialized_row_type = python::Type::makeTupleType(col_types);
//                return projected_specialized_row_type;
            } else {
                // simple, no reprojection done. Just specialize type.
                return ds.specialize_row_type(inputNode->getOutputSchema().getRowType());
            }
        }

        std::vector<std::string> get_column_names_from_sample(const std::vector<Row>& sample) {
            std::vector<std::string> names;
            std::unordered_set<std::string> names_seen;
            for(const auto& row : sample) {
                if(row.getRowType().isRowType()) {
                    for(const auto& name : row.getRowType().get_column_names()) {
                        if(names_seen.find(name) == names_seen.end()) {
                            names.push_back(name);
                            names_seen.insert(name);
                        }
                    }
                }
            }
            return names;
        }

        struct SparsifyInfo {
            std::vector<std::vector<access_path_t>> column_access_paths;
            std::vector<std::string> columns;

            inline std::vector<access_path_t> get_paths(const std::string& column) {
                auto idx = indexInVector(column, columns);
                if(idx >= 0)
                    return column_access_paths[idx];
                return {};
            }
        };

        void merge_access_paths(std::vector<access_path_t>& paths, const std::vector<access_path_t>& in_paths) {
            if(paths.empty()) {
                paths = in_paths;
                return;
            }

            // can use string rep as hash.
            std::set<std::string> hashes;
            for(auto path : paths)
                hashes.insert(access_path_to_str(path));

            // insert any paths that are not existing yet.
            for(auto path : in_paths) {
                auto key = access_path_to_str(path);
                if(hashes.find(key) == hashes.end()) {
                    paths.push_back(path);
                    hashes.insert(key); // avoids duplicates in in_paths;
                }
            }
        }

        /*!
         * remove existing annotations.
         * @param node
         */
        void reset_annotations(ASTNode* node) {
            ApplyVisitor av([](const ASTNode* node) { return true; }, [](ASTNode& node) {
                node.removeAnnotation();
            });
            node->accept(av);
        }

        bool StagePlanner::input_schema_contains_structs(const Schema& schema) {
            auto input_row_type_as_tuple = schema.getRowType();
            if(input_row_type_as_tuple.isRowType())
                input_row_type_as_tuple = input_row_type_as_tuple.get_columns_as_tuple_type();

            for(const auto& col_type : input_row_type_as_tuple.parameters())
                if(col_type.withoutOption().isStructuredDictionaryType() || col_type.withoutOption().isSparseStructuredDictionaryType())
                   return true;
            return false;
        }

        std::vector<std::shared_ptr<LogicalOperator>> StagePlanner::sparsifyStructs(std::vector<Row> sample,
                                                                                    const option<std::vector<std::string>>& sample_columns,
                                                                                    bool relax_for_sample) {
            using namespace std;
            vector<shared_ptr<LogicalOperator>> opt_ops; // the operators to return.

            std::stringstream os; // output stream where to capture optimization details.

            auto& logger = Logger::instance().logger("specializing stage optimizer");

            // check current input node schema, if it contains no struct_dict - can skip.
            if(!_inputNode) {
                logger.error("internal problem with _inputNode, skipping.");
                return vec_prepend(_inputNode, _operators);
            }

            if(sample.empty()) {
                logger.info("Empty sample, skipping sparsifyStructs optimization.");
                return vec_prepend(_inputNode, _operators);
            }

            // TODO: could sparsify within operators as well.

            auto struct_dict_found = input_schema_contains_structs(_inputNode->getOutputSchema());

            if(!struct_dict_found) {
                logger.error("Skipping sparsify-struct pass, because no struct dicts found in input operator.");
                return vec_prepend(_inputNode, _operators);
            }

            // get column names from sample.
            auto column_names = get_column_names_from_sample(sample);

            // check with provided values.
            if(sample_columns.has_value())
                column_names = sample_columns.value();

#ifndef NDEBUG
            // check that counts match.
            {
                auto n_columns = column_names.size();
                for(const auto& row : sample) {
                    if(row.getRowType().isRowType())
                        assert(n_columns >= extract_columns_from_type(row.getRowType()));
                    else
                        assert(n_columns == extract_columns_from_type(row.getRowType()));
                }
            }
#endif

            std::vector<PyObject*> python_sample;

            // go through operators with sample, and then for each record which paths are accessed:
            std::unordered_map<LogicalOperator*, SparsifyInfo> info_map;
            bool done=false;
            for(const auto& op : vec_prepend(_inputNode, _operators)) {
                if(done)
                    break;

                switch(op->type()) {
                    case LogicalOperatorType::FILEINPUT: {
                        stringstream ss;
                        ss<<"Found file input parameter, processing sample: ";
                        logger.debug(ss.str());
                        break;
                    }
                    case LogicalOperatorType::MAP:
                    case LogicalOperatorType::FILTER: {
                        auto udfop = std::dynamic_pointer_cast<UDFOperator>(op);
                        auto func_root = udfop->getUDF().getAnnotatedAST().getFunctionAST();
                        assert(func_root);

                        reset_annotations(func_root);

                        // Trace now which columns are accessed.
                        // convert to python.
                        python::lockGIL();
                        python_sample.clear();
                        for(auto row : sample) {
                            // row = row.with_columns(column_names);
                            // reorder_and_fill_missing_will_null(row,
                            //                                   row.getRowType().get_column_names(),
                            //                                   column_names);
                            python_sample.push_back(python::rowToPython(row));
                        }

                        // Trace
                        TraceVisitor tv;
                        for (unsigned i = 0; i < sample.size(); ++i) {
                            auto py_object = python_sample[i];
                            tv.recordTrace(func_root, py_object, column_names);
                        }

                        python::unlockGIL();

                        // results, and print them out:
                        auto column_access_paths = tv.columnAccessPaths();

                        info_map[udfop.get()].columns = tv.columns();
                        info_map[udfop.get()].column_access_paths = tv.columnAccessPaths();

                        // check which names are accessed:
                        auto columns = tv.columns();
                        int num_accessed = 0;
                        for (unsigned i = 0; i < columns.size(); ++i) {
                            if (!column_access_paths[i].empty()) {
                                os << "Access paths for column: " << columns[i] << "\n";
                                for (auto path: column_access_paths[i])
                                    os << " -- " << access_path_to_str(path) << "\n";
                                os << std::endl;
                                num_accessed++;
                            }
                        }
                        os << num_accessed << "/" << pluralize(columns.size(), "column") << " accessed." << endl;

                        if(op->type() == LogicalOperatorType::MAP) {
                            // processing stops after the first map operator encountered.
                            done=true;
//                            throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " Need to update sample for map operator.");
                        }
                        break;
                    }
                    case LogicalOperatorType::WITHCOLUMN: {
                        // apply same tracing logic.
                        auto wop = std::dynamic_pointer_cast<WithColumnOperator>(op);
                        auto func_root = wop->getUDF().getAnnotatedAST().getFunctionAST();
                        assert(func_root);

                        reset_annotations(func_root);

                        // Trace now which columns are accessed.
                        // convert to python.
                        python::lockGIL();
                        python_sample.clear();
                        for(const auto& row : sample) {
                            // row = row.with_columns(column_names);
                            // reorder_and_fill_missing_will_null(row,
                            //                                   row.getRowType().get_column_names(),
                            //                                   column_names);
                            python_sample.push_back(python::rowToPython(row));
                        }

                        // Trace
                        TraceVisitor tv;
                        std::vector<Field> results;
                        for (unsigned i = 0; i < sample.size(); ++i) {
                            auto py_object = python_sample[i];
                            tv.recordTrace(func_root, py_object, column_names);
                            auto field = python::pythonToField(tv.lastResult());
                            results.push_back(field);
                        }

                        python::unlockGIL();

                        // results, and print them out:
                        auto column_access_paths = tv.columnAccessPaths();

                        info_map[wop.get()].columns = tv.columns();
                        info_map[wop.get()].column_access_paths = tv.columnAccessPaths();

                        os<<"Sparsify struct tracing results for operator "<<op->name()<<"::\n";

                        os<<"UDF:\n"<<wop->getUDF().getCode()<<endl<<endl;

                        // check which names are accessed:
                        auto columns = tv.columns();
                        int num_accessed = 0;
                        for (unsigned i = 0; i < columns.size(); ++i) {
                            if (!column_access_paths[i].empty()) {
                                os << "Access paths for column: " << columns[i] << "\n";
                                for (auto path: column_access_paths[i])
                                    os << " -- " << access_path_to_str(path) << "\n";
                                os << std::endl;
                                num_accessed++;
                            }
                        }
                        os << num_accessed << "/" << pluralize(columns.size(), "column") << " accessed." << endl;

                        // update sample (and column names!)
                        if(wop->creates_new_column()) {
                            // go through python result objects.
                            assert(results.size() == sample.size());
                            column_names.push_back(wop->columnToMap());
                            for(unsigned i = 0; i < sample.size(); ++i) {
                                auto field = results[i];
                                auto fields = sample[i].to_vector();
                                fields.push_back(field);
                                sample[i] = Row::from_vector(fields);
                            }
                        } else {
                            throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " update sample for withColumn in sparsifyStructs.");
                        }

                        break;
                    }
                    default: {
                        throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " unknown operator " + op->name() + " in sparsifyStructs.");
                    }
                }
            }

            // for each operator, it is now known which access paths there are.
            // next step is to (by reversing) identify the file operators valid paths in order to sparsify.
            std::vector<LogicalOperator*> r_operators;
            for(const auto& op : vec_prepend(_inputNode, _operators))
                r_operators.push_back(op.get());
            std::reverse(r_operators.begin(), r_operators.end());

            // retrieve for input operator which columns are accessed how.
            std::unordered_map<std::string, std::vector<access_path_t>> column_to_access_path_map;
            for(const auto& op : r_operators) {
                if(info_map.find(op) != info_map.end()) {

                    auto info = info_map[op];

                    switch(op->type()) {
                        case LogicalOperatorType::WITHCOLUMN: {
                            auto wop = static_cast<WithColumnOperator*>(op);

                            // is the new column already contained? then remove.
                            if(wop->creates_new_column() && column_to_access_path_map.find(wop->columnToMap()) != column_to_access_path_map.end()) {
                                column_to_access_path_map.erase(wop->columnToMap());
                            } else {
                                // replace access (override column)
                                column_to_access_path_map[wop->columnToMap()] = info_map[op].get_paths(wop->columnToMap());
                            }

                            // merge paths for all other columns.
                            for(unsigned i = 0; i < info.columns.size(); ++i) {
                                // handled above.
                                if(info.columns[i] == wop->columnToMap())
                                    continue;

                                if(!info.column_access_paths[i].empty()) {
                                    merge_access_paths(column_to_access_path_map[info.columns[i]], info.column_access_paths[i]);
                                }
                            }

                            break;
                        }
                        case LogicalOperatorType::FILTER: {
                            // merge paths for all other columns.
                            for(unsigned i = 0; i < info.columns.size(); ++i) {
                                 if(!info.column_access_paths[i].empty()) {
                                    merge_access_paths(column_to_access_path_map[info.columns[i]], info.column_access_paths[i]);
                                }
                            }

                            break;
                        }
                        case LogicalOperatorType::MAP: {
                            // reset access paths.
                            column_to_access_path_map = {};

                            // merge paths for all other columns.
                            for(unsigned i = 0; i < info.columns.size(); ++i) {
                                if(!info.column_access_paths[i].empty()) {
                                    merge_access_paths(column_to_access_path_map[info.columns[i]], info.column_access_paths[i]);
                                }
                            }
                            break;
                        }
                        default: {
                            throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " unknown operator " + op->name() + " in sparsifyStructs.");
                        }
                    }
                }
            }

            // erase now all columns which aren't present in input operator
            std::vector<std::string> columns_to_remove;
            for(const auto& kv : column_to_access_path_map) {
                if(indexInVector(kv.first, _inputNode->columns()) < 0)
                    columns_to_remove.push_back(kv.first);
            }
            for(auto name : columns_to_remove)
                column_to_access_path_map.erase(name);

            // sparsify now.
            auto r_row_type = _inputNode->getOutputSchema().getRowType();
            std::vector<std::vector<access_path_t>> r_access_paths;
            for(auto name : _inputNode->columns()) {
                r_access_paths.push_back({});
                if(column_to_access_path_map.find(name) != column_to_access_path_map.end()) {
                    r_access_paths.back() = column_to_access_path_map[name];
                }
            }

            auto sparse_type = sparsify_and_project_row_type(r_row_type, r_access_paths);

            os<<"Input row type before sparsification:\n"<<r_row_type.desc()<<endl;
            os<<"Input row type after sparsification:\n"<<sparse_type.desc()<<endl;


            // if desired, relax type to make sure sample passes.
            if(relax_for_sample) {

                std::vector<std::pair<int, int>> indices;
                int pos = 0;
                for(auto name : sparse_type.get_column_names()) {
                    if(sparse_type.get_column_type(pos).withoutOption().isStructuredDictionaryType() || sparse_type.get_column_type(pos).isSparseStructuredDictionaryType()) {
                        indices.push_back(make_pair(pos, indexInVector(name, sample_columns.data())));
                        assert(indices.back().second >= 0);
                    }
                    pos++;
                }

                auto before_type = sparse_type;

                for(const auto& row : sample) {
                    // for each sparse struct in row_type check whether it passes or not, if not -> change from NOT_PRESENT or ALWAYS_PRESENT to MAYBE_PRESENT.
                    // now check sparse types for fields to keep
                    for(auto p : indices) {
                        auto i = p.first;
                        auto idx = p.second;
                        auto field = row.get(idx);
                        if(!field.isNull() && field.getType().withoutOption().isStructuredDictionaryType()) {

                            // TODO: better struct/JSON type matching here...

                            auto column_name = sparse_type.get_column_name(i);

                            // different type?
                            auto expected_type = sparse_type.get_column_type(i);
                            bool is_option = expected_type.isOptionType();
                            expected_type = expected_type.withoutOption();
                            auto expected_pairs = expected_type.get_struct_pairs();
                            if(field.getType().withoutOption() != expected_type) {

                                if(expected_type.isStructuredDictionaryType()) {
                                    // full check:
                                    throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " not yet implemented");
                                } else {
                                    assert(expected_type.isSparseStructuredDictionaryType());
                                    // partial check:

                                    bool relaxation_found = false;

                                    // i.e., all always present/not present in sparse dict must be ok.
                                    for(auto& kv_pair : expected_pairs) {
                                        if(kv_pair.presence == python::MAYBE_PRESENT)
                                            continue;

                                        access_path_t path{make_pair(kv_pair.key, kv_pair.keyType)};
                                        auto field_present = is_field_present(field, path);

                                        if(kv_pair.presence == python::ALWAYS_PRESENT) {
                                            // ensure that kv_pair is present, else relax to maybe present.
                                            if(!field_present) {
                                                os<<column_name<<": Path "<<access_path_to_str(path)<<" not present, relax struct pair for key "<<kv_pair.key<<" to maybe present"<<endl;
                                                relaxation_found = true;
                                                kv_pair = python::StructEntry(kv_pair.key, kv_pair.keyType, kv_pair.valueType, python::MAYBE_PRESENT);
                                            }
                                        } else if(kv_pair.presence == python::NOT_PRESENT) {
                                            // ensure that kv_pair is not present, else relax to maybe present.
                                            if(field_present) {

                                                // NOT_PRESENT uses often ~> PYOBJECT. Yet this can get compiled to => PYOBJECT easily.
                                                // infer therefore the type from the field.
                                                auto value_type = kv_pair.valueType;
                                                if(python::Type::PYOBJECT == value_type) {
                                                    // find from field the type
                                                    value_type = python::Type::UNKNOWN;
                                                    try {
                                                        auto field_element = get_struct_field_by_path(field, path);
                                                        if(field_element.getType() != python::Type::PYOBJECT)
                                                            value_type = field_element.getType();
                                                    } catch(const std::exception& path) {
                                                        //... nothing, simply silent exception.
                                                    }
                                                }

                                                if(value_type != python::Type::UNKNOWN) {
                                                    os<<column_name<<": Path "<<access_path_to_str(path)<<" present, relax struct pair for key "<<kv_pair.key<<" to maybe present"<<endl;
                                                    relaxation_found = true;
                                                    kv_pair = python::StructEntry(kv_pair.key, kv_pair.keyType, value_type, python::MAYBE_PRESENT);
                                                }
                                            } else {
                                                //// check that not is pyobject. If not simplify.
                                                //if(kv_pair.valueType != python::Type::PYOBJECT)
                                                //    kv_pair = python::StructEntry(kv_pair.key, kv_pair.keyType, python::Type::PYOBJECT, python::NOT_PRESENT);
                                            }
                                        }
                                    }

                                    // relaxation found? => update sparse type!
                                    if(relaxation_found) {
                                        auto new_expected_type = python::Type::makeStructuredDictType(expected_pairs, expected_type.isSparseStructuredDictionaryType());

                                        // If previous type was identified as optional, carry over here.
                                        if(is_option)
                                            new_expected_type = python::Type::makeOptionType(new_expected_type);

                                        // create new sparse type
                                        auto column_names = sparse_type.get_column_names();
                                        auto column_types = sparse_type.get_column_types();
                                        column_types[i] = new_expected_type;
                                        sparse_type = python::Type::makeRowType(column_types, column_names);
                                    }

                                }

                            } else {
                                // all good, type match.
                            }
                        }
                    }
                }

                if(before_type != sparse_type) {
                    os<<"Relaxed row type from\n"<<before_type.desc()<<" to \n"<<sparse_type.desc()<<endl;
                }

            }

            // Overwrite input node with new sparse type & propagate through operators.
            auto inputNode = _inputNode->clone(false);
            RetypeConfiguration conf;
            conf.row_type = sparse_type;
            if(sparse_type.isRowType()) {
                conf.row_type = sparse_type.get_columns_as_tuple_type();
                conf.columns = sparse_type.get_column_names();
            }
            conf.is_projected = true;

            // project columns, also need to update check indices (checks refer to output column indices).
            if(!conf.columns.empty()) {
                // !!! use here input columns, not output columns.
                auto columns_before = inputNode->inputColumns(); // output columns.

                // step 1: validate checks
                for(const auto& check : _checks) {
                    for(auto idx : check.colNos)
                        if(idx >= columns_before.size())
                            throw std::runtime_error("Check " + check.to_string() + " has invalid index " + std::to_string(idx));
                }

                // step 2: reselect relevant columns (i.e. perform selection pushdown).
                std::dynamic_pointer_cast<FileInputOperator>(inputNode)->selectColumns(conf.columns);

                // step 3: update checks (their indices) to new columns
                auto columns_after = inputNode->inputColumns();
                for(auto& check : _checks) {
                    for(auto& idx : check.colNos) {
                        auto new_idx = indexInVector(columns_before[idx], columns_after);
                        if(new_idx < 0) {
                            std::stringstream ss;
                            ss<<"Check "<<check.to_string()<<" has invalid index "
                              <<new_idx<<", required column "<<columns_before[idx]
                              <<" can not be found in newly selected columns "<<columns_after;
                            throw std::runtime_error(ss.str());
                        }
                        idx = new_idx; // this should update it!
                    }
                }

                // step 4: validate current checks again, this time against columns_after.
                for(const auto& check : _checks) {
                    for(auto idx : check.colNos)
                        if(idx >= columns_after.size())
                            throw std::runtime_error("Check " + check.to_string() + " has invalid index " + std::to_string(idx));
                }
            }

            inputNode->retype(conf);
            std::dynamic_pointer_cast<FileInputOperator>(inputNode)->useNormalCase();
            opt_ops.push_back(inputNode);

            assert(std::dynamic_pointer_cast<FileInputOperator>(inputNode)->storedSampleRowCount() == sample.size()); // this should match, indeed the sample should be identical...

            auto lastParent = inputNode;
            // retype the other operators.
            for(const auto& op : _operators) {
                // clone operator & specialize!
                auto opt_op = op->clone(false);
                opt_op->setParent(lastParent);
                opt_op->setID(op->getID());
                if(hasUDF(opt_op.get())) {
                    // important.
                    reset_annotations(std::dynamic_pointer_cast<UDFOperator>(opt_op)->getUDF().getAnnotatedAST().getFunctionAST());
                }

                // before row type
                std::stringstream ss;
                ss<<op->name()<<" (before): "<<op->getInputSchema().getRowType().desc()<<" -> "<<op->getOutputSchema().getRowType().desc()<<endl;
                // retype
                ss<<"retyping with parent's output schema: "<<lastParent->getOutputSchema().getRowType().desc()<<endl;
                opt_op->retype(lastParent->getOutputSchema().getRowType(), true);
                // after retype
                ss<<opt_op->name()<<" (after): "<<opt_op->getInputSchema().getRowType().desc()<<" -> "<<opt_op->getOutputSchema().getRowType().desc();
                logger.debug(ss.str());
                opt_ops.push_back(opt_op);
                lastParent = opt_op;
            }

            return opt_ops;
        }

        size_t struct_field_count(const python::Type& t, bool recurse =true) {
            if(!t.isStructuredDictionaryType() && !t.isSparseStructuredDictionaryType())
                return 1;

            if(!recurse)
                return t.get_struct_pairs().size();

            size_t field_count = 0;
            for(auto kv_pair : t.get_struct_pairs()) {
                field_count += struct_field_count(kv_pair.valueType.withoutOption(), recurse);
            }
            return field_count;
        }

        std::vector<std::shared_ptr<LogicalOperator>> StagePlanner::simplifyLargeStructs(size_t max_field_count) {
            using namespace std;

            auto& logger = Logger::instance().logger("specializing stage optimizer");

            // check whether input node contains any struct types.
            auto struct_dict_found = input_schema_contains_structs(_inputNode->getOutputSchema());

            if(!struct_dict_found) {
                logger.info("Skipping simplify-large-structs pass, because no struct dicts found in input operator.");
                return vec_prepend(_inputNode, _operators);
            }

            // structs are found, for each struct count now depth and then simplify
            auto columns = _inputNode->columns();
            auto tuple_row_type = _inputNode->getOutputSchema().getRowType();
            if(tuple_row_type.isRowType())
                tuple_row_type = tuple_row_type.get_columns_as_tuple_type();
            assert(columns.size() == tuple_row_type.parameters().size());

            auto col_types = tuple_row_type.parameters();
            for(unsigned i = 0; i < columns.size(); ++i) {
                auto original_col_type = col_types[i];
                auto col_type = original_col_type.withoutOption();
                if(col_type.isStructuredDictionaryType() || col_type.isSparseStructuredDictionaryType()) {
                    auto col_field_count = struct_field_count(col_type);
                    std::stringstream ss;
                    ss<<"Column "<<columns[i]<<": "<<pluralize(col_field_count, "field");
                    if(col_field_count > max_field_count) {
                        ss<<" --> exceeds maximum field count of "<<max_field_count<<", substituting with generic dict.";
                        if(original_col_type.isOptionType()) {
                            col_types[i] = python::Type::makeOptionType(python::Type::GENERICDICT);
                        } else {
                            col_types[i] = python::Type::GENERICDICT;
                        }
                    }
                    logger.info(ss.str());
                }
            }

            // retype if different!
            auto new_row_type = python::Type::makeRowType(col_types, columns);
            if(new_row_type.get_columns_as_tuple_type() != tuple_row_type) {
                // TOOD: retype.

                // Overwrite input node with new sparse type & propagate through operators.
                auto inputNode = _inputNode->clone(false);
                RetypeConfiguration conf;
                conf.row_type = new_row_type;
                if(new_row_type.isRowType()) {
                    conf.row_type = new_row_type.get_columns_as_tuple_type();
                    conf.columns = new_row_type.get_column_names();
                }
                conf.is_projected = true;

                // no columns change (only their types), so no need to reproject checks.
                std::vector<std::shared_ptr<LogicalOperator>> opt_ops;
                inputNode->retype(conf);
                std::dynamic_pointer_cast<FileInputOperator>(inputNode)->useNormalCase();
                opt_ops.push_back(inputNode);

                auto lastParent = inputNode;
                // retype the other operators.
                for(const auto& op : _operators) {
                    // clone operator & specialize!
                    auto opt_op = op->clone(false);
                    opt_op->setParent(lastParent);
                    opt_op->setID(op->getID());
                    if(hasUDF(opt_op.get())) {
                        // important.
                        reset_annotations(std::dynamic_pointer_cast<UDFOperator>(opt_op)->getUDF().getAnnotatedAST().getFunctionAST());
                    }

                    // before row type
                    std::stringstream ss;
                    ss<<op->name()<<" (before): "<<op->getInputSchema().getRowType().desc()<<" -> "<<op->getOutputSchema().getRowType().desc()<<endl;
                    // retype
                    ss<<"retyping with parent's output schema: "<<lastParent->getOutputSchema().getRowType().desc()<<endl;
                    opt_op->retype(lastParent->getOutputSchema().getRowType(), true);
                    // after retype
                    ss<<opt_op->name()<<" (after): "<<opt_op->getInputSchema().getRowType().desc()<<" -> "<<opt_op->getOutputSchema().getRowType().desc();
                    logger.debug(ss.str());
                    opt_ops.push_back(opt_op);
                    lastParent = opt_op;
                }

                return opt_ops;
            }

            // no change.
            return vec_prepend(_inputNode, _operators);
        }

        std::vector<std::shared_ptr<LogicalOperator>> StagePlanner::applyLogicalOptimizer() {
            using namespace std;

            auto& logger = Logger::instance().logger("specializing stage optimizer");

            if(!_inputNode || !_inputNode->isDataSource()) {
                logger.info("Skipping apply-logical-optimizer optimization because no data source found.");
                // no change.
                return vec_prepend(_inputNode, _operators);
            }

            auto opt_ops = vec_prepend(_inputNode, _operators);

            // Init logical optimizer.
            // Data source may require fewer columns to get accessed.
            //    --> perform projection pushdown and then eliminate as many checks as possible.
            auto accessed_columns_before_opt = get_accessed_columns(opt_ops);

            // basically use just on this stage the logical optimization pipeline
            auto logical_opt = std::make_unique<LogicalOptimizer>(options());

            // logically optimize pipeline, this performs reordering/projection pushdown etc.
            opt_ops = logical_opt->optimize(opt_ops, true); // inplace optimization

            std::vector<size_t> accessed_columns = get_accessed_columns(opt_ops);

            // so accCols should be sorted, now map to columns. Then check the position in accColsBefore
            // => look up what the original cols were!
            // => then push that down to reader/input node!
            std::vector<size_t> out_accessed_columns;
            std::vector<size_t> out_accessed_columns_before_opt;
            // project each when input op present
            if(_inputNode && _inputNode->type() == LogicalOperatorType::FILEINPUT) {
                auto fop = std::dynamic_pointer_cast<FileInputOperator>(_inputNode);
                for(const auto& idx : accessed_columns)
                    out_accessed_columns.emplace_back(fop->projectReadIndex(idx));
                for(const auto& idx : accessed_columns_before_opt)
                    out_accessed_columns_before_opt.emplace_back(fop->projectReadIndex(idx));
            } else {
                out_accessed_columns = accessed_columns;
                out_accessed_columns_before_opt = accessed_columns_before_opt;
            }
            _normalToGeneralMapping = createNormalToGeneralMapping(out_accessed_columns, out_accessed_columns_before_opt);

            {
                // print out new chain of types
                std::stringstream ss;
                ss<<"Pipeline (types):\n";

                for(const auto& op : opt_ops) {
                    ss<<op->name()<<":\n";
                    ss<<"  in: "<<op->getInputSchema().getRowType().desc()<<"\n";
                    ss<<" out: "<<op->getOutputSchema().getRowType().desc()<<"\n";
                    ss<<"  in columns: "<<op->inputColumns()<<"\n";
                    ss<<" out columns: "<<op->columns()<<"\n";
                    ss<<"----\n";
                }

                logger.debug(ss.str());
            }

            // Project checks (because columns may have been removed)
            // check which input columns are required and remove checks. --> this requires to use ORIGINAL indices.
            // --> this requires pushdown to work before!
            auto acc_cols = accessed_columns_before_opt;
            std::vector<NormalCaseCheck> projected_checks;

            // reproject checks, get for this actual input columns of operator.
            auto current_input_columns = opt_ops.front()->inputColumns();
            auto checks = _checks;
            for(auto check : checks) {
                if(!check.colNames.empty()) {
                    check.colNos.clear();
                    for(auto name : check.colNames) {
                        check.colNos.push_back(indexInVector(name, current_input_columns));
                    }
                }
                projected_checks.push_back(check);
            }

            logger.debug("normal case detection requires "
                         + pluralize(checks.size(), "check")
                         + ", given current logical optimizations "
                         + pluralize(projected_checks.size(), "check")
                         + " are required to detect normal case.");


            {
                std::stringstream ss;
                ss<<"pipeline post-logical optimization requires now only "<<pluralize(accessed_columns.size(), "column");
                ss<<" (general: "<<pluralize(accessed_columns_before_opt.size(), "column")<<")";
                ss<<", reduced checks from "<<checks.size()<<" to "<<projected_checks.size();
                logger.debug(ss.str());

                ss.str("normal col to general col mapping:\n");
                for(auto kv : _normalToGeneralMapping) {
                    ss<<kv.first<<" -> "<<kv.second<<"\n";
                }
                logger.debug(ss.str());
            }

            // Add all projected checks.
            _checks.clear();
            for(const auto& check : projected_checks)
                _checks.push_back(check);

            // no change.
            return vec_prepend(_inputNode, _operators);
        }

        python::Type simplify_constant_single_types(const python::Type& tuple_type) {
            assert(tuple_type.isTupleType());

            static std::vector<python::Type> single_types{python::Type::EMPTYTUPLE, python::Type::EMPTYDICT, python::Type::EMPTYLIST, python::Type::EMPTYSET};

            auto col_types = tuple_type.parameters();
            for(auto& type : col_types) {
                if(type.isConstantValued()) {
                    for(auto single_type : single_types)
                        if(type.underlying() == single_type) {
                            type = single_type;
                            break;
                        }
                }
            }
            return python::Type::makeTupleType(col_types);
        }

        std::vector<std::shared_ptr<LogicalOperator>> StagePlanner::constantFoldingOptimization(const std::vector<Row>& sample, const std::vector<std::string>& sample_columns) {
            using namespace std;
            vector<shared_ptr<LogicalOperator>> opt_ops;

            auto& logger = Logger::instance().logger("specializing stage optimizer");

            // at least 10 samples required to be somehow reliable (better: 100)
            // for sure exclude the case of 1 row directly.
            static size_t MINIMUM_SAMPLES_REQUIRED = 10;

            if(!_useConstantFolding || sample.size() < MINIMUM_SAMPLES_REQUIRED || sample.size() <= 1) {
                if(sample.size() < MINIMUM_SAMPLES_REQUIRED)
                    logger.warn("not enough samples to reliably apply constant folding optimization, consider increasing sample size.");
                 return vec_prepend(_inputNode, _operators);
            }

            // should have at least 100 samples to determine this...
            // check which columns could be constants and if so propagate that information!
            logger.info("Performing constant folding optimization (sample size=" + std::to_string(sample.size()) + ")");

            // first, need to detect which columns are required. This is important because of the checks.
            auto acc_cols_before_opt = this->get_accessed_columns();
            {
                std::string col_str = "";
                for(auto idx : acc_cols_before_opt) {
                    col_str += std::to_string(idx) + " ";
                }
                logger.info("accessed columns before folding: " + col_str);
            }


            // detect constants over sample.
            // note that this sample is WITHOUT any projection pushdown, i.e. full columns
            DetectionStats ds;
            ds.detect(sample);
            assert(!sample_columns.empty());

            {
                assert(!sample.empty());
                std::stringstream ss;
                ss<<"sample has "<<pluralize(sample.size(), "row")<<" rows with "<<pluralize(sample.front().getNumColumns(), "column");
                logger.debug(ss.str());
            }

            // no column constants? skip optimization!
            if(ds.constant_column_indices().empty()) {
                logger.debug("skipping constant folding optimization, no constants detected.");
                return vec_prepend(_inputNode, _operators);
            }

            // clone input operator
            auto inputNode = _inputNode ? _inputNode->clone() : nullptr;
            if(inputNode) {
                inputNode->setID(_inputNode->getID());
                if(inputNode->type() == LogicalOperatorType::FILEINPUT)
                    std::dynamic_pointer_cast<FileInputOperator>(inputNode)->cloneCaches(*((FileInputOperator*)_inputNode.get()));
            }


            {
                // print info
                std::stringstream ss;
                ss<<"Identified "<<pluralize(ds.constant_column_indices().size(), "column")<<" to be constant: "<<ds.constant_column_indices()<<endl;

                // print out which rows are considered constant (and with which values!)
                // --> note that these checks are done POST initial pushdown.
                // i.e. use output columns. (previously: input columns)
                for(auto idx : ds.constant_column_indices()) {
                    string column_name;
                    if(inputNode && !inputNode->columns().empty())
                        column_name = inputNode->columns()[idx];
                    ss<<" - "<<column_name<<": "<<ds.constant_row.get(idx).desc()<<" : "<<ds.constant_row.get(idx).getType().desc()<<endl;
                }
                logger.debug(ss.str());
            }

            // generate checks for all the original column indices that are constant
            vector<size_t> checks_indices = ds.constant_column_indices();
            vector<NormalCaseCheck> checks;
            auto unprojected_row_type = unprojected_optimized_row_type();
            if(inputNode) {
                for(auto idx : checks_indices) {
                    auto underlying_type = ds.constant_row.getType(idx);
                    auto underlying_constant = ds.constant_row.get(idx).desc();
                    auto constant_type = python::Type::makeConstantValuedType(underlying_type, underlying_constant);

                    constant_type = simplifyConstantType(constant_type);

                    // check if empty tuple, list, dict, ... -> skip. Parser will handle this.
                    if(constant_type.isConstantValued() && (constant_type.underlying() == python::Type::EMPTYDICT || constant_type.underlying() == python::Type::EMPTYTUPLE
                    || constant_type.underlying() == python::Type::EMPTYLIST || constant_type.underlying() == python::Type::EMPTYSET))
                        continue;

                    // which column is considered constant.
                    auto column_name = sample_columns[idx];

                    // original index is from ALL available rows in input node
                    size_t original_idx = 0;
                    if(inputNode->type() == LogicalOperatorType::FILEINPUT)
                        original_idx = std::dynamic_pointer_cast<FileInputOperator>(inputNode)->reverseProjectToReadIndex(
                                idx);
                    else {
                        // no project on other operators.
                        original_idx = idx;
                        auto out_column_count = inputNode->getOutputSchema().getRowType().parameters().size();
                        assert(original_idx <= out_column_count);
                    }

                    // null-checks handled separately, do not add them
                    // if null-value optimization has been already performed.
                    // --> i.e., they're done using the schema (?)
                    assert(original_idx < extract_columns_from_type(unprojected_row_type));
                    auto opt_schema_col_type = unprojected_row_type.isRowType() ? unprojected_row_type.get_column_type(original_idx) : unprojected_row_type.parameters()[original_idx];
                    if(constant_type == python::Type::NULLVALUE && opt_schema_col_type == python::Type::NULLVALUE) {
                        // skip
                    } else {
                        if(constant_type.isConstantValued()) {
                            auto check = NormalCaseCheck::ConstantCheck(original_idx, constant_type);
                            check.colNames.push_back(column_name);
                            checks.emplace_back(check);
                        } else if(constant_type == python::Type::NULLVALUE) {
                            auto check = NormalCaseCheck::NullCheck(original_idx);
                            check.colNames.push_back(column_name);
                            checks.emplace_back(check);
                        } else {
                            logger.error("invalid constant type to check for: " + constant_type.desc());
                        }
                    }
                }
                logger.debug("generated " + pluralize(checks.size(), "check") + " for stage");
            } else {
                logger.debug("skipped check generation, because input node is empty.");
            }

            // folding is done now in two steps:
            // 1. propagate the new input type through the ASTs, i.e. make certain fields constant
            //    this will help to drop potentially columns!

            // first issue is, stage could have been already equipped with projection pushdown --> need to reflect this
            // => get mapping from original to pushed down for input types.
            // i.e. create dummy and fill in
            auto projected_specialized_row_type = get_specialized_row_type(inputNode, ds);

            // simplify things like _Constant[{}] because they make not really much sense
            projected_specialized_row_type = simplify_constant_single_types(projected_specialized_row_type);

            logger.debug("specialized output-type of " + inputNode->name() + " from " +
                         inputNode->getOutputSchema().getRowType().desc() + " to " + projected_specialized_row_type.desc());

            // set input type for input node
            auto input_type_before = inputNode->getOutputSchema().getRowType();
            logger.debug("retyping stage with type " + projected_specialized_row_type.desc());

            // TODO: allow for other input nodes...
            // use retype configuration
            auto r_conf = retype_configuration_from(projected_specialized_row_type, inputNode->columns());
            if(inputNode->type() != LogicalOperatorType::FILEINPUT) {
                std::stringstream ss;
                    ss<<__FILE__<<":"<<__LINE__<<" unsupported input type.";
                    throw std::runtime_error(ss.str());
            }

            std::dynamic_pointer_cast<FileInputOperator>(inputNode)->retype(r_conf, true);
            if(inputNode->type() == LogicalOperatorType::FILEINPUT)
                ((FileInputOperator*)inputNode.get())->useNormalCase();
            auto lastParent = inputNode;
            opt_ops.push_back(inputNode);
            logger.debug("input (before): " + input_type_before.desc() +
            "\ninput (after): " + inputNode->getOutputSchema().getRowType().desc());

            // retype the other operators.
            for(const auto& op : _operators) {

                // clone operator & specialize!
                auto opt_op = op->clone();
                opt_op->setParent(lastParent);
                opt_op->setID(op->getID());

                // before row type
                std::stringstream ss;
                ss<<op->name()<<" (before): "<<op->getInputSchema().getRowType().desc()<<" -> "<<op->getOutputSchema().getRowType().desc()<<endl;
                // retype
                ss<<"retyping with parent's output schema: "<<lastParent->getOutputSchema().getRowType().desc()<<endl;
                opt_op->retype(lastParent->getOutputSchema().getRowType(), true);
                // after retype
                ss<<opt_op->name()<<" (after): "<<opt_op->getInputSchema().getRowType().desc()<<" -> "<<opt_op->getOutputSchema().getRowType().desc();
                logger.debug(ss.str());
                opt_ops.push_back(opt_op);
                lastParent = opt_op;
            }

            {
                // print out new chain of types
                std::stringstream ss;
                ss<<"Pipeline (types/before logical opt):\n";

                for(const auto& op : opt_ops) {
                    ss<<op->name()<<":\n";
                    ss<<"  in: "<<op->getInputSchema().getRowType().desc()<<"\n";
                    ss<<" out: "<<op->getOutputSchema().getRowType().desc()<<"\n";
                    ss<<"  in columns: "<<op->inputColumns()<<"\n";
                    ss<<" out columns: "<<op->columns()<<"\n";
                    ss<<"----\n";
                }

                logger.debug(ss.str());
            }


            // 2. because some fields were replaced with constants, fewer columns might need to get accessed!
            //    --> perform projection pushdown and then eliminate as many checks as possible
            auto accessed_columns_before_opt = get_accessed_columns(opt_ops);

            // basically use just on this stage the logical optimization pipeline
            auto logical_opt = std::make_unique<LogicalOptimizer>(options());

            // logically optimize pipeline, this performs reordering/projection pushdown etc.
            opt_ops = logical_opt->optimize(opt_ops, true); // inplace optimization

            std::vector<size_t> accessed_columns = get_accessed_columns(opt_ops);

            // so accCols should be sorted, now map to columns. Then check the position in accColsBefore
            // => look up what the original cols were!
            // => then push that down to reader/input node!
            std::vector<size_t> out_accessed_columns;
            std::vector<size_t> out_accessed_columns_before_opt;
            // project each when input op present
            if(inputNode && inputNode->type() == LogicalOperatorType::FILEINPUT) {
                auto fop = std::dynamic_pointer_cast<FileInputOperator>(inputNode);
                for(const auto& idx : accessed_columns)
                    out_accessed_columns.emplace_back(fop->projectReadIndex(idx));
                for(const auto& idx : accessed_columns_before_opt)
                    out_accessed_columns_before_opt.emplace_back(fop->projectReadIndex(idx));
            } else {
                out_accessed_columns = accessed_columns;
                out_accessed_columns_before_opt = accessed_columns_before_opt;
            }
            _normalToGeneralMapping = createNormalToGeneralMapping(out_accessed_columns, out_accessed_columns_before_opt);

            {
                // print out new chain of types
                std::stringstream ss;
                ss<<"Pipeline (types):\n";

                for(const auto& op : opt_ops) {
                    ss<<op->name()<<":\n";
                    ss<<"  in: "<<op->getInputSchema().getRowType().desc()<<"\n";
                    ss<<" out: "<<op->getOutputSchema().getRowType().desc()<<"\n";
                    ss<<"  in columns: "<<op->inputColumns()<<"\n";
                    ss<<" out columns: "<<op->columns()<<"\n";
                    ss<<"----\n";
                }

                logger.debug(ss.str());
            }

            // Project checks (because columns may have been removed)
            // check which input columns are required and remove checks. --> this requires to use ORIGINAL indices.
            // --> this requires pushdown to work before!
            auto acc_cols = acc_cols_before_opt;
            std::vector<NormalCaseCheck> projected_checks;

            // reproject checks, get for this actual input columns of operator.
            auto current_input_columns = opt_ops.front()->inputColumns();
            for(auto check : checks) {
                if(!check.colNames.empty()) {
                    check.colNos.clear();
                    for(auto name : check.colNames) {
                        check.colNos.push_back(indexInVector(name, current_input_columns));
                    }
                }
                projected_checks.push_back(check);
            }

//            for(const auto& col_idx : acc_cols) {
//                // changed acc cols to retrieve original indices, below is commented old code:
//                //  auto original_col_idx =  inputNode->type() == LogicalOperatorType::FILEINPUT ?
//                //          std::dynamic_pointer_cast<FileInputOperator>(inputNode)->reverseProjectToReadIndex(col_idx)
//                //          : col_idx;
//                // new
//                auto original_col_idx = col_idx;
//                for(const auto& check : checks) {
//                    if(check.isSingleColCheck() && check.colNo() == original_col_idx) {
//                        projected_checks.emplace_back(check); // need to adjust internal colNo? => no, keep for now.
//                    } else if(!check.isSingleColCheck()) {
//                        throw std::runtime_error("multi-col check not supported yet");
//                    }
//                }
//            }
            logger.debug("normal case detection requires "
                         + pluralize(checks.size(), "check")
                         + ", given current logical optimizations "
                         + pluralize(projected_checks.size(), "check")
                         + " are required to detect normal case.");


            {
                std::stringstream ss;
                ss<<"constant folded pipeline requires now only "<<pluralize(accessed_columns.size(), "column");
                ss<<" (general: "<<pluralize(accessed_columns_before_opt.size(), "column")<<")";
                ss<<", reduced checks from "<<checks.size()<<" to "<<projected_checks.size();
                logger.debug(ss.str());

                ss.str("normal col to general col mapping:\n");
                for(auto kv : _normalToGeneralMapping) {
                    ss<<kv.first<<" -> "<<kv.second<<"\n";
                }
                logger.debug(ss.str());
            }

            // add all projected checks
            for(const auto& check : projected_checks)
                _checks.push_back(check);


//            // only keep in projected checks the ones that are needed
//            // ?? how ??
//
//            // go over operators and see what can be pushed down!
//            lastParent = inputNode;
//            opt_ops.push_back(inputNode);
//            for(const auto& op : _operators) {
//                // clone operator & specialize!
//                auto opt_op = op->clone();
//                opt_op->setParent(lastParent);
//                opt_op->setID(op->getID());
//
//                switch(opt_op->type()) {
//                    case LogicalOperatorType::MAP: {
//                        auto mop = std::dynamic_pointer_cast<MapOperator>(opt_op);
//                        assert(mop);
//
//                        // do opt only if input cols are valid...!
//
//                        // retype UDF
//                        os<<"input type before: "<<mop->getInputSchema().getRowType().desc()<<endl;
//                        os<<"output type before: "<<mop->getOutputSchema().getRowType().desc()<<endl;
//                        os<<"num input columns required: "<<mop->inputColumns().size()<<endl;
//                        // retype
//                        auto input_cols = mop->inputColumns(); // HACK! won't work if no input cols are specified.
//                        auto input_type = mop->getInputSchema().getRowType();
//                        if(input_cols.empty()) {
//                            logger.debug("skipping, only for input cols now working...");
//                            return _operators;
//                        }
//                        // for all constants detected, add type there & use that for folding!
//                        // if(input_type.parameters().size() == 1 && input_type.parameters().front().isTupleType())
//                        auto tuple_mode = input_type.parameters().size() == 1 && input_type.parameters().front().isTupleType();
//                        if(!tuple_mode) {
//                            logger.debug("only tuple/dict mode supported! skipping for now");
//                            return _operators;
//                        }
//
//                        auto param_types = input_type.parameters()[0].parameters();
//                        if(param_types.size() != input_cols.size()) {
//                            logger.warn("Something wrong, numbers do not match up.");
//                            return _operators;
//                        }
//
//                        // now update these vars with whatever is possible
//                        std::unordered_map<std::string, python::Type> constant_types;
//                        // HACK! do not change column names, else this will fail...!
//                        for(auto idx : ds.constant_column_indices()) {
//                            string column_name;
//                            if(inputNode && !inputNode->inputColumns().empty()) {
//                                column_name = inputNode->inputColumns()[idx];
//                                constant_types[column_name] = python::Type::makeConstantValuedType(ds.constant_row.get(idx).getType(), ds.constant_row.get(idx).desc()); // HACK
//                            }
//                        }
//                        // lookup column names (NOTE: this should be done using integers & properly propagated through op graph)
//                        for(unsigned i = 0; i < input_cols.size(); ++i) {
//                            auto name = input_cols[i];
//                            auto it = constant_types.find(name);
//                            if(it != constant_types.end())
//                                param_types[i] = it->second;
//                        }
//
//                        // now update specialized type with constant if possible!
//                        auto specialized_type = tuple_mode ? python::Type::makeTupleType({python::Type::makeTupleType(param_types)}) : python::Type::makeTupleType(param_types);
//                        if(specialized_type != input_type) {
//                            os<<"specialized type "<<input_type.desc()<<endl;
//                            os<<"  - to - "<<endl;
//                            os<<specialized_type.desc()<<endl;
//                        } else {
//                            os<<"no specialization possible, same type";
//                            // @TODO: can skip THIS optimization, continue with the next one!
//                        }
//
//                        auto accColsBeforeOpt = mop->getUDF().getAccessedColumns();
//
//                        mop->retype({specialized_type});
//
//                        // now check again what columns are required from input, if different count -> push down!
//                        // @TODO: this could get difficult for general graphs...
//                        auto accCols = mop->getUDF().getAccessedColumns();
//                        // Note: this works ONLY for now, because no other op after this...
//
//
//                        // check again
//                        os<<"input type after: "<<mop->getInputSchema().getRowType().desc()<<endl;
//                        os<<"output type after: "<<mop->getOutputSchema().getRowType().desc()<<endl;
//                        os<<"num input columns required after opt: "<<accCols.size()<<endl;
//
//                        // which columns where eliminated?
//                        //     const std::vector<int> v1 {1, 2, 5, 5, 5, 9};
//                        //    const std::vector<int> v2 {2, 5, 7};
//                        //    std::vector<int> diff; // { 1 2 5 5 5 9 } ∖ { 2 5 7 } = { 1 5 5 9 }
//                        //
//                        //    std::set_difference(v1.begin(), v1.end(), v2.begin(), v2.end(),
//                        //                        std::inserter(diff, diff.begin()));
//                        std::sort(accColsBeforeOpt.begin(), accColsBeforeOpt.end());
//                        std::sort(accCols.begin(), accCols.end());
//                        std::vector<size_t> diff;
//                        std::set_difference(accColsBeforeOpt.begin(), accColsBeforeOpt.end(),
//                                            accCols.begin(), accCols.end(), std::inserter(diff, diff.begin()));
//                        os<<"There were "<<pluralize(diff.size(), "column")<<" optimized away:"<<endl;
//                        vector<string> opt_away_names;
//                        for(auto idx : diff)
//                            opt_away_names.push_back(mop->inputColumns()[idx]);
//                        os<<"-> "<<opt_away_names<<endl;
//
//                        // rewrite which columns to access in input node
//                        if(inputNode->type() != LogicalOperatorType::FILEINPUT) {
//                            logger.error("stopping here, should get support for ops...");
//                            return opt_ops;
//                        }
//                        auto fop = std::dynamic_pointer_cast<FileInputOperator>(inputNode);
//                        auto colsToSerialize = fop->columnsToSerialize();
//                        vector<size_t> colsToSerializeIndices;
//                        for(unsigned i = 0; i < colsToSerialize.size(); ++i)
//                            if(colsToSerialize[i])
//                                colsToSerializeIndices.push_back(i);
//                        os<<"reading columns: "<<colsToSerializeIndices<<endl;
//
//                        os<<"Column indices to read before opt: "<<accColsBeforeOpt<<endl;
//                        os<<"After opt only need to read: "<<accCols<<endl;
//
//                        // TODO: need to also rewrite access in mop again
//                        // mop->rewriteParametersInAST(rewriteMap);
//
//                        // gets a bit more difficult now:
//                        // num input columns required after opt: 13
//                        //There were 4 columns optimized away:
//                        //-> [YEAR, MONTH, CRS_DEP_TIME, CRS_ELAPSED_TIME]
//                        //reading columns: [0, 2, 3, 6, 10, 11, 20, 29, 31, 42, 50, 54, 56, 57, 58, 59, 60]
//                        //Column indices to read before opt: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
//                        //After opt only need to read: [2, 3, 4, 5, 6, 8, 9, 11, 12, 13, 14, 15, 16]
//
//                        // so accCols should be sorted, now map to columns. Then check the position in accColsBefore
//                        // => look up what the original cols were!
//                        // => then push that down to reader/input node!
//                        unordered_map<size_t, size_t> rewriteMap;
//                        vector<size_t> indices_to_read_from_previous_op;
//                        vector<string> rewriteInfo; // for printing info!
//                        vector<string> col_names_to_read_before;
//                        vector<string> col_names_to_read_after;
//
//                        // redo normal to general mapping
//                        _normalToGeneralMapping.clear();
//                        for(unsigned i = 0; i < accCols.size(); ++i) {
//                            rewriteMap[accCols[i]] = i;
//
//                            rewriteInfo.push_back(to_string(accCols[i]) + " -> " + to_string(i));
//
//                            // save normal -> general mapping
//                            _normalToGeneralMapping[i] = accCols[i];
//
//                            int j = 0;
//                            while(j < accColsBeforeOpt.size() && accCols[i] != accColsBeforeOpt[j])
//                                ++j;
//                            indices_to_read_from_previous_op.push_back(colsToSerializeIndices[j]);
//                        }
//
//                        for(auto idx : colsToSerializeIndices)
//                            col_names_to_read_before.push_back(fop->inputColumns()[idx]);
//                        for(auto idx : indices_to_read_from_previous_op)
//                            col_names_to_read_after.push_back(fop->inputColumns()[idx]);
//                        os<<"Rewriting indices: "<<rewriteInfo<<endl;
//
//                        // this is quite hacky...
//                        // ==> CLONE???
//                        fop->selectColumns(indices_to_read_from_previous_op);
//                        os<<"file input now only reading: "<<indices_to_read_from_previous_op<<endl;
//                        os<<"I.e., read before: "<<col_names_to_read_before<<endl;
//                        os<<"now: "<<col_names_to_read_after<<endl;
//                        fop->useNormalCase(); // !!! retype input op. mop already retyped above...
//
//                        mop->rewriteParametersInAST(rewriteMap);
//                        // retype!
//                        os<<"mop updated: \ninput type: "<<mop->getInputSchema().getRowType().desc()
//                            <<"\noutput type: "<<mop->getOutputSchema().getRowType().desc()<<endl;
//
//#ifdef GENERATE_PDFS
//                        mop->getUDF().getAnnotatedAST().writeGraphToPDF("final_mop_udf.pdf");
//                        // write typed version as well
//                        mop->getUDF().getAnnotatedAST().writeGraphToPDF("final_mop_udf_wtypes.pdf", true);
//#endif
//                        // @TODO: should clone operators etc. here INCL. input oprator else issue.
//                        // input operator needs additional check... => put this check into parser...
//
//
//                        break;
//                    }
//                    default:
//                        throw std::runtime_error("Unknown operator " + opt_op->name());
//                }
//                lastParent = opt_op;
//
//                opt_ops.push_back(opt_op);
//            }

            // check accesses -> i.e. need to check for all funcs till first map or end of stage is reached.
            // why? b.c. map destroys structure. The other require analysis though...!
            // i.e. trace using sample... (this could get expensive!)

            return opt_ops;
        }

        void StagePlanner::optimize(bool use_sample) {
            using namespace std;

            std::stringstream os;

            // clear checks
            //_checks.clear();

            vector<shared_ptr<LogicalOperator>> optimized_operators = vec_prepend(_inputNode, _operators);
            auto& logger = Logger::instance().logger("specializing stage optimizer");

            // run validation on initial pipeline
            bool validation_rc = validatePipeline();
            logger.debug(std::string("initial pipeline validation: ") + (validation_rc ? "ok" : "failed"));

            // step 1: retrieve sample from inputnode!
            std::vector<Row> sample;
            if(use_sample)
                sample = fetchInputSample();
            std::vector<std::string> sample_columns = _inputNode ? _inputNode->columns() : std::vector<std::string>();

            // columns may change, reflect here.
            if(use_sample && !sample.empty()) {
                if(sample.front().getRowType().isRowType())
                    sample_columns = sample.front().getRowType().get_column_names();
            }

            os<<"Got sample of "<<pluralize(sample.size(), "row")<<" with columns "<<sample_columns<<endl;

            if(!sample.empty()) {
                os<<"First sample:\n";
                for(unsigned i = 0; i < sample_columns.size(); ++i) {
                    os<<"name="<<sample_columns[i]<<" type="<<sample.front().getType(i).desc()<<":\n"<<sample.front().get(i).toPythonString()<<endl;
                }
            }

            // retype using sample, need to do this initially.
            if(use_sample)
                retypeOperators(sample, sample_columns, use_sample);

            // check now whether filters can be promoted or not
            if(_useFilterPromo) {
                if(!use_sample) {
                    logger.debug("Skipping filter promotion, because use of sample is deactivated.");
                } else {
                    auto input_columns_before = _inputNode->columns();

                    std::vector<Row> sample_after_filter;
                    auto rc_filter = promoteFilters(&sample_after_filter);
                    if(rc_filter)
                        sample = sample_after_filter;
                    else {
                        os<<"Filter promo not carried out, because filter found no relevant rows to keep. Can't specialize."<<endl;
                    }

                    if(!sample.empty()) {
                        os<<"First sample (post-filter):\n";
                        for(unsigned i = 0; i < sample_columns.size(); ++i) {
                            os<<"name="<<sample_columns[i]<<" type="<<sample.front().getType(i).desc()<<":\n"<<sample.front().get(i).toPythonString()<<endl;
                        }
                    }

                    // want to redo typing if checks changed!
                    if(_checks.end() != std::find_if(_checks.begin(),
                                                     _checks.end(),
                                                     [](const NormalCaseCheck& check) {
                                                         return check.type == CheckType::CHECK_FILTER;
                                                     })) {
                        logger.info("retyping b.c. of promoted filter");
                        auto input_type_before_promo = _inputNode->getOutputSchema().getRowType();
                        sample = fetchInputSample();
                        sample_columns = _inputNode ? _inputNode->columns() : std::vector<std::string>();

                        if(!sample.empty()) {
                            os<<"First sample (retype due to filter promo):\n";
                            for(unsigned i = 0; i < sample_columns.size(); ++i) {
                                os<<"name="<<sample_columns[i]<<" type="<<sample.front().getType(i).desc()<<":\n"<<sample.front().get(i).toPythonString()<<endl;
                            }
                        }

                        retypeOperators(sample, sample_columns, use_sample);

                        auto input_type_after_promo = _inputNode->getOutputSchema().getRowType();
                        if(input_type_after_promo == input_type_before_promo) {
                            logger.debug("input row type didn't change due to promoted filter.");
                        } else {
                            logger.debug("input row type changed:\nwas before: " + input_type_before_promo.desc() + "\nis now: " + input_type_after_promo.desc());
                        }

                        // did column order change?
                        if(!vec_equal(sample_columns, input_columns_before)) {
                            std::stringstream ss;
                            ss<<"input columns changed:\nbefore: "<<input_columns_before<<"\nafter: "<<sample_columns;
                            logger.debug(ss.str());
                        }

                        logger.debug("filter promo done.");
                    }
                }
            }


            // perform sample based optimizations
            if(_useConstantFolding) {
                if(!use_sample) {
                    logger.debug("Skipping constant-folding, because use of sample is deactivated.");
                } else {
                    // perform only when input op is present (could do later, but requires sample!)
                    if(_inputNode && _inputNode->type() == LogicalOperatorType::FILEINPUT) {
                        logger.info("Performing constant folding optimization (sample size=" + std::to_string(sample.size()) + ")");
                        optimized_operators = constantFoldingOptimization(sample, sample_columns);

                        // overwrite internal operators to apply subsequent optimizations
                        _inputNode = _inputNode ? optimized_operators.front() : nullptr;
                        _operators = _inputNode ? vector<shared_ptr<LogicalOperator>>{optimized_operators.begin() + 1,
                                                                                      optimized_operators.end()}
                                                : optimized_operators;

                        // run validation after applying constant folding
                        validation_rc = validatePipeline();
                        logger.debug(std::string("post-constant-folding pipeline validation: ") + (validation_rc ? "ok" : "failed"));
                    } else {
                        logger.debug("skipping constant-folding optimization for stage");
                    }
                }
            }


            // perform struct sparsification
            if(_useSparsifyStructs) {
                if(!use_sample) {
                    logger.debug("Skipping struct sparsification, because use of sample is deactivated.");
                } else {
                    // perform only when input op is present (could do later, but requires sample!)
                    if(_inputNode && _inputNode->type() == LogicalOperatorType::FILEINPUT) {
                        logger.info("Performing struct sparsification optimization (sample size=" + std::to_string(sample.size()) + ")");
                        optimized_operators = sparsifyStructs(sample, sample_columns);

                        // overwrite internal operators to apply subsequent optimizations
                        _inputNode = _inputNode ? optimized_operators.front() : nullptr;
                        _operators = _inputNode ? vector<shared_ptr<LogicalOperator>>{optimized_operators.begin() + 1,
                                                                                      optimized_operators.end()}
                                                : optimized_operators;

                        // run validation after applying constant folding
                        validation_rc = validatePipeline();
                        logger.debug(std::string("post-sparsify-structs pipeline validation: ") + (validation_rc ? "ok" : "failed"));
                    } else {
                        logger.debug("skipping sparsify structs optimization for stage");
                    }
                }
            }

            // simplify large structs (threshold 20?)
            // this is necessary because large structs will kill the performance of the LLVM optimizer (in its default -O2 setting).
            // This pass should come after sparsification to allow large structs to get sparsified first.
            if(_simplifyLargeStructs) {
                optimized_operators = simplifyLargeStructs(20);

                // overwrite internal operators to apply subsequent optimizations
                _inputNode = _inputNode ? optimized_operators.front() : nullptr;
                _operators = _inputNode ? vector<shared_ptr<LogicalOperator>>{optimized_operators.begin() + 1,
                                                                              optimized_operators.end()}
                                        : optimized_operators;
            }


            // All optimizations were carried out, if this stage has as input operator a source, perform logical optimization step as last one.
            if(_inputNode->isDataSource()) {
                optimized_operators = applyLogicalOptimizer();
                _inputNode = _inputNode ? optimized_operators.front() : nullptr;
                _operators = _inputNode ? vector<shared_ptr<LogicalOperator>>{optimized_operators.begin() + 1,
                                                                              optimized_operators.end()}
                                        : optimized_operators;
            }

            // can filters get pushed down even further? => check! constant folding may remove code!
        }

        bool are_in_and_out_schemas_compatible(const python::Type& prev_out_row_type, const python::Type& in_row_type) {
            // check whether schemas are compatible

            // special case: in_row_type is empty_tuple or empty_row -> always compatible
            if(in_row_type == python::Type::EMPTYTUPLE || in_row_type == python::Type::EMPTYROW)
                return true;

            // they're ok if the flattened representation matches
            return flattenedType(prev_out_row_type) == flattenedType(in_row_type);
        }

        bool StagePlanner::validatePipeline() {

            auto& logger = Logger::instance().logger("specializing stage optimizer");

            python::Type lastRowType;
            if(_inputNode) {
                if(_inputNode->type() == LogicalOperatorType::FILEINPUT) {
                    auto fop = std::dynamic_pointer_cast<FileInputOperator>(_inputNode);
                    lastRowType = fop->getOutputSchema().getRowType();
                } else {
                    lastRowType = _inputNode->getOutputSchema().getRowType();
                }
            } else {
                if(_operators.empty())
                    return true;
                lastRowType = _operators.front()->getInputSchema().getRowType();
            }

            bool validation_ok = true;

            // go through ops and check input/output is compatible (flattened!)
            for(const auto& op : _operators) {
                // note: chain of resolvers works differently, i.e. update lastRowType ONLY for non-resolve ops
                // (ignore is fine)
                if(op->type() == LogicalOperatorType::RESOLVE) {
                    auto rop = std::dynamic_pointer_cast<ResolveOperator>(op);
                    assert(rop);
                    lastRowType = rop->getNormalParent()->getInputSchema().getRowType();
                }

                if(lastRowType == python::Type::UNKNOWN) {
                    logger.error("operator " + op->name() + " has unknown schema.");
                    validation_ok = false;
                    return validation_ok;
                }

                if(!are_in_and_out_schemas_compatible(lastRowType, op->getInputSchema().getRowType())) {
                    logger.error("(" + op->name() + "): input schema "
                                     + op->getInputSchema().getRowType().desc()
                                     + " incompatible with previous operator's output schema "
                                     + lastRowType.desc());
                    validation_ok = false;
                }

                lastRowType = op->getOutputSchema().getRowType();
            }

            return validation_ok;
        }

        std::vector<std::shared_ptr<LogicalOperator>> StagePlanner::retypeUsingOptimizedInputSchema(const python::Type& input_row_type,
                                                                                                    const std::vector<std::string>& input_column_names) {
            using namespace std;

            auto& logger = Logger::instance().logger("specializing stage optimizer");

//            // only null-value opt yet supported
//            if(!_useNVO)
//                return vec_prepend(_inputNode, _operators);

            // special case: cache operator! might have exceptions or no exceptions => specialize depending on that!
            // i.e. an interesting case happens when join(... .cache(), ...) is used. Then, need to upcast result to general case
            // => for now, do not support...
            // => upcasting should be done in LocalBackend.


            // no input node or input node not FileInputOperator?
            // => can't specialize...
            if(!_inputNode)
                return _operators;

            if(_inputNode->type() != LogicalOperatorType::FILEINPUT && _inputNode->type() != LogicalOperatorType::CACHE) {
                logger.debug("Skipping null-value optimization for pipeline because input node is " + _inputNode->name());
                // @TODO: maybe this should also be done using retyping?
                return vec_prepend(_inputNode, _operators);
            }

            // fetch optimized schema from input operator
            Schema opt_input_schema;
            if(_inputNode->type() == LogicalOperatorType::FILEINPUT)
                opt_input_schema = std::dynamic_pointer_cast<FileInputOperator>(_inputNode)->getOptimizedOutputSchema();
            else if(_inputNode->type() == LogicalOperatorType::CACHE) {
                throw std::runtime_error("need to fix here case when columns differ...");
                opt_input_schema = std::dynamic_pointer_cast<CacheOperator>(_inputNode)->getOptimizedOutputSchema();
            } else
                throw std::runtime_error("internal error in specializing for the normal case");
            auto opt_input_rowtype = opt_input_schema.getRowType();

            logger.info("Row type before retype: " + opt_input_rowtype.desc());
            opt_input_rowtype = input_row_type;
            logger.info("Row type after retype: " + opt_input_rowtype.desc());

#ifdef VERBOSE_BUILD
            {
                stringstream ss;
                ss<<FLINESTR<<endl;
                ss<<"specializing pipeline for normal case ("<<pluralize(_operators.size(), "operator")<<")"<<endl;
                ss<<"input node: "<<_inputNode->name()<<endl;
                ss<<"optimized schema of input node: "<<opt_input_rowtype.desc()<<endl;
                logger.debug(ss.str());
            }
#endif
            {
                stringstream ss;
                auto original_input_rowtype = _inputNode->getOutputSchema().getRowType();
                if(opt_input_rowtype != original_input_rowtype)
                    ss<<"NVO can specialize input schema from\n"<<original_input_rowtype.desc()<<"\n- to - \n"<<opt_input_rowtype.desc();
                else
                    ss<<"no specialization using NVO, because types are identical.";
                logger.debug(ss.str());
            }

            auto last_rowtype = opt_input_rowtype;
            checkRowType(last_rowtype);

            bool pipeline_breaker_present = false;

            // go through ops & specialize (leave jop as is)
            vector<std::shared_ptr<LogicalOperator>> opt_ops;
            std::shared_ptr<LogicalOperator> lastNode = nullptr;
            auto fop = std::dynamic_pointer_cast<FileInputOperator>(_inputNode->clone());
            fop->cloneCaches(*((FileInputOperator*)_inputNode.get())); // copy samples!

            // need to restrict potentially?
            auto r_conf = retype_configuration_from(input_row_type, input_column_names);

            if(!fop->retype(r_conf, true))
                throw std::runtime_error("failed to retype " + fop->name() + " operator."); // for input operator, ignore Option[str] compatibility which is set per default
            fop->useNormalCase(); // this forces output schema to be normalcase (i.e. overwrite internally output schema to be normal case schema)
            opt_ops.push_back(fop);

            last_rowtype = fop->getOutputSchema().getRowType();
            auto last_columns = fop->columns(); // the columns delivered by this particular operator.

            // go over the other ops from the stage...
            for(const auto& node : _operators) {
                auto lastParent = opt_ops.empty() ? _inputNode : opt_ops.back();
                if(!lastNode)
                    lastNode = lastParent; // set lastNode to parent to make join on fileop work!

                // update RetypeConfiguration
                r_conf.is_projected = true;
                r_conf.row_type = last_rowtype;
                r_conf.columns = last_columns;
                r_conf.remove_existing_annotations = true; // remove all annotations (except the column restore ones?)

                {
                    std::stringstream ss;
                    ss<<__FILE__<<":"<<__LINE__<<" node "<<node->name()<<" retype with type: "<<last_rowtype.desc()<<" columns: "<<last_columns;
                    logger.debug(ss.str());
                }

                switch(node->type()) {
                    // handled above...
                    //  case LogicalOperatorType::PARALLELIZE: {
                    //      opt_ops.push_back(node);
                    //      break;
                    //  }
                    //  case LogicalOperatorType::FILEINPUT: {
                    //      // create here copy using normalcase!
                    //      auto op = std::dynamic_pointer_cast<FileInputOperator>(node->clone());
                    //      op->useNormalCase();
                    //      opt_ops.push_back(op);
                    //      break;
                    //  }
                        // construct clone with parents & retype
                    case LogicalOperatorType::FILTER:
                    case LogicalOperatorType::MAP:
                    case LogicalOperatorType::MAPCOLUMN:
                    case LogicalOperatorType::WITHCOLUMN:
                    case LogicalOperatorType::IGNORE: {

                        // if(node->getInputSchema() == Schema::UNKNOWN) {
                        //
                        //     Logger::instance().logger("codegen").debug(std::string(__FILE__) + ":" + std::to_string(__LINE__)
                        //     + " node " + node->name() + " has invalid input schema, was it properly retyped?");
                        //
                        //     throw std::runtime_error("invalid node encountered.");
                        // }

                        // assert(node->getInputSchema() != Schema::UNKNOWN);

                        auto op = node->clone(false); // no need to clone with parents, b.c. assigned below.
                        auto oldInputType = op->getInputSchema().getRowType();
                        auto oldOutputType = op->getInputSchema().getRowType();
                        auto columns_before = op->columns();
                        auto in_columns_before = op->inputColumns();

                        checkRowType(last_rowtype.isRowType() ? last_rowtype.get_columns_as_tuple_type() : last_rowtype);
                        // set FIRST the parent. Why? because operators like ignore depend on parent schema
                        // therefore, this needs to get updated first.
                        op->setParent(lastParent); // need to call this before retype, so that columns etc. can be utilized.
                        if(!op->retype(r_conf)) {
                            std::stringstream ss;
                            ss<<__FILE__<<":"<<__LINE__<<" Failed retype operator " + op->name()<<"\n";
                            ss<<"operator columns: "<<columns_before<<"\n";
                            if(hasUDF(op.get())) {
                                auto udfop = std::dynamic_pointer_cast<UDFOperator>(op);
                                ss<<"UDF:\n"<<udfop->getUDF().getCode()<<"\n";
                            }
                            throw std::runtime_error(ss.str());
                        }
                        opt_ops.push_back(op);
                        opt_ops.back()->setID(node->getID());
#ifdef VERBOSE_BUILD
                        {
                            stringstream ss;
                            ss<<FLINESTR<<endl;
                            ss<<"retyped "<<op->name()<<endl;
                            ss<<"\told input type: "<<oldInputType.desc()<<endl;
                            ss<<"\told output type: "<<oldOutputType.desc()<<endl;
                            ss<<"\tnew input type: "<<op->getInputSchema().getRowType().desc()<<endl;
                            ss<<"\tnew output type: "<<op->getOutputSchema().getRowType().desc()<<endl;


                            // columns: before/after
                            ss<<"input columns before:  "<<in_columns_before<<endl;
                            ss<<"output columns before: "<<columns_before<<endl;
                            ss<<"---"<<endl;
                            ss<<"input columns after: "<<op->inputColumns()<<endl;
                            ss<<"output columns after:  "<<op->columns()<<endl;

                            logger.debug(ss.str());
                        }
#endif

                        break;
                    }

                    case LogicalOperatorType::JOIN: {
                        pipeline_breaker_present = true;
                        auto jop = std::dynamic_pointer_cast<JoinOperator>(node);
                        assert(lastNode);

//#error "bug is here: basically if lastParent is cache, then it hasn't been cloned and thus getOutputSchema gives the general case"
//                        "output schema, however, if the cache operator is the inputnode, then optimized schema should be used..."

                        // this here is a bit more involved.
                        // I.e., is it left side or right side?
                        vector<std::shared_ptr<LogicalOperator>> parents;
                        if(lastNode == jop->left()) {
                            // left side is pipeline
                            //os<<"pipeline is left side"<<endl;

                            // i.e. leave right side as is => do not take normal case there!
                            parents.push_back(lastParent); // --> normal case on left side
                            parents.push_back(jop->right());
                        } else {
                            // right side is pipeline
                            assert(lastNode == jop->right());
                            //os<<"pipeline is right side"<<endl;

                            // i.e. leave left side as is => do not take normal case there!
                            parents.push_back(jop->left());
                            parents.push_back(lastParent); // --> normal case on right side
                        }

                        opt_ops.push_back(std::shared_ptr<LogicalOperator>(new JoinOperator(parents[0], parents[1], jop->leftColumn(), jop->rightColumn(),
                                                           jop->joinType(), jop->leftPrefix(), jop->leftSuffix(), jop->rightPrefix(),
                                                           jop->rightSuffix())));
                        opt_ops.back()->setID(node->getID()); // so lookup map works!

//#error "need a retype operator for the join operation..."
#ifdef VERBOSE_BUILD
                        {
                            jop = std::dynamic_pointer_cast<JoinOperator>(opt_ops.back());
                            stringstream ss;
                            ss<<FLINESTR<<endl;
                            ss<<"retyped "<<node->name()<<endl;
                            ss<<"\tleft type: "<<jop->left()->getOutputSchema().getRowType().desc()<<endl;
                            ss<<"\tright type: "<<jop->right()->getOutputSchema().getRowType().desc()<<endl;

                            logger.debug(ss.str());
                        }
#endif

                        break;
                    }

                    case LogicalOperatorType::RESOLVE: {
                        // ignore, just return parent. This is the fast path!
                        break;
                    }
                    case LogicalOperatorType::TAKE: {
                        opt_ops.push_back(std::shared_ptr<LogicalOperator>(new TakeOperator(lastParent, std::dynamic_pointer_cast<TakeOperator>(node)->limit())));
                        opt_ops.back()->setID(node->getID());
                        break;
                    }
                    case LogicalOperatorType::FILEOUTPUT: {
                        auto fop = std::dynamic_pointer_cast<FileOutputOperator>(node);

                        opt_ops.push_back(std::shared_ptr<LogicalOperator>(new FileOutputOperator(lastParent, fop->uri(), fop->udf(), fop->name(),
                                                                 fop->fileFormat(), fop->options(), fop->numParts(), fop->splitSize(),
                                                                 fop->limit())));
                        break;
                    }
                    case LogicalOperatorType::CACHE: {
                        pipeline_breaker_present = true;
                        // two options here: Either cache is used as last node or as source!
                        // source?
                        auto cop = std::dynamic_pointer_cast<CacheOperator>(node);
                        if(!cop->children().empty()) {
                            // => cache is a source, i.e. fetch optimized schema from it!
                            last_rowtype = cop->getOptimizedOutputSchema().getRowType();
                            last_columns = cop->columns();
                            checkRowType(last_rowtype);
                            // os<<"cache is a source: optimized schema "<<last_rowtype.desc()<<endl;

                            // use normal case & clone WITHOUT parents
                            // clone, set normal case & push back
                            cop = std::dynamic_pointer_cast<CacheOperator>(cop->cloneWithoutParents());
                            cop->setOptimizedOutputType(last_rowtype);
                            cop->useNormalCase();
                        } else {
                            // cache should not have any children
                            assert(cop->children().empty());
                            // => i.e. first time cache is seen, it's processed as action!
                            // os<<"cache is action, optimized schema: "<<endl;
                            // os<<"cache normal case will be: "<<last_rowtype.desc()<<endl;
                            // => reuse optimized schema!
                            cop->setOptimizedOutputType(last_rowtype);
                            // simply push back, no cloning here necessary b.c. no data is altered
                        }

                        opt_ops.push_back(cop);
                        break;
                    }
                    case LogicalOperatorType::AGGREGATE: {
                        // aggregate is currently not part of codegen, i.e. the aggregation happens when writing out the output!
                        // ==> what about aggByKey though?

                        auto aop = std::dynamic_pointer_cast<AggregateOperator>(node);
                        // need to retype aggregateByKey operator with new, specialized input type!
                        assert(node->getInputSchema() != Schema::UNKNOWN);

                        auto op = node->clone(false); // no need to clone with parents, b.c. assigned below.
                        auto oldInputType = node->getInputSchema().getRowType();
                        auto oldOutputType = node->getInputSchema().getRowType();
                        auto columns_before = node->columns();

                        {
                            std::stringstream ss;
                            ss<<"retyping "<<aop->name()<<std::endl;
                            logger.debug(ss.str());
                        }


                        checkRowType(last_rowtype);
                        // set FIRST the parent. Why? because operators like ignore depend on parent schema
                        // therefore, this needs to get updated first.
                        op->setParent(lastParent); // need to call this before retype, so that columns etc. can be utilized.
                        if(!op->retype(r_conf))
                            throw std::runtime_error("could not retype operator " + op->name());
                        opt_ops.push_back(op);
                        opt_ops.back()->setID(node->getID());

#ifdef VERBOSE_BUILD
                        {
                            stringstream ss;
                            ss<<FLINESTR<<endl;
                            ss<<"retyped "<<op->name()<<endl;
                            ss<<"\told input type: "<<oldInputType.desc()<<endl;
                            ss<<"\told output type: "<<oldOutputType.desc()<<endl;
                            ss<<"\tnew input type: "<<op->getInputSchema().getRowType().desc()<<endl;
                            ss<<"\tnew output type: "<<op->getOutputSchema().getRowType().desc()<<endl;


                            // columns: before/after
                            ss<<"output columns before: "<<columns_before<<endl;
                            ss<<"output columns after: "<<op->columns()<<endl;

                            logger.debug(ss.str());
                        }
#endif
                        break;
                    }
                    default: {
                        std::stringstream ss;
                        ss<<"unknown operator " + node->name() + " encountered in fast path creation";
                        logger.error(ss.str());
                        throw std::runtime_error(ss.str());
                    }
                }

                if(!opt_ops.empty()) {
                    // os<<"last opt_op name: "<<opt_ops.back()->name()<<endl;
                    // os<<"last opt_op output type: "<<opt_ops.back()->getOutputSchema().getRowType().desc()<<endl;
                    if(opt_ops.back()->type() != LogicalOperatorType::CACHE) {
                        last_rowtype = opt_ops.back()->getOutputSchema().getRowType();
                        last_columns = opt_ops.back()->columns();
                    }

                    checkRowType(last_rowtype.isRowType() ? last_rowtype.get_columns_as_tuple_type() : last_rowtype);
                }

                lastNode = node;
            }


            // important to have cost available
            if(!opt_ops.empty() && pipeline_breaker_present)
                assert(opt_ops.back()->cost() > 0);

            return opt_ops;
        }

        std::vector<Row> StagePlanner::fetchInputSample() {
            if(!_inputNode)
                return {};
            return _inputNode->getSample(10000); // 10k rows. -> should be stratified sample?
        }
    }

    // HACK: magical experiment function!!!
    // HACK!
    // bool hyperspecialize(TransformStage *stage, const URI& uri, size_t file_size,
    //                         double nc_threshold, size_t sample_limit, bool enable_cf)
    bool hyperspecialize(TransformStage *stage,
                         const URI& uri,
                         size_t file_size,
                         size_t sample_limit,
                         size_t strata_size=1,
                         size_t samples_per_strata=1,
                         const codegen::StageBuilderConfiguration& conf=codegen::StageBuilderConfiguration()) {
        auto& logger = Logger::instance().logger("hyper specializer");

        std::stringstream os;

        // run hyperspecialization using planner, yay!
        assert(stage);

        // need to decode CodeGenerationCOntext from stage
        if(stage->_encodedData.empty()) {
            logger.info("did not find encoded codegen context, skipping.");
            return false;
        }

        logger.info("specializing code to file " + uri.toString());

        // deserialize using Cereal

        // fetch codeGenerationContext & restore logical operator tree!
        codegen::CodeGenerationContext ctx;
        Timer timer;
#ifdef BUILD_WITH_CEREAL
        {
            auto compressed_str = stage->_encodedData;
            auto decompressed_str = decompress_string(compressed_str);
            logger.info("Decompressed Code context from " + sizeToMemString(compressed_str.size()) + " to " + sizeToMemString(decompressed_str.size()));
            Timer deserializeTimer;
            std::istringstream iss(decompressed_str);
            cereal::BinaryInputArchive ar(iss);
            ar(ctx);
            logger.info("Deserialization of Code context took " + std::to_string(deserializeTimer.time()) + "s");
        }
        logger.info("Total Stage Decode took " + std::to_string(timer.time()) + "s");
#else
        // use custom JSON encoding
        auto compressed_str = stage->_encodedData;
        auto decompressed_str = decompress_string(compressed_str);
        logger.info("Decompressed Code context from " + sizeToMemString(compressed_str.size()) + " to " + sizeToMemString(decompressed_str.size()));
        Timer deserializeTimer;
        ctx = codegen::CodeGenerationContext::fromJSON(decompressed_str);
        logger.info("Deserialization of Code context took " + std::to_string(deserializeTimer.time()) + "s");
        logger.info("Total Stage Decode took " + std::to_string(timer.time()) + "s");
#endif
        // old hacky version
        // auto ctx = codegen::CodeGenerationContext::fromJSON(stage->_encodedData);

        assert(ctx.slowPathContext.valid());
        // decoded, now specialize
        auto path_ctx = ctx.slowPathContext;
        //specializePipeline(fastPath, uri, file_size);
        auto inputNode = path_ctx.inputNode;
        auto operators = path_ctx.operators;

        auto input_row_type = inputNode->getOutputSchema().getRowType();
        auto input_column_names = inputNode->columns();
        if(input_column_names.empty() && input_row_type.isRowType())
            input_column_names = input_row_type.get_column_names();
        // transform to tuple type.
        if(input_row_type.isRowType())
            input_row_type = input_row_type.get_columns_as_tuple_type();

        assert(input_row_type.isTupleType());
        assert(input_row_type.parameters().size() == input_column_names.size());

        size_t num_input_before_hyper = input_row_type.parameters().size();
        {
            std::stringstream ss;
            ss<<"number of input columns before hyperspecialization: " + std::to_string(num_input_before_hyper);
            ss<<"\ninput columns: "<<input_column_names;
           logger.info(ss.str());
        }

        // force resampling b.c. of thin layer
        Timer samplingTimer;
        bool enable_cf = conf.constantFoldingOptimization;
        if(inputNode->type() == LogicalOperatorType::FILEINPUT) {
            auto fop = std::dynamic_pointer_cast<FileInputOperator>(inputNode); assert(fop);

            // // debug: overwrite values!
            // auto temp_uri = URI("/home/leonhards/projects/tuplex-public/tuplex/cmake-build-debug-w-cereal/dist/bin/bad_sample.ndjson");
            // strata_size = 1;
            // samples_per_strata = 1;
            // VirtualFileSystem::fromURI(temp_uri).file_size(temp_uri, file_size);
            // fop->setInputFiles({temp_uri}, {file_size}, true, sample_limit, true, strata_size, samples_per_strata);

            // use sampling size provided
            if(0 != conf.sampling_size)
                fop->setSamplingSize(conf.sampling_size);

            // Check if URI is encoded range uri. This is not yet supported, warn about it but continue with full uri.
            auto uri_to_sample_on = uri;
            URI decoded_uri;
            size_t range_start = 0, range_end = 0;
            decodeRangeURI(uri.toString(), decoded_uri, range_start, range_end);
            if(range_start != 0 || range_end != 0) {
                logger.warn("Found range-based uri " + uri.toString() + ", using for sampling original uri " + decoded_uri.toString() + ". Ranges not yet supported for hyperspecialziation.");
                uri_to_sample_on = decoded_uri;
            }
            // resample.
            fop->setInputFiles({uri_to_sample_on}, {file_size}, true, sample_limit, true, strata_size, samples_per_strata);

            if(fop->fileFormat() == FileFormat::OUTFMT_JSON) {
                enable_cf = false;
                logger.warn("Disabled constant-folding for now (not supported for JSON yet).");
            }
        }

        {
            std::stringstream ss;
            ss<<"sampling (setInputFiles) took " + std::to_string(samplingTimer.time()) + "s";
            if(sample_limit < std::numeric_limits<size_t>::max())
                ss << " (limit=" << sample_limit << ")";
            logger.info(ss.str());
        }

        // node need to find some smart way to QUICKLY detect whether the optimization can be applied or should be rather skipped...
        codegen::StagePlanner planner(inputNode, operators, conf.policy.normalCaseThreshold);
        apply_stage_builder_conf_to_planner(conf, planner);

        planner.optimize();
        path_ctx.inputNode = planner.input_node();
        path_ctx.operators = planner.optimized_operators();
        path_ctx.checks = planner.checks();

        assert(inputNode->getID() == path_ctx.inputNode->getID());

        // TODO: refactor together with code in StageBuilder.

        // output schema: the schema this stage yields ultimately after processing
        // input schema: the (optimized/projected, normal case) input schema this stage reads from (CSV, Tuplex, ...)
        // read schema: the schema to read from (specialized) but unprojected.
        if(path_ctx.inputNode->type() == LogicalOperatorType::FILEINPUT) {
            auto fop = std::dynamic_pointer_cast<FileInputOperator>(path_ctx.inputNode);
            path_ctx.inputSchema = fop->getOptimizedOutputSchema();
            path_ctx.readSchema = fop->getOptimizedInputSchema(); // when null-value opt is used, then this is different! hence apply!
            path_ctx.columnsToRead = fop->columnsToSerialize();
             // print out columns & types!
            auto col_types = path_ctx.inputSchema.getRowType().isRowType() ? path_ctx.inputSchema.getRowType().get_column_types() : path_ctx.inputSchema.getRowType().parameters();
            assert(fop->columns().size() == col_types.size());
            for(unsigned i = 0; i < fop->columns().size(); ++i) {
                os<<"col "<<i<<" (" + fop->columns()[i] + ")"<<": "<<col_types[i].desc()<<std::endl;
            }

        } else {
            path_ctx.inputSchema = path_ctx.inputNode->getOutputSchema();
            path_ctx.readSchema = Schema::UNKNOWN; // not set, b.c. not a reader...
            path_ctx.columnsToRead = {};
        }

        path_ctx.outputSchema = path_ctx.operators.back()->getOutputSchema();
        logger.info("specialized to input:  " + path_ctx.inputSchema.getRowType().desc());
        logger.info("specialized to output: " + path_ctx.outputSchema.getRowType().desc());

        // print out:
        // os<<"input schema ("<<pluralize(path_ctx.inputSchema.getRowType().parameters().size(), "column")<<"): "<<path_ctx.inputSchema.getRowType().desc()<<std::endl;
        // os<<"read schema ("<<pluralize(path_ctx.readSchema.getRowType().parameters().size(), "column")<<"): "<<path_ctx.readSchema.getRowType().desc()<<std::endl;
        // os<<"output schema ("<<pluralize(path_ctx.outputSchema.getRowType().parameters().size(), "column")<<"): "<<path_ctx.outputSchema.getRowType().desc()<<std::endl;

        size_t numToRead = 0;
        for(auto indicator : path_ctx.columnsToRead)
            numToRead += indicator;
        logger.info("specialized code reads: " + pluralize(numToRead, "column"));

        {
            std::stringstream ss;
            // print out normal-case columns and types
            unsigned pos = 0;
            auto col_types = path_ctx.inputSchema.getRowType().isRowType() ? path_ctx.inputSchema.getRowType().get_column_types() : path_ctx.inputSchema.getRowType().parameters();
            for(unsigned i = 0; i < path_ctx.columnsToRead.size(); ++i) {
                if(path_ctx.columnsToRead[i]) {
                    ss<<"column "<<i<<" '"<<path_ctx.columns()[pos]<<"': "<<col_types[pos].desc()<<"\n";
                    pos++;
                }
            }

            logger.info(ss.str());
        }


        ctx.fastPathContext = path_ctx;

        auto generalCaseInputRowType = ctx.slowPathContext.inputSchema.getRowType();
        if(ctx.slowPathContext.inputNode->type() == LogicalOperatorType::FILEINPUT)
            generalCaseInputRowType = ctx.slowPathContext.readSchema.getRowType();

        auto generalCaseOutputRowType = ctx.slowPathContext.outputSchema.getRowType();

        // did specialized stage only yield exceptions or unknown? => then specialization failed.
        if(path_ctx.inputSchema.getRowType().isExceptionType()
            || path_ctx.inputSchema == Schema::UNKNOWN) {
            logger.warn("could not hyperspecialize stage (input type: "
                        + path_ctx.inputSchema.getRowType().desc()
                        + "), skipping code generation.");
            return false;
        }
        if(path_ctx.outputSchema.getRowType().isExceptionType()
           || path_ctx.outputSchema == Schema::UNKNOWN) {
            logger.warn("could not hyperspecialize stage (output type: "
                        + path_ctx.outputSchema.getRowType().desc()
                        + "), skipping code generation.");
            return false;
        }

        // assignment to stage should happen below...

        // generate code! Add to stage, can then compile this. Yay!
        timer.reset();
        TransformStage::StageCodePath fast_code_path;
        try {
            fast_code_path = codegen::StageBuilder::generateFastCodePath(ctx,
                                                                         ctx.fastPathContext,
                                                                         generalCaseInputRowType,
                                                                         ctx.slowPathContext.columnsToRead,
                                                                         generalCaseOutputRowType,
                                                                         ctx.slowPathContext.columns(),
                                                                         planner.normalToGeneralMapping(),
                                                                         stage->number(),
                                                                         conf.exceptionSerializationMode);
        } catch(const std::exception& e) {
            std::stringstream ss;
            ss<<"Exception occurred during fast code generation in hyperspecialization, details: ";
            ss<<e.what();
            logger.error(ss.str());
            return false;
        } catch(...) {
            std::stringstream ss;
            ss<<"Unknown Exception occurred during fast code generation in hyperspecialization.";
            logger.error(ss.str());
            return false;
        }

        // update schemas!
        stage->_fastCodePath = fast_code_path;

        auto stage_normal_case_before = stage->_normalCaseInputSchema.getRowType();
        stage->_normalCaseInputSchema = Schema(stage->_normalCaseInputSchema.getMemoryLayout(), ctx.fastPathContext.inputSchema.getRowType());
        logger.info("Transform stage row types with hyperspecialization:\n-- before: " + stage_normal_case_before.desc() + "\n-- after: " + stage->normalCaseInputSchema().getRowType().desc());

        // the output schema (str in tocsv) case is not finalized yet...
        // stage->_normalCaseOutputSchema = Schema(stage->_normalCaseOutputSchema.getMemoryLayout(), path_ctx.outputSchema.getRowType());
        auto after_hyper_output_row_type = ctx.fastPathContext.inputNode->getOutputSchema().getRowType();
        logger.info("Input node output type after hyperspecialization: " + after_hyper_output_row_type.desc());
        size_t num_input_after_hyper = after_hyper_output_row_type.isRowType() ? after_hyper_output_row_type.get_column_count() : after_hyper_output_row_type.parameters().size();
        logger.info("Number of input columns after hyperspecialization: " + std::to_string(num_input_after_hyper));

        logger.info("Generated code in " + std::to_string(timer.time()) + "s");

        // can then compile everything, hooray!
        return true;
    }

    std::vector<size_t>
    codegen::StagePlanner::get_accessed_columns(const std::vector<std::shared_ptr<LogicalOperator>> &ops) {
        std::vector<size_t> col_idxs;

        if(ops.empty())
            return col_idxs;
        if(ops.front()->type() == LogicalOperatorType::FILEINPUT) {
            auto fop = std::dynamic_pointer_cast<FileInputOperator>(ops.front());
            auto cols = fop->columnsToSerialize();
            auto num_columns = cols.size();
            for(unsigned i = 0; i < num_columns; ++i) {
                if(cols[i])
                    col_idxs.emplace_back(i);
            }
        } else {
            // check from type.
            auto num_columns = extract_columns_from_type(ops.front()->getOutputSchema().getRowType());
            for(unsigned i = 0; i < num_columns; ++i)
                col_idxs.emplace_back(i);
        }

        return col_idxs;
    }

    std::map<int, int> codegen::StagePlanner::createNormalToGeneralMapping(const std::vector<size_t>& normalAccessedOriginalIndices,
                                               const std::vector<size_t>& generalAccessedOriginalIndices) {
        std::map<int, int> m;

        // normal case should be less than general
        assert(normalAccessedOriginalIndices.size() <= generalAccessedOriginalIndices.size());

        // create two lookup maps (incl. original columns!)
        std::unordered_map<size_t, size_t> originalToGeneral;
        for(unsigned i = 0; i < generalAccessedOriginalIndices.size(); ++i) {
            originalToGeneral[generalAccessedOriginalIndices[i]] = i;
        }

        // now lookup from normal
        for(unsigned i = 0; i < normalAccessedOriginalIndices.size(); ++i) {
            // this should NOT error
            m[i] = originalToGeneral[normalAccessedOriginalIndices[i]];
        }

        // debug check
#ifndef NDEBUG
        for(auto kv : m) {
            // original index should correspond!
            assert(normalAccessedOriginalIndices[kv.first] == generalAccessedOriginalIndices[kv.second]);
        }
#endif

        return m;
    }

    namespace codegen {

        // recursive helper function
        std::vector<size_t> acc_helper(const std::shared_ptr<LogicalOperator>& op,
                                       const std::shared_ptr<LogicalOperator>& child,
                                       std::vector<size_t> requiredCols,
                                       bool dropOperators) {

            using namespace std;
            auto& logger = Logger::instance().logger("logical");

            if(!op)
                return requiredCols;

            // type to restrict columns of?
            assert(op);

            // get schemas
            auto inputRowType = op->parents().size() != 1 ? python::Type::UNKNOWN : op->getInputSchema().getRowType(); // could be also a tuple of one element!!!
            auto outputRowType = op->getOutputSchema().getRowType();

            vector<size_t> accCols; // indices of accessed columns from input row type!

            // udf operator? ==> selection possible!
            if(hasUDF(op.get())) {
                auto udfop = std::dynamic_pointer_cast<UDFOperator>(op);
                assert(udfop);

                switch(op->type()) {
                    case LogicalOperatorType::MAP:
                    case LogicalOperatorType::WITHCOLUMN:
                    case LogicalOperatorType::FILTER: {
                        // UDF access of input...
                        accCols = udfop->getUDF().getAccessedColumns();
                        break;
                    }
                    case LogicalOperatorType::MAPCOLUMN: {
                        // special case: mapColumn! ==> because it takes single column as input arg!
                        accCols = vector<size_t>{static_cast<unsigned long>(std::dynamic_pointer_cast<MapColumnOperator>(op)->getColumnIndex())};
                        break;
                    }
                    case LogicalOperatorType::RESOLVE: {
                        // special case: resolve!
                        // if normalParent is mapColumn, not necessary to ask for cols (because index already in accCols!)
                        auto rop = std::dynamic_pointer_cast<ResolveOperator>(op); assert(rop);
                        auto np = rop->getNormalParent(); assert(np);

                        if(np->type() != LogicalOperatorType::MAPCOLUMN) {
                            accCols = udfop->getUDF().getAccessedColumns();
                        }

                        break;
                    }
                    case LogicalOperatorType::IGNORE: {
                        // skip, nothing to do...
                        break;
                    }
                    default:
                        throw std::runtime_error("unsupported UDFOperator in projection pushdown " + op->name());
                }

#ifndef NDEBUG
                {
                    std::stringstream ss;
                    ss<<"operator "<<op->name()<<" accesses column indices: "<<accCols<<"\n";
                    if(!op->inputColumns().empty()) {
                        ss<<"\tequal to columns: ";
                        for(unsigned idx : accCols) {
                            if(idx >= op->inputColumns().size())
                                ss<<"INVALID INDEX ";
                            else
                                ss<<op->inputColumns()[idx]<<" ";
                        }
                    }
                    logger.debug(ss.str());
                }
#endif

                // only a map operator selects a number of columns, i.e. the lowest map operator determines the pushdown!
                // ==> because of nested maps, subselect from current requiredCols if they are not empty!
                if(op->type() == LogicalOperatorType::MAP) {
                    // some UDFs may only subselect columns but perform no operations on them...
                    // i.e. could restrict further, use the following code to find
                    if(!accCols.empty() && !udfop->getUDF().empty())  {
                        // don't update for rename, i.e. empty UDF!

                        // special case are resolve operators following, because they will change what columns are required. I.e.
                        // compute union with them.

                        set<size_t> cols(accCols.begin(), accCols.end());


                        // go over all resolvers following map and combine required columns with this map operator
                        if(op->children().size() == 1) {
                            auto cur_op = op->children().front();
                            while(cur_op->type() == LogicalOperatorType::RESOLVE) {
                                auto rop = std::dynamic_pointer_cast<ResolveOperator>(cur_op); assert(rop);
                                accCols = rop->getUDF().getAccessedColumns();
                                for(auto c : accCols)
                                    cols.insert(c);

                                if(cur_op->children().size() != 1)
                                    break;

                                cur_op = cur_op->children().front();
                            }
                        }

                        requiredCols = vector<size_t>(cols.begin(), cols.end());
                    }
                }

                // filter operator also enforces a requirement, because records could be dropped!
                // ==> i.e. add required columns of filters coming BEFORE map operations!
                if(op->type() == LogicalOperatorType::FILTER) {
                    set<size_t> cols(requiredCols.begin(), requiredCols.end());
                    for(auto idx : accCols)
                        cols.insert(idx);
                    requiredCols = vector<size_t>(cols.begin(), cols.end());
                }

                // special case withColumn operator:
                // if column to create is not overwriting an existing column, can drop it from required cols!
                if(op->type() == LogicalOperatorType::WITHCOLUMN) {
                    auto wop = std::dynamic_pointer_cast<WithColumnOperator>(op);
                    if(wop->creates_new_column()) {
                        requiredCols.erase(std::find(requiredCols.begin(), requiredCols.end(), wop->getColumnIndex()));
                    }
                }

                // if dropping is not allowed, then mapColumn/withColumn will be executed
                if (!dropOperators &&
                    (op->type() == LogicalOperatorType::MAPCOLUMN || op->type() == LogicalOperatorType::WITHCOLUMN ||
                     op->type() == LogicalOperatorType::FILTER || op->type() == LogicalOperatorType::RESOLVE)) {
                    set<size_t> cols(requiredCols.begin(), requiredCols.end());
                    for (auto idx : accCols)
                        cols.insert(idx);
                    requiredCols = vector<size_t>(cols.begin(), cols.end());
                }
            }

            if(op->type() == LogicalOperatorType::AGGREGATE) {
                auto aop = std::dynamic_pointer_cast<AggregateOperator>(op); assert(aop);

#warning "@TODO: implement proper analysis of the aggregate function to deduct which columns are required!"

                if(aop->aggType() == AggregateType::AGG_GENERAL || aop->aggType() == AggregateType::AGG_BYKEY) {
                    // Note: this here is a quick hack:
                    // simply require all columns.
                    // However, we need a better solution for the aggregate function...
                    // this will also involve rewriting...
                    auto rowtype = aop->getInputSchema().getRowType();

                    assert(rowtype.isTupleType());
                    set<size_t> cols(requiredCols.begin(), requiredCols.end());
                    for (int i = 0; i < rowtype.parameters().size(); ++i) {
                        cols.insert(i);
                    }
                    requiredCols = vector<size_t>(cols.begin(), cols.end());
                } else if(aop->aggType() == AggregateType::AGG_UNIQUE) {
                    // unique makes all the columns required: add them all in
                    auto rowtype = aop->getInputSchema().getRowType();

                    assert(rowtype.isTupleType());
                    set<size_t> cols(requiredCols.begin(), requiredCols.end());
                    for (int i = 0; i < rowtype.parameters().size(); ++i) {
                        cols.insert(i);
                    }
                    requiredCols = vector<size_t>(cols.begin(), cols.end());
                } else {
                    throw std::runtime_error("unknown aggregate type found in logical plan optimization!");
                }
            }

            // traverse
            if(op->type() == LogicalOperatorType::JOIN) {

                auto jop = std::dynamic_pointer_cast<JoinOperator>(op); assert(jop);
                vector<size_t> leftRet;
                vector<size_t> rightRet;

                // fetch num cols of operators BEFORE correction
                auto numLeftColumnsBeforePushdown = jop->left()->columns().size();

                if(requiredCols.empty()) {
                    leftRet = acc_helper(jop->left(), jop, requiredCols, dropOperators);
                    rightRet = acc_helper(jop->right(), jop, requiredCols, dropOperators);
                } else {
                    // need to split traversal up
                    set<size_t> reqLeft;
                    set<size_t> reqRight;

                    auto numLeftCols = jop->left()->getOutputSchema().getRowType().parameters().size();
                    auto numRightCols = jop->right()->getOutputSchema().getRowType().parameters().size();
                    for(auto idx : requiredCols) {
                        // required is key column + all that fall on left side for left
                        if(idx < numLeftCols)
                            reqLeft.insert(idx + (idx >= jop->leftKeyIndex())); // correct for join column drop
                    }
                    reqLeft.insert(jop->leftKeyIndex());

                    for(auto idx : requiredCols) {
                        // need to correct for left number of cols (join is over one key)
                        if(idx >= numLeftCols) {
                            assert(idx < numRightCols + numLeftCols);
                            reqRight.insert(idx - numLeftCols + (idx - numLeftCols >= jop->rightKeyIndex())); // correct for join column drop
                        }
                    }
                    reqRight.insert(jop->rightKeyIndex());

                    auto requiredLeftCols = vector<size_t>(reqLeft.begin(), reqLeft.end());
                    auto requiredRightCols = vector<size_t>(reqRight.begin(), reqRight.end());

                    leftRet = acc_helper(jop->left(), jop, requiredLeftCols, dropOperators);
                    rightRet = acc_helper(jop->right(), jop, requiredRightCols, dropOperators);
                }

                // rewrite of join now necessary...
                vector<size_t> ret = leftRet; // @TODO: correct indices??

                for(auto idx : rightRet) {
                    ret.push_back(idx + numLeftColumnsBeforePushdown); // maybe correct for key column?
                }

                //os<<"need to rewrite join here with combined "<<ret<<endl;
                // update join (because columns have changed)
                assert(jop);

                auto oldLeftKeyIndex = jop->leftKeyIndex();
                auto oldRightKeyIndex = jop->rightKeyIndex();

                jop->projectionPushdown();
                // construct map

                // Note: the weird - (i >= ...) is because of the key column being rearranged
                // i.e. remember the result of a join is
                // |left non key cols | key col | right non key cols |
                vector<size_t> colsToKeep;
                for(int i = 0; i < leftRet.size(); ++i)
                    if(i != jop->leftKeyIndex())
                        colsToKeep.push_back(leftRet[i] - (i >= jop->leftKeyIndex()));

                // keep the key column
                colsToKeep.push_back(numLeftColumnsBeforePushdown - 1);

                // fill in columns from right side to keep
                for(int i = 0; i < rightRet.size(); ++i) {
                    if(i != jop->rightKeyIndex())
                        colsToKeep.push_back(numLeftColumnsBeforePushdown + rightRet[i] - (i >= jop->rightKeyIndex()));
                }

                return colsToKeep;

            }

#ifndef NDEBUG
            {
                std::stringstream ss;
                ss<<"operator "<<op->name()<<" required column indices: "<<requiredCols<<"\n";
                if(!op->inputColumns().empty()) {
                    ss<<"\tequal to columns: ";
                    for(unsigned idx : requiredCols) {
                        if(idx >= op->inputColumns().size())
                            ss<<"INVALID INDEX ";
                        else
                            ss<<op->inputColumns()[idx]<<" ";
                    }
                }
                logger.debug(ss.str());
            }

            logger.debug("----\n time to recurse!\n----");
#endif


            // make sure only one parent
            assert(op->parents().size() <= 1);
            // special case CacheOperator, exec with parent nullptr if child is not null
            auto ret = op->type() == LogicalOperatorType::CACHE && child ?
                       acc_helper(nullptr, op, requiredCols, dropOperators) :
                       acc_helper(op->parent(), op, requiredCols, dropOperators);

#ifndef NDEBUG
            logger.debug("recursion done");
            {
                std::stringstream ss;
                ss<<"operator "<<op->name()<<" returned required column indices: "<<ret<<"\n";
                if(!op->inputColumns().empty()) {
                    ss<<"\tequal to columns: ";
                    for(unsigned idx : ret) {
                        if(idx >= op->inputColumns().size())
                            ss<<"INVALID INDEX ";
                        else
                            ss<<op->inputColumns()[idx]<<" ";
                    }
                }
                logger.debug(ss.str());
            }
#endif

            // CSV operator? do rewrite here!
            // ==> because it's a source node, use requiredCols!
            if(op->type() == LogicalOperatorType::FILEINPUT) {
                // rewrite csv here
                auto csvop = std::dynamic_pointer_cast<FileInputOperator>(op);

#ifndef NDEBUG
                logger.debug("input operator::");
                {
                    std::stringstream ss;
                    ss<<"operator "<<op->name()<<" required output column indices: "<<ret<<"\n";
                    if(!op->columns().empty()) {
                        ss<<"\tequal to columns: ";
                        for(unsigned idx : ret) {
                            if(idx >= op->columns().size())
                                ss<<"INVALID INDEX ";
                            else
                                ss<<op->columns()[idx]<<" ";
                        }
                    }
                    logger.debug(ss.str());
                }
#endif

                assert(csvop);
                auto inputRowType = csvop->getInputSchema().getRowType();
                vector<size_t> colsToSerialize;
                for (auto idx : ret) {
                    //if (idx < inputRowType.parameters().size())
                    //    colsToSerialize.emplace_back(idx);
                    // reverse project
                    auto read_index = csvop->reverseProjectToReadIndex(idx); // output idx -> read index1
                    colsToSerialize.emplace_back(read_index);
                }
                sort(colsToSerialize.begin(), colsToSerialize.end());

                return colsToSerialize;
            }

            // list other input operators here...
            // -> e.g. Parallelize, ... => could theoretically perform pushdown there as well
            if(op->type() == LogicalOperatorType::PARALLELIZE || op->type() == LogicalOperatorType::CACHE) {
                // this is a source operator
                // => no pushdown implemented here yet. Therefore, require all columns
                python::Type rowtype;
                if(op->type() == LogicalOperatorType::PARALLELIZE) {
                    auto pop = std::dynamic_pointer_cast<ParallelizeOperator>(op); assert(pop);
                    rowtype = pop->getOutputSchema().getRowType();
                } else {
                    auto cop = std::dynamic_pointer_cast<CacheOperator>(op); assert(cop);
                    rowtype = cop->getOutputSchema().getRowType();
                }

                vector<size_t> colsToSerialize;
                assert(rowtype.isTupleType());
                for(auto i = 0; i < rowtype.parameters().size(); ++i)
                    colsToSerialize.emplace_back(i);

                return colsToSerialize;
            }

            // make sure all source ops have been handled by above code!
            assert(!op->isDataSource());

            return ret;
//            // b.c. of some special unrolling etc. could happen that ret is smaller than accCols!
//            // -> make sure all requiredCols are within ret!
//            std::set<size_t> col_set(ret.begin(), ret.end());
//            for(auto col : requiredCols) {
//                col_set.insert(col);
//            }
//            ret = vector<size_t>(col_set.begin(), col_set.end());
//
//            // construct rewrite Map (??) apply ??
//            unordered_map<size_t, size_t> rewriteMap;
//            if(!ret.empty()) {
//                auto max_idx = *max_element(ret.begin(), ret.end()); // limit by max idx available
//                unsigned counter = 0;
//                for (unsigned i = 0; i <= max_idx; ++i) {
//                    if (std::find(ret.begin(), ret.end(), i) != ret.end()) {
//                        rewriteMap[i] = counter++;
//                    }
//                }
//            }
//
//            // following would rewrite, yet this func only delivers the columns required
//            std::sort(ret.begin(), ret.end());
//            return ret;
        }


        std::vector<size_t> StagePlanner::get_accessed_columns() const {

            // start with last operator
            std::vector<std::shared_ptr<LogicalOperator>> ops;
            if(_inputNode)
                ops.push_back(_inputNode);
            for(auto op : _operators)
                ops.push_back(op);
            // reverse!
            std::reverse(ops.begin(), ops.end());
//            // i.e. child -> parent -> grandparent -> ... -> input node
//            for(auto op : ops) {
//                if(hasUDF(op.get())) {
//
//                }
//            }

            auto node = ops.front();
            std::vector<size_t> cols;
            // start with requiring all columns from action node!
            // there's a subtle difference now b.c. output schema for csv was changed to str
            // --> use therefore input schema of the operator!
            auto num_cols = node->getInputSchema().getRowType().parameters().size();
            for(unsigned i = 0; i < num_cols; ++i)
                cols.emplace_back(i);
            return acc_helper(node, nullptr, cols, false); // dropOperators should be true??
        }

        static bool canPromoteFilterToCheck(const std::shared_ptr<FilterOperator>& fop) {
            assert(fop->type() == LogicalOperatorType::FILTER);

            // compiled?
            if(!fop->getUDF().isCompiled())
                return false;

            // can promote if parent is input operator!
            if(fop->parents().empty())
                throw std::runtime_error("filter has no parent, can't check");

            if(fop->parent()->isDataSource())
                return true;

            // can promote if all parents are filters!
            std::shared_ptr<LogicalOperator> node = fop;
            while(node->parent() && !node->parent()->isDataSource()) {
                if(node->type() != LogicalOperatorType::FILTER)
                    return false;
                node = node->parent();
            }
            return true;
        }

        static codegen::NormalCaseCheck filterToCheck(const std::shared_ptr<FilterOperator>& fop) {
            // serialize
            std::string serialized_filter;
#ifdef BUILD_WITH_CEREAL
            std::ostringstream oss(std::stringstream::binary);
            {
                cereal::BinaryOutputArchive ar(oss);
                ar(fop);
                // ar going out of scope flushes everything
            }
            auto bytes_str = oss.str();
            serialized_filter = bytes_str;
#else
            serialized_filter = fop->to_json().dump();
#endif
            auto acc_cols = fop->getUDF().getAccessedColumns();
            return NormalCaseCheck::FilterCheck(acc_cols, serialized_filter);
        }

        bool StagePlanner::promoteFilters(std::vector<Row>* filtered_sample) {
            auto& logger = Logger::instance().logger("optimizer");

            logger.debug("carrying out potential filter promotion");

            if(!_inputNode) {
                logger.warn("no input node, skip optimization.");
                return false;
            }

            // Need to pushdown filters (so canPromoteFilter check works).
            //             // basically use just on this stage the logical optimization pipeline
            //            auto logical_opt = std::make_unique<LogicalOptimizer>(options());
            //
            //            // logically optimize pipeline, this performs reordering/projection pushdown etc.
            //            opt_ops = logical_opt->optimize(opt_ops, true); // inplace optimization


            size_t original_sample_size = 0;
            std::vector<Row> original_sample;
            if(_inputNode->type() == LogicalOperatorType::FILEINPUT) {
                original_sample_size = std::dynamic_pointer_cast<FileInputOperator>(_inputNode)->storedSampleRowCount();

                if(filtered_sample)
                    original_sample = std::dynamic_pointer_cast<FileInputOperator>(_inputNode)->getSample(original_sample_size);
            }

            if(0 == original_sample_size) {
                logger.debug("skip because empty original sample");
                return false;
            }

            std::vector<std::shared_ptr<LogicalOperator>> operators_post_op;

            bool filter_promo_applied = false;
            bool return_sample = filtered_sample != nullptr;
            std::vector<size_t> indices_to_keep(original_sample_size); // track which of the original samples to keep.
            // init indices as all
            for(unsigned i = 0; i < original_sample_size; ++i)
                indices_to_keep[i] = i;

            // the current sample (to assign the input operator to)
            std::vector<Row> current_input_sample; // empty for now.

            // check if there is at least one filter operator!
            // -> carry then repeated filters out!
            auto node = _inputNode;
            while(node && !node->children().empty()) {
                assert(node->children().size() == 1); // only single child yet supported...

                logger.debug(node->name() + " has child: " + node->children().front()->name());

                auto next_node = node->children().front();

                // is it a filter? can the filter be promoted?
                if(node->type() == LogicalOperatorType::FILTER) {
                    auto filter_node = std::dynamic_pointer_cast<FilterOperator>(node);
                    // get sample! is it non-empty and smaller than the original sample size?
                    std::vector<size_t> indices_kept;
                    auto samples_post_filter = filter_node->getSample(original_sample_size, true, &indices_kept);
                    logger.debug("sample size post-filter: " + pluralize(samples_post_filter.size(), "row"));

                    if(!samples_post_filter.empty()) {
                        // apply indices_kept to original indices
                        std::vector<size_t> new_indices;
                        new_indices.reserve(indices_kept.size());
                        for(auto idx : indices_kept) {
                            new_indices.push_back(indices_to_keep[idx]);
                        }
                        indices_to_keep = new_indices;

                        // update:
                        current_input_sample.clear();
                        current_input_sample.reserve(indices_to_keep.size());
                        for(auto idx : indices_to_keep) {
                            assert(idx < original_sample.size());

                            auto row = original_sample[idx];
                            // ensure row type (w. columns)
                            if(!row.getRowType().isRowType())
                                row = row.with_columns(_inputNode->columns());

                            current_input_sample.push_back(row);
                        }
                    }


                    // @TODD: There's two possibilities here:
                    // [1] if sample is empty, replace pipeline with simple filter node throwing normal-case if filter condition is not met.
                    // [2] sample is empty, ignore filter promot and continue with other majority case. Above is more aggressive, the other one less.

                    if(!samples_post_filter.empty() && samples_post_filter.size() < original_sample_size) {
                        logger.info("filter is candidate for promotion, reduced sample size from " + std::to_string(original_sample_size) + " -> " + std::to_string(samples_post_filter.size()));

                        // check that filter is only dependent on input operator, and not other operator (not yet supported)
                        // if so, then add a new check for the filter. -> if the check passes (and filter=true), stay in normal case
                        // can also remove filter from pipeline, because check is identical with filter, i.e. when check is true also filter will be true!
                        // if check doesn't pass, no problem. Row anyway not processed, skip. no problem.
                        // but set to
                        if(canPromoteFilterToCheck(filter_node) && _inputNode && _inputNode->type() == LogicalOperatorType::FILEINPUT) {

                            // promote ONLY if there's a significant schema change with filter promotion.
                            // else, there's no benefit.
                            // @TODO:
                            logger.debug("add here logic so no accidental promo happens for flights query...");

                            // get additional information about filter:
                            // i.e., rowtype and which columns it accesses
                            std::stringstream ss;
                            ss<<"promoted filter operator, detailed info::\n";
                            ss<<"filter input columns: "<<filter_node->inputColumns()<<"\n";
                            ss<<"filter output columns: "<<filter_node->columns()<<"\n";
                            ss<<"filter input schema: "<<filter_node->getInputSchema().getRowType().desc()<<"\n";

                            // retype filter operator
                            std::vector<std::string> acc_column_names;
                            std::vector<python::Type> acc_col_types;
                            auto acc_cols = filter_node->getUDF().getAccessedColumns(false);
                            auto col_types = filter_node->getInputSchema().getRowType().isRowType() ? filter_node->getInputSchema().getRowType().get_column_types() : filter_node->getInputSchema().getRowType().parameters();
                            for(auto idx : acc_cols) {
                                acc_column_names.push_back(filter_node->inputColumns()[idx]);

                                if(filter_node->getInputSchema().getRowType().isRowType()) {
                                    assert(filter_node->inputColumns()[idx] == filter_node->getInputSchema().getRowType().get_column_names()[idx]);
                                }
                                acc_col_types.push_back(col_types[idx]);
                            }

                            // however, these here should show correct columns/types.
                            ss<<"filter accessed input columns: "<<acc_column_names<<"\n";
                            ss<<"filter accessed, condensed input schema: "<<python::Type::makeTupleType(acc_col_types).desc()<<"\n";
                            logger.debug(ss.str());

                            // remove filter and add check!
                            filter_node->remove();

                            // retype now with input & make smaller
                            RetypeConfiguration conf;
                            conf.columns = _inputNode->columns();
                            conf.row_type = _inputNode->getOutputSchema().getRowType();
                            conf.is_projected = true;
                            auto ret = filter_node->retype(conf);
                            if(!ret) {
                                throw std::runtime_error("did not succeed retyping filter supposed to be promoted to check");
                            }

                            // no need for parents etc.
                            auto check = filterToCheck(filter_node);
                            check.colNames = acc_column_names; // <-- accessed column names?
                            _checks.push_back(check);
                            filter_promo_applied = true;

                            assert(current_input_sample.size() == indices_to_keep.size());

                            // manipulate input node sample!
                            std::dynamic_pointer_cast<FileInputOperator>(_inputNode)->setRowsSample(current_input_sample);
                            logger.debug("replaced samples in input operator with " + pluralize(current_input_sample.size(), "filtered sample"));
                            logger.debug("promoted filter to check: \n" + core::withLineNumbers(filter_node->getUDF().getCode()));

                            node = nullptr;
                        }
                    }
                }
                if(node && node->getID() != _inputNode->getID())
                    operators_post_op.push_back(node);

                // go on...
                node = next_node;
            }
            if(node)
                operators_post_op.push_back(node);
            _operators = operators_post_op;

            // output filtered rows if desired (and filter promo was applied)
            if(filtered_sample && filter_promo_applied) {
                if(indices_to_keep.size() == current_input_sample.size())
                    *filtered_sample = current_input_sample;
                else {
                    *filtered_sample = std::vector<Row>();
                    for(auto idx : indices_to_keep) {
                        assert(idx < original_sample.size());
                        filtered_sample->push_back(original_sample[idx]);
                    }
                }
            }

            return filter_promo_applied;
        }

        bool StagePlanner::retypeOperators(const std::vector<Row>& sample,
                                           const std::vector<std::string>& sample_columns, bool use_sample) {
            auto& logger = Logger::instance().logger("specializing stage optimizer");

            // @TODO: stats on types for sample. Use this to retype!
            // --> important first step!
            std::unordered_map<std::string, size_t> counts;
            std::unordered_map<python::Type, size_t> t_counts;
            for(const auto& row : sample) {
                counts[row.getRowType().desc()]++;
                t_counts[row.getRowType()]++;
            }
            // for(const auto& keyval : counts) {
            //     os<<keyval.second<<": "<<keyval.first<<std::endl;
            // }

            if(use_sample && sample.empty()) {
                logger.info("Got empty sample, skipping optimization.");
                return true;
            }

            auto projectedColumns = _inputNode->columns();

            // detect majority type
            // detectMajorityRowType(const std::vector<Row>& rows, double threshold, bool independent_columns)
            python::Type majType=python::Type::UNKNOWN, projectedMajType=python::Type::UNKNOWN;
            if(use_sample && !sample.empty()) {
                logger.info("Detecting majority type using " + pluralize(sample.size(), "sample") + " with threshold=" + std::to_string(_nc_threshold));
                majType = detectMajorityRowType(sample, _nc_threshold, true, _useNVO);
                projectedMajType = majType;
                if(sample.front().getRowType().isRowType()) // <-- samples should have same type.
                    projectedColumns = sample.front().getRowType().get_column_names();
            } else {
                majType = _inputNode->getOutputSchema().getRowType();
                projectedMajType = majType;

                // special case, fileinput operator -> use normal case?

            }

            if(use_sample) {
                assert(majType.isTupleType());
                assert(projectedMajType.isTupleType());
                size_t num_columns_before_pushdown = majType.parameters().size();
                size_t num_columns_after_pushdown = projectedMajType.parameters().size();

#ifndef NDEBUG
                // check how many rows adhere to the normal-case type.
                // (can upcast to normal-case type)
                size_t num_passing = 0;
                std::vector<Row> majRows;
                for(auto row: sample) {
                    auto row_tuple_type = row.getRowType();
                    if(row_tuple_type.isRowType())
                        row_tuple_type = row_tuple_type.get_columns_as_tuple_type();
                    if(python::canUpcastToRowType(deoptimizedType(row_tuple_type), deoptimizedType(majType))) {
                        num_passing++;
                        majRows.push_back(row);
                    }
                }
                std::stringstream sample_stream;
                for(const auto& row: sample) {
                    auto row_as_str = row.toJsonString(sample_columns); //row.toPythonString();
                    sample_stream<<row_as_str<<"\n";
                }

                std::string sample_dbg_save_path = "extracted_sample.ndjson";
                stringToFile(sample_dbg_save_path, sample_stream.str());

                logger.debug("saved obtained sample to " + sample_dbg_save_path + " as ndjson");
                logger.debug("Of " + pluralize(sample.size(), "sample row") + ", " + std::to_string(num_passing) + " adhere to detected majority type.");
                for(unsigned i = 0; i < std::min(majRows.size(), 5ul); ++i)
                    os<<majRows[i].toPythonString()<<std::endl;

                // check if any forkevents are found in the sample
                std::vector<std::string> rows_as_python_strings;
                size_t fork_events_found = 0;
                for(auto row : sample) {
                    rows_as_python_strings.push_back(row.toPythonString());
                    if(rows_as_python_strings.back().find("ForkEvent") != std::string::npos)
                        fork_events_found++;
                }
                os<<"Found forkevents: "<<fork_events_found<<"x"<<std::endl;
#endif
            }


            // the detected majority type here is BEFORE projection pushdown.
            // --> therefore restrict it to the type of the input operator.
            // os<<"Majority detected row type is: "<<projectedMajType.desc()<<std::endl;

            // if majType of sample is different from input node type input sample -> retype!
            // also need to restrict type first!
            {
                std::stringstream ss;
                ss<<__FILE__<<":"<<__LINE__<<" Performing Retyping with type: "<<projectedMajType.desc()<<" columns: "<<projectedColumns;
                logger.debug(ss.str());
            }

            auto optimized_operators = retypeUsingOptimizedInputSchema(projectedMajType, projectedColumns);

            // overwrite internal operators to apply subsequent optimizations
            _inputNode = _inputNode ? optimized_operators.front() : nullptr;
            _operators = _inputNode ? std::vector<std::shared_ptr<LogicalOperator>>{optimized_operators.begin() + 1,
                                                                          optimized_operators.end()}
                                    : optimized_operators;


            // run validation after forcing majority sample based type
            auto validation_rc = validatePipeline();
            logger.debug(std::string("post-specialization pipeline validation: ") + (validation_rc ? "ok" : "failed"));
            return validation_rc;
        }
    }
}