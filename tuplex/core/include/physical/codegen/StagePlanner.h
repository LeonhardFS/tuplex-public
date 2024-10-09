//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_STAGEPLANNER_H
#define TUPLEX_STAGEPLANNER_H

#include <physical/execution/TransformStage.h>
#include <logical/LogicalOperator.h>

// this here is the class to create a specialized version of a stage.
namespace tuplex {
    namespace codegen {

        struct DetectionStats {
            size_t num_rows;
            size_t num_columns_min;
            size_t num_columns_max;
            std::vector<bool> is_column_constant;
            Row constant_row;

            DetectionStats() : num_rows(0),
                               num_columns_min(std::numeric_limits<size_t>::max()),
                               num_columns_max(std::numeric_limits<size_t>::min()) {}
            std::vector<size_t> constant_column_indices(bool return_optional_constants=false) const {
                std::vector<size_t> v;
                for(unsigned i = 0; i < is_column_constant.size(); ++i) {
                    if(is_column_constant[i]) {

                        // skip options if not explicitly desired.
                        if(constant_row.get(i).getType().isOptionType() && !return_optional_constants)
                            continue;

                        // return index
                        v.push_back(i);
                    }
                }
                return v;
            }

            inline python::Type specialize_row_type(const python::Type& row_type) const {
                assert(row_type.isRowType() || row_type.isTupleType());
                assert(extract_columns_from_type(row_type) == constant_row.getNumColumns());

                std::vector<python::Type> colTypes = row_type.isRowType() ? row_type.get_column_types() : row_type.parameters();

                // fill in constant valued types
                for(auto idx : constant_column_indices()) {
                    auto underlying_type = constant_row.getType(idx);
                    auto underlying_constant = constant_row.get(idx).desc();

                    // options can be simplified depending on the constant value
                    auto constant_type = python::Type::makeConstantValuedType(underlying_type, underlying_constant);

                    // _Constant[Null, None] --> null
                    // _Constant[Option[T], ...] --> null or T
                    constant_type = simplifyConstantType(constant_type);

                    colTypes[idx] = constant_type;
                }
                return python::Type::makeTupleType(colTypes);
            }

            void detect(const std::vector<Row>& rows) {
                if(rows.empty())
                    return;

                // init?
                if(0 == num_rows) {
                    constant_row = rows.front();
                    // mark everything as constant!
                    is_column_constant = std::vector<bool>(constant_row.getNumColumns(), true);
                }
                size_t row_number = 0;
                for(const auto& row : rows) {

                    // ignore small rows
                    if(row.getNumColumns() < constant_row.getNumColumns())
                        continue;

                    // compare current row with constant row.
                    for(unsigned i = 0; i < std::min(constant_row.getNumColumns(), row.getNumColumns()); ++i) {
                        // field comparisons might be expensive, so compare only if not marked yet as false...
                        // field different? replace!
                        if(is_column_constant[i] && constant_row.get(i).withoutOption() != row.get(i).withoutOption()) {
                            // allow option types to be constant!
                            if(constant_row.get(i).isNull() && !row.get(i).isNull()) {
                                // saved row value is null -> replace with constant!
                                constant_row.set(i, row.get(i).makeOptional());
                            } else if(row.get(i).isNull()) {
                                // update to make optional to indicate null
                                if(!constant_row.get(i).getType().isOptionType()) {
                                    constant_row.set(i, constant_row.get(i).makeOptional());
                                }
                            } else  {
                                is_column_constant[i] = false;
                            }
                        }
                    }
                    row_number++;

                    // cur row larger? replace!

                    num_columns_min = std::min(num_columns_min, row.getNumColumns());
                    num_columns_max = std::max(num_columns_max, row.getNumColumns());
                }

                num_rows += rows.size();
            }
        };

        /*!
         * this class creates a specialized version of a stage
         */
        class StagePlanner {
        public:

            /*!
             * constructor, taking nodes of a stage in
             * @param inputNode the input node of the stage (parent of first operator)
             * @param operators operators following the input node.
             */
            StagePlanner(const std::shared_ptr<LogicalOperator>& inputNode,
                         const std::vector<std::shared_ptr<LogicalOperator>>& operators,
                         double nc_threshold) : _inputNode(inputNode),
                                                _operators(operators), _nc_threshold(nc_threshold),
                                                _useNVO(false), _useConstantFolding(false), _useDelayedParsing(false),
                                                _useSparsifyStructs(false), _simplifyLargeStructs(false), _largeStructThreshold(0) {
                assert(inputNode);
                for(auto op : operators)
                    assert(op);
                enableAll();

                // no optimizations carried out yet, hence store the original types for later lookup.
                assert(inputNode);
                if(LogicalOperatorType::FILEINPUT == inputNode->type()) {
                    auto fop = std::dynamic_pointer_cast<FileInputOperator>(inputNode);
                    _unprojected_unoptimized_row_type = fop->getInputSchema().getRowType();
                } else {
                    _unprojected_unoptimized_row_type = inputNode->getOutputSchema().getRowType();
                }
            }

             /*!
             * create optimized, specialized pipeline, i.e. first operator returned is the input operator.
             * The others are (possibly rearranged) operators. Operators are clones/copies of original operators. I.e.
             * planner is non-destructive.
             * @param use_sample if true, then sample is used to carry out optimizations. Else, relies on the type of the first operator given (which may be a normal-case type).
             */
            void optimize(bool use_sample=true);

            std::vector<NormalCaseCheck> checks() const {
                return _checks;
            }

            std::vector<std::shared_ptr<LogicalOperator>> optimized_operators() const {
                return _operators;
            }

            std::shared_ptr<LogicalOperator> input_node() const {
                return _inputNode;
            }

            /*!
             * shortcut to enable all optimizations
             */
            void enableAll() {
                enableNullValueOptimization();
                enableConstantFoldingOptimization();
                enableDelayedParsingOptimization();
                enableFilterPromoOptimization();
                enableSparsifyStructsOptimization();
                enableSimplifyLargeStructs(20);
            }

            void disableAll() {
                _useNVO = false;
                _useConstantFolding = false;
                _useDelayedParsing = false;
                _useFilterPromo = false;
                _useSparsifyStructs = false;
                _simplifyLargeStructs = true;
                _largeStructThreshold = 0;
            }

            void enableNullValueOptimization() { _useNVO = true; }
            void enableConstantFoldingOptimization() { _useConstantFolding = true; }
            void enableDelayedParsingOptimization() { _useDelayedParsing = true; }
            void enableFilterPromoOptimization() { _useFilterPromo = true; }
            void enableSparsifyStructsOptimization() { _useSparsifyStructs = true; }
            void enableSimplifyLargeStructs(size_t threshold) {
                if(threshold == 0) {
                    throw std::runtime_error("Invalid threshold of 0 submitted, set at least to 1.");
                }
                _simplifyLargeStructs = true;
                _largeStructThreshold = threshold;
            }

            std::map<int, int> normalToGeneralMapping() const { return _normalToGeneralMapping; }

            inline std::string info_string() const {
                std::stringstream ss;
                ss<<"Stage Planner active optimizations:\n";
                std::vector<std::string> opt_names;
                if(_useNVO)
                    opt_names.push_back("null-value optimization");
                if(_useConstantFolding)
                    opt_names.push_back("constant-folding");
                if(_useDelayedParsing)
                    opt_names.push_back("delayed-parsing");
                if(_useFilterPromo)
                    opt_names.push_back("filter-promotion");
                if(_useSparsifyStructs)
                    opt_names.push_back("struct-sparsification");
                if(_simplifyLargeStructs)
                    opt_names.push_back("simplify-large-structs[th=" + std::to_string(_largeStructThreshold) + "]");
                ss<<opt_names;
                return ss.str();
            }

            // helper functions regarding row types
            /*!
             * @return returns the original, unoptimized input row type
             */
            inline python::Type unprojected_unoptimized_row_type() const {
                return _unprojected_unoptimized_row_type;
            }
            python::Type projected_unoptimized_row_type() const {
                auto unopt = unprojected_unoptimized_row_type();

                // calc via projection matrix (only for fileinput)
                if(_inputNode && LogicalOperatorType::FILEINPUT == _inputNode->type()) {
                    auto col_types = unopt.parameters();
                    auto fop = std::dynamic_pointer_cast<FileInputOperator>(_inputNode);
                    auto cols_to_serialize = fop->columnsToSerialize();
                    assert(cols_to_serialize.size() == fop->inputColumnCount());
                    assert(cols_to_serialize.size() == col_types.size());
                    std::vector<python::Type> proj_col_types;
                    for(unsigned i = 0; i < cols_to_serialize.size(); ++i) {
                        if(cols_to_serialize[i])
                            proj_col_types.push_back(col_types[i]);
                    }
                    return python::Type::makeTupleType(proj_col_types);
                } else {
                    return unopt;
                }
            }
            python::Type unprojected_optimized_row_type() const {
                assert(_inputNode);
                if(LogicalOperatorType::FILEINPUT == _inputNode->type()) {
                    auto fop = std::dynamic_pointer_cast<FileInputOperator>(_inputNode);
                    auto t = fop->getOptimizedInputSchema().getRowType();
                    assert(extract_columns_from_type(t) == extract_columns_from_type(unprojected_unoptimized_row_type()));
                    return t;
                } else {
                    // normal-case is always the propagated schema
                    return _inputNode->getOutputSchema().getRowType();
                }
            }

            python::Type projected_optimized_row_type() const {
                assert(_inputNode);
                if(LogicalOperatorType::FILEINPUT == _inputNode->type()) {
                    auto fop = std::dynamic_pointer_cast<FileInputOperator>(_inputNode);
                    return fop->getOptimizedOutputSchema().getRowType();
                } else {
                    // normal-case is always the propagated schema
                    return _inputNode->getOutputSchema().getRowType();
                }
            }

            bool promoteFilters(std::vector<Row>* filtered_sample=nullptr);

        private:
            std::shared_ptr<LogicalOperator> _inputNode;
            std::vector<std::shared_ptr<LogicalOperator>> _operators;
            std::vector<NormalCaseCheck> _checks;

            python::Type _unprojected_unoptimized_row_type;

            double _nc_threshold;
            bool _useNVO;
            bool _useConstantFolding;
            bool _useDelayedParsing;
            bool _useFilterPromo;
            bool _useSparsifyStructs;
            bool _simplifyLargeStructs;
            size_t _largeStructThreshold;

            // helper when normal-case is specialized to yield less rows than general case
            std::map<int, int> _normalToGeneralMapping;

            // helper functions
            std::vector<Row> fetchInputSample();

            /*!
             * perform null-value optimization & return full pipeline. First op is inputnode
             * @param input_row_type the input row type to use to retype the pipeline.
             * @param input_column_names the input columns to use for retyping the UDF.
             * @return vector of operators
             */
            std::vector<std::shared_ptr<LogicalOperator>> retypeUsingOptimizedInputSchema(const python::Type& input_row_type,
                                                                                          const std::vector<std::string>& input_column_names);

            /*!
             * perform constant folding optimization using sample.
             */
            std::vector<std::shared_ptr<LogicalOperator>> constantFoldingOptimization(const std::vector<Row>& sample,
                                                                                      const std::vector<std::string>& sample_columns);

            /*!
             * perform sparsification of structured dictionaries.
             * @param sample sample to use for tracing sparse accesses
             * @param sample_columns optional column description for sample, best to pass
             * @param relax_for_sample it could happen that not all samples will work with the sparse type detected.
             * To avoid overfitting, relax the type to adjust top-level struct paths to maybe_present from not_present/always_present.
             * @return retyped & reoptimized operators.
             */
            std::vector<std::shared_ptr<LogicalOperator>> sparsifyStructs(std::vector<Row> sample,
                    const option<std::vector<std::string>>& sample_columns=tuplex::option<std::vector<std::string>>::none,
                    bool relax_for_sample = true);

            /*!
             * optimization pass to convert very large struct / sparse struct to generic dict if (nested) max_field count is exceeded.
             * @param max_field_count
             * @return retyped & reoptimized operators.
             */
            std::vector<std::shared_ptr<LogicalOperator>> simplifyLargeStructs(size_t max_field_count);

            bool retypeOperators(const std::vector<Row>& sample,
                                 const std::vector<std::string>& sample_columns,
                                 bool use_sample);

            python::Type get_specialized_row_type(const std::shared_ptr<LogicalOperator>& inputNode, const DetectionStats& ds) const;

            /*!
             * creates a mapping between a column index of the normal case to the column index of the general case.
             * @param normalAccessedOriginalIndices the i-th entry is the original column index AFTER pushdown of the i-th column in the normal case
             * @param generalAccessedOriginalIndices  the j-th entry is the original column index AFTER pushdown of the j-th column in the general case
             * @return mapping
             */
            std::map<int, int> createNormalToGeneralMapping(const std::vector<size_t>& normalAccessedOriginalIndices,
                                                            const std::vector<size_t>& generalAccessedOriginalIndices);

            /*!
             * perform filter-reordering using sample selectivity
             */
            std::vector<std::shared_ptr<LogicalOperator>> filterReordering(const std::vector<Row>& sample);

            ContextOptions options() const {
                auto opt = ContextOptions::defaults();
                // enable all logical optimizations??
                // @TODO: pass this down somewhow?
                opt.set("tuplex.optimizer.filterPushdown", "true");
                opt.set("tuplex.optimizer.operatorReordering", "true");
                opt.set("tuplex.optimizer.selectionPushdown", "true");
                opt.set("tuplex.optimizer.constantFoldingOptimization", "true");
                opt.set("tuplex.optimizer.sparsifyStructs", "true");
                return opt;
            }

            std::vector<size_t> get_accessed_columns() const; // get accessed columns of operators for first/input node

            static std::vector<size_t> get_accessed_columns(const std::vector<std::shared_ptr<LogicalOperator>>& ops);

            bool validatePipeline();

            bool input_schema_contains_structs(const Schema& schema);
        };
    }

    // HACK!
    //extern bool hyperspecialize(TransformStage *stage, const URI& uri, size_t file_size, double nc_threshold, size_t sample_limit=std::numeric_limits<size_t>::max(), bool enable_cf=true);
    extern bool hyperspecialize(TransformStage *stage,
                                const URI& uri,
                                size_t file_size,
                                size_t sample_limit,
                                size_t strata_size,
                                size_t samples_per_strata,
                                const codegen::StageBuilderConfiguration& conf);

    inline RetypeConfiguration retype_configuration_from(const python::Type& projected_row_type, const std::vector<std::string>& projected_columns) {
        // need to restrict potentially?
        RetypeConfiguration r_conf;
        r_conf.is_projected = true;
        r_conf.row_type = projected_row_type;
        r_conf.columns = projected_columns;
        r_conf.remove_existing_annotations = true; // remove all annotations (except the column restore ones?)

        if(extract_columns_from_type(r_conf.row_type) != r_conf.columns.size() && r_conf.row_type.isRowType() && !r_conf.row_type.get_column_names().empty())
            r_conf.columns = r_conf.row_type.get_column_names();

        // make sure all existing columns get a type, if the given columns are less than the current ones -> expand the column + type description.
        //if(extract_columns_from_type())

        // perform quick sanity check
        if(!r_conf.columns.empty()) {
            auto n_cols_row_type = extract_columns_from_type(r_conf.row_type);
            auto n_cols_count = r_conf.columns.size();
            assert(n_cols_row_type == n_cols_count);
        }
        return r_conf;
    }
}

#endif