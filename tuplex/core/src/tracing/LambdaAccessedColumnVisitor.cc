//
// Created by leonhards on 6/24/24.
//

#include <tracing/LambdaAccessedColumnVisitor.h>

namespace tuplex {
    void LambdaAccessedColumnVisitor::visit(tuplex::NSubscription *n) {
        if(!n)
            return;

        preOrder(n);
        ApatheticVisitor::visit(n);
        postOrder(n);
    }

    void LambdaAccessedColumnVisitor::visit(tuplex::NIfElse *node) {
        // annotation? stop visit then.
        if(node->hasAnnotation()) {

            // visit at all?
            if(node->annotation().numTimesVisited == 0)
                return;

            // visit cond
            node->_expression->accept(*this);

            if(!node->_then->hasAnnotation() || node->_then->annotation().numTimesVisited > 0)
                node->_then->accept(*this);

            if(node->_else && (!node->_else->hasAnnotation() || node->_else->annotation().numTimesVisited > 0))
                node->_else->accept(*this);

            return;
        }

        // regular visit
        preOrder(node);
        ApatheticVisitor::visit(node);
        postOrder(node);
    }

    void LambdaAccessedColumnVisitor::visit(tuplex::NCall *node) {

        // special case: row.get(...)

        auto call = (NCall*)node;
        if(call->_func->type() == ASTNodeType::Attribute) {
            // -> use this (together with constant string to mark as partial)
            auto attr = (NAttribute*)call->_func.get();

            // if unknown, that means not typed (e.g. branch not visited)
            if(attr->_attribute->_name == "get" && attr->_value->type() == ASTNodeType::Identifier && attr->_value->getInferredType().withoutOption().isRowType()) {

                auto id_name = ((NIdentifier*)attr->_value.get())->_name;

                if(std::find(_argNames.begin(), _argNames.end(), id_name) != _argNames.end()) {
                    if(!call->_positionalArguments.empty()) {

                        // call visit on positional args (others do not need to get visited)
                        for(auto& pos_arg : call->_positionalArguments)
                            pos_arg->accept(*this);

                        auto get_arg = call->_positionalArguments.front().get();
                        if(get_arg->type() == ASTNodeType::String) {
                            // string/key index...
                            auto key = ((NString*)get_arg)->value();
                            auto column_names = attr->_value->getInferredType().withoutOption().get_column_names();
                            auto idx = indexInVector(key, column_names);
                            if(idx >= 0)
                                _argSubscriptIndices[id_name].push_back(idx);

                            return; // stop exploring deeper.
                        }

                        if(get_arg->type() == ASTNodeType::Number && get_arg->getInferredType() == python::Type::I64) {
                            // integer index
                            throw std::runtime_error("not yet implemented");
                            return;
                        }
                    }
                }
                std::cout<<"Found"<<std::endl;
            }
        }


        // regular visit
        preOrder(node);
        ApatheticVisitor::visit(node);
        postOrder(node);
    }


    std::vector<size_t> LambdaAccessedColumnVisitor::getAccessedIndices() const {

        std::set<size_t> idxs;
        assert(_multiArgs.has_value());

        if(0 == _numColumns)
            return {};

        // first check what type it is
        if(!_multiArgs.value()) {
            assert(_argNames.size() == 1);
            std::string argName = _argNames.front();
            if(_argFullyUsed.at(argName)) {
                for(unsigned i = 0; i < _numColumns; ++i)
                    idxs.insert(i);
            } else {
                auto v_idxs = _argSubscriptIndices.at(argName);
                idxs = std::set<size_t>(v_idxs.begin(), v_idxs.end());
            }
        } else {
            // simple, see which params are fully used.
            for(unsigned i = 0; i < _argNames.size(); ++i) {
                if(_argFullyUsed.at(_argNames[i]))
                    idxs.insert(i);
            }
        }

        return std::vector<size_t>(idxs.begin(), idxs.end());
    }

    void LambdaAccessedColumnVisitor::preOrder(ASTNode *node) {
        switch(node->type()) {
            case ASTNodeType::Lambda: {
                assert(!_singleLambda);
                _singleLambda = true;

                NLambda* lambda = (NLambda*)node;
                auto itype = lambda->_arguments->getInferredType();
                assert(itype.isTupleType());

                // are multiple arguments used or not?
                _multiArgs = lambda->_arguments->_args.size() > 1;
                if(_multiArgs.value()) {
                    _numColumns = lambda->_arguments->_args.size();
                } else {

                    // normalize tuple type argument
                    auto normalized_input_row_type = itype.isTupleType() && itype.parameters().size() == 1 &&
                                                     itype.parameters().front().isTupleType() &&
                                                     itype.parameters().front() != python::Type::EMPTYTUPLE ? itype.parameters().front() : itype;
                    if(itype.isTupleType() && itype.parameters().size() == 1 && itype.parameters().front().isRowType())
                        normalized_input_row_type = itype.parameters().front();

                    if(normalized_input_row_type.isTupleType())
                        _numColumns = normalized_input_row_type.parameters().size();
                    else if(normalized_input_row_type.isRowType())
                        _numColumns = normalized_input_row_type.get_column_count();
                    else
                        _numColumns = 1;
                }

                // fetch identifiers for args
                for(const auto &argNode : lambda->_arguments->_args) {
                    assert(argNode->type() == ASTNodeType::Parameter);
                    NIdentifier* id = ((NParameter*)argNode.get())->_identifier.get();
                    _argNames.push_back(id->_name);
                    _argFullyUsed[id->_name] = false;
                    _argSubscriptIndices[id->_name] = std::vector<size_t>();
                }

                break;
            }

            case ASTNodeType::Function: {
                assert(!_singleLambda);
                _singleLambda = true;

                NFunction* func = (NFunction*)node;
                auto itype = func->_parameters->getInferredType();
                assert(itype.isTupleType());

                // are multiple arguments used or not?
                _multiArgs = func->_parameters->_args.size() > 1;
                if(_multiArgs.value()) {
                    _numColumns = func->_parameters->_args.size();
                } else {
                    // normalize tuple type argument
                    auto normalized_input_row_type = itype.isTupleType() && itype.parameters().size() == 1 &&
                                                     itype.parameters().front().isTupleType() &&
                                                     itype.parameters().front() != python::Type::EMPTYTUPLE ? itype.parameters().front() : itype;
                    if(itype.isTupleType() && itype.parameters().size() == 1 && itype.parameters().front().isRowType())
                        normalized_input_row_type = itype.parameters().front();

                    if(normalized_input_row_type.isTupleType())
                        _numColumns = normalized_input_row_type.parameters().size();
                    else if(normalized_input_row_type.isRowType())
                        _numColumns = normalized_input_row_type.get_column_count();
                    else
                        _numColumns = 1;
                }


                // fetch identifiers for args
                for(const auto &argNode : func->_parameters->_args) {
                    assert(argNode->type() == ASTNodeType::Parameter);
                    NIdentifier* id = ((NParameter*)argNode.get())->_identifier.get();
                    _argNames.push_back(id->_name);
                    _argFullyUsed[id->_name] = false;
                    _argSubscriptIndices[id->_name] = std::vector<size_t>();
                }

                break;
            }

            case ASTNodeType::Identifier: {
                NIdentifier* id = (NIdentifier*)node;
                if(parent()->type() != ASTNodeType::Parameter &&
                   parent()->type() != ASTNodeType::Subscription) {
                    // the actual identifier is fully used, mark as such!
                    _argFullyUsed[id->_name] = true;
                }

                break;
            }

                // check whether function with single parameter which is a tuple is accessed.
            case ASTNodeType::Subscription: {
                NSubscription* sub = (NSubscription*)node;

                // ignore subs that are typed as KeyError or unknown
                if(sub->getInferredType() == python::Type::UNKNOWN || sub->getInferredType().isExceptionType())
                    return;

                assert(sub->_value->getInferredType() != python::Type::UNKNOWN); // type annotation/tracing must have run for this...

                auto val_type = sub->_value->getInferredType();

                // special treatment, row type
                if(PARAM_USE_ROW_TYPE && val_type.isRowType() && sub->_value->type() == ASTNodeType::Identifier) {
                    NIdentifier* id = (NIdentifier*)sub->_value.get();
                    if(std::find(_argNames.begin(), _argNames.end(), id->_name) != _argNames.end()) {
                        if(sub->_expression->type() == ASTNodeType::Number) {
                            NNumber* num = (NNumber*)sub->_expression.get();
                            // can save this one!
                            auto idx = num->getI64();

                            // negative indices should be converted to positive ones
                            if(idx < 0)
                                idx += _numColumns;
                            assert(idx >= 0 && idx < _numColumns);

                            _argSubscriptIndices[id->_name].push_back(idx);
                        } else if(sub->_expression->type() == ASTNodeType::String) {
                            NString* str = (NString*)sub->_expression.get();
                            auto columns = val_type.get_column_names();
                            auto it = std::find(columns.begin(), columns.end(), str->value());
                            if(it != columns.end())
                                _argSubscriptIndices[id->_name].push_back(it - columns.begin());

                        } else if(sub->_expression->getInferredType().isConstantValued() && sub->_expression->getInferredType().underlying() == python::Type::I64) {
                            // can save this one!
                            auto idx = std::stoi(sub->_expression->getInferredType().constant());

                            // negative indices should be converted to positive ones
                            if(idx < 0)
                                idx += _numColumns;
                            assert(idx >= 0 && idx < _numColumns);

                            _argSubscriptIndices[id->_name].push_back(idx);
                        } else if(sub->_expression->getInferredType().isConstantValued() && sub->_expression->getInferredType().underlying() == python::Type::STRING) {
                            auto columns = val_type.get_column_names();
                            auto it = std::find(columns.begin(), columns.end(), sub->_expression->getInferredType().constant());
                            if(it != columns.end())
                                _argSubscriptIndices[id->_name].push_back(it - columns.begin());
                        } else {
                            // dynamic access into identifier, so need to push completely back.
                            _argFullyUsed[id->_name] = true;
                        }
                        return;
                    }
                }

                // access/rewrite makes only sense for dict/tuple types!
                // just simple stuff yet.
                if (sub->_value->type() == ASTNodeType::Identifier &&
                    (val_type.isTupleType() || val_type.isDictionaryType())) {
                    NIdentifier* id = (NIdentifier*)sub->_value.get();

                    // first check whether this identifier is in args,
                    // if not ignore.
                    if(std::find(_argNames.begin(), _argNames.end(), id->_name) != _argNames.end()) {
                        // no nested paths yet, i.e. x[0][2]
                        if(sub->_expression->type() == ASTNodeType::Number) {
                            NNumber* num = (NNumber*)sub->_expression.get();
#ifndef NDEBUG
                            // should be I64 or bool
                            auto deopt_num_type = deoptimizedType(num->getInferredType());
                            assert(deopt_num_type == python::Type::BOOLEAN ||
                                   deopt_num_type == python::Type::I64);
#endif

                            // can save this one!
                            auto idx = num->getI64();

                            // negative indices should be converted to positive ones
                            if(idx < 0)
                                idx += _numColumns;
                            assert(idx >= 0 && idx < _numColumns);

                            _argSubscriptIndices[id->_name].push_back(idx);
                        } else {
                            // dynamic access into identifier, so need to push completely back.
                            _argFullyUsed[id->_name] = true;
                        }
                    }

                }

                break;
            }

            default:
                // other nodes not supported.
                break;
        }
    }
}