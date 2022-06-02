//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <ColumnRewriteVisitor.h>
#include <Utils.h>

namespace tuplex {

    std::string processString(const std::string &val) {
        assert(val.length() >= 2);
        // only simple strings supported, i.e. those that start with ' ' or " "
        assert((val[0] == '\'' && val[val.length() - 1] == '\'')
               || (val[0] == '\"' && val[val.length() - 1] == '\"'));

        return val.substr(1, val.length() - 2);
    }


    ASTNode * ColumnRewriteVisitor::replace(ASTNode *parent, ASTNode *node) {
        if(!node)
            return nullptr;

        switch(node->type()) {
            case ASTNodeType::Subscription: {
                NSubscription* sub = (NSubscription*)node;

                if(sub->_value->type() == ASTNodeType::Identifier &&
                   sub->_expression->type() == ASTNodeType::String) {
                    auto id = (NIdentifier*)sub->_value;
                    auto str = (NString*)sub->_expression;

                    // rewrite if matches param
                    if(id->_name == _parameter) {
                        // exchange expression with number (index in column names array)

                        auto colName = str->value();

                        int idx = indexInVector(colName, _columnNames);
                        if(idx < 0)
                            error("could not find column '" + colName + "' in dataset.");

                        // make true, access found
                        _dictAccessFound = true;

                        // only rewrite in non-dry mode
                        if(_rewrite) {
                            // special case: If there is a single column, do not use param[idx],
                            //               but just return param due to unpacking
                            if(_columnNames.size() == 1)
                                return id->clone();
                            else
                                return new NSubscription(id, new NNumber(static_cast<int64_t>(idx)));
                        }
                    }
                }

                return node;
            }
            default:
                return node;
        }
    }
}