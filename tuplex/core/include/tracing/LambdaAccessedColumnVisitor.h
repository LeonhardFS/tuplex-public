//
// Created by leonhards on 6/24/24.
//

#ifndef TUPLEX_LAMBDAACCESSEDCOLUMNVISITOR_H
#define TUPLEX_LAMBDAACCESSEDCOLUMNVISITOR_H

#include <visitors/IPrePostVisitor.h>

namespace tuplex {
    // @TODO: put in separate file
    class LambdaAccessedColumnVisitor : public IPrePostVisitor {
    protected:
        virtual void postOrder(ASTNode *node) override {}
        virtual void preOrder(ASTNode *node) override;

        /// whether function has multiple arguments or not. If multiple args, use different map.
        option<bool> _multiArgs;
        size_t _numColumns;
        bool _singleLambda;
        std::vector<std::string> _argNames;
        std::unordered_map<std::string, bool> _argFullyUsed;
        std::unordered_map<std::string, std::vector<size_t>> _argSubscriptIndices;

    public:
        LambdaAccessedColumnVisitor() : _multiArgs(option<bool>::none),
                                        _numColumns(0), _singleLambda(false) {}

        // override subscript to handle special cases, i.e. stop traversal on (nested) dictionaries/lists.
        virtual void visit(NSubscription* n) override;

        virtual void visit(NCall* node) override;

        virtual void visit(NIfElse* node) override;

        std::vector<size_t> getAccessedIndices() const;
    };
}

#endif //TUPLEX_LAMBDAACCESSEDCOLUMNVISITOR_H
