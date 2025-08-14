//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_APPLYVISITOR_H
#define TUPLEX_APPLYVISITOR_H

#include "IPrePostVisitor.h"

namespace tuplex {

    /*!
     * helper class to execute lambda function for all nodes of a specific type fulfilling some predicate
     */
    class ApplyVisitor : public IPrePostVisitor {
    protected:
        std::function<bool(const ASTNode*)> _predicate;
        std::function<void(const ASTNode*, ASTNode&)> _func;

        void postOrder(ASTNode *node) override {
            if(_predicate(node))
                _func(parent(), *node);
        }

        void preOrder(ASTNode *node) override {}
    public:
        ApplyVisitor() = delete;

        /*!
         * create a new ApplyVisitor
         * @param predicate visit only nodes which return true
         * @param func call this function on every node which satisfies the predicate
         * @param followAll whether to visit all nodes, or only follow the normal-case path
         */
        ApplyVisitor(std::function<bool(const ASTNode*)> predicate,
                std::function<void(const ASTNode*, ASTNode&)> func, bool followAll=true) : _predicate(predicate), _func(func), _followAll(followAll)   {}

        ApplyVisitor(std::function<bool(const ASTNode*)> predicate,
                std::function<void(ASTNode&)> func, bool followAll=true) : _predicate(std::move(predicate)), _func([func](const ASTNode* parent, ASTNode& node) { func(node); }), _followAll(followAll)   {}

        // speculation so far only on ifelse branches
        void visit(NIfElse* ifelse) override {
            if(!_followAll) {
                // speculation on?
                bool speculate = ifelse->annotation().numTimesVisited > 0;
                auto visit_t = whichBranchToVisit(ifelse);
                auto visit_ifelse = std::get<0>(visit_t);
                auto visit_if = std::get<1>(visit_t);
                auto visit_else = std::get<2>(visit_t);

                // only one should be true, logical xor
                if(speculate && (!visit_if != !visit_else)) {
                    if(visit_if) {
                        ifelse->_expression->accept(*this);
                        ifelse->_then->accept(*this);
                    }
                    if(visit_else && ifelse->_else) {
                        ifelse->_expression->accept(*this);
                        ifelse->_else->accept(*this);
                    }
                    return;
                }
            }

            IPrePostVisitor::visit(ifelse);
        }

        void visit(NSuite* suite) override {
            if (!_followAll) {
                if (suite->hasAnnotation() && suite->annotation().numTimesVisited == 0)
                    return;

                // some statements may stop execution in a suite (break, continue, return).
                // stop calling in this case.
                for (auto& stmt : suite->_statements) {
                    switch (stmt->type()) {
                        case ASTNodeType::Break:
                        case ASTNodeType::Continue:
                        case ASTNodeType::Return: {
                            // accept visitor, and then stop.
                            stmt->accept(*this);
                            return;
                        }
                    default:
                        stmt->accept(*this);
                        break;
                    }
                }
                return;
            }
            IPrePostVisitor::visit(suite);
        }

        // speculation also on for loops/while loops.
        void visit(NFor* forelse) override {
            if (!_followAll) {
                // no visit at all?
                if (forelse->hasAnnotation() && forelse->annotation().numTimesVisited == 0) {
                    return;
                }

                // visit expression if has not annotation.
                forelse->expression->accept(*this);

                if (forelse->target->hasAnnotation() && forelse->target->annotation().numTimesVisited > 0) {
                    forelse->target->accept(*this);
                }
                forelse->suite_body->accept(*this);
                // check if else suite exists.
                if (forelse->suite_else)
                    forelse->suite_else->accept(*this);
            }
        }

    private:
        bool _followAll;
    };
}

#endif //TUPLEX_FORCETYPEVISITOR_H