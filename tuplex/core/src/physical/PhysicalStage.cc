//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/PhysicalStage.h>
#include <physical/PhysicalPlan.h>

#include <logical/ParallelizeOperator.h>
#include <logical/FileInputOperator.h>

namespace tuplex {
    PhysicalStage::~PhysicalStage() {
        for(auto stage : _predecessors) {
            if(stage)
                delete stage;
            stage = nullptr;
        }
    }

    void PhysicalStage::execute(const tuplex::Context &context) {
        // execute predecessors
        for(auto stage : _predecessors)
            stage->execute(context);

        // execute stage via backend
        assert(_backend);
        _backend->execute(this);
    }

    nlohmann::json PhysicalStage::getJSON() const {
        using namespace nlohmann;
        using namespace std;

        json j;
        j["id"] = _number;
        vector<json> children;
        for(auto c : this->_predecessors)
            children.emplace_back(c->_number);
        j["predecessors"] = children;
        return j;
    }

    const Context &PhysicalStage::context() const {
        return plan()->getContext();
    }

    SamplingMode PhysicalStage::samplingMode() const {
        // Check if operators have sampling defined.
        if(!_operators.empty()) {
            // source node?
           auto& op = _operators.front();
           switch(op->type()) {
               case LogicalOperatorType::PARALLELIZE: {
                   return std::dynamic_pointer_cast<ParallelizeOperator>(op)->samplingMode();
               }
               case LogicalOperatorType::FILEINPUT: {
                   return std::dynamic_pointer_cast<FileInputOperator>(op)->samplingMode();
               }
               default: {
                   break;
               }
           }
        }

        // Check if stage has predecessors, if so return first well-defined one.
        for(const auto& pred : _predecessors) {
            auto sm  = pred->samplingMode();
            if(sm != SamplingMode::UNKNOWN)
                return sm;
        }

        return SamplingMode::UNKNOWN;
    }
}