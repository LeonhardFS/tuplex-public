//
// Created by Leonhard Spiegelberg on 9/29/25.
//

#ifndef PYTHONTRANSFORMTASK_H
#define PYTHONTRANSFORMTASK_H

#include "IExceptionableTask.h"
#include "physical/codegen/CodeDefs.h"
#include "physical/execution/FileInputReader.h"
#include <hashmap.h>

namespace tuplex {
    class PythonTransformTask : public IExecutorTask {
    public:
        std::vector<Partition*> getOutputPartitions() const override { return _outputPartitions; }

        TaskType type() const override { return TaskType::PYTHONTRAFOTASK; }

        double wallTime() const override { return _wallTime; }

        void execute() override {
            // TODO: Implement Python transform execution
            // This is a placeholder implementation
        }

        void releaseAllLocks() override {
            // TODO: Implement lock release if needed
        }

    private:
        size_t _numInputRowsRead = 0;
        double _wallTime = 0.0;
        std::vector<Partition*> _outputPartitions;

        size_t getNumInputRows() const override {
            return _numInputRowsRead;
        }

    };
}

#endif //PYTHONTRANSFORMTASK_H
