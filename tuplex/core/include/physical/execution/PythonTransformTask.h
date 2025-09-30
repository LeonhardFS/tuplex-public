//
// Created by Leonhard Spiegelberg on 9/29/25.
//

#ifndef PYTHONTRANSFORMTASK_H
#define PYTHONTRANSFORMTASK_H

#include "IExceptionableTask.h"
#include "physical/codegen/CodeDefs.h"
#include "physical/execution/FileInputReader.h"
#include "ExceptionCodes.h"
#include <hashmap.h>

namespace tuplex {
    class PythonTransformTask : public IExecutorTask {
    public:
        PythonTransformTask() = default;
        virtual ~PythonTransformTask() = default;

        std::vector<Partition*> getOutputPartitions() const override { return _outputPartitions; }
        std::vector<Partition*> getExceptionPartitions() const { return _exceptionPartitions; }
        std::vector<Partition*> getGeneralPartitions() const { return _generalPartitions; }
        std::vector<Partition*> getFallbackPartitions() const { return _fallbackPartitions; }
        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> exceptionCounts() const { return _exceptionCounts; }

        TaskType type() const override { return TaskType::PYTHONTRAFOTASK; }

        double wallTime() const override { return _wallTime; }

        void execute() override {
            // TODO: Implement Python transform execution
            // This is a placeholder implementation
        }

        void releaseAllLocks() override {
            // TODO: Implement lock release if needed
        }

        size_t getNumInputRows() const override {
            return _numInputRowsRead;
        }

        // Methods to configure the task (similar to TransformTask)
        void setInputPartitions(const std::vector<Partition*>& normalPartitions,
                               const std::vector<Partition*>& generalPartitions,
                               const std::vector<Partition*>& fallbackPartitions) {
            _inputNormalPartitions = normalPartitions;
            _inputGeneralPartitions = generalPartitions;
            _inputFallbackPartitions = fallbackPartitions;
        }

        void setStageID(int64_t stageID) {
            _stageID = stageID;
        }

        void setOutputLimit(size_t limit) {
            _outputLimit = limit;
        }

    private:
        size_t _numInputRowsRead = 0;
        double _wallTime = 0.0;
        int64_t _stageID = -1;
        size_t _outputLimit = 0;
        
        // Input partitions
        std::vector<Partition*> _inputNormalPartitions;
        std::vector<Partition*> _inputGeneralPartitions;
        std::vector<Partition*> _inputFallbackPartitions;
        
        // Output partitions
        std::vector<Partition*> _outputPartitions;
        std::vector<Partition*> _exceptionPartitions;
        std::vector<Partition*> _generalPartitions;
        std::vector<Partition*> _fallbackPartitions;
        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> _exceptionCounts;

    };
}

#endif //PYTHONTRANSFORMTASK_H
