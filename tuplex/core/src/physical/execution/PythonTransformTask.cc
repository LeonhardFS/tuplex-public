//
// Created by Leonhard Spiegelberg on 9/29/25.
//
#include <physical/execution/PythonTransformTask.h>
#include <physical/execution/TransformTask.h>
#include <python/PythonHelpers.h>
#include <python/PythonTypes.h>
#include <physical/memory/Partition.h>
#include <utils/Timer.h>
#include <utils/Utils.h>
#include <iostream>

namespace tuplex {

    void PythonTransformTask::execute() {
        Timer timer;
        
        // Check that we have input partitions
        if (_inputNormalPartitions.empty() && 
            _inputGeneralPartitions.empty() && 
            _inputFallbackPartitions.empty()) {
            throw std::runtime_error("PythonTransformTask has no input partitions");
        }
        
        // Reset counters
        _numInputRowsRead = 0;
        _wallTime = 0.0;
        
        // Process normal partitions
        for (auto& partition : _inputNormalPartitions) {
            processPartition(partition, _outputPartitions);
        }
        
        // Process general partitions
        for (auto& partition : _inputGeneralPartitions) {
            processPartition(partition, _generalPartitions);
        }
        
        // Process fallback partitions
        for (auto& partition : _inputFallbackPartitions) {
            processPartition(partition, _fallbackPartitions);
        }
        
        // Record wall time
        _wallTime = timer.time();
        
#ifndef NDEBUG
        owner()->info("PythonTransformTask completed (" + 
                     pluralize(_numInputRowsRead, "input row") + 
                     ", " + pluralize(_outputPartitions.size(), "output partition") + 
                     ", " + pluralize(_exceptionPartitions.size(), "exception partition") + ")");
#endif
    }
    
    void PythonTransformTask::processPartition(Partition* inputPartition, std::vector<Partition*>& outputPartitions) {
        if (!inputPartition) {
            return;
        }
        
        // Lock the input partition
        int64_t inSize = inputPartition->size();
        const uint8_t* inPtr = inputPartition->lockRaw();
        
        try {
            // Extract number of rows from partition header
            int64_t numRows = *((int64_t*)inPtr);
            _numInputRowsRead += static_cast<size_t>(numRows);
            
            // Move past the row count header
            const uint8_t* dataPtr = inPtr + sizeof(int64_t);
            int64_t dataSize = inSize - sizeof(int64_t);
            
            // Process each row in the partition
            processRows(dataPtr, dataSize, numRows, outputPartitions);
            
            // Unlock input partition (assume this doesn't throw an exception)
            inputPartition->unlock();
        } catch (const std::exception& e) {
            // TODO: Handle exception properly
            std::cerr << "Error processing partition: " << e.what() << std::endl;
            inputPartition->unlock();
            throw;
        } catch (...) {
            // TODO: Handle unknown exception properly
            std::cerr << "Unknown exception occurred while processing partition" << std::endl;
            inputPartition->unlock();
            throw;
        }
    }
    
    void PythonTransformTask::processRows(const uint8_t* dataPtr, 
                                         int64_t dataSize, 
                                         int64_t numRows, 
                                         std::vector<Partition*>& outputPartitions) {
        // This is a placeholder implementation
        // In a real implementation, this would:
        // 1. Deserialize each row from the partition data
        // 2. Convert to Python objects
        // 3. Call the Python transform function
        // 4. Convert results back to Tuplex rows
        // 5. Write to output partitions
        
        // For now, we'll create a simple pass-through implementation
        // that just copies the data to output partitions
        
        if (numRows == 0) {
            return;
        }
        
        // Create output partition if needed
        if (outputPartitions.empty()) {
            // This would need proper schema and context information
            // For now, we'll skip actual partition creation
            return;
        }
        
        // In a real implementation, we would:
        // 1. Lock GIL: python::lockGIL();
        // 2. Deserialize rows and convert to Python objects
        // 3. Call Python function: python::callFunctionEx(pythonFunc, args, kwargs);
        // 4. Convert results back to Tuplex format
        // 5. Write to output partitions using rowToMemorySink
        // 6. Unlock GIL: python::unlockGIL();
        
        // Placeholder: just count the rows processed
        // The actual Python execution would happen here
    }
    
    void PythonTransformTask::releaseAllLocks() {
        // Unlock all input partitions
        for (auto& partition : _inputNormalPartitions) {
            if (partition) {
                partition->unlock();
            }
        }
        
        for (auto& partition : _inputGeneralPartitions) {
            if (partition) {
                partition->unlock();
            }
        }
        
        for (auto& partition : _inputFallbackPartitions) {
            if (partition) {
                partition->unlock();
            }
        }
        
        // Unlock all output partitions
        for (auto& partition : _outputPartitions) {
            if (partition) {
                partition->unlockWrite();
            }
        }
        
        for (auto& partition : _exceptionPartitions) {
            if (partition) {
                partition->unlockWrite();
            }
        }
        
        for (auto& partition : _generalPartitions) {
            if (partition) {
                partition->unlockWrite();
            }
        }
        
        for (auto& partition : _fallbackPartitions) {
            if (partition) {
                partition->unlockWrite();
            }
        }
    }

}