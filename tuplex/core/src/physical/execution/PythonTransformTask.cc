//
// Created by Leonhard Spiegelberg on 9/29/25.
//
#include <physical/execution/PythonTransformTask.h>
#include <physical/execution/TransformTask.h>
#include <PythonHelpers.h>
#include <physical/memory/Partition.h>
#include <Timer.h>
#include <Utils.h>
#include <iostream>
#include <cstring>

namespace tuplex {

    void PythonTransformTask::execute() {
        Timer timer;
        
        // Check that we have input partitions
        if (_inputNormalPartitions.empty() && 
            _inputGeneralPartitions.empty() && 
            _inputFallbackPartitions.empty()) {
            throw std::runtime_error("PythonTransformTask has no input partitions");
        }
        
        // Reset counters and sinks
        _numInputRowsRead = 0;
        _wallTime = 0.0;
        _output.reset();
        _exceptions.reset();
        
        // Initialize hash table if needed
        if (hasHashTableSink() && !_htable) {
            _htable = new HashTableSink();
        }
        
        // Process normal partitions
        for (auto& partition : _inputNormalPartitions) {
            processPartition(partition, false);
        }
        
        // Process general partitions
        for (auto& partition : _inputGeneralPartitions) {
            processPartition(partition, false);
        }
        
        // Process fallback partitions
        for (auto& partition : _inputFallbackPartitions) {
            processPartition(partition, true);
        }

        releaseAllLocks();

        // Record wall time
        _wallTime = timer.time();
        
#ifndef NDEBUG
        owner()->info("PythonTransformTask completed (" + 
                     pluralize(_numInputRowsRead, "input row") + 
                     ", " + pluralize(_output.partitions.size(), "output partition") +
                     ", " + pluralize(_exceptions.partitions.size(), "exception partition") + ")");
#endif
    }
    
    void PythonTransformTask::processPartition(Partition* inputPartition, bool isFallback) {
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
            
            // Get the input schema from the partition
            Schema inputSchema = inputPartition->schema();
            
            // Process each row in the partition
            processRows(dataPtr, dataSize, numRows, inputSchema, isFallback);
            
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
                                         const Schema& inputSchema, bool isFallback) {
        if (numRows == 0) {
            return;
        }

        assert(_pyFunctor);
        
        python::lockGIL();
        try {
            const uint8_t* currentPtr = dataPtr;
            
            // write as fallback partition/general python object
            for (int64_t i = 0; i < numRows; ++i) {
                int64_t bytes_read = 0;
                int64_t remaining_size = dataSize - (currentPtr - dataPtr);

                // Deserialize row to Python object
                PyObject* pyRow = deserializeRowToPython(currentPtr, remaining_size, bytes_read, inputSchema, isFallback);
                
                if (!pyRow) {
                   // TODO: error handling
                   continue;
                }


#ifndef NDEBUG
                // Print row for inspection purposes.
                Py_XINCREF(pyRow);
                PyObject_Print(pyRow, stdout, 0);
                std::cout<<std::endl;
#endif


                // call pipFunctor
                size_t num_python_args = 1 + _py_intermediates.size() + hasHashTableSink();

                // special case unique, no arg required (done via output)
                if(hasHashTableSink() && _hash_agg_type == AggregateType::AGG_UNIQUE)
                    num_python_args -= 1;

                PyObject* args = PyTuple_New(num_python_args);
                PyTuple_SET_ITEM(args, 0, pyRow);
                for(unsigned i = 0; i < _py_intermediates.size(); ++i) {
                    Py_XINCREF(_py_intermediates[i]);
                    PyTuple_SET_ITEM(args, i + 1, _py_intermediates[i]);
                }
                // set hash table sink
                if(hasHashTableSink() && _hash_agg_type != AggregateType::AGG_UNIQUE) { // special case: unique -> note: unify handling this with the other cases...
                    assert(_htable->hybrid_hm);
                    Py_XINCREF(_htable->hybrid_hm);
                    PyTuple_SET_ITEM(args, num_python_args - 1, _htable->hybrid_hm);
                }

                auto kwargs = PyDict_New();
                auto pcr = python::callFunctionEx(_pyFunctor, args, kwargs);

                if(pcr.exceptionCode != ExceptionCode::SUCCESS) {
                    // this should not happen, bad internal error. codegen'ed python should capture everything.
                    owner()->error("bad internal python error: " + pcr.exceptionMessage);
                    // cleanup and continue to next row
                    Py_DECREF(args);
                    Py_DECREF(kwargs);
                    continue;
                } else {
                    // all good, row is fine. exception occurred?
                    assert(pcr.res);

                    // type check: save to regular rows OR save to python row collection
                    if(!pcr.res) {
                        owner()->error("bad internal python error, NULL object returned");
                    } else {
                        // write result to fallback partition
                        writePythonResultToPartition(pcr.res);
                    }
                }

                // cleanup
                Py_DECREF(args);
                Py_DECREF(kwargs);
                
                // Advance to next row
                currentPtr += bytes_read;
            }
            
        } catch (const std::exception& e) {
            std::cerr << "Error processing rows: " << e.what() << std::endl;
            python::unlockGIL();
            throw;
        } catch (...) {
            std::cerr << "Unknown exception while processing rows" << std::endl;
            python::unlockGIL();
            throw;
        }
        
        python::unlockGIL();
    }
    
    PyObject* PythonTransformTask::deserializeRowToPython(const uint8_t* dataPtr, int64_t remaining_size, int64_t& bytes_read,
                                                         const Schema& schema, bool isFallback) {
        if (isFallback) {
            throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " not yet implemented, need to fix.");
            // // For fallback partitions, the data is a pickled Python object
            // // Format: 4 * sizeof(int64_t) header + pickled data
            // if (dataSize < 4 * sizeof(int64_t)) {
            //     return nullptr;
            // }
            //
            // const uint8_t* headerPtr = dataPtr;
            // int64_t rowNumber = *reinterpret_cast<const int64_t*>(headerPtr); headerPtr += sizeof(int64_t);
            // int64_t ecCode = *reinterpret_cast<const int64_t*>(headerPtr); headerPtr += sizeof(int64_t);
            // int64_t opID = *reinterpret_cast<const int64_t*>(headerPtr); headerPtr += sizeof(int64_t);
            // int64_t pyObjectSize = *reinterpret_cast<const int64_t*>(headerPtr); headerPtr += sizeof(int64_t);
            //
            // if (dataSize < 4 * sizeof(int64_t) + pyObjectSize) {
            //     return nullptr;
            // }
            //
            // // Deserialize the pickled Python object
            // return python::deserializePickledObject(python::getMainModule(),
            //                                        reinterpret_cast<const char*>(headerPtr),
            //                                        pyObjectSize);
        } else {
            // For normal and general partitions, deserialize using Row::fromMemory
            try {
                // TODO: this here is inefficient.
                Row row = Row::fromMemory(schema, dataPtr, remaining_size);
                bytes_read = row.serializedLength();

                return python::rowToPython(row, false);
            } catch (const std::exception& e) {
                std::cerr << "Error deserializing row: " << e.what() << std::endl;
                return nullptr;
            }
        }
    }
    
    void PythonTransformTask::writePythonResultToPartition(PyObject* result) {
        if (!result) {
            return;
        }
        
        try {


            auto exceptionObject = PyDict_GetItemString(result, "exception");
            if(exceptionObject) {

                // overwrite operatorID which is throwing.
                auto exceptionOperatorID = PyDict_GetItemString(result, "exceptionOperatorID");
                //operatorID = PyLong_AsLong(exceptionOperatorID);
                auto exceptionType = PyObject_Type(exceptionObject);
                // can ignore input row.
                auto ecCode = ecToI64(python::translatePythonExceptionType(exceptionType));

#ifndef NDEBUG
                // debug printing of exception and what the reason is...
                // print res obj
                Py_XINCREF(result);
                std::cout<<"exception occurred while processing using python: "<<std::endl;
                PyObject_Print(result, stdout, 0);
                std::cout<<std::endl;
#endif
            } else {
#ifndef NDEBUG
                // debug printing of exception and what the reason is...
                // print res obj
                Py_XINCREF(result);
                std::cout<<"result to be written is: "<<std::endl;
                PyObject_Print(result, stdout, 0);
                std::cout<<std::endl;
#endif
            }


            // TODO: need to fix code blow....
            // // Always write Python objects as pickled data to fallback partitions
            // // This follows the same pattern as ResolveTask::writePythonObjectToFallbackSink
            //
            // // Pickle the Python object
            // auto pickledObject = python::pickleObject(python::getMainModule(), result);
            // auto pyObjectSize = pickledObject.size();
            // auto bufSize = 4 * sizeof(int64_t) + pyObjectSize;
            //
            // uint8_t* buf = new uint8_t[bufSize];
            // auto ptr = buf;
            //
            // // Write header: row number, exception code, operator ID, object size
            // *((int64_t*)ptr) = 0; ptr += sizeof(int64_t);  // row number (placeholder)
            // *((int64_t*)ptr) = ecToI64(ExceptionCode::PYTHON_PARALLELIZE); ptr += sizeof(int64_t);
            // *((int64_t*)ptr) = -1; ptr += sizeof(int64_t);  // operator ID (placeholder)
            // *((int64_t*)ptr) = pyObjectSize; ptr += sizeof(int64_t);
            //
            // // Copy pickled data
            // memcpy(ptr, pickledObject.c_str(), pyObjectSize);
            //
            // static Schema fallbackSchema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::PYOBJECT}));
            //
            // // Create a MemorySink from the partition
            // rowToMemorySink(owner(), _output, fallbackSchema, 0, _stageID, buf, bufSize);
            //
            // delete[] buf;
            
        } catch (const std::exception& e) {
            std::cerr << "Error writing Python result to partition: " << e.what() << std::endl;
        }
    }
    
    void PythonTransformTask::releaseAllLocks() {
        // Unlock all output partitions
        _output.unlock();
        _exceptions.unlock();
    }

}