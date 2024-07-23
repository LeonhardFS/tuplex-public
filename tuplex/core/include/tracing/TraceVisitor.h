//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TRACEVISITOR_H
#define TUPLEX_TRACEVISITOR_H

#include <visitors/ApatheticVisitor.h>
#include <Python.h>
#include <utility>
#include <vector>
#include <PythonHelpers.h>
#include <codegen/IFailable.h>
#include <csetjmp>
#include <symbols/ClosureEnvironment.h>
#include <ast/ASTHelpers.h>
#include <codegen/CodegenHelper.h>
#include "StructCommon.h"

#define TI_FLAGS_NONE 0x0
#define TI_FLAGS_INPUT_PARAMETER 0x1

// a tracing visitor to determine optimizations within functions!
namespace tuplex {
    class TraceVisitor : public ApatheticVisitor, public IFailable {
    private:
        PyObject *_args;
        bool _functionSeen;
        size_t _numSamplesProcessed;
        struct TraceItem {
            int id;
            PyObject* value;
            std::string name;
            size_t flags;

            explicit TraceItem(int _id, PyObject* obj) : id(_id), value(obj), name(""), flags(0)   {}

            TraceItem(int _id, PyObject* obj, std::string n) : id(_id), value(obj), name(std::move(n)), flags(0) {}

            static TraceItem param(int _id, PyObject* obj, const std::string n="") {
                TraceItem ti(_id, obj, n);
                static_assert(TI_FLAGS_INPUT_PARAMETER != 0, "flag must not be 0");
                ti.flags = TI_FLAGS_INPUT_PARAMETER;
                return ti;
            }

            python::Type type() const;
            Field as_field() const;
        };

        struct SubscriptEvent {
            int traceId;
            python::Type valueType;
            Field key;
            int valueObjectId; // <-- track by object Id.
            int resultObjectId; // <-- track by object Id.
            bool on_input_param;
        };

        // current trace id (to store to which trace something belongs)
        int _currentTraceId;
        int _currentObjectId; // <-- assign to traceitem whenever created!
        std::vector<SubscriptEvent> _subscriptEvents; // trace subscripting
        std::unordered_map<int, std::string> _inputParamIds; // object ids of input parameters.

        // evaluation stack
        std::vector<TraceItem> _evalStack;
        // use the helper function to add annotations to ASTs!
        void addTraceResult(ASTNode* node, TraceItem item);

        // symbols
        std::vector<TraceItem> _symbols;

        std::vector<std::string> _argsColumns;

        // access paths to input parameters, encoded via strings...
        // works for now only via x, or x[0] or so
        // future: more complex stuff like x[0][2] or so!
        std::unordered_map<std::string, size_t> _inputAccessPaths;
        std::vector<std::string> _columnNames;
        inline void inc_access_path(const std::string& path) {
            auto it = _inputAccessPaths.find(path);
            if(it == _inputAccessPaths.end())
                _inputAccessPaths[path] = 0;
            _inputAccessPaths[path]++;
        }

        MessageHandler& logger() { return Logger::instance().logger("tracer"); }

        void unpackFunctionParameters(const std::vector<ASTNode*> &astArgs);

        python::Type mapPythonToTuplexType(PyObject* obj, bool autoUpcast);

        /*!
         * types of traced input arguments
         */
        std::vector<std::vector<python::Type>> _colTypes;
        std::vector<std::vector<python::Type>> _retColTypes;

        /*!
         * any exceptions that might have occurred while processing the sample
         */
         std::vector<std::string> _exceptions; // for now simple strings => aggregate

         // store tracebacks for clean display
         // TODO: could avoid storing duplicates so index by type & line number & column number, yet this not supported yet.

        // was a break statement executed in the ongoing loop?
        std::vector<bool> _loopBreakStack;

        // each element vector corresponds to {{symbols created before loop}, symbolTypeChange} for an ongoing loop
        // whenever the type of a symbol that is in {symbols created before loop} changes, set symbolTypeChange to true
        std::vector<std::pair<std::vector<std::string>, bool>> _symbolsTypeChangeStack;

        TraceItem _retValue;

        python::Type _inputRowType; // optional schema to filter out bad input rows (will save as exception)

        codegen::CompilePolicy _policy; // holds compilation parameters

        void fetchAndStoreError();

        void errCheck();

        // need to record access paths for params -> multiple levels?

        /*!
         * internal class thrown when errors occur to leave control flow
         */
        class TraceException : public std::exception {
        };


        inline int next_object_id() {
            return _currentObjectId++;
        }

        void process_access_path_batch(std::vector<std::vector<access_path_t>>& paths, const std::vector<SubscriptEvent>& batch) const;

    public:
        explicit TraceVisitor(const python::Type& inputRowType=python::Type::UNKNOWN,
                              const codegen::CompilePolicy& policy=codegen::CompilePolicy()) : _args(nullptr),
                    _functionSeen(false),
                    _retValue(-1, nullptr), _inputRowType(inputRowType), _numSamplesProcessed(0), _policy(policy), _currentTraceId(0), _currentObjectId(0) {
        }

        /*!
         * trace input over AST and record within tree what happened
         * @param node
         * @param args PyObject to trace
         * @param columns
         */
        void recordTrace(ASTNode* node, PyObject* args, const std::vector<std::string>& columns={});

        python::Type majorityInputType() const;
        python::Type majorityOutputType() const;

        size_t columnCount() const {
            auto input_type = majorityInputType();
            assert(input_type.isTupleType());

            // unpack if it's format ((a, b, c))
            if(input_type.parameters().size() == 1 && input_type.parameters().front().isTupleType())
                input_type = input_type.parameters().front();
            return input_type.parameters().size();
        }

        /*!
         * this function returns a vector counting how often each column was accessed.
         * @return vector with n columns counting the accesses
         */
        std::vector<size_t> columnAccesses() const;

        inline std::vector<std::string> columns() const { return _argsColumns; }

        /*!
         * set global constants, variables, imports etc. from closure environment
         * @param ce
         */
        void setClosure(const ClosureEnvironment& ce, bool acquireGIL);

        /*!
         * retrieve last result of function...
         * @return
         */
        PyObject* lastResult() const { return _retValue.value; }

        /*!
         * retrieve access paths for each column. Empty acess path means this element has not been accessed.
         * @return vector of access oaths length of vector is equivalent to number of columns detected.
         */
        std::vector<std::vector<access_path_t>> columnAccessPaths() const;

        // leaf nodes
        void visit(NNone *) override;

        void visit(NNumber *) override;

        void visit(NIdentifier *) override;

        void visit(NBoolean *) override;

        void visit(NEllipsis *) override {}

        void visit(NString *) override;

        // non-leaf nodes, recursive calls are carried out for these
        void visit(NParameter *) override;

        void visit(NParameterList *) override;

        void visit(NFunction *) override;

        void visit(NBinaryOp *) override;

        void visit(NUnaryOp *) override;

        void visit(NSuite *) override;

        void visit(NModule *) override;

        void visit(NLambda *) override;

        void visit(NAwait *) override;

        void visit(NStarExpression *) override;

        void visit(NCompare *) override;

        void visit(NIfElse *) override;

        void visit(NTuple *) override;

        void visit(NDictionary *) override;

        void visit(NSubscription *) override;

        void visit(NReturn *) override;

        void visit(NAssign *) override;

        void visit(NCall *) override;

        void visit(NAttribute *) override;

        void visit(NSlice *) override;

        void visit(NSliceItem *) override;

        void visit(NFor *) override;

        void visit(NWhile *) override;

        void visit(NRange *) override;

        void visit(NList *) override;
    };
}

#endif //TUPLEX_TRACEVISITOR_H