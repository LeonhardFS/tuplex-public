//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_FUNCTIONREGISTRY_H
#define TUPLEX_FUNCTIONREGISTRY_H

#include "llvm/ADT/APFloat.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/IR/BasicBlock.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Target/TargetMachine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"

#include <deque>
#include <ApatheticVisitor.h>
#include <LLVMEnvironment.h>
#include <Token.h>
#include <LambdaFunction.h>
#include <unordered_map>
#include <IteratorContextProxy.h>

#include <Utils.h>

namespace tuplex {

    namespace codegen {

        /*!
         * class to handle Python builtin functions, typing etc.
         */
        class FunctionRegistry {
        public:
            FunctionRegistry(LLVMEnvironment& env, bool sharedObjectPropagation) : _env(env), _sharedObjectPropagation(sharedObjectPropagation) {
                _iteratorContextProxy = std::make_shared<IteratorContextProxy>(&env);
            }

            codegen::SerializableValue createGlobalSymbolCall(LambdaFunctionBuilder& lfb,
                    const codegen::IRBuilder& builder,
                    const std::string& symbol,
                    const python::Type& argsType,
                    const python::Type& retType,
                    const std::vector<codegen::SerializableValue>& args);

            codegen::SerializableValue createAttributeCall(LambdaFunctionBuilder& lfb,
                    const codegen::IRBuilder& builder,
                    const std::string& symbol,
                    const python::Type& callerType,
                    const python::Type& argsType,
                    const python::Type& retType,
                    const SerializableValue& caller,
                    const std::vector<codegen::SerializableValue>& args);

            // global functions
            SerializableValue createLenCall(const codegen::IRBuilder& builder,
                    const python::Type &argsType,
                    const python::Type &retType,
                    const std::vector<tuplex::codegen::SerializableValue> &args);

            SerializableValue createFormatCall(const codegen::IRBuilder& builder,
                                               const SerializableValue& caller,
                                               const std::vector<tuplex::codegen::SerializableValue>& args,
                                               const std::vector<python::Type>& argsTypes);
            SerializableValue createLowerCall(const codegen::IRBuilder& builder, const SerializableValue& caller);
            SerializableValue createUpperCall(const codegen::IRBuilder& builder, const SerializableValue& caller);
            SerializableValue createSwapcaseCall(const codegen::IRBuilder& builder, const SerializableValue& caller);
            SerializableValue createFindCall(const codegen::IRBuilder& builder, const SerializableValue& caller, const SerializableValue& needle);
            SerializableValue createReverseFindCall(const codegen::IRBuilder& builder, const SerializableValue& caller, const SerializableValue& needle);
            SerializableValue createStripCall(const codegen::IRBuilder& builder, const SerializableValue& caller, const std::vector<tuplex::codegen::SerializableValue>& args);
            SerializableValue createLStripCall(const codegen::IRBuilder& builder, const SerializableValue& caller, const std::vector<tuplex::codegen::SerializableValue>& args);
            SerializableValue createRStripCall(const codegen::IRBuilder& builder, const SerializableValue& caller, const std::vector<tuplex::codegen::SerializableValue>& args);
            SerializableValue createReplaceCall(const codegen::IRBuilder& builder, const SerializableValue& caller, const SerializableValue& from, const SerializableValue& to);
            SerializableValue createCenterCall(LambdaFunctionBuilder& lfb, const codegen::IRBuilder& builder, const SerializableValue &caller, const SerializableValue &width, const SerializableValue *fillchar);
            SerializableValue createJoinCall(const codegen::IRBuilder& builder, const SerializableValue& caller, const SerializableValue& list);
            SerializableValue createSplitCall(LambdaFunctionBuilder& lfb, const codegen::IRBuilder& builder, const tuplex::codegen::SerializableValue &caller, const tuplex::codegen::SerializableValue &delimiter);

            SerializableValue createIntCast(LambdaFunctionBuilder& lfb, const codegen::IRBuilder& builder, python::Type argsType, const std::vector<tuplex::codegen::SerializableValue> &args);

            SerializableValue createCapwordsCall(LambdaFunctionBuilder& lfb, const codegen::IRBuilder& builder, const SerializableValue& caller);

            SerializableValue
            createReSearchCall(LambdaFunctionBuilder &lfb, const codegen::IRBuilder& builder, const python::Type &argsType,
                               const std::vector<tuplex::codegen::SerializableValue> &args);

            SerializableValue
            createReSubCall(LambdaFunctionBuilder &lfb, const codegen::IRBuilder& builder, const python::Type &argsType,
                               const std::vector<tuplex::codegen::SerializableValue> &args);

            SerializableValue createRandomChoiceCall(LambdaFunctionBuilder &lfb, const codegen::IRBuilder& builder, const python::Type &argType, const SerializableValue &arg);

            SerializableValue createIterCall(LambdaFunctionBuilder &lfb,
                                             const codegen::IRBuilder &builder,
                                             const python::Type &argsType,
                                             const python::Type &retType,
                                             const std::vector<tuplex::codegen::SerializableValue> &args);

            SerializableValue createReversedCall(LambdaFunctionBuilder &lfb,
                                                 const codegen::IRBuilder &builder,
                                             const python::Type &argsType,
                                             const python::Type &retType,
                                             const std::vector<tuplex::codegen::SerializableValue> &args);

            SerializableValue createNextCall(LambdaFunctionBuilder &lfb,
                                             const codegen::IRBuilder &builder,
                                             const python::Type &argsType,
                                             const python::Type &retType,
                                             const std::vector<tuplex::codegen::SerializableValue> &args,
                                             const std::shared_ptr<IteratorInfo> &iteratorInfo);

            SerializableValue createZipCall(LambdaFunctionBuilder &lfb,
                                            const codegen::IRBuilder &builder,
                                             const python::Type &argsType,
                                             const python::Type &retType,
                                             const std::vector<tuplex::codegen::SerializableValue> &args,
                                             const std::shared_ptr<IteratorInfo> &iteratorInfo);

            SerializableValue createEnumerateCall(LambdaFunctionBuilder &lfb,
                                                  const codegen::IRBuilder &builder,
                                            const python::Type &argsType,
                                            const python::Type &retType,
                                            const std::vector<tuplex::codegen::SerializableValue> &args,
                                            const std::shared_ptr<IteratorInfo> &iteratorInfo);

            /*!
             * Create calls related to iterators. Including iterator generating calls (iter(), zip(), enumerate())
             * or function calls that take iteratorType as argument (next())
             * @param lfb
             * @param builder
             * @param symbol
             * @param argsType
             * @param retType
             * @param args
             * @param iteratorInfo
             * @return
             */
            SerializableValue createIteratorRelatedSymbolCall(tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                              const codegen::IRBuilder &builder,
                                                              const std::string &symbol,
                                                              const python::Type &argsType,
                                                              const python::Type &retType,
                                                              const std::vector<tuplex::codegen::SerializableValue> &args,
                                                              const std::shared_ptr<IteratorInfo> &iteratorInfo);

            SerializableValue createDictConstructor(LambdaFunctionBuilder& lfb, const codegen::IRBuilder& builder, python::Type argsType, const std::vector<tuplex::codegen::SerializableValue> &args);
            void getValueFromcJSON(const codegen::IRBuilder& builder, llvm::Value *cjson_val, python::Type retType,
                                   llvm::Value *retval,
                                   llvm::Value *retsize);
            SerializableValue createCJSONPopCall(LambdaFunctionBuilder& lfb,
                                            const codegen::IRBuilder& builder,
                                            const SerializableValue& caller,
                                            const std::vector<tuplex::codegen::SerializableValue>& args,
                                            const std::vector<python::Type>& argsTypes,
                                            const python::Type& retType);
            SerializableValue createCJSONPopItemCall(LambdaFunctionBuilder &lfb, const codegen::IRBuilder& builder, const SerializableValue &caller,
                              const python::Type &retType);

            SerializableValue createFloatCast(LambdaFunctionBuilder& lfb, const codegen::IRBuilder& builder, python::Type argsType, const std::vector<tuplex::codegen::SerializableValue> &args);
            SerializableValue createBoolCast(LambdaFunctionBuilder& lfb, const codegen::IRBuilder& builder, python::Type argsType, const std::vector<tuplex::codegen::SerializableValue> &args);
            SerializableValue createStrCast(LambdaFunctionBuilder& lfb, const codegen::IRBuilder& builder, python::Type argsType, const std::vector<tuplex::codegen::SerializableValue> &args);
            SerializableValue createIndexCall(LambdaFunctionBuilder& lfb, const codegen::IRBuilder& builder, const SerializableValue& caller, const SerializableValue& needle);
            SerializableValue createReverseIndexCall(LambdaFunctionBuilder& lfb, const codegen::IRBuilder& builder, const SerializableValue& caller, const SerializableValue& needle);
            SerializableValue createCountCall(const codegen::IRBuilder& builder, const SerializableValue &caller, const SerializableValue &needle);
            SerializableValue createStartswithCall(LambdaFunctionBuilder &lfb, const codegen::IRBuilder& builder, const SerializableValue &caller, const SerializableValue &needle);
            SerializableValue createEndswithCall(LambdaFunctionBuilder &lfb, const codegen::IRBuilder& builder, const SerializableValue &caller, const SerializableValue &suffix);
            SerializableValue createIsDecimalCall(LambdaFunctionBuilder &lfb, const codegen::IRBuilder& builder, const SerializableValue &caller);
            SerializableValue createIsDigitCall(LambdaFunctionBuilder &lfb, const codegen::IRBuilder& builder, const SerializableValue &caller);
            SerializableValue createIsAlphaCall(LambdaFunctionBuilder &lfb, const codegen::IRBuilder& builder, const SerializableValue &caller);
            SerializableValue createIsAlNumCall(LambdaFunctionBuilder &lfb, const codegen::IRBuilder& builder, const SerializableValue &caller);
            SerializableValue createMathToRadiansCall(const codegen::IRBuilder& builder, const python::Type &argsType,
                                                                                 const python::Type &retType,
                                                                                 const std::vector<tuplex::codegen::SerializableValue> &args);
            SerializableValue createMathToDegreesCall(const codegen::IRBuilder& builder, const python::Type &argsType,
                                                      const python::Type &retType,
                                                      const std::vector<tuplex::codegen::SerializableValue> &args);

            SerializableValue createMathIsNanCall(const codegen::IRBuilder& builder, const python::Type &argsType,
                                                  const python::Type &retType,
                                                  const std::vector<tuplex::codegen::SerializableValue> &args);
            
            SerializableValue createMathIsInfCall(const codegen::IRBuilder& builder, const python::Type &argsType,
                                                  const python::Type &retType,
                                                  const std::vector<tuplex::codegen::SerializableValue> &args);
                                                  
            SerializableValue createMathIsCloseCall(tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                    const codegen::IRBuilder& builder, const python::Type &argsType,
                                                    const std::vector<tuplex::codegen::SerializableValue> &args);

            // math module functions
            SerializableValue createMathCeilFloorCall(LambdaFunctionBuilder& lfb, const codegen::IRBuilder& builder, const std::string& qual_name, const SerializableValue& arg);

        private:
            LLVMEnvironment& _env;
            bool _sharedObjectPropagation;
            std::shared_ptr<IteratorContextProxy> _iteratorContextProxy;

            // lookup (symbolname, typehash)
            std::unordered_map<std::tuple<std::string, python::Type>, llvm::Function*> _funcMap;

            void constructIfElse(llvm::Value *condition, std::function<llvm::Value*(void)> ifCase,
                                                                 std::function<llvm::Value*(void)> elseCase,
                                                                 llvm::Value *res,
                                                                 tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                                 const codegen::IRBuilder& builder);


            inline std::tuple<llvm::Value*, llvm::Value*, llvm::Value*> loadPCRE2Contexts(const IRBuilder& builder) {
                if(_sharedObjectPropagation) {
                    // create runtime contexts that are allocated on regular heap: general, compile, match (in order to pass rtmalloc/rtfree)
                    auto contexts = _env.addGlobalPCRE2RuntimeContexts();
                    auto general_context = builder.CreateLoad(_env.i8ptrType(), std::get<0>(contexts));
                    auto match_context = builder.CreateLoad(_env.i8ptrType(), std::get<1>(contexts));
                    auto compile_context = builder.CreateLoad(_env.i8ptrType(), std::get<2>(contexts));
                    return std::make_tuple(general_context, match_context, compile_context);
                } else {
                    // create runtime contexts for the row
                    auto general_context = builder.CreateCall(pcre2GetLocalGeneralContext_prototype(_env.getContext(), _env.getModule().get()));
                    auto match_context = builder.CreateCall(pcre2MatchContextCreate_prototype(_env.getContext(), _env.getModule().get()), {general_context});
                    auto compile_context = builder.CreateCall(pcre2CompileContextCreate_prototype(_env.getContext(), _env.getModule().get()), {general_context});
                    return std::make_tuple(general_context, match_context, compile_context);
                }
            }
        };
    }
}

#endif //TUPLEX_FUNCTIONREGISTRY_H