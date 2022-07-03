//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 7/3/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "gtest/gtest.h"
#include <LLVMEnvironment.h>
#include <JITCompiler.h>
#include <string>
#include <vector>
#include <physical/AggregateFunctions.h>
#include <ContextOptions.h>
#include <RuntimeInterface.h>

#include <HashHelper.h>

// this file is designed to help with calling/initializing the hashing utils

typedef int64_t(*test_func_f)(void* userData, const char* ptr);


TEST(BetterHashing, BasicHashingInitialization) {
    using namespace tuplex::codegen;
    using namespace tuplex;
    using namespace std;

    // test function
    JITCompiler jit;


    // create test func
    auto env = make_unique<LLVMEnvironment>();
    auto& ctx = env->getContext();
    auto FT = llvm::FunctionType::get(ctypeToLLVM<int64_t>(ctx), {ctypeToLLVM<void*>(ctx), ctypeToLLVM<char*>(ctx)}, false);
    auto& mod = *env->getModule().get();
    auto func = llvm::cast<llvm::Function>(mod.getOrInsertFunction("test_main", FT).getCallee());
    auto bb = llvm::BasicBlock::Create(ctx, "entry", func);
    llvm::IRBuilder<> builder(bb);

    HashProxy local(builder);
    HashProxy global(builder, true);

    builder.CreateRet(env->i64Const(0));

    // print
    cout<<env->getIR()<<endl;
}