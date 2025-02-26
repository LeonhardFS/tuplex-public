//
// Created by leonhards on 4/3/24.
//
#include "gtest/gtest.h"
#include <codegen/LLVMEnvironment.h>
#include <jit/JITCompiler.h>
#include <string>
#include <vector>
#include <physical/codegen/AggregateFunctions.h>
#include <ContextOptions.h>
#include <jit/RuntimeInterface.h>

#include <VirtualFileSystem.h>

namespace tuplex {
    namespace codegen {

        // test function for CodegenHelper.BitmapSizeFunc
        llvm::Function* generate_bitmap_size_test_func(LLVMEnvironment& env, const std::string& func_name) {
            using namespace llvm;
            auto& ctx = env.getContext();
            auto FT = FunctionType::get(ctypeToLLVM<int64_t>(ctx), {ctypeToLLVM<int64_t>(ctx)}, false);
            auto func = getOrInsertFunction(*env.getModule(), func_name, FT);

            auto bb = BasicBlock::Create(ctx, "body", func);
            IRBuilder builder(bb);
            auto args = mapLLVMFunctionArgs(func, {"x"});
            auto arg = args["x"];

            auto ans = calc_bitmap_size_in_64bit_blocks(builder, arg);

            builder.CreateRet(ans);

            return func;
        }

        llvm::Function* generate_unpack_size_and_offset_test_func(LLVMEnvironment& env, const std::string& func_name) {
            using namespace llvm;
            auto& ctx = env.getContext();
            auto FT = FunctionType::get(llvm::Type::getVoidTy(ctx), {ctypeToLLVM<int64_t>(ctx),
                                                                 ctypeToLLVM<int64_t>(ctx)->getPointerTo(),
                                                                 ctypeToLLVM<int64_t>(ctx)->getPointerTo()}, false);
            auto func = getOrInsertFunction(*env.getModule(), func_name, FT);

            auto bb = BasicBlock::Create(ctx, "body", func);
            IRBuilder builder(bb);
            auto args = mapLLVMFunctionArgs(func, {"input", "offset_out", "size_out"});
            auto arg = args["input"];
            auto out_offset = args["offset_out"];
            auto out_size = args["size_out"];
            llvm::Value* offset, *size;
            std::tie(offset, size) = unpack_offset_and_size(builder, arg);

            builder.CreateStore(offset, out_offset);
            builder.CreateStore(size, out_size);

            builder.CreateRetVoid();

            return func;
        }
    }
}


// checks that codegen bitmap func and real bitmap func work correctly
TEST(CodegenHelper, BitmapSizeFunc) {
    using namespace tuplex::codegen;
    using namespace tuplex;

    LLVMEnvironment env;
    JITCompiler jit;

    auto name = "bitmap_size_func_test";
    generate_bitmap_size_test_func(env, name);

    jit.compile(std::move(env.getModule()));
    auto foo = reinterpret_cast<int64_t(*)(int64_t)>(jit.getAddrOfSymbol(name));

    std::vector<int64_t> test_data{0, 1, 10, 64, 128, 2345678, 2947};

    for(auto num_elements : test_data) {
        auto ans = foo(num_elements);
        EXPECT_EQ(ans, calc_bitmap_size_in_64bit_blocks(num_elements));
    }
}

TEST(CodegenHelper, UnpackSizeAndOffset) {
    using namespace tuplex::codegen;
    using namespace tuplex;

    LLVMEnvironment env;
    JITCompiler jit;

    auto name = "unpack_size_and_offset_func_test";
    generate_unpack_size_and_offset_test_func(env, name);

    jit.compile(std::move(env.getModule()));
    auto foo = reinterpret_cast<int64_t(*)(int64_t, int64_t*,int64_t*)>(jit.getAddrOfSymbol(name));

    std::vector<uint64_t> test_data{pack_offset_and_size(0, 0), pack_offset_and_size(10, 8), pack_offset_and_size(12345,36737),
                                   pack_offset_and_size(0, 1035898)};

    for(auto x : test_data) {
        int64_t offset=0, size=0;
        foo(x, &offset, &size);

        int64_t ref_offset, ref_size;
        std::tie(ref_offset, ref_size) = unpack_offset_and_size_from_value(x);

        EXPECT_EQ(offset, ref_offset);
        EXPECT_EQ(size, ref_size);
    }
}