//
// Created by Leonhard Spiegelberg on 7/3/22.
//
#include <HashHelper.h>

namespace tuplex {
    namespace codegen {
        // these are helper functions to deal with generating code to hash different keys etc.
        extern void hashmap_put(llvm::IRBuilder<>& builder,
                                const SerializableValue& key,
                                const SerializableValue& value) {
            // check what type key is => this determines the structure
        }

        HashProxy::HashProxy(llvm::IRBuilder<>& builder, bool global, const std::string& name) {

            auto hashmap_name = !name.empty() ? name : "hashmap";
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());
            assert(builder.GetInsertBlock()->getParent()->getParent());
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            auto& ctx = mod->getContext();
            if(global) {
                auto var = llvm::cast<llvm::GlobalVariable>(mod->getOrInsertGlobal(hashmap_name, llvm::Type::getInt8PtrTy(ctx, 0)));
                var->setLinkage(llvm::GlobalValue::ExternalLinkage); // common linkage or external linkage??
                var->setAlignment(8);
                var->setInitializer(llvm::ConstantPointerNull::get(llvm::Type::getInt8PtrTy(ctx, 0)));
                _hashmap_ptr = var;
                var = llvm::cast<llvm::GlobalVariable>(mod->getOrInsertGlobal(hashmap_name + "_null_bucket", llvm::Type::getInt8PtrTy(ctx, 0)));
                var->setLinkage(llvm::GlobalValue::ExternalLinkage); // common linkage or external linkage??
                var->setAlignment(8);
                var->setInitializer(llvm::ConstantPointerNull::get(llvm::Type::getInt8PtrTy(ctx, 0)));
                _nullbucket_ptr = var;
            } else {
                // first block alloca

            }
        }

    }
}