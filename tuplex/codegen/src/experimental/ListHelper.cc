//
// Created by leonhard on 9/22/22.
//

#include <experimental/ListHelper.h>
#include <experimental/StructDictHelper.h>
#include <codegen/FlattenedTuple.h>

namespace tuplex {
    namespace codegen {

        // forward declare:
        llvm::Value* list_serialize_varitems_to(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr,
                                                const python::Type& list_type, llvm::Value* dest_ptr,
                                                std::function<llvm::Value*(LLVMEnvironment&, const IRBuilder&, llvm::Value*, llvm::Value*)> f_item_size,
                                                std::function<llvm::Value*(LLVMEnvironment&, const IRBuilder&, llvm::Value*, llvm::Value*, llvm::Value*)> f_item_serialize_to);

        void list_free(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type) {
            // list should only use runtime allocations -> hence free later!
        }

        llvm::Function* create_list_serialize_bitmap_function(LLVMEnvironment& env, const std::string& func_key) {

            using namespace llvm;

            auto& ctx = env.getContext();
            auto FT = FunctionType::get(llvm::Type::getVoidTy(ctx), {env.i64Type(), env.i8ptrType(), env.i8ptrType()}, false);
            llvm::Function* func = getOrInsertFunction(*env.getModule(), func_key, FT);
            func->setLinkage(llvm::Function::InternalLinkage);

            auto bbEntry = BasicBlock::Create(ctx, "body", func);

            IRBuilder builder(bbEntry);

            auto args = mapLLVMFunctionArgs(func, {"num_elements", "bitmap_ptr", "dest_ptr"});

            auto num_elements = args["num_elements"];
            auto bitmap_ptr = args["bitmap_ptr"];
            auto dest_ptr = args["dest_ptr"];

            // create loop to decode bits and store them in bitmap
            auto bbLoopHeader = BasicBlock::Create(env.getContext(), "list_serialize_bitmap_loop_header", builder.GetInsertBlock()->getParent());
            auto bbLoopBody = BasicBlock::Create(env.getContext(), "list_serialize_bitmap_loop_body", builder.GetInsertBlock()->getParent());
            auto bbLoopDone = BasicBlock::Create(env.getContext(), "list_serialize_bitmap_loop_done", builder.GetInsertBlock()->getParent());

            // memset dest ptr to zero
            auto bitmap_size = calc_bitmap_size_in_64bit_blocks(builder, num_elements);
            builder.CreateMemSet(dest_ptr, env.i8Const(0), bitmap_size, 0);


            auto i_var = env.CreateFirstBlockAlloca(builder, env.i64Type());
            builder.CreateStore(env.i64Const(0), i_var);

            env.printValue(builder, num_elements, "serializing bitmap for List with n_elements=");

            builder.CreateBr(bbLoopHeader);

            {
                builder.SetInsertPoint(bbLoopHeader);
                auto i = builder.CreateLoad(builder.getInt64Ty(), i_var);
                auto loop_cond = builder.CreateICmpSLT(i, num_elements);
                builder.CreateCondBr(loop_cond, bbLoopBody, bbLoopDone);
            }


            {
                builder.SetInsertPoint(bbLoopBody);
                auto i = builder.CreateLoad(builder.getInt64Ty(), i_var);

                // read from bitmap ptr
                llvm::Value* is_null = builder.CreateLoad(env.i8Type(), builder.CreateGEP(env.i8Type(), bitmap_ptr, i));
                env.printValue(builder, is_null, "element is null (i8): ");
                is_null = builder.CreateICmpEQ(is_null, env.i8Const(1));

                env.printValue(builder, i, "serializing element no=");
                env.printValue(builder, is_null, "element is null: ");

                auto i64_ptr = builder.CreatePointerCast(dest_ptr, env.i64ptrType());
                auto block_idx = builder.CreateSDiv(i, env.i64Const(64));
                auto target_block_ptr = builder.CreateGEP(env.i64Type(), i64_ptr, block_idx);
                llvm::Value* block = builder.CreateLoad(env.i64Type(), target_block_ptr);
                auto bit_idx = builder.CreateSRem(i, env.i64Const(64));


                // create nth bit set & or to block + save block back!
                block = builder.CreateOr(block, builder.CreateShl(
                        builder.CreateZExt(is_null, env.i64Type()),
                        bit_idx));

                env.printValue(builder, block, "block after: ");

                builder.CreateStore(block, target_block_ptr);

                builder.CreateStore(builder.CreateAdd(i, env.i64Const(1)), i_var);
                builder.CreateBr(bbLoopHeader);
            }

            builder.SetInsertPoint(bbLoopDone);

            builder.CreateRetVoid();

            return func;
        }

        void list_serialize_bitmap_to(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* dest_ptr) {
            using namespace llvm;

            auto num_elements = list_length(env, builder, list_ptr, list_type);

            assert(list_type.elementType().isOptionType());

            // get bitmap pointer
            auto llvm_list_type = env.createOrGetListType(list_type);
            auto n_struct_elements = llvm_list_type->getStructNumElements();

            auto bitmap_ptr = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, n_struct_elements - 1);

            // TOOD: make sure names do not clash...
            auto func_key = "list_serialize_bitmap";
            if(!env.functionCacheHasKey(func_key)) {
                auto func = create_list_serialize_bitmap_function(env, func_key);
                env.functionCacheSet(func_key, func);
            }
            auto func = env.functionCacheGet(func_key);

            builder.CreateCall(func, {num_elements, builder.CreatePointerCast(bitmap_ptr, env.i8ptrType()), builder.CreatePointerCast(dest_ptr, env.i8ptrType())});
        }

        llvm::Value* list_deserialize_bitmap(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* ptr, llvm::Value* num_elements) {

            using namespace llvm;

            // alloc pointer of num_elements bytes for fast read.
            auto bitmap_ptr = env.malloc(builder, num_elements);

            // create loop to decode bits and store them in bitmap
            auto bbLoopHeader = BasicBlock::Create(env.getContext(), "decode_list_bitmap_loop_header", builder.GetInsertBlock()->getParent());
            auto bbLoopBody = BasicBlock::Create(env.getContext(), "decode_list_bitmap_loop_body", builder.GetInsertBlock()->getParent());
            auto bbLoopDone = BasicBlock::Create(env.getContext(), "decode_list_bitmap_loop_done", builder.GetInsertBlock()->getParent());

            auto i_var = env.CreateFirstBlockAlloca(builder, env.i64Type());
            builder.CreateStore(env.i64Const(0), i_var);
            builder.CreateBr(bbLoopHeader);

            {
                builder.SetInsertPoint(bbLoopHeader);
                auto i = builder.CreateLoad(builder.getInt64Ty(), i_var);
                auto loop_cond = builder.CreateICmpSLT(i, num_elements);
                builder.CreateCondBr(loop_cond, bbLoopBody, bbLoopDone);
            }


            {
                builder.SetInsertPoint(bbLoopBody);
                auto i = builder.CreateLoad(builder.getInt64Ty(), i_var);

                // read from ptr
                auto i64_ptr = builder.CreatePointerCast(ptr, env.i64ptrType());
                auto block_idx = builder.CreateSDiv(i, env.i64Const(64));
                auto block = builder.CreateLoad(env.i64Type(), builder.CreateGEP(env.i64Type(), i64_ptr, block_idx));

                auto bit_idx = builder.CreateSRem(i, env.i64Const(64));

                auto is_null = env.extractNthBit(builder, block, bit_idx);

                env.printValue(builder, i, "extracting bit number=");
                env.printValue(builder, is_null, "nth element is null: ");

                auto byte_value = builder.CreateZExt(is_null, env.i8Type());
                builder.CreateStore(byte_value, builder.CreateGEP(env.i8Type(), bitmap_ptr, i));

                builder.CreateStore(builder.CreateAdd(i, env.i64Const(1)), i_var);
                builder.CreateBr(bbLoopHeader);
            }

            builder.SetInsertPoint(bbLoopDone);
            return bitmap_ptr;
        }

        void list_init_empty(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type) {
            using namespace llvm;
            using namespace std;

            assert(list_type.isListType());
            if(python::Type::EMPTYLIST == list_type)
                return; // nothing to do

            // check ptr has correct type
            auto llvm_list_type = env.createOrGetListType(list_type);
            if(list_ptr->getType() != llvm_list_type->getPointerTo())
                throw std::runtime_error("expected pointer of " + env.getLLVMTypeName(llvm_list_type->getPointerTo()) + " but list_ptr has " + env.getLLVMTypeName(list_ptr->getType()));

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();
            auto elements_optional = elementType.isOptionType();
            if(elements_optional)
                elementType = elementType.getReturnType();

            if(elementType.isSingleValued()) {
                if(elements_optional) {
                    auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
                    builder.CreateStore(env.i64Const(0), idx_capacity);
                    auto idx_size = builder.CreateStructGEP(list_ptr, llvm_list_type, 1); assert(idx_size->getType() == env.i64ptrType());
                    builder.CreateStore(env.i64Const(0), idx_size);
                    auto idx_opt_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);
                    builder.CreateStore(env.nullConstant(env.i8ptrType()), idx_opt_values);
                } else {
                    // the list is represented as single i64
                    builder.CreateStore(env.i64Const(0), list_ptr);
                }
            } else if(elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN) {

                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);
                auto idx_size = builder.CreateStructGEP(list_ptr, llvm_list_type, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_size);

                auto idx_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);
                llvm::Type* llvm_element_type = env.pythonToLLVMType(elementType);
                builder.CreateStore(env.nullConstant(llvm_element_type->getPointerTo()), idx_values);

                if(elements_optional) {
                    auto idx_opt_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 3);
                    builder.CreateStore(env.nullConstant(env.i8ptrType()), idx_opt_values);
                }

            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {

                // string array/pyobject array is special. It contains 4 members! capacity, size and then arrays for the values and sizes
                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);
                auto idx_size = builder.CreateStructGEP(list_ptr, llvm_list_type, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_size);

                auto idx_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);
                builder.CreateStore(env.nullConstant(env.i8ptrType()->getPointerTo()), idx_values);

                auto idx_sizes = builder.CreateStructGEP(list_ptr, llvm_list_type, 3);
                builder.CreateStore(env.nullConstant(env.i64ptrType()), idx_sizes);

                if(elements_optional) {
                    auto idx_opt_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 4);
                    builder.CreateStore(env.nullConstant(env.i8ptrType()), idx_opt_values);
                }
            } else if(elementType.isStructuredDictionaryType() || elementType == python::Type::GENERICDICT) {
                // pointer to the structured dict type!

                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);
                auto idx_size = builder.CreateStructGEP(list_ptr, llvm_list_type, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_size);

                auto idx_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);

                if(elementType.isStructuredDictionaryType()) {
                    auto llvm_element_type = env.getOrCreateStructuredDictType(elementType);
                    builder.CreateStore(env.nullConstant(llvm_element_type->getPointerTo()->getPointerTo()), idx_values);
                } else {
                    // array of cJSON* objects.
                    builder.CreateStore(env.nullConstant(env.i8ptrType()->getPointerTo()), idx_values);
                }

                if(elements_optional) {
                    auto idx_opt_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 3);
                    builder.CreateStore(env.nullConstant(env.i8ptrType()), idx_opt_values);
                }

            } else if(elementType.isListType()) {
                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);
                auto idx_size = builder.CreateStructGEP(list_ptr, llvm_list_type, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_size);

                auto llvm_element_type = env.createOrGetListType(elementType);
                auto idx_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);
                builder.CreateStore(env.nullConstant(llvm_element_type->getPointerTo()->getPointerTo()), idx_values);

                if(elements_optional) {
                    auto idx_opt_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 3);
                    builder.CreateStore(env.nullConstant(env.i8ptrType()), idx_opt_values);
                }
            } else if(elementType.isTupleType()) {
                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);
                auto idx_size = builder.CreateStructGEP(list_ptr, llvm_list_type, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_size);

                auto llvm_element_type = env.getOrCreateTupleType(elementType);

                auto idx_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);
                builder.CreateStore(env.nullConstant(llvm_element_type->getPointerTo()), idx_values);

                if(elements_optional) {
                    auto idx_opt_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 3);
                    builder.CreateStore(env.nullConstant(env.i8ptrType()), idx_opt_values);
                }
            } else {
                throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " Unsupported list element type: " + list_type.desc());
            }
        }

        // helper function to allocate and store a pointer via rtmalloc + potentially initialize it
        void list_init_array(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* capacity, size_t struct_index, bool initialize) {
            using namespace llvm;
            using namespace std;

            assert(list_ptr && capacity);

            auto llvm_list_type = env.createOrGetListType(list_type);

            auto idx_values = builder.CreateStructGEP(list_ptr, llvm_list_type, struct_index);
            assert(idx_values && idx_values->getType()->isPointerTy());

            assert(llvm_list_type->isStructTy());
            assert(struct_index < llvm_list_type->getStructNumElements());

            // this is not the element type, but rather the type of the data array.
            // the struct stores a pointer, so the size will be 8 bytes. However, correct thing is to allocate
            // actual struct size if its a pointer to struct.
            llvm::Type* llvm_data_type = llvm_list_type->getStructElementType(struct_index);

            auto element_type = list_type.withoutOption().elementType();
            if((element_type.isTupleType() && element_type != python::Type::EMPTYTUPLE)) {
                llvm_data_type = env.pythonToLLVMType(element_type);
            }

            const auto& DL = env.getModule()->getDataLayout();
            // debug, print type name.
            std::string t_name = env.getLLVMTypeName(llvm_data_type);
            size_t llvm_data_element_size = DL.getTypeAllocSize(llvm_data_type);

            // perform sanity check in LLVM9:
#if LLVM_VERSION_MAJOR < 15
            if(llvm_list_type->getStructElementType(struct_index)->isPointerTy()) {
                auto dbg_type = llvm_list_type->getStructElementType(struct_index)->getPointerElementType();
                if(dbg_type->isStructTy()) {
                    assert(llvm_data_type == dbg_type);
                }
            }
#endif
            // allocate new memory of size sizeof(int64_t) * capacity
            auto data_size = builder.CreateMul(env.i64Const(llvm_data_element_size), capacity);

            auto llvm_target_data_type = !llvm_data_type->isPointerTy() ? llvm_data_type->getPointerTo() : llvm_data_type; // <-- could be primitive type.
            auto data_ptr = builder.CreatePointerCast(env.malloc(builder, data_size), llvm_target_data_type);

            env.printValue(builder, data_size, "allocated data_ptr for " + list_type.desc() + " of size (in bytes): ");

            if(initialize) {
                // call memset
                builder.CreateMemSet(data_ptr, env.i8Const(0), data_size, 0);
            }

            builder.CreateStore(data_ptr, idx_values);
        }


        void list_reserve_capacity(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* capacity, bool initialize) {
            // reserve capacity
            // --> free upfront
            list_free(env, builder, list_ptr, list_type);

            using namespace llvm;
            using namespace std;

            assert(list_ptr);
            assert(capacity && capacity->getType() == env.i64Type());

            assert(list_type.isListType());
            if(python::Type::EMPTYLIST == list_type)
                return; // nothing to do

            auto llvm_list_type = env.createOrGetListType(list_type);

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();
            auto elements_optional = elementType.isOptionType();
            if(elements_optional)
                elementType = elementType.getReturnType();

            if(elementType.isSingleValued()) {
                if(elements_optional) {
                    // a list consists of 3 fields: capacity, size and a pointer to the members
                    auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
                    builder.CreateStore(capacity, idx_capacity);

                    list_init_array(env, builder, list_ptr, list_type, capacity, 2, initialize);
                } else
                // the list is represented as single i64
                builder.CreateStore(env.i64Const(0), list_ptr);
            } else if(elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN) {

                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(capacity, idx_capacity);

                list_init_array(env, builder, list_ptr, list_type, capacity, 2, initialize);
                if(elements_optional)
                    list_init_array(env, builder, list_ptr, list_type, capacity, 3, initialize);
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {

                // string array/pyobject array is special. It contains 4 members! capacity, size and then arrays for the values and sizes
                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(capacity, idx_capacity);

                list_init_array(env, builder, list_ptr, list_type, capacity, 2, initialize);
                list_init_array(env, builder, list_ptr, list_type, capacity, 3, initialize);
                if(elements_optional)
                    list_init_array(env, builder, list_ptr, list_type, capacity, 4, initialize);
            } else if(elementType.isStructuredDictionaryType() || elementType == python::Type::GENERICDICT) {

                // pointer to the structured dict type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                list_init_array(env, builder, list_ptr, list_type, capacity, 2, initialize);
                if(elements_optional)
                    list_init_array(env, builder, list_ptr, list_type, capacity, 3, initialize);

            } else if(elementType.isListType()) {
                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                list_init_array(env, builder, list_ptr, list_type, capacity, 2, initialize);
                if(elements_optional)
                    list_init_array(env, builder, list_ptr, list_type, capacity, 3, initialize);
            } else if(elementType.isTupleType()) {
                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                list_init_array(env, builder, list_ptr, list_type, capacity, 2, initialize);
                if(elements_optional)
                    list_init_array(env, builder, list_ptr, list_type, capacity, 3, initialize);
            } else {
                throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " Unsupported list element type: " + list_type.desc());
            }
        }

        // NEW version
        SerializableValue list_load_value(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* idx) {
            SerializableValue ret;
            auto element_type = list_type.elementType();

            // special cases: single valued elements
            if(element_type.isSingleValued()) {
                if(python::Type::NULLVALUE == element_type)
                    return SerializableValue(nullptr, nullptr, env.i1Const(true));

                if(element_type.isConstantValued())
                    return constantValuedTypeToLLVM(builder, element_type);

                if(element_type == python::Type::EMPTYTUPLE) {
                    auto llvm_empty_tuple_type = env.getEmptyTupleType();
                    auto alloc = builder.CreateAlloca(llvm_empty_tuple_type, 0, nullptr);
                    auto load = builder.CreateLoad(llvm_empty_tuple_type, alloc);
                    return {load, env.i64Const(sizeof(int64_t))};
                }

                if(element_type == python::Type::EMPTYDICT || element_type == python::Type::EMPTYLIST) {
                    return {};
                }

                throw std::runtime_error("list load of single-valued type " + element_type.desc() + " not yet supported");
            }

            // define struct load indices
            int data_index = 2;
            int nullmap_index = 3;
            int size_idx = -1;

            // adjust for certain types
            if(element_type == python::Type::STRING || element_type == python::Type::PYOBJECT) {
                size_idx = 3;
                nullmap_index = 4;
            }

            // List[Option[str]] or List[Option[pyobject]]
            if(element_type.withoutOption() == python::Type::STRING || element_type.withoutOption() == python::Type::PYOBJECT) {
                size_idx = 3;
                nullmap_index = 4;
            }

            if(!element_type.isOptionType())
                nullmap_index = -1;


            auto llvm_list_type = env.createOrGetListType(list_type);


            if(element_type.withoutOption().isSingleValued())
                nullmap_index = 2;

            // (1) is null
            // load whether entry is null (or not)
            if(nullmap_index >= 1) {
                auto nullmap_ptr = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, nullmap_index);
                auto is_null = builder.CreateLoad(builder.getInt8Ty(), builder.CreateGEP(builder.getInt8Ty(), nullmap_ptr, idx));
                assert(is_null->getType() == env.i8Type());
                ret.is_null = builder.CreateICmpNE(is_null, env.i8Const(0));
            } else {
                ret.is_null = env.i1Const(false);
            }


            // struct and list element types require special handling
            if(element_type.withoutOption().isStructuredDictionaryType() || element_type.withoutOption().isListType()) {
                // special case: empty list -> return allocated empty list structure.
                if(element_type.withoutOption() == python::Type::EMPTYLIST) {
                    // note: that isnull is filled above...
                    auto ptr = env.CreateHeapAlloca(builder, llvm_list_type, true);
                    ret.val = ptr;
                    ret.size = env.i64Const(0);
                    return ret;
                }


                // special case: data ptr of List is heap-allocated struct dict pointers. Hence, use heap here
                auto llvm_value_type = env.pythonToLLVMType(element_type.withoutOption());
                auto data_ptr = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, data_index);

                llvm::Value* data_entry = nullptr;
                if(element_type.withoutOption().isStructuredDictionaryType() || element_type.withoutOption().isListType())
                    data_entry = builder.CreateLoad(llvm_value_type->getPointerTo(), builder.CreateGEP(llvm_value_type->getPointerTo(), data_ptr, idx));

                ret.val = data_entry;
                assert(ret.val->getType()->isPointerTy());
                ret.size = env.i64Const(0); // dummy value.

                return ret;
            }
            // @TODO: list in list loading...
            if(element_type.withoutOption().isListType())
                throw std::runtime_error("list type " + list_type.desc() + " load not yet supported.");

            // (2) fill value
            auto llvm_value_type = env.pythonToLLVMType(list_type.elementType().withoutOption());
            auto data_ptr = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, data_index);
            auto data_entry = builder.CreateLoad(llvm_value_type, builder.CreateGEP(llvm_value_type, data_ptr, idx));
            ret.val = data_entry;

            // special case string: -> could be nullptr, to ensure printing fill with empty str...
            if(element_type.withoutOption() == python::Type::STRING)
                ret.val = builder.CreateSelect(builder.CreateICmpEQ(ret.val, env.nullConstant(env.i8ptrType())), env.strConst(builder, "<null>"), ret.val);


            // (3) fill size
            if(size_idx >= 0) {
                auto size_ptr = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, size_idx);
                ret.size = builder.CreateLoad(builder.getInt64Ty(), builder.CreateGEP(builder.getInt64Ty(), size_ptr, idx));
            } else {
                ret.size = env.i64Const(sizeof(double));
            }


            env.printValue(builder, idx, "list access with index=");
            env.printValue(builder, ret.val, "list [] got=");

            if(element_type.withoutOption() == python::Type::GENERICDICT) {
                auto formatted_str = call_cjson_to_string(builder, ret.val);
                env.printValue(builder, formatted_str.val, list_type.desc() + " encodes following JSON dict: ");
            }

            return ret;
        }

//        // old and buggy
//        SerializableValue list_load_value(LLVMEnvironment& env, const IRBuilder& builder,
//                              llvm::Value* list_ptr,
//                              const python::Type& list_type,
//                              llvm::Value* idx) {
//            using namespace llvm;
//            using namespace std;
//
//            SerializableValue ret;
//
//            assert(list_ptr);
//            assert(idx && idx->getType() == env.i64Type());
//
//            assert(list_type.isListType());
//            if(python::Type::EMPTYLIST == list_type)
//                throw std::runtime_error("there's no value to be loaded from an empty list");
//
//            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
//            // init accordingly.
//            auto elementType = list_type.elementType();
//            auto elements_optional = elementType.isOptionType();
//            if(elements_optional)
//                elementType = elementType.getReturnType();
//
//            // if optional elements are used, only store if optional indicator is true!
//            BasicBlock* bElementIsNotNull = nullptr;
//            BasicBlock* bLoadDone = nullptr;
//            BasicBlock* bIsNullLastBlock = nullptr;
//            if(elements_optional) {
//                unsigned struct_opt_index = -1;
//                if(elementType.isSingleValued()) {
//                    // nothing gets stored, ignore.
//                    struct_opt_index = 2;
//                } else if(elementType == python::Type::I64
//                          || elementType == python::Type::F64
//                          || elementType == python::Type::BOOLEAN) {
//                    struct_opt_index = 3;
//                } else if(elementType == python::Type::STRING
//                          || elementType == python::Type::PYOBJECT) {
//                    struct_opt_index = 4;
//                } else if(elementType.isStructuredDictionaryType()) {
//                    struct_opt_index = 3;
//                } else if(elementType.isListType()) {
//                    struct_opt_index = 3;
//                } else if(elementType.isTupleType()) {
//                    struct_opt_index = 3;
//                } else {
//                    throw std::runtime_error("Unsupported list element type: " + list_type.desc());
//                }
//
//                // create blocks
//                auto& ctx = env.getContext();
//                auto F = builder.GetInsertBlock()->getParent();
//
//                bElementIsNotNull = BasicBlock::Create(ctx, "load_element", F);
//                bLoadDone = BasicBlock::Create(ctx, "load_done", F);
//
//                // load whether null or not
//                if(list_ptr->getType()->isPointerTy()) {
//                    auto idx_nulls = builder.CreateStructGEP(list_ptr, llvm_list_type, struct_opt_index);
//                    auto ptr = builder.CreateLoad(idx_nulls);
//                    auto idx_null = builder.CreateGEP(ptr, idx);
//                    ret.is_null = builder.CreateTrunc(builder.CreateLoad(idx_null), env.i1Type()); // could be == i8(0) as well
//                } else {
//                    auto idx_nulls = builder.CreateStructGEP(list_ptr, llvm_list_type, struct_opt_index);
//                    auto ptr = idx_nulls;
//                    auto idx_null = builder.CreateGEP(ptr, idx);
//                    ret.is_null = builder.CreateTrunc(builder.CreateLoad(idx_null), env.i1Type()); // could be == i8(0) as well
//                }
//
//                // jump now according to block!
//                bIsNullLastBlock = builder.GetInsertBlock();
//                builder.CreateCondBr(ret.is_null, bLoadDone, bElementIsNotNull);
//                builder.SetInsertPoint(bElementIsNotNull);
//            }
//
//            if(elementType.isSingleValued()) {
//                // nothing to load, keep as is.
//                ret.is_null = env.i1Const(false);
//            } else if(elementType == python::Type::I64
//                      || elementType == python::Type::F64
//                      || elementType == python::Type::BOOLEAN) {
//
//                auto idx_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);
//                llvm::Type* llvm_element_type = env.pythonToLLVMType(elementType);
//                if(list_ptr->getType()->isPointerTy()) {
//                    auto ptr = builder.CreateLoad(idx_values);
//                    auto idx_value = builder.CreateGEP(ptr, idx);
//                    ret.val = builder.CreateLoad(idx_value);
//                } else {
//                    auto idx_value = builder.CreateGEP(idx_values, idx);
//                    ret.val = builder.CreateLoad(idx_value);
//                }
//                ret.size = env.i64Const(sizeof(int64_t));
//                ret.is_null = env.i1Const(false);
//            } else if(elementType == python::Type::STRING
//                      || elementType == python::Type::PYOBJECT) {
//                auto idx_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);
//                auto idx_sizes = builder.CreateStructGEP(list_ptr, llvm_list_type, 3);
//
//                if(list_ptr->getType()->isPointerTy()) {
//                    auto ptr_values = builder.CreateLoad(idx_values);
//                    auto ptr_sizes = builder.CreateLoad(idx_sizes);
//
//                    auto idx_value = builder.CreateGEP(ptr_values, idx);
//                    auto idx_size = builder.CreateGEP(ptr_sizes, idx);
//
//                    ret.val = builder.CreateLoad(idx_value);
//                    ret.size = builder.CreateLoad(idx_size);
//                } else {
//                    auto idx_value = builder.CreateGEP(idx_values, idx);
//                    auto idx_size = builder.CreateGEP(idx_sizes, idx);
//
//                    ret.val = builder.CreateLoad(idx_value);
//                    ret.size = builder.CreateLoad(idx_size);
//                }
//
//            } else if(elementType.isStructuredDictionaryType()) {
//                // pointer to the structured dict type!
//
//                // this is quite simple, store a HEAP allocated pointer.
//                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
//                builder.CreateStore(env.i64Const(0), idx_capacity);
//
//                auto llvm_element_type = env.getOrCreateStructuredDictType(elementType);
//                auto idx_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);
//
//                // load pointer
//                auto ptr = builder.CreateLoad(idx_values);
//                auto idx_value = builder.CreateGEP(ptr, idx);
//
//                ret.val = builder.CreateLoad(idx_value);
//            } else if(elementType.isListType()) {
//                // pointers to the list type!
//                // similar to above - yet, keep it here extra for more control...
//                // a list consists of 3 fields: capacity, size and a pointer to the members
//                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
//                builder.CreateStore(env.i64Const(0), idx_capacity);
//
//                auto llvm_element_type = env.createOrGetListType(elementType);
//                auto idx_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);
//
//                // load pointer
//                auto ptr = builder.CreateLoad(idx_values);
//                auto idx_value = builder.CreateGEP(ptr, idx);
//
//                ret.val = builder.CreateLoad(idx_value);
//            } else if(elementType.isTupleType()) {
//                throw std::runtime_error("tuple-load not yet supported, fix with code below");
//
//                // below is store code:
////                // pointers to the list type!
////                // similar to above - yet, keep it here extra for more control...
////                // a list consists of 3 fields: capacity, size and a pointer to the members
////                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
////                builder.CreateStore(env.i64Const(0), idx_capacity);
////
////                auto llvm_element_type = env.getOrCreateTupleType(elementType);
////
////                auto ptr_values = CreateStructLoad(builder, list_ptr, 2); // this should be a pointer
////                auto tuple = value.val;
////
////                // load FlattenedTuple from value (this ensures it is a heap ptr)
////                FlattenedTuple ft = FlattenedTuple::fromLLVMStructVal(&env, builder, tuple, elementType);
////
////                // this here works.
////                // auto heap_ptr = ft.loadToPtr(builder);
////
////                // must be a heap ptr, else invalid.
////                auto heap_ptr = ft.loadToHeapPtr(builder);
////
////                // // debug:
////                // env.printValue(builder, idx, "storing tuple of type " + elementType.desc() + " at index: ");
////                // env.printValue(builder, ft.getSize(builder), "tuple size is: ");
////                // env.printValue(builder, heap_ptr, "pointer to store: ");
////
////                // store pointer --> should be HEAP allocated pointer. (maybe add attributes to check?)
////                auto target_idx = builder.CreateGEP(ptr_values, idx);
////
////                // store the heap ptr.
////                builder.CreateStore(heap_ptr, target_idx, true);
////
////                // debug:
////                // env.printValue(builder, builder.CreateLoad(target_idx), "pointer stored - post update: ");
//
//            } else {
//                throw std::runtime_error("Unsupported list element type: " + list_type.desc());
//            }
//
//            // connect blocks + create storage for null
//            if(elements_optional) {
//                assert(bLoadDone);
//                auto lastBlock = builder.GetInsertBlock();
//                builder.CreateBr(bLoadDone);
//                builder.SetInsertPoint(bLoadDone);
//
//                // update val/size with phi based
//                auto phi_val = builder.CreatePHI(ret.val->getType(), 2);
//                auto phi_size = builder.CreatePHI(env.i64Type(), 2);
//                phi_val->addIncoming(ret.val, lastBlock);
//                phi_val->addIncoming(env.nullConstant(ret.val->getType()), bIsNullLastBlock);
//                phi_size->addIncoming(ret.size, lastBlock);
//                phi_size->addIncoming(env.i64Const(0), bIsNullLastBlock);
//                ret.val = phi_val;
//                ret.size = phi_size;
//            }
//
//            return ret;
//        }


        void list_store_value(LLVMEnvironment& env, const IRBuilder& builder,
                              llvm::Value* list_ptr,
                              const python::Type& list_type,
                              llvm::Value* idx,
                              const SerializableValue& value) {
            using namespace llvm;
            using namespace std;

            assert(list_ptr);
            assert(idx && idx->getType() == env.i64Type());

            // use i32 for idx because of LLVM bugs
            idx = builder.CreateZExtOrTrunc(idx, env.i32Type());

            assert(list_type.isListType());
            if(python::Type::EMPTYLIST == list_type)
                return; // nothing to do

            // @TODO: create verify function for list pointer -> only one valid scenario ok...!

            // check ptr has correct type
            auto llvm_list_type = env.createOrGetListType(list_type);
            if(list_ptr->getType() != llvm_list_type->getPointerTo())
                throw std::runtime_error("expected pointer of " + env.getLLVMTypeName(llvm_list_type->getPointerTo()) + " but list_ptr has " + env.getLLVMTypeName(list_ptr->getType()));

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();
            auto elements_optional = elementType.isOptionType();
            if(elements_optional)
                elementType = elementType.getReturnType();

            // if optional elements are used, only store if optional indicator is true!
            BasicBlock* bElementIsNotNull = nullptr;
            BasicBlock* bStoreDone = nullptr;
            if(elements_optional) {
                unsigned struct_opt_index = -1;
                if(elementType.isSingleValued()) {
                    // nothing gets stored, ignore.
                    struct_opt_index = 2;
                } else if(elementType == python::Type::I64
                          || elementType == python::Type::F64
                          || elementType == python::Type::BOOLEAN) {
                    struct_opt_index = 3;
                } else if(elementType == python::Type::STRING
                          || elementType == python::Type::PYOBJECT) {
                    struct_opt_index = 4;
                } else if(elementType.isStructuredDictionaryType() || elementType == python::Type::GENERICDICT) {
                    // note that internally genericdict is stored as cJSON* objects.
                    struct_opt_index = 3;
                } else if(elementType.isListType()) {
                    struct_opt_index = 3;
                } else if(elementType.isTupleType()) {
                    struct_opt_index = 3;
                } else {
                    throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " Unsupported list element type: " + list_type.desc());
                }

                // create blocks
                auto& ctx = env.getContext();
                auto F = builder.GetInsertBlock()->getParent();

                bElementIsNotNull = BasicBlock::Create(ctx, "store_element", F);
                bStoreDone = BasicBlock::Create(ctx, "store_done", F);

                // need to ALWAYS store null
                auto nullmap_ptr = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, struct_opt_index);
                auto is_null_to_store = builder.CreateZExtOrTrunc(value.is_null, env.i8Type());
                builder.CreateStore(is_null_to_store, builder.MovePtrByBytes( nullmap_ptr, idx), true);


                env.printValue(builder, is_null_to_store, "value to store is: ");
                env.printValue(builder, idx, "storing value to idx in list: ");
                env.printValue(builder, value.is_null, "value is null: ");

                // jump now according to block!
                builder.CreateCondBr(value.is_null, bStoreDone, bElementIsNotNull);
                builder.SetInsertPoint(bElementIsNotNull);
            }


            if(elementType.isSingleValued()) {
                // nothing gets stored, ignore.
            } else if(elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN) {

                // new:
                auto data_index = 2;
                auto data_ptr = builder.CreateStructLoad(llvm_list_type, list_ptr, data_index);
                auto llvm_value_type = env.pythonToLLVMType(elementType);
                builder.CreateStore(value.val, builder.CreateGEP(llvm_value_type, data_ptr, idx));

                // old:
                // auto idx_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);
                // llvm::Type* llvm_element_type = env.pythonToLLVMType(elementType);
                // assert(value.val && llvm_element_type == value.val->getType());
                // auto ptr = builder.CreateLoad(idx_values);
                // auto idx_value = builder.CreateGEP(ptr, idx);
                // builder.CreateStore(value.val, idx_value);
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {
                auto idx_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);
                auto idx_sizes = builder.CreateStructGEP(list_ptr, llvm_list_type, 3);

                // store value.val and value.size
                assert(value.val && value.size);

                auto ptr_values = builder.CreateLoad(llvm_list_type->getStructElementType(2), idx_values);
                auto ptr_sizes = builder.CreateLoad(llvm_list_type->getStructElementType(3), idx_sizes);

                auto llvm_value_type = env.pythonToLLVMType(elementType);
                auto idx_value = builder.CreateGEP(llvm_value_type, ptr_values, idx);
                auto idx_size = builder.CreateGEP(builder.getInt64Ty(), ptr_sizes, idx);

                builder.CreateStore(value.val, idx_value);
                builder.CreateStore(value.size, idx_size);
            } else if(elementType == python::Type::GENERICDICT) {
                // this is quite simple, store a HEAP allocated pointer.
                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                auto idx_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);

                // store struct to pointer
                assert(value.val);
                auto llvm_value_type = env.pythonToLLVMType(elementType);
                auto ptr = builder.CreateLoad(llvm_list_type->getStructElementType(2), idx_values);
                auto idx_value = builder.CreateGEP(llvm_value_type, ptr, idx);

                auto dict = value.val;
                builder.CreateStore(dict, idx_value);
            } else if(elementType.isStructuredDictionaryType()) {
                // pointer to the structured dict type!

                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                auto llvm_element_type = env.pythonToLLVMType(elementType);
                auto ptr_values = builder.CreateStructLoad(llvm_list_type, list_ptr, 2);

                auto target_idx = builder.CreateGEP(llvm_element_type->getPointerTo(), ptr_values, idx);

                llvm::Value* element_as_ptr = nullptr;
                // is element given as struct or pointer?
                // => if pointer, assume it is a valid heap ptr.
                if(value.val->getType()->isStructTy()) {
                    // not given as pointer, hence alloc heap (!) pointer and store with this.
                    element_as_ptr = env.CreateHeapAlloca(builder, llvm_element_type);

                    env.debugPrint(builder, "allocated new heap ptr for struct, value to store is: ");
                    struct_dict_print(env, builder, value, elementType);

                    builder.CreateStore(value.val, element_as_ptr);
                } else {
                    element_as_ptr = value.val;
                }

                env.debugPrint(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " list_store_value into " + list_type.desc());
                struct_dict_print(env, builder, SerializableValue(element_as_ptr, nullptr), elementType);

                // store into array
                assert(element_as_ptr->getType()->getPointerTo() == target_idx->getType());
                builder.CreateStore(element_as_ptr, target_idx, true);
            } else if(elementType.isListType()) {
                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                auto llvm_element_type = env.createOrGetListType(elementType);
                auto ptr_values = builder.CreateStructLoad(llvm_list_type, list_ptr, 2);

                auto target_idx = builder.CreateGEP(llvm_element_type->getPointerTo(), ptr_values, idx);

                llvm::Value* element_as_ptr = nullptr;
                // is element given as struct or pointer?
                // => if pointer, assume it is a valid heap ptr.
                if(value.val->getType()->isStructTy()) {
                    // not given as pointer, hence alloc heap (!) pointer and store with this.
                    element_as_ptr = env.CreateHeapAlloca(builder, llvm_element_type);
                    builder.CreateStore(value.val, element_as_ptr);
                } else {
                    element_as_ptr = value.val;
                }

                // store into array
                assert(element_as_ptr->getType()->getPointerTo() == target_idx->getType());
                builder.CreateStore(element_as_ptr, target_idx, true);
            } else if(elementType.isTupleType() && elementType != python::Type::EMPTYTUPLE) {
                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = builder.CreateStructGEP(list_ptr, llvm_list_type, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                auto llvm_element_type = env.getOrCreateTupleType(elementType);

                auto ptr_values = builder.CreateStructLoad(llvm_list_type, list_ptr, 2); // this should be a pointer

                // load FlattenedTuple from value (this ensures it is a heap ptr)
                FlattenedTuple ft = FlattenedTuple::fromLLVMStructVal(&env, builder, value.val, elementType);

                env.printValue(builder, idx, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " storing element " + elementType.desc() + " at position: ");
                ft.print(builder);

                // must be a heap ptr, else invalid.
                auto tuple_to_store = ft.getLoad(builder);

                // // debug:
                // env.printValue(builder, idx, "storing tuple of type " + elementType.desc() + " at index: ");
                // env.printValue(builder, ft.getSize(builder), "tuple size is: ");
                // env.printValue(builder, heap_ptr, "pointer to store: ");

                // store pointer --> should be HEAP allocated pointer. (maybe add attributes to check?)
                auto target_idx = builder.CreateGEP(llvm_element_type, ptr_values, idx);

                // debug print:
                // env.printValue(builder, builder.CreateLoad(target_idx), "pointer currently stored: ");
                // {
                //     env.debugPrint(builder, "-- check of heap_ptr --");
                //     auto tuple_type = elementType;
                //     auto num_tuple_elements = tuple_type.parameters().size();
                //     llvm::Value* item = builder.CreateLoad(heap_ptr); // <-- this will be wrong. need to fix that in order to work.
                //     //item = heap_ptr; // <-- this should work (?)
                //
                //     auto item_type_name = env.getLLVMTypeName(item->getType());
                //     std::cout<<"[DEBUG] item type anme: "<<item_type_name<<std::endl;
                //     auto ft_check = FlattenedTuple::fromLLVMStructVal(&env, builder, item, tuple_type);
                //     for(unsigned i = 0; i < num_tuple_elements; ++i) {
                //         auto item_size = ft_check.getSize(i);
                //         auto element_type = tuple_type.parameters()[i];
                //         env.printValue(builder, item_size, "size of tuple element " + std::to_string(i) + " of type " + element_type.desc() + " is: ");
                //         env.printValue(builder, ft_check.get(i), "content of tuple element " + std::to_string(i) + " of type " + element_type.desc() + " is: ");
                //     }
                //     env.debugPrint(builder, "-- check end --");
                // }

                // store the heap ptr.
                builder.CreateStore(tuple_to_store, target_idx, true);

                // debug:
                // env.printValue(builder, builder.CreateLoad(target_idx), "pointer stored - post update: ");

            } else {
                throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " Unsupported list element type: " + list_type.desc());
            }

            // connect blocks + create storage for null
            if(elements_optional) {
                assert(bStoreDone);
                builder.CreateBr(bStoreDone);
                builder.SetInsertPoint(bStoreDone);

                // now load value to check it's ok
                auto check = list_load_value(env, builder, list_ptr, list_type, idx);
                env.printValue(builder, check.is_null, "CHECK: val isnull= ");
                env.printValue(builder, check.val, "CHECK: val value= ");
            }
        }


        void list_store_size(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* size) {
            assert(list_type.isListType());
            if(python::Type::EMPTYLIST == list_type)
                return; // nothing to do

            // check ptr has correct type
            auto llvm_list_type = env.createOrGetListType(list_type);
            if(list_ptr->getType() != llvm_list_type->getPointerTo())
                throw std::runtime_error("expected pointer of " + env.getLLVMTypeName(llvm_list_type->getPointerTo()) + " but list_ptr has " + env.getLLVMTypeName(list_ptr->getType()));

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();

            auto elements_optional = elementType.isOptionType();
            if(elements_optional)
                elementType = elementType.getReturnType();

            if(elementType.isSingleValued()) {
                if(elements_optional) {
                    // capacity is field 0...
                    auto idx_size = builder.CreateStructGEP(list_ptr, llvm_list_type, 1); assert(idx_size->getType() == env.i64ptrType());
                    builder.CreateStore(size, idx_size);
                } else {
                    // the list is represented as single i64
                    builder.CreateStore(size, list_ptr);
                }
            } else if(elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN) {
                auto idx_size = builder.CreateStructGEP(list_ptr, llvm_list_type, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(size, idx_size);
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {
                auto idx_size = builder.CreateStructGEP(list_ptr, llvm_list_type, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(size, idx_size);
            } else if(elementType.isStructuredDictionaryType() || elementType == python::Type::GENERICDICT) {
                auto idx_size = builder.CreateStructGEP(list_ptr, llvm_list_type, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(size, idx_size);
            } else if(elementType.isListType()) {
                auto idx_size = builder.CreateStructGEP(list_ptr, llvm_list_type, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(size, idx_size);
            } else if(elementType.isTupleType()) {
                auto idx_size = builder.CreateStructGEP(list_ptr, llvm_list_type, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(size, idx_size);
            } else {
                throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " Unsupported list element type: " + list_type.desc());
            }
        }

        llvm::Value* list_length(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type) {
            assert(list_type.isListType());
            if(python::Type::EMPTYLIST == list_type)
                return env.i64Const(0); // empty list is well,... guess (drum roll) -> empty.

            // check ptr has correct type
            auto llvm_list_type = env.createOrGetListType(list_type);
            auto original_list_type = list_type;
            // if(list_ptr->getType() != llvm_list_type->getPointerTo())
            //    throw std::runtime_error("expected pointer of " + env.getLLVMTypeName(llvm_list_type->getPointerTo()) + " but list_ptr has " + env.getLLVMTypeName(list_ptr->getType()));

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();

            auto elements_optional = elementType.isOptionType();
            if(elements_optional)
                elementType = elementType.getReturnType();

            // check that list type is supported
            if(!(elementType.isSingleValued()
            || elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN
                      || elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT
                      || elementType == python::Type::GENERICDICT
                      || elementType.isStructuredDictionaryType()
                      || elementType.isListType()
                      || elementType.isTupleType())) {
                throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " Unsupported list element type: " + list_type.desc());
            }

            // shorten the code below
            if(elementType.isSingleValued() && !elements_optional) {
                if(list_ptr->getType()->isPointerTy())
                    // the list is represented as single i64
                    return builder.CreateLoad(builder.getInt64Ty(), list_ptr);
                else {
                    assert(list_ptr->getType() == env.i64Type());
                    return list_ptr;
                }
            } else {
                auto size_position = 1;

                auto ans = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, size_position);
                assert(ans->getType() == env.i64Type());
                return ans;
            }
        }

        llvm::Value* list_of_structs_size(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type) {
            using namespace llvm;

            // should be same as in serialize_to!!!
            auto f_struct_element_size = [list_type](LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();

                assert(element_type.isStructuredDictionaryType());

                auto llvm_list_type = env.createOrGetListType(list_type);
                auto ptr_values = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, 2);

                std::cout<<"ptr_values type name: "<<env.getLLVMTypeName(ptr_values->getType())<<std::endl;
                auto llvm_list_element_type = env.pythonToLLVMType(list_type.elementType().withoutOption());
                std::cout<<"type name: "<<env.getLLVMTypeName(llvm_list_element_type)<<std::endl;
                std::cout<<"type name: "<<env.getLLVMTypeName(llvm_list_type->getStructElementType(2))<<std::endl;

                auto item_idx = builder.CreateGEP(llvm_list_element_type->getPointerTo(), ptr_values, index);
                auto item = builder.CreateLoad(llvm_list_element_type->getPointerTo(), item_idx);
                // old:
                //auto item = builder.CreateLoad(llvm_list_element_type, ptr_values, index);
                auto item_size = struct_dict_serialized_memory_size(env, builder, item, element_type).val;

                return item_size;
            };

            return list_of_varitems_serialized_size(env, builder, list_ptr, list_type, f_struct_element_size);
        }

        llvm::Value* list_of_lists_size(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type) {
            // new

            // define proper helper functions here
            auto list_get_list_item_size = [list_type](LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                assert(index && index->getType() == env.i64Type());

                env.printValue(builder, index, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " computing list " + list_type.desc() + " element size for idx=");
                auto element_type = list_type.elementType();

                assert(element_type.withoutOption().isListType());

                auto llvm_list_type = env.createOrGetListType(list_type);
                auto ptr_values = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, 2);
                assert(ptr_values->getType()->isPointerTy());

                // fetch size by calling struct_size on each retrieved pointer! (they should be ALL valid)
                // --> no check here!
                auto llvm_list_element_type = env.pythonToLLVMType(list_type.elementType().withoutOption());
                auto item_ptr = builder.CreateGEP(llvm_list_element_type->getPointerTo(), ptr_values, index);
                auto item = builder.CreateLoad(llvm_list_element_type->getPointerTo(), item_ptr);

                // blocks for option decode with if.
                llvm::BasicBlock* bBlockBeforeItemDecode = nullptr;
                llvm::BasicBlock* bBlockDecodeDone = nullptr;
                // special case: element_type is option type: check if NULL
                if(element_type.isOptionType()) {
                    auto struct_bitmap_index = llvm_list_type->getStructNumElements() - 1;
                    assert(struct_bitmap_index == 3);
                    auto nullmap_ptr = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, struct_bitmap_index);
                    assert(nullmap_ptr);

                    // in deserialize, best to store nullptr for this...
                    llvm::Value* is_null = builder.CreateLoad(builder.getInt8Ty(), builder.CreateGEP(builder.getInt8Ty(), nullmap_ptr, index));
                    is_null = builder.CreateICmpNE(is_null, env.i8Const(0));

                    auto bBlockDecode = llvm::BasicBlock::Create(builder.getContext(), "decode_list_item", builder.GetInsertBlock()->getParent());
                    bBlockDecodeDone = llvm::BasicBlock::Create(builder.getContext(), "decode_list_item_done", builder.GetInsertBlock()->getParent());
                    bBlockBeforeItemDecode = builder.GetInsertBlock();
                    builder.CreateCondBr(is_null, bBlockDecodeDone, bBlockDecode);
                    builder.SetInsertPoint(bBlockDecode);
                }


                auto item_length = list_length(env, builder, item, element_type.withoutOption());
                env.printValue(builder, item_length, "got list " + element_type.desc() + " with n elements: ");

                // call function! (or better said: emit the necessary code...)
                auto item_size = list_serialized_size(env, builder, item, element_type.withoutOption());

                if(element_type.isOptionType()) {
                    // end with phi block.
                    auto lastBlock = builder.GetInsertBlock();
                    builder.CreateBr(bBlockDecodeDone);

                    builder.SetInsertPoint(bBlockDecodeDone);
                    auto phi_size = builder.CreatePHI(builder.getInt64Ty(), 2);
                    phi_size->addIncoming(item_size, lastBlock);
                    phi_size->addIncoming(env.i64Const(0), bBlockBeforeItemDecode);
                    item_size = phi_size;
                }

                return item_size;
            };

            auto size_in_bytes = list_of_varitems_serialized_size(env, builder, list_ptr, list_type, list_get_list_item_size);
             env.printValue(builder, size_in_bytes, "total serialized size of list " + list_type.desc() + " is: ");
            return size_in_bytes;
        }

        FlattenedTuple get_tuple_item(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* index) {

            assert(index && index->getType() == env.i64Type());
            auto element_type = list_type.elementType();
            assert(element_type.isTupleType());


            // what type is there?
            unsigned ptr_position = 2;
            llvm::Value* ptr_values = nullptr;
            auto llvm_list_type = env.createOrGetListType(list_type);
            if(list_ptr->getType()->isPointerTy()) {
                auto idx_ptr = builder.CreateStructGEP(list_ptr, llvm_list_type, ptr_position);
                //assert(idx_size->getType() == env.i64ptrType());
                ptr_values = builder.CreateLoad(llvm_list_type->getStructElementType(ptr_position), idx_ptr);
            } else {
                ptr_values = builder.CreateExtractValue(list_ptr, std::vector<unsigned>(1, ptr_position));
            }

            //auto ptr_values = builder.CreateStructGEP(list_ptr, llvm_list_type, 2); // should be struct.tuple**

            auto t_ptr_values = env.getLLVMTypeName(ptr_values->getType());
            // now load the i-th element from ptr_values as struct.tuple*
            auto llvm_value_type = env.pythonToLLVMType(element_type);
            auto item_ptr = builder.CreateInBoundsGEP(ptr_values, llvm_value_type, index);
            auto t_item_ptr = env.getLLVMTypeName(item_ptr->getType()); // should be struct.tuple**

            auto item = builder.CreateLoad(llvm_value_type, item_ptr); // <-- should be struct.tuple*
            auto t_item = env.getLLVMTypeName(llvm_value_type);

            // debug print: retrieve heap ptr
            // env.printValue(builder, item, "stored heap ptr is: ");
            //
            // // printing the i64 from the pointer...
            // auto dbg_ptr = builder.CreatePointerCast(item, env.i8ptrType());
            // for(unsigned i = 0; i < 10; ++i) {
            //     auto i64_val = builder.CreateLoad(builder.CreatePointerCast(dbg_ptr, env.i64ptrType()));
            //     dbg_ptr = builder.CreateGEP(dbg_ptr, env.i64Const(sizeof(int64_t)));
            //     env.printValue(builder, i64_val, "bytes " + std::to_string(i * 8) + "-" + std::to_string((i+1)*8) + ": ");
            // }


            // call function! (or better said: emit the necessary code...)
            FlattenedTuple ft = FlattenedTuple::fromLLVMStructVal(&env, builder, item, element_type);
            return ft;
        }


        llvm::Value* list_of_tuples_size(LLVMEnvironment& env, const IRBuilder& builder,
                                         llvm::Value* list_ptr, const python::Type& list_type) {

            auto f_tuple_element_size = [list_type](LLVMEnvironment& env, const IRBuilder& builder,
                    llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                auto element_type = list_type.elementType();
                assert(element_type.isTupleType());

                if(python::Type::EMPTYTUPLE == element_type)
                    return env.i64Const(0);

                auto ft = get_tuple_item(env, builder, list_ptr, list_type, index);

                // get size
                auto item_size = ft.getSize(builder);
                return item_size;
            };

            auto l_size = list_of_varitems_serialized_size(env, builder, list_ptr, list_type, f_tuple_element_size);

            return l_size;
        }

        llvm::Value* list_of_generic_dicts_size(LLVMEnvironment& env, const IRBuilder& builder,
                                         llvm::Value* list_ptr, const python::Type& list_type) {

            auto f_generic_dict_element_size = [list_type](LLVMEnvironment& env, const IRBuilder& builder,
                                                    llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                auto llvm_list_type = env.createOrGetListType(list_type);
                auto ptr_values = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, 2);

                auto item_ptr = builder.CreateGEP(env.i8ptrType(), ptr_values, index);
                auto item = builder.CreateLoad(env.i8ptrType(), item_ptr);

                // call cjson serialize (if not NULL)
                auto val = call_cjson_to_string(builder, item);

                // if optional, load isnull from bitmap
                if(list_type.elementType().isOptionType()) {
                    auto struct_bitmap_index = llvm_list_type->getStructNumElements() - 1;
                    assert(struct_bitmap_index == 3);
                    auto nullmap_ptr = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, struct_bitmap_index);
                    assert(nullmap_ptr);

                    // in deserialize, best to store nullptr for this...
                    auto is_null = builder.CreateLoad(builder.getInt8Ty(), builder.CreateGEP(builder.getInt8Ty(), nullmap_ptr, index));
                    val.is_null = builder.CreateICmpNE(is_null, env.i8Const(0));

                    env.printValue(builder, val.is_null, "generic dict item, isnull: ");
                }

                // get size
                auto item_size = val.size;

                // set to 0 if null
                if(val.is_null)
                    item_size = builder.CreateSelect(val.is_null, env.i64Const(0), item_size);

                env.printValue(builder, index, "generic dict item size for i=");
                env.printValue(builder, item_size, "generic dict item size is");

                return item_size;
            };

            auto l_size = list_of_varitems_serialized_size(env, builder, list_ptr, list_type, f_generic_dict_element_size);

            return l_size;
        }

        static llvm::Value* list_serialize_fixed_sized_to(LLVMEnvironment& env, const IRBuilder& builder,
                                                          llvm::Value* list_ptr,
                                                          const python::Type& list_type,
                                                          llvm::Value* dest_ptr) {
            assert(list_type.isListType());
            auto elementType = list_type.elementType();
            assert(elementType.withoutOption() == python::Type::I64
                   || elementType.withoutOption() == python::Type::F64
                   || elementType.withoutOption() == python::Type::BOOLEAN);

            auto llvm_list_type = env.createOrGetListType(list_type);

            // note that size if basically 8 bytes for size + 8 * len
            // need to write in a loop -> yet can speed it up using memcpy!

            // it's the size field + the size * sizeof(int64_t)
            auto len = list_length(env, builder, list_ptr, list_type);
            auto casted_dest_ptr = builder.CreateBitOrPointerCast(dest_ptr, env.i64ptrType());
            builder.CreateStore(len, casted_dest_ptr);
            dest_ptr = builder.MovePtrByBytes(dest_ptr, sizeof(int64_t));

            // env.debugPrint(builder, "serializing list of size: ", len);

            // memcpy data_ptr
            auto ptr_values = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, 2);
            ptr_values = builder.CreatePointerCast(ptr_values, env.i8ptrType());
            auto data_size = builder.CreateMul(env.i64Const(8), len); // size in bytes!
            builder.CreateMemCpy(dest_ptr, 0, ptr_values, 0, data_size);
            auto size = builder.CreateAdd(env.i64Const(8), data_size);
            // env.debugPrint(builder, "total serialized list size are bytes: ", size);
            return size;
        }

        llvm::Value* list_of_structs_serialize_to(LLVMEnvironment& env, const IRBuilder& builder,
                                                  llvm::Value* list_ptr, const python::Type& list_type,
                                                  llvm::Value* dest_ptr) {
            // quite complex, basically write like strings/pyobjects incl. offset array!

            // -> there may be structs that are of fixed size. Could write them optimized. for now, write as var always...
            auto f_struct_element_size = [list_type](LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();

                assert(element_type.isStructuredDictionaryType());

                auto llvm_list_type = env.createOrGetListType(list_type);
                auto ptr_values = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, 2);

                auto llvm_list_element_type = env.pythonToLLVMType(element_type.withoutOption());

                // old:
                //auto item = builder.CreateGEP(llvm_list_element_type, ptr_values, index);

                // new:
                auto item_idx = builder.CreateGEP(llvm_list_element_type->getPointerTo(), ptr_values, index);
                auto item = builder.CreateLoad(llvm_list_element_type->getPointerTo(), item_idx);

                auto item_size = struct_dict_serialized_memory_size(env, builder, item, element_type).val;

                // env.printValue(builder, item_size, "list of element (" + element_type.desc() + ") to serialize is: ");

                return item_size;
            };

            auto f_struct_element_serialize_to = [list_type](LLVMEnvironment& env, const IRBuilder& builder,
                                                            llvm::Value* list_ptr, llvm::Value* index, llvm::Value* dest_ptr) -> llvm::Value* {

                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();

                assert(element_type.isStructuredDictionaryType());

                auto llvm_list_type = env.createOrGetListType(list_type);

                env.printValue(builder, index, "struct element serialize of type " + element_type.desc());

                auto ptr_values = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, 2);

                auto llvm_list_element_type = env.pythonToLLVMType(element_type.withoutOption());

                // old:
                // auto item = builder.CreateGEP(llvm_list_element_type, ptr_values, index);

                // new:
                auto item_idx = builder.CreateGEP(llvm_list_element_type->getPointerTo(), ptr_values, index);
                auto item = builder.CreateLoad(llvm_list_element_type->getPointerTo(), item_idx);

                // call struct dict serialize
                auto s_result = struct_dict_serialize_to_memory(env, builder, item, element_type, dest_ptr);
                return s_result.val;
            };


            return list_serialize_varitems_to(env, builder, list_ptr, list_type,
                                              dest_ptr, f_struct_element_size, f_struct_element_serialize_to);
        }

        llvm::Value* list_of_tuples_serialize_to(LLVMEnvironment& env,
                                                 const IRBuilder& builder,
                                                 llvm::Value* list_ptr,
                                                 const python::Type& list_type,
                                                 llvm::Value* dest_ptr) {
            // quite complex, basically write like strings/pyobjects incl. offset array!

            // -> there may be structs that are of fixed size. Could write them optimized.
            // for now, write as var always...
            // -> use same Lambda func for size helper func...
            auto f_tuple_element_size = [list_type](LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();
                assert(element_type.isTupleType());

                if(python::Type::EMPTYTUPLE == element_type)
                    return env.i64Const(0);

                auto ft = get_tuple_item(env, builder, list_ptr, list_type, index);

                // get size
                auto item_size = ft.getSize(builder);
                return item_size;
            };

            auto f_tuple_element_serialize_to = [list_type](LLVMEnvironment& env, const IRBuilder& builder,
                                                             llvm::Value* list_ptr, llvm::Value* index, llvm::Value* dest_ptr) -> llvm::Value* {

                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();

                if(python::Type::EMPTYTUPLE == element_type)
                    return env.i64Const(0);

                auto ft = get_tuple_item(env, builder, list_ptr, list_type, index);

                env.printValue(builder, index, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " storing element " + element_type.desc() + " at position: ");
                ft.print(builder);
                auto s_size = ft.serialize(builder, dest_ptr);
                env.printValue(builder, s_size, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " serialized size of " + element_type.desc() + " is: ");
                return s_size;
            };

            // debug print
            // env.debugPrint(builder, "---\nstarting list serialize\n---");

            return list_serialize_varitems_to(env, builder, list_ptr, list_type,
                                              dest_ptr, f_tuple_element_size, f_tuple_element_serialize_to);
        }

        llvm::Value* list_of_generic_dicts_serialize_to(LLVMEnvironment& env,
                                                 const IRBuilder& builder,
                                                 llvm::Value* list_ptr,
                                                 const python::Type& list_type,
                                                 llvm::Value* dest_ptr) {

            // TODO: could call cJSON only once...
            auto f_generic_dict_element_size = [list_type](LLVMEnvironment& env, const IRBuilder& builder,
                                                           llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                auto llvm_list_type = env.createOrGetListType(list_type);
                auto ptr_values = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, 2);

                auto item_ptr = builder.CreateGEP(env.i8ptrType(), ptr_values, index);
                auto item = builder.CreateLoad(env.i8ptrType(), item_ptr);

                // call cjson serialize (if not NULL)
                auto val = call_cjson_to_string(builder, item);

                // if optional, load isnull from bitmap
                if(list_type.elementType().isOptionType()) {
                    auto struct_bitmap_index = llvm_list_type->getStructNumElements() - 1;
                    assert(struct_bitmap_index == 3);
                    auto nullmap_ptr = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, struct_bitmap_index);
                    assert(nullmap_ptr);

                    // in deserialize, best to store nullptr for this...
                    auto is_null = builder.CreateLoad(builder.getInt8Ty(), builder.CreateGEP(builder.getInt8Ty(), nullmap_ptr, index));
                    val.is_null = builder.CreateICmpNE(is_null, env.i8Const(0));

                    env.printValue(builder, val.is_null, "generic dict item, isnull: ");
                }

                // get size
                auto item_size = val.size;

                // set to 0 if null
                if(val.is_null)
                    item_size = builder.CreateSelect(val.is_null, env.i64Const(0), item_size);

                env.printValue(builder, index, "generic dict item size for i=");
                env.printValue(builder, item_size, "generic dict item size is");

                return item_size;
            };

            auto f_generic_dict_element_serialize_to = [list_type](LLVMEnvironment& env, const IRBuilder& builder,
                                                            llvm::Value* list_ptr, llvm::Value* index, llvm::Value* dest_ptr) -> llvm::Value* {

                assert(index && index->getType() == env.i64Type());

                auto llvm_list_type = env.createOrGetListType(list_type);
                auto ptr_values = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, 2);

                auto item_ptr = builder.CreateGEP(env.i8ptrType(), ptr_values, index);
                auto item = builder.CreateLoad(env.i8ptrType(), item_ptr);

                // call cjson serialize
                auto val = call_cjson_to_string(builder, item);

                if(list_type.elementType().isOptionType()) {
                    auto struct_bitmap_index = llvm_list_type->getStructNumElements() - 1;
                    assert(struct_bitmap_index == 3);
                    auto nullmap_ptr = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, struct_bitmap_index);
                    assert(nullmap_ptr);

                    // in deserialize, best to store nullptr for this...
                    auto is_null = builder.CreateLoad(builder.getInt8Ty(), builder.CreateGEP(builder.getInt8Ty(), nullmap_ptr, index));
                    val.is_null = builder.CreateICmpNE(is_null, env.i8Const(0));

                    // update both val and size accordingly
                    std::string null_default_dict("{}");
                    assert(null_default_dict.size() + 1 == 3);
                    val.val = builder.CreateSelect(val.is_null, env.strConst(builder,  null_default_dict), val.val);
                    val.size = builder.CreateSelect(val.is_null, env.i64Const(null_default_dict.size() + 1), val.size);
                }

                env.printValue(builder, index, "serializing list element no: ");
                env.printValue(builder, val.val, "value to serialize is: ");
                env.printValue(builder, val.size, "size of value to serialize is: ");

                builder.CreateMemCpy(dest_ptr, 0, val.val, 0, val.size);
                auto s_size = val.size;
                return s_size;
            };

            return list_serialize_varitems_to(env, builder, list_ptr, list_type,
                                              dest_ptr, f_generic_dict_element_size, f_generic_dict_element_serialize_to);
        }

        // generic list of variable fields serialization function
        llvm::Value* list_serialize_varitems_to(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr,
                                               const python::Type& list_type, llvm::Value* dest_ptr,
                                                std::function<llvm::Value*(LLVMEnvironment&, const IRBuilder&, llvm::Value*, llvm::Value*)> f_item_size,
                                                std::function<llvm::Value*(LLVMEnvironment&, const IRBuilder&, llvm::Value*, llvm::Value*, llvm::Value*)> f_item_serialize_to) {
            using namespace llvm;

            assert(dest_ptr && dest_ptr->getType() == env.i8ptrType());

            // serialization format of this is as follows:

            // -> (offset|size) is packed
            // len | (offset|size) | .... | (offset|size) | item1 | .... |item_len

            // fetch length of list
            auto len = list_length(env, builder, list_ptr, list_type);

            // debug print:
            // env.printValue(builder, len, "serializing var item list of length: ");

            // write len of list to dest_ptr
            auto ptr = dest_ptr;
            builder.CreateStore(len, builder.CreatePointerCast(ptr, env.i64ptrType()));
            // env.debugPrint(builder, "stored length to pointer address");
            ptr = builder.MovePtrByBytes(ptr, sizeof(int64_t));

            auto info_start_ptr = ptr;
            auto var_ptr = builder.MovePtrByBytes(ptr, builder.CreateMul(len, env.i64Const(sizeof(int64_t)))); // offset from current is len * sizeof(int64_t)

            // generate loop to go over items.
            auto loop_i = env.CreateFirstBlockAlloca(builder, env.i64Type());
            builder.CreateStore(env.i64Const(0), loop_i);

            // start loop going over the size entries (--> this could be vectorized!)
            auto& ctx = env.getContext(); auto F = builder.GetInsertBlock()->getParent();
            BasicBlock *bLoopHeader = BasicBlock::Create(ctx, "list_of_var_items_serialize_loop_header", F);
            BasicBlock *bLoopBody = BasicBlock::Create(ctx, "list_of_var_items_serialize_loop_body", F);
            BasicBlock *bLoopExit = BasicBlock::Create(ctx, "list_of_var_items_serialize_loop_done", F);


            // env.printValue(builder, len, "serializing list of var items (" + list_type.desc() +") to memory of length=");

            auto varlen_bytes_var = env.CreateFirstBlockAlloca(builder, env.i64Type());
            builder.CreateStore(env.i64Const(0), varlen_bytes_var);
            builder.CreateBr(bLoopHeader);

            {
                // --- header ---
                builder.SetInsertPoint(bLoopHeader);
                // if i < len:
                auto loop_i_val = builder.CreateLoad(env.i64Type(), loop_i);
                auto loop_cond = builder.CreateICmpULT(loop_i_val, len);
                builder.CreateCondBr(loop_cond, bLoopBody, bLoopExit);
            }


            {
                // --- body ---
                builder.SetInsertPoint(bLoopBody);
                auto loop_i_val = builder.CreateLoad(env.i64Type(), loop_i);

                // env.printValue(builder, loop_i_val, "serializing item no: ");

                // get item's serialized size
                auto item_size = f_item_size(env, builder, list_ptr, loop_i_val);

                // env.printValue(builder, item_size, "item size to serialize is: ");

                auto varlen_bytes = builder.CreateLoad(builder.getInt64Ty(), varlen_bytes_var);
                assert(varlen_bytes->getType() == env.i64Type());
                // env.printValue(builder, varlen_bytes, "so far serialized (bytes): ");
                auto item_dest_ptr = builder.MovePtrByBytes(var_ptr, varlen_bytes);
                // env.debugPrint(builder, "calling item func");
                auto actual_size = f_item_serialize_to(env, builder, list_ptr, loop_i_val, item_dest_ptr);
                // env.printValue(builder, actual_size, "actually realized item size is: ");
                // env.debugPrint(builder, "item func called.");
                // offset is (numSerialized - serialized_idx) * sizeof(int64_t) + varsofar.
                // i.e. here (len - loop_i_val) * 8 + var
                auto offset = builder.CreateAdd(varlen_bytes, builder.CreateMul(env.i64Const(8), builder.CreateSub(len, loop_i_val)));

                // env.printValue(builder, offset, "field offset:  ");

                // save offset and item size.
                // where to write this? -> current
                auto info = pack_offset_and_size(builder, offset, item_size);

                // store info & inc pointer
                auto info_ptr = builder.MovePtrByBytes(info_start_ptr, builder.CreateMul(env.i64Const(8), loop_i_val));
                builder.CreateStore(info, builder.CreatePointerCast(info_ptr, env.i64ptrType()));

                // inc. variable length bytes serialized so far
                builder.CreateStore(builder.CreateAdd(varlen_bytes, item_size), varlen_bytes_var);

                // inc. loop counter
                builder.CreateStore(builder.CreateAdd(env.i64Const(1), loop_i_val), loop_i);
                builder.CreateBr(bLoopHeader);
            }

            builder.SetInsertPoint(bLoopExit);

            // calculate actual size
            auto varlen_bytes = builder.CreateLoad(builder.getInt64Ty(), varlen_bytes_var);

            auto size = builder.CreateAdd(env.i64Const(8), builder.CreateAdd(builder.CreateMul(env.i64Const(8), len), varlen_bytes));
            assert(size->getType() == env.i64Type());

            // env.printValue(builder, size, "total list size serialized: ");

            return size;
        }

        // generic list of variable fields serialization function
        llvm::Value* list_of_varitems_serialized_size(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr,
                                                const python::Type& list_type,
                                                std::function<llvm::Value*(LLVMEnvironment&, const IRBuilder&, llvm::Value*, llvm::Value*)> f_item_size) {
            using namespace llvm;

            // serialization format of this is as follows:

            // -> (offset|size) is packed
            // len | (offset|size) | .... | (offset|size) | item1 | .... |item_len

            // fetch length of list
            auto len = list_length(env, builder, list_ptr, list_type);

            env.printValue(builder, len, "---\ncomputing serialized size of list " + list_type.desc() + " with #elements = ");

            llvm::Value* opt_size = env.i64Const(0);
            if(list_type.elementType().isOptionType()) {
                opt_size = calc_bitmap_size_in_64bit_blocks(builder, len);

                env.printValue(builder, opt_size, "opt_size is: ");
            }

            // generate loop to go over items.
            auto loop_i = env.CreateFirstBlockAlloca(builder, env.i64Type());
            builder.CreateStore(env.i64Const(0), loop_i);

            // start loop going over the size entries (--> this could be vectorized!)
            auto& ctx = env.getContext(); auto F = builder.GetInsertBlock()->getParent();
            BasicBlock *bLoopHeader = BasicBlock::Create(ctx, "list_of_var_items_size_loop_header", F);
            BasicBlock *bLoopBody = BasicBlock::Create(ctx, "list_of_var_items_size_loop_body", F);
            BasicBlock *bLoopExit = BasicBlock::Create(ctx, "list_of_var_items_size_loop_done", F);

            auto varlen_bytes_var = env.CreateFirstBlockAlloca(builder, env.i64Type());
            builder.CreateStore(env.i64Const(0), varlen_bytes_var);
            builder.CreateBr(bLoopHeader);

            {
                // --- header ---
                builder.SetInsertPoint(bLoopHeader);
                // if i < len:
                auto loop_i_val = builder.CreateLoad(env.i64Type(), loop_i);
                auto loop_cond = builder.CreateICmpULT(loop_i_val, len);
                builder.CreateCondBr(loop_cond, bLoopBody, bLoopExit);
            }


            {
                // --- body ---
                builder.SetInsertPoint(bLoopBody);
                auto loop_i_val = builder.CreateLoad(env.i64Type(), loop_i);

                 env.printValue(builder, loop_i_val, "Computing size of element " + list_type.elementType().desc() + " of list " + list_type.desc() + " for i=");


                // get item's serialized size
                auto item_size = f_item_size(env, builder, list_ptr, loop_i_val);
                auto varlen_bytes = builder.CreateLoad(builder.getInt64Ty(), varlen_bytes_var);

                 env.printValue(builder, item_size, "-> item size is: ");

                // inc. variable length bytes serialized so far
                builder.CreateStore(builder.CreateAdd(varlen_bytes, item_size), varlen_bytes_var);
                 env.printValue(builder, builder.CreateLoad(builder.getInt64Ty(), varlen_bytes_var), "cum bytes so far: ");


                // inc. loop counter
                builder.CreateStore(builder.CreateAdd(env.i64Const(1), loop_i_val), loop_i);
                builder.CreateBr(bLoopHeader);
            }

            builder.SetInsertPoint(bLoopExit);

            // calculate actual size
            auto varlen_bytes = builder.CreateLoad(builder.getInt64Ty(), varlen_bytes_var);

            // debug print factors:
            env.printValue(builder, env.i8Const(8), "8 bytes for length");
            env.printValue(builder, builder.CreateMul(env.i64Const(8), len), "bytes for offsets/sizes of individual elements: ");
            env.printValue(builder, varlen_bytes, "bytes for varlength data: ");
            env.printValue(builder, opt_size, "opt size bytes: ");

            auto size = builder.CreateAdd(env.i64Const(8), builder.CreateAdd(builder.CreateMul(env.i64Const(8), len), varlen_bytes));
            size = builder.CreateAdd(size, opt_size);
            assert(size->getType() == env.i64Type());
             env.printValue(builder, size, "===\ncomputed serialized size of list " + list_type.desc() + " as: ");

            return size;
        }


        llvm::Value* list_of_lists_serialize_to(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr,
                                                const python::Type& list_type, llvm::Value* dest_ptr) {

            // define proper helper functions here
            // @TODO: refactor s.t. this here is the same as the in the list size function.
            auto list_get_list_item_size = [list_type](LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                assert(index && index->getType() == env.i64Type());

                env.printValue(builder, index, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " computing list " + list_type.desc() + " element size for idx=");
                auto element_type = list_type.elementType();

                assert(element_type.withoutOption().isListType());

                auto llvm_list_type = env.createOrGetListType(list_type);
                auto ptr_values = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, 2);
                assert(ptr_values->getType()->isPointerTy());

                env.printValue(builder, ptr_values, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " loaded ptr_values");

                // fetch size by calling struct_size on each retrieved pointer! (they should be ALL valid)
                // --> no check here!
                auto llvm_list_element_type = env.pythonToLLVMType(list_type.elementType().withoutOption());

                // blocks for option decode with if.
                llvm::BasicBlock* bBlockBeforeItemDecode = nullptr;
                llvm::BasicBlock* bBlockDecodeDone = nullptr;
                // special case: element_type is option type: check if NULL
                if(element_type.isOptionType()) {
                    auto struct_bitmap_index = llvm_list_type->getStructNumElements() - 1;
                    assert(struct_bitmap_index == 3);
                    auto nullmap_ptr = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, struct_bitmap_index);
                    assert(nullmap_ptr);

                    // in deserialize, best to store nullptr for this...
                    llvm::Value* is_null = builder.CreateLoad(builder.getInt8Ty(), builder.CreateGEP(builder.getInt8Ty(), nullmap_ptr, index));
                    is_null = builder.CreateICmpNE(is_null, env.i8Const(0));

                    auto bBlockDecode = llvm::BasicBlock::Create(builder.getContext(), "decode_list_item", builder.GetInsertBlock()->getParent());
                    bBlockDecodeDone = llvm::BasicBlock::Create(builder.getContext(), "decode_list_item_done", builder.GetInsertBlock()->getParent());
                    bBlockBeforeItemDecode = builder.GetInsertBlock();

                    env.printValue(builder, index, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " is element null for idx=");

                    builder.CreateCondBr(is_null, bBlockDecodeDone, bBlockDecode);
                    builder.SetInsertPoint(bBlockDecode);
                }

                env.printValue(builder, index, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " actually loading element for idx=");

                auto item_ptr = builder.CreateGEP(llvm_list_element_type->getPointerTo(), ptr_values, index);
                auto item = builder.CreateLoad(llvm_list_element_type->getPointerTo(), item_ptr);

                auto item_length = list_length(env, builder, item, element_type.withoutOption());
                env.printValue(builder, item_length, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " got list " + element_type.desc() + " with n elements: ");

                // call function! (or better said: emit the necessary code...)
                auto item_size = list_serialized_size(env, builder, item, element_type.withoutOption());

                if(element_type.isOptionType()) {
                    // end with phi block.
                    auto lastBlock = builder.GetInsertBlock();
                    builder.CreateBr(bBlockDecodeDone);

                    builder.SetInsertPoint(bBlockDecodeDone);
                    auto phi_size = builder.CreatePHI(builder.getInt64Ty(), 2);
                    phi_size->addIncoming(item_size, lastBlock);
                    phi_size->addIncoming(env.i64Const(0), bBlockBeforeItemDecode);
                    item_size = phi_size;
                }

                return item_size;
            };

            auto list_serialize_list_item = [list_type](LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, llvm::Value* index, llvm::Value* dest_ptr) -> llvm::Value* {
                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();

                assert(element_type.withoutOption().isListType());
                auto llvm_list_type = env.createOrGetListType(list_type);
                auto ptr_values = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, 2);
                assert(ptr_values->getType()->isPointerTy());

                // blocks for option decode with if. Do not serialize/access list if NULL.
                llvm::BasicBlock* bBlockBeforeItemDecode = nullptr;
                llvm::BasicBlock* bBlockDecodeDone = nullptr;
                // special case: element_type is option type: check if NULL
                if(element_type.isOptionType()) {
                    auto struct_bitmap_index = llvm_list_type->getStructNumElements() - 1;
                    assert(struct_bitmap_index == 3);
                    auto nullmap_ptr = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, struct_bitmap_index);
                    assert(nullmap_ptr);

                    // in deserialize, best to store nullptr for this...
                    llvm::Value* is_null = builder.CreateLoad(builder.getInt8Ty(), builder.CreateGEP(builder.getInt8Ty(), nullmap_ptr, index));
                    is_null = builder.CreateICmpNE(is_null, env.i8Const(0));

                    auto bBlockDecode = llvm::BasicBlock::Create(builder.getContext(), "decode_list_item", builder.GetInsertBlock()->getParent());
                    bBlockDecodeDone = llvm::BasicBlock::Create(builder.getContext(), "decode_list_item_done", builder.GetInsertBlock()->getParent());
                    bBlockBeforeItemDecode = builder.GetInsertBlock();

                    env.printValue(builder, index, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " is element null for idx=");

                    builder.CreateCondBr(is_null, bBlockDecodeDone, bBlockDecode);
                    builder.SetInsertPoint(bBlockDecode);
                }


                // fetch size by calling struct_size on each retrieved pointer! (they should be ALL valid)
                // --> no check here!
                auto llvm_list_element_type = env.pythonToLLVMType(element_type.withoutOption());
                auto item_ptr = builder.CreateGEP(llvm_list_element_type->getPointerTo(), ptr_values, index);
                auto item = builder.CreateLoad(llvm_list_element_type->getPointerTo(), item_ptr);

                env.printValue(builder, index, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " serializing element for idx=");

                // call function! (or better said: emit the necessary code...)
                auto item_size = list_serialize_to(env, builder, item, element_type.withoutOption(), dest_ptr);


                if(element_type.isOptionType()) {
                    // end with phi block.
                    auto lastBlock = builder.GetInsertBlock();
                    builder.CreateBr(bBlockDecodeDone);

                    builder.SetInsertPoint(bBlockDecodeDone);
                    auto phi_size = builder.CreatePHI(builder.getInt64Ty(), 2);
                    phi_size->addIncoming(item_size, lastBlock);
                    phi_size->addIncoming(env.i64Const(0), bBlockBeforeItemDecode);
                    item_size = phi_size;
                }

                return item_size;
            };

            llvm::Value* l_size = list_serialize_varitems_to(env, builder, list_ptr, list_type, dest_ptr, list_get_list_item_size, list_serialize_list_item);
            return l_size;
        }

        llvm::Value* list_serialized_size_str_like(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr,
        const python::Type& list_type) {

            // optional?
            assert(list_type.isListType());
            auto element_type = list_type.elementType();
            assert(element_type.withoutOption() == python::Type::STRING || element_type.withoutOption() == python::Type::PYOBJECT);

            auto list_get_str_like_item_size = [list_type](LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                assert(index && index->getType() == env.i64Type());

                auto llvm_list_type = env.createOrGetListType(list_type);

                auto ptr_sizes = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, 3);
                auto idx_size = builder.CreateGEP(env.i64Type(), ptr_sizes, index);
                auto item_size = builder.CreateLoad(env.i64Type(), idx_size);

                // env.printValue(builder, index, "computed size of str in " + list_type.desc() + " at position: ");
                // env.printValue(builder, item_size, "size of str in " + list_type.desc() + " is: ");

                return item_size;
            };

            auto ans = list_of_varitems_serialized_size(env, builder, list_ptr, list_type, list_get_str_like_item_size);
            return ans;
        }

        llvm::Value* list_serialize_str_like_to(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr,
                                                const python::Type& list_type, llvm::Value* dest_ptr) {
            auto list_get_str_like_item_size = [list_type](LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                assert(index && index->getType() == env.i64Type());
                auto llvm_list_type = env.createOrGetListType(list_type);

                auto ptr_sizes = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, 3);
                auto idx_size = builder.CreateGEP(env.i64Type(), ptr_sizes, index);
                auto item_size = builder.CreateLoad(env.i64Type(), idx_size);

                return item_size;
            };

            auto list_serialize_str_like_item = [list_type](LLVMEnvironment& env, const IRBuilder& builder,
                    llvm::Value* list_ptr, llvm::Value* index, llvm::Value* dest_ptr) -> llvm::Value* {

                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();

                assert(element_type.withoutOption() == python::Type::STRING || element_type.withoutOption() == python::Type::PYOBJECT);

                auto llvm_list_type = env.createOrGetListType(list_type);
                auto ptr_values = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, 2);
                auto ptr_sizes = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, 3);

                assert(ptr_values->getType() == env.i8ptrType()->getPointerTo(0));
                assert(ptr_sizes->getType() == env.i64ptrType());

                auto llvm_value_type = env.pythonToLLVMType(element_type.withoutOption());
                auto idx_value = builder.CreateGEP(llvm_value_type, ptr_values, index);
                auto idx_size = builder.CreateGEP(env.i64Type(), ptr_sizes, index);

                auto item_size = builder.CreateLoad(env.i64Type(), idx_size);
                auto str_ptr = builder.CreateLoad(llvm_value_type, idx_value);
                assert(item_size->getType() == env.i64Type());
                assert(str_ptr->getType() == env.i8ptrType());

                // env.printValue(builder, str_ptr, "serializing str: ");
                // env.printValue(builder, item_size, "str size is: ");

                // memcpy contents
                builder.CreateMemCpy(dest_ptr, 0, str_ptr, 0, item_size);

                return item_size;
            };

            llvm::Value* l_size = list_serialize_varitems_to(env, builder, list_ptr, list_type, dest_ptr,
                                                             list_get_str_like_item_size, list_serialize_str_like_item);
            return l_size;
        }

        llvm::Value* list_serialize_to(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, python::Type list_type, llvm::Value* dest_ptr) {

            using namespace llvm;
            using namespace std;

            assert(list_ptr);
            assert(list_type.isListType());

            assert(dest_ptr && dest_ptr->getType() == env.i8ptrType());

            env.debugPrint(builder, "serializing list " + list_type.desc() + " to memory");

            if(python::Type::EMPTYLIST == list_type)
                return env.i64Const(0); // nothing to do

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();
            auto original_list_type = list_type;

            bool has_optional_elements = elementType.isOptionType();
            llvm::Value* num_elements = nullptr, *bitmap_size_in_bytes=nullptr, *bitmap_dest_ptr=nullptr;
            if(has_optional_elements) {
                num_elements = list_length(env, builder, list_ptr, list_type);
                bitmap_size_in_bytes = calc_bitmap_size_in_64bit_blocks(builder, num_elements);

                // save now to pointer num_elements, and move with bitmap size.
                // i.e. first 8 bytes will be num elements, then comes bitmap, then data.
                // however, do here the trick of first serializing the data which overwrites the last 8 bytes.
                // bitmap at end overwrites this!
                builder.CreateStore(num_elements, builder.CreatePointerCast(dest_ptr, env.i64ptrType()));
                bitmap_dest_ptr = builder.MovePtrByBytes(dest_ptr, sizeof(int64_t));
                dest_ptr = builder.MovePtrByBytes(dest_ptr, bitmap_size_in_bytes);

                elementType = elementType.withoutOption();
                list_type = python::Type::makeListType(elementType);
            }

            llvm::Value* size_in_bytes = nullptr;
            if(elementType.isSingleValued()) {
                // store length of list to dest_ptr!
                auto len = list_length(env, builder, list_ptr, original_list_type);
                auto casted_dest_ptr = builder.CreateBitOrPointerCast(dest_ptr, env.i64ptrType());
                builder.CreateStore(len, casted_dest_ptr);
                size_in_bytes = env.i64Const(8);
            } else if(elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN) {
                size_in_bytes = list_serialize_fixed_sized_to(env, builder, list_ptr, original_list_type, dest_ptr);
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {
                size_in_bytes = list_serialize_str_like_to(env, builder, list_ptr, original_list_type, dest_ptr);
            } else if(elementType.isStructuredDictionaryType()) {
                size_in_bytes = list_of_structs_serialize_to(env, builder, list_ptr, list_type, dest_ptr);
            } else if(elementType.isTupleType()) {
                size_in_bytes = list_of_tuples_serialize_to(env, builder, list_ptr, list_type, dest_ptr);
            } else if(elementType.isListType()) {
                size_in_bytes = list_of_lists_serialize_to(env, builder, list_ptr, original_list_type, dest_ptr);
            } else if(elementType == python::Type::GENERICDICT) {
                size_in_bytes = list_of_generic_dicts_serialize_to(env, builder, list_ptr, original_list_type, dest_ptr);
            } else {
                throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " Unsupported list to serialize: " + original_list_type.desc());
            }

            // Now time to serialize bitmap
            // => note that bitmap is saved in memory as pointer
            if(has_optional_elements) {
                list_type = python::Type::makeListType(python::Type::makeOptionType(elementType));
                list_serialize_bitmap_to(env, builder, list_ptr, list_type, bitmap_dest_ptr);
                size_in_bytes = builder.CreateAdd(size_in_bytes, bitmap_size_in_bytes);
            }

            return size_in_bytes;
        }

        llvm::Value* list_serialized_size(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, python::Type list_type) {
            using namespace llvm;
            using namespace std;

            assert(list_ptr);

            assert(list_type.isListType());
            if(python::Type::EMPTYLIST == list_type)
                return env.i64Const(0); // nothing to do

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();

            auto elements_optional = elementType.isOptionType();
            if(elements_optional)
                elementType = elementType.getReturnType();

            python::Type original_list_type = list_type;

            // optional? => add size!
            llvm::Value* opt_size = env.i64Const(0);
            if(elements_optional) {
                auto len = list_length(env, builder, list_ptr, list_type);
                opt_size = calc_bitmap_size_in_64bit_blocks(builder, len);
                list_type = python::Type::makeListType(elementType);
            }

            if(elementType.isSingleValued()) {
                if(elements_optional) {
                    return builder.CreateAdd(env.i64Const(8), opt_size);
                } else {
                    // nothing gets stored, ignore.
                    return env.i64Const(8); // just store the size field.
                }
            } else if(elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN) {
                // it's the size field + the size * sizeof(int64_t) + opt_size
                auto len = list_length(env, builder, list_ptr, original_list_type);
                llvm::Value* l_size = builder.CreateAdd(env.i64Const(8), builder.CreateMul(env.i64Const(8), len));
                if(elements_optional)
                    l_size = builder.CreateAdd(l_size, opt_size);
                env.printValue(builder, l_size, "serialized size of " + list_type.desc() + " is: ");

                return l_size;
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {
                auto size_in_bytes = list_serialized_size_str_like(env, builder, list_ptr, original_list_type);
                return size_in_bytes;
            } else if(elementType.isStructuredDictionaryType()) {
                // pointer to the structured dict type!
                // this is quite involved, therefore put into its own function. basically iterate over elements and then query their size!
                llvm::Value* l_size =  list_of_structs_size(env, builder, list_ptr, original_list_type);
                return l_size;
            } else if(elementType.isListType()) {
                llvm::Value* l_size = list_of_lists_size(env, builder, list_ptr, original_list_type);
                return l_size;
            } else if(elementType.isTupleType()) {
                llvm::Value* l_size = list_of_tuples_size(env, builder, list_ptr, original_list_type);
                return l_size;
            } else if(elementType == python::Type::GENERICDICT) {
                llvm::Value* l_size = list_of_generic_dicts_size(env, builder, list_ptr, original_list_type);

                env.printValue(builder, l_size, "generic dict / serialized list size (for test case should be 52): ");

                return l_size;
            } else {
                throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " Unsupported list element type: " + original_list_type.desc());
            }
        }

        static llvm::Value* list_deserialize_list_of_struct_dicts_from_memory(LLVMEnvironment& env,
                                                                              const codegen::IRBuilder& builder,
                                                                              llvm::Value* ptr,
                                                                              const python::Type& list_type,
                                                                              llvm::Value* num_elements,
                                                                              llvm::Value* list_ptr) {
            using namespace llvm;

            // len | (offset|size) | .... | (offset|size) | item1 | .... |item_len

            assert(list_type.isListType());
            auto element_type = list_type.elementType();
            assert(element_type.isStructuredDictionaryType());

            // this var can be eliminated --> put this logic into func?
            auto acc_size_var = env.CreateFirstBlockAlloca(builder, builder.getInt64Ty());
            builder.CreateStore(env.i64Const(0), acc_size_var);

            auto llvm_list_type = env.createOrGetListType(list_type);
            auto llvm_list_element_type = env.getOrCreateStructuredDictType(element_type);
            auto offset_ptr = builder.CreateBitCast(ptr, env.i64ptrType()); // get pointer to i64 serialized array of offsets
            auto& context = env.getContext();

            // need to point to each of the strings and calculate lengths
            llvm::Function *func = builder.GetInsertBlock()->getParent();
            assert(func);
            BasicBlock *loopCondition = BasicBlock::Create(context, "list_loop_condition", func);
            BasicBlock *loopBody = BasicBlock::Create(context, "list_loop_body", func);
            BasicBlock *loopBodyEnd = BasicBlock::Create(context, "list_loop_body_end", func);
            BasicBlock *after = BasicBlock::Create(context, "list_after", func);

            // allocate the dict_ptr* array
            auto list_arr_malloc = builder.CreatePointerCast(env.malloc(builder, builder.CreateMul(num_elements, env.i64Const(8))),
                                                             llvm_list_type->getStructElementType(2));

            // read the elements
            auto loopCounter = builder.CreateAlloca(Type::getInt64Ty(context));
            builder.CreateStore(env.i64Const(0), loopCounter);
            builder.CreateBr(loopCondition);

            builder.SetInsertPoint(loopCondition);
            auto loopNotDone = builder.CreateICmpSLT(builder.CreateLoad(builder.getInt64Ty(), loopCounter),
                                                     num_elements);
            builder.CreateCondBr(loopNotDone, loopBody, after);

            builder.SetInsertPoint(loopBody);
            // store the pointer to the string
            auto current_offset_ptr = builder.CreateGEP(builder.getInt64Ty(),
                                                        offset_ptr,
                                                        builder.CreateLoad(builder.getInt64Ty(), loopCounter));
            auto current_offset = builder.CreateLoad(builder.getInt64Ty(),
                                                     current_offset_ptr);
            auto next_struct_ptr = builder.CreateGEP(llvm_list_element_type->getPointerTo(), list_arr_malloc, builder.CreateLoad(builder.getInt64Ty(), loopCounter));

            llvm::Value* offset=nullptr; llvm::Value *size=nullptr;
            std::tie(offset, size) = unpack_offset_and_size(builder, current_offset);
            builder.CreateStore(builder.CreateAdd(size, builder.CreateLoad(builder.getInt64Ty(), acc_size_var)), acc_size_var);

            auto serialized_struct_ptr = builder.MovePtrByBytes(builder.CreateBitCast(current_offset_ptr, env.i8ptrType()), offset);

            env.printValue(builder, builder.CreateLoad(builder.getInt64Ty(), loopCounter), "deserializing " + element_type.desc() + " at list position: ");

            // decode into rtmalloced pointer & store
            auto dict_ptr = struct_dict_deserialize_from_memory(env, builder, serialized_struct_ptr, element_type, true);

            // old: when not using struct_dict** in list, but rather struct_dict*.
            // auto dict = builder.CreateLoad(llvm_list_element_type, dict_ptr.val);
            //builder.CreateStore(dict, next_struct_ptr);

            // new:
            builder.CreateStore(dict_ptr.val, next_struct_ptr); // <-- MUST BE heap allocated.

            builder.CreateBr(loopBodyEnd);


            builder.SetInsertPoint(loopBodyEnd);
            // update the loop variable and return
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), loopCounter),
                                                  env.i64Const(1)), loopCounter);
            builder.CreateBr(loopCondition);

            builder.SetInsertPoint(after);

            // store the malloc'd and populated array to the struct
            auto list_arr = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);
            builder.CreateStore(list_arr_malloc, list_arr);

            // last element in offset will have size + offset. can use that to decode total size?
            auto total_varlen_size = builder.CreateLoad(builder.getInt64Ty(), acc_size_var);
            auto total_offset = builder.CreateAdd(builder.CreateMul(env.i64Const(sizeof(int64_t)), num_elements), total_varlen_size);
            return total_offset;
        }

        static llvm::Value* list_deserialize_list_of_tuples_from_memory(LLVMEnvironment& env,
                                                                              const codegen::IRBuilder& builder,
                                                                              llvm::Value* ptr,
                                                                              const python::Type& list_type,
                                                                              llvm::Value* num_elements,
                                                                              llvm::Value* list_ptr) {
            using namespace llvm;

            // len | (offset|size) | .... | (offset|size) | item1 | .... |item_len

            assert(list_type.isListType());
            auto element_type = list_type.elementType();
            assert(element_type.isTupleType());

            // this var can be eliminated --> put this logic into func?
            auto acc_size_var = env.CreateFirstBlockAlloca(builder, builder.getInt64Ty());
            builder.CreateStore(env.i64Const(0), acc_size_var);

            auto llvm_list_type = env.createOrGetListType(list_type);
            auto llvm_list_element_type = env.getOrCreateTupleType(element_type);
            auto offset_ptr = builder.CreateBitCast(ptr, env.i64ptrType()); // get pointer to i64 serialized array of offsets
            auto& context = env.getContext();

            // need to point to each of the strings and calculate lengths
            llvm::Function *func = builder.GetInsertBlock()->getParent();
            assert(func);
            BasicBlock *loopCondition = BasicBlock::Create(context, "list_loop_condition_tuple_decode", func);
            BasicBlock *loopBody = BasicBlock::Create(context, "list_loop_body_tuple_decode", func);
            BasicBlock *loopBodyEnd = BasicBlock::Create(context, "list_loop_body_end_tuple_decode", func);
            BasicBlock *after = BasicBlock::Create(context, "list_after_tuple_decode", func);

            // allocate the dict_ptr* array
            auto list_arr_malloc = builder.CreatePointerCast(env.malloc(builder, builder.CreateMul(num_elements, env.i64Const(8))),
                                                             llvm_list_type->getStructElementType(2));

            // read the elements
            auto loopCounter = builder.CreateAlloca(Type::getInt64Ty(context));
            builder.CreateStore(env.i64Const(0), loopCounter);
            builder.CreateBr(loopCondition);

            builder.SetInsertPoint(loopCondition);
            auto loopNotDone = builder.CreateICmpSLT(builder.CreateLoad(builder.getInt64Ty(), loopCounter),
                                                     num_elements);
            builder.CreateCondBr(loopNotDone, loopBody, after);

            builder.SetInsertPoint(loopBody);
            // store the pointer to the string
            auto current_offset_ptr = builder.CreateGEP(builder.getInt64Ty(),
                                                        offset_ptr,
                                                        builder.CreateLoad(builder.getInt64Ty(), loopCounter));
            auto current_offset = builder.CreateLoad(builder.getInt64Ty(),
                                                     current_offset_ptr);
            auto next_tuple_ptr = builder.CreateGEP(llvm_list_element_type, list_arr_malloc, builder.CreateLoad(builder.getInt64Ty(), loopCounter));

            llvm::Value* offset=nullptr; llvm::Value *size=nullptr;
            std::tie(offset, size) = unpack_offset_and_size(builder, current_offset);
            builder.CreateStore(builder.CreateAdd(size, builder.CreateLoad(builder.getInt64Ty(), acc_size_var)), acc_size_var);

            auto serialized_tuple_ptr = builder.MovePtrByBytes(builder.CreateBitCast(current_offset_ptr, env.i8ptrType()), offset);

            env.printValue(builder, builder.CreateLoad(builder.getInt64Ty(), loopCounter), "deserializing " + element_type.desc() + " at list position: ");


            // decode tuple, no need for heap alloc here because tuples are immutable.
            FlattenedTuple ft(&env);
            ft.init(element_type);
            ft.deserializationCode(builder, serialized_tuple_ptr);
            auto tuple_ptr = ft.getLoad(builder);

            builder.CreateStore(tuple_ptr, next_tuple_ptr);
            builder.CreateBr(loopBodyEnd);


            builder.SetInsertPoint(loopBodyEnd);
            // update the loop variable and return
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), loopCounter),
                                                  env.i64Const(1)), loopCounter);
            builder.CreateBr(loopCondition);

            builder.SetInsertPoint(after);

            // store the malloc'd and populated array to the struct
            auto list_arr = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);
            builder.CreateStore(list_arr_malloc, list_arr);

            // last element in offset will have size + offset. can use that to decode total size?
            auto total_varlen_size = builder.CreateLoad(builder.getInt64Ty(), acc_size_var);
            auto total_offset = builder.CreateAdd(builder.CreateMul(env.i64Const(sizeof(int64_t)), num_elements), total_varlen_size);
            return total_offset;
        }

        static llvm::Value* list_deserialize_list_of_lists_from_memory(LLVMEnvironment& env,
                                                                              const codegen::IRBuilder& builder,
                                                                              llvm::Value* ptr,
                                                                              const python::Type& list_type,
                                                                              llvm::Value* num_elements,
                                                                              llvm::Value* list_ptr) {
            using namespace llvm;

            // len | (offset|size) | .... | (offset|size) | item1 | .... |item_len

            assert(list_type.isListType());
            auto element_type = list_type.elementType();
            assert(element_type.withoutOption().isListType());

            env.printValue(builder, num_elements, "Deserializing " + list_type.desc() + " from memory with #elements: ");

            // this var can be eliminated --> put this logic into func?
            auto acc_size_var = env.CreateFirstBlockAlloca(builder, builder.getInt64Ty());
            builder.CreateStore(env.i64Const(0), acc_size_var);

            auto llvm_list_type = env.createOrGetListType(list_type);
            auto llvm_list_element_type = env.createOrGetListType(element_type.withoutOption());
            auto offset_ptr = builder.CreateBitCast(ptr, env.i64ptrType()); // get pointer to i64 serialized array of offsets
            auto& context = env.getContext();

            // need to point to each of the strings and calculate lengths
            llvm::Function *func = builder.GetInsertBlock()->getParent();
            assert(func);
            BasicBlock *loopCondition = BasicBlock::Create(context, "list_loop_condition", func);
            BasicBlock *loopBody = BasicBlock::Create(context, "list_loop_body", func);
            BasicBlock *loopBodyEnd = BasicBlock::Create(context, "list_loop_body_end", func);
            BasicBlock *after = BasicBlock::Create(context, "list_after", func);

            // allocate the list_ptr* array
            auto list_arr_malloc = builder.CreatePointerCast(env.malloc(builder, builder.CreateMul(num_elements, env.i64Const(8))),
                                                             llvm_list_type->getStructElementType(2));

            // read the elements
            auto loopCounter = builder.CreateAlloca(Type::getInt64Ty(context));
            builder.CreateStore(env.i64Const(0), loopCounter);
            builder.CreateBr(loopCondition);

            builder.SetInsertPoint(loopCondition);
            auto loopNotDone = builder.CreateICmpSLT(builder.CreateLoad(builder.getInt64Ty(), loopCounter),
                                                     num_elements);
            builder.CreateCondBr(loopNotDone, loopBody, after);

            builder.SetInsertPoint(loopBody);
            // store the pointer to the string
            auto current_offset_ptr = builder.CreateGEP(builder.getInt64Ty(),
                                                        offset_ptr,
                                                        builder.CreateLoad(builder.getInt64Ty(), loopCounter));
            auto current_offset = builder.CreateLoad(builder.getInt64Ty(),
                                                     current_offset_ptr);
            auto next_list_ptr = builder.CreateGEP(llvm_list_element_type->getPointerTo(), list_arr_malloc, builder.CreateLoad(builder.getInt64Ty(), loopCounter));

            llvm::Value* offset=nullptr; llvm::Value *size=nullptr;
            std::tie(offset, size) = unpack_offset_and_size(builder, current_offset);
            builder.CreateStore(builder.CreateAdd(size, builder.CreateLoad(builder.getInt64Ty(), acc_size_var)), acc_size_var);

            auto i = builder.CreateLoad(builder.getInt64Ty(), loopCounter);
            env.printValue(builder, i, "deserializing element no: ");
            env.printValue(builder, offset, element_type.desc() + " current element offset: ");
            env.printValue(builder, size, element_type.desc() + " current element size: ");

            auto serialized_list_element_ptr = builder.MovePtrByBytes(builder.CreateBitCast(current_offset_ptr, env.i8ptrType()), offset);

            // decode into rtmalloced pointer & store
            llvm::Value* element_size_in_bytes=nullptr;
            llvm::Value* is_null = nullptr;
            if(element_type.isOptionType()) {
                auto nullmap_index = llvm_list_type->getStructNumElements() - 1;
                auto nullmap_ptr = builder.CreateStructLoadOrExtract(llvm_list_type, list_ptr, nullmap_index);
                is_null = builder.CreateLoad(builder.getInt8Ty(), builder.CreateGEP(builder.getInt8Ty(), nullmap_ptr, i));
                assert(is_null->getType() == env.i8Type());
                is_null = builder.CreateICmpNE(is_null, env.i8Const(0));
                env.printValue(builder, is_null, "is list element to deserialize None: ");
            }
            auto stack_element_list_ptr = list_deserialize_from(env, builder, serialized_list_element_ptr, element_type.withoutOption(), is_null, &element_size_in_bytes);
            auto element_list = builder.CreateLoad(llvm_list_element_type, stack_element_list_ptr.val);

            // heap alloc!
            auto element_list_ptr = env.CreateHeapAlloca(builder, llvm_list_element_type);
            builder.CreateStore(element_list, element_list_ptr);

            builder.CreateStore(element_list_ptr, next_list_ptr);

            builder.CreateBr(loopBodyEnd);


            builder.SetInsertPoint(loopBodyEnd);
            // update the loop variable and return
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), loopCounter),
                                                  env.i64Const(1)), loopCounter);
            builder.CreateBr(loopCondition);

            builder.SetInsertPoint(after);

            // store the malloc'd and populated array to the struct
            auto list_arr = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);
            builder.CreateStore(list_arr_malloc, list_arr);

            // last element in offset will have size + offset. can use that to decode total size?
            auto total_varlen_size = builder.CreateLoad(builder.getInt64Ty(), acc_size_var);
            auto total_offset = builder.CreateAdd(builder.CreateMul(env.i64Const(sizeof(int64_t)), num_elements), total_varlen_size);

            env.printValue(builder, total_offset, "total deserialized size: ");

            return total_offset;
        }

        SerializableValue list_deserialize_from(LLVMEnvironment& env,
                                                const IRBuilder& builder,
                                                llvm::Value* ptr,
                                                const python::Type& list_type,
                                                llvm::Value* is_null,
                                                llvm::Value** serialized_size_in_bytes) {
            using namespace std;
            using namespace llvm;

            assert(ptr && ptr->getType() == env.i8ptrType());
            assert(list_type.isListType());

//            if(is_null)
//                throw std::runtime_error("Option[List] decode in list_deserialize_from not yet implemented.");


            // fetch element type and whether there's a bitmap to indicate optional list elements.
            auto elementType = list_type.elementType();
            auto elements_optional = elementType.isOptionType();
            if(elements_optional)
                elementType = elementType.getReturnType();

            // alloc list ptr
            SerializableValue list_val;
            auto llvm_list_type = env.createOrGetListType(list_type);

            // heap alloc!
            auto list_ptr = env.CreateHeapAlloca(builder, llvm_list_type); // env.CreateFirstBlockAlloca(builder, llvm_list_type);
            list_val.val = list_ptr;
            list_val.is_null = is_null;
            list_init_empty(env, builder, list_val.val, list_type);

            if(python::Type::EMPTYLIST == list_type)
                return list_val;

            auto original_ptr = ptr;

            BasicBlock* bbDecodeList = nullptr;
            BasicBlock* bbDecodeDone = nullptr;
            BasicBlock* bbFirstBlock = nullptr;
            if(is_null) {
                // create blocks & connect.
                auto& ctx = builder.getContext();
                bbFirstBlock = builder.GetInsertBlock();
                bbDecodeList = BasicBlock::Create(ctx, "list_deserialize_if_not_null", builder.GetInsertBlock()->getParent());
                bbDecodeDone = BasicBlock::Create(ctx, "list_deserialize_done", builder.GetInsertBlock()->getParent());
                builder.CreateCondBr(is_null, bbDecodeDone, bbDecodeList);
                builder.SetInsertPoint(bbDecodeList);
            }


            // each list has first 8 bytes always store size
            auto num_elements = builder.CreateLoad(builder.getInt64Ty(), builder.CreateBitCast(ptr, env.i64ptrType()), "list_num_elements");

            // init list with capacity & store size
            list_reserve_capacity(env, builder, list_ptr, list_type, num_elements, false);
            list_store_size(env, builder, list_ptr, list_type, num_elements);

            env.debugPrint(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " decoding list " + list_type.desc() + " with num_elements=", num_elements);

            // move the size of the first i64 indicating the length
            ptr = builder.MovePtrByBytes(ptr, env.i64Const(sizeof(int64_t)));

            // bitmap? --> move ptr to rest of data & store
            if(elements_optional) {

                auto opt_size = calc_bitmap_size_in_64bit_blocks(builder, num_elements);

                env.printValue(builder, opt_size, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " list " + list_type.desc() + " has serialized bitmap of size=");

                auto bitmap_ptr = list_deserialize_bitmap(env, builder, ptr, num_elements);

                auto n_struct_elements = llvm_list_type->getStructNumElements();
                assert(n_struct_elements >= 3);

                auto target_bitmap_ptr = builder.CreateStructGEP(list_ptr, llvm_list_type, n_struct_elements - 1);
                builder.CreateStore(bitmap_ptr, target_bitmap_ptr);

                ptr = builder.MovePtrByBytes(ptr, opt_size);
            }

            // deserialize based on list element type.
            if(elementType.withoutOption().isSingleValued()) {
                // nothing todo, bitmap already decoded above.
            } else if(python::Type::STRING == elementType || python::Type::PYOBJECT == elementType || python::Type::GENERICDICT == elementType) {
                // These here are all stored as strings.
                // The per element decode differs though.

                llvm::Value* list_arr_malloc = nullptr;
                llvm::Value* list_sizearr_malloc = nullptr;


                auto offset_ptr = builder.CreateBitCast(ptr, env.i64ptrType()); // get pointer to i64 serialized array of offsets
//               llvm::Value* listSize = env.CreateFirstBlockAlloca(builder, env.i64Type());
                auto& context = env.getContext();

                // need to point to each of the strings and calculate lengths
                llvm::Function *func = builder.GetInsertBlock()->getParent();
                assert(func);
                BasicBlock *loopCondition = BasicBlock::Create(context, "list_loop_condition", func);
                BasicBlock *loopBody = BasicBlock::Create(context, "list_loop_body", func);
                BasicBlock *loopBodyEnd = BasicBlock::Create(context, "list_loop_body_end", func);
                BasicBlock *after = BasicBlock::Create(context, "list_after", func);

                if(python::Type::STRING == elementType || python::Type::PYOBJECT == elementType) {
                    // allocate the char* array
                    list_arr_malloc = builder.CreatePointerCast(env.malloc(builder, builder.CreateMul(num_elements, env.i64Const(8))),
                                                                llvm_list_type->getStructElementType(2));
                    // allocate the sizes array
                    list_sizearr_malloc = builder.CreatePointerCast(env.malloc(builder, builder.CreateMul(num_elements, env.i64Const(8))),
                                                                    llvm_list_type->getStructElementType(3));
                } else {
                    assert(elementType == python::Type::GENERICDICT);
                    list_arr_malloc = builder.CreatePointerCast(env.malloc(builder, builder.CreateMul(num_elements, env.i64Const(8))),
                                                                env.i8ptrType()->getPointerTo());
                }

                // read the elements
                auto loopCounter = builder.CreateAlloca(Type::getInt64Ty(context));
                builder.CreateStore(env.i64Const(0), loopCounter);
                builder.CreateBr(loopCondition);

                builder.SetInsertPoint(loopCondition);
                auto loopNotDone = builder.CreateICmpSLT(builder.CreateLoad(builder.getInt64Ty(), loopCounter),
                                                         num_elements);
                builder.CreateCondBr(loopNotDone, loopBody, after);

                builder.SetInsertPoint(loopBody);
                // store the pointer to the string
                auto current_offset_ptr = builder.CreateGEP(builder.getInt64Ty(),
                                                            offset_ptr,
                                                            builder.CreateLoad(builder.getInt64Ty(), loopCounter));
                auto current_offset = builder.CreateLoad(builder.getInt64Ty(),
                                                         current_offset_ptr);
                auto arr_target_ptr = builder.CreateGEP(env.i8ptrType(), list_arr_malloc, builder.CreateLoad(builder.getInt64Ty(), loopCounter));

                llvm::Value* offset=nullptr; llvm::Value *size=nullptr;
                std::tie(offset, size) = unpack_offset_and_size(builder, current_offset);

                auto curStrPtr = builder.MovePtrByBytes(builder.CreateBitCast(current_offset_ptr, env.i8ptrType()), offset);

                // for GENERICDICT decode cJSON
                if(elementType == python::Type::GENERICDICT) {
                    env.printValue(builder, curStrPtr, "decoding " + elementType.desc() + " by parsing as cjson from ptr= ");
                    env.printValue(builder, current_offset, "current info: ");
                    env.printValue(builder, offset, "current offset=");
                    env.printValue(builder, size, "current size=");
                    auto item = call_cjson_parse(builder, curStrPtr);
                    builder.CreateStore(item, arr_target_ptr);
                } else {
                    // for str/pyobject store raw data ref
                    builder.CreateStore(curStrPtr, arr_target_ptr);

                    // store also size
                    // set up to calculate the size based on offsets
                    auto next_size_ptr = builder.CreateGEP(builder.getInt64Ty(), list_sizearr_malloc, builder.CreateLoad(builder.getInt64Ty(), loopCounter));
                    builder.CreateStore(size, next_size_ptr);
                }

                builder.CreateBr(loopBodyEnd);


                builder.SetInsertPoint(loopBodyEnd);
                // update the loop variable and return
                builder.CreateStore(builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), loopCounter),
                                                      env.i64Const(1)), loopCounter);
                builder.CreateBr(loopCondition);

                builder.SetInsertPoint(after);

                // store the malloc'd and populated array to the struct
                auto list_arr = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);
                builder.CreateStore(list_arr_malloc, list_arr);

                if(elementType != python::Type::GENERICDICT && list_sizearr_malloc) {
                    auto list_sizearr = builder.CreateStructGEP(list_ptr, llvm_list_type, 3);
                    builder.CreateStore(list_sizearr_malloc, list_sizearr);
                }

                // move ptr, according to last offset.
                // instead of adding everything up, we can do a trick: I.e., use the last offset + size to check end.
                auto last_info_index = builder.CreateSub(num_elements, env.i64Const(1));
                auto last_info_ptr = builder.CreateGEP(builder.getInt64Ty(),
                                                  offset_ptr,
                                                  last_info_index);
                auto last_info = builder.CreateLoad(builder.getInt64Ty(), last_info_ptr);
                std::tie(offset, size) = unpack_offset_and_size(builder, last_info);
                env.printValue(builder, offset, "last info offset: ");
                env.printValue(builder, size, "last size: ");
                auto num_elements_plus_one = builder.CreateAdd(num_elements, env.i64Const(0));
                llvm::Value* var_data_length = builder.CreateAdd(builder.CreateMul(env.i64Const(sizeof(int64_t)), num_elements_plus_one), builder.CreateAdd(offset, size));
                var_data_length = builder.CreateSelect(builder.CreateICmpEQ(env.i64Const(0), num_elements), env.i64Const(0), var_data_length);
                env.printValue(builder, var_data_length, "varlength after initial i64 field (bytes to be moved + 8): ");

                ptr = builder.MovePtrByBytes(builder.CreatePointerCast(offset_ptr, env.i8ptrType()), var_data_length);

            } else if(python::Type::I64 == elementType || python::Type::F64 == elementType || python::Type::BOOLEAN == elementType) {
                // can just directly point to the serialized data
                auto list_arr = builder.CreateStructGEP(list_ptr, llvm_list_type, 2);
                auto llvm_target_cast_type = python::Type::F64 == elementType ? env.doublePointerType() : env.i64ptrType();
                auto data_ptr = builder.CreateBitCast(ptr, llvm_target_cast_type);
                builder.CreateStore(data_ptr, list_arr);
                auto size_in_bytes = builder.CreateMul(num_elements, env.i64Const(8));
                ptr = builder.MovePtrByBytes(ptr, size_in_bytes);
            } else if(elementType.isStructuredDictionaryType()) {
                auto size_in_bytes = list_deserialize_list_of_struct_dicts_from_memory(env, builder, ptr, list_type, num_elements, list_ptr);

                env.printValue(builder, size_in_bytes, "deserialized " + list_type.desc() + " from memory stored in bytes: ");

                ptr = builder.MovePtrByBytes(ptr, size_in_bytes);
            } else if(elementType.isListType()) {
                auto size_in_bytes = list_deserialize_list_of_lists_from_memory(env, builder, ptr, list_type, num_elements, list_ptr);
                ptr = builder.MovePtrByBytes(ptr, size_in_bytes);
            } else if(elementType.isTupleType() && elementType != python::Type::EMPTYTUPLE) {
                auto size_in_bytes = list_deserialize_list_of_tuples_from_memory(env, builder, ptr, list_type, num_elements, list_ptr);
                ptr = builder.MovePtrByBytes(ptr, size_in_bytes);
            } else {
                throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " list of type " + list_type.desc() + " deserialize not yet supported.");
            }

            // if Option[List] deode, insert blocks.
            if(is_null) {
                auto currentBlock = builder.GetInsertBlock();
                builder.CreateBr(bbDecodeDone);

                builder.SetInsertPoint(bbDecodeDone);
                // now create phi for: 1.) value, 2.) size, 3.) ptr.
                auto phi_ptr = builder.CreatePHI(ptr->getType(), 2);
                phi_ptr->addIncoming(ptr, currentBlock);
                phi_ptr->addIncoming(original_ptr, bbFirstBlock);
                ptr = phi_ptr;
            }

            // debug print
            if(serialized_size_in_bytes) {
                *serialized_size_in_bytes = builder.CreatePtrDiff(builder.getInt8Ty(), ptr, original_ptr);

                env.debugPrint(builder, "read bytes=", *serialized_size_in_bytes);
                auto dbg_serialized_bytes = list_serialized_size(env, builder, list_ptr, list_type);
                env.debugPrint(builder, "serialized size should be in bytes for "+list_type.desc() + ": ", dbg_serialized_bytes);
            }

            return list_val;
        }

        static llvm::Function* list_create_upcast_func(LLVMEnvironment& env, const python::Type& list_type, const python::Type& target_list_type) {
            using namespace llvm;

            assert(python::canUpcastType(list_type, target_list_type));
            // check elements are ok
            assert(list_type.isListType() && target_list_type.isListType());

            assert(list_type != python::Type::EMPTYLIST);

            auto el_type = list_type.elementType();
            auto target_el_type = target_list_type.elementType();
            assert(python::canUpcastType(el_type, target_el_type));

            auto llvm_target_list_type = env.pythonToLLVMType(target_list_type);
            auto llvm_list_type = env.pythonToLLVMType(list_type);
            auto FT = FunctionType::get(env.i64Type(), {llvm_list_type->getPointerTo(0), llvm_target_list_type->getPointerTo(0)}, false);

            auto funcName = "list_upcast_" + list_type.desc() + "_to_" + target_list_type.desc();
            Function *func = Function::Create(FT, Function::InternalLinkage, funcName, env.getModule().get());

            auto& ctx = func->getContext();
            BasicBlock* bbEntry = BasicBlock::Create(ctx, "entry", func);
            IRBuilder builder(bbEntry);
            auto m = mapLLVMFunctionArgs(func, {"in", "out"});
            auto list_ptr = m["in"];
            auto target_list_ptr = m["out"];

            auto list_size = list_length(env, builder, list_ptr, list_type);

            // debug:
            // env.printValue(builder, list_size, "got input list of size=");

            list_init_empty(env, builder, target_list_ptr, target_list_type);
            list_reserve_capacity(env, builder, target_list_ptr, target_list_type, list_size);
            list_store_size(env, builder, target_list_ptr, target_list_type, list_size);

            // create loop over elements now & store everything
            // create loop to fill in values from old list into new list after upcast (should this be emitted as function to inline?)
            // create integer var
            auto i_var =env.CreateFirstBlockAlloca(builder, env.i64Type());
            builder.CreateStore(env.i64Const(0), i_var);
            llvm::BasicBlock* bbLoopHeader = llvm::BasicBlock::Create(ctx, "list_upcast_loop_header", func);
            llvm::BasicBlock* bbLoopBody = llvm::BasicBlock::Create(ctx, "list_upcast_loop_body", func);
            llvm::BasicBlock* bbLoopDone = llvm::BasicBlock::Create(ctx, "list_upcast_loop_done", func);

            // debug
            // env.debugPrint(builder, "go to loop header, entry for upcast");

            builder.CreateBr(bbLoopHeader);
            builder.SetInsertPoint(bbLoopHeader);
            auto icmp = builder.CreateICmpULT(builder.CreateLoad(env.i64Type(), i_var), list_size);
            builder.CreateCondBr(icmp, bbLoopBody, bbLoopDone);

            builder.SetInsertPoint(bbLoopBody);
            // inc.
            auto idx = builder.CreateLoad(env.i64Type(), i_var);

            // debug
            // env.printValue(builder, idx, "i=");

            auto src_el = list_load_value(env, builder, list_ptr, list_type, idx);
            auto target_el = env.upcastValue(builder, src_el, el_type, target_el_type);
            list_store_value(env, builder, target_list_ptr, target_list_type, idx, target_el);

            builder.CreateStore(builder.CreateAdd(idx, env.i64Const(1)), i_var);
            builder.CreateBr(bbLoopHeader);
            builder.SetInsertPoint(bbLoopDone);

            builder.CreateRet(env.i64Const(0));
            return func;
        }

        // new, create func
        llvm::Value* list_upcast(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr,
                                 const python::Type& list_type, const python::Type& target_list_type) {
            // function creation can be cached...
            auto llvm_list_type = env.pythonToLLVMType(list_type);
            auto llvm_target_list_type = env.pythonToLLVMType(target_list_type);
            // Note: can not use stack alloced var here, because in list of list scenario - this could get saved as list ptr into list which would be a problem.
            // --> could do stack alloc check & promote if necessary.
            auto target_list_ptr = env.CreateHeapAlloca(builder, llvm_target_list_type); // env.CreateFirstBlockAlloca(builder, llvm_target_list_type); <-- stack alloc version
            list_init_empty(env, builder, target_list_ptr, target_list_type);

            // shortcut, if empty list -> return target list as empty!
            if(list_type == python::Type::EMPTYLIST) {
                auto num_elements = env.i64Const(0);
                list_reserve_capacity(env, builder, target_list_ptr, target_list_type, num_elements);
                list_store_size(env, builder, target_list_ptr, target_list_type, num_elements);
                return target_list_ptr;
            }

            // create upcast func & fill in. (@TODO: could cache this!)
            auto upcast_func = list_create_upcast_func(env, list_type, target_list_type);

            // this is a hacky workaround... -> should be ptr, but no time to fix this...
             // if list is not a real pointer, store in temp var!
             if(!list_ptr->getType()->isPointerTy()) {
                 auto list_val = list_ptr;
                 list_ptr = env.CreateFirstBlockAlloca(builder, llvm_list_type);
                 builder.CreateStore(list_val, list_ptr);
             }

#ifndef NDEBUG
            if(list_ptr->getType() != llvm_list_type->getPointerTo()) {
                std::cerr<<"list_ptr type is not expected type, "<<std::endl;
                std::cerr<<"is: "<<env.getLLVMTypeName(list_ptr->getType())<<std::endl;
                std::cerr<<"expected: "<<env.getLLVMTypeName(llvm_list_type->getPointerTo())<<std::endl;
                std::cerr<<"wrong load/alloc?"<<std::endl;
            }
#endif

            // check args are fine
            assert(list_ptr->getType() == llvm_list_type->getPointerTo());
            assert(target_list_ptr->getType() == llvm_target_list_type->getPointerTo());

            builder.CreateCall(upcast_func, {list_ptr, target_list_ptr});
            return target_list_ptr;
        }

        // old, causes bug in LLVM for float...
//        llvm::Value* list_upcast(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr,
//                                 const python::Type& list_type, const python::Type& target_list_type) {
//
//            env.debugPrint(builder, "enter list upcast function " + list_type.desc() + " -> " + target_list_type.desc());
//
//            // make sure current element and target element type are compatible!
//            assert(python::canUpcastType(list_type, target_list_type));
//
//            // check elements are ok
//            assert(list_type.isListType() && target_list_type.isListType());
//
//            auto el_type = list_type.elementType();
//            auto target_el_type = target_list_type.elementType();
//            assert(python::canUpcastType(el_type, target_el_type));
//
//            auto llvm_target_list_type = env.pythonToLLVMType(target_list_type);
//            auto target_list_ptr = env.CreateFirstBlockAlloca(builder, llvm_target_list_type);
//
//            // special case: empty list -> something?
//            if(list_type == python::Type::EMPTYLIST) {
//                // -> simply alloc empty list of target type and return
//                list_init_empty(env, builder, target_list_ptr, target_list_type);
//                return target_list_ptr;
//            }
//
//            // get list size of original list & alloc new list with target type!
//            auto list_size = list_length(env, builder, list_ptr, list_type);
//            list_init_empty(env, builder, target_list_ptr, target_list_type);
//            list_reserve_capacity(env, builder, target_list_ptr, target_list_type, list_size);
//
//            // create loop to fill in values from old list into new list after upcast (should this be emitted as function to inline?)
//            auto& ctx = builder.getContext();
//            auto func = builder.GetInsertBlock()->getParent();
//            // create integer var
//            auto i_var =env.CreateFirstBlockAlloca(builder, env.i64Type());
//            builder.CreateStore(env.i64Const(0), i_var);
//            llvm::BasicBlock* bbLoopHeader = llvm::BasicBlock::Create(ctx, "list_upcast_loop_header", func);
//            llvm::BasicBlock* bbLoopBody = llvm::BasicBlock::Create(ctx, "list_upcast_loop_body", func);
//            llvm::BasicBlock* bbLoopDone = llvm::BasicBlock::Create(ctx, "list_upcast_loop_done", func);
//            env.debugPrint(builder, "go to loop header, entry for upcast");
//            builder.CreateBr(bbLoopHeader);
//            builder.SetInsertPoint(bbLoopHeader);
//            auto icmp = builder.CreateICmpULT(builder.CreateLoad(i_var), list_size);
//            builder.CreateCondBr(icmp, bbLoopBody, bbLoopDone);
//
//            builder.SetInsertPoint(bbLoopBody);
//            // inc.
//            auto idx = builder.CreateLoad(i_var);
//            builder.CreateStore(builder.CreateAdd(idx, env.i64Const(1)), i_var);
//            // store element (after upcast)
////            auto src_el = list_load_value(env, builder, list_ptr, list_type, idx);
////            auto target_el = env.upcastValue(builder, src_el, el_type, target_el_type);
////            list_store_value(env, builder, target_list_ptr, target_list_type, idx, target_el);
//            builder.CreateBr(bbLoopHeader);
//
//            builder.SetInsertPoint(bbLoopDone);
//            return target_list_ptr;
//        }


        void list_print(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr,
                        const python::Type& list_type) {
            using namespace llvm;

            // get first number of element
            auto len = list_length(env, builder, list_ptr, list_type);

            auto element_type = list_type.elementType();

            env.printValue(builder, len, "Got list " + list_type.desc() + " with #elements=");

            // depending on element type, invoke helper
            // start loop going over the size entries (--> this could be vectorized!)
            auto& ctx = env.getContext(); auto F = builder.GetInsertBlock()->getParent();
            BasicBlock *bLoopHeader = BasicBlock::Create(ctx, "list_print_loop_header", F);
            BasicBlock *bLoopBody = BasicBlock::Create(ctx, "list_print_loop_body", F);
            BasicBlock *bLoopExit = BasicBlock::Create(ctx, "list_print_loop_done", F);

            auto loop_i = env.CreateFirstBlockAlloca(builder, builder.getInt64Ty());
            builder.CreateStore(env.i64Const(0), loop_i);

            builder.CreateBr(bLoopHeader);

            {
                // --- header ---
                builder.SetInsertPoint(bLoopHeader);
                // if i < len:
                auto loop_i_val = builder.CreateLoad(env.i64Type(), loop_i);
                auto loop_cond = builder.CreateICmpULT(loop_i_val, len);
                builder.CreateCondBr(loop_cond, bLoopBody, bLoopExit);
            }


            {
                // --- body ---
                builder.SetInsertPoint(bLoopBody);
                auto loop_i_val = builder.CreateLoad(env.i64Type(), loop_i);

                auto element = list_load_value(env, builder, list_ptr, list_type, loop_i_val);

                if(element_type == python::Type::STRING) {
                    env.printValue(builder, element.val, "element (str): ");
                    env.printValue(builder, element.size, "size (str): ");
                } else if(element_type.isListType()) {
                    env.debugPrint(builder, "sub list ->");
                    list_print(env, builder, element.val, element_type);
                    env.debugPrint(builder, "<- sub list ");
                } else {
                    // throw std::runtime_error("list_print for type " + list_type.desc() + " not supported yet");
                }

                // inc. loop counter
                builder.CreateStore(builder.CreateAdd(env.i64Const(1), loop_i_val), loop_i);
                builder.CreateBr(bLoopHeader);
            }

            builder.SetInsertPoint(bLoopExit);
        }
    }
}