//
// Created by leonhard on 9/22/22.
//

#ifndef TUPLEX_LISTHELPER_H
#define TUPLEX_LISTHELPER_H

#include <codegen/LLVMEnvironment.h>

// contains helper functions to generate code to work with lists

namespace tuplex {
    namespace codegen {
        extern void list_init_empty(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type);

        /*!
         * note that this doesn't perform any size vs. capacity check etc. It's a dumb function to simply change the capacity and (runtime) allocate a new array.
         * @param env
         * @param builder
         * @param list_ptr
         * @param list_type
         * @param capacity
         * @param initialize
         */
        extern void list_reserve_capacity(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* capacity, bool initialize=false);

        /*!
         * returns length / size of list in elements. I.e. for [1, 2, 3, 4] this is the same as len([1, 2, 3, 4])
         * @param env
         * @param builder
         * @param list_ptr
         * @param list_type
         * @return i64 holding the list length.
         */
        extern llvm::Value* list_length(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type);

        /*!
         * stores value (WITHOUT ANY CHECKS for mem safety) at index idx in the list.
         * @param env
         * @param builder
         * @param list_ptr
         * @param list_type
         * @param value
         */
        extern void list_store_value(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* idx, const SerializableValue& value);

        /*!
         * loadas value (WITHOUT ANY CHECKS for mem safety) at index idx from list.
         * @param env
         * @param builder
         * @param list_ptr
         * @param list_type
         * @param idx
         * @return
         */
        extern SerializableValue list_load_value(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* idx);

        extern void list_store_size(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* size);

        /*!
         * return the serialized bytes the list would require in bytes.
         * @param env
         * @param builder
         * @param list_ptr
         * @param list_type
         * @return
         */
        extern llvm::Value* list_serialized_size(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, python::Type list_type);


        extern llvm::Value* list_serialize_to(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr, python::Type list_type, llvm::Value* dest_ptr);

        /*!
         * deserialize list from memory pointer. Returns number of decoded bytes (if not null), and list pointer.
         * @param env
         * @param builder
         * @param ptr
         * @param list_type
         * @param is_null optional parameter, ignored if nullptr which tells whether the value is null. Then, list is decoded as option.
         * @return updated memory pointer (position after list was deserialized) and the list value as llvm value.
         */
        extern SerializableValue list_deserialize_from(LLVMEnvironment& env,
                                                       const IRBuilder& builder,
                                                       llvm::Value* ptr,
                                                       const python::Type& list_type,
                                                       llvm::Value* is_null=nullptr,
                                                       llvm::Value** serialized_size_in_bytes=nullptr);



        llvm::Value* list_of_varitems_serialized_size(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr,
                                                      const python::Type& list_type,
                                                      std::function<llvm::Value*(LLVMEnvironment&, const IRBuilder&, llvm::Value*, llvm::Value*)> f_item_size);

        extern llvm::Value* list_upcast(LLVMEnvironment& env, const IRBuilder& builder, llvm::Value* list_ptr,
                                        const python::Type& list_type, const python::Type& target_list_type);

        extern void list_print(LLVMEnvironment& env, const IRBuilder& builder,  llvm::Value* list_ptr,
        const python::Type& list_type);

    }
}

#endif //TUPLEX_LISTHELPER_H
