//
// Created by leonhards on 6/25/24.
//

#include <physical/experimental/yyjson_helper.h>
#include <jit/RuntimeInterface.h>
#include <iostream>

namespace tuplex {
    bool yyjson_is_array_of_objects(yyjson_mut_val* val) {
        if(!val)
            return false;

        if(yyjson_mut_is_arr(val))
            return false;

        yyjson_mut_arr_iter iter = yyjson_mut_arr_iter_with(val);
        while ((val = yyjson_mut_arr_iter_next(&iter))) {
            if(!yyjson_mut_is_obj(val))
                return false;
        }
        return true;
    }

    void* yy_malloc_wrapper(void* ctx, unsigned long size) {
        assert(runtime::loaded());
        return runtime::rtmalloc(size);
    }

    void* yy_realloc_wrapper(void* ctx, void* ptr, size_t old_size, size_t size) {
        assert(runtime::loaded());
        auto new_ptr = runtime::rtmalloc(size);
        if(size == 0 || !ptr)
            return new_ptr;

        memmove(new_ptr, ptr, size);
        return new_ptr;
    }

    void yy_free_wrapper(void* ctx, void* ptr) {
        // nothind todo.
    }

    void yyjson_set_runtime_alc(yyjson_alc* alc) {
        if(!alc)
            return;

        alc->malloc = yy_malloc_wrapper;
        alc->realloc = yy_realloc_wrapper;
        alc->ctx = nullptr;
        alc->free = yy_free_wrapper;
    }

    yyjson_mut_doc* yyjson_init_doc() {
        yyjson_alc alc;
        yyjson_set_runtime_alc(&alc);

        return yyjson_mut_doc_new(&alc);
    }

    char* yyjson_print_to_runtime_str(yyjson_mut_val* val, int64_t* out_size) {
        assert(out_size);

        yyjson_alc alc;
        yyjson_set_runtime_alc(&alc);
        size_t str_len = 0;

#ifndef NDEBUG
        yyjson_write_err err;
        auto err_ptr = &err;
#else
        yyjson_write_err* err_ptr = nullptr;
#endif

        // consider using for flags YYJSON_WRITE_ALLOW_INF_AND_NAN
        // as this allows NaN / Infinity to be correctly written (though this violates the standard).
        // should then also use YYJSON_READ_ALLOW_INF_AND_NAN

        auto str_ptr = yyjson_mut_val_write_opts(val, 0, &alc, &str_len, err_ptr);

#ifndef NDEBUG
        if(err_ptr) {
            std::cerr<<"yyjson write error: ["<<err_ptr->code<<"]  "<<err_ptr->msg<<std::endl;
        }
#endif

        *out_size = str_len + 1;
        return str_ptr;
    }

    yyjson_mut_doc* JsonItem_to_yyjson_mut_doc(codegen::JsonItem* item) {
        // TODO: could optimize this, for now go indirection route of dump/unparse
        std::stringstream ss;
        ss<<item->o;
        auto json_line = ss.str();

        yyjson_alc alc;
        yyjson_set_runtime_alc(&alc);

#ifndef NDEBUG
        yyjson_read_err err;
        auto err_ptr = &err;
#else
        yyjson_read_err* err_ptr = nullptr;
#endif

        auto yy_doc = yyjson_read_opts(const_cast<char*>(json_line.c_str()), json_line.size(), 0, nullptr, err_ptr);

#ifndef NDEBUG
        if(err_ptr) {
            std::cerr<<"yyjson write error: ["<<err_ptr->code<<"]  "<<err_ptr->msg<<std::endl;
        }
#endif

        auto yy_mut_doc = yyjson_doc_mut_copy(yy_doc, &alc);
        yyjson_doc_free(yy_doc);

        return yy_mut_doc;
    }

    char* yyjson_type_as_runtime_str(yyjson_mut_val* val, int64_t* out_size) {
        std::string s;

        // TOOD:
        s = "NOT YET IMPLEMENTED";

        auto ptr = runtime::rtmalloc(s.length() + 1);
        memcpy(ptr, s.c_str(), s.length() + 1);

        *out_size = s.length() + 1;
        return (char*)ptr;
    }
}