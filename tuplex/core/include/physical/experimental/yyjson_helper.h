//
// Created by leonhards on 6/25/24.
//

#ifndef TUPLEX_YYJSON_HELPER_H
#define TUPLEX_YYJSON_HELPER_H

#include <yyjson.h>
#include <physical/experimental/JsonHelper.h>

namespace tuplex {
    extern bool yyjson_is_array_of_objects(yyjson_mut_val* val);

    extern yyjson_mut_doc* yyjson_init_doc();

    extern char* yyjson_print_to_runtime_str(yyjson_mut_val* val, int64_t* out_size);

    extern char* yyjson_type_as_runtime_str(yyjson_mut_val* val, int64_t* out_size);

    extern yyjson_mut_doc* JsonItem_to_yyjson_mut_doc(codegen::JsonItem* item);
}
#endif //TUPLEX_YYJSON_HELPER_H
