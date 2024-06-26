//
// Created by leonhards on 10/30/23.
//

#ifndef TUPLEX_PARAMETERS_H
#define TUPLEX_PARAMETERS_H

// uncomment following line to use yyjson library instead of (slow cJSON) library
#define USE_YYJSON_INSTEAD

namespace tuplex {
    // use row type to push down compute (should become standard)
    static const bool PARAM_USE_ROW_TYPE=true;

    // TODO: remove.
    // // check to use generic dict instead of struct dict type to execute
    // static const bool PARAM_USE_GENERIC_DICT=false;//true;

    // // manually modify for Github types to parse only in JSON what's necessary.
    // static const bool PARAM_USE_SPARSE_HACK=true;
}
#endif //TUPLEX_PARAMETERS_H
