//
// Created by leonhard on 10/13/22.
//

#ifndef TUPLEX_STRUCTCOMMON_H
#define TUPLEX_STRUCTCOMMON_H

#include "Base.h"
#include <unordered_map>
#include <iostream>
#include "TypeSystem.h"
#include "Utils.h"
#include "TypeHelper.h"
#include "Field.h"

// holds helper structures to decode struct dicts from memory e.g.
namespace tuplex {

    // typedefs for struct types
    // recursive function to decode data, similar to flattening the type below
    // each item should be access_path | value_type | alwaysPresent |  value : SerializableValue | present : i1
    using access_path_t = std::vector<std::pair<std::string, python::Type>>;

    // flatten struct dict.
    using flattened_struct_dict_entry_t = std::tuple<std::vector<std::pair<std::string, python::Type>>, python::Type, bool>;
    using flattened_struct_dict_entry_list_t = std::vector<flattened_struct_dict_entry_t>;

    inline bool noNeedToSerializeType(const python::Type &t) {
        // some types do not need to get serialized. This function specifies this
        if (t.isConstantValued())
            return true; // no need to serialize constants!
        if (t.isSingleValued())
            return true; // no need to serialize special constant values (like null, empty dict, empty list, empty tuple, ...)
        return false;
    }

    extern std::string access_path_to_str(const access_path_t& path);

    inline std::string json_access_path_to_string(const std::vector<std::pair<std::string, python::Type>>& path,
                                                  const python::Type& value_type,
                                                  bool always_present) {
        std::stringstream ss;
        // first the path:
        ss<<access_path_to_str(path)<<" -> ";
        auto presence = !always_present ? "  (maybe)" : "";
        auto v_type = value_type;
        auto value_desc = v_type.isStructuredDictionaryType() ? "Struct[...]" : v_type.desc();
        ss << value_desc << presence;
        return ss.str();
    }

    extern void flatten_recursive_helper(flattened_struct_dict_entry_list_t &entries,
                                         const python::Type &dict_type,
                                         std::vector<std::pair<std::string, python::Type>> prefix = {},
                                         bool include_maybe_structs = true);

    extern void print_flatten_structured_dict_type(const python::Type &dict_type, std::ostream& os=std::cout);

    extern bool struct_dict_field_present(const std::unordered_map<access_path_t, std::tuple<int, int, int, int>>& indices,
                                          const access_path_t& path, const std::vector<uint64_t>& presence_map);

    /*!
     * get all indices of the form bitmap_idx, present_idx, value_idx, size_idx.
     * @param dict_type
     * @return map of access paths to indices
     */
    extern std::unordered_map<access_path_t, std::tuple<int, int, int, int>> struct_dict_load_indices(const python::Type& dict_type);

    /*!
     * get single index tuple of bitmap_idx, present_idx, value_idx, size_idx for an access path. If indices for path are not there, return -1, -1, -1, -1.
     * @param dict_type
     * @param path
     * @return tuple of bitmap_idx, present_idx, value_idx, size_idx
     */
    extern std::tuple<int, int, int, int> struct_dict_get_indices(const python::Type& dict_type, const access_path_t& path);

    extern bool struct_dict_has_bitmap(const python::Type& dict_type);
    extern bool struct_dict_has_presence_map(const python::Type& dict_type);

    /*!
     * retrieve the element type under access path.
     * @param dict_type
     * @param path
     * @return element type or UNKNOWN if access path is not found.
     */
    extern python::Type struct_dict_type_get_element_type(const python::Type& dict_type, const access_path_t& path);

    extern void retrieve_bitmap_counts(const flattened_struct_dict_entry_list_t& entries, size_t& bitmap_element_count, size_t& presence_map_element_count);

    // decode
    extern std::string decodeListAsJSON(const python::Type& list_type, const uint8_t* buf, size_t buf_size);

    extern std::string decodeStructDictFromBinary(const python::Type& dict_type, const uint8_t* buf, size_t buf_size);

    /*!
     * determine size of field holding struct dict type (or option thereof).
     * @param f
     * @return size in bytes encoded this requires.
     */
    extern size_t struct_dict_get_size(const Field& f);
    extern size_t struct_dict_get_size(const python::Type& dict_type, const char* json_data, size_t json_data_size);

    extern size_t struct_dict_serialize_to(const Field& f, uint8_t* ptr);
    extern size_t struct_dict_serialize_to(const python::Type& dict_type, const char* json_data, size_t json_data_size, uint8_t* ptr);

    extern Field struct_dict_upcast_field(const Field& f, const python::Type& target_type);

    extern std::vector<uint64_t> boolean_array_to_block_bitmap(const std::vector<bool>& bitmap, size_t n_blocks);

    inline bool struct_dict_access_paths_equal(const access_path_t& lhs, const access_path_t& rhs) {
        return json_access_path_to_string(lhs, python::Type::UNKNOWN, false).compare(json_access_path_to_string(lhs, python::Type::UNKNOWN, false)) == 0;
    }

    extern std::vector<access_path_t> struct_dict_get_optional_nesting_paths_along_path(const python::Type& dict_type, const access_path_t& path);

    inline bool struct_dict_access_path_has_optional_nesting(const python::Type& dict_type, const access_path_t& path) {
        return !struct_dict_get_optional_nesting_paths_along_path(dict_type, path).empty();
    }
}

#endif //TUPLEX_STRUCTCOMMON_H
