//
// Created by leonhards on 12/9/24.
//

#include "TestUtils.h"
#include <physical/experimental/yyjson_helper.h>
#include <Context.h>

namespace tuplex {
    // Test a few of tje yyjson helper functions to make sure they're working properly.

    TEST(yyjson, is_array_of_objects) {
        // Requires runtime alloc for yyjson allocs.
        ContextOptions co = ContextOptions::defaults();
        runtime::init(co.RUNTIME_LIBRARY().toPath());


        EXPECT_FALSE(yyjson_is_array_of_objects(nullptr));

        // test a few documents.
        EXPECT_FALSE(yyjson_is_array_of_objects(yyjson_mut_doc_get_root(yyjson_mut_parse_cc_string("{}"))));
        EXPECT_FALSE(yyjson_is_array_of_objects(yyjson_mut_doc_get_root(yyjson_mut_parse_cc_string("42"))));
        EXPECT_FALSE(yyjson_is_array_of_objects(yyjson_mut_doc_get_root(yyjson_mut_parse_cc_string("[{}, null, {}]"))));
        EXPECT_FALSE(yyjson_is_array_of_objects(yyjson_mut_doc_get_root(yyjson_mut_parse_cc_string("[[], {}]"))));
        EXPECT_FALSE(yyjson_is_array_of_objects(yyjson_mut_doc_get_root(yyjson_mut_parse_cc_string("[1, null, 2]"))));

        EXPECT_TRUE(yyjson_is_array_of_objects(yyjson_mut_doc_get_root(yyjson_mut_parse_cc_string("[]"))));
        EXPECT_TRUE(yyjson_is_array_of_objects(yyjson_mut_doc_get_root(yyjson_mut_parse_cc_string("[{}]"))));
        EXPECT_TRUE(yyjson_is_array_of_objects(yyjson_mut_doc_get_root(yyjson_mut_parse_cc_string("[{}, {},{}]"))));
        EXPECT_TRUE(yyjson_is_array_of_objects(yyjson_mut_doc_get_root(yyjson_mut_parse_cc_string("[{\"a\":20}, {\"b\":10, \"a\":10}]"))));
    }

    TEST(yyjson, construct_from_empty) {
        // Requires runtime alloc for yyjson allocs.
        ContextOptions co = ContextOptions::defaults();
        runtime::init(co.RUNTIME_LIBRARY().toPath());

        auto doc = yyjson_init_doc();
        yyjson_mut_doc_set_root(doc, yyjson_mut_obj(doc));

        auto root = yyjson_mut_doc_get_root(doc);
        ASSERT_TRUE(root);
        int64_t size = -1;
        std::string view = yyjson_print_to_runtime_str(root, &size);
        EXPECT_EQ(size, 3);
        EXPECT_EQ(view, "{}");

        // Check now setting string to value.
        yyjson_mut_obj_put(root, yyjson_mut_str(doc, "abc"), yyjson_mut_str(doc, "test"));
        view = yyjson_print_to_runtime_str(root, &size);
        EXPECT_EQ(view, "{\"abc\":\"test\"}");

        yyjson_mut_doc_set_root(doc, yyjson_mut_str(doc, "hello"));
        root = yyjson_mut_doc_get_root(doc);
        view = yyjson_print_to_runtime_str(root, &size);
        EXPECT_EQ(view, "\"hello\"");
    }
}