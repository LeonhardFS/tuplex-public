//
// Created by leonhards on 6/13/24.
//
#include "gtest/gtest.h"
#include <StringUtils.h>
#include <CSVUtils.h>
#include <third_party/i64toa_sse2.h>

#include <StructCommon.h>


TEST(StructCommon, NestedStruct) {
    using namespace tuplex;

    auto encoded_type = "Struct[(str,'description'->str),(str,'html_url'->str),(str,'id'->i64),(str,'members_url'->str),(str,'name'->str),(str,'node_id'->str),(str,'parent'->Option[Struct[(str,'name'->str),(str,'id'->i64),(str,'node_id'->str),(str,'slug'->str),(str,'description'->str),(str,'privacy'->str),(str,'url'->str),(str,'html_url'->str),(str,'members_url'->str),(str,'repositories_url'->str),(str,'permission'->str)]]),(str,'permission'->str),(str,'privacy'->str),(str,'repositories_url'->str),(str,'slug'->str),(str,'url'->str)]";

    auto json_data = "{\"name\":\"leads\",\"id\":2930428,\"node_id\":\"MDQ6VGVhbTI5MzA0Mjg=\",\"slug\":\"leads\",\"description\":\"🎉 leads of UBC Launch Pad\",\"privacy\":\"closed\",\"url\":\"https://api.github.com/organizations/12406312/team/2930428\",\"html_url\":\"https://github.com/orgs/ubclaunchpad/teams/leads\",\"members_url\":\"https://api.github.com/organizations/12406312/team/2930428/members{/member}\",\"repositories_url\":\"https://api.github.com/organizations/12406312/team/2930428/repos\",\"permission\":\"pull\",\"parent\":null}";

    auto json_data_size = 483;

    auto json_type = python::Type::decode(encoded_type);
    auto s_size = struct_dict_get_size(json_type, json_data, json_data_size);
    EXPECT_GT(s_size, 0);
}