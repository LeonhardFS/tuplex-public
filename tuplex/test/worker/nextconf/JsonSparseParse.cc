//
// Created by leonhards on 6/17/24.
//

#include "TestUtils.h"
#include "JsonStatistic.h"
#include "physical/codegen/JsonSourceTaskBuilder.h"
#include <codegen/CodegenHelper.h>

TEST(JsonSparseParse, SimpleJsonString) {
    using namespace tuplex;
    using namespace std;

    auto test_data = "{\"type\":\"PushEvent\",\"public\":true,\"actor\":{\"gravatar_id\":\"19b0daa323b6af55864f706d8e25100e\",\"url\":\"https://api.github.com/users/MartinJKurz\",\"avatar_url\":\"https://secure.gravatar.com/avatar/19b0daa323b6af55864f706d8e25100e?d=%2Fimages%2Fgravatars%2Fgravatar-user-420.png\",\"id\":898159,\"login\":\"MartinJKurz\"},\"created_at\":\"2011-10-15T00:00:00Z\",\"payload\":{\"head\":\"4d8e6f13daec514ea2e075afeeb3d021afb7473e\",\"size\":1,\"push_id\":46262531,\"commits\":[{\"sha\":\"4d8e6f13daec514ea2e075afeeb3d021afb7473e\",\"author\":{\"name\":\"Martin J. Kurz\",\"email\":\"e7b16791bb505733357139208fac489d81c7842a@googlemail.com\"},\"url\":\"https://api.github.com/repos/MartinJKurz/invaders/commits/4d8e6f13daec514ea2e075afeeb3d021afb7473e\",\"message\":\"debug output, minor changes 2\"}],\"ref\":\"refs/heads/master\",\"legacy\":{\"head\":\"4d8e6f13daec514ea2e075afeeb3d021afb7473e\",\"size\":1,\"push_id\":46262531,\"shas\":[[\"4d8e6f13daec514ea2e075afeeb3d021afb7473e\",\"martin.j.kurz@googlemail.com\",\"debug output, minor changes 2\",\"Martin J. Kurz\"]],\"ref\":\"refs/heads/master\"}},\"id\":\"1492634118\",\"repo\":{\"url\":\"https://api.github.com/repos/MartinJKurz/invaders\",\"id\":2355143,\"name\":\"MartinJKurz/invaders\"}}";

    // // -> for Row['created_at'->str,
    //        // 'type'->str,
    //        // 'payload'->Struct['commits'->List[...], 'target'->Struct['id'->int64],'id'->int64],
    //        // 'id'->Option[int64],
    //        // 'repo'->Struct['id'->Option[int64]]]

    auto author_struct_type = python::Type::makeStructuredDictType({make_pair("name", python::Type::STRING), make_pair("email", python::Type::STRING)});

    auto commits_struct_type = python::Type::makeStructuredDictType({make_pair("sha", python::Type::STRING),
                                                                     make_pair("author", author_struct_type),
                                                                     make_pair("url", python::Type::STRING),
                                                                     make_pair("message", python::Type::STRING)}, false);

    auto column_names = std::vector<std::string>{"created_at",
                                                 "type",
                                                 "payload",
                                                 "repository",
                                                 "repo"};

    auto payload_entries = std::vector<python::StructEntry>{python::StructEntry("'commits'", python::Type::STRING, python::Type::makeListType(commits_struct_type), true),
                                                            python::StructEntry("'target'", python::Type::STRING, python::Type::makeStructuredDictType({make_pair("id", python::Type::I64)}, true)),
                                                            python::StructEntry("'id'", python::Type::STRING, python::Type::I64, false)};

    auto column_types = std::vector<python::Type>{python::Type::STRING,
                                                  python::Type::STRING,
                                                  python::Type::makeStructuredDictType(payload_entries, true),
                                                  python::Type::makeOptionType(python::Type::makeStructuredDictType({make_pair("id", python::Type::I64)}, true)),
                                                  python::Type::makeOptionType(python::Type::makeStructuredDictType({make_pair("id", python::Type::I64)}, true))
    };



    auto sparse_row_type = python::Type::makeRowType(column_types, column_names);

    codegen::LLVMEnvironment env;
    JITCompiler jit;

    auto runtime_path = ContextOptions::defaults().RUNTIME_LIBRARY().toPath();
    auto rc_runtime = runtime::init(runtime_path);
    ASSERT_TRUE(rc_runtime);


    string parse_func_name = "parse_json_test";
    auto row_type = sparse_row_type.get_columns_as_tuple_type();
    auto columns = sparse_row_type.get_column_names();
    auto F_parse = codegen::json_generateParseStringFunction(env, parse_func_name, row_type, columns);

    // create LLVM wrapper function to call string upon & use for output serialization
    string func_name = "test_dummy";
    {
        using namespace llvm;
        auto func = codegen::getOrInsertFunction(env.getModule().get(), func_name, env.i64Type(),
                                        env.i8ptrType(), env.i64Type(), env.i8ptrType()->getPointerTo(), env.i64ptrType());
        func_name = func->getName().str();
        auto bbBody = BasicBlock::Create(env.getContext(), "body", func);
        codegen::IRBuilder builder(bbBody);

        auto args = codegen::mapLLVMFunctionArgs(func, {"str", "str_size", "out_ptr", "out_ptr_size"});

        env.printValue(builder, args["str"], "input str: ");
        env.printValue(builder, args["str_size"], "input str size: ");

        codegen::FlattenedTuple ft(&env);
        ft.init(row_type);
        auto var = env.CreateFirstBlockAlloca(builder, ft.getLLVMType());
        auto rc = builder.CreateCall(F_parse, {var, args["str"], args["str_size"]});

        env.printValue(builder, rc, "rc of json parse is: ");

        ft = codegen::FlattenedTuple::fromLLVMStructVal(&env, builder, var, row_type);
        auto serialized_size = ft.getSize(builder);

        auto ptr = env.cmalloc(builder, serialized_size);
        builder.CreateStore(serialized_size, args["out_ptr_size"]);
        ft.serialize(builder, ptr);
        builder.CreateStore(ptr, args["out_ptr"]);

        builder.CreateRet(serialized_size);
    }

    codegen::annotateModuleWithInstructionPrint(*env.getModule().get());

    auto ir = env.getIR();

    // compile
    ASSERT_TRUE(jit.compile(ir));

    python::initInterpreter();

    // get function
    auto foo = reinterpret_cast<int64_t(*)(const char*, int64_t, uint8_t**,int64_t*)>(jit.getAddrOfSymbol(func_name));

    uint8_t* out_ptr = nullptr;
    int64_t out_size = 0;
    int64_t serialized_size = foo(test_data, strlen(test_data) + 1, &out_ptr, &out_size);

    EXPECT_GT(serialized_size, 0);

    if(out_ptr)
        free(out_ptr);

    python::closeInterpreter();

    cout<<"Hello world"<<endl;
}