//
// Created by leonhards on 6/17/24.
//

#include "TestUtils.h"
#include "JsonStatistic.h"
#include "physical/codegen/JsonSourceTaskBuilder.h"
#include <codegen/CodegenHelper.h>

namespace tuplex {
    python::Type github_sparse_row_type() {
        using namespace std;

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

        auto payload_entries = std::vector<python::StructEntry>{python::StructEntry("'commits'", python::Type::STRING, python::Type::makeListType(commits_struct_type), false),
                                                                python::StructEntry("'target'", python::Type::STRING, python::Type::makeStructuredDictType({make_pair("id", python::Type::I64)}, true), false),
                                                                python::StructEntry("'id'", python::Type::STRING, python::Type::I64, false)};

        auto column_types = std::vector<python::Type>{python::Type::STRING,
                                                      python::Type::STRING,
                                                      python::Type::makeOptionType(python::Type::makeStructuredDictType(payload_entries, true)), // <-- PublicEvents have no payload.
                                                      python::Type::makeOptionType(python::Type::makeStructuredDictType(std::vector<python::StructEntry>{python::StructEntry("'id'", python::Type::STRING, python::Type::I64, false)}, true)),
                                                      python::Type::makeOptionType(python::Type::makeStructuredDictType(std::vector<python::StructEntry>{python::StructEntry("'id'", python::Type::STRING, python::Type::I64, false)}, true)) // GistEvent have no repo id (instead a url)
        };


        auto sparse_row_type = python::Type::makeRowType(column_types, column_names);

        return sparse_row_type;
    }


    std::string generate_line_parse_function(codegen::LLVMEnvironment& env, const python::Type& sparse_row_type) {
        using namespace std;

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

            // env.printValue(builder, args["str"], "input str: ");
            // env.printValue(builder, args["str_size"], "input str size: ");

            codegen::FlattenedTuple ft(&env);
            ft.init(row_type);
            auto var = env.CreateFirstBlockAlloca(builder, ft.getLLVMType());
            auto rc = builder.CreateCall(F_parse, {var, args["str"], args["str_size"]});

            // env.printValue(builder, rc, "rc of json parse is: ");

            // only deserialize if rc is 0
            auto rc_ok = builder.CreateICmpEQ(rc, env.i64Const(0));
            auto& ctx = builder.getContext();
            auto bbOK = BasicBlock::Create(ctx, "parse_ok", func);
            auto bbFailure = BasicBlock::Create(ctx, "parse_failed", func);
            builder.CreateCondBr(rc_ok, bbOK, bbFailure);
            builder.SetInsertPoint(bbFailure);
            builder.CreateStore(env.i64Const(0), args["out_ptr_size"]);
            builder.CreateRet(builder.CreateMul(env.i64Const(-1), rc));

            builder.SetInsertPoint(bbOK);
            ft = codegen::FlattenedTuple::fromLLVMStructVal(&env, builder, var, row_type);
            auto serialized_size = ft.getSize(builder);

            auto ptr = env.cmalloc(builder, serialized_size);

            builder.CreateStore(serialized_size, args["out_ptr_size"]);
            //ft.serialize(builder, ptr);
            builder.CreateStore(ptr, args["out_ptr"]);

            builder.CreateRet(serialized_size);
        }

        return func_name;
    }
}


TEST(JsonSparseParse, SingleStringJsonParse) {
    using namespace tuplex;
    using namespace std;

    auto& logger = Logger::instance().logger("");
    logger.info("test");

//    Logger::instance().reset();
//    // init logger to only act with stdout sink
//    Logger::instance().init({std::make_shared<spdlog::sinks::ansicolor_stdout_sink_mt>()});
    Logger::instance().defaultLogger().info("global_init(): logging system initialized");

    string test_data = "{\"type\":\"PushEvent\",\"public\":true,\"actor\":{\"gravatar_id\":\"19b0daa323b6af55864f706d8e25100e\",\"url\":\"https://api.github.com/users/MartinJKurz\",\"avatar_url\":\"https://secure.gravatar.com/avatar/19b0daa323b6af55864f706d8e25100e?d=%2Fimages%2Fgravatars%2Fgravatar-user-420.png\",\"id\":898159,\"login\":\"MartinJKurz\"},\"created_at\":\"2011-10-15T00:00:00Z\",\"payload\":{\"head\":\"4d8e6f13daec514ea2e075afeeb3d021afb7473e\",\"size\":1,\"push_id\":46262531,\"commits\":[{\"sha\":\"4d8e6f13daec514ea2e075afeeb3d021afb7473e\",\"author\":{\"name\":\"Martin J. Kurz\",\"email\":\"e7b16791bb505733357139208fac489d81c7842a@googlemail.com\"},\"url\":\"https://api.github.com/repos/MartinJKurz/invaders/commits/4d8e6f13daec514ea2e075afeeb3d021afb7473e\",\"message\":\"debug output, minor changes 2\"}],\"ref\":\"refs/heads/master\",\"legacy\":{\"head\":\"4d8e6f13daec514ea2e075afeeb3d021afb7473e\",\"size\":1,\"push_id\":46262531,\"shas\":[[\"4d8e6f13daec514ea2e075afeeb3d021afb7473e\",\"martin.j.kurz@googlemail.com\",\"debug output, minor changes 2\",\"Martin J. Kurz\"]],\"ref\":\"refs/heads/master\"}},\"id\":\"1492634118\",\"repo\":{\"url\":\"https://api.github.com/repos/MartinJKurz/invaders\",\"id\":2355143,\"name\":\"MartinJKurz/invaders\"}}";

    // {
    //  "type": "PushEvent",
    //  "public": true,
    //  "actor": {
    //    "gravatar_id": "19b0daa323b6af55864f706d8e25100e",
    //    "url": "https://api.github.com/users/MartinJKurz",
    //    "avatar_url": "https://secure.gravatar.com/avatar/19b0daa323b6af55864f706d8e25100e?d=%2Fimages%2Fgravatars%2Fgravatar-user-420.png",
    //    "id": 898159,
    //    "login": "MartinJKurz"
    //  },
    //  "created_at": "2011-10-15T00:00:00Z",
    //  "payload": {
    //    "head": "4d8e6f13daec514ea2e075afeeb3d021afb7473e",
    //    "size": 1,
    //    "push_id": 46262531,
    //    "commits": [
    //      {
    //        "sha": "4d8e6f13daec514ea2e075afeeb3d021afb7473e",
    //        "author": {
    //          "name": "Martin J. Kurz",
    //          "email": "e7b16791bb505733357139208fac489d81c7842a@googlemail.com"
    //        },
    //        "url": "https://api.github.com/repos/MartinJKurz/invaders/commits/4d8e6f13daec514ea2e075afeeb3d021afb7473e",
    //        "message": "debug output, minor changes 2"
    //      }
    //    ],
    //    "ref": "refs/heads/master",
    //    "legacy": {
    //      "head": "4d8e6f13daec514ea2e075afeeb3d021afb7473e",
    //      "size": 1,
    //      "push_id": 46262531,
    //      "shas": [
    //        [
    //          "4d8e6f13daec514ea2e075afeeb3d021afb7473e",
    //          "martin.j.kurz@googlemail.com",
    //          "debug output, minor changes 2",
    //          "Martin J. Kurz"
    //        ]
    //      ],
    //      "ref": "refs/heads/master"
    //    }
    //  },
    //  "id": "1492634118",
    //  "repo": {
    //    "url": "https://api.github.com/repos/MartinJKurz/invaders",
    //    "id": 2355143,
    //    "name": "MartinJKurz/invaders"
    //  }
    //}

    // Other test data (WatchEvent), doesn't even have any entries:
    test_data = "{\"type\":\"WatchEvent\",\"public\":true,\"actor\":{\"gravatar_id\":\"3e9b0b47a98cdc664bcf58fa3ff5a758\",\"url\":\"https://api.github.com/users/chrismiles\",\"avatar_url\":\"https://secure.gravatar.com/avatar/3e9b0b47a98cdc664bcf58fa3ff5a758?d=%2Fimages%2Fgravatars%2Fgravatar-user-420.png\",\"id\":110112,\"login\":\"chrismiles\"},\"created_at\":\"2011-10-15T00:22:57Z\",\"payload\":{\"action\":\"started\"},\"id\":\"1492636479\",\"repo\":{\"url\":\"https://api.github.com/repos/robc/ShooterDemo\",\"id\":2561931,\"name\":\"robc/ShooterDemo\"}}";

    test_data = "{\"type\":\"GistEvent\",\"public\":true,\"actor\":{\"gravatar_id\":\"e2e776d3d6fbc6a6b7e4c692bd080595\",\"url\":\"https://api.github.com/users/aheckmann\",\"avatar_url\":\"https://secure.gravatar.com/avatar/e2e776d3d6fbc6a6b7e4c692bd080595?d=%2Fimages%2Fgravatars%2Fgravatar-user-420.png\",\"id\":166834,\"login\":\"aheckmann\"},\"created_at\":\"2011-10-15T00:00:17Z\",\"payload\":{\"action\":\"create\",\"gist\":{\"created_at\":\"2011-10-15T00:00:16Z\",\"comments\":0,\"public\":true,\"files\":{},\"updated_at\":\"2011-10-15T00:00:16Z\",\"git_push_url\":\"git@gist.github.com:1288718.git\",\"url\":\"https://api.github.com/gists/1288718\",\"id\":\"1288718\",\"git_pull_url\":\"git://gist.github.com/1288718.git\",\"description\":null,\"user\":{\"gravatar_id\":\"e2e776d3d6fbc6a6b7e4c692bd080595\",\"avatar_url\":\"https://secure.gravatar.com/avatar/e2e776d3d6fbc6a6b7e4c692bd080595?d=https://a248.e.akamai.net/assets.github.com%2Fimages%2Fgravatars%2Fgravatar-140.png\",\"url\":\"https://api.github.com/users/aheckmann\",\"id\":166834,\"login\":\"aheckmann\"},\"html_url\":\"https://gist.github.com/1288718\"},\"legacy\":{\"name\":\"gist: 1288718\",\"action\":\"create\",\"url\":\"https://gist.github.com/1288718\",\"id\":1288718,\"desc\":null}},\"id\":\"1492634145\",\"repo\":{\"url\":\"https://api.github.com/repos//\",\"name\":\"/\"}}";

    test_data = "{\"created_at\":\"2013-10-15T00:03:17-07:00\",\"public\":true,\"type\":\"PublicEvent\",\"url\":\"https://github.com/BillyRen/Mayo_Project\",\"actor\":\"BillyRen\",\"actor_attributes\":{\"login\":\"BillyRen\",\"type\":\"User\",\"gravatar_id\":\"05d99abfd12b8c99a8d1e046c9dbade1\",\"name\":\"Chengbin(Billy) REN\",\"company\":\"Zhejiang University, China\",\"blog\":\"http://renchengbin.lofter.com\",\"location\":\"Hangzhou, China\",\"email\":\"e457824463f87b21e63dc32fe4f4ba457325a935@gmail.com\"},\"repository\":{\"id\":11428319,\"name\":\"Mayo_Project\",\"url\":\"https://github.com/BillyRen/Mayo_Project\",\"description\":\"\",\"watchers\":0,\"stargazers\":0,\"forks\":0,\"fork\":false,\"size\":398,\"owner\":\"BillyRen\",\"private\":false,\"open_issues\":0,\"has_issues\":true,\"has_downloads\":true,\"has_wiki\":true,\"created_at\":\"2013-07-15T09:51:23-07:00\",\"pushed_at\":\"2013-08-07T05:54:21-07:00\",\"master_branch\":\"master\"}}";

    test_data = "{\"type\":\"GistEvent\",\"public\":true,\"actor\":{\"url\":\"https://api.github.com/users/\",\"avatar_url\":\"https://secure.gravatar.com/avatar/?d=%2Fimages%2Fgravatars%2Fgravatar-user-420.png\"},\"created_at\":\"2011-10-15T00:01:54Z\",\"payload\":{\"action\":\"update\",\"gist\":{\"created_at\":\"2011-10-14T22:36:47Z\",\"comments\":0,\"public\":true,\"files\":{},\"updated_at\":\"2011-10-14T22:36:47Z\",\"git_push_url\":\"git@gist.github.com:1288558.git\",\"url\":\"https://api.github.com/gists/1288558\",\"id\":\"1288558\",\"git_pull_url\":\"git://gist.github.com/1288558.git\",\"description\":\"\",\"user\":null,\"html_url\":\"https://gist.github.com/1288558\"},\"legacy\":{\"name\":\"gist: 1288558\",\"action\":\"update\",\"url\":\"https://gist.github.com/1288558\",\"id\":1288558,\"desc\":\"\"}},\"id\":\"1492634320\",\"repo\":{\"url\":\"https://api.github.com/repos//\",\"name\":\"/\"}}";

    auto sparse_row_type = github_sparse_row_type();

    codegen::LLVMEnvironment env;
    JITCompiler jit;

    auto runtime_path = ContextOptions::defaults().RUNTIME_LIBRARY().toPath();
    auto rc_runtime = runtime::init(runtime_path);
    ASSERT_TRUE(rc_runtime);

    auto func_name = generate_line_parse_function(env, sparse_row_type);

    auto ir = env.getIR();

    llvm::LLVMContext context;
    auto mod = codegen::stringToModule(context, ir);
    // codegen::annotateModuleWithInstructionPrint(*mod);

    ir = codegen::moduleToString(*mod);

    // compile
    ASSERT_TRUE(jit.compile(ir));

    // get function
    auto foo = reinterpret_cast<int64_t(*)(const char*, int64_t, uint8_t**,int64_t*)>(jit.getAddrOfSymbol(func_name));

    uint8_t* out_ptr = nullptr;
    int64_t out_size = 0;
    int64_t serialized_size = foo(test_data.c_str(), strlen(test_data.c_str()) + 1, &out_ptr, &out_size);

    EXPECT_GT(serialized_size, 0); // make sure no exception happened.

    if(out_ptr)
        free(out_ptr);
}


size_t json_call_per_row(const std::string& path, std::function<bool(size_t row_number, const std::string& line)> f) {
    using namespace std;

    size_t row_count = 0;

    std::string line;
    std::ifstream ifs(path);
    while(std::getline(ifs, line)) {

        if(!f(row_count, line)) {
            std::cerr<<"Bad row: "<<row_count<<" in path "<<path<<std::endl;
            return row_count;
        }

        row_count++;
    }


    return row_count;
}

TEST(JsonSparseParse, ParseAllLines) {
    using namespace tuplex;
    using namespace std;

    auto& logger = Logger::instance().logger("test");
    logger.info("Starting per-line parsing test::");

    string input_pattern = "../resources/hyperspecialization/github_daily/*.json.sample";
    input_pattern = "../resources/hyperspecialization/github_daily/*.json.sample";

    // full data
    input_pattern = "/hot/data/github_daily/*.json";

    auto output_uris = glob(input_pattern);
    logger.info("Found " + pluralize(output_uris.size(), "input path") + " to process.");


    logger.info("Preparing LLVM jitted function.");

    auto sparse_row_type = github_sparse_row_type();

    codegen::LLVMEnvironment env;
    JITCompiler jit;

    auto runtime_path = ContextOptions::defaults().RUNTIME_LIBRARY().toPath();
    auto rc_runtime = runtime::init(runtime_path);
    ASSERT_TRUE(rc_runtime);

    auto func_name = generate_line_parse_function(env, sparse_row_type);

    auto ir = env.getIR();

    llvm::LLVMContext context;
    auto mod = codegen::stringToModule(context, ir);
    // codegen::annotateModuleWithInstructionPrint(*mod);

    ir = codegen::moduleToString(*mod);

    // compile
    ASSERT_TRUE(jit.compile(ir));

    // get function
    auto foo = reinterpret_cast<int64_t(*)(const char*, int64_t, uint8_t**,int64_t*)>(jit.getAddrOfSymbol(func_name));

    for(const auto& path : output_uris) {
        logger.info("Processing " + path);

        auto row_count = json_call_per_row(path, [foo](size_t row_number, const std::string& line) {

            // skip empty lines.
            if(line.empty())
                return true;

            std::cout<<"-- row "<<row_number<<std::endl;

            uint8_t* out_ptr = nullptr;
            int64_t out_size = 0;
            int64_t serialized_size = foo(line.c_str(), strlen(line.c_str()) + 1, &out_ptr, &out_size);
            if(out_ptr)
                free(out_ptr);

            if(serialized_size < 0) {
                std::cerr<<"Bad line ec="<<-serialized_size<<" ("<<row_number<<"): "<<line<<endl;
                return false;
            }

            assert(serialized_size >= 0);
            EXPECT_GE(serialized_size, 0); // make sure no exception happened.

            return true;
        });

        logger.info("Processed " + pluralize(row_count, "row"));
    }

}