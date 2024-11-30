//
// Created by leonhards on 10/22/24.
//

#include <gtest/gtest.h>

#include "S3File.h"
#include "Pipe.h"
#include "jit/RuntimeInterface.h"
#include <ContextOptions.h>
#include <Timer.h>

#include "helper.h"
#include "ee/aws/LambdaWorkerApp.h"
#include "spdlog/sinks/ostream_sink.h"
#include <ee/worker/WorkerBackend.h>
#include <ee/aws/AWSLambdaBackend.h>

// tuplex files
#include <UDF.h>
#include <Context.h>

using namespace tuplex;

class LocalWorkerFixture : public ::testing::Test {
public:
    static std::string yaml_path;
    static std::string lambda_endpoint;
protected:

    std::stringstream logStream;
    std::string testName;
    std::string scratchDir;

    inline std::unordered_map<std::string, std::string> shared_experiment_settings() {
        std::unordered_map<std::string, std::string> m;
        m["tuplex.sample.strataSize"] = "1024";
        m["tuplex.sample.samplesPerStrata"] = "1";
        m["tuplex.sample.maxDetectionMemory"] = "32MB";
        m["tuplex.sample.maxDetectionRows"] = "30000";

        m["tuplex.backend"] = "worker";
        m["tuplex.aws.scratchDir"] = scratchDir;

        m["tuplex.webui.enable"] = "False";
        m["tuplex.driverMemory"] = "2G";
        m["tuplex.partitionSize"] = "32MB";
        m["tuplex.runTimeMemory"] = "32MB";
        m["tuplex.useLLVMOptimizer"] = "True";
        m["tuplex.optimizer.generateParser"] = "False";
        m["tuplex.optimizer.nullValueOptimization"] = "True";
        m["tuplex.optimizer.constantFoldingOptimization"] = "True";
        m["tuplex.optimizer.selectionPushdown"] = "True";
        m["tuplex.experimental.forceBadParseExceptFormat"] = "False";
        m["tuplex.resolveWithInterpreterOnly"] = "False";
        m["tuplex.experimental.opportuneCompilation"] =  "false";

        auto sampling_mode =
                SamplingMode::FIRST_ROWS | SamplingMode::LAST_ROWS | SamplingMode::FIRST_FILE |
                SamplingMode::LAST_FILE;
        m["sampling_mode"] = std::to_string(sampling_mode);

        // this allows large files to be processed without splitting.
        m["tuplex.inputSplitSize"] = "500G"; // set extreme large so there's no split.
        m["tuplex.experimental.worker.workerBufferSize"] = "12G"; // each normal, exception buffer in worker get 3G before they start spilling to disk

        return m;
    }

    inline Context create_worker_context(const std::unordered_map<std::string, std::string>& conf_override={},
                                         const std::unordered_map<std::string, std::string>& env_override={}) {

        // Create a mini test context with Lambda backend for Tuplex.
        auto co = ContextOptions::defaults();
        co.set("tuplex.backend", "worker");
        auto exp_settings = shared_experiment_settings();
        for (const auto &kv: exp_settings)
            if (startsWith(kv.first, "tuplex."))
                co.set(kv.first, kv.second);

        // Overwrite settings with conf_override.
        for(const auto& kv : conf_override) {
            co.set(kv.first, kv.second);
        }

        Context ctx(co);
        runtime::init(co.RUNTIME_LIBRARY().toPath());

        // Adjust S3 endpoint in Lambda by sending over environment as in local S3 tests.
        assert(ctx.backend());
        auto wb = dynamic_cast<WorkerBackend*>(ctx.backend());
        assert(wb);

        std::unordered_map<std::string, std::string> env;

        for(const auto& kv : env_override)
            env[kv.first] = kv.second;

        wb->setEnvironment(env);

        return std::move(ctx);
    }

    void SetUp() override {
        testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) +
                   std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
        auto user = tuplex::getUserName();
        if (user.empty()) {
            std::cerr << "could not retrieve user name, setting to user" << std::endl;
            user = "user";
        }
        scratchDir = "/tmp/" + user + "/" + testName;

        // reset global static variables, i.e. whether to use UDF compilation or not!
        tuplex::UDF::enableCompilation();

        // init logger to write to both stream as well as stdout
        // ==> searching the stream can be used to validate err Messages
        Logger::init({std::make_shared<spdlog::sinks::ansicolor_stdout_sink_mt>(),
                      std::make_shared<spdlog::sinks::ostream_sink_mt>(logStream)});

    }

    static void SetUpTestSuite() {
        using namespace tuplex;
        using namespace std;

        cout<<"Initializing interpreter and releasing GIL"<<endl;
        python::initInterpreter();
        python::unlockGIL();
    }

    static void TearDownTestSuite() {
        using namespace std;
        using namespace tuplex;

        if(python::isInterpreterRunning()) {
            cout<<"Shutting down interpreter."<<endl;
            python::lockGIL();
            python::closeInterpreter();
        }

        cout<<"Shutting down AWSSDK."<<endl;
        shutdownAWS();
    }
};

TEST_F(LocalWorkerFixture, GithubGenericDictHyper) {
    using namespace std;
    using namespace tuplex;

    // Test over github daily with generic hyper dicts.
    auto input_pattern = "/hot/data/github_daily/*.json";
    auto output_path = "./local-exp/" + testName + "/";
    // remove output files if they exist
    cout << "Removing files (if they exist) from " << output_path << endl;
    boost::filesystem::remove_all(output_path.c_str());

    std::unordered_map<std::string, std::string> conf;

    // disable sparse structs, enable generic dicts
    conf["tuplex.optimizer.sparsifyStructs"] = "false";
    conf["tuplex.experimental.useGenericDicts"] = "true";
    conf["tuplex.experimental.hyperspecialization"] = "true";

    auto ctx = create_worker_context(conf);

    cout<<"Running github pipeline."<<endl;

    github_pipeline(ctx, input_pattern, output_path);

    cout<<"Pipeline done."<<endl;

    // Load stats result from json:
    auto job_path = URI("worker_app_job.json");
    auto job_data = fileToString(job_path);
    ASSERT_FALSE(job_data.empty());


    // check results (from python reference number, add up total line count)
    cout << "Analyzing result: " << endl;
    auto output_uris = glob(output_path + "*.csv");
    cout << "Found " << pluralize(output_uris.size(), "output file") << endl;
    size_t total_row_count = 0;
    for (auto path: output_uris) {
        auto row_count = csv_row_count(path);
        cout << "-- file " << path << ": " << pluralize(row_count, "row") << endl;
        total_row_count += row_count;
    }
    EXPECT_EQ(total_row_count, 294195);

    // There should be two (!) output schemas.

    //QueryConfiguration{"benchmark", , true, true, false, 294195}, // <-- hyper w. generic dicts.
}

TEST_F(LocalWorkerFixture, GithubSparseStructHyper) {
    using namespace std;
    using namespace tuplex;

    // Test over github daily with generic hyper dicts.
    auto input_pattern = "/hot/data/github_daily/*.json";
    auto output_path = "./local-exp/" + testName + "/";
    // remove output files if they exist
    cout << "Removing files (if they exist) from " << output_path << endl;
    boost::filesystem::remove_all(output_path.c_str());

    std::unordered_map<std::string, std::string> conf;

    // enable sparse structs, disable generic dicts
    conf["tuplex.optimizer.sparsifyStructs"] = "true";
    conf["tuplex.experimental.useGenericDicts"] = "false";
    conf["tuplex.experimental.hyperspecialization"] = "true";

    auto ctx = create_worker_context(conf);

    cout<<"Running github pipeline."<<endl;

    github_pipeline(ctx, input_pattern, output_path);

    cout<<"Pipeline done."<<endl;

    // Load stats result from json:
    auto job_path = URI("worker_app_job.json");
    auto job_data = fileToString(job_path);
    ASSERT_FALSE(job_data.empty());


    // check results (from python reference number, add up total line count)
    cout << "Analyzing result: " << endl;
    auto output_uris = glob(output_path + "*.csv");
    cout << "Found " << pluralize(output_uris.size(), "output file") << endl;
    size_t total_row_count = 0;
    for (auto path: output_uris) {
        auto row_count = csv_row_count(path);
        cout << "-- file " << path << ": " << pluralize(row_count, "row") << endl;
        total_row_count += row_count;
    }
    EXPECT_EQ(total_row_count, 294195);

    // There should be two (!) output schemas.

    //QueryConfiguration{"benchmark", , true, true, false, 294195}, // <-- hyper w. generic dicts.
}