//
// Created by leonhards on 5/8/24.
//

#include "TestUtils.h"

namespace tuplex {
    struct QueryConfiguration {
        std::string name; // <-- specify unique test name here.
        std::string input_pattern;

        // settings for experiment
        bool use_hyper;
        bool use_llvm_optimizer;

        // ref variable
        size_t expected_result_row_count;
    };


    // 31 + 19 + 54 + 56 + 63 + 41 + 32 + 16 + 27 + 25 + 25 - 11 = 378 <-- how many rows result for *.json.sample has.

    std::vector<QueryConfiguration> g_configurations_to_test({QueryConfiguration{"small_sample", "../resources/hyperspecialization/github_daily/*.json.sample", false, false, 378},
                                                                QueryConfiguration{"small_sample", "../resources/hyperspecialization/github_daily/*.json.sample", false, true, 378},
                                                                QueryConfiguration{"small_sample", "../resources/hyperspecialization/github_daily/*.json.sample", true, false, 378},
                                                                QueryConfiguration{"small_sample", "../resources/hyperspecialization/github_daily/*.json.sample", true, true, 378},
                                                                QueryConfiguration{"benchmark", "/hot/data/github_daily/", false, true, 294195},
                                                                QueryConfiguration{"benchmark", "/hot/data/github_daily/", true, true, 294195}});

// see https://github.com/google/googletest/blob/main/docs/advanced.md#specifying-names-for-value-parameterized-test-parameters

    class NextConfFullTestSuite : public ::testing::TestWithParam<QueryConfiguration> {

    protected:
        std::stringstream logStream;
        std::string testName;
        std::string scratchDir;

        inline void remove_temp_files() {
            tuplex::Timer timer;
            boost::filesystem::remove_all(scratchDir.c_str());
            std::cout<<"removed temp files in "<<timer.time()<<"s"<<std::endl;
        }

        void SetUp() override {
            testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
            auto user = tuplex::getUserName();
            if(user.empty()) {
                std::cerr<<"could not retrieve user name, setting to user"<<std::endl;
                user = "user";
            }
            scratchDir = "/tmp/" + user + "/" + testName;

            // reset global static variables, i.e. whether to use UDF compilation or not!
            tuplex::UDF::enableCompilation();

            // init logger to write to both stream as well as stdout
            // ==> searching the stream can be used to validate err Messages
            Logger::init({std::make_shared<spdlog::sinks::ansicolor_stdout_sink_mt>(),
                          std::make_shared<spdlog::sinks::ostream_sink_mt>(logStream)});

            python::initInterpreter();
            // release GIL
            python::unlockGIL();
        }

        void TearDown() override {
            python::lockGIL();
            // important to get GIL for this
            python::closeInterpreter();

            // release runtime memory
            tuplex::runtime::releaseRunTimeMemory();

            // remove all loggers ==> note: this crashed because of multiple threads not being done yet...
            // call only AFTER all threads/threadpool is terminated from Context/LocalBackend/LocalEngine...
            Logger::instance().reset();

            tuplex::UDF::enableCompilation(); // reset
        }

        ~NextConfFullTestSuite() {
            remove_temp_files();
        }

        [[maybe_unused]] std::unordered_map<std::string, std::string> contextTestSettings() {
            std::unordered_map<std::string, std::string> m;

            // use stratified sampling
            m["tuplex.aws.httpThreadCount"] = std::to_string(410);
            m["tuplex.aws.maxConcurrency"] = std::to_string(410);
            m["tuplex.aws.lambdaMemory"] = "10000";
            m["tuplex.aws.lambdaThreads"] = "3";

            m["tuplex.autoUpcast"] = "True";
            m["tuplex.executorCount"] = std::to_string(0);
            m["tuplex.backend"] = "lambda";
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
            m["tuplex.experimental.opportuneCompilation"] = "True";
            m["tuplex.aws.scratchDir"] = "s3://tuplex-leonhard/scratch/github-exp";

            // for github use smaller split size? --> need to test/check!
            m["tuplex.inputSplitSize"] = "64MB"; // tiny tasks?

            // sampling settings incl.
            // stratified sampling (to make things work & faster)
            m["tuplex.sample.strataSize"] = "1024";
            m["tuplex.sample.samplesPerStrata"] = "10";
            m["tuplex.sample.maxDetectionMemory"] = "32MB";
            m["tuplex.sample.maxDetectionRows"] = "30000";

            m["tuplex.backend"] = "worker";
            m["tuplex.aws.scratchDir"] = scratchDir;

            auto sampling_mode =
                    SamplingMode::FIRST_ROWS | SamplingMode::LAST_ROWS | SamplingMode::FIRST_FILE | SamplingMode::LAST_FILE;
            m["sampling_mode"] = std::to_string(sampling_mode);

            return m;
        }

    };


    size_t csv_row_count(const std::string& path) {
        // parse CSV from path, and count rows.
        csvmonkey::MappedFileCursor stream;
        csvmonkey::CsvReader<csvmonkey::MappedFileCursor> reader(stream);

        stream.open(path.c_str());
        csvmonkey::CsvCursor &row = reader.row();
        if(! reader.read_row()) {
            throw std::runtime_error("Cannot read header row");
        }

        size_t row_count = 0;
        while(reader.read_row()) {
            row_count++;
        }
        return row_count;
    }

    TEST_P(NextConfFullTestSuite, Github) {
        using namespace tuplex;
        using namespace std;

        auto test_conf = GetParam();
        auto it = std::find_if(g_configurations_to_test.begin(), g_configurations_to_test.end(), [&](const QueryConfiguration& conf) { return conf.name == test_conf.name; });
        ASSERT_NE(it, g_configurations_to_test.end());
        auto id = std::distance(it, g_configurations_to_test.begin());
        auto output_path = "./local-exp/" + testName + "/" + test_conf.name + "/" + std::to_string(id) + "/";
        auto input_pattern = test_conf.input_pattern;
        std::cout << "Performing test: " << test_conf.name << " with output path: "<<output_path<<std::endl;

        // check that test files exist, else skip.
        auto file_uris = glob(input_pattern);
        if(file_uris.empty()) {
            GTEST_SKIP() << "Did not find any files for pattern " + input_pattern + " skipping test.";
        }

        // set input/output paths
        // auto exp_settings = lambdaSettings(true);
        auto exp_settings = contextTestSettings();
        exp_settings["tuplex.experimental.hyperspecialization"] = boolToString(test_conf.use_hyper);
        exp_settings["tuplex.useLLVMOptimizer"] = boolToString(test_conf.use_llvm_optimizer);
        SamplingMode sm = static_cast<SamplingMode>(stoi(exp_settings["sampling_mode"]));
        sm = sm | SamplingMode::SINGLETHREADED;
        ContextOptions co = ContextOptions::defaults();
        for(const auto& kv : exp_settings)
            if(startsWith(kv.first, "tuplex."))
                co.set(kv.first, kv.second);

        // this allows large files to be processed without splitting.
        co.set("tuplex.inputSplitSize", "20G");
        co.set("tuplex.experimental.worker.workerBufferSize", "12G"); // each normal, exception buffer in worker get 3G before they start spilling to disk!

        // create context according to settings
        Context ctx(co);
        runtime::init(co.RUNTIME_LIBRARY().toPath());

        // start pipeline incl. output
        auto repo_id_code = "def extract_repo_id(row):\n"
                            "    if 2012 <= row['year'] <= 2014:\n"
                            "        \n"
                            "        if row['type'] == 'FollowEvent':\n"
                            "            return row['payload']['target']['id']\n"
                            "        \n"
                            "        if row['type'] == 'GistEvent':\n"
                            "            return row['payload']['id']\n"
                            "        \n"
                            "        repo = row.get('repository')\n"
                            "        \n"
                            "        if repo is None:\n"
                            "            return None\n"
                            "        return repo.get('id')\n"
                            "    else:\n"
                            "        return row['repo'].get('id')";

        // remove output files if they exist
        cout<<"Removing files (if they exist) from "<<output_path<<endl;
        boost::filesystem::remove_all(output_path.c_str());

        ctx.json(input_pattern, true, true, sm)
                .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
                .withColumn("repo_id", UDF(repo_id_code))
                .filter(UDF("lambda x: x['type'] == 'ForkEvent'")) // <-- this is challenging to push down.
                .withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
                .withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
                .selectColumns(vector<string>{"type", "repo_id", "year", "number_of_commits"})
                .tocsv(output_path);

        // check results (from python reference number, add up total line count)
        cout<<"Analyzing result: "<<endl;
        auto output_uris = glob(output_path + "*.csv");
        cout<<"Found "<<pluralize(output_uris.size(), "output file")<<endl;
        size_t total_row_count = 0;
        for(auto path : output_uris) {
            auto row_count = csv_row_count(path);
            cout<<"-- file "<<path<<": "<<pluralize(row_count, "row")<<endl;
            total_row_count += row_count;
        }
        EXPECT_EQ(total_row_count, test_conf.expected_result_row_count);
    }

    INSTANTIATE_TEST_SUITE_P(AllQueries, NextConfFullTestSuite, testing::ValuesIn(g_configurations_to_test),
                             [](const testing::TestParamInfo<NextConfFullTestSuite::ParamType>& info) {
                                 // Can use info.param here to generate the test suffix
                                 auto param = info.param;

                                 std::stringstream ss;
                                 ss<<param.name;
                                 if(param.use_hyper)
                                     ss<<"_hyper";
                                 else
                                     ss<<"_no_hyper";
                                 if(param.use_llvm_optimizer)
                                     ss<<"_llvm_opt";
                                 else
                                     ss<<"_no_llvm_opt";
                                 return ss.str();
                             });
}
