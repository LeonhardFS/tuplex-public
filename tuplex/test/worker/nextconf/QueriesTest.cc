//
// Created by leonhards on 5/8/24.
//

#include "TestUtils.h"
#include "JsonStatistic.h"
#include "tracing/LambdaAccessedColumnVisitor.h"

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


    // TODO: stack smash happens (prob. decoding error) for general case exceptions type
    // (Option[str],Option[bool],Option[Struct[(str,'avatar_url'=>str),(str,'display_login'=>str),(str,'gravatar_id'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'url'=>str)]],Option[str],Option[Struct[(str,'action'=>str),(str,'before'=>str),(str,'comment'=>Struct[(str,'created_at'=>str),(str,'body'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'id'=>i64),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'id'=>i64),(str,'login'=>str)])]),(str,'commits'=>List[Struct[(str,'author'->Struct[(str,'name'->str),(str,'email'->str)]),(str,'distinct'=>bool),(str,'message'->str),(str,'sha'->str),(str,'url'->str)]]),(str,'description'=>Option[str]),(str,'distinct_size'=>i64),(str,'gist'=>Struct[(str,'created_at'=>str),(str,'comments'=>i64),(str,'public'=>bool),(str,'files'=>{}),(str,'updated_at'=>str),(str,'git_push_url'=>str),(str,'url'=>str),(str,'id'=>str),(str,'git_pull_url'=>str),(str,'description'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'id'=>i64),(str,'login'=>str)]),(str,'html_url'=>str)]),(str,'head'=>str),(str,'issue'=>Struct[(str,'number'=>i64),(str,'created_at'=>str),(str,'pull_request'=>Struct[(str,'diff_url'=>null),(str,'patch_url'=>null),(str,'html_url'=>null)]),(str,'body'=>str),(str,'comments'=>i64),(str,'title'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'id'=>i64),(str,'assignee'=>null),(str,'milestone'=>null),(str,'closed_at'=>null),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'id'=>i64),(str,'login'=>str)]),(str,'html_url'=>str),(str,'labels'=>List[Struct[(str,'name'->str),(str,'url'->str),(str,'color'->str)]]),(str,'state'=>str)]),(str,'legacy'=>Struct[(str,'action'=>str),(str,'comment_id'=>i64),(str,'desc'=>str),(str,'head'=>str),(str,'id'=>i64),(str,'issue_id'=>i64),(str,'name'=>str),(str,'push_id'=>i64),(str,'ref'=>str),(str,'shas'=>List[List[str]]),(str,'size'=>i64),(str,'url'=>str)]),(str,'master_branch'=>str),(str,'member'=>Struct[(str,'gravatar_id'=>str),(str,'avatar_url'=>str),(str,'url'=>str),(str,'id'=>i64),(str,'login'=>str)]),(str,'number'=>i64),(str,'pull_request'=>Struct[(str,'url'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'html_url'=>str),(str,'diff_url'=>str),(str,'patch_url'=>str),(str,'issue_url'=>str),(str,'number'=>i64),(str,'state'=>str),(str,'locked'=>bool),(str,'title'=>str),(str,'user'=>Struct[(str,'login'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'html_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'organizations_url'=>str),(str,'repos_url'=>str),(str,'events_url'=>str),(str,'received_events_url'=>str),(str,'type'=>str),(str,'site_admin'=>bool)]),(str,'body'=>str),(str,'created_at'=>str),(str,'updated_at'=>str),(str,'closed_at'=>null),(str,'merged_at'=>null),(str,'merge_commit_sha'=>null),(str,'assignee'=>Struct[(str,'login'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'html_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'organizations_url'=>str),(str,'repos_url'=>str),(str,'events_url'=>str),(str,'received_events_url'=>str),(str,'type'=>str),(str,'site_admin'=>bool)]),(str,'assignees'=>List[Struct[(str,'login'->str),(str,'id'->i64),(str,'node_id'->str),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->bool)]]),(str,'requested_reviewers'=>[]),(str,'requested_teams'=>[]),(str,'labels'=>[]),(str,'milestone'=>null),(str,'draft'=>bool),(str,'commits_url'=>str),(str,'review_comments_url'=>str),(str,'review_comment_url'=>str),(str,'comments_url'=>str),(str,'statuses_url'=>str),(str,'head'=>Struct[(str,'label'=>str),(str,'ref'=>str),(str,'sha'=>str),(str,'user'=>Struct[(str,'login'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'html_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'organizations_url'=>str),(str,'repos_url'=>str),(str,'events_url'=>str),(str,'received_events_url'=>str),(str,'type'=>str),(str,'site_admin'=>bool)]),(str,'repo'=>Struct[(str,'id'=>i64),(str,'node_id'=>str),(str,'name'=>str),(str,'full_name'=>str),(str,'private'=>bool),(str,'owner'=>Struct[(str,'login'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'html_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'organizations_url'=>str),(str,'repos_url'=>str),(str,'events_url'=>str),(str,'received_events_url'=>str),(str,'type'=>str),(str,'site_admin'=>bool)]),(str,'html_url'=>str),(str,'description'=>str),(str,'fork'=>bool),(str,'url'=>str),(str,'forks_url'=>str),(str,'keys_url'=>str),(str,'collaborators_url'=>str),(str,'teams_url'=>str),(str,'hooks_url'=>str),(str,'issue_events_url'=>str),(str,'events_url'=>str),(str,'assignees_url'=>str),(str,'branches_url'=>str),(str,'tags_url'=>str),(str,'blobs_url'=>str),(str,'git_tags_url'=>str),(str,'git_refs_url'=>str),(str,'trees_url'=>str),(str,'statuses_url'=>str),(str,'languages_url'=>str),(str,'stargazers_url'=>str),(str,'contributors_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'commits_url'=>str),(str,'git_commits_url'=>str),(str,'comments_url'=>str),(str,'issue_comment_url'=>str),(str,'contents_url'=>str),(str,'compare_url'=>str),(str,'merges_url'=>str),(str,'archive_url'=>str),(str,'downloads_url'=>str),(str,'issues_url'=>str),(str,'pulls_url'=>str),(str,'milestones_url'=>str),(str,'notifications_url'=>str),(str,'labels_url'=>str),(str,'releases_url'=>str),(str,'deployments_url'=>str),(str,'created_at'=>str),(str,'updated_at'=>str),(str,'pushed_at'=>str),(str,'git_url'=>str),(str,'ssh_url'=>str),(str,'clone_url'=>str),(str,'svn_url'=>str),(str,'homepage'=>null),(str,'size'=>i64),(str,'stargazers_count'=>i64),(str,'watchers_count'=>i64),(str,'language'=>str),(str,'has_issues'=>bool),(str,'has_projects'=>bool),(str,'has_downloads'=>bool),(str,'has_wiki'=>bool),(str,'has_pages'=>bool),(str,'forks_count'=>i64),(str,'mirror_url'=>null),(str,'archived'=>bool),(str,'disabled'=>bool),(str,'open_issues_count'=>i64),(str,'license'=>Struct[(str,'key'=>str),(str,'name'=>str),(str,'spdx_id'=>str),(str,'url'=>str),(str,'node_id'=>str)]),(str,'allow_forking'=>bool),(str,'is_template'=>bool),(str,'topics'=>[]),(str,'visibility'=>str),(str,'forks'=>i64),(str,'open_issues'=>i64),(str,'watchers'=>i64),(str,'default_branch'=>str)])]),(str,'base'=>Struct[(str,'label'=>str),(str,'ref'=>str),(str,'sha'=>str),(str,'user'=>Struct[(str,'login'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'html_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'organizations_url'=>str),(str,'repos_url'=>str),(str,'events_url'=>str),(str,'received_events_url'=>str),(str,'type'=>str),(str,'site_admin'=>bool)]),(str,'repo'=>Struct[(str,'id'=>i64),(str,'node_id'=>str),(str,'name'=>str),(str,'full_name'=>str),(str,'private'=>bool),(str,'owner'=>Struct[(str,'login'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'html_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'organizations_url'=>str),(str,'repos_url'=>str),(str,'events_url'=>str),(str,'received_events_url'=>str),(str,'type'=>str),(str,'site_admin'=>bool)]),(str,'html_url'=>str),(str,'description'=>str),(str,'fork'=>bool),(str,'url'=>str),(str,'forks_url'=>str),(str,'keys_url'=>str),(str,'collaborators_url'=>str),(str,'teams_url'=>str),(str,'hooks_url'=>str),(str,'issue_events_url'=>str),(str,'events_url'=>str),(str,'assignees_url'=>str),(str,'branches_url'=>str),(str,'tags_url'=>str),(str,'blobs_url'=>str),(str,'git_tags_url'=>str),(str,'git_refs_url'=>str),(str,'trees_url'=>str),(str,'statuses_url'=>str),(str,'languages_url'=>str),(str,'stargazers_url'=>str),(str,'contributors_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'commits_url'=>str),(str,'git_commits_url'=>str),(str,'comments_url'=>str),(str,'issue_comment_url'=>str),(str,'contents_url'=>str),(str,'compare_url'=>str),(str,'merges_url'=>str),(str,'archive_url'=>str),(str,'downloads_url'=>str),(str,'issues_url'=>str),(str,'pulls_url'=>str),(str,'milestones_url'=>str),(str,'notifications_url'=>str),(str,'labels_url'=>str),(str,'releases_url'=>str),(str,'deployments_url'=>str),(str,'created_at'=>str),(str,'updated_at'=>str),(str,'pushed_at'=>str),(str,'git_url'=>str),(str,'ssh_url'=>str),(str,'clone_url'=>str),(str,'svn_url'=>str),(str,'homepage'=>null),(str,'size'=>i64),(str,'stargazers_count'=>i64),(str,'watchers_count'=>i64),(str,'language'=>str),(str,'has_issues'=>bool),(str,'has_projects'=>bool),(str,'has_downloads'=>bool),(str,'has_wiki'=>bool),(str,'has_pages'=>bool),(str,'forks_count'=>i64),(str,'mirror_url'=>null),(str,'archived'=>bool),(str,'disabled'=>bool),(str,'open_issues_count'=>i64),(str,'license'=>Struct[(str,'key'=>str),(str,'name'=>str),(str,'spdx_id'=>str),(str,'url'=>str),(str,'node_id'=>str)]),(str,'allow_forking'=>bool),(str,'is_template'=>bool),(str,'topics'=>[]),(str,'visibility'=>str),(str,'forks'=>i64),(str,'open_issues'=>i64),(str,'watchers'=>i64),(str,'default_branch'=>str)])]),(str,'_links'=>Struct[(str,'self'=>Struct[(str,'href'=>str)]),(str,'html'=>Struct[(str,'href'=>str)]),(str,'issue'=>Struct[(str,'href'=>str)]),(str,'comments'=>Struct[(str,'href'=>str)]),(str,'review_comments'=>Struct[(str,'href'=>str)]),(str,'review_comment'=>Struct[(str,'href'=>str)]),(str,'commits'=>Struct[(str,'href'=>str)]),(str,'statuses'=>Struct[(str,'href'=>str)])]),(str,'author_association'=>str),(str,'auto_merge'=>null),(str,'active_lock_reason'=>null),(str,'merged'=>bool),(str,'mergeable'=>null),(str,'rebaseable'=>null),(str,'mergeable_state'=>str),(str,'merged_by'=>null),(str,'comments'=>i64),(str,'review_comments'=>i64),(str,'maintainer_can_modify'=>bool),(str,'commits'=>i64),(str,'additions'=>i64),(str,'deletions'=>i64),(str,'changed_files'=>i64)]),(str,'push_id'=>i64),(str,'pusher_type'=>str),(str,'ref'=>Option[str]),(str,'ref_type'=>str),(str,'size'=>i64)]],Option[str],Option[Struct[(str,'id'=>i64),(str,'name'=>str),(str,'url'=>str)]],Option[Struct[(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'id'->i64),(str,'login'->str),(str,'url'->str)]])
    // this happens for small_sample -> no_hyper / llvm_opt. --> could be an optimization gone wrong.
    // debug strategy:
    // --> find example within files which adheres to this exception type.
    // then, check whether this passes serialize/deserialize both manual and with codegen.

    // 31 + 19 + 54 + 56 + 63 + 41 + 32 + 16 + 27 + 25 + 25 - 11 = 378 <-- how many rows result for *.json.sample has.

    std::vector<QueryConfiguration> g_configurations_to_test({QueryConfiguration{"small_sample", "../resources/hyperspecialization/github_daily/*.json.sample", false, false, 378},
                                                                QueryConfiguration{"small_sample", "../resources/hyperspecialization/github_daily/*.json.sample", false, true, 378},
                                                                QueryConfiguration{"small_sample", "../resources/hyperspecialization/github_daily/*.json.sample", true, false, 378},
                                                                QueryConfiguration{"small_sample", "../resources/hyperspecialization/github_daily/*.json.sample", true, true, 378},
                                                                QueryConfiguration{"benchmark", "/hot/data/github_daily/*.json", false, true, 294195},
                                                                QueryConfiguration{"benchmark", "/hot/data/github_daily/*.json", true, true, 294195}});

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

    size_t csv_row_count_for_pattern(const std::string& pattern) {
        using namespace std;
        auto output_uris = glob(pattern);
        cout<<"Found "<<pluralize(output_uris.size(), "output file")<<endl;
        size_t total_row_count = 0;
        for(auto path : output_uris) {
            auto row_count = csv_row_count(path);
            cout<<"-- file "<<path<<": "<<pluralize(row_count, "row")<<endl;
            total_row_count += row_count;
        }
        return total_row_count;
    }

    TEST_P(NextConfFullTestSuite, Github) {
        using namespace tuplex;
        using namespace std;

        auto test_conf = GetParam();
        auto it = std::find_if(g_configurations_to_test.begin(), g_configurations_to_test.end(), [&](const QueryConfiguration& conf) { return conf.name == test_conf.name; });
        ASSERT_NE(it, g_configurations_to_test.end());
        auto id = std::abs(std::distance(it, g_configurations_to_test.begin()));
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
                            "        repo =  row.get('repo')\n"
                            "        if repo:\n"
                            "            return repo.get('id')\n"
                            "        else:\n"
                            "            return None\n";


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

    auto sorted_view_of_values(const std::unordered_map<python::Type, size_t>& values){
        std::vector<std::pair<python::Type, size_t>> view(values.begin(), values.end());
        std::sort(view.begin(), view.end(), [](const auto& lhs, const auto& rhs) {return lhs.second < rhs.second; });
        return view;
    }

    inline std::unordered_map<std::string, python::Type> row_type_to_column_hints(const python::Type& row_type) {
        assert(row_type.isRowType());

        std::unordered_map<std::string, python::Type> m;

        auto names = row_type.get_column_names();
        auto types = row_type.get_column_types();
        assert(names.size() == types.size());
        for(unsigned i = 0; i < names.size(); ++i)
            m[names[i]] = types[i];
        return m;
    }

    TEST(AllQueries, AllUniqueRowTypesFromSample) {
        using namespace std;

        Timer loadTimer;

        //string input_pattern = "../resources/hyperspecialization/github_daily/*.json.sample";

        // full data
        string input_pattern = "/hot/data/github_daily/*.json";

        // bad file:
        // input_pattern = "/hot/data/github_daily/2020-10-15.json";

//        // for faster dev
//        input_pattern = "../resources/hyperspecialization/github_daily/2011-10-15.json.sample";

        auto uris = glob(input_pattern);
        //EXPECT_EQ(uris.size(), 11);

        std::stringstream stats_stream;

        // parse all rows, and find unique row-types
        std::vector<Row> rows;
        cout<<"Found "<<pluralize(uris.size(), "file")<<endl;
        for(auto path : uris) {
            cout<<"-- parsing "<<path<<endl;
            Timer timer;
            auto data = fileToString(path);
            std::vector<std::vector<std::string>> out_column_names;
            auto part_rows = parseRowsFromJSONStratified(data.c_str(), data.size(), &out_column_names, true,
                                                         true, 99999999, 1, 1,
                                                         1, {}, false);

            cout<<"-- converting to row type (elapsed="<<timer.time()<<")"<<endl;
            for(unsigned i = 0; i < part_rows.size(); ++i) {
                auto row = part_rows[i];
                part_rows[i] = row.with_columns(out_column_names[i]);
            }

            // count per file and add to output:
            {
                std::unordered_map<python::Type, size_t> type_counts;
                for(const auto& row : part_rows) {
                    type_counts[row.getRowType()]++;
                }

                for(auto kv : type_counts) {
                    nlohmann::json j;
                    j["path"] = path;
                    j["type"] = kv.first.desc();
                    j["type_hash"] = kv.first.hash();
                    j["type_count"] = kv.second;
                    j["row_count"] = part_rows.size();
                    stats_stream<<j.dump()<<endl;
                }
            }

            std::copy(part_rows.begin(), part_rows.end(), std::back_inserter(rows));
            cout<<"-- took "<<timer.time()<<"s"<<endl;
        }
        cout<<"Got "<<pluralize(rows.size(), "row")<<" from all files."<<endl;
        //EXPECT_EQ(rows.size(), 11 * 1200);
        std::unordered_map<python::Type, size_t> type_counts;
        for(const auto& row : rows) {
            type_counts[row.getRowType()]++;
        }

        cout<<"Got "<<type_counts.size()<<" unique row types"<<endl;
        auto view_of_counts = sorted_view_of_values(type_counts);
        std::reverse(view_of_counts.begin(), view_of_counts.end());
        for(auto p : view_of_counts) {
            cout<<p.second<<"  "<<p.first.desc()<<endl;
        }

        // save to file (per_file_row_type_stats.ndjson)
        string stats_file_path = "per_file_row_type_stats.ndjson";
        cout<<"Saving per-file stats to "<<stats_file_path<<endl;
        stringToFile(stats_file_path, stats_stream.str());

        cout<<"Load took in total "<<loadTimer.time()<<"s"<<endl;

        for(unsigned id = 0; id < std::min(9999999ul, view_of_counts.size()); ++id) {
            auto normal_case_row_type = view_of_counts[id].first;

            string testName = ::testing::UnitTest::GetInstance()->current_test_info()->name();
            auto output_path = "./local-exp/" + testName + "/" + std::to_string(id) + "/";


            // init interpreter
            python::initInterpreter();
            python::unlockGIL();

            // check now with pipeline and set type.
            ContextOptions co = ContextOptions::defaults();
//        for(const auto& kv : exp_settings)
//            if(startsWith(kv.first, "tuplex."))
//                co.set(kv.first, kv.second);

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
                                "        repo =  row.get('repo')\n"
                                "        if repo:\n"
                                "            return repo.get('id')\n"
                                "        else:\n"
                                "            return None\n";

            // remove output files if they exist
            cout<<"Removing files (if they exist) from "<<output_path<<endl;
            boost::filesystem::remove_all(output_path.c_str());

            cout<<"Testing with normal-case row type: "<<normal_case_row_type.desc()<<endl;

            ctx.json(input_pattern, true, true, SamplingMode::SINGLETHREADED, row_type_to_column_hints(normal_case_row_type))
                    .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
                    .withColumn("repo_id", UDF(repo_id_code))
                    .filter(UDF("lambda x: x['type'] == 'ForkEvent'")) // <-- this is challenging to push down.
                    .withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
                    .withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
                    .selectColumns(vector<string>{"type", "repo_id", "year", "number_of_commits"})
                    .tocsv(output_path);

            auto result_row_count = csv_row_count_for_pattern(output_path + "*.csv");
            EXPECT_EQ(result_row_count, 378); // result which is correct for all rows.

            python::lockGIL();
            python::closeInterpreter();

            if(result_row_count != 378) {
                cerr<<"RUN id="<<id<<" failed, incorrect result, is: "<<result_row_count<<endl;
                break;
            }

            cout<<"RUN "<<(id+1)<<"/"<<view_of_counts.size()<<" done."<<endl;
        }



        // TODO: improve performance by reuse and get rid off nlohmann in struct_dict. --> DONE.s

        // TODO: implement support for setting schema in json.

        // TODO: check that metrics match, i.e. total normal-case row count MUST match whichever type count is reported.
        // --> don't care much about general-case row count.
        // However, can quickly create a general-case by turning every struct-dict into a general dict.
        // Then, the general count should work for ALL rows.
        // TODO: ensure that this type works as normal-case for ALL files. --> this is a separate test.
    }


    TEST(AllQueries, SingleChallengingRetypeType) {

        using namespace std;
        using namespace tuplex;

        auto encoded_type = "Row['created_at'->str,'payload'->Struct[(str,'shas'->List[(str,str,str,str,bool)]),(str,'size'->i64),(str,'ref'->str),(str,'head'->str)],'public'->bool,'type'->str,'url'->str,'actor'->str,'actor_attributes'->Struct[(str,'login'->str),(str,'type'->str),(str,'gravatar_id'->str),(str,'name'->str),(str,'company'->str),(str,'blog'->str),(str,'location'->str),(str,'email'->str)],'repository'->Struct[(str,'id'->i64),(str,'name'->str),(str,'full_name'->str),(str,'owner'->str),(str,'private'->bool),(str,'html_url'->str),(str,'description'->str),(str,'fork'->bool),(str,'url'->str),(str,'forks_url'->str),(str,'keys_url'->str),(str,'collaborators_url'->str),(str,'teams_url'->str),(str,'hooks_url'->str),(str,'issue_events_url'->str),(str,'events_url'->str),(str,'assignees_url'->str),(str,'branches_url'->str),(str,'tags_url'->str),(str,'blobs_url'->str),(str,'git_tags_url'->str),(str,'git_refs_url'->str),(str,'trees_url'->str),(str,'statuses_url'->str),(str,'languages_url'->str),(str,'stargazers_url'->str),(str,'contributors_url'->str),(str,'subscribers_url'->str),(str,'subscription_url'->str),(str,'commits_url'->str),(str,'git_commits_url'->str),(str,'comments_url'->str),(str,'issue_comment_url'->str),(str,'contents_url'->str),(str,'compare_url'->str),(str,'merges_url'->str),(str,'archive_url'->str),(str,'downloads_url'->str),(str,'issues_url'->str),(str,'pulls_url'->str),(str,'milestones_url'->str),(str,'notifications_url'->str),(str,'labels_url'->str),(str,'releases_url'->str),(str,'created_at'->str),(str,'updated_at'->str),(str,'pushed_at'->str),(str,'git_url'->str),(str,'ssh_url'->str),(str,'clone_url'->str),(str,'svn_url'->str),(str,'homepage'->null),(str,'size'->i64),(str,'stargazers_count'->i64),(str,'watchers_count'->i64),(str,'language'->str),(str,'has_issues'->bool),(str,'has_downloads'->bool),(str,'has_wiki'->bool),(str,'has_pages'->bool),(str,'forks_count'->i64),(str,'mirror_url'->null),(str,'open_issues_count'->i64),(str,'forks'->i64),(str,'open_issues'->i64),(str,'watchers'->i64),(str,'default_branch'->str),(str,'stargazers'->i64),(str,'master_branch'->str)]]";

        auto normal_case_row_type = python::decodeType(encoded_type);

        string input_pattern = "../resources/hyperspecialization/github_daily/*.json.sample";

        string testName = ::testing::UnitTest::GetInstance()->current_test_info()->name();
        auto output_path = "./local-exp/" + testName + "/" + "output" + "/";

        // init interpreter
        python::initInterpreter();
        python::unlockGIL();

        // check now with pipeline and set type.
        ContextOptions co = ContextOptions::defaults();

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
                            "        repo =  row.get('repo')\n"
                            "        if repo:\n"
                            "            return repo.get('id')\n"
                            "        else:\n"
                            "            return None\n";

        // remove output files if they exist
        cout<<"Removing files (if they exist) from "<<output_path<<endl;
        boost::filesystem::remove_all(output_path.c_str());

        cout<<"Testing with normal-case row type: "<<normal_case_row_type.desc()<<endl;

        // // debug:
        // ctx.json(input_pattern, true, true, SamplingMode::SINGLETHREADED, row_type_to_column_hints(normal_case_row_type))
        //         .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
        //         .withColumn("repo_id", UDF(repo_id_code))
        //         .filter(UDF("lambda x: x['type'] == 'ForkEvent'"))
        //         .withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
        //         .withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
        //         .selectColumns(vector<string>{"type", "repo_id", "year", "number_of_commits"})
        //         .tocsv(output_path);

        // original:
        ctx.json(input_pattern, true, true, SamplingMode::SINGLETHREADED, row_type_to_column_hints(normal_case_row_type))
             .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
             .withColumn("repo_id", UDF(repo_id_code))
             .filter(UDF("lambda x: x['type'] == 'ForkEvent'")) // <-- this is challenging to push down.
             .withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
             .withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
             .selectColumns(vector<string>{"type", "repo_id", "year", "number_of_commits"})
             .tocsv(output_path);

        auto result_row_count = csv_row_count_for_pattern(output_path + "*.csv");
        EXPECT_EQ(result_row_count, 378); // result which is correct for all rows.

        python::lockGIL();
        python::closeInterpreter();
    }

    TEST(AllQueries, SparseType) {

        using namespace std;
        using namespace tuplex;


        auto sparse_row_type = github_sparse_row_type();
        auto encoded_type = sparse_row_type.encode();
        cout<<"sparse row type is: "<<encoded_type<<endl;

        auto normal_case_row_type = python::decodeType(encoded_type);

        cout<<"Number of columns to decode with this type (at most): "<<normal_case_row_type.get_column_count()<<endl;

        string input_pattern = "../resources/hyperspecialization/github_daily/*.json.sample";

        // full data
        // input_pattern = "/hot/data/github_daily/*.json";

        string testName = ::testing::UnitTest::GetInstance()->current_test_info()->name();
        auto output_path = "./local-exp/" + testName + "/" + "output" + "/";

        // init interpreter
        python::initInterpreter();
        python::unlockGIL();

        // check now with pipeline and set type.
        ContextOptions co = ContextOptions::defaults();

        co.set("tuplex.backend", "worker");

        // this allows large files to be processed without splitting.
        co.set("tuplex.experimental.worker.numWorkers", "0"); // <-- single worker.
        co.set("tuplex.inputSplitSize", "20G");
        co.set("tuplex.experimental.worker.workerBufferSize", "12G"); // each normal, exception buffer in worker get 3G before they start spilling to disk!

        co.set("tuplex.resolveWithInterpreterOnly", "true");

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
                            "        repo =  row.get('repo')\n"
                            "        if repo:\n"
                            "            return repo.get('id')\n"
                            "        else:\n"
                            "            return None\n";

        // remove output files if they exist
        cout<<"Removing files (if they exist) from "<<output_path<<endl;
        boost::filesystem::remove_all(output_path.c_str());

        cout<<"Testing with normal-case row type: "<<normal_case_row_type.desc()<<endl;

         // debug:
         ctx.json(input_pattern, true, true, SamplingMode::SINGLETHREADED, row_type_to_column_hints(normal_case_row_type))
                 .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
                 .filter(UDF("lambda x: x['type'] == 'ForkEvent'"))
                 .withColumn("repo_id", UDF(repo_id_code))
                 .selectColumns(vector<string>{"type", "year","repo_id"})
                 .tocsv(output_path);

//        // original:
//        ctx.json(input_pattern, true, true, SamplingMode::SINGLETHREADED, row_type_to_column_hints(normal_case_row_type))
//                .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
//                .withColumn("repo_id", UDF(repo_id_code))
//                .filter(UDF("lambda x: x['type'] == 'ForkEvent'")) // <-- this is challenging to push down.
//                .withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
//                .withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
//                .selectColumns(vector<string>{"type", "repo_id", "year", "number_of_commits"})
//                .tocsv(output_path);

        auto result_row_count = csv_row_count_for_pattern(output_path + "*.csv");

        auto expected_row_count = 378;
        // full data?
        if(!strEndsWith(input_pattern, ".sample"))
            expected_row_count = 294195; // full query result.

        EXPECT_EQ(result_row_count, expected_row_count); // result which is correct for all rows.

        python::lockGIL();
        python::closeInterpreter();
    }

    TEST(AllQueries, GetAccessedColumnsInExtractRepoIDWithStaticAnnotation) {
        using namespace tuplex;
        using namespace std;

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
                            "        repo =  row.get('repo')\n"
                            "        if repo:\n"
                            "            return repo.get('id')\n"
                            "        else:\n"
                            "            return None\n";

        // extracting accessed columns (from row type) is tricky for the above UDF.
        // In this test, check that .get(...) is supported.

        UDF udf(repo_id_code);

        auto input_row_type = python::Type::decode("Row['repository'->Option[SparseStruct[(str,'id'=>i64)]],'actor_attributes'->Option[Struct[(str,'blog'=>str),(str,'company'=>str),"
                                                   "(str,'email'->str),(str,'gravatar_id'->str),(str,'location'=>str),(str,'login'->str),(str,'name'=>str),(str,'type'->str)]],"
                                                   "'payload'->Option[SparseStruct[(str,'commits'=>List[Struct[(str,'sha'->str),(str,'author'->Struct[(str,'name'->str),(str,'email'->str)]),"
                                                   "(str,'url'->str),(str,'message'->str)]]),(str,'target'=>SparseStruct[(str,'id'->i64)]),(str,'id'=>i64)]],'created_at'->str,"
                                                   "'url'->Option[str],'type'->str,'actor'->Option[str],'public'->Option[bool],'repo'->Option[SparseStruct[(str,'id'=>i64)]],'year'->i64]");




        // TODO: make this work WITHOUT sample. Should be possible.
        bool hint_rc = false;


        hint_rc = udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, input_row_type));
        ASSERT_TRUE(hint_rc);

        // now check access, following columns should be (at most) in it:
        // year, type, payload, repository, repo

        LambdaAccessedColumnVisitor v;
        udf.getAnnotatedAST().getFunctionAST()->accept(v);

        auto acc_indices = v.getAccessedIndices();

        for(auto idx: acc_indices) {
            cout<<"Accessed column "<<input_row_type.get_column_names()[idx]<<endl;
        }

        EXPECT_EQ(acc_indices.size(), 5);
    }

    TEST(AllQueries, GetAccessedColumnsInExtractRepoIDWithTracedAnnotation) {
        using namespace tuplex;
        using namespace std;

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
                            "        repo =  row.get('repo')\n"
                            "        if repo:\n"
                            "            return repo.get('id')\n"
                            "        else:\n"
                            "            return None\n";

        // extracting accessed columns (from row type) is tricky for the above UDF.
        // In this test, check that .get(...) is supported.

        UDF udf(repo_id_code);

        auto input_row_type = python::Type::decode("Row['repository'->Option[SparseStruct[(str,'id'=>i64)]],'actor_attributes'->Option[Struct[(str,'blog'=>str),(str,'company'=>str),"
                                                   "(str,'email'->str),(str,'gravatar_id'->str),(str,'location'=>str),(str,'login'->str),(str,'name'=>str),(str,'type'->str)]],"
                                                   "'payload'->Option[SparseStruct[(str,'commits'=>List[Struct[(str,'sha'->str),(str,'author'->Struct[(str,'name'->str),(str,'email'->str)]),"
                                                   "(str,'url'->str),(str,'message'->str)]]),(str,'target'=>SparseStruct[(str,'id'->i64)]),(str,'id'=>i64)]],'created_at'->str,"
                                                   "'url'->Option[str],'type'->str,'actor'->Option[str],'public'->Option[bool],'repo'->Option[SparseStruct[(str,'id'=>i64)]],'year'->i64]");

        bool hint_rc = false;

        python::initInterpreter();

        // "repository"
        // "actor_attributes"
        // "payload"
        // "created_at"
        // "url"
        // "type"
        // "actor"
        // "public"
        // "repo"
        // "year"
        Row row_1(Field::null(), Field::null(), Field::null(), Field("2012-11-01"), Field::null(), "TestEvent","test", false,Field::null(), 1234);

        ASSERT_EQ(row_1.getNumColumns(), input_row_type.get_column_count());

        std::vector<Row> original_sample_rows;
        original_sample_rows.push_back(row_1.with_columns(input_row_type.get_column_names()));
        std::vector<PyObject*> sample;
        for(const auto& row : original_sample_rows)
            sample.push_back(python::rowToPython(row));

        // use sample
        udf.removeTypes();
        hint_rc = udf.hintSchemaWithSample(sample, original_sample_rows, input_row_type);
        python::closeInterpreter();

        ASSERT_TRUE(hint_rc);

        // now check access, following columns should be (at most) in it:
        // year, type, payload, repository, repo

        LambdaAccessedColumnVisitor v;
        udf.getAnnotatedAST().getFunctionAST()->accept(v);

        auto acc_indices = v.getAccessedIndices();

        for(auto idx: acc_indices) {
            cout<<"Accessed column "<<input_row_type.get_column_names()[idx]<<endl;
        }

        EXPECT_EQ(acc_indices.size(), 2); // only repo and year due to tracing.
    }
}
