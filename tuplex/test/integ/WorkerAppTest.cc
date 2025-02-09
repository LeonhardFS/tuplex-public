//
// Created by leonhards on 9/10/24.
//

#include <gtest/gtest.h>

#include "S3File.h"
#include "Pipe.h"
#include "jit/RuntimeInterface.h"
#include <ContextOptions.h>
#include <Timer.h>

#include "helper.h"
#include <ee/worker/WorkerApp.h>

namespace tuplex {
    class WorkerAppTest : public ::testing::Test {
    protected:
        std::string testName;

        std::shared_ptr<WorkerApp> app;

        // Per Test setup
        void SetUp() override {
            app = std::make_shared<WorkerApp>();
            ASSERT_TRUE(app);
            auto rc = app->globalInit(false);
            ASSERT_EQ(rc, WORKER_OK);
            testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
        }

        void TearDown() override {
            ASSERT_TRUE(app);
            app->shutdown();
        }

        static void SetUpTestSuite() {
            using namespace tuplex;
            using namespace std;

            if(!is_docker_installed())
                GTEST_SKIP() << "Docker not found, can not initialize test suite.";

            // init AWS SDK
            cout<<"Initializing AWS SDK"<<endl;
            initAWSSDK(Aws::Utils::Logging::LogLevel::Trace);

            cout<<"Initializing interpreter and releasing GIL"<<endl;
            python::initInterpreter();
            python::unlockGIL();
        }

        static void TearDownTestSuite() {
            using namespace std;
            using namespace tuplex;

            cout<<"Shutting down interpreter"<<endl;
            python::lockGIL();
            python::closeInterpreter();

            cout<<"Shutting down AWSSDK"<<endl;
            shutdownAWS();
        }
    };

    TEST_F(WorkerAppTest, EnvironmentMessage) {
        using namespace std;

        // Send basic Environment request message.
        std::string json_buf;
        messages::InvocationRequest req;
        req.set_type(messages::MessageType::MT_ENVIRONMENTINFO);
        google::protobuf::util::MessageToJsonString(req, &json_buf);

        cout<<"JSON message to send:\n"<<json_buf<<endl;

        auto absl_rc = google::protobuf::util::JsonStringToMessage(json_buf, &req);
        ASSERT_TRUE(absl_rc.ok());

        auto rc = app->processJSONMessage(json_buf);
        ASSERT_EQ(rc, WORKER_OK);

        auto response = app->response();
        ASSERT_EQ(response.resources_size(), 1); // 1 resource for encoded JSON
        ASSERT_EQ(response.resources(0).type(), static_cast<uint32_t>(ResourceType::ENVIRONMENT_JSON));

        auto j = nlohmann::json::parse(response.resources(0).payload());
        cout<<"Environment information message:\n"<<j.dump(2)<<endl;
    }
}
