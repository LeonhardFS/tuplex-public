//
// Created by leonhards on 3/30/25.
//
#include "FileSystemUtils.h"
#include "S3File.h"
#include <ContextOptions.h>
#include <Timer.h>

#ifdef BUILD_WITH_AWS

#include <AWSCommon.h>
#include <VirtualFileSystem.h>
#include <S3Cache.h>

#ifndef S3_TEST_BUCKET
// define dummy to compile
#ifdef SKIP_AWS_TESTS
#define S3_TEST_BUCKET "tuplex-test"
#endif

#include <S3Cache.h>

#warning "need S3 Test bucket to run these tests"
#endif

namespace tuplex {
    static const std::string s3TestBase = "s3://" + std::string(S3_TEST_BUCKET) + "/tests";

    class S3CacheTests : public ::testing::Test {
    protected:
        std::string testName;

        void SetUp() override {
            using namespace tuplex;

            // init S3 file system
            auto cred = AWSCredentials::get();
            NetworkSettings ns;
            initAWS(cred, ns, true);

            VirtualFileSystem::addS3FileSystem(cred.access_key, cred.secret_key, cred.session_token, cred.default_region, ns, false, true);
            testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
        }

        void TearDown() override {
            shutdownAWS();
        }
    };

    TEST_F(S3CacheTests, DownloadLargeFile) {
        // this test should not be shipped as part of release, it's existing to demonstrate download feasibilty.
        URI largeFileURI("s3://tuplex-public/data/github_daily/2020-10-15.json");

        auto s3PreCacheSize = memStringToSize("1GB");

        auto& cache = S3FileCache::instance();
        cache.reset(s3PreCacheSize);
        cache.setFS(*VirtualFileSystem::getS3FileSystemImpl());

        auto f = cache.putAsync(largeFileURI, 0, 0, true);
        f.wait();
        EXPECT_NE(f.get(), 0);
    }

}


#endif