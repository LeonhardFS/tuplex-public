//
// Created by leonhards on 9/1/24.
//

#ifndef TUPLEX_HELPER_H
#define TUPLEX_HELPER_H

#include <string>
#include <VirtualFileSystem.h>

#include "JsonStatistic.h"
#include "tracing/LambdaAccessedColumnVisitor.h"
#include "StructCommon.h"
#include "tracing/TraceVisitor.h"

#include <AWSCommon.h>
#include <Context.h>
#include <VirtualFileSystem.h>
#include <S3Cache.h>
#include <filesystem>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <nlohmann/json.hpp>
#include <boost/interprocess/streams/bufferstream.hpp>

#include "S3File.h"
#include "Pipe.h"

#include "S3File.h"
#include "Pipe.h"
#include "jit/RuntimeInterface.h"
#include <ContextOptions.h>
#include <Timer.h>
#include <utils/Messages.h>

namespace tuplex {
    // dummy values used for testing
    static const std::string MINIO_ACCESS_KEY="AKIAIOSFODNN7EXAMPLE";
    static const std::string MINIO_SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    static const std::string MINIO_DOCKER_CONTAINER_NAME="tuplex-local-s3-minio";
    static const int MINIO_S3_ENDPOINT_PORT=9000;
    static const int MINIO_S3_CONSOLE_PORT=9001;

    // the sampling mode to use in the experiments.
    // NOTE: DEFAULT_SAMPLING_MODE is ONLY first rows.
    static const SamplingMode DEFAULT_EXPERIMENT_SAMPLING_MODE=SamplingMode::FIRST_FILE | SamplingMode::LAST_FILE | SamplingMode::FIRST_ROWS | SamplingMode::LAST_ROWS;

    extern size_t csv_row_count(const std::string &path);

    extern size_t csv_row_count_for_pattern(const std::string &pattern);

    extern bool is_docker_installed();

    extern std::vector<std::string> list_containers(bool only_active=false);

    extern bool stop_container(const std::string& container_name);

    extern bool remove_container(const std::string& container_name);

    extern bool start_local_s3_server();

    extern bool stop_local_s3_server();

    extern std::tuple<Aws::Auth::AWSCredentials, Aws::Client::ClientConfiguration> local_s3_credentials(const std::string& access_key=MINIO_ACCESS_KEY, const std::string& secret_key=MINIO_SECRET_KEY, int port=MINIO_S3_ENDPOINT_PORT);

    extern std::string minio_data_location();

    extern messages::InvocationResponse process_request_with_worker(const std::string& worker_path, const std::string& scratch_dir, const messages::InvocationRequest& request, bool invoke_process=true);

    extern void github_pipeline(Context& ctx, const std::string& input_pattern, const std::string& output_path, const SamplingMode& sm=DEFAULT_EXPERIMENT_SAMPLING_MODE);
}

#endif //TUPLEX_HELPER_H
