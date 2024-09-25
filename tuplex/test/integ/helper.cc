//
// Created by leonhards on 9/1/24.
//

#include "helper.h"
#include "physical/execution/CSVReader.h"
#include "ee/worker/WorkerBackend.h"
#include <ee/aws/ContainerInfo.h>

namespace tuplex {
    ContainerInfo getThisContainerInfo() {
        ContainerInfo info;
        return info;
    }
}

namespace tuplex {

    bool is_docker_installed() {
        using namespace tuplex;
        using namespace std;

        // Check by running `docker --version`, whether locally docker is available.
        // If not, skip tests.
        Pipe p("docker --version");
        p.pipe();
        if(p.retval() == 0) {
            auto p_stdout = p.stdout();
            trim(p_stdout);
            cout<<"Found "<<p_stdout<<"."<<endl;
            return true;
        }
        return false;
    }

    std::string minio_data_location() {
        using namespace tuplex;

        // create location if needed
        auto absolute_path = URI("../resources/data").toPath();
        if(!dirExists(absolute_path.c_str())) {
            std::filesystem::create_directories(absolute_path);
        }
        return absolute_path;
    }

    std::tuple<Aws::Auth::AWSCredentials, Aws::Client::ClientConfiguration> local_s3_credentials(const std::string& access_key, const std::string& secret_key, int port) {
        Aws::Auth::AWSCredentials credentials(access_key.c_str(), secret_key.c_str(), "");
        Aws::Client::ClientConfiguration config;
        config.endpointOverride = "http://localhost:" + std::to_string(port);
        config.enableEndpointDiscovery = false;
        // need to disable signing https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html.
        config.verifySSL = false;
        config.connectTimeoutMs = 1500; // 1.5s timeout (local machine)
        return std::make_tuple(credentials, config);
    }

    std::vector<std::string> list_containers(bool only_active) {
        using namespace std;
        stringstream ss;
        ss<<"docker ps ";
        if(!only_active)
            ss<<" -a ";
        ss<<" --format=json";

        Pipe p(ss.str());
        p.pipe();
        if(p.retval() != 0) {
            throw std::runtime_error("Failed to list docker containers: " + p.stderr());
        } else {
            string json_str = p.stdout();
            std::vector<std::string> names;
            std::string line;
            std::stringstream input; input<<json_str;
            while(std::getline(input, line)) {
                auto j = nlohmann::json::parse(line);

                names.push_back(j["Names"].get<std::string>());
            }

            return names;
        }
    }

    bool stop_container(const std::string& container_name) {
        using namespace std;
        stringstream ss;
        ss<<"docker stop "<<container_name;
        Pipe p(ss.str());
        p.pipe();
        if(p.retval() != 0) {
            cerr<<"Failed stopping container "<<container_name<<": "<<p.stderr()<<endl;
            return false;
        } else {
            cout<<"Stopped container "<<container_name<<"."<<endl;
#ifndef NDEBUG
            cout<<p.stdout()<<endl;
#endif
            return true;
        }
    }

    bool remove_container(const std::string& container_name) {
        using namespace std;
        stringstream ss;
        ss<<"docker rm "<<container_name;
        Pipe p(ss.str());
        p.pipe();
        if(p.retval() != 0) {
            cerr<<"Failed removing container "<<container_name<<": "<<p.stderr()<<endl;
            return false;
        } else {
            cout<<"Removed container "<<container_name<<"."<<endl;
#ifndef NDEBUG
            cout<<p.stdout()<<endl;
#endif
            return true;
        }
    }

    bool start_local_s3_server() {
        using namespace std;

        // check if container with the name already exists, if so stop & remove.
        auto container_names = list_containers();
        if(std::find(container_names.begin(), container_names.end(), MINIO_DOCKER_CONTAINER_NAME) != container_names.end()) {
            cout<<"Found existing oontainer named "<<MINIO_DOCKER_CONTAINER_NAME<<", stopping and removing container."<<endl;
            stop_container(MINIO_DOCKER_CONTAINER_NAME);
            remove_container(MINIO_DOCKER_CONTAINER_NAME);
        }

        stringstream ss;
        // General form is docker run [OPTIONS] IMAGE [COMMAND] [ARG...]
        ss<<"docker run -p 9000:"<<MINIO_S3_ENDPOINT_PORT<<" -p 9001:"<<MINIO_S3_CONSOLE_PORT
          <<" -e \"MINIO_ROOT_USER="<<MINIO_ACCESS_KEY<<"\""
          <<" -e \"MINIO_ROOT_PASSWORD="<<MINIO_SECRET_KEY<<"\""
          <<" --name \""<<MINIO_DOCKER_CONTAINER_NAME<<"\""
          <<" -d " // detached mode.
          <<" quay.io/minio/minio server /data --console-address \":"<<MINIO_S3_CONSOLE_PORT<<"\"";

        Pipe p(ss.str());
        p.pipe();
        if(p.retval() != 0) {
            cerr<<"Failed starting local s3 server: "<<p.stderr()<<endl;
            return false;
        } else {
            cout<<"Started local s3 server."<<endl;
#ifndef NDEBUG
            cout<<p.stdout()<<endl;
#endif
            return true;
        }
    }


    bool stop_local_s3_server() {
        using namespace std;

        if(!stop_container(MINIO_DOCKER_CONTAINER_NAME))
            return false;
        if(!remove_container(MINIO_DOCKER_CONTAINER_NAME))
            return false;

        return true;
    }

    size_t csv_row_count(const std::string &path) {
        // parse CSV from path, and count rows.
        VFCSVStreamCursor stream(path, ',', '"');
        csvmonkey::CsvReader<csvmonkey::StreamCursor> reader(stream);

        csvmonkey::CsvCursor &row = reader.row();
        if (!reader.read_row()) {
            throw std::runtime_error("Cannot read header row");
        }

        size_t row_count = 0;
        while (reader.read_row()) {
            row_count++;
        }
        return row_count;
    }

    size_t csv_row_count_for_pattern(const std::string &pattern) {
        using namespace std;
        auto output_uris = VirtualFileSystem::fromURI(pattern).glob(pattern);
        cout << "Found " << pluralize(output_uris.size(), "output file") << endl;
        size_t total_row_count = 0;
        for (auto path: output_uris) {
            auto row_count = csv_row_count(path.toString());
            cout << "-- file " << path << ": " << pluralize(row_count, "row") << endl;
            total_row_count += row_count;
        }
        return total_row_count;
    }

    static std::string runCommand(const std::string& cmd) {
        std::array<char, 128> buffer;
        std::string result;
        std::shared_ptr<FILE> pipe(popen(cmd.c_str(), "r"), pclose);
        if (!pipe) throw std::runtime_error("popen() failed!");
        while (!feof(pipe.get())) {
            if (fgets(buffer.data(), 128, pipe.get()) != nullptr)
                result += buffer.data();
        }
        return result;
    }

    messages::InvocationResponse process_request_with_worker(const std::string& worker_path,
                                                             const std::string& scratch_dir,
                                                             const messages::InvocationRequest& request,
                                                             bool invoke_process) {
        auto& logger = Logger::instance().logger("test");

        std::string json_message;
        google::protobuf::util::MessageToJsonString(request, &json_message);

        if(invoke_process) {
            auto actual_worker_path = find_worker(worker_path);
            logger.info("Found worker executable " + actual_worker_path);


            // ensure cache dir exists.
            VirtualFileSystem::fromURI(scratch_dir).create_dir(scratch_dir);

            auto process_id = 0;
            auto request_no = 1;
            Timer timer;
            auto message_path = URI(scratch_dir).join("message_process_" + std::to_string(process_id) + "_" + std::to_string(request_no-1) + ".json");
            auto stats_path = URI(scratch_dir).join("message_stats_" + std::to_string(process_id) + "_" + std::to_string(request_no-1) + ".json");
            auto response_path = URI(scratch_dir).join("message_response_" + std::to_string(process_id) + "_" + std::to_string(request_no-1) + ".json");

            // save to file
            stringToFile(message_path, json_message);

            auto cmd = actual_worker_path + " -m " + message_path.toPath() + " -s " + stats_path.toPath() + " -o " + response_path.toPath();
            auto res_stdout = runCommand(cmd);

//        // parse stats as answer out
//        auto stats = nlohmann::json::parse(fileToString(stats_path));
//        auto worker_invocation_duration = timer.time();
//        stats["request_total_time"] = worker_invocation_duration;
//
//        std::cout<<"Response:\n"<<stats.dump(2)<<std::endl;

            messages::InvocationResponse response;
            google::protobuf::util::JsonStringToMessage(fileToString(response_path), &response);
            return response;
        } else {
            auto app = std::make_unique<WorkerApp>(WorkerSettings());
            app->globalInit(true);

            app->processJSONMessage(json_message);
            return app->response();
        }
    }

    void github_pipeline(Context& ctx, const std::string& input_pattern, const std::string& output_path) {
        using namespace std;
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

        ctx.json(input_pattern, true, true)
                .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
                .withColumn("repo_id", UDF(repo_id_code))
                .filter(UDF("lambda x: x['type'] == 'ForkEvent'")) // <-- this is challenging to push down.
                .withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
                .withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
                .selectColumns(vector<string>{"type", "repo_id", "year", "number_of_commits"})
                .tocsv(output_path);
    }
}