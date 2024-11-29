//
// Created by leonhards on 11/29/24.
//

#include <ee/IRequestBackend.h>
#include <ee/aws/ContainerInfo.h>
#include <google/protobuf/util/json_util.h>

namespace tuplex {
    void
    IRequestBackend::dumpAsJSON(std::stringstream &ss, uint64_t startTimestamp, uint64_t endTimestamp, bool use_hyper,
                              double cost, size_t n_requests, size_t total_mbmbs, double cost_per_gb_second,
                              size_t total_input_normal_path, size_t total_input_general_path,
                              size_t total_input_fallback_path, size_t total_input_unresolved, size_t total_output_rows,
                              size_t total_output_exceptions,
                              const std::vector<TaskTriplet>& requests_responses) {
        using namespace std;

        ss << "{";

        // 0. general info
        ss << "\"stageStartTimestamp\":" << startTimestamp << ",";
        ss << "\"stageEndTimestamp\":" << endTimestamp << ",";

        // settings etc.
        ss << "\"hyper_mode\":" << (use_hyper ? "true" : "false") << ",";
        ss << "\"cost\":" << cost << ",";

        // detailed billing info to breakdown better.
        ss <<"\"billing\":{";
        ss<<"\"requestCount\":"<<n_requests<<",";
        ss<<"\"mbms\":"<<total_mbmbs<<",";
        ss<<"\"costPerGBSecond\":"<<cost_per_gb_second;
        ss<<"},";

        ss << "\"input_paths_taken\":{"
           << "\"normal\":" << total_input_normal_path << ","
           << "\"general\":" << total_input_general_path << ","
           << "\"fallback\":" << total_input_fallback_path << ","
           << "\"unresolved\":" << total_input_unresolved
           << "},";
        ss << "\"output_paths_taken\":{"
           << "\"normal\":" << total_output_rows << ","
           << "\"unresolved\":" << total_output_exceptions
           << "},";


        // 1. tasks
        ss << "\"tasks\":[";
        {

            int task_counter = 0;
            for (const auto &t: requests_responses) {
                auto response = t.response;
                ContainerInfo info = response.container();
                ss << "{\"container\":" << info.asJSON() << ",\"invoked_containers\":[";
                for (unsigned i = 0; i < response.invokedresponses_size(); ++i) {
                    auto invoked_response = response.invokedresponses(i);
                    ContainerInfo info = invoked_response.container();
                    ss << info.asJSON();
                    if (i != response.invokedresponses_size() - 1)
                        ss << ",";
                }
                ss << "]";

                // log
                for (const auto &r: response.resources()) {
                    if (r.type() == static_cast<uint32_t>(ResourceType::LOG)) {
                        auto log = decompress_string(r.payload());
                        ss << ",\"log\":" << escape_for_json(log) << "";
                        break;
                    }
                }

                ss << ",\"invoked_requests\":[";
                RequestInfo r_info;
                for (int i = 0; i < response.invokedrequests_size(); ++i) {
                    r_info = response.invokedrequests(i);
                    r_info.fillInFromResponse(response.invokedresponses(i));
                    ss << r_info.asJSON();
                    if (i != response.invokedrequests_size() - 1)
                        ss << ",";
                }
                ss << "]";

                // invoked input uris
                ss << ",\"input_uris\":[";
                for (unsigned i = 0; i < response.inputuris_size(); ++i) {
                    ss << "\"" << response.inputuris(i) << "\"";
                    if (i != response.inputuris_size() - 1)
                        ss << ",";
                }

                ss << "]";

                // end container.
                ss << "}";
                if (task_counter != requests_responses.size() - 1)
                    ss << ",";
                task_counter++;
            }
        }
        ss << "],";


        // 2. requests & responses?
        // 1. tasks
        ss << "\"requests\":[";
        {
            for (unsigned i = 0; i < requests_responses.size(); ++i) {
                auto &info = requests_responses[i].client_info;
                ss << info.asJSON();
                if (i != requests_responses.size() - 1)
                    ss << ",";
            }
        }
        ss << "]";

        // Dump all responses (full data for analysis).
        ss << ",\"responses\":[";
        {
            for (unsigned i = 0; i < requests_responses.size(); ++i) {
                std::string response_as_str;
                google::protobuf::util::MessageToJsonString(requests_responses[i].response, &response_as_str);
                ss << response_as_str;
                if (i != requests_responses.size() - 1)
                    ss << ",";
            }
        }
        ss << "]";

        ss << "}";
    }

    JobInfo gather_total_stats_from_triplets(const std::vector<TaskTriplet>& tasks) {
        JobInfo info;

        for (const auto &task: tasks) {
            info.total_output_rows += task.response.numrowswritten();
            info.total_output_exceptions += task.response.numexceptions();

            info.total_input_normal_path += task.response.rowstats().normal();
            info.total_input_general_path += task.response.rowstats().general();
            info.total_input_fallback_path += task.response.rowstats().interpreter();
            info.total_input_unresolved += task.response.rowstats().unresolved();
        }

        return info;
    }
}