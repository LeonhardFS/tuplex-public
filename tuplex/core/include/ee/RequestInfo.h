//
// Created by Leonhard Spiegelberg on 1/31/22.
//

#ifndef TUPLEX_REQUESTINFO_H
#define TUPLEX_REQUESTINFO_H

#include <Base.h>
#include <StringUtils.h>
#include <utils/Messages.h>
#include "JSONUtils.h"

namespace tuplex {

    // helper function to calculate how many parts to invoke from recurse specification
    inline size_t lambdaCount(const std::vector<size_t>& recursive_counts) {

        if(recursive_counts.empty())
            return 0;

        size_t total_parts = 1;
        size_t prod = 1;
        size_t num_lambdas_to_invoke = 0;
        for(unsigned i = 0; i < recursive_counts.size(); ++i) {
            auto count = recursive_counts[i];
            if(count != 0) {
                total_parts += count * prod; // this is recursive, so try splitting into that many parts!
                prod *= count;
            }
        }
        return total_parts;
    }

    struct AWSLambdaTimings {
        // These are displayed at the end of the log in the form of
        // Duration: <val> Billed Duration: <val> Memory Size: <val> Max Memory Used: <val> Init Duration: <val>
        double initTimeInMs;
        double durationInMs;
        size_t billedDurationInMs;
        size_t memorySizeInMb;
        size_t maxMemoryUsedInMb;

        AWSLambdaTimings(): initTimeInMs(0), durationInMs(0), billedDurationInMs(0), memorySizeInMb(0), maxMemoryUsedInMb(0) {}

        static AWSLambdaTimings from_proto(const messages::LambdaTimings& msg) {
            AWSLambdaTimings t;
            t.initTimeInMs = msg.inittimeinms();
            t.durationInMs = msg.durationinms();
            t.billedDurationInMs = msg.billeddurationinms();
            t.memorySizeInMb = msg.memorysizeinmb();
            t.maxMemoryUsedInMb = msg.maxmemoryusedinmb();
            return t;
        }

        void fill_proto(messages::LambdaTimings* msg) const {
            if(!msg)
                return;

            msg->set_inittimeinms(initTimeInMs);
            msg->set_durationinms(durationInMs);
            msg->set_billeddurationinms(billedDurationInMs);
            msg->set_memorysizeinmb(memorySizeInMb);
            msg->set_maxmemoryusedinmb(maxMemoryUsedInMb);
        }


        static AWSLambdaTimings parse_from_log(const std::string& line) {
            AWSLambdaTimings t;

            std::vector<std::string> tabCols;
            splitString(line, '\t', [&](const std::string &s) { tabCols.emplace_back(s); });

            if (tabCols.empty())
                return t;

            // extract parts
            for (auto col: tabCols) {
                trim(col);

                if (strStartsWith(col, "Init Duration: ") && strEndsWith(col, " ms")) {
                    // extract ID and store it
                    auto r = col.substr(strlen("Init Duration: "), col.length() - 3 - strlen("Init Duration: "));
                    t.initTimeInMs = std::stod(r);
                }

                if (strStartsWith(col, "Duration: ") && strEndsWith(col, " ms")) {
                    // extract ID and store it
                    auto r = col.substr(strlen("Duration: "), col.length() - 3 - strlen("Duration: "));
                    t.durationInMs = std::stod(r);
                }

                if (strStartsWith(col, "Billed Duration: ") && strEndsWith(col, " ms")) {
                    // extract ID and store it
                    auto r = col.substr(strlen("Billed Duration: "), col.length() - 3 - strlen("Billed Duration: "));
                    t.billedDurationInMs = std::stoi(r);
                }

                if (strStartsWith(col, "Memory Size: ") && strEndsWith(col, " MB")) {
                    // extract ID and store it
                    auto r = col.substr(strlen("Memory Size: "), col.length() - 3 - strlen("Memory Size: "));
                    t.memorySizeInMb = std::stoi(r);
                }

                if (strStartsWith(col, "Max Memory Used: ") && strEndsWith(col, " MB")) {
                    // extract ID and store it
                    auto r = col.substr(strlen("Max Memory Used: "), col.length() - 3 - strlen("Max Memory Used: "));
                    t.maxMemoryUsedInMb = std::stoi(r);
                }
            }

            return t;
        }
    };

    /*!
     * Helper struct holding decoded information obtained from a log of a Lambda request.
     */
    struct RequestInfo {
        std::string requestId;
        std::string containerId; //! uuid of container

        // Lambda timings (measured from log)
        AWSLambdaTimings awsTimings;

        uint64_t tsRequestStart; //! ns UTC timestamp
        uint64_t tsRequestEnd; //! ns UTC timestamp

        // log may display error message!
        int returnCode;
        std::string errorMessage;

        // input row split
        size_t in_normal, in_general, in_fallback, in_unresolved;
        size_t out_normal, out_unresolved;

        // types
        python::Type normal_input_type;
        python::Type normal_output_type;
        python::Type general_input_type;
        python::Type general_output_type;

        // time infos
        double fast_path_time, general_and_interpreter_time, compile_time, hyper_time;

        RequestInfo() :
        returnCode(0), tsRequestStart(0), tsRequestEnd(0),
        in_normal(0), in_general(0), in_fallback(0), in_unresolved(0), out_normal(0), out_unresolved(0),
        fast_path_time(0), general_and_interpreter_time(0), compile_time(0), hyper_time(0) {}


        RequestInfo(const messages::RequestInfo& info) : in_normal(0), in_general(0), in_fallback(0), in_unresolved(0), out_normal(0), out_unresolved(0),
                                                         fast_path_time(0), general_and_interpreter_time(0), compile_time(0), hyper_time(0), requestId(info.requestid().c_str()),
        containerId(info.containerid()),
                                                         awsTimings(std::move(AWSLambdaTimings::from_proto(info.timings()))), returnCode(info.returncode()), errorMessage(info.errormessage().c_str()),
        tsRequestStart(info.tsrequeststart()), tsRequestEnd(info.tsrequestend()) {}

        static RequestInfo parseFromLog(const std::string& log);

        inline void fillInFromResponse(const messages::InvocationResponse& response) {
            in_normal = response.rowstats().normal();
            in_general = response.rowstats().general();
            in_fallback = response.rowstats().interpreter();
            in_unresolved = response.rowstats().unresolved();

            // decode types (from str)
            normal_input_type   = python::Type::decode(response.rowstats().normal_input_schema());
            normal_output_type  = python::Type::decode(response.rowstats().normal_output_schema());
            general_input_type  = python::Type::decode(response.rowstats().general_input_schema());
            general_output_type = python::Type::decode(response.rowstats().general_output_schema());

            out_normal = response.numrowswritten();
            out_unresolved = response.numexceptions();

            if(response.breakdowntimes().contains("fast_path_execution_time"))
                fast_path_time = response.breakdowntimes().at("fast_path_execution_time");
            if(response.breakdowntimes().contains("compile_time"))
                compile_time = response.breakdowntimes().at("compile_time");
            if(response.breakdowntimes().contains("general_and_interpreter_time"))
                general_and_interpreter_time = response.breakdowntimes().at("general_and_interpreter_time");
            if(response.breakdowntimes().contains("hyperspecialization_time"))
                hyper_time = response.breakdowntimes().at("hyperspecialization_time");
        }

        // protobuf representation
        inline std::string asJSON() const {
            std::stringstream ss;
            ss<<"{\"requestId\":\""<<requestId<<"\"";
            ss<<",\"containerId\":\""<<containerId<<"\"";
            ss<<",\"initTimeInMs\":"<<awsTimings.initTimeInMs;
            ss<<",\"durationInMs\":"<<awsTimings.durationInMs;
            ss<<",\"billedDurationInMs\":"<<awsTimings.billedDurationInMs;
            ss<<",\"memorySizeInMb\":"<<awsTimings.memorySizeInMb;
            ss<<",\"maxMemoryUsedInMb\":"<<awsTimings.maxMemoryUsedInMb;
            ss<<",\"returnCode\":"<<returnCode;
            ss<<",\"errorMessage\":\""<<errorMessage<<"\"";
            // row stats
            ss<<",\"input_paths_taken\":{"
              <<"\"normal\":"<<in_normal<<","
              <<"\"general\":"<<in_general<<","
              <<"\"fallback\":"<<in_fallback<<","
              <<"\"unresolved\":"<<in_unresolved<<"}";
            ss<<",\"output_paths_taken\":{"
              <<"\"normal\":"<<out_normal<<","
              <<"\"unresolved\":"<<out_unresolved<<"}";
            // schemas (allows to reason about hyperspecialization)
            ss<<",\"input_schemas\":{"
              <<"\"normal\":"<<escape_json_string(normal_input_type.encode())<<","
              <<"\"general\":"<<escape_json_string(general_input_type.encode())<<"}";
            ss<<",\"output_schemas\":{"
              <<"\"normal\":"<<escape_json_string(normal_output_type.encode())<<","
              <<"\"general\":"<<escape_json_string(general_output_type.encode())<<"}";

            // timing info
            ss<<",\"t_hyper\":"<<hyper_time;
            ss<<",\"t_compile\":"<<compile_time;
            ss<<",\"t_fast\":"<<fast_path_time;
            ss<<",\"t_slow\":"<<general_and_interpreter_time;
            // global timing info
            ss<<",\"tsRequestStart\":"<<tsRequestStart;
            ss<<",\"tsRequestEnd\":"<<tsRequestEnd;
            ss<<"}";
            return ss.str();
        }

        inline void fill(messages::RequestInfo* r) const {
            if(!r)
                return;

            r->set_requestid(requestId.c_str());
            r->set_containerid(containerId.c_str());

            awsTimings.fill_proto(r->mutable_timings());

            r->set_returncode(returnCode);
            r->set_errormessage(errorMessage.c_str());
            r->set_tsrequeststart(tsRequestStart);
            r->set_tsrequestend(tsRequestEnd);
        }

        inline messages::RequestInfo* to_protobuf() const {
            auto r = new messages::RequestInfo();
            fill(r);
            return r;
        }
    };


    // Helper structure to track client-side request info, request and response.
    struct TaskTriplet {
        RequestInfo client_info;
        messages::InvocationRequest request;
        messages::InvocationResponse response;
    };
}

#endif //TUPLEX_REQUESTINFO_H
