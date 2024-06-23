//
// Created by Leonhard Spiegelberg on 6/22/24.
//

#include <iostream>
#include <vector>
#include <simdjson.h>
#include "timer.h"
#include <glob.h>
#include <sstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <lyra/lyra.hpp>

// use AWS SDK bundled cjson
#include <aws/core/external/cjson/cJSON.h>
#include <fstream>
#include <any>
#include <filesystem>

int main(int argc, char* argv[]) {
    using namespace std;

//    static_assert(false == SIMDJSON_THREADS_ENABLED, "threads disabled");

    // parse arguments
    string input_pattern;
    string output_path = "local-output";
    string mode = "best";
    string result_path;
    std::vector<std::string> supported_modes{"best", "cjson", "cstruct", "yyjson"};
    bool show_help = false;

    // construct CLI
    auto cli = lyra::cli();
    cli.add_argument(lyra::help(show_help));
    cli.add_argument(lyra::opt(input_pattern, "inputPattern").name("-i").name("--input-pattern").help("input pattern from which to read files."));
    cli.add_argument(lyra::opt(output_path, "outputPath").name("-o").name("--output-path").help("output path where to store results, will be created as directory if not exists."));
    cli.add_argument(lyra::opt(result_path, "resultPath").name("-r").name("--result-path").help("output path where to store timings."));
    cli.add_argument(lyra::opt(mode, "mode").name("-m").name("--mode").help("C++ mode to use, supported modes are: " +
                                                                            vec_to_string(supported_modes)));
    auto result = cli.parse({argc, argv});
    if(!result) {
        cerr<<"Error parsing command line: "<<result.errorMessage()<<std::endl;
        return 1;
    }

    if(show_help) {
        cout<<cli<<endl;
        return 0;
    }

    if(input_pattern.empty()) {
        cerr<<"Must specify input pattern via -i or --input-pattern."<<endl;
        return 1;
    }

    if(supported_modes.end() == std::find(supported_modes.begin(), supported_modes.end(), mode)) {
        cerr<<"Unknown mode "<<mode<<" found, supported are "<<vec_to_string(supported_modes)<<endl;
        return 1;
    }


    Timer timer;
    cout<<"starting C++ baseline::"<<endl;
    cout<<"Pipeline will process: "<<input_pattern<<" -> "<<output_path<<endl;
    cout<<timer.time()<<"s "<<"Globbing files from "<<input_pattern<<endl;
    auto paths = glob_pattern(input_pattern);
    cout<<timer.time()<<"s "<<"found "<<paths.size()<<" paths."<<endl;

    // create local output dir if it doesn't exist yet.
    if(!dirExists(output_path.c_str())) {
//        int rc = mkdir(output_path.c_str(), 0777);
//        if(rc != 0) {
//            cerr<<"Failed to create dir "<<output_path;
//            exit(1);
//        }
        std::filesystem::create_directories(output_path);
    }

    cout<<timer.time()<<"s "<<"saving output to "<<output_path<<endl;

    std::stringstream ss;
    for(unsigned i = 0; i < paths.size(); ++i) {
        auto path = paths[i];
        cout<<"Processing path "<<(i+1)<<"/"<<paths.size()<<endl;
        Timer path_timer;
        auto part_path = output_path + "/part_" + std::to_string(i) + ".csv";
        int input_row_count=0,output_row_count=0;
        double loading_time = 0.0;
        std::tie(input_row_count, output_row_count, loading_time) = process_path(path, part_path, mode);
        ss<<mode<<","<<path<<","<<part_path<<","<<path_timer.time()<<","<<loading_time<<",$$STUB$$,"<<input_row_count<<","<<output_row_count<<"\n";
    }

    auto csv = "mode,input_path,output_path,time_in_s,loading_time_in_s,total_time_in_s,input_row_count,output_row_count\n" + ss.str();

    // replace $$STUB$$ with total time
    double total_time_in_s = timer.time();
    csv = replace_all(csv, "$$STUB$$", std::to_string(total_time_in_s));
    cout<<"per-file stats in CSV format::\n"<<csv<<"\n"<<endl;

    if(!result_path.empty()) {
        cout<<"Saving timings to "<<result_path<<endl;
        std::ofstream ofs(result_path, ios::app);
        ofs<<csv;
        ofs.close();
    }

    cout<<"Processed files in "<<timer.time()<<"s"<<endl;
    return 0;
}