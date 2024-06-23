//
// Created by Leonhard Spiegelberg on 6/22/24.
//

#include <iostream>
#include <vector>
#include <simdjson.h>
#include <Timer.h>
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

int dirExists(const char *path)
{
    struct stat info;

    if(stat( path, &info ) != 0)
        return 0;
    else if(info.st_mode & S_IFDIR)
        return 1;
    else
        return 0;
}

std::vector<std::string> glob_pattern(const std::string &pattern) {
    using namespace std;

    // from https://stackoverflow.com/questions/8401777/simple-glob-in-c-on-unix-system
    // glob struct resides on the stack
    glob_t glob_result;
    memset(&glob_result, 0, sizeof(glob_result));

    // do the glob operation
    int return_value = ::glob(pattern.c_str(), GLOB_TILDE | GLOB_MARK, NULL, &glob_result);
    if(return_value != 0) {
        globfree(&glob_result);

        // special case, no match
        if(GLOB_NOMATCH == return_value) {
            std::cerr<<"did not find any files for pattern '" + pattern + "'"<<std::endl;
            return {};
        }

        stringstream ss;
        ss << "glob() failed with return_value " << return_value << endl;
        throw std::runtime_error(ss.str());
    }

    // collect all the filenames into a std::list<std::string>
    vector<std::string> uris;
    for(size_t i = 0; i < glob_result.gl_pathc; ++i) {
        uris.emplace_back(std::string(glob_result.gl_pathv[i]));
    }

    // cleanup
    globfree(&glob_result);

    // done
    return uris;
}

std::string view_to_str(const std::string_view& v) {
    return std::string(v.begin(), v.end());
}

std::string vec_to_string(const std::vector<std::string>& v) {
    if(v.empty())
        return "[]";
    std::stringstream ss;
    for(unsigned i = 0; i < v.size(); ++i) {
        ss<<v[i];
        if(i != v.size() - 1)
            ss<<",";
    }
    return ss.str();
}

std::string replace_all(std::string str, const std::string &from, const std::string &to) {
    size_t start_pos = 0;
    while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length(); // Handles case where 'to' is a substring of 'from'
    }
    return str;
}

std::tuple<int,int,double> process_path(const std::string& input_path, const std::string& output_path, const std::string& mode) {
    using namespace std;
//    Timer load_timer;
//    // read file into memory
//    uint8_t* buffer = nullptr;
//    struct stat s;
//    stat(input_path.c_str(), &s);
//    cout<<"Found file "<<input_path<<" with size "<<s.st_size<<", loading to memory..."<<endl;
//    buffer = new uint8_t[s.st_size + simdjson::SIMDJSON_PADDING];
//
//    FILE *pf = fopen(input_path.c_str(), "r");
//    fread(buffer, s.st_size, 1, pf);
//    fclose(pf);
//    pf = nullptr;
//    auto loading_time_in_s = load_timer.time();
//
//
//    // select function to use
//    auto functor = mode::best::process_pipeline;
//
//    if(mode == "best") {
//        functor = mode::best::process_pipeline;
//    } else if(mode == "cjson") {
//        functor = mode::cjson::process_pipeline;
//    } else if(mode == "cstruct") {
//        functor = mode::load_to_condensed_c_struct::process_pipeline;
//    } else if(mode == "yyjson") {
//        functor = mode::yyjson::process_pipeline;
//    } else {
//        throw std::runtime_error("unsupported mode " + mode);
//    }
//
//    // output file
//    FILE *pfout = nullptr;
//
//    Timer timer;
//    cout<<"Parsing file"<<endl;
//    size_t row_count = 0;
//    size_t output_row_count = 0;
//    simdjson::dom::parser parser;
//    simdjson::dom::document_stream stream;
//    auto error = parser.parse_many((const char*)buffer,
//                                   (size_t)s.st_size,
//                                   std::min(simdjson::SIMDJSON_MAXSIZE_BYTES, (size_t)s.st_size)).get(stream);
//    if (error) { /* do something */ }
//    auto i = stream.begin();
//    for(; i != stream.end(); ++i) {
//        auto doc = *i;
//        if (!doc.error()) {
//            // std::cout << "got full document at " << i.current_index() << std::endl;
//            //std::cout << i.source() << std::endl;
//
//            // process pipeline here based on simdjson doc:
//            output_row_count += functor(&pfout, output_path, doc.value());
//
//
//            row_count++;
//        } else {
//            std::cout << "got broken document at " << i.current_index() << std::endl;
//            break;
//        }
//    }
//    cout<<"Parsed "<<row_count<<" rows from file "<<input_path<<endl;
//    cout<<"Wrote "<<output_row_count<<" output rows to "<<output_path<<endl;
//    cout<<"Took "<<timer.time()<<"s to process"<<endl;
//
//    // lazy close file
//    if(pfout) {
//        fflush(pfout);
//        fclose(pfout);
//    }
//
//    delete [] buffer;

    size_t row_count = 0;
    size_t output_row_count = 0;
    double loading_time_in_s = 0.0;

    return make_tuple(row_count, output_row_count, loading_time_in_s);
}

int main(int argc, char* argv[]) {
    using namespace std;
    using namespace tuplex;

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