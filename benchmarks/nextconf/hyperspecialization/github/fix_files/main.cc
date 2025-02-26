//
// Created by leonhards on 9/1/24.
//

// Helper binary to fix up downloaded github files.

#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <iostream>
#include <vector>
#include <simdjson.h>
#include <glob.h>
#include <sstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <lyra/lyra.hpp>
#include <filesystem>


using namespace std;

unordered_map<string, unordered_map<int64_t, int64_t>> split_points{make_pair("/hot/data/github_download/monthly/2018-10.json",
                                                                              unordered_map<int64_t, int64_t>{make_pair(28538086, 18902), make_pair(28586340, 16053 + 4)}),
                                                                    make_pair("/hot/data/github_download/monthly/2020-10.json",
                                                                              unordered_map<int64_t, int64_t>{make_pair(41838626, 6079 + 5), make_pair(73346146, 919), make_pair(81547712, 6238+4)})};

// trim from start (in place)
inline void ltrim(std::string &s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) {
        return !std::isspace(ch);
    }));
}

// trim from end (in place)
inline void rtrim(std::string &s) {
    s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) {
        return !std::isspace(ch);
    }).base(), s.end());
}

// trim from both ends (in place)
inline void trim(std::string &s) {
    ltrim(s);
    rtrim(s);
}

inline std::string trim(const std::string& s) {
    std::string copy(s.c_str());
    ltrim(copy);
    rtrim(copy);
    return copy;
}


void write_line(std::ofstream& ofs, string line) {
    trim(line);

    if(!line.empty())
        ofs << line << "\n";
}

// limited getline
std::istream& custom_getline(std::istream& is, std::string& s, size_t max_chars =1024 * 1024 * 48, char delim = '\n'){
    s.clear();
    char c;
    std::string temp;
    if(is.get(c)){
        temp.push_back(c);
        while((is.get(c)) && (c != delim))
            temp.push_back(c);
        if(!is.bad())
            s = temp;
        if(!is.bad() && is.eof())
            is.clear(std::ios_base::eofbit);
        if(temp.size() > max_chars)
            throw std::runtime_error("max char size of " + std::to_string(max_chars) + " exceeded in getline.");
    }
    return is;
}


int check_for_parsing_errors(const std::string& path, size_t *doc_count=nullptr) {
    using namespace std;

    ifstream infile(path);

    if(!infile.good()) {
        cerr<<"Failed reading "<<path<<endl;
        return -2;
    }


    // Get file size.
    auto file_size = std::filesystem::file_size(path);
    cout<<"Found file of size: "<<file_size/1024/1024<<" MB"<<endl;

    // every 1GB print out
    size_t print_delta = 1024 * 1024 * 1024;
    size_t next_print = print_delta;

    std::string line;
    size_t line_count = 0;
    size_t parsed_docs = 0;
    size_t bytes_read = 0;
//    while(std::getline(infile, line)) {
    while(custom_getline(infile, line)) {

        if(line.empty())
            continue;

        bytes_read += line.size();

        if(bytes_read >= next_print) {
            double percent_read = (double)bytes_read / (double)file_size * 100.0;
            cout<<"-- read "<<(bytes_read / 1024 / 1024)<<" MB / "<<file_size/1024/1024<<" MB ("<<std::fixed<<std::setprecision(1)<<percent_read<<"%)"<<endl;
            next_print += print_delta;
        }

        auto json = simdjson::padded_string(line);
        size_t max_document_size = 128 * 1024 * 1024; // support 128MB max.
        simdjson::ondemand::parser parser(max_document_size);
        simdjson::ondemand::document_stream stream;
        auto error = parser.iterate_many(json).get(stream);
        if (error) {
            cerr<<"line "<<line_count<<": simdjson error: "<<error_message(error)<<endl;
            return false;
        }
        auto i = stream.begin();
        size_t count{0};
        for(; i != stream.end(); ++i) {
            auto doc = *i;
            if (!i.error()) {
                //std::cout << "got full document at " << i.current_index() << std::endl;
                //std::cout << i.source() << std::endl;
                count++;
            } else {
                std::cout << "line "<<line_count<<": got broken document at " << i.current_index() << std::endl;
                std::cout<<"parsed so far "<<count<<" documents."<<endl;
                std::cout<<"simdjson error: "<<error_message(i.error())<<endl;
                std::cout<<"Offending line:\n"<<line<<endl;
                // std::cout<<"Details:\n"<<i.source()<<endl;
                std::cerr<<"skipping this line (for now)..."<<std::endl;
                // return false;
            }
        }

        parsed_docs += count;
        if(count != 1) {
            cout<<"line "<<line_count<<": Got "<<count<<" documents, expected 1 document"<<endl;
        }
        line_count++;
    }

    cout<<"Read file with "<<line_count<<" lines"<<endl;
    cout<<"Parsed in total "<<parsed_docs<<" for "<<line_count<<" lines"<<endl;

    if(doc_count)
        *doc_count = parsed_docs;

    return line_count;
}

void write_fixed_doc(const std::string& path, const std::string& output_path, size_t total_line_count) {
    using namespace std;

    ifstream infile(path);
    ofstream ofs(output_path, ofstream::out);
    if(!infile.good()) {
        cerr<<"Failed reading "<<path<<endl;
        return;
    }

    // Get file size.
    auto file_size = std::filesystem::file_size(path);
    cout<<"Found file of size: "<<file_size/1024/1024<<" MB to fix up ("<<total_line_count<<" lines given)"<<endl;

    std::string line;
    size_t line_count = 0;
    size_t parsed_docs = 0;
    while(std::getline(infile, line)) {

        auto json = simdjson::padded_string(line);
        simdjson::ondemand::parser parser;
        simdjson::ondemand::document_stream stream;
        auto error = parser.iterate_many(json).get(stream);
        if (error) {


            /* do something */ }
        auto i = stream.begin();
        size_t count{0};
        for(; i != stream.end(); ++i) {
            auto doc = *i;
            if (!i.error()) {
                // write file (newline delimited)
                std::stringstream ss;
                ss<<i.source();
                auto output_line = ss.str();
                trim(output_line);

                if(output_line.empty())
                    continue;

                output_line += "\n"; // add line ending.

                ofs<<output_line;

                count++;
            } else {
                std::cerr << "line "<<line_count<<": got broken document at " << i.current_index() << std::endl;
            }
        }

        parsed_docs += count;
        if(count != 1) {
            cout<<"line "<<line_count<<": Got "<<count<<" documents, expected 1 document"<<endl;
        }
        line_count++;
    }

    infile.close();
    ofs.close();

    cout<<"Read file with "<<line_count<<" lines"<<endl;
    cout<<"Wrote in total "<<parsed_docs<<" to "<<output_path<<" ("<<std::filesystem::file_size(output_path)/1024/1024<<" MB)"<<endl;
}

int main(int argc, char** argv) {

    bool show_help=false;
    std::string input_path;
    // construct CLI
    auto cli = lyra::cli();
    cli.add_argument(lyra::help(show_help));
    cli.add_argument(lyra::opt(input_path, "inputPath").name("-i").name("--input-path").help("input path from which to read file."));
    auto result = cli.parse({argc, argv});
    if(!result) {
        cerr<<"Error parsing command line: "<<result.errorMessage()<<std::endl;
        return 1;
    }

    if(show_help) {
        cout<<cli<<endl;
        return 0;
    }

    if(input_path.empty()) {
        cerr<<"Must specify input path via -i or --input-path."<<endl;
        return 1;
    }

    try {
        cout<<"Checking "<<input_path<<" for JSON errors."<<endl;
        size_t doc_count = 0;
        auto line_count = check_for_parsing_errors(input_path, &doc_count);
        if(doc_count != line_count) {
            cout<<"Document count does not match line count, need to fix up file -> writing to "<<input_path<<".fixed"<<endl;

            write_fixed_doc(input_path, input_path + ".fixed", line_count);


            cout<<"Checking output file is ok..."<<endl;
            // check one more time.
            line_count = check_for_parsing_errors(input_path + ".fixed", &doc_count);
            if(line_count == doc_count) {
                cout<<"SUCCESS! line count matches doc count now."<<endl;
                return 0;
            } else {
                cerr<<"Something went wrong..."<<endl;
                return 1;
            }
        }

        cout<<"file ok. No further action needed."<<endl;
    } catch(const std::exception& e) {
        cerr<<"Program aborted with: "<<e.what()<<endl;
        return 1;
    }

    return 0;
}