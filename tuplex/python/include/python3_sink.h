//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PYTHON3_SINK_H
#define TUPLEX_PYTHON3_SINK_H

#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/formatter.h>
#include <spdlog/details/null_mutex.h>
#include <mutex>


namespace tuplex {
    /*!
     * helper class to print logs out to python output. Required because of Jupyter notebooks
     * who do not print stdout/stderr by default...
     */
    template<typename Mutex> class python3_sink : public spdlog::sinks::base_sink <Mutex> {
    protected:
        virtual void sink_it_(const spdlog::details::log_msg& msg) override {
            fmt::memory_buffer formatted;
            this->formatter_->format(msg, formatted);
            std::string formatted_msg = fmt::to_string(formatted);
        }

        virtual void flush_() override {
            // nothing to do...
            // PySys auto flushes...
        }
    };

    using python3_sink_mt = python3_sink<std::mutex>;
    using python3_sink_st = python3_sink<spdlog::details::null_mutex>;
}

#endif //TUPLEX_PYTHON3_SINK_H