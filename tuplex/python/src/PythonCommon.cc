//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 11/9/2021                                                                //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <PythonCommon.h>


// include backward lib
#ifdef __APPLE__
// init backtrace
#ifndef BACKWARD_HAS_DWARF
#define BACKWARD_HAS_DWARF 1
#endif
#include <backward.hpp>
backward::SignalHandling sh;
#endif

#include <spdlog/sinks/callback_sink.h>

namespace tuplex {

    // Global sink objects (keep alive).
    std::vector<spdlog::sink_ptr> g_python_log_sinks;

    py::object getPythonVersion() {
        std::stringstream ss;
        ss<<PY_VERSION<<" ("<<PY_VERSION_HEX<<")";
        auto version_string = ss.str();
        return py::str(version_string);
    }

    void call_python_logging_with_message(PyObject* functor, const spdlog::details::log_msg& msg) {
        auto message = std::string(msg.payload.data());
        auto timestamp = msg.time;
        auto logger = std::string(msg.logger_name.begin(), msg.logger_name.end());
        auto level = msg.level;

        // perform callback in python...
        auto args = PyTuple_New(4);
        auto py_lvl = PyLong_FromLong(spdlog_level_to_number(msg.level));
        auto py_time = python::PyString_FromString(chronoToISO8601(timestamp).c_str());
        auto py_logger = python::PyString_FromString(logger.c_str());
        auto py_msg = python::PyString_FromString(message.c_str());
        PyTuple_SET_ITEM(args, 0, py_lvl);
        PyTuple_SET_ITEM(args, 1, py_time);
        PyTuple_SET_ITEM(args, 2, py_logger);
        PyTuple_SET_ITEM(args, 3, py_msg);

        Py_XINCREF(functor);
        Py_XINCREF(args);
        Py_XINCREF(py_lvl);
        Py_XINCREF(py_logger);
        Py_XINCREF(py_msg);

        PyObject_Call(functor, args, nullptr);
        if(PyErr_Occurred()) {
            PyErr_Print();
            std::cout<<std::endl;
            PyErr_Clear();
        }
    }


    py::object registerPythonLoggingCallback(py::object callback_functor) {

        // make this easier, i.e. via
        // https://github.com/gabime/spdlog/pull/2610/files -> callback.

        python::registerWithInterpreter();

        // get object
        callback_functor.inc_ref();
        auto functor_obj = callback_functor.ptr();

        if(!functor_obj) {
            std::cerr<<"invalid functor obj passed?"<<std::endl;
            return py::none();
        }

        // make sure it's callable etc.
        if(!PyCallable_Check(functor_obj))
            throw std::runtime_error(python::PyString_AsString(functor_obj) + " is not callable. Can't register as logger.");

        // check that func takes exactly 4 args
        // add new sink to loggers with this function
        python::unlockGIL();
        try {
            Py_XINCREF(functor_obj);
            // this replaces current logging scheme with python only redirect...
            auto& log = Logger::instance();

            // Save in global to avoid move issues.
            // this sink here has issues and leads to deadlock.
            // g_python_log_sinks.push_back(std::make_shared<no_gil_python3_sink_mt>(functor_obj));

            // Use callback log instead.
            g_python_log_sinks.push_back(std::make_shared<spdlog::sinks::callback_sink_mt>([functor_obj](const spdlog::details::log_msg &msg) {
                 // for example you can be notified by sending an email to yourself
                 spdlog::memory_buf_t formatted;
                 spdlog::pattern_formatter formatter;
                 formatter.format(msg, formatted);
                 auto eol_len = strlen(spdlog::details::os::default_eol);
                 std::string line(formatted.begin(), formatted.end() - eol_len);

                 // This is a fallback for debugging purposes, i.e. if code below fails.
                 // Note: logging should NOT be used between lockGIL()...unlockGIL().
                 // // print now out.
                 // spdlog::details::console_mutex m;
                 // m.mutex().lock();
                 // std::cout<<"-- callback -- "<<line<<std::endl;
                 // m.mutex().unlock();

                 // Log now via python (lock GIL!)
                 if(python::holdsGIL()) {
                     m.mutex().lock();
                     std::cerr<<"Can not log via python, thread is holding GIL. Original log message:\n"<<line<<std::endl;
                     m.mutex().unlock();
                 } else {
                     python::lockGIL();
                     call_python_logging_with_message(functor_obj, msg);
                     python::unlockGIL();
                 }
             }));

            log.init(g_python_log_sinks);
        } catch(const std::exception& e) {
            // use C printing for the exception here
            std::cerr<<"while registering python callback logging mechanism, following error occurred: "<<e.what()<<std::endl;
        }

        python::lockGIL();

        // return None
        return py::none();
    }
}