//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 12/2/2021                                                                //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "gtest/gtest.h"
#include "absl/debugging/failure_signal_handler.h"
#include "absl/debugging/stacktrace.h"
#include "absl/debugging/symbolize.h"
//#ifndef NDEBUG
//#include <mcheck.h>
//#endif

int main(int argc, char **argv) {
    // enable malloc tracing on linux
//#ifndef NDEBUG
//mtrace();
//#endif

   absl::InitializeSymbolizer(argv[0]);

   absl::FailureSignalHandlerOptions options;
   absl::InstallFailureSignalHandler(options);

    ::testing::InitGoogleTest(&argc, argv);
    int ret = RUN_ALL_TESTS();
    return ret;
}