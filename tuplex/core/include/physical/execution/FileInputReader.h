//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_FILEINPUTREADER_H
#define TUPLEX_FILEINPUTREADER_H

#include <URI.h>
#include <VirtualFileSystem.h>

namespace tuplex {

    struct PartialReadInfo {
        URI uri; // last URI read.
        size_t offset; // offset in file.
        size_t row_count; // how many rows read in file.

        PartialReadInfo() : offset(0), row_count(0) {}
    };

    class FileInputReader {
    private:
        PartialReadInfo _partial_read_result;
    protected:
        void setPartialReadInformation(const PartialReadInfo& pri) {
            assert(pri.uri != URI::INVALID); // used for partial read indication.
            _partial_read_result = pri;
        }

    public:
        virtual ~FileInputReader() {}
        virtual void read(const URI& inputFilePath, const std::function<bool()>& blockFunctor) = 0;

        void read(const URI& inputFilePath) {
            read(inputFilePath, nullptr);
        }
        virtual size_t inputRowCount() const = 0;

        PartialReadInfo getPartialReadInformation() const {
            return _partial_read_result;
        }

        bool isOnlyPartiallyRead() const {
            return _partial_read_result.uri != URI::INVALID;
        }
    };
}

#endif //TUPLEX_FILEINPUTREADER_H