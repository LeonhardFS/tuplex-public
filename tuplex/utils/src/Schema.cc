//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Schema.h>
#include <map>

namespace tuplex {
    const Schema Schema::UNKNOWN = Schema(Schema::MemoryLayout::UNKNOWN, python::Type::UNKNOWN);


    bool Schema::hasFixedSize() const {
        assert(_rowType.isTupleType());
        // schema has a fixed size if no varargs are within it
        return _rowType.isFixedSizeType();
    }


    int Schema::recursiveSizeEstimator(const python::Type &t) const {

        std::map<python::Type, int> serializationSizes = {{python::Type::I64, 8},
                                                          {python::Type::F64, 8},
                                                          {python::Type::BOOLEAN, 8},
                                                          };

        // branch on tuple and primitive type
        if(t.isTupleType()) {
            int sum = 0;
            for(auto param : t.parameters()) {
                sum += recursiveSizeEstimator(param);
            }
            return sum;
        } else {
            // fixed size type?
            if(t.isFixedSizeType()) {
                auto it = serializationSizes.find(t);
                if(it != serializationSizes.end())
                    return it->second;
                else {
                    Logger::instance().logger("schema").error("unknown fixed size type '"
                                                              + t.desc() +
                                                                      "' encountered, could determine fixed schema size");
                    return 0;
                }
            } else {
                Logger::instance().logger("schema").error("var size types can't be used "
                                                                  "for size estimation of fixed length schema");
                return 0;
            }
        }
    }

    void check_column_names(const std::vector<std::string>& columns) {
        // Column names must be:
        // a.) non-empty
        // b.) not be _\d+ (i.e. _0, _1, ..., _\d+)
        for (const auto& name : columns) {
            if (name.empty())
                throw std::invalid_argument(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " column name must be non-empty");
            // check that column name is NOT _\d+
            if (name.at(0) == '_') {
                for (unsigned i = 1; i < name.size(); ++i)
                    if (isdigit(name.at(i)))
                        throw std::invalid_argument(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " column name " + name + " reserved, can't use.");
            }
        }
    }
}