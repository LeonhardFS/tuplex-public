//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 8/9/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#ifndef TUPLEX_CJSONDICTPROXYIMPL_H
#define TUPLEX_CJSONDICTPROXYIMPL_H

#include "BuiltinDictProxy.h"
#include "BuiltinDictProxyImpl.h"

namespace tuplex {
    namespace codegen {
        class cJSONDictProxyImpl : public BuiltinDictProxyImpl {
        public:
            void putItem(const Field& key, const Field& value) override;
            void putItem(const python::Type& keyType, const SerializableValue& key, const python::Type& valueType, const SerializableValue& value) override;
        };
    }
}

#endif //TUPLEX_CJSONDICTPROXYIMPL_H
