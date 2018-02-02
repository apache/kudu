// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <memory>
#include <string>
#include <vector>

#include "kudu/common/common.pb.h"
#include "kudu/common/key_encoder.h"
#include "kudu/gutil/singleton.h"

using std::unique_ptr;
using std::vector;

namespace kudu {

class faststring;

// A resolver for Encoders
template <typename Buffer>
class EncoderResolver {
 public:
  const KeyEncoder<Buffer>& GetKeyEncoder(DataType t) {
    DCHECK(HasKeyEncoderForType(t));
    return *encoders_[t];
  }

  const bool HasKeyEncoderForType(DataType t) {
    return t < encoders_.size() && encoders_[t];
  }

 private:
  EncoderResolver<Buffer>() {
    AddMapping<UINT8>();
    AddMapping<INT8>();
    AddMapping<UINT16>();
    AddMapping<INT16>();
    AddMapping<UINT32>();
    AddMapping<INT32>();
    AddMapping<UINT64>();
    AddMapping<INT64>();
    AddMapping<BINARY>();
    AddMapping<INT128>();
  }

  template<DataType Type> void AddMapping() {
    KeyEncoderTraits<Type, Buffer> traits;
    if (encoders_.size() <= Type) {
      encoders_.resize(static_cast<size_t>(Type) + 1);
    }
    CHECK(!encoders_[Type]) << "already have mapping for " << DataType_Name(Type);
    encoders_[Type].reset(new KeyEncoder<Buffer>(traits));
  }

  friend class Singleton<EncoderResolver<Buffer>>;
  // We use a vector instead of a map here since this shows up in some hot paths
  // and we know that the valid data types all have low enough IDs that the
  // vector will be small.
  vector<unique_ptr<KeyEncoder<Buffer>>> encoders_;
};

template <typename Buffer>
const KeyEncoder<Buffer>& GetKeyEncoder(const TypeInfo* typeinfo) {
  return Singleton<EncoderResolver<Buffer>>::get()->GetKeyEncoder(typeinfo->physical_type());
}

// Returns true if the type is allowed in keys.
const bool IsTypeAllowableInKey(const TypeInfo* typeinfo) {
  return Singleton<EncoderResolver<faststring>>::get()->HasKeyEncoderForType(
      typeinfo->physical_type());
}

//------------------------------------------------------------
//// Template instantiations: We instantiate all possible templates to avoid linker issues.
//// see: https://isocpp.org/wiki/faq/templates#separate-template-fn-defn-from-decl
////------------------------------------------------------------

template
const KeyEncoder<std::string>& GetKeyEncoder(const TypeInfo* typeinfo);

template
const KeyEncoder<faststring>& GetKeyEncoder(const TypeInfo* typeinfo);

}  // namespace kudu
