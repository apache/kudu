// Copyright (c) 2012, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <string>
#include <tr1/unordered_map>
#include <vector>

#include "kudu/common/common.pb.h"
#include "kudu/common/key_encoder.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/singleton.h"
#include "kudu/util/faststring.h"

namespace kudu {

using std::tr1::unordered_map;

// A resolver for Encoders
template <typename Buffer>
class EncoderResolver {
 public:
  const KeyEncoder<Buffer>& GetKeyEncoder(DataType t) {
    return *FindOrDie(encoders_, t);
  }

  const bool HasKeyEncoderForType(DataType t) {
    return ContainsKey(encoders_, t);
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
  }

  template<DataType Type> void AddMapping() {
    KeyEncoderTraits<Type, Buffer> traits;
    InsertOrDie(&encoders_, Type,
                boost::shared_ptr<KeyEncoder<Buffer> >(new KeyEncoder<Buffer>(traits)));
  }

  friend class Singleton<EncoderResolver<Buffer> >;
  unordered_map<DataType,
                boost::shared_ptr<KeyEncoder<Buffer> >,
                std::tr1::hash<size_t> > encoders_;
};

template <typename Buffer>
const KeyEncoder<Buffer>& GetKeyEncoder(const TypeInfo* typeinfo) {
  return Singleton<EncoderResolver<Buffer> >::get()->GetKeyEncoder(typeinfo->physical_type());
}

// Returns true if the type is allowed in keys.
const bool IsTypeAllowableInKey(const TypeInfo* typeinfo) {
  return Singleton<EncoderResolver<faststring> >::get()->HasKeyEncoderForType(
      typeinfo->physical_type());
}

//------------------------------------------------------------
//// Template instantiations: We instantiate all possible templates to avoid linker issues.
//// see: https://isocpp.org/wiki/faq/templates#separate-template-fn-defn-from-decl
////------------------------------------------------------------

template
const KeyEncoder<string>& GetKeyEncoder(const TypeInfo* typeinfo);

template
const KeyEncoder<faststring>& GetKeyEncoder(const TypeInfo* typeinfo);

}  // namespace kudu
