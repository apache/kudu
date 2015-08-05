// Copyright (c) 2012, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <tr1/unordered_map>
#include <vector>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/singleton.h"

#include "kudu/common/common.pb.h"
#include "kudu/common/key_encoder.h"

namespace kudu {

using std::tr1::unordered_map;

// A resolver for Encoders
class EncoderResolver {
 public:
  const KeyEncoder &GetKeyEncoder(DataType t) {
    return *FindOrDie(encoders_, t);
  }

  const bool HasKeyEncoderForType(DataType t) {
    return ContainsKey(encoders_, t);
  }

 private:
  EncoderResolver() {
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
    KeyEncoderTraits<Type> traits;
    InsertOrDie(&encoders_, Type, boost::shared_ptr<KeyEncoder>(new KeyEncoder(traits)));
  }

  friend class Singleton<EncoderResolver>;
  unordered_map<DataType,
                boost::shared_ptr<KeyEncoder>,
                std::tr1::hash<size_t> > encoders_;
};

const KeyEncoder &GetKeyEncoder(const TypeInfo* typeinfo) {
  return Singleton<EncoderResolver>::get()->GetKeyEncoder(typeinfo->physical_type());
}

// Returns true if the type is allowed in keys.
const bool IsTypeAllowableInKey(const TypeInfo* typeinfo) {
  return Singleton<EncoderResolver>::get()->HasKeyEncoderForType(typeinfo->physical_type());
}

}  // namespace kudu
