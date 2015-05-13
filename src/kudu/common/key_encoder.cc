// Copyright (c) 2012, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <vector>

#include "kudu/gutil/singleton.h"

#include "kudu/common/common.pb.h"
#include "kudu/common/key_encoder.h"

namespace kudu {

// A resolver for Encoders
class EncoderResolver {
 public:
  const KeyEncoder &GetKeyEncoder(DataType t) {
    CHECK_LT(t, encoders_.size()) << "Unsupported DataType";
    return *encoders_[t];
  }

  const bool HasKeyEncoderForType(DataType t) {
    return t < encoders_.size();
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
    AddMapping<STRING>();
  }

  template<DataType Type> void AddMapping() {
    KeyEncoderTraits<Type> traits;
    encoders_.push_back(boost::shared_ptr<KeyEncoder>(new KeyEncoder(traits)));
  }

  friend class Singleton<EncoderResolver>;
  vector<boost::shared_ptr<KeyEncoder> > encoders_;
};

const KeyEncoder &GetKeyEncoder(DataType type) {
  return Singleton<EncoderResolver>::get()->GetKeyEncoder(type);
}

// Returns true if the type is allowed in keys.
const bool IsTypeAllowableInKey(DataType type) {
  return Singleton<EncoderResolver>::get()->HasKeyEncoderForType(type);
}

}  // namespace kudu
