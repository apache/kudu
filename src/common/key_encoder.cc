// Copyright (c) 2012, Cloudera, inc.

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <vector>

#include "gutil/singleton.h"

#include "common/common.pb.h"
#include "common/key_encoder.h"

namespace kudu {

// A resolver for Encoders
class EncoderResolver {
 public:
  const KeyEncoder &GetKeyEncoder(DataType t) {
    CHECK_LT(t, encoders_.size()) << "Unsupported DataType";
    return *encoders_[t];
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
    AddMapping<BOOL>();
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

}  // namespace kudu
