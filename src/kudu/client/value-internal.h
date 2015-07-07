// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_VALUE_INTERNAL_H
#define KUDU_CLIENT_VALUE_INTERNAL_H

#include "kudu/gutil/macros.h"
#include "kudu/util/slice.h"

namespace kudu {
namespace client {

class KuduValue::Data {
 public:
  enum Type {
    INT,
    FLOAT,
    DOUBLE,
    SLICE
  };
  Type type_;
  union {
    int64_t int_val_;
    float float_val_;
    double double_val_;
  };
  Slice slice_val_;
};

} // namespace client
} // namespace kudu
#endif /* KUDU_CLIENT_VALUE_INTERNAL_H */
