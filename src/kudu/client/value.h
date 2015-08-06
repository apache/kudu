// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_VALUE_H
#define KUDU_CLIENT_VALUE_H

#ifdef KUDU_HEADERS_NO_STUBS
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#else
#include "kudu/client/stubs.h"
#endif
#include "kudu/util/slice.h"
#include "kudu/util/kudu_export.h"

namespace kudu {
namespace client {

// A constant cell value with a specific type.
class KUDU_EXPORT KuduValue {
 public:
  // Return a new identical KuduValue object.
  KuduValue* Clone() const;

  // Construct a KuduValue from the given integer.
  static KuduValue* FromInt(int64_t v);

  // Construct a KuduValue from the given float.
  static KuduValue* FromFloat(float f);

  // Construct a KuduValue from the given double.
  static KuduValue* FromDouble(double d);

  // Construct a KuduValue from the given bool.
  static KuduValue* FromBool(bool b);

  // Construct a KuduValue by copying the value of the given Slice.
  static KuduValue* CopyString(Slice s);

  ~KuduValue();
 private:
  friend class ComparisonPredicateData;
  friend class KuduColumnSpec;

  class KUDU_NO_EXPORT Data;
  explicit KuduValue(Data* d);

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduValue);
};

} // namespace client
} // namespace kudu
#endif /* KUDU_CLIENT_VALUE_H */
