// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_SCAN_PREDICATE_INTERNAL_H
#define KUDU_CLIENT_SCAN_PREDICATE_INTERNAL_H

#include "kudu/client/value.h"
#include "kudu/client/value-internal.h"
#include "kudu/common/scan_spec.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {
namespace client {

class KuduPredicate::Data {
 public:
  Data();
  virtual ~Data();
  virtual Status AddToScanSpec(ScanSpec* spec) = 0;
};

// A predicate implementation which represents an error constructing
// some other predicate.
//
// This allows us to provide a simple API -- if a predicate fails to
// construct, we return an instance of this class instead of the requested
// predicate implementation. Then, when the caller adds it to a scanner,
// the error is returned.
class ErrorPredicateData : public KuduPredicate::Data {
 public:
  explicit ErrorPredicateData(const Status& s)
  : status_(s) {
  }

  virtual ~ErrorPredicateData() {
  }

  virtual Status AddToScanSpec(ScanSpec* spec) OVERRIDE {
    return status_;
  }

 private:
  Status status_;
};


// A simple binary comparison predicate between a column and
// a constant.
class ComparisonPredicateData : public KuduPredicate::Data {
 public:
  ComparisonPredicateData(const ColumnSchema& col,
                          KuduPredicate::ComparisonOp op,
                          KuduValue* value);
  virtual ~ComparisonPredicateData();

  virtual Status AddToScanSpec(ScanSpec* spec) OVERRIDE;

 private:
  friend class KuduScanner;

  // Check that 'val_' has the expected type 'type', returning
  // a nice error Status if not.
  Status CheckValType(KuduValue::Data::Type type,
                      const char* type_str) const;

  // Check that 'val_' is a boolean constant, and set *val_void to
  // point to it if so.
  Status CheckAndPointToBool(void** val_void);

  // Check that 'val_' is an integer constant within the valid range,
  // and set *val_void to point to it if so.
  Status CheckAndPointToInt(size_t int_size, void** val_void);

  // Check that 'val_' is a string constant, and set *val_void to
  // point to it if so.
  Status CheckAndPointToString(void** val_void);

  ColumnSchema col_;
  KuduPredicate::ComparisonOp op_;
  gscoped_ptr<KuduValue> val_;

  // Owned.
  ColumnRangePredicate* pred_;
};

} // namespace client
} // namespace kudu
#endif /* KUDU_CLIENT_SCAN_PREDICATE_INTERNAL_H */
