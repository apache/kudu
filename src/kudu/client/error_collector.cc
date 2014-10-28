// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/client.h"
#include "kudu/client/error_collector.h"

#include <vector>

#include "kudu/gutil/stl_util.h"

namespace kudu {
namespace client {
namespace internal {

ErrorCollector::ErrorCollector() {
}

ErrorCollector::~ErrorCollector() {
  STLDeleteElements(&errors_);
}

void ErrorCollector::AddError(gscoped_ptr<KuduError> error) {
  lock_guard<simple_spinlock> l(&lock_);
  errors_.push_back(error.release());
}

int ErrorCollector::CountErrors() const {
  lock_guard<simple_spinlock> l(&lock_);
  return errors_.size();
}

void ErrorCollector::GetErrors(std::vector<KuduError*>* errors, bool* overflowed) {
  lock_guard<simple_spinlock> l(&lock_);
  errors->swap(errors_);
  *overflowed = false;
}


} // namespace internal
} // namespace client
} // namespace kudu
