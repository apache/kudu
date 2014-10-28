// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_ERROR_COLLECTOR_H
#define KUDU_CLIENT_ERROR_COLLECTOR_H

#include <vector>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

namespace kudu {
namespace client {

class KuduError;
class KuduInsert;

namespace internal {

class ErrorCollector : public RefCountedThreadSafe<ErrorCollector> {
 public:
  ErrorCollector();

  void AddError(gscoped_ptr<KuduError> error);

  // See KuduSession for details.
  int CountErrors() const;

  // See KuduSession for details.
  void GetErrors(std::vector<KuduError*>* errors, bool* overflowed);

 private:
  friend class RefCountedThreadSafe<ErrorCollector>;
  virtual ~ErrorCollector();

  mutable simple_spinlock lock_;
  std::vector<KuduError*> errors_;

  DISALLOW_COPY_AND_ASSIGN(ErrorCollector);
};

} // namespace internal
} // namespace client
} // namespace kudu
#endif /* KUDU_CLIENT_ERROR_COLLECTOR_H */
