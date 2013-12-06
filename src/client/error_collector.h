// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CLIENT_ERROR_COLLECTOR_H
#define KUDU_CLIENT_ERROR_COLLECTOR_H

#include <vector>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "gutil/ref_counted.h"
#include "util/locks.h"
#include "util/status.h"

namespace kudu {
namespace client {

class Error;
class Insert;

namespace internal {

class ErrorCollector : public base::RefCountedThreadSafe<ErrorCollector> {
 public:
  ErrorCollector();

  void AddError(gscoped_ptr<Error> error);

  // See KuduSession for details.
  int CountErrors() const;

  // See KuduSession for details.
  void GetErrors(std::vector<Error*>* errors, bool* overflowed);

 private:
  friend class base::RefCountedThreadSafe<ErrorCollector>;
  virtual ~ErrorCollector();

  mutable simple_spinlock lock_;
  std::vector<Error*> errors_;

  DISALLOW_COPY_AND_ASSIGN(ErrorCollector);
};

} // namespace internal
} // namespace client
} // namespace kudu
#endif /* KUDU_CLIENT_ERROR_COLLECTOR_H */
