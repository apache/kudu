// Copyright (c) 2014 Cloudera, Inc.

#ifndef KUDU_UTIL_DEBUG_REF_COUNTED_H_
#define KUDU_UTIL_DEBUG_REF_COUNTED_H_

#include <glog/logging.h>

#include "gutil/ref_counted.h"
#include "util/debug-util.h"

namespace kudu {

// For use in debugging. Change a ref-counted class to inherit from this,
// instead of base::RefCountedThreadSafe, and fill your logs with stack traces.
template <class T, typename Traits = base::DefaultRefCountedThreadSafeTraits<T> >
class DebugRefCountedThreadSafe : public base::RefCountedThreadSafe<T, Traits> {
 public:
  DebugRefCountedThreadSafe() {}

  void AddRef() const {
    base::RefCountedThreadSafe<T, Traits>::AddRef();
    LOG(INFO) << "Incremented ref on " << this << ":\n" << GetStackTrace();
  }

  void Release() const {
    LOG(INFO) << "Decrementing ref on " << this << ":\n" << GetStackTrace();
    base::RefCountedThreadSafe<T, Traits>::Release();
  }

 protected:
  ~DebugRefCountedThreadSafe() {}

 private:
  friend struct base::DefaultRefCountedThreadSafeTraits<T>;

  DISALLOW_COPY_AND_ASSIGN(DebugRefCountedThreadSafe);
};

} // namespace kudu

#endif // KUDU_UTIL_DEBUG_REF_COUNTED_H_
