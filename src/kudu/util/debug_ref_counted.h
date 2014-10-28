// Copyright (c) 2014 Cloudera, Inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_UTIL_DEBUG_REF_COUNTED_H_
#define KUDU_UTIL_DEBUG_REF_COUNTED_H_

#include <glog/logging.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/debug-util.h"

namespace kudu {

// For use in debugging. Change a ref-counted class to inherit from this,
// instead of RefCountedThreadSafe, and fill your logs with stack traces.
template <class T, typename Traits = DefaultRefCountedThreadSafeTraits<T> >
class DebugRefCountedThreadSafe : public RefCountedThreadSafe<T, Traits> {
 public:
  DebugRefCountedThreadSafe() {}

  void AddRef() const {
    RefCountedThreadSafe<T, Traits>::AddRef();
    LOG(INFO) << "Incremented ref on " << this << ":\n" << GetStackTrace();
  }

  void Release() const {
    LOG(INFO) << "Decrementing ref on " << this << ":\n" << GetStackTrace();
    RefCountedThreadSafe<T, Traits>::Release();
  }

 protected:
  ~DebugRefCountedThreadSafe() {}

 private:
  friend struct DefaultRefCountedThreadSafeTraits<T>;

  DISALLOW_COPY_AND_ASSIGN(DebugRefCountedThreadSafe);
};

} // namespace kudu

#endif // KUDU_UTIL_DEBUG_REF_COUNTED_H_
