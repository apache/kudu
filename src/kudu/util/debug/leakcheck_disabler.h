// Copyright (c) 2015 Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_DEBUG_LEAKCHECK_DISABLER_H_
#define KUDU_UTIL_DEBUG_LEAKCHECK_DISABLER_H_

#include <gperftools/heap-checker.h>
#include "kudu/gutil/macros.h"
#include "kudu/util/debug/leak_annotations.h"

namespace kudu {
namespace debug {

// Scoped object that generically disables leak checking in a given scope,
// supporting both the tcmalloc heap leak checker and LSAN.
// While this object is alive, calls to "new" will not be checked for leaks.
class ScopedLeakCheckDisabler {
 public:
  ScopedLeakCheckDisabler() {}

 private:
#ifdef TCMALLOC_ENABLED
  HeapLeakChecker::Disabler hlc_disabler;
#endif

#if defined(__has_feature)
#  if __has_feature(address_sanitizer)
  ScopedLSANDisabler lsan_disabler;
#  endif
#endif

  DISALLOW_COPY_AND_ASSIGN(ScopedLeakCheckDisabler);
};

} // namespace debug
} // namespace kudu

#endif // KUDU_UTIL_DEBUG_LEAKCHECK_DISABLER_H_
