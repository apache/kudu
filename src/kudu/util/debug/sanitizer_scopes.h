// Copyright (c) 2015 Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Wrappers around the annotations from gutil/dynamic_annotations.h,
// provided as C++-style scope guards.
#ifndef KUDU_UTIL_DEBUG_SANITIZER_SCOPES_H_
#define KUDU_UTIL_DEBUG_SANITIZER_SCOPES_H_

#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/macros.h"

namespace kudu {
namespace debug {

// Scope guard which instructs TSAN to ignore all reads and writes
// on the current thread as long as it is alive. These may be safely
// nested.
class ScopedTSANIgnoreReadsAndWrites {
 public:
  ScopedTSANIgnoreReadsAndWrites() {
    ANNOTATE_IGNORE_READS_AND_WRITES_BEGIN();
  }
  ~ScopedTSANIgnoreReadsAndWrites() {
    ANNOTATE_IGNORE_READS_AND_WRITES_END();
  }
 private:
  DISALLOW_COPY_AND_ASSIGN(ScopedTSANIgnoreReadsAndWrites);
};

} // namespace debug
} // namespace kudu

#endif  // KUDU_UTIL_DEBUG_SANITIZER_SCOPES_H_
