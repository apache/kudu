// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_UTIL_TRACE_H
#define KUDU_UTIL_TRACE_H

#include <iosfwd>
#include <vector>

#include "gutil/macros.h"
#include "gutil/strings/stringpiece.h"
#include "gutil/strings/substitute.h"
#include "gutil/gscoped_ptr.h"
#include "util/locks.h"

namespace kudu {

class ThreadSafeArena;

struct TraceEntry;

// A trace for a request or other process. This supports collecting trace entries
// from a number of threads, and later dumping the results to a stream.
//
// This class is thread-safe.
class Trace {
 public:
  Trace();
  ~Trace();

  // Logs a message into the trace buffer.
  //
  // See strings::Substitute for details.
  void SubstituteAndTrace(StringPiece format,
                          const strings::internal::SubstituteArg& arg0 = strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg1 = strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg2 = strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg3 = strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg4 = strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg5 = strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg6 = strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg7 = strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg8 = strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg9 = strings::internal::SubstituteArg::NoArg);

  void Message(StringPiece s);

  // Dump the trace buffer to the given output stream.
  void Dump(std::ostream* out) const;

 private:

  TraceEntry* NewEntry(int len);

  mutable simple_spinlock lock_;
  gscoped_ptr<ThreadSafeArena> arena_;
  std::vector<TraceEntry*> entries_;

  DISALLOW_COPY_AND_ASSIGN(Trace);
};

} // namespace kudu
#endif /* KUDU_UTIL_TRACE_H */
