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

  // Allocate a new entry from the arena, with enough space to hold a
  // message of length 'len'.
  TraceEntry* NewEntry(int len);

  // Add the entry to the linked list of entries.
  void AddEntry(TraceEntry* entry);

  gscoped_ptr<ThreadSafeArena> arena_;

  // Lock protecting the entries linked list.
  mutable simple_spinlock lock_;
  // The head of the linked list of entries (allocated inside arena_)
  TraceEntry* entries_head_;
  // The tail of the linked list of entries (allocated inside arena_)
  TraceEntry* entries_tail_;

  DISALLOW_COPY_AND_ASSIGN(Trace);
};

} // namespace kudu
#endif /* KUDU_UTIL_TRACE_H */
