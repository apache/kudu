// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_UTIL_TRACE_H
#define KUDU_UTIL_TRACE_H

#include <iosfwd>
#include <string>

#include "gutil/macros.h"
#include "gutil/strings/stringpiece.h"
#include "gutil/strings/substitute.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/ref_counted.h"
#include "gutil/threading/thread_collision_warner.h"
#include "util/locks.h"

// Adopt a Trace on the current thread for the duration of the current
// scope. The old current Trace is restored when the scope is exited.
//
// 't' should be a Trace* pointer.
#define ADOPT_TRACE(t) kudu::ScopedAdoptTrace _adopt_trace(t);

// Issue a trace message, if tracing is enabled in the current thread.
// See Trace::SubstituteAndTrace for arguments.
// Example:
//  TRACE("Acquired txid $0", txid);
#define TRACE(format, substitutions...) \
  do { \
    kudu::Trace* _trace = Trace::CurrentTrace(); \
    if (_trace) { \
      _trace->SubstituteAndTrace((format), ##substitutions);  \
    } \
  } while (0);

namespace kudu {

class ThreadSafeArena;
struct TraceEntry;

// A trace for a request or other process. This supports collecting trace entries
// from a number of threads, and later dumping the results to a stream.
//
// This class is thread-safe.
class Trace : public base::RefCountedThreadSafe<Trace> {
 public:
  Trace();

  // Logs a message into the trace buffer.
  //
  // See strings::Substitute for details.
  void SubstituteAndTrace(StringPiece format,
                          const strings::internal::SubstituteArg& arg0 =
                            strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg1 =
                            strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg2 =
                            strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg3 =
                            strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg4 =
                            strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg5 =
                            strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg6 =
                            strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg7 =
                            strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg8 =
                            strings::internal::SubstituteArg::NoArg,
                          const strings::internal::SubstituteArg& arg9 =
                            strings::internal::SubstituteArg::NoArg);

  void Message(StringPiece s);

  // Dump the trace buffer to the given output stream.
  void Dump(std::ostream* out) const;

  // Dump the trace buffer as a string.
  std::string DumpToString() const;

  // Return the current trace attached to this thread, if there is one.
  static Trace* CurrentTrace() {
    return threadlocal_trace_;
  }

 private:
  friend class ScopedAdoptTrace;
  friend class base::RefCountedThreadSafe<Trace>;
  ~Trace();

  // The current trace for this thread. Threads should only set this using
  // using ScopedAdoptTrace, which handles reference counting the underlying
  // object.
  static __thread Trace* threadlocal_trace_;

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

// Adopt a Trace object into the current thread for the duration
// of this object.
// This should only be used on the stack (and thus created and destroyed
// on the same thread)
class ScopedAdoptTrace {
 public:
  explicit ScopedAdoptTrace(Trace* t) :
    old_trace_(Trace::threadlocal_trace_) {
    Trace::threadlocal_trace_ = t;
    if (t) {
      t->AddRef();
    }
    DFAKE_SCOPED_LOCK_THREAD_LOCKED(ctor_dtor_);
  }

  ~ScopedAdoptTrace() {
    if (Trace::threadlocal_trace_) {
      Trace::threadlocal_trace_->Release();
    }
    Trace::threadlocal_trace_ = old_trace_;
    DFAKE_SCOPED_LOCK_THREAD_LOCKED(ctor_dtor_);
  }

 private:
  DFAKE_MUTEX(ctor_dtor_);
  Trace* old_trace_;

  DISALLOW_COPY_AND_ASSIGN(ScopedAdoptTrace);
};

} // namespace kudu
#endif /* KUDU_UTIL_TRACE_H */
