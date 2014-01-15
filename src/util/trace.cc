// Copyright (c) 2013, Cloudera, inc.

#include "util/trace.h"

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <iomanip>
#include <ios>
#include <ostream>
#include <strstream>
#include <string>
#include <vector>

#include "gutil/strings/substitute.h"
#include "gutil/walltime.h"
#include "util/memory/arena.h"

namespace kudu {

using strings::internal::SubstituteArg;

__thread Trace* Trace::threadlocal_trace_;

Trace::Trace()
  : arena_(new ThreadSafeArena(1024, 128*1024)),
    entries_head_(NULL),
    entries_tail_(NULL) {
}

Trace::~Trace() {
}

// Struct which precedes each entry in the trace.
struct TraceEntry {
  MicrosecondsInt64 timestamp_micros;

  // The source file and line number which generated the trace message.
  const char* file_path;
  int line_number;

  uint32_t message_len;
  TraceEntry* next;

  // The actual trace message follows the entry header.
  char* message() {
    return reinterpret_cast<char*>(this) + sizeof(*this);
  }
};

// Get the part of filepath after the last path separator.
// (Doesn't modify filepath, contrary to basename() in libgen.h.)
// Borrowed from glog.
static const char* const_basename(const char* filepath) {
  const char* base = strrchr(filepath, '/');
#ifdef OS_WINDOWS  // Look for either path separator in Windows
  if (!base)
    base = strrchr(filepath, '\\');
#endif
  return base ? (base+1) : filepath;
}


void Trace::SubstituteAndTrace(const char* file_path,
                               int line_number,
                               StringPiece format,
                               const SubstituteArg& arg0, const SubstituteArg& arg1,
                               const SubstituteArg& arg2, const SubstituteArg& arg3,
                               const SubstituteArg& arg4, const SubstituteArg& arg5,
                               const SubstituteArg& arg6, const SubstituteArg& arg7,
                               const SubstituteArg& arg8, const SubstituteArg& arg9) {
  const SubstituteArg* const args_array[] = {
    &arg0, &arg1, &arg2, &arg3, &arg4, &arg5, &arg6, &arg7, &arg8, &arg9, NULL
  };

  int msg_len = strings::internal::SubstitutedSize(format, args_array);
  TraceEntry* entry = NewEntry(msg_len, file_path, line_number);
  SubstituteToBuffer(format, args_array, entry->message());
  AddEntry(entry);
}

TraceEntry* Trace::NewEntry(int msg_len, const char* file_path, int line_number) {
  int size = sizeof(TraceEntry) + msg_len;
  uint8_t* dst = reinterpret_cast<uint8_t*>(arena_->AllocateBytes(size));
  TraceEntry* entry = reinterpret_cast<TraceEntry*>(dst);
  entry->timestamp_micros = GetCurrentTimeMicros();
  entry->message_len = msg_len;
  entry->file_path = file_path;
  entry->line_number = line_number;
  return entry;
}

void Trace::AddEntry(TraceEntry* entry) {
  boost::lock_guard<simple_spinlock> l(lock_);
  entry->next = NULL;

  if (entries_tail_ != NULL) {
    entries_tail_->next = entry;
  } else {
    DCHECK(entries_head_ == NULL);
    entries_head_ = entry;
  }
  entries_tail_ = entry;
}

void Trace::Dump(std::ostream* out) const {
  // Gather a copy of the list of entries under the lock. This is fast
  // enough that we aren't worried about stalling concurrent tracers
  // (whereas doing the logging itself while holding the lock might be
  // too slow, if the output stream is a file, for example).
  vector<TraceEntry*> entries;
  {
    boost::lock_guard<simple_spinlock> l(lock_);
    for (TraceEntry* cur = entries_head_;
         cur != NULL;
         cur = cur->next) {
      entries.push_back(cur);
    }
  }

  // Save original flags.
  std::ios::fmtflags save_flags(out->flags());

  BOOST_FOREACH(TraceEntry* e, entries) {
    // Log format borrowed from glog/logging.cc
    time_t secs_since_epoch = e->timestamp_micros / 1000000;
    int usecs = e->timestamp_micros % 1000000;
    struct tm tm_time;
    localtime_r(&secs_since_epoch, &tm_time);

    using std::setw;
    out->fill('0');

    *out << setw(2) << (1 + tm_time.tm_mon)
         << setw(2) << tm_time.tm_mday
         << ' '
         << setw(2) << tm_time.tm_hour  << ':'
         << setw(2) << tm_time.tm_min   << ':'
         << setw(2) << tm_time.tm_sec   << '.'
         << setw(6) << usecs
         << ' '
         << const_basename(e->file_path) << ':' << e->line_number
         << "] ";
    out->write(reinterpret_cast<char*>(e) + sizeof(TraceEntry),
               e->message_len);
    *out << std::endl;
  }

  // Restore stream flags.
  out->flags(save_flags);
}

string Trace::DumpToString() const {
  std::stringstream s;
  Dump(&s);
  return s.str();
}

} // namespace kudu
