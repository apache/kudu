// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/util/trace.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <map>
#include <ostream>  // IWYU pragma: keep
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/container/small_vector.hpp>
#include <boost/container/vector.hpp>
#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/logging.h"
#include "kudu/util/memory/arena.h"

using std::ostream;
using std::ostringstream;
using std::pair;
using std::string;
using std::stringstream;
using std::vector;
using strings::internal::SubstituteArg;

namespace kudu {

thread_local Trace* Trace::threadlocal_trace_;

Trace::Trace()
    : arena_(new ThreadSafeArena(1024)),
      entries_head_(nullptr),
      entries_tail_(nullptr) {
  // We expect small allocations from our Arena so no need to have
  // a large arena component. Small allocations are more likely to
  // come out of thread cache and be fast.
  arena_->SetMaxBufferSize(4096);
  // Reserve space for up to 16 child traces upfront to avoid re-allocations
  // upon calling of AddChildTrace().
  child_traces_.reserve(16);
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
  const char* message() const {
    return reinterpret_cast<const char*>(this) + sizeof(*this);
  }
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
    &arg0, &arg1, &arg2, &arg3, &arg4, &arg5, &arg6, &arg7, &arg8, &arg9, nullptr
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
  std::lock_guard l(lock_);
  entry->next = nullptr;

  if (entries_tail_ != nullptr) {
    entries_tail_->next = entry;
  } else {
    DCHECK(entries_head_ == nullptr);
    entries_head_ = entry;
  }
  entries_tail_ = entry;
}

void Trace::Dump(ostream* out, int flags) const {
  // Gather a copy of the list of entries under the lock. This is fast
  // enough that we aren't worried about stalling concurrent tracers
  // (whereas doing the logging itself while holding the lock might be
  // too slow, if the output stream is a file, for example).
  // To avoid memory re-allocations, reserve space for up to 64 trace entries;
  // this should be enough for majority of cases.
  vector<TraceEntry*> entries;
  entries.reserve(64);
  vector<pair<StringPiece, scoped_refptr<Trace>>> child_traces;
  {
    std::lock_guard l(lock_);
    for (TraceEntry* cur = entries_head_; cur != nullptr; cur = cur->next) {
      entries.push_back(cur);
    }

    child_traces = child_traces_;
  }

  // Save original flags.
  auto& os = *out;
  const std::ios::fmtflags saved_flags(os.flags());

  int64_t prev_usecs = 0;
  for (const auto* e : entries) {
    // Log format borrowed from glog/logging.cc
    int64_t usecs_since_prev = 0;
    if (prev_usecs != 0) {
      usecs_since_prev = e->timestamp_micros - prev_usecs;
    }
    prev_usecs = e->timestamp_micros;

    os << FormatTimestampForLog(e->timestamp_micros);
    os << ' ';
    if (flags & INCLUDE_TIME_DELTAS) {
      os.fill(' ');
      os << "(+" << std::setw(6) << usecs_since_prev << "us) ";
    }
    os << const_basename(e->file_path) << ':' << e->line_number << "] ";
    os.write(e->message(), e->message_len);
    os << std::endl;
  }

  for (const auto& [key, trace] : child_traces) {
    os << "Related trace '" << key << "':" << std::endl;
    trace->Dump(&os, flags & (~INCLUDE_METRICS));
  }

  if (flags & INCLUDE_METRICS) {
    stringstream s;
    JsonWriter jw(&s, JsonWriter::COMPACT);
    MetricsToJSON(&jw);
    os << "Metrics: " << s.rdbuf();
  }

  // Restore stream flags.
  os.flags(saved_flags);
}

string Trace::DumpToString(int flags) const {
  ostringstream s;
  Dump(&s, flags);
  return s.str();
}

string Trace::MetricsAsJSON() const {
  ostringstream s;
  JsonWriter jw(&s, JsonWriter::COMPACT);
  MetricsToJSON(&jw);
  return s.str();
}

void Trace::MetricsToJSON(JsonWriter* jw) const {
  // Convert into a map with StringPiece keys instead of 'const char*'
  // pointer-style keys, so that the results are sorted by the metric name.
  const auto& metrics = metrics_.Get();
  boost::container::small_vector<pair<StringPiece, int64_t>, 32> counters;
  std::copy(metrics.begin(), metrics.end(), std::back_inserter(counters));
  std::sort(counters.begin(),
            counters.end(),
            [](const auto& lhs, const auto& rhs) {
              return lhs.first < rhs.first;
            }
  );

  jw->StartObject();
  for (const auto& [key, val] : counters) {
    jw->String(key.data(), key.size());
    jw->Int64(val);
  }
  vector<pair<StringPiece, scoped_refptr<Trace>>> child_traces;
  {
    std::lock_guard l(lock_);
    child_traces = child_traces_;
  }

  if (!child_traces.empty()) {
    jw->String("child_traces");
    jw->StartArray();

    for (const auto& e : child_traces) {
      jw->StartArray();
      jw->String(e.first.data(), e.first.size());
      e.second->MetricsToJSON(jw);
      jw->EndArray();
    }
    jw->EndArray();
  }
  jw->EndObject();
}

void Trace::DumpCurrentTrace() {
  Trace* t = CurrentTrace();
  if (t == nullptr) {
    LOG(INFO) << "No trace is currently active.";
    return;
  }
  t->Dump(&std::cerr, INCLUDE_TIME_DELTAS);
}

void Trace::AddChildTrace(StringPiece label, Trace* child_trace) {
  CHECK(arena_->RelocateStringPiece(label, &label));

  std::lock_guard l(lock_);
  child_traces_.emplace_back(label, child_trace);
}

vector<pair<StringPiece, scoped_refptr<Trace>>> Trace::ChildTraces() const {
  std::lock_guard l(lock_);
  return child_traces_;
}

ostream& operator<<(ostream& os, const Trace* tptr) {
  DCHECK(tptr) << "null trace";
  if (tptr) {
    tptr->Dump(&os);
  } else {
    // A special case for null trace targeting release builds where null trace
    // isn't caught (if ever) by the DCHECK() above.
    os << "{null}";
  }
  return os;
}

ostream& operator<<(ostream& os, const Trace& t) {
  return os << &t;
}

ostream& operator<<(ostream& os, const scoped_refptr<Trace>& trefptr) {
  return os << trefptr.get();
}


} // namespace kudu
