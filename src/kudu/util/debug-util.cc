// Copyright (c) 2013, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/debug-util.h"

#include <execinfo.h>
#include <glog/logging.h>
#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/numbers.h"

// Evil hack to grab a function from glog
namespace google {
namespace glog_internal_namespace_ {
extern void DumpStackTraceToString(std::string *s);
}}

namespace kudu {

std::string GetStackTrace() {
  std::string s;
  google::glog_internal_namespace_::DumpStackTraceToString(&s);
  return s;
}

std::string GetStackTraceHex() {
  char buf[1024];
  HexStackTraceToString(buf, 1024);
  return std::string(buf);
}

void HexStackTraceToString(char* buf, size_t size) {
  StackTrace trace;
  trace.Collect();
  trace.StringifyToHex(buf, size);
}

void StackTrace::Collect() {
  num_frames_ = backtrace(frames_, arraysize(frames_));
}

void StackTrace::StringifyToHex(char* buf, size_t size) const {
  char* dst = buf;

  // Reserve kHexEntryLength for the first iteration of the loop, 1 byte for a
  // space (which we may not need if there's just one frame), and 1 for a nul
  // terminator.
  char* limit = dst + size - kHexEntryLength - 2;
  for (int i = 0; i < num_frames_ && dst < limit; i++) {
    if (i != 0) {
      *dst++ = ' ';
    }
    FastHex64ToBuffer(reinterpret_cast<uintptr_t>(frames_[i]), dst);
    dst += kHexEntryLength;
  }
  *dst = '\0';
}

string StackTrace::ToHexString() const {
  // Each frame requires kHexEntryLength, plus a space
  // We also need one more byte at the end for '\0'
  char buf[kMaxFrames * (kHexEntryLength + 1) + 1];
  StringifyToHex(buf, arraysize(buf));
  return string(buf);
}

}  // namespace kudu
