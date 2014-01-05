// Copyright (c) 2013, Cloudera,inc.

#include "util/debug-util.h"

#include <execinfo.h>
#include <glog/logging.h>
#include "gutil/strings/numbers.h"

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

void HexStackTraceToString(char* buf, size_t size) {
  const size_t kHexEntryLength = 16;
  const size_t kMaxFrames = 64;
  void* frames[kMaxFrames];

  int num_frames = backtrace(frames, kMaxFrames);
  int rem = size;
  char* dst = buf;
  char* limit = dst + size - kHexEntryLength - 2;
  for (int i = 0; i < num_frames && dst < limit; i++) {
    if (i != 0) {
      *dst++ = ' ';
      rem--;
    }
    FastHex64ToBuffer(reinterpret_cast<uintptr_t>(frames[i]), dst);
    dst += kHexEntryLength;

  }
  *dst = '\0';
}

}  // namespace kudu
