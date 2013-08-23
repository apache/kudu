// Copyright (c) 2013, Cloudera,inc.

#include "util/debug-util.h"

#include <glog/logging.h>

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

}  // namespace kudu
