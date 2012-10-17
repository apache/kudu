// Copyright 2010 Google Inc.  All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//          jelonka@google.com (Ilona Gaweda)
//
// Encapsulation of errors.

#ifndef DATAWAREHOUSE_COMMON_EXCEPTION_STACK_TRACE_H_
#define DATAWAREHOUSE_COMMON_EXCEPTION_STACK_TRACE_H_

#include <string>
using std::string;
#include <vector>
using std::vector;

#include "gutil/exception/stack_trace.pb.h"
#include "gutil/strings/stringpiece.h"

namespace common {

void AddStackTraceElement(const StringPiece& function,
                          const StringPiece& filename,
                          int line,
                          const StringPiece& context,
                          StackTrace* trace);

// Appends a dump of the stack trace to the specified out string. For
// convenience, returns a const reference to *out.
const string& AppendStackTraceDump(const StackTrace& trace, string* out);

}  // namespace

#endif  // DATAWAREHOUSE_COMMON_EXCEPTION_STACK_TRACE_H_
