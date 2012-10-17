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

#include <vector>
using std::vector;

#include "gutil/exception/failureor.h"
#include "gutil/exception/stack_trace.h"


// #include "testing/base/public/gunit.h"
#include <gtest/gtest.h>

namespace common {

TEST(StackTraceTest, Composing) {
  StackTrace trace;
  AddStackTraceElement("Foo", "foo.cc", 10, "foo_context", &trace);
  AddStackTraceElement("Bar", "bar.cc", 15, "bar_context", &trace);
  ASSERT_EQ(2, trace.element_size());
  EXPECT_EQ("Foo",         trace.element(0).function());
  EXPECT_EQ("foo.cc",      trace.element(0).filename());
  EXPECT_EQ(10,            trace.element(0).line());
  EXPECT_EQ("foo_context", trace.element(0).context());
  EXPECT_EQ("Bar",         trace.element(1).function());
  EXPECT_EQ("bar.cc",      trace.element(1).filename());
  EXPECT_EQ(15,            trace.element(1).line());
  EXPECT_EQ("bar_context", trace.element(1).context());
}

TEST(StackTraceTest, Dumping) {
  StackTrace trace;
  AddStackTraceElement("Foo", "foo.cc", 10, "foo_context", &trace);
  AddStackTraceElement("Bar", "bar.cc", 15, "bar_context", &trace);
  string out;
  EXPECT_EQ(
      "    at Foo(foo.cc:10) foo_context\n"
      "    at Bar(bar.cc:15) bar_context\n",
      AppendStackTraceDump(trace, &out));
}

}  // namespace
