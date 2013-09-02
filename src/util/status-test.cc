// Copyright (c) 2013, Cloudera, inc.
//
// Some portions Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <errno.h>
#include <vector>
#include "util/status.h"

namespace kudu {

TEST(StatusTest, TestPosixCode) {
  Status ok = Status::OK();
  ASSERT_EQ(0, ok.posix_code());
  Status file_error = Status::IOError("file error", Slice(), ENOTDIR);
  ASSERT_EQ(ENOTDIR, file_error.posix_code());
}

TEST(StatusTest, TestToString) {
  Status file_error = Status::IOError("file error", Slice(), ENOTDIR);
  ASSERT_EQ(string("IO error: file error (error 20)"), file_error.ToString());
}

TEST(StatusTest, TestClonePrepend) {
  Status file_error = Status::IOError("file error", "msg2", ENOTDIR);
  Status appended = file_error.CloneAndPrepend("Heading");
  ASSERT_EQ(string("IO error: Heading: file error: msg2 (error 20)"), appended.ToString());
}

}  // namespace kudu
