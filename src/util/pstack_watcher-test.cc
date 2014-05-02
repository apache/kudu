// Copyright (c) 2014, Cloudera, inc.

#include "util/pstack_watcher.h"

#include <gtest/gtest.h>
#include <poll.h>
#include <stdio.h>
#include <tr1/memory>
#include <vector>

#include "gutil/strings/substitute.h"
#include "util/bitmap.h"
#include "util/env.h"
#include "util/test_macros.h"

using std::string;
using std::tr1::shared_ptr;
using strings::Substitute;

namespace kudu {

TEST(TestPstackWatcher, TestPstackWatcherCancellation) {
  PstackWatcher watcher(MonoDelta::FromSeconds(1000000));
  watcher.Shutdown();
}

static shared_ptr<FILE> RedirectStdout(string *temp_path) {
  string temp_dir;
  CHECK_OK(Env::Default()->GetTestDirectory(&temp_dir));
  *temp_path = Substitute("$0/pstack_watcher-dump.$1.txt",
                      temp_dir, getpid());
  return shared_ptr<FILE>(
      freopen(temp_path->c_str(), "w", stdout), fclose);
}

TEST(TestPstackWatcher, TestPstackWatcherRunning) {
  string stdout_file;
  {
    shared_ptr<FILE> out_fp = RedirectStdout(&stdout_file);
    PstackWatcher watcher(MonoDelta::FromMilliseconds(500));
    while (watcher.IsRunning()) {
      usleep(1000);
    }
  }
  faststring contents;
  CHECK(ReadFileToString(Env::Default(), stdout_file, &contents).ok());
  ASSERT_STR_CONTAINS(contents.ToString(), "BEGIN STACKS");
  freopen("/dev/stdout", "w", stdout);
  unlink(stdout_file.c_str());
}

} // namespace kudu
