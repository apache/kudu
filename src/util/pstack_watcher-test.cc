// Copyright (c) 2014, Cloudera, inc.

#include "util/pstack_watcher.h"

#include <gtest/gtest.h>
#include <poll.h>
#include <stdio.h>
#include <tr1/memory>
#include <unistd.h>
#include <vector>

#include "gutil/strings/substitute.h"
#include "util/bitmap.h"
#include "util/env.h"
#include "util/errno.h"
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
  int old_stdout;
  CHECK_ERR(old_stdout = dup(STDOUT_FILENO));
  {
    shared_ptr<FILE> out_fp = RedirectStdout(&stdout_file);
    PCHECK(out_fp.get());
    PstackWatcher watcher(MonoDelta::FromMilliseconds(500));
    while (watcher.IsRunning()) {
      usleep(1000);
    }
  }
  CHECK_ERR(dup2(old_stdout, STDOUT_FILENO));
  PCHECK(stdout = fdopen(STDOUT_FILENO, "w"));

  faststring contents;
  CHECK_OK(ReadFileToString(Env::Default(), stdout_file, &contents));
  ASSERT_STR_CONTAINS(contents.ToString(), "BEGIN STACKS");
  CHECK_ERR(unlink(stdout_file.c_str()));
  ASSERT_GE(fprintf(stdout, "%s\n", contents.ToString().c_str()), 0)
      << "errno=" << errno << ": " << ErrnoToString(errno);
}

} // namespace kudu
