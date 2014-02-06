// Copyright (c) 2014, Cloudera, inc.

#include <vector>
#include <string>

#include <gtest/gtest.h>
#include "util/subprocess.h"
#include "util/test_util.h"

using std::string;
using std::vector;

namespace kudu {

class SubprocessTest : public KuduTest {};

TEST_F(SubprocessTest, TestSimplePipe) {
  vector<string> argv;
  argv.push_back("tr");
  argv.push_back("a-z");
  argv.push_back("A-Z");
  Subprocess p("/usr/bin/tr", argv);
  ASSERT_STATUS_OK(p.Start());

  FILE* out = fdopen(p.ReleaseChildStdinFd(), "w");
  PCHECK(out);
  FILE* in = fdopen(p.from_child_stdout_fd(), "r");
  PCHECK(in);

  fprintf(out, "hello world\n");
  // We have to close 'out' or else tr won't write any output, since
  // it enters a buffered mode if it detects that its input is a FIFO.
  fclose(out);

  char buf[1024];
  ASSERT_EQ(buf, fgets(buf, sizeof(buf), in));
  ASSERT_STREQ("HELLO WORLD\n", &buf[0]);

  int wait_status = 0;
  ASSERT_STATUS_OK(p.Wait(&wait_status));
  ASSERT_TRUE(WIFEXITED(wait_status));
  ASSERT_EQ(0, WEXITSTATUS(wait_status));
}

TEST_F(SubprocessTest, TestKill) {
  vector<string> argv;
  argv.push_back("cat");
  Subprocess p("/bin/cat", argv);
  ASSERT_STATUS_OK(p.Start());

  ASSERT_STATUS_OK(p.Kill(SIGKILL));

  int wait_status = 0;
  ASSERT_STATUS_OK(p.Wait(&wait_status));
  ASSERT_EQ(SIGKILL, WTERMSIG(wait_status));
}

} // namespace kudu
