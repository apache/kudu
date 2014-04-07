// Copyright (c) 2014, Cloudera, inc.
//
// Imported from Impala. Changes include:
// - Namespace and imports.
// - Replaced GetStrErrMsg with ErrnoToString.
// - Replaced StringParser with strings/numbers.
// - Fixes for cpplint.

#include "util/os-util.h"

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unistd.h>

#include "gutil/strings/numbers.h"
#include "gutil/strings/substitute.h"
#include "util/errno.h"

using boost::is_any_of;
using boost::token_compress_on;
using std::ifstream;
using std::istreambuf_iterator;
using std::stringstream;
using strings::Substitute;

namespace kudu {

// Ensure that Impala compiles on earlier kernels. If the target kernel does not support
// _SC_CLK_TCK, sysconf(_SC_CLK_TCK) will return -1.
#ifndef _SC_CLK_TCK
#define _SC_CLK_TCK 2
#endif

static const int64_t TICKS_PER_SEC = sysconf(_SC_CLK_TCK);

// Offsets into the ../stat file array of per-thread statistics
static const int64_t USER_TICKS = 13;
static const int64_t KERNEL_TICKS = 14;
static const int64_t IO_WAIT = 41;

// Largest offset we are interested in, to check we get a well formed stat file.
static const int64_t MAX_OFFSET = IO_WAIT;

Status GetThreadStats(int64_t tid, ThreadStats* stats) {
  DCHECK(stats != NULL);
  if (TICKS_PER_SEC <= 0) {
    return Status::NotSupported("ThreadStats not supported");
  }

  stringstream proc_path;
  proc_path << "/proc/self/task/" << tid << "/stat";
  if (!boost::filesystem::exists(proc_path.str())) {
    return Status::NotFound("Thread path does not exist");
  }

  ifstream proc_file(proc_path.str().c_str());
  if (!proc_file.is_open()) {
    return Status::IOError("Could not open ifstream");
  }

  string buffer((istreambuf_iterator<char>(proc_file)),
      istreambuf_iterator<char>());

  vector<string> splits;
  split(splits, buffer, is_any_of(" "), token_compress_on);
  if (splits.size() < MAX_OFFSET) {
    return Status::IOError("Unrecognised /proc format");
  }

  int64 tmp;
  if (safe_strto64(splits[USER_TICKS], &tmp)) {
    stats->user_ns = tmp * (1e9 / TICKS_PER_SEC);
  }
  if (safe_strto64(splits[KERNEL_TICKS], &tmp)) {
    stats->kernel_ns = tmp * (1e9 / TICKS_PER_SEC);
  }
  if (safe_strto64(splits[IO_WAIT], &tmp)) {
    stats->iowait_ns = tmp * (1e9 / TICKS_PER_SEC);
  }
  return Status::OK();
}

bool RunShellProcess(const string& cmd, string* msg) {
  DCHECK(msg != NULL);
  FILE* fp = popen(cmd.c_str(), "r");
  if (fp == NULL) {
    *msg = Substitute("Failed to execute shell cmd: '$0', error was: $1", cmd,
        ErrnoToString(errno));
    return false;
  }
  // Read the first 1024 bytes of any output before pclose() so we have some idea of what
  // happened on failure.
  char buf[1024];
  size_t len = fread(buf, 1, 1024, fp);
  string output;
  output.assign(buf, len);

  // pclose() returns an encoded form of the sub-process' exit code.
  int status = pclose(fp);
  if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
    *msg = output;
    return true;
  }

  *msg = Substitute("Shell cmd: '$0' exited with an error: '$1'. Output was: '$2'", cmd,
      ErrnoToString(errno), output);
  return false;
}

} // namespace kudu
