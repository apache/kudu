// Copyright (c) 2013, Cloudera, inc.

#include "twitter-demo/parser.h"

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <errno.h>
#include <unistd.h>

#include "util/env.h"
#include "util/status.h"
#include "util/test_util.h"
#include "gutil/strings/split.h"

namespace kudu {
namespace twitter_demo {

// Return the path of the currently-running executable.
static Status GetExecutablePath(string* result) {
  int size = 64;
  while (true) {
    gscoped_ptr<char[]> buf(new char[size]);
    ssize_t rc = readlink("/proc/self/exe", buf.get(), size);
    if (rc == -1) {
      return Status::IOError("Unable to determine own executable path", "",
                             errno);
    }
    if (rc < size) {
      result->assign(&buf[0], rc);
      break;
    }
    // Buffer wasn't large enough
    size *= 2;
  }
  return Status::OK();
}

// Return the directory of the currently-running executable.
static string GetExecutableDir() {
  string exec;
  CHECK_OK(GetExecutablePath(&exec));
  size_t last_slash = exec.rfind('/');
  if (last_slash == string::npos) {
    return ".";
  } else {
    return exec.substr(0, last_slash);
  }
}

static Status LoadFile(const string& name, vector<string>* lines) {
  string path = GetExecutableDir() + "/" + name;
  faststring data;
  RETURN_NOT_OK(ReadFileToString(Env::Default(), path, &data));

  *lines = strings::Split(data.ToString(), "\n");
  return Status::OK();
}

static void EnsureFileParses(const char* file, TwitterEventType expected_type) {
  TwitterEventParser p;
  TwitterEvent event;

  SCOPED_TRACE(file);
  vector<string> jsons;
  CHECK_OK(LoadFile(file, &jsons));

  int line_number = 1;
  BOOST_FOREACH(const string& json, jsons) {
    if (json.empty()) continue;
    SCOPED_TRACE(json);
    SCOPED_TRACE(line_number);
    ASSERT_STATUS_OK(p.Parse(json, &event));
    ASSERT_EQ(expected_type, event.type);
    line_number++;
  }
}

// example-tweets.txt includes a few hundred tweets collected
// from the sample hose.
TEST(ParserTest, TestParseTweets) {
  EnsureFileParses("example-tweets.txt", TWEET);
}

// example-deletes.txt includes a few hundred deletes collected
// from the sample hose.
TEST(ParserTest, TestParseDeletes) {
  EnsureFileParses("example-deletes.txt", DELETE_TWEET);
}

TEST(ParserTest, TestReformatTime) {
  ASSERT_EQ("20130814063107", TwitterEventParser::ReformatTime("Wed Aug 14 06:31:07 +0000 2013"));
}

} // namespace twitter_demo
} // namespace kudu
